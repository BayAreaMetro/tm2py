"""Transit assignment module."""

from __future__ import annotations

import inspect
import json as _json
import os
import textwrap
import copy
import pandas as pd
from collections import defaultdict as _defaultdict
from functools import partial
from typing import TYPE_CHECKING, Dict, List, Set, Tuple, Union

from tm2py import tools
from tm2py.components.component import Component
from tm2py.components.demand.prepare_demand import PrepareTransitDemand
from tm2py.emme.manager import EmmeNetwork, EmmeScenario
from tm2py.logger import LogStartEnd
from tm2py.components.network.transit.transit_network import PrepareTransitNetwork

if TYPE_CHECKING:
    from tm2py.config import (
        CcrWeightsConfig,
        CongestedWeightsConfig,
        TransitClassConfig,
        TransitConfig,
        TransitModeConfig,
    )
    from tm2py.controller import RunController


# QUESTION - can we put these functions in the TransitAssignment class? I pulled them out in case Emme was going to be picky about them being intertwined


def time_period_capacity(
    vehicle_capacity: float, headway: float, time_period_duration: float
) -> float:
    """_summary_

    Args:
        vehicle_capacity (float): Vehicle capacity per hour. For vehicles with multiple cars
            (i.e. trainsets), should be the capacity of all of them that are traveling together.
        headway (float): Vehicle (or train sets) per hour.
        time_period_duration (float): duration of the time period in minutes

    Returns:
        float: capacity for the whole time period
    """
    return vehicle_capacity * time_period_duration * 60 / headway


def func_returns_crowded_segment_cost(time_period_duration, weights: CcrWeightsConfig):
    """
    function that returns the calc_segment_cost function for emme assignment, with partial preloaded parameters
    acts like partial as emme does not take partial
    """

    def calc_segment_cost(transit_volume: float, capacity, segment) -> float:
        """Calculates crowding factor for a segment.

        Toronto implementation limited factor between 1.0 and 10.0.
        For use with Emme Capacitated assignment normalize by subtracting 1

        Args:
            time_period_duration(float): time period duration in minutes
            weights (_type_): transit capacity weights
            segment_pax (float): transit passengers for the segment for the time period
            segment: emme line segment

        Returns:
            float: crowding factor for a segment
        """

        from tm2py.config import (
            CcrWeightsConfig,
            EawtWeightsConfig,
            TransitClassConfig,
            TransitConfig,
            TransitModeConfig,
        )

        if transit_volume == 0:
            return 0.0

        line = segment.line
        # segment_capacity = time_period_capacity(
        #     line.vehicle.total_capacity, line.headway, time_period_duration
        # )
        # seated_capacity = time_period_capacity(
        #     line.vehicle.seated_capacity, line.headway, time_period_duration
        # )

        seated_capacity = (
            line.vehicle.seated_capacity * {time_period_duration} * 60 / line.headway
        )

        seated_pax = min(transit_volume, seated_capacity)
        standing_pax = max(transit_volume - seated_pax, 0)

        seated_cost = {weights}.min_seat + ({weights}.max_seat - {weights}.min_seat) * (
            transit_volume / capacity
        ) ** {weights}.power_seat

        standing_cost = {weights}.min_stand + (
            {weights}.max_stand - {weights}.min_stand
        ) * (transit_volume / capacity) ** {weights}.power_stand

        crowded_cost = (seated_cost * seated_pax + standing_cost * standing_pax) / (
            transit_volume + 0.01
        )

        normalized_crowded_cost = max(crowded_cost - 1, 0)

        return normalized_crowded_cost

    # return textwrap.dedent(inspect.getsource(calc_segment_cost))

    return textwrap.dedent(inspect.getsource(calc_segment_cost)).format(
        time_period_duration=time_period_duration, weights=weights
    )


def func_returns_segment_congestion(
    time_period_duration,
    scenario,
    weights: CongestedWeightsConfig,
    use_fares: bool = False,
):
    """
    function that returns the calc_segment_cost function for emme assignment, with partial preloaded parameters
    acts like partial as emme does not take partial
    """
    if use_fares:
        values = scenario.get_attribute_values("TRANSIT_LINE", ["#src_mode"])
        scenario.set_attribute_values("TRANSIT_LINE", ["#src_mode"], values)

    def calc_segment_cost(transit_volume: float, capacity, segment) -> float:
        """Calculates crowding factor for a segment.

        Toronto implementation limited factor between 1.0 and 10.0.
        For use with Emme Capacitated assignment normalize by subtracting 1

        Args:
            time_period_duration(float): time period duration in minutes
            weights (_type_): transit capacity weights
            segment: emme line segment

        Returns:
            float: crowding factor for a segment
        """

        from tm2py.config import (
            CongestedWeightsConfig,
            TransitClassConfig,
            TransitConfig,
            TransitModeConfig,
        )

        if transit_volume <= 0:
            return 0.0

        line = segment.line

        if {use_fares}:
            mode_char = line["#src_mode"]
        else:
            mode_char = line.mode.id

        if mode_char in ["p"]:
            congestion = 0.25 * ((transit_volume / capacity) ** 8)
        else:
            seated_capacity = (
                line.vehicle.seated_capacity
                * {time_period_duration}
                * 60
                / line.headway
            )

            seated_pax = min(transit_volume, seated_capacity)
            standing_pax = max(transit_volume - seated_pax, 0)

            seated_cost = {weights}.min_seat + (
                {weights}.max_seat - {weights}.min_seat
            ) * (transit_volume / capacity) ** {weights}.power_seat

            standing_cost = {weights}.min_stand + (
                {weights}.max_stand - {weights}.min_stand
            ) * (transit_volume / capacity) ** {weights}.power_stand

            crowded_cost = (seated_cost * seated_pax + standing_cost * standing_pax) / (
                transit_volume
            )

            congestion = max(crowded_cost, 1) - 1.0

        return congestion

    return textwrap.dedent(inspect.getsource(calc_segment_cost)).format(
        time_period_duration=time_period_duration, weights=weights, use_fares=use_fares
    )


# def calc_segment_cost_curry(func, time_period_duration: float, weights):
#     """
#     curry function for calc_segment_cost
#     """
#     return (lambda y: func(time_period_duration, weights, y))

# def calc_segment_cost(
#     time_period_duration: float, weights, transit_volume: float, segment
# ) -> float:
#     """Calculates crowding factor for a segment.

#     Toronto implementation limited factor between 1.0 and 10.0.
#     For use with Emme Capacitated assignment normalize by subtracting 1

#     Args:
#         time_period_duration(float): time period duration in minutes
#         weights (_type_): transit capacity weights
#         segment_pax (float): transit passengers for the segment for the time period
#         segment: emme line segment

#     Returns:
#         float: crowding factor for a segment
#     """

#     if transit_volume == 0:
#         return 0.0

#     line = segment.line
#     segment_capacity = time_period_capacity(
#         line.vehicle.total_capacity, line.headway, time_period_duration
#     )
#     seated_capacity = time_period_capacity(
#         line.vehicle.seated_capacity, line.headway, time_period_duration
#     )

#     seated_pax = min(transit_volume, seated_capacity)
#     standing_pax = max(transit_volume - seated_pax, 0)

#     seated_cost = (
#         weights.min_seat
#         + (weights.max_seat - weights.min_seat)
#         * (transit_volume / segment_capacity) ** weights.power_seat
#     )

#     standing_cost = (
#         weights.min_stand
#         + (weights.max_stand - weights.min_stand)
#         * (transit_volume / segment_capacity) ** weights.power_stand
#     )

#     crowded_cost = (seated_cost * seated_pax + standing_cost * standing_pax) / (
#         transit_volume + 0.01
#     )

#     normalized_crowded_cost = max(crowded_cost - 1, 0)

#     return normalized_crowded_cost


def calc_total_offs(line) -> float:
    """Calculate total alightings for a line.

    Args:
        line (_type_): _description_
    """
    # NOTE This was done previously using:
    # total_offs += prev_seg.transit_volume - seg.transit_volume + seg.transit_boardings
    # but offs should equal ons for a whole line, so this seems simpler
    offs = [seg.transit_boardings for seg in line.segments(True)]
    total_offs = sum(offs)
    # added lambda due to divide by zero error
    return total_offs if total_offs >= 0.001 else 9999


def calc_offs_thru_segment(segment) -> float:
    """_summary_

    Args:
        segment (_type_): _description_

    Returns:
        float: _description_
    """
    # SIJIA TODO check that it should be [:segment.number+1] . Not sure if 0-indexed in emme or 1-indexed?
    segments_thru_this_segment = [seg for seg in iter(segment.line.segments(True))][
        : segment.number + 1
    ]
    offs_thru_this_seg = [
        prev_seg.transit_volume - this_seg.transit_volume + this_seg.transit_boardings
        for prev_seg, this_seg in zip(
            segments_thru_this_segment[:-1], segments_thru_this_segment[1:]
        )
    ]
    total_offs_thru_this_seg = sum(offs_thru_this_seg)
    return total_offs_thru_this_seg


def calc_extra_wait_time(
    segment,
    segment_capacity: float,
    eawt_weights,
    mode_config: dict,
    use_fares: bool = False,
):
    """Calculate extra added wait time based on...

    # TODO document fully.

    Args:
        segment (_type_): Emme transit segment object.
        segment_capacity (float): _description_
        eawt_weights: extra added wait time weights
        mode_config: mode character to mode config
        use_fares (bool, optional): _description_. Defaults to False.

    Returns:
        _type_: _description_
    """
    _transit_volume = segment.transit_volume
    _headway = segment.line.headway if segment.line.headway >= 0.1 else 9999
    _total_offs = calc_total_offs(segment.line)
    _offs_thru_segment = calc_offs_thru_segment(segment)

    # TODO Document and add params to config. Have no idea what source is here.
    eawt = (
        eawt_weights.constant
        + eawt_weights.weight_inverse_headway * (1 / _headway)
        + eawt_weights.vcr * (_transit_volume / segment_capacity)
        + eawt_weights.exit_proportion * (_offs_thru_segment / _total_offs)
    )

    if use_fares:
        eawt_factor = (
            1
            if segment.line["#src_mode"] == ""
            else mode_config[segment.line["#src_mode"]]["eawt_factor"]
        )
    else:
        eawt_factor = (
            1
            if segment.line.mode.id == ""
            else mode_config[segment.line.mode.id]["eawt_factor"]
        )

    return eawt * eawt_factor


def calc_adjusted_headway(segment, segment_capacity: float) -> float:
    """Headway adjusted based on ....?

    TODO: add documentation about source and theory behind this.

    Args:
        segment: Emme transit segment object
        segment_capacity (float): _description_

    Returns:
        float: Adjusted headway
    """
    # TODO add to params
    max_hdwy_growth = 1.5
    max_headway = 999.98
    # QUESTION FOR INRO: what is the difference between segment["@phdwy"] and line.headway?
    # is one the perceived headway?
    _transit_volume = segment.transit_volume
    _transit_boardings = segment.transit_boardings
    _previous_headway = segment["@phdwy"]
    _current_headway = segment.line.headway
    _available_capacity = max(
        segment_capacity - _transit_volume + _transit_boardings, 0
    )

    adjusted_headway = min(
        max_headway,
        _previous_headway
        * min((_transit_boardings + 1) / (_available_capacity + 1), 1.5),
    )
    adjusted_headway = max(_current_headway, adjusted_headway)

    return adjusted_headway


def func_returns_calc_updated_perceived_headway(
    time_period_duration, eawt_weights, mode_config, use_fares
):
    """
    function that returns the calc_headway function for emme assignment, with partial preloaded parameters
    acts like partial as emme does not take partial
    """

    def calc_headway(transit_volume, transit_boardings, headway, capacity, segment):
        """Calculate perceived (???) headway updated by ... and extra added wait time.

        # TODO Document more fully.

        Args:
            time_period_duration(float): time period duration in minutes
            segment: Emme Transit segment object
            eawt_weights:
            mode_config:
            use_fares (bool): if true, will use fares

        Returns:
            _type_: _description_
        """
        # QUESTION FOR INRO: Kevin separately put segment.line.headway and headway as an arg.
        # Would they be different? Why?
        # TODO: Either can we label the headways so it is clear what is diff about them or just use single value?

        from tm2py.config import (
            CcrWeightsConfig,
            EawtWeightsConfig,
            TransitClassConfig,
            TransitConfig,
            TransitModeConfig,
        )

        _segment_capacity = capacity

        vcr = transit_volume / _segment_capacity

        _extra_added_wait_time = calc_extra_wait_time(
            segment,
            _segment_capacity,
            {eawt_weights},
            {mode_config},
            {use_fares},
        )

        _adjusted_headway = calc_adjusted_headway(
            segment,
            _segment_capacity,
        )

        return _adjusted_headway + _extra_added_wait_time

    return textwrap.dedent(inspect.getsource(calc_headway)).format(
        time_period_duration=time_period_duration,
        eawt_weights=eawt_weights,
        mode_config=mode_config,
        use_fares=use_fares,
    )


# def calc_headway_curry(func, time_period_duration: float, eawt_weights, mode_config, use_fares):
#     """
#     curry function for calc_segment_cost
#     """
#     return lambda y: func(time_period_duration, eawt_weights, mode_config, y, use_fares)

# def calc_headway(
#     time_period_duration: float,
#     eawt_weights,
#     mode_config,
#     segment,
#     use_fares: bool = False,
# ):
#     """Calculate perceived (???) headway updated by ... and extra added wait time.

#     # TODO Document more fully.

#     Args:
#         time_period_duration(float): time period duration in minutes
#         segment: Emme Transit segment object
#         eawt_weights:
#         mode_config:
#         use_fares (bool): if true, will use fares

#     Returns:
#         _type_: _description_
#     """
#     # QUESTION FOR INRO: Kevin separately put segment.line.headway and headway as an arg.
#     # Would they be different? Why?
#     # TODO: Either can we label the headways so it is clear what is diff about them or just use single value?

#     _segment_capacity = time_period_capacity(
#         segment.line.headway, segment.line.vehicle.total_capacity, time_period_duration
#     )

#     _extra_added_wait_time = calc_extra_wait_time(
#         segment,
#         _segment_capacity,
#         eawt_weights,
#         mode_config,
#         use_fares,
#     )

#     _adjusted_headway = calc_adjusted_headway(
#         segment,
#         _segment_capacity,
#     )

#     return _adjusted_headway + _extra_added_wait_time


EmmeTransitJourneyLevelSpec = List[
    Dict[
        str,
        Union[
            str, bool, List[Dict[str, Union[int, str]]], Dict[str, Union[float, str]]
        ],
    ]
]
EmmeTransitSpec = Dict[
    str,
    Union[
        str,
        Dict[str, Union[str, float, bool, Dict[str, Union[str, float]]]],
        List[str],
        EmmeTransitJourneyLevelSpec,
        None,
    ],
]


class TransitAssignment(Component):
    """Run transit assignment."""

    def __init__(self, controller: "RunController"):
        """Constructor for TransitAssignment.

        Args:
            controller: RunController object.
        """
        super().__init__(controller)
        self.config = self.controller.config.transit
        self.sub_components = {
            "prepare transit demand": PrepareTransitDemand(controller),
        }
        self.transit_network = PrepareTransitNetwork(controller)
        self._demand_matrix = None  # FIXME
        self._num_processors = self.controller.emme_manager.num_processors
        self._time_period = None
        self._scenario = None
        self._transit_emmebank = None

    def validate_inputs(self):
        """Validate the inputs."""
        # TODO

    @property
    def transit_emmebank(self):
        if not self._transit_emmebank:
            self._transit_emmebank = self.controller.emme_manager.transit_emmebank
        return self._transit_emmebank

    @LogStartEnd("Transit assignments")
    def run(self):
        """Run transit assignments."""

        for time_period in self.time_period_names:
            if self.controller.iteration == 0:
                use_ccr = False
                congested_transit_assignment = False
                # update auto times
                print("update_auto_times")
                self.transit_network.update_auto_times(time_period)

                # run extended transit assignment and skimming
                # if run warm start, trim the demands based on extended transit assignment and run congested assignment
                # otherwise run with 0 demands
                if self.controller.config.run.warmstart.warmstart:
                    # import transit demands
                    print("warmstart")
                    self.sub_components["prepare transit demand"].run()
                    print("uncongested")
                    self.run_transit_assign(
                        time_period, use_ccr, congested_transit_assignment
                    )
                    # TODO: run skim
                    # TODO: trim_demand
                    # congested_transit_assignment = self.config.congested_transit_assignment
                    # # apply peaking factor
                    # if self.config.congested.use_peaking_factor:
                    #     path_boardings = self.get_abs_path(
                    #         self.config.output_transit_boardings_path
                    #         )
                    #     ea_df_path = path_boardings.format(period='ea_pnr')
                    #     if (time_period.lower() == 'am') and (os.path.isfile(ea_df_path)==False):
                    #         raise Exception("run ea period first to account for the am peaking factor")
                    #     if (time_period.lower() == 'am') and (os.path.isfile(ea_df_path)==True):
                    #         print("peaking_factor")
                    #         ea_df = pd.read_csv(ea_df_path)
                    #         self._apply_peaking_factor(time_period, ea_df=ea_df)
                    #     if (time_period.lower() == 'pm'):
                    #         self._apply_peaking_factor(time_period)
                    # print("congested")
                    # self.run_transit_assign(time_period, use_ccr, congested_transit_assignment)
                    # if self.config.congested.use_peaking_factor and (time_period.lower() == 'ea'):
                    #     self._apply_peaking_factor(time_period)
                else:
                    self.transit_emmebank.zero_matrix  # TODO: need further test

            else:  # iteration >=1
                use_ccr = self.config.use_ccr
                if time_period in ["EA", "EV", "MD"]:
                    congested_transit_assignment = False
                else:
                    congested_transit_assignment = (
                        self.config.congested_transit_assignment
                    )
                # update auto times
                self.transit_network.update_auto_times(time_period)
                # import transit demands
                self.sub_components["prepare transit demand"].run()

                #                if (self.config.congested.trim_demand_before_congested_transit_assignment and
                #                    congested_transit_assignment):
                #                    use_ccr = False
                #                    congested_transit_assignment =  False
                #                    self.run_transit_assign(time_period, use_ccr, congested_transit_assignment)
                #                    #TODO: run skim
                #                    #TODO: trim_demand

                self.run_transit_assign(
                    time_period, use_ccr, congested_transit_assignment
                )

            # output_summaries
            if self.config.output_stop_usage_path is not None:
                network, class_stop_attrs = self._calc_connector_flows(time_period)
                self._export_connector_flows(network, class_stop_attrs, time_period)
            if self.config.output_transit_boardings_path is not None:
                self._export_boardings_by_line(time_period)
            if self.config.output_transit_segment_path is not None:
                self._export_transit_segment(time_period)
            if self.config.output_station_to_station_flow_path is not None:
                self._export_boardings_by_station(time_period)
            if self.config.output_transfer_at_station_path is not None:
                self._export_transfer_at_stops(time_period)

    @LogStartEnd("Transit assignments for a time period")
    def run_transit_assign(
        self, time_period: str, use_ccr: bool, congested_transit_assignment: bool
    ):

        if use_ccr:
            self._run_ccr_assign(time_period)
        elif congested_transit_assignment:
            self._run_congested_assign(time_period)
        else:
            self._run_extended_assign(time_period)

    def _apply_peaking_factor(self, time_period: str, ea_df=None):
        """apply peaking factors.

        Args:
            time_period: time period name abbreviation
        """
        _emme_scenario = self.transit_emmebank.scenario(time_period)
        _network = _emme_scenario.get_network()
        _duration = self.time_period_durations[time_period.lower()]

        if time_period.lower() == "am":
            for line in _network.transit_lines():
                line["@orig_hdw"] = line.headway
                line_name = line.id
                line_veh = line.vehicle
                line_hdw = line.headway
                line_cap = 60 * _duration * line_veh.total_capacity / line_hdw
                if line_name in ea_df["line_name_am"].to_list():
                    ea_boardings = ea_df.loc[
                        ea_df["line_name_am"] == line_name, "boardings"
                    ].values[0]
                else:
                    ea_boardings = 0
                pnr_peaking_factor = (
                    line_cap - ea_boardings
                ) / line_cap  # substract ea boardings from am parking capacity
                non_pnr_peaking_factor = self.config.congested.am_peaking_factor
                # in Emme transit assignment, the capacity is computed for each transit line as: 60 * _duration * vehicle.total_capacity / line.headway
                # so instead of applying peaking factor to calculated capacity, we can divide line.headway by this peaking factor
                # if ea number of parkers exceed the am parking capacity, set the headway to a very large number
                if pnr_peaking_factor > 0:
                    pnr_line_hdw = line_hdw / pnr_peaking_factor
                else:
                    pnr_line_hdw = 999
                non_pnr_line_hdw = line_hdw * non_pnr_peaking_factor
                if ("pnr" in line_name) and ("egr" in line_name):
                    continue
                elif ("pnr" in line_name) and ("acc" in line_name):
                    line.headway = pnr_line_hdw
                else:
                    line.headway = non_pnr_line_hdw

        if time_period.lower() == "pm":
            for line in _network.transit_lines():
                line["@orig_hdw"] = line.headway
                line_name = line.id
                line_hdw = line.headway
                non_pnr_peaking_factor = self.config.congested.pm_peaking_factor
                non_pnr_line_hdw = line_hdw * non_pnr_peaking_factor
                if "pnr" in line_name:
                    continue
                else:
                    line.headway = non_pnr_line_hdw

        if time_period.lower() == "ea":
            line_name = []
            boards = []
            ea_pnr_df = pd.DataFrame()
            for line in _network.transit_lines():
                boardings = 0
                for segment in line.segments(include_hidden=True):
                    boardings += segment.transit_boardings
                line_name.append(line.id)
                boards.append(boardings)
            ea_pnr_df["line_name"] = line_name
            ea_pnr_df["boardings"] = boards
            ea_pnr_df["line_name_am"] = ea_pnr_df["line_name"].str.replace(
                "EA", "AM"
            )  # will substract ea boardings from am parking capacity
            path_boardings = self.get_abs_path(
                self.config.output_transit_boardings_path
            )
            ea_pnr_df.to_csv(path_boardings.format(period="ea_pnr"), index=False)

        _update_attributes = {"TRANSIT_LINE": ["@orig_hdw", "headway"]}
        self.controller.emme_manager.copy_attribute_values(
            _network, _emme_scenario, _update_attributes
        )

    def _transit_classes(self, time_period) -> List[TransitAssignmentClass]:
        emme_manager = self.controller.emme_manager
        if self.config.use_fares:
            fare_modes = _defaultdict(lambda: set([]))
            network = self.transit_emmebank.scenario(time_period).get_partial_network(
                ["TRANSIT_LINE"], include_attributes=False
            )
            emme_manager.copy_attribute_values(
                self.transit_emmebank.scenario(time_period),
                network,
                {"TRANSIT_LINE": ["#src_mode"]},
            )
            for line in network.transit_lines():
                fare_modes[line["#src_mode"]].add(line.mode.id)
        else:
            fare_modes = None
        spec_dir = os.path.join(
            self.get_abs_path(
                os.path.dirname(self.controller.config.emme.project_path)
            ),
            "Specifications",
        )
        transit_classes = []
        for class_config in self.config.classes:
            transit_classes.append(
                TransitAssignmentClass(
                    class_config,
                    self.config,
                    time_period,
                    self.controller.iteration,
                    self._num_processors,
                    fare_modes,
                    spec_dir,
                )
            )
        return transit_classes

    def _run_ccr_assign(self, time_period: str) -> None:
        """Runs capacity constrained (??) CCR transit assignment for a time period + update penalties.

        Args:
            time_period: time period name
        """
        _duration = self.time_period_durations[time_period.lower()]
        _ccr_weights = self.config.ccr_weights
        _eawt_weights = self.config.eawt_weights
        _mode_config = {
            mode_config.mode_id: mode_config for mode_config in self.config.modes
        }
        _emme_scenario = self.transit_emmebank.scenario(time_period)
        transit_classes = self._transit_classes(time_period)

        assign_transit = self.controller.emme_manager.tool(
            "inro.emme.transit_assignment.capacitated_transit_assignment"
        )
        _tclass_specs = [tclass.emme_transit_spec for tclass in transit_classes]
        _tclass_names = [tclass.name for tclass in transit_classes]

        # NOTE TO SIJIA
        # If sending the actual function doesn't work in EMME and its needs the TEXT of the
        # function, then you can send it using
        #
        # put at top of code:
        # import inspect.getsource
        #
        # replace _cost_func["python_function"]:... with
        # "python_function":  inspect.getsource(partial.crowded_segment_cost(_duration, _ccr_weights))
        #
        # do similar with _headway_cost_function, etc.

        # segment_curry = calc_segment_cost_curry(
        #     calc_segment_cost, _duration, _ccr_weights
        # )

        # headway_curry = calc_headway_curry(
        #         calc_headway,
        #         _duration,
        #         _eawt_weights,
        #         _mode_config,
        #         use_fares=self.config.use_fares,
        # )

        _cost_func = {
            "segment": {
                "type": "CUSTOM",
                # "python_function": textwrap.dedent(inspect.getsource(segment_curry)),
                # "python_function": textwrap.dedent(inspect.getsource(lambda y: calc_segment_cost(_duration, _ccr_weights, y))),
                "python_function": func_returns_crowded_segment_cost(
                    _duration, _ccr_weights
                ),
                "congestion_attribute": "us3",
                "orig_func": False,
            },
            "headway": {
                "type": "CUSTOM",
                # "python_function": textwrap.dedent(inspect.getsource(headway_curry)),
                "python_function": func_returns_calc_updated_perceived_headway(
                    _duration,
                    _eawt_weights,
                    _mode_config,
                    use_fares=self.config.use_fares,
                )
                + "\n"
                + textwrap.dedent(inspect.getsource(calc_extra_wait_time))
                + "\n"
                + textwrap.dedent(inspect.getsource(calc_adjusted_headway))
                + "\n"
                + textwrap.dedent(inspect.getsource(calc_total_offs))
                + "\n"
                + textwrap.dedent(inspect.getsource(calc_offs_thru_segment)),
            },
            "assignment_period": _duration,
        }

        _stop_criteria = {
            "max_iterations": self.config.ccr_stop_criteria.max_iterations,
            "relative_difference": self.config.ccr_stop_criteria.relative_difference,
            "percent_segments_over_capacity": self.config.ccr_stop_criteria.percent_segments_over_capacity,
        }
        add_volumes = False
        assign_transit(
            _tclass_specs,
            congestion_function=_cost_func,
            stopping_criteria=_stop_criteria,
            class_names=_tclass_names,
            scenario=_emme_scenario,
            log_worksheets=False,
        )
        add_volumes = True

        # question - why do we need to do this between iterations AND ALSO give it to the EMME cost function? Does EMME not use it?
        self._calc_segment_ccr_penalties(time_period)

    def _run_congested_assign(self, time_period: str) -> None:
        """Runs congested transit assignment for a time period.

        Args:
            time_period: time period name
        """
        _duration = self.time_period_durations[time_period.lower()]
        _congested_weights = self.config.congested_weights
        _emme_scenario = self.transit_emmebank.scenario(time_period)
        transit_classes = self._transit_classes(time_period)

        assign_transit = self.controller.emme_manager.tool(
            "inro.emme.transit_assignment.congested_transit_assignment"
        )
        _tclass_specs = [tclass.emme_transit_spec for tclass in transit_classes]
        _tclass_names = [tclass.name for tclass in transit_classes]

        _cost_func = {
            "type": "CUSTOM",
            "python_function": func_returns_segment_congestion(
                _duration,
                _emme_scenario,
                _congested_weights,
                use_fares=self.config.use_fares,
            ),
            "congestion_attribute": "us3",
            "orig_func": False,
            "assignment_period": _duration,
        }

        _stop_criteria = {
            "max_iterations": self.congested_transit_assn_max_iteration[
                time_period.lower()
            ],
            "normalized_gap": self.config.congested.normalized_gap,
            "relative_gap": self.config.congested.relative_gap,
        }
        add_volumes = False
        assign_transit(
            _tclass_specs,
            congestion_function=_cost_func,
            stopping_criteria=_stop_criteria,
            class_names=_tclass_names,
            scenario=_emme_scenario,
            log_worksheets=False,
        )
        add_volumes = True

    def _run_extended_assign(self, time_period: str) -> None:
        """Run transit assignment without CCR.

        Args:
            time_period (_type_): time period name
        """
        assign_transit = self.controller.emme_manager.modeller.tool(
            "inro.emme.transit_assignment.extended_transit_assignment"
        )
        _emme_scenario = self.transit_emmebank.scenario(time_period)

        # Question for INRO: Why are we only adding subsequent volumes shouldn't it assume to be
        #   zero to begin with?
        # Question for INRO: Can this function be distributed across machines? If so, how would
        #   that be structured?
        add_volumes = False
        for tclass in self._transit_classes(time_period):
            assign_transit(
                tclass.emme_transit_spec,
                class_name=tclass.name,
                add_volumes=add_volumes,
                scenario=_emme_scenario,
            )
            add_volumes = True

    def _get_network_with_boardings(
        self, emme_scenario: "EmmeScenario"
    ) -> "EmmeNetwork":
        """Get networkw ith transit boardings by line and segment.

        Args:
            emme_scenario (_type_):

        Returns:
            EmmeNetwork: with transit boardings by line and segment.
        """
        network = emme_scenario.get_partial_network(
            ["TRANSIT_LINE", "TRANSIT_SEGMENT"], include_attributes=False
        )
        _attributes = {
            "TRANSIT_LINE": ["description", "#src_mode"],
            "TRANSIT_SEGMENT": ["transit_boardings"],
        }
        _emme_manager = self.controller.emme_manager
        _emme_manager.copy_attribute_values(emme_scenario, network, _attributes)
        return network

    def _export_boardings_by_line(self, time_period: str) -> None:
        """Export total boardings by line to config.transit.output_transit_boardings_file.

        args:
            time_period (str): time period abbreviation
        """
        _emme_scenario = self.transit_emmebank.scenario(time_period)
        network = _emme_scenario.get_network()

        output_transit_boardings_file = self.get_abs_path(
            self.config.output_transit_boardings_path
        )

        os.makedirs(os.path.dirname(output_transit_boardings_file), exist_ok=True)

        with open(
            output_transit_boardings_file.format(period=time_period.lower()),
            "w",
            encoding="utf8",
        ) as out_file:
            out_file.write(
                ",".join(
                    [
                        "line_name",
                        "description",
                        "total_boarding",
                        "total_hour_cap",
                        "tm2_mode",
                        "line_mode",
                        "headway",
                        "fare_system",
                    ]
                )
            )
            out_file.write("\n")
            for line in network.transit_lines():
                boardings = 0
                capacity = line.vehicle.total_capacity
                hdw = line.headway
                line_hour_cap = 60 * capacity / hdw
                if self.config.use_fares:
                    mode = line["#src_mode"]
                else:
                    mode = line.mode
                for segment in line.segments(include_hidden=True):
                    boardings += segment.transit_boardings
                    # total_board = sum(seg.transit_boardings for seg in line.segments)
                out_file.write(
                    ",".join(
                        [
                            str(x)
                            for x in [
                                line.id,
                                line["#description"],
                                boardings,
                                line_hour_cap,
                                line["#mode"],
                                mode,
                                line.headway,
                                line["#faresystem"],
                            ]
                        ]
                    )
                )
                out_file.write("\n")

    def _calc_connector_flows(
        self, time_period: str
    ) -> Tuple["EmmeNetwork", Dict[str, str]]:
        """Calculate boardings and alightings by assignment class.

        args:
            time_period (str): time period abbreviation

        returns:
            EmmeNetwork with aux_transit_volumes
            transit class stop attributes: {<transit_class_name>: @aux_volume_<transit_class_name>...}
        """
        _emme_manager = self.controller.emme_manager
        _emme_scenario = self.transit_emmebank.scenario(time_period)
        network_results = _emme_manager.tool(
            "inro.emme.transit_assignment.extended.network_results"
        )
        create_extra = _emme_manager.tool(
            "inro.emme.data.extra_attribute.create_extra_attribute"
        )
        tclass_stop_attrs = {}
        for tclass in self.config.classes:
            attr_name = f"@aux_vol_{tclass.name}".lower()  # maximum length 20 limit
            create_extra("LINK", attr_name, overwrite=True, scenario=_emme_scenario)
            spec = {
                "type": "EXTENDED_TRANSIT_NETWORK_RESULTS",
                "on_links": {"aux_transit_volumes": attr_name},
            }
            network_results(spec, class_name=tclass.name, scenario=_emme_scenario)
            tclass_stop_attrs[tclass.name] = attr_name

        # optimization: partial network to only load links and certain attributes
        network = _emme_scenario.get_partial_network(["LINK"], include_attributes=True)
        attributes = {
            "LINK": tclass_stop_attrs.values(),
            "NODE": ["@taz_id", "#node_id"],
        }
        _emme_manager.copy_attribute_values(_emme_scenario, network, attributes)
        return network, tclass_stop_attrs

    def _export_connector_flows(
        self, network: EmmeNetwork, class_stop_attrs: Dict[str, str], time_period: str
    ):
        """Export boardings and alightings by assignment class, stop(connector) and TAZ.

        args:
            network: network to use
            class_stop_attrs: list of attributes to export
        """
        path_tmplt = self.get_abs_path(self.config.output_stop_usage_path)
        os.makedirs(os.path.dirname(path_tmplt), exist_ok=True)
        with open(
            path_tmplt.format(period=time_period.lower()), "w", encoding="utf8"
        ) as out_file:
            out_file.write(",".join(["mode", "taz", "stop", "boardings", "alightings"]))
            out_file.write("\n")
            for zone in network.centroids():
                taz_id = int(zone["@taz_id"])
                for link in zone.outgoing_links():
                    stop_id = link.j_node["#node_id"]
                    for name, attr_name in class_stop_attrs.items():
                        alightings = (
                            link.reverse_link[attr_name] if link.reverse_link else 0.0
                        )
                        out_file.write(
                            f"{name}, {taz_id}, {stop_id}, {link[attr_name]}, {alightings}\n"
                        )
                for link in zone.incoming_links():
                    if link.reverse_link:  # already exported
                        continue
                    stop_id = link.i_node["#node_id"]
                    for name, attr_name in class_stop_attrs.items():
                        out_file.write(
                            f"{name}, {taz_id}, {stop_id}, 0.0, {link[attr_name]}\n"
                        )

    def _export_transit_segment(self, time_period: str):
        # add total boardings by access mode
        _emme_manager = self.controller.emme_manager
        _emme_scenario = self.transit_emmebank.scenario(time_period)
        network_results = _emme_manager.tool(
            "inro.emme.transit_assignment.extended.network_results"
        )
        create_extra = _emme_manager.tool(
            "inro.emme.data.extra_attribute.create_extra_attribute"
        )
        for tclass in self.config.classes:
            initial_board_attr_name = f"@iboard_{tclass.name}".lower()
            direct_xboard_attr_name = f"@dboard_{tclass.name}".lower()
            auxiliary_xboard_attr_name = f"@aboard_{tclass.name}".lower()
            create_extra(
                "TRANSIT_SEGMENT",
                initial_board_attr_name,
                overwrite=True,
                scenario=_emme_scenario,
            )
            create_extra(
                "TRANSIT_SEGMENT",
                direct_xboard_attr_name,
                overwrite=True,
                scenario=_emme_scenario,
            )
            create_extra(
                "TRANSIT_SEGMENT",
                auxiliary_xboard_attr_name,
                overwrite=True,
                scenario=_emme_scenario,
            )
            spec = {
                "type": "EXTENDED_TRANSIT_NETWORK_RESULTS",
                "on_segments": {
                    "initial_boardings": initial_board_attr_name,
                    "transfer_boardings_direct": direct_xboard_attr_name,
                    "transfer_boardings_indirect": auxiliary_xboard_attr_name,
                },
            }
            network_results(spec, class_name=tclass.name, scenario=_emme_scenario)

        network = _emme_scenario.get_network()
        path_boardings = self.get_abs_path(self.config.output_transit_segment_path)
        with open(path_boardings.format(period=time_period.lower()), "w") as f:
            f.write(
                ",".join(
                    [
                        "line",
                        "stop_name",
                        "i_node",
                        "j_node",
                        "dwt",
                        "ttf",
                        "voltr",
                        "board",
                        "con_time",
                        "uncon_time",
                        "mode",
                        "src_mode",
                        "mdesc",
                        "hdw",
                        "orig_hdw",
                        "speed",
                        "vauteq",
                        "vcaps",
                        "vcapt",
                        "initial_board_ptw",
                        "initial_board_wtp",
                        "initial_board_ktw",
                        "initial_board_wtk",
                        "initial_board_wtw",
                        "direct_transfer_board_ptw",
                        "direct_transfer_board_wtp",
                        "direct_transfer_board_ktw",
                        "direct_transfer_board_wtk",
                        "direct_transfer_board_wtw",
                        "auxiliary_transfer_board_ptw",
                        "auxiliary_transfer_board_wtp",
                        "auxiliary_transfer_board_ktw",
                        "auxiliary_transfer_board_wtk",
                        "auxiliary_transfer_board_wtw",
                    ]
                )
            )
            f.write("\n")

            for line in network.transit_lines():
                for segment in line.segments(include_hidden=True):
                    if self.config.use_fares:
                        mode = segment.line["#src_mode"]
                    else:
                        mode = segment.line.mode
                    if self.config.congested.use_peaking_factor and (
                        time_period.lower() in ["am", "pm"]
                    ):
                        orig_headway = segment.line["@orig_hdw"]
                    else:
                        orig_headway = segment.line.headway
                    f.write(
                        ",".join(
                            [
                                str(x)
                                for x in [
                                    segment.id,
                                    '"{0}"'.format(segment["#stop_name"]),
                                    segment.i_node,
                                    segment.j_node,
                                    segment.dwell_time,
                                    segment.transit_time_func,
                                    segment.transit_volume,
                                    segment.transit_boardings,
                                    segment.transit_time,
                                    segment["@trantime_seg"],
                                    segment.line.mode,
                                    mode,
                                    segment.line.mode.description,
                                    segment.line.headway,
                                    orig_headway,
                                    segment.line.speed,
                                    segment.line.vehicle.auto_equivalent,
                                    segment.line.vehicle.seated_capacity,
                                    segment.line.vehicle.total_capacity,
                                    segment["@iboard_pnr_trn_wlk"],
                                    segment["@iboard_wlk_trn_pnr"],
                                    segment["@iboard_knr_trn_wlk"],
                                    segment["@iboard_wlk_trn_knr"],
                                    segment["@iboard_wlk_trn_wlk"],
                                    segment["@dboard_pnr_trn_wlk"],
                                    segment["@dboard_wlk_trn_pnr"],
                                    segment["@dboard_knr_trn_wlk"],
                                    segment["@dboard_wlk_trn_knr"],
                                    segment["@dboard_wlk_trn_wlk"],
                                    segment["@aboard_pnr_trn_wlk"],
                                    segment["@aboard_wlk_trn_pnr"],
                                    segment["@aboard_knr_trn_wlk"],
                                    segment["@aboard_wlk_trn_knr"],
                                    segment["@aboard_wlk_trn_wlk"],
                                ]
                            ]
                        )
                    )
                    f.write("\n")

    def _export_boardings_by_station(self, time_period: str):
        _emme_manager = self.controller.emme_manager
        _emme_scenario = self.transit_emmebank.scenario(time_period)
        network = _emme_scenario.get_network()
        sta2sta = _emme_manager.tool(
            "inro.emme.transit_assignment.extended.station_to_station_analysis"
        )
        sta2sta_spec = {
            "type": "EXTENDED_TRANSIT_STATION_TO_STATION_ANALYSIS",
            "transit_line_selections": {
                "first_boarding": "mode=h",
                "last_alighting": "mode=h",
            },
            "analyzed_demand": None,
        }

        # map to used modes in apply fares case
        fare_modes = _defaultdict(lambda: set([]))
        for line in network.transit_lines():
            if self.config.use_fares:
                fare_modes[line["#src_mode"]].add(line.mode.id)
            else:
                fare_modes[line.mode.id].add(line.mode.id)

        operator_dict = {
            # mode: network_selection
            "bart": "h",
            "caltrain": "r",
        }

        for tclass in self.config.classes:
            for op, cut in operator_dict.items():
                demand_matrix = "mfTRN_%s_%s" % (tclass.name, time_period)
                output_file_name = self.get_abs_path(
                    self.config.output_station_to_station_flow_path
                )

                sta2sta_spec["transit_line_selections"][
                    "first_boarding"
                ] = "mode=" + ",".join(list(fare_modes[cut]))
                sta2sta_spec["transit_line_selections"][
                    "last_alighting"
                ] = "mode=" + ",".join(list(fare_modes[cut]))
                sta2sta_spec["analyzed_demand"] = demand_matrix

                output_path = output_file_name.format(
                    operator=op, tclass=tclass.name, period=time_period.lower()
                )
                sta2sta(
                    specification=sta2sta_spec,
                    output_file=output_path,
                    scenario=_emme_scenario,
                    append_to_output_file=False,
                    class_name=tclass.name,
                )

    def _export_transfer_at_stops(self, time_period: str):
        _emme_manager = self.controller.emme_manager
        _emme_scenario = self.transit_emmebank.scenario(time_period)
        network = _emme_scenario.get_network()
        transfers_at_stops = _emme_manager.tool(
            "inro.emme.transit_assignment.extended.apps.transfers_at_stops"
        )

        stop_location = self.config.output_transfer_at_station_node_ids
        stop_location_val_key = {val: key for key, val in stop_location.items()}

        for node in network.nodes():
            if stop_location_val_key.get(node["#node_id"]):
                stop_location[stop_location_val_key[node["#node_id"]]] = node.id

        for tclass in self.config.classes:
            for stop_name, stop_id in stop_location.items():
                demand_matrix = "mfTRN_%s_%s" % (tclass.name, time_period)
                output_file_name = self.get_abs_path(
                    self.config.output_transfer_at_station_path
                )
                output_path = output_file_name.format(
                    tclass=tclass.name, stop=stop_name, period=time_period.lower()
                )

                transfers_at_stops(
                    selection=f"i={stop_id}",
                    export_path=output_path,
                    scenario=_emme_scenario,
                    class_name=tclass.name,
                    analyzed_demand=demand_matrix,
                )

    def _add_ccr_vars_to_scenario(self, emme_scenario: "EmmeScenario") -> None:
        """Add Extra Added Wait Time and Capacity Penalty to emme scenario.

        Args:
            emme_scenario : EmmeScenario
        """
        create_extra = self.controller.emme_manager.tool(
            "inro.emme.data.extra_attribute.create_extra_attribute"
        )
        create_extra(
            "TRANSIT_SEGMENT",
            "@eawt",
            "extra added wait time",
            overwrite=True,
            scenario=emme_scenario,
        )
        create_extra(
            "TRANSIT_SEGMENT",
            "@capacity_penalty",
            "capacity penalty at boarding",
            overwrite=True,
            scenario=emme_scenario,
        )

    def _get_network_with_ccr_scenario_attributes(self, emme_scenario):

        self._add_ccr_vars_to_scenario(emme_scenario)

        _attributes = {
            "TRANSIT_SEGMENT": [
                "@phdwy",
                "transit_volume",
                "transit_boardings",
            ],
            "TRANSIT_VEHICLE": ["seated_capacity", "total_capacity"],
            "TRANSIT_LINE": ["headway"],
        }
        if self.config.use_fares:
            _attributes["TRANSIT_LINE"].append("#src_mode")

        # load network object from scenario (on disk) and copy some attributes
        network = emme_scenario.get_partial_network(
            ["TRANSIT_SEGMENT"], include_attributes=False
        )
        network.create_attribute("TRANSIT_LINE", "capacity")

        self.emme_manager.copy_attribute_values(emme_scenario, network, _attributes)
        return network

    def _calc_segment_ccr_penalties(self, time_period):
        """Calculate extra average wait time (@eawt) and @capacity_penalty on the segments.

        TODO: INRO Please document


        """
        _emme_scenario = self.transit_emmebank.scenario(time_period)
        _network = self._get_network_with_ccr_scenario_attributes(_emme_scenario)

        _eawt_weights = self.config.eawt_weights
        _mode_config = {
            mode_config.mode_id: mode_config for mode_config in self.config.modes
        }

        _duration = self.time_period_durations[time_period.lower()]
        for line in _network.transit_lines():
            line.capacity = time_period_capacity(
                line.vehicle.total_capacity, line.headway, _duration
            )

        # QUESTION: document origin of this param.
        _hdwy_fraction = 0.5  # fixed in assignment spec
        for segment in _network.transit_segments():
            segment["@eawt"] = calc_extra_wait_time(
                segment,
                segment.line.capacity,
                _eawt_weights,
                _mode_config,
                use_fares=self.config.use_fares,
            )
            segment["@capacity_penalty"] = (
                max(segment["@phdwy"] - segment["@eawt"] - segment.line.headway, 0)
                * _hdwy_fraction
            )
        # copy (save) results back from the network to the scenario (on disk)
        _ccr_attributes = {"TRANSIT_SEGMENT": ["@eawt", "@capacity_penalty"]}
        self.emme_manager.copy_attribute_values(
            _network, _emme_scenario, _ccr_attributes
        )


class TransitAssignmentClass:
    """Transit assignment class, represents data from config and conversion to Emme specs.

    Internal properties:
        _name: the class name loaded from config (not to be changed)
        _class_config: the transit class config (TransitClassConfig)
        _transit_config: the root transit assignment config (TransitConfig)
        _time_period: the time period name
        _iteration: the current iteration
        _num_processors: the number of processors to use, loaded from config
        _fare_modes: the mapping from the generated fare mode ID to the original
            source mode ID
        _spec_dir: directory to find the generated journey levels tables from
            the apply fares step
    """

    # disable too many instance attributes and arguments recommendations
    # pylint: disable=R0902, R0913

    def __init__(
        self,
        tclass_config: TransitClassConfig,
        config: TransitConfig,
        time_period: str,
        iteration: int,
        num_processors: int,
        fare_modes: Dict[str, Set[str]],
        spec_dir: str,
    ):
        """Assignment class constructor.

        Args:
            tclass_config: the transit class config (TransitClassConfig)
            config: the root transit assignment config (TransitConfig)
            time_period: the time period name
            iteration: the current iteration
            num_processors: the number of processors to use, loaded from config
            fare_modes: the mapping from the generated fare mode ID to the original
                source mode ID
            spec_dir: directory to find the generated journey levels tables from
                the apply fares step
        """
        self._name = tclass_config.name
        self._class_config = tclass_config
        self._config = config
        self._time_period = time_period
        self._iteration = iteration
        self._num_processors = num_processors
        self._fare_modes = fare_modes
        self._spec_dir = spec_dir

    @property
    def name(self) -> str:
        """The class name."""
        return self._name

    @property
    def emme_transit_spec(self) -> EmmeTransitSpec:
        """Return Emme Extended transit assignment specification.

        Converted from input config (transit.classes, with some parameters from
        transit table), see also Emme Help for
        Extended transit assignment for specification details.

        """
        spec = {
            "type": "EXTENDED_TRANSIT_ASSIGNMENT",
            "modes": self._modes,
            "demand": self._demand_matrix,
            "waiting_time": {
                "effective_headways": self._config.effective_headway_source,
                "headway_fraction": "@hdw_fraction",
                "perception_factor": self._config.initial_wait_perception_factor,
                "spread_factor": 1.0,
            },
            "boarding_cost": {"global": {"penalty": 0, "perception_factor": 1}},
            "boarding_time": {
                "on_lines": {
                    "penalty": "@iboard_penalty",
                    "perception_factor": 1,
                }
            },
            "in_vehicle_cost": None,
            "in_vehicle_time": {"perception_factor": "@invehicle_factor"},
            "aux_transit_time": {
                "perception_factor": 1
            },  # walk and drive perception factors are specified in mode definition "speed_or_time_factor"
            "aux_transit_cost": None,
            "journey_levels": self._journey_levels,
            "flow_distribution_between_lines": {"consider_total_impedance": True},
            "flow_distribution_at_origins": {
                "fixed_proportions_on_connectors": None,
                "choices_at_origins": "OPTIMAL_STRATEGY",
            },
            "flow_distribution_at_regular_nodes_with_aux_transit_choices": {
                "choices_at_regular_nodes": "OPTIMAL_STRATEGY"
            },
            "circular_lines": {"stay": False},
            "connector_to_connector_path_prohibition": None,
            "od_results": {"total_impedance": None},
            "performance_settings": {"number_of_processors": self._num_processors},
        }
        if self._config.use_fares:
            fare_perception = 60 / self._config.value_of_time
            spec["boarding_cost"] = {
                "on_segments": {
                    "penalty": "@board_cost",
                    "perception_factor": fare_perception,
                }
            }
            spec["in_vehicle_cost"] = {
                "penalty": "@invehicle_cost",
                "perception_factor": fare_perception,
            }
        # Optional aux_transit_cost, used for walk time on connectors,
        #          set if override_connector_times is on
        if self._config.get("override_connector_times", False):
            spec["aux_transit_cost"] = {
                "penalty": f"@walk_time_{self.name.lower()}",
                "perception_factor": self._config.walk_perception_factor,
            }
        return spec

    @property
    def _demand_matrix(self) -> str:
        # if self._iteration < 1:
        #     return 'ms"zero"'  # zero demand matrix
        return f'mf"TRN_{self._class_config.skim_set_id}_{self._time_period}"'

    def _get_used_mode_ids(self, modes: List[TransitModeConfig]) -> List[str]:
        """Get list of assignment Mode IDs from input list of Emme mode objects.

        Accounts for fare table (mapping from input mode ID to auto-generated
        set of mode IDs for fare transition table (fares.far input) by applyfares
        component.
        """
        if self._config.use_fares:
            out_modes = set([])
            for mode in modes:
                if mode.assign_type == "TRANSIT":
                    out_modes.update(self._fare_modes[mode.mode_id])
                else:
                    out_modes.add(mode.mode_id)
            return list(out_modes)
        return [mode.mode_id for mode in modes]

    @property
    def _modes(self) -> List[str]:
        """List of modes IDs (str) to use in assignment for this class."""
        all_modes = self._config.modes
        mode_types = self._class_config.mode_types
        modes = [mode for mode in all_modes if mode.type in mode_types]
        return self._get_used_mode_ids(modes)

    @property
    def _transit_modes(self) -> List[str]:
        """List of transit modes IDs (str) to use in assignment for this class."""
        all_modes = self._config.modes
        mode_types = self._class_config.mode_types
        modes = [
            mode
            for mode in all_modes
            if mode.type in mode_types and mode.assign_type == "TRANSIT"
        ]
        return self._get_used_mode_ids(modes)

    @property
    def fare_perception(self):
        return 60 / self._config.value_of_time

    @property
    def headway_fraction(self):
        return 0.5

    @property
    def _journey_levels(self) -> EmmeTransitJourneyLevelSpec:
        modes = self._transit_modes
        effective_headway_source = self._config.effective_headway_source
        if self._config.use_fares:
            fare_perception = self.fare_perception
            file_name = f"{self._time_period}_ALLPEN_journey_levels.ems"
            with open(
                os.path.join(self._spec_dir, file_name), "r", encoding="utf8"
            ) as jl_spec:
                journey_levels = _json.load(jl_spec)["journey_levels"]

            if self.name == "PNR_TRN_WLK":
                new_journey_levels = copy.deepcopy(journey_levels)

                for i in range(0, len(new_journey_levels)):
                    jls = new_journey_levels[i]
                    for level in jls["transition_rules"]:
                        level["next_journey_level"] = level["next_journey_level"] + 1
                    jls["transition_rules"].extend(
                        [
                            {"mode": "e", "next_journey_level": i + 2},
                            {
                                "mode": "D",
                                "next_journey_level": len(new_journey_levels) + 2,
                            },
                            {"mode": "w", "next_journey_level": i + 2},
                            {
                                "mode": "p",
                                "next_journey_level": len(new_journey_levels) + 2,
                            },
                        ]
                    )
                # level 0: drive access
                transition_rules_drive_access = copy.deepcopy(
                    journey_levels[0]["transition_rules"]
                )
                for level in transition_rules_drive_access:
                    level["next_journey_level"] = len(new_journey_levels) + 2
                transition_rules_drive_access.extend(
                    [
                        {
                            "mode": "e",
                            "next_journey_level": len(new_journey_levels) + 2,
                        },
                        {"mode": "D", "next_journey_level": 0},
                        {
                            "mode": "w",
                            "next_journey_level": len(new_journey_levels) + 2,
                        },
                        {"mode": "p", "next_journey_level": 1},
                    ]
                )
                # level 1: use transit
                transition_rules_pnr = copy.deepcopy(
                    journey_levels[0]["transition_rules"]
                )
                for level in transition_rules_pnr:
                    level["next_journey_level"] = 2
                transition_rules_pnr.extend(
                    [
                        {
                            "mode": "e",
                            "next_journey_level": len(new_journey_levels) + 2,
                        },
                        {
                            "mode": "D",
                            "next_journey_level": len(new_journey_levels) + 2,
                        },
                        {
                            "mode": "w",
                            "next_journey_level": len(new_journey_levels) + 2,
                        },
                        {"mode": "p", "next_journey_level": 1},
                    ]
                )
                # level len(new_journey_levels)+2: every mode is prohibited
                transition_rules_prohibit = copy.deepcopy(
                    journey_levels[0]["transition_rules"]
                )
                for level in transition_rules_prohibit:
                    level["next_journey_level"] = len(new_journey_levels) + 2
                transition_rules_prohibit.extend(
                    [
                        {
                            "mode": "e",
                            "next_journey_level": len(new_journey_levels) + 2,
                        },
                        {
                            "mode": "D",
                            "next_journey_level": len(new_journey_levels) + 2,
                        },
                        {
                            "mode": "w",
                            "next_journey_level": len(new_journey_levels) + 2,
                        },
                        {
                            "mode": "p",
                            "next_journey_level": len(new_journey_levels) + 2,
                        },
                    ]
                )
                new_journey_levels.insert(
                    0,
                    {
                        "description": "drive access",
                        "destinations_reachable": False,
                        "transition_rules": transition_rules_drive_access,
                        "waiting_time": None,
                        "boarding_time": None,
                        "boarding_cost": None,
                    },
                )
                new_journey_levels.insert(
                    1,
                    {
                        "description": "pnr",
                        "destinations_reachable": False,
                        "transition_rules": transition_rules_pnr,
                        "waiting_time": None,
                        "boarding_time": None,
                        "boarding_cost": None,
                    },
                )
                new_journey_levels.append(
                    {
                        "description": "prohibit",
                        "destinations_reachable": False,
                        "transition_rules": transition_rules_prohibit,
                        "waiting_time": None,
                        "boarding_time": None,
                        "boarding_cost": None,
                    }
                )
                for level in new_journey_levels[2:-1]:
                    level["waiting_time"] = {
                        "headway_fraction": "@hdw_fraction",
                        "effective_headways": effective_headway_source,
                        "spread_factor": 1,
                        "perception_factor": "@wait_pfactor",
                    }
                    level["boarding_time"] = {
                        "on_lines": {
                            "penalty": "@xboard_penalty",
                            "perception_factor": 1,
                        },
                        "at_nodes": {
                            "penalty": "@xboard_nodepen",
                            "perception_factor": 1,
                        },
                    }
                # add in the correct value of time parameter
                for level in new_journey_levels:
                    if level["boarding_cost"]:
                        level["boarding_cost"]["on_segments"][
                            "perception_factor"
                        ] = fare_perception

            elif self.name == "WLK_TRN_PNR":
                new_journey_levels = copy.deepcopy(journey_levels)

                for i in range(0, len(new_journey_levels)):
                    jls = new_journey_levels[i]
                    jls["destinations_reachable"] = False
                    jls["transition_rules"].extend(
                        [
                            {
                                "mode": "a",
                                "next_journey_level": len(new_journey_levels) + 2,
                            },
                            {
                                "mode": "D",
                                "next_journey_level": len(new_journey_levels) + 2,
                            },
                            {"mode": "w", "next_journey_level": i + 1},
                            {
                                "mode": "p",
                                "next_journey_level": len(new_journey_levels) + 1,
                            },
                        ]
                    )
                # level 0: walk access
                transition_rules_walk_access = copy.deepcopy(
                    journey_levels[0]["transition_rules"]
                )
                for level in transition_rules_walk_access:
                    level["next_journey_level"] = 1
                transition_rules_walk_access.extend(
                    [
                        {"mode": "a", "next_journey_level": 0},
                        {
                            "mode": "D",
                            "next_journey_level": len(new_journey_levels) + 2,
                        },
                        {
                            "mode": "w",
                            "next_journey_level": len(new_journey_levels) + 2,
                        },
                        {
                            "mode": "p",
                            "next_journey_level": len(new_journey_levels) + 2,
                        },
                    ]
                )
                # level len(new_journey_levels)+1: drive home
                transition_rules_drive_home = copy.deepcopy(
                    journey_levels[0]["transition_rules"]
                )
                for level in transition_rules_drive_home:
                    level["next_journey_level"] = len(new_journey_levels) + 2
                transition_rules_drive_home.extend(
                    [
                        {
                            "mode": "a",
                            "next_journey_level": len(new_journey_levels) + 2,
                        },
                        {
                            "mode": "D",
                            "next_journey_level": len(new_journey_levels) + 1,
                        },
                        {
                            "mode": "w",
                            "next_journey_level": len(new_journey_levels) + 2,
                        },
                        {
                            "mode": "p",
                            "next_journey_level": len(new_journey_levels) + 2,
                        },
                    ]
                )
                # level len(new_journey_levels)+2: every mode is prohibited
                transition_rules_prohibit = copy.deepcopy(
                    journey_levels[0]["transition_rules"]
                )
                for level in transition_rules_prohibit:
                    level["next_journey_level"] = len(new_journey_levels) + 2
                transition_rules_prohibit.extend(
                    [
                        {
                            "mode": "a",
                            "next_journey_level": len(new_journey_levels) + 2,
                        },
                        {
                            "mode": "D",
                            "next_journey_level": len(new_journey_levels) + 2,
                        },
                        {
                            "mode": "w",
                            "next_journey_level": len(new_journey_levels) + 2,
                        },
                        {
                            "mode": "p",
                            "next_journey_level": len(new_journey_levels) + 2,
                        },
                    ]
                )
                new_journey_levels.insert(
                    0,
                    {
                        "description": "walk access",
                        "destinations_reachable": True,
                        "transition_rules": transition_rules_walk_access,
                        "waiting_time": None,
                        "boarding_time": None,
                        "boarding_cost": None,
                    },
                )
                new_journey_levels.append(
                    {
                        "description": "drive home",
                        "destinations_reachable": True,
                        "transition_rules": transition_rules_drive_home,
                        "waiting_time": None,
                        "boarding_time": None,
                        "boarding_cost": None,
                    }
                )
                new_journey_levels.append(
                    {
                        "description": "prohibit",
                        "destinations_reachable": False,
                        "transition_rules": transition_rules_prohibit,
                        "waiting_time": None,
                        "boarding_time": None,
                        "boarding_cost": None,
                    }
                )
                for level in new_journey_levels[1:-2]:
                    level["waiting_time"] = {
                        "headway_fraction": "@hdw_fraction",
                        "effective_headways": effective_headway_source,
                        "spread_factor": 1,
                        "perception_factor": "@wait_pfactor",
                    }
                    level["boarding_time"] = {
                        "on_lines": {
                            "penalty": "@xboard_penalty",
                            "perception_factor": 1,
                        },
                        "at_nodes": {
                            "penalty": "@xboard_nodepen",
                            "perception_factor": 1,
                        },
                    }
                # add in the correct value of time parameter
                for level in new_journey_levels:
                    if level["boarding_cost"]:
                        level["boarding_cost"]["on_segments"][
                            "perception_factor"
                        ] = fare_perception

            elif self.name == "KNR_TRN_WLK":
                new_journey_levels = copy.deepcopy(journey_levels)

                for i in range(0, len(new_journey_levels)):
                    jls = new_journey_levels[i]
                    for level in jls["transition_rules"]:
                        level["next_journey_level"] = level["next_journey_level"] + 1
                    jls["transition_rules"].extend(
                        [
                            {"mode": "e", "next_journey_level": i + 2},
                            {
                                "mode": "D",
                                "next_journey_level": len(new_journey_levels) + 2,
                            },
                            {"mode": "w", "next_journey_level": i + 2},
                            ## {'mode': 'p', 'next_journey_level': len(new_journey_levels)+2},
                            {
                                "mode": "k",
                                "next_journey_level": len(new_journey_levels) + 2,
                            },
                        ]
                    )
                # level 0: drive access
                transition_rules_drive_access = copy.deepcopy(
                    journey_levels[0]["transition_rules"]
                )
                for level in transition_rules_drive_access:
                    level["next_journey_level"] = len(new_journey_levels) + 2
                transition_rules_drive_access.extend(
                    [
                        {
                            "mode": "e",
                            "next_journey_level": len(new_journey_levels) + 2,
                        },
                        {"mode": "D", "next_journey_level": 0},
                        {
                            "mode": "w",
                            "next_journey_level": len(new_journey_levels) + 2,
                        },
                        ## {'mode': 'p', 'next_journey_level': len(new_journey_levels)+2},
                        {"mode": "k", "next_journey_level": 1},
                    ]
                )
                # level 1: use transit
                transition_rules_knr = copy.deepcopy(
                    journey_levels[0]["transition_rules"]
                )
                for level in transition_rules_knr:
                    level["next_journey_level"] = 2
                transition_rules_knr.extend(
                    [
                        {
                            "mode": "e",
                            "next_journey_level": len(new_journey_levels) + 2,
                        },
                        {
                            "mode": "D",
                            "next_journey_level": len(new_journey_levels) + 2,
                        },
                        {
                            "mode": "w",
                            "next_journey_level": len(new_journey_levels) + 2,
                        },
                        ## {'mode': 'p', 'next_journey_level': len(new_journey_levels)+2},
                        {"mode": "k", "next_journey_level": 1},
                    ]
                )
                # level len(new_journey_levels)+2: every mode is prohibited
                transition_rules_prohibit = copy.deepcopy(
                    journey_levels[0]["transition_rules"]
                )
                for level in transition_rules_prohibit:
                    level["next_journey_level"] = len(new_journey_levels) + 2
                transition_rules_prohibit.extend(
                    [
                        {
                            "mode": "e",
                            "next_journey_level": len(new_journey_levels) + 2,
                        },
                        {
                            "mode": "D",
                            "next_journey_level": len(new_journey_levels) + 2,
                        },
                        {
                            "mode": "w",
                            "next_journey_level": len(new_journey_levels) + 2,
                        },
                        ## {'mode': 'p', 'next_journey_level': len(new_journey_levels)+2},
                        {
                            "mode": "k",
                            "next_journey_level": len(new_journey_levels) + 2,
                        },
                    ]
                )
                new_journey_levels.insert(
                    0,
                    {
                        "description": "drive access",
                        "destinations_reachable": False,
                        "transition_rules": transition_rules_drive_access,
                        "waiting_time": None,
                        "boarding_time": None,
                        "boarding_cost": None,
                    },
                )
                new_journey_levels.insert(
                    1,
                    {
                        "description": "knr",
                        "destinations_reachable": False,
                        "transition_rules": transition_rules_knr,
                        "waiting_time": None,
                        "boarding_time": None,
                        "boarding_cost": None,
                    },
                )
                new_journey_levels.append(
                    {
                        "description": "prohibit",
                        "destinations_reachable": False,
                        "transition_rules": transition_rules_prohibit,
                        "waiting_time": None,
                        "boarding_time": None,
                        "boarding_cost": None,
                    }
                )
                for level in new_journey_levels[2:-1]:
                    level["waiting_time"] = {
                        "headway_fraction": "@hdw_fraction",
                        "effective_headways": effective_headway_source,
                        "spread_factor": 1,
                        "perception_factor": "@wait_pfactor",
                    }
                    level["boarding_time"] = {
                        "on_lines": {
                            "penalty": "@xboard_penalty",
                            "perception_factor": 1,
                        },
                        "at_nodes": {
                            "penalty": "@xboard_nodepen",
                            "perception_factor": 1,
                        },
                    }
                # add in the correct value of time parameter
                for level in new_journey_levels:
                    if level["boarding_cost"]:
                        level["boarding_cost"]["on_segments"][
                            "perception_factor"
                        ] = fare_perception

            elif self.name == "WLK_TRN_KNR":
                new_journey_levels = copy.deepcopy(journey_levels)

                for i in range(0, len(new_journey_levels)):
                    jls = new_journey_levels[i]
                    jls["destinations_reachable"] = False
                    jls["transition_rules"].extend(
                        [
                            {
                                "mode": "a",
                                "next_journey_level": len(new_journey_levels) + 2,
                            },
                            {
                                "mode": "D",
                                "next_journey_level": len(new_journey_levels) + 2,
                            },
                            {"mode": "w", "next_journey_level": i + 1},
                            ## {'mode': 'p', 'next_journey_level': len(new_journey_levels)+2},
                            {
                                "mode": "k",
                                "next_journey_level": len(new_journey_levels) + 1,
                            },
                        ]
                    )
                # level 0: walk access
                transition_rules_walk_access = copy.deepcopy(
                    journey_levels[0]["transition_rules"]
                )
                for level in transition_rules_walk_access:
                    level["next_journey_level"] = 1
                transition_rules_walk_access.extend(
                    [
                        {"mode": "a", "next_journey_level": 0},
                        {
                            "mode": "D",
                            "next_journey_level": len(new_journey_levels) + 2,
                        },
                        {
                            "mode": "w",
                            "next_journey_level": len(new_journey_levels) + 2,
                        },
                        ## {'mode': 'p', 'next_journey_level': len(new_journey_levels)+2},
                        {
                            "mode": "k",
                            "next_journey_level": len(new_journey_levels) + 2,
                        },
                    ]
                )
                # level len(new_journey_levels)+1: drive home
                transition_rules_drive_home = copy.deepcopy(
                    journey_levels[0]["transition_rules"]
                )
                for level in transition_rules_drive_home:
                    level["next_journey_level"] = len(new_journey_levels) + 2
                transition_rules_drive_home.extend(
                    [
                        {
                            "mode": "a",
                            "next_journey_level": len(new_journey_levels) + 2,
                        },
                        {
                            "mode": "D",
                            "next_journey_level": len(new_journey_levels) + 1,
                        },
                        {
                            "mode": "w",
                            "next_journey_level": len(new_journey_levels) + 2,
                        },
                        ## {'mode': 'p', 'next_journey_level': len(new_journey_levels)+2},
                        {
                            "mode": "k",
                            "next_journey_level": len(new_journey_levels) + 2,
                        },
                    ]
                )
                # level len(new_journey_levels)+2: every mode is prohibited
                transition_rules_prohibit = copy.deepcopy(
                    journey_levels[0]["transition_rules"]
                )
                for level in transition_rules_prohibit:
                    level["next_journey_level"] = len(new_journey_levels) + 2
                transition_rules_prohibit.extend(
                    [
                        {
                            "mode": "a",
                            "next_journey_level": len(new_journey_levels) + 2,
                        },
                        {
                            "mode": "D",
                            "next_journey_level": len(new_journey_levels) + 2,
                        },
                        {
                            "mode": "w",
                            "next_journey_level": len(new_journey_levels) + 2,
                        },
                        ## {'mode': 'p', 'next_journey_level': len(new_journey_levels)+2},
                        {
                            "mode": "k",
                            "next_journey_level": len(new_journey_levels) + 2,
                        },
                    ]
                )
                new_journey_levels.insert(
                    0,
                    {
                        "description": "walk access",
                        "destinations_reachable": True,
                        "transition_rules": transition_rules_walk_access,
                        "waiting_time": None,
                        "boarding_time": None,
                        "boarding_cost": None,
                    },
                )
                new_journey_levels.append(
                    {
                        "description": "drive home",
                        "destinations_reachable": True,
                        "transition_rules": transition_rules_drive_home,
                        "waiting_time": None,
                        "boarding_time": None,
                        "boarding_cost": None,
                    }
                )
                new_journey_levels.append(
                    {
                        "description": "prohibit",
                        "destinations_reachable": False,
                        "transition_rules": transition_rules_prohibit,
                        "waiting_time": None,
                        "boarding_time": None,
                        "boarding_cost": None,
                    }
                )
                for level in new_journey_levels[1:-2]:
                    level["waiting_time"] = {
                        "headway_fraction": "@hdw_fraction",
                        "effective_headways": effective_headway_source,
                        "spread_factor": 1,
                        "perception_factor": "@wait_pfactor",
                    }
                    level["boarding_time"] = {
                        "on_lines": {
                            "penalty": "@xboard_penalty",
                            "perception_factor": 1,
                        },
                        "at_nodes": {
                            "penalty": "@xboard_nodepen",
                            "perception_factor": 1,
                        },
                    }
                # add in the correct value of time parameter
                for level in new_journey_levels:
                    if level["boarding_cost"]:
                        level["boarding_cost"]["on_segments"][
                            "perception_factor"
                        ] = fare_perception

            elif self.name == "WLK_TRN_WLK":
                new_journey_levels = copy.deepcopy(journey_levels)

                for i in range(0, len(new_journey_levels)):
                    jls = new_journey_levels[i]
                    jls["transition_rules"].extend(
                        [
                            {"mode": "e", "next_journey_level": i+1},
                            {"mode": "w", "next_journey_level": i+1},
                            {
                                "mode": "a",
                                "next_journey_level": i+1,
                            },
                        ]
                    )
                # level 0: only allow walk access and walk auxilary
                # must use the trasit modes to get onto the next level, 
                transition_rules_walk = copy.deepcopy(journey_levels[0]["transition_rules"])
                transition_rules_walk.extend(
                    [
                        {
                            "mode": "e",
                            "next_journey_level": 0,
                        },
                        {
                            "mode": "w",
                            "next_journey_level": 0,
                        },
                        {"mode": "a", "next_journey_level": 0},
                    ]
                )
                new_journey_levels.insert(
                    0,
                    {
                        "description": "base",
                        "destinations_reachable": False,
                        "transition_rules": transition_rules_walk,
                        "waiting_time": None,
                        "boarding_time": None,
                        "boarding_cost": None,
                    },
                )
                for level in new_journey_levels[1:]:
                    level["waiting_time"] = {
                        "headway_fraction": "@hdw_fraction",
                        "effective_headways": effective_headway_source,
                        "spread_factor": 1,
                        "perception_factor": "@wait_pfactor",
                    }
                    level["boarding_time"] = {
                        "on_lines": {
                            "penalty": "@xboard_penalty",
                            "perception_factor": 1,
                        },
                        "at_nodes": {
                            "penalty": "@xboard_nodepen",
                            "perception_factor": 1,
                        },
                    }
                # add in the correct value of time parameter
                for level in new_journey_levels:
                    if level["boarding_cost"]:
                        level["boarding_cost"]["on_segments"][
                            "perception_factor"
                        ] = fare_perception

            with open(
                os.path.join(
                    self._spec_dir,
                    "%s_%s_journey_levels.ems" % (self._time_period, self.name),
                ),
                "w",
            ) as jl_spec_file:
                spec = {
                    "type": "EXTENDED_TRANSIT_ASSIGNMENT",
                    "journey_levels": new_journey_levels,
                }
                _json.dump(spec, jl_spec_file, indent=4)

        else:
            new_journey_levels = [
                {
                    "description": "",
                    "destinations_reachable": True,
                    "transition_rules": [
                        {"mode": m, "next_journey_level": 1} for m in modes
                    ],
                },
                {
                    "description": "",
                    "destinations_reachable": True,
                    "transition_rules": [
                        {"mode": m, "next_journey_level": 1} for m in modes
                    ],
                    "waiting_time": {
                        "headway_fraction": "@hdw_fraction",
                        "effective_headways": effective_headway_source,
                        "spread_factor": 1,
                        "perception_factor": "@wait_pfactor",
                    },
                },
            ]
            for level in new_journey_levels[1:]:
                level["boarding_time"] = {
                    "on_lines": {"penalty": "@xboard_penalty", "perception_factor": 1},
                    "at_nodes": {"penalty": "@xboard_nodepen", "perception_factor": 1},
                }

        return new_journey_levels
