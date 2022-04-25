"""Transit assignment module"""

from __future__ import annotations
from typing import Union, List, Dict, Set, Tuple, TYPE_CHECKING
from collections import defaultdict as _defaultdict
import os

import json as _json

from tm2py.components.component import Component
from tm2py.components.demand.demand import PrepareTransitDemand
from tm2py.emme.manager import EmmeNetwork
from tm2py.logger import LogStartEnd
from tm2py import tools

if TYPE_CHECKING:
    from tm2py.controller import RunController
    from tm2py.config import TransitClassConfig, TransitConfig, TransitModeConfig


_SEGMENT_COST_FUNCTION = """
min_seat_weight = 1.0
max_seat_weight = 1.4
power_seat_weight = 2.2
min_stand_weight = 1.4
max_stand_weight = 1.6
power_stand_weight = 3.4

def calc_segment_cost(transit_volume, capacity, segment):
    if transit_volume == 0:
        return 0.0
    line = segment.line
    # need assignment period in seated_capacity calc
    seated_capacity = line.vehicle.seated_capacity * {0} * 60 / line.headway
    num_seated = min(transit_volume, seated_capacity)
    num_standing = max(transit_volume - seated_capacity, 0)

    vcr = transit_volume / capacity
    crowded_factor = (((
           (min_seat_weight+(max_seat_weight-min_seat_weight)*(transit_volume/capacity)**power_seat_weight)*num_seated
           +(min_stand_weight+(max_stand_weight-min_stand_weight)*(transit_volume/capacity)**power_stand_weight)*num_standing
           )/(transit_volume+0.01)))

    # Toronto implementation limited factor between 1.0 and 10.0, 
    # for use with Emme Capacitated assignment normalize by subtracting 1 
    return max(crowded_factor - 1, 0)
"""

_HEADWAY_COST_FUNCTION = """
max_hdwy_growth = 1.5
max_hdwy = 999.98


def calc_eawt(segment, vcr, headway):
    # EAWT_AM = 0. 259625 + 1. 612019*(1/Headway) + 0.005274*(Arriving V/C) + 0. 591765*(Total Offs Share)
    # EAWT_MD = 0. 24223 + 3.40621* (1/Headway) + 0.02709*(Arriving V/C) + 0. 82747 *(Total Offs Share)
    line = segment.line
    prev_segment = line.segment(segment.number - 1)
    alightings = 0
    total_offs = 0
    all_segs = iter(line.segments(True))
    prev_seg = next(all_segs)
    for seg in all_segs:
        total_offs += prev_seg.transit_volume - seg.transit_volume + seg.transit_boardings
        if seg == segment:
            alightings = total_offs
        prev_seg = seg
    if total_offs < 0.001:
        total_offs = 9999  # added due to divide by zero error
    if headway < .01:
        headway = 9999
    eawt = 0.259625 + 1.612019*(1/headway) + 0.005274*(vcr) + 0.591765*(alightings / total_offs)
    # if mode is LRT / BRT mult eawt * 0.4, if HRT /commuter mult by 0.2
    # use either .mode.id or ["#src_mode"] if fares are used
    mode_char = line{0}
    if mode_char in ["l", "x"]:
        eawt_factor = 0.4
    elif mode_char in ["h", "c", "f"]:
        eawt_factor = 0.2
    else:
        eawt_factor = 1
    return eawt * eawt_factor


def calc_adj_headway(transit_volume, transit_boardings, headway, capacity, segment):
    prev_hdwy = segment["@phdwy"]
    delta_cap = max(capacity - transit_volume + transit_boardings, 0)
    adj_hdwy = min(max_hdwy, prev_hdwy * min((transit_boardings+1) / (delta_cap+1), 1.5))
    adj_hdwy = max(headway, adj_hdwy)
    return adj_hdwy

def calc_headway(transit_volume, transit_boardings, headway, capacity, segment):
    vcr = transit_volume / capacity
    eawt = calc_eawt(segment, vcr, segment.line.headway)
    adj_hdwy = calc_adj_headway(transit_volume, transit_boardings, headway, capacity, segment)
    return adj_hdwy + eawt
"""

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

    def __init__(self, controller: RunController):
        super().__init__(controller)
        self.demand = PrepareTransitDemand(self.controller)
        self._num_processors = tools.parse_num_processors(
            self.config.emme.num_processors
        )
        self._time_period = None
        self._scenario = None

    @LogStartEnd("Transit assignments")
    def run(self):
        """Run transit assignments"""
        emmebank_path = self.get_abs_path(self.config.emme.transit_database_path)
        emmebank = self.controller.emme_manager.emmebank(emmebank_path)
        use_ccr = False
        if self.controller.iteration >= 1:
            use_ccr = self.config.transit.use_ccr
            self.demand.run()
        else:
            self.demand.create_zero_matrix()
        for self._time_period in self.time_period_names():
            msg = f"Transit assignment for period {self._time_period}"
            with self.logger.log_start_end(msg):
                self._scenario = self.get_emme_scenario(emmebank, self._time_period)
                if use_ccr:
                    self._run_ccr_assign()
                    self._calc_segment_ccr_penalties()
                else:
                    self._run_extended_assign()
                if self.config.transit.output_stop_usage_path is not None:
                    network, class_stop_attrs = self._calc_connector_flows()
                    self._export_connector_flows(network, class_stop_attrs)
                if self.config.transit.output_transit_boardings_path is not None:
                    self._export_boardings_by_line()

    @property
    def _transit_classes(self) -> List[AssignmentClass]:
        emme_manager = self.controller.emme_manager
        if self.config.transit.use_fares:
            fare_modes = _defaultdict(lambda: set([]))
            network = self._scenario.get_partial_network(
                ["TRANSIT_LINE"], include_attributes=False
            )
            emme_manager.copy_attribute_values(
                self._scenario, network, {"TRANSIT_LINE": ["#src_mode"]}
            )
            for line in network.transit_lines():
                fare_modes[line["#src_mode"]].add(line.mode.id)
        else:
            fare_modes = None
        spec_dir = os.path.join(
            os.path.dirname(self.config.emme.project_path), "Specifications"
        )
        transit_classes = []
        for class_config in self.config.transit.classes:
            transit_classes.append(
                AssignmentClass(
                    class_config,
                    self.config.transit,
                    self._time_period,
                    self.controller.iteration,
                    self._num_processors,
                    fare_modes,
                    spec_dir,
                )
            )
        return transit_classes

    @property
    def _duration(self) -> float:
        duration_lookup = dict(
            (p.name, p.length_hours) for p in self.config.time_periods
        )
        return duration_lookup[self._time_period]

    def _run_ccr_assign(self):
        transit_classes = self._transit_classes
        assign_transit = self.controller.emme_manager.tool(
            "inro.emme.transit_assignment.capacitated_transit_assignment"
        )
        specs = [klass.emme_transit_spec for klass in transit_classes]
        names = [klass.name for klass in transit_classes]
        if self.config.transit.use_fares:
            mode_attr = '["#src_mode"]'
        else:
            mode_attr = ".mode.id"
        func = {
            "segment": {
                "type": "CUSTOM",
                "python_function": _SEGMENT_COST_FUNCTION.format(self._duration),
                "congestion_attribute": "us3",
                "orig_func": False,
            },
            "headway": {
                "type": "CUSTOM",
                "python_function": _HEADWAY_COST_FUNCTION.format(mode_attr),
            },
            "assignment_period": self._duration,
        }
        stop = {
            "max_iterations": self.config.transit.max_ccr_iterations,
            "relative_difference": 0.01,
            "percent_segments_over_capacity": 0.01,
        }
        assign_transit(
            specs,
            congestion_function=func,
            stopping_criteria=stop,
            class_names=names,
            scenario=self._scenario,
            log_worksheets=False,
        )

    def _run_extended_assign(self):
        assign_transit = self.controller.emme_manager.tool(
            "inro.emme.transit_assignment.extended_transit_assignment"
        )
        add_volumes = False
        for klass in self._transit_classes:
            assign_transit(
                klass.emme_transit_spec,
                class_name=klass.name,
                add_volumes=add_volumes,
                scenario=self._scenario,
            )
            add_volumes = True

    def _export_boardings_by_line(self):
        scenario = self._scenario
        emme_manager = self.controller.emme_manager
        output_transit_boardings_file = self.get_abs_path(
            self.config.transit.output_transit_boardings_file
        )
        network = scenario.get_partial_network(
            ["TRANSIT_LINE", "TRANSIT_SEGMENT"], include_attributes=False
        )
        attributes = {
            "TRANSIT_LINE": ["description", "#src_mode", "@mode"],
            "TRANSIT_SEGMENT": ["transit_boardings"],
        }
        emme_manager.copy_attribute_values(self._scenario, network, attributes)
        os.makedirs(os.path.dirname(output_transit_boardings_file), exist_ok=True)
        with open(output_transit_boardings_file, "w", encoding="utf8") as out_file:
            out_file.write("line_name, description, total_boardings, mode, line_mode\n")
            for line in network.transit_lines():
                total_board = sum(seg.transit_boardings for seg in line.segments)
                out_file.write(
                    f"{line.id}, {line.description}, {total_board}, "
                    f"{line['#src_mode']}, {line['@mode']}\n"
                )

    def _calc_connector_flows(self) -> Tuple[EmmeNetwork, Dict[str, str]]:
        emme_manager = self.controller.emme_manager
        # calculate boardings and alightings by assignment class
        network_results = emme_manager.tool(
            "inro.emme.transit_assignment.extended.network_results"
        )
        create_extra = emme_manager.tool(
            "inro.emme.data.extra_attribute.create_extra_attribute"
        )
        class_stop_attrs = {}
        for klass in self.config.transit.classes:
            attr_name = f"@aux_volume_{klass.name}".lower()
            create_extra("LINK", attr_name, overwrite=True, scenario=self._scenario)
            spec = {
                "type": "EXTENDED_TRANSIT_NETWORK_RESULTS",
                "on_links": {"aux_transit_volumes": attr_name},
            }
            network_results(spec, class_name=klass.name, scenario=self._scenario)
            class_stop_attrs[klass.name] = attr_name

        # optimization: partial network to only load links and certain attributes
        network = self._scenario.get_partial_network(["LINK"], include_attributes=False)
        attributes = {
            "LINK": class_stop_attrs.values(),
            "NODE": ["@taz_id", "#node_id"],
        }
        emme_manager.copy_attribute_values(self._scenario, network, attributes)
        return network, class_stop_attrs

    def _export_connector_flows(
        self, network: EmmeNetwork, class_stop_attrs: Dict[str, str]
    ):
        # export boardings and alightings by assignment class, stop(connector) and TAZ
        path_tmplt = self.get_abs_path(self.config.transit.output_stop_usage_path)
        os.makedirs(os.path.dirname(path_tmplt), exist_ok=True)
        with open(
            path_tmplt.format(period=self._time_period), "w", encoding="utf8"
        ) as out_file:
            out_file.write(",".join(["mode", "taz", "stop", "boardings", "alightings"]))
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

    def _calc_segment_ccr_penalties(self):
        # calculate extra average wait time (@eawt) and @capacity_penalty
        # on the segments
        emme_manager = self.controller.emme_manager
        create_extra = emme_manager.tool(
            "inro.emme.data.extra_attribute.create_extra_attribute"
        )
        create_extra(
            "TRANSIT_SEGMENT",
            "@eawt",
            "extra added wait time",
            overwrite=True,
            scenario=self._scenario,
        )
        create_extra(
            "TRANSIT_SEGMENT",
            "@capacity_penalty",
            "capacity penalty at boarding",
            overwrite=True,
            scenario=self._scenario,
        )
        attributes = {
            "TRANSIT_SEGMENT": [
                "@phdwy",
                "transit_volume",
                "transit_boardings",
            ],
            "TRANSIT_VEHICLE": ["seated_capacity", "total_capacity"],
            "TRANSIT_LINE": ["headway"],
        }
        if self.config.transit.use_fares:
            # only if use_fares, otherwise will use .mode.id
            attributes["TRANSIT_LINE"].append("#src_mode")
            mode_name = '["#src_mode"]'
        else:
            mode_name = ".mode.id"
        # load network object from scenario (on disk) and copy some attributes
        network = self._scenario.get_partial_network(
            ["TRANSIT_SEGMENT"], include_attributes=False
        )
        network.create_attribute("TRANSIT_LINE", "capacity")
        emme_manager.copy_attribute_values(self._scenario, network, attributes)
        enclosing_scope = {"network": network, "scenario": self._scenario}
        code = compile(
            _HEADWAY_COST_FUNCTION.format(mode_name),
            "headway_cost_function",
            "exec",
        )
        # Yes pylint, I know exec is being used here
        # pylint: disable=W0122
        exec(code, enclosing_scope)
        calc_eawt = enclosing_scope["calc_eawt"]
        hdwy_fraction = 0.5  # fixed in assignment spec
        duration = self._duration
        for line in network.transit_lines():
            line.capacity = 60.0 * duration * line.vehicle.total_capacity / line.headway
        for segment in network.transit_segments():
            vcr = segment.transit_volume / segment.line.capacity
            segment["@eawt"] = calc_eawt(segment, vcr, segment.line.headway)
            segment["@capacity_penalty"] = (
                max(segment["@phdwy"] - segment["@eawt"] - segment.line.headway, 0)
                * hdwy_fraction
            )
        # copy (save) results back from the network to the scenario (on disk)
        attributes = {"TRANSIT_SEGMENT": ["@eawt", "@capacity_penalty"]}
        emme_manager.copy_attribute_values(network, self._scenario, attributes)


class AssignmentClass:
    """Transit assignment class, represents data from config and conversion to Emme specs

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
        class_config: TransitClassConfig,
        transit_config: TransitConfig,
        time_period: str,
        iteration: int,
        num_processors: int,
        fare_modes: Dict[str, Set[str]],
        spec_dir: str,
    ):
        """

        Args:
            class_config: the transit class config (TransitClassConfig)
            transit_config: the root transit assignment config (TransitConfig)
            time_period: the time period name
            iteration: the current iteration
            num_processors: the number of processors to use, loaded from config
            fare_modes: the mapping from the generated fare mode ID to the original
                source mode ID
            spec_dir: directory to find the generated journey levels tables from
                the apply fares step
        """
        self._name = class_config.name
        self._class_config = class_config
        self._transit_config = transit_config
        self._time_period = time_period
        self._iteration = iteration
        self._num_processors = num_processors
        self._fare_modes = fare_modes
        self._spec_dir = spec_dir

    @property
    def name(self) -> str:
        """The class name"""
        return self._name

    @property
    def emme_transit_spec(self) -> EmmeTransitSpec:
        """Return Emme Extended transit assignment specification

        Converted from input config (transit.classes, with some parameters from
        transit table), see also Emme Help for
        Extended transit assignment for specification details.

        """
        spec = {
            "type": "EXTENDED_TRANSIT_ASSIGNMENT",
            "modes": self._modes,
            "demand": self._demand_matrix,
            "waiting_time": {
                "effective_headways": self._transit_config.effective_headway_source,
                "headway_fraction": 0.5,
                "perception_factor": self._transit_config.initial_wait_perception_factor,
                "spread_factor": 1.0,
            },
            "boarding_cost": {"global": {"penalty": 0, "perception_factor": 1}},
            "boarding_time": {
                "global": {
                    "penalty": self._transit_config.initial_boarding_penalty,
                    "perception_factor": 1,
                }
            },
            "in_vehicle_cost": None,
            "in_vehicle_time": {"perception_factor": "@invehicle_factor"},
            "aux_transit_time": {
                "perception_factor": self._transit_config.walk_perception_factor
            },
            "aux_transit_cost": None,
            "journey_levels": self._journey_levels,
            "flow_distribution_between_lines": {"consider_total_impedance": False},
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
        if self._transit_config.use_fares:
            fare_perception = 60 / self._transit_config.value_of_time
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
        if self._transit_config.get("override_connector_times", False):
            spec["aux_transit_cost"] = {
                "penalty": f"@walk_time_{self.name.lower()}",
                "perception_factor": self._transit_config.walk_perception_factor,
            }
        return spec

    @property
    def _demand_matrix(self) -> str:
        if self._iteration < 1:
            return 'ms"zero"'  # zero demand matrix
        return f'mf"TRN_{self._class_config.skim_set_id}_{self._time_period}"'

    def _get_used_mode_ids(self, modes: List[TransitModeConfig]) -> List[str]:
        """Get list of assignment Mode IDs from input list of Emme mode objects.

        Accounts for fare table (mapping from input mode ID to auto-generated
        set of mode IDs for fare transition table (fares.far input) by applyfares
        component.
        """
        if self._transit_config.use_fares:
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
        """List of modes IDs (str) to use in assignment for this class"""
        all_modes = self._transit_config.modes
        mode_types = self._class_config.mode_types
        modes = [mode for mode in all_modes if mode.type in mode_types]
        return self._get_used_mode_ids(modes)

    @property
    def _transit_modes(self) -> List[str]:
        """List of transit modes IDs (str) to use in assignment for this class"""
        all_modes = self._transit_config.modes
        mode_types = self._class_config.mode_types
        modes = [
            mode
            for mode in all_modes
            if mode.type in mode_types and mode.assign_type == "TRANSIT"
        ]
        return self._get_used_mode_ids(modes)

    @property
    def _journey_levels(self) -> EmmeTransitJourneyLevelSpec:
        modes = self._transit_modes
        effective_headway_source = self._transit_config.effective_headway_source
        xfer_perception_factor = self._transit_config.transfer_wait_perception_factor
        xfer_boarding_penalty = self._transit_config.transfer_boarding_penalty
        if self._transit_config.use_fares:
            fare_perception = 60 / self._transit_config.value_of_time
            file_name = f"{self._time_period}_{self.name}_journey_levels.ems"
            with open(
                os.path.join(self._spec_dir, file_name), "r", encoding="ut8"
            ) as jl_spec:
                journey_levels = _json.load(jl_spec)["journey_levels"]
            # add transfer wait perception penalty
            for level in journey_levels[1:]:
                level["waiting_time"] = {
                    "headway_fraction": 0.5,
                    "effective_headways": effective_headway_source,
                    "spread_factor": 1,
                    "perception_factor": xfer_perception_factor,
                }
            # add in the correct value of time parameter
            for level in journey_levels:
                if level["boarding_cost"]:
                    level["boarding_cost"]["on_segments"][
                        "perception_factor"
                    ] = fare_perception
        else:
            journey_levels = [
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
                        "headway_fraction": 0.5,
                        "effective_headways": effective_headway_source,
                        "spread_factor": 1,
                        "perception_factor": xfer_perception_factor,
                    },
                },
            ]
        if xfer_boarding_penalty is not None:
            for level in journey_levels[1:]:
                level["boarding_time"] = {
                    "global": {"penalty": xfer_boarding_penalty, "perception_factor": 1}
                }
        return journey_levels
