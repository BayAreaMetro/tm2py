"""Performs transit assignment and generates transit skims.

"""

from contextlib import contextmanager as _context
import numpy as np
from os.path import join as _join, dirname as _dir


from tm2py.core.component import Component as _Component, Controller as _Controller
import tm2py.core.emme as _emme_tools

# TODO: imports from skim_transit_network.py, to be reviewed
import inro.modeller as _m
import inro.emme.database.emmebank as _eb
import inro.emme.core.exception as _except
import inro.emme.desktop.worksheet as _worksheet
import traceback as _traceback
from copy import deepcopy as _copy
from collections import defaultdict as _defaultdict, OrderedDict

import numpy
import os as _os
import json as _json
from copy import deepcopy as _copy

import openmatrix as _omx


# TODO: should express these in the config
# TODO: or make global lists tuples
_all_access_modes = ["WLK", "PNR", "KNRTNC", "KNRPRV"]
_all_sets = ["set1", "set2", "set3"]
_set_dict = {"BUS": "set1", "PREM": "set2", "ALLPEN": "set3"}

transit_modes = ["b", "x", "f", "l", "h", "r"]
aux_transit_modes = ["w", "a", "e"]
_walk_modes = ["a", "w", "e"]
_local_modes = ["b"]
_premium_modes = ["x", "f", "l", "h", "r"]
_skim_names = [
    "FIRSTWAIT",
    "TOTALWAIT",
    "XFERS",
    "TOTALWALK",
    "LBIVTT",
    "EBIVTT",
    "LRIVTT",
    "HRIVTT",
    "CRIVTT",
    "FRIVTT",
    "XFERWAIT",
    "FARE",
    "XFERWALK",
    "TOTALIVTT",
    "LINKREL",
    "CROWD",
    "EAWT",
    "CAPPEN",
]

_segment_cost_function = """
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
    # need assignment period in seated_capacity calc?
    seated_capacity = line.vehicle.seated_capacity * {0} * 60 / line.headway
    num_seated = min(transit_volume, seated_capacity)
    num_standing = max(transit_volume - seated_capacity, 0)

    vcr = transit_volume / capacity
    crowded_factor = (((
           (min_seat_weight+(max_seat_weight-min_seat_weight)*(transit_volume/capacity)**power_seat_weight)*num_seated
           +(min_stand_weight+(max_stand_weight-min_stand_weight)*(transit_volume/capacity)**power_stand_weight)*num_standing
           )/(transit_volume+0.01)))

    # Toronto implementation limited factor between 1.0 and 10.0
    return crowded_factor
"""

_headway_cost_function = """
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


class TransitAssignment(_Component):
    """Run transit assignment and skims."""

    def __init__(self, controller: _Controller):
        """Run transit assignment and skims.

        Args:
            controller: parent Controller object
        """
        super().__init__(controller)
        self._emme_manager = None
        self._num_processors = _emme_tools.parse_num_processors(
            self.config.emme.num_processors
        )

    # @property
    # def _modeller(self):
    #     return self._emme_manager.modeller

    def run(self):
        """Run transit assignment and skims."""
        project_path = _join(self._root_dir, self.config.emme.project_path)
        self._emme_manager = _emme_tools.EmmeManager()
        # Initialize Emme desktop if not already started
        emme_app = self._emme_manager.project(project_path)
        with self._setup():
            # NOTE: fixed path to database for now
            emmebank_path = _join(_dir(self.config.emme.project_path), "Database_transit")
            self._emmebank = self._emme_manager.emmebank(emmebank_path)
            ref_scenario = self._emmebank.scenario(self.config.periods[0].emme_scenario_id)
            period_names = [time.name for time in self.config.periods]
            self.initialize_skim_matrices(period_names, scenario)
            # Run assignment and skims for all specified periods
            for period in self.config.periods:
                scenario = emmebank.scenario(period.emme_scenario_id)
                #ctramp_output_folder = _os.path.join(self._root_dir, "ctramp_output")
                if self.controller.iteration >= 1:
                    self.import_demand_matrices(period.name, scenario)
                else:
                    self.create_empty_demand_matrices(period.name, scenario)

                use_ccr = False
                use_fares = False
                with self._setup(scenario):
                    self.assign_and_skim(
                        scenario,
                        period=period,
                        assignment_only=True,
                        use_fares=use_fares,
                        use_ccr=use_ccr,
                    )
                    output_omx_file = _os.path.join(
                        skims_path, "transit_skims_{}.omx".format(period)
                    )
                    _skim_transit.export_matrices_to_omx(
                        omx_file=output_omx_file,
                        periods=[period],
                        scenario=scenario,
                        big_to_zero=True,
                        max_transfers=3,
                    )

                    # if use_ccr and save_iter_flows:
                    #     self.save_per_iteration_flows(scenario)

                    # if output_transit_boardings:
                    #     desktop.data_explorer().replace_primary_scenario(scenario)
                    #     output_transit_boardings_file = _os.path.join(
                    #          _os.getcwd(), args.trn_path, "boardings_by_line_{}.csv".format(period))
                    #     export_boardings_by_line(emme_app, output_transit_boardings_file)

    @_context
    def _setup(self, scenario):
        with self._emme_manager.logbook_trace("Transit assignments"):
            self._matrix_cache = _emme_tools.MatrixCache(scenario)
            self._skim_matrices = []
            with self._emme_manager.logbook_trace("Traffic assignments"):
                try:
                    yield
                finally:
                    self._matrix_cache.clear()
                    self._matrix_cache = None

    def initialize_skim_matrices(self, time_periods, scenario):
        tmplt_matrices = [
            # ("GENCOST",    "total impedance"),
            ("FIRSTWAIT", "first wait time"),
            ("XFERWAIT", "transfer wait time"),
            ("TOTALWAIT", "total wait time"),
            ("FARE", "fare"),
            ("XFERS", "num transfers"),
            ("XFERWALK", "transfer walk time"),
            ("TOTALWALK", "total walk time"),
            ("TOTALIVTT", "total in-vehicle time"),
            ("LBIVTT", "local bus in-vehicle time"),
            ("EBIVTT", "express bus in-vehicle time"),
            ("LRIVTT", "light rail in-vehicle time"),
            ("HRIVTT", "heavy rail in-vehicle time"),
            ("CRIVTT", "commuter rail in-vehicle time"),
            ("FRIVTT", "ferry in-vehicle time"),
            ("IN_VEHICLE_COST", "In vehicle cost"),
            ("LINKREL", "Link reliability"),
            ("CROWD", "Crowding penalty"),
            ("EAWT", "Extra added wait time"),
            ("CAPPEN", "Capacity penalty"),
        ]
        skim_sets = [
            ("BUS", "Local bus only"),
            ("PREM", "Premium modes only"),
            ("ALLPEN", "All w/ xfer pen"),
        ]
        matrices = ["ms1", "zero", "zero"]
        emmebank = scenario.emmebank
        for time in time_periods:
            for set_name, set_desc in skim_sets:
                for name, desc in tmplt_matrices:
                    matrix_name = f"{period}_{set_name}_ {name}"
                    # Add matrix to list to be created if it does not exist
                    if not emmebank.matrix(matrix_name):
                        matrices.append("mf", matrix_name, f"{period} {set_desc}: {desc}")
        # check on database dimensions
        dim_full_matrices = emmebank.dimensions["full_matrices"]
        used_matrices = len([m for m in emmebank.matrices() if m.type == "FULL"])
        if len(matrices) > dim_full_matrices - used_matrices:
            raise Exception(
                "emmebank full_matrix capacity insuffcient, increase to at least %s"
                % (len(matrices) + used_matrices)
            )
        create_matrix = self._emme_manager.modeller.tool("inro.emme.data.matrix.create_matrix")
        for mtype, name, desc in matrices:
            create_matrix(mtype, name, desc, scenario=scenario, overwrite=True)

    def import_demand_matrices(self, period_name, scenario):
        # TODO: this needs some work
        #      - would like to save multiple matrices per OMX file (requires CT-RAMP changes)
        #      - would like to cross-reference the transit class structure 
        #        and the demand grouping in the config (identical to highway)
        #      - period should be the only placeholder key
        #      - should use separate methods to load and sum the demand
        #      - matrix names

        num_zones = len(scenario.zone_numbers)
        emmebank = scenario.emmebank
        msa_iteration = self.controller.iteration
        #omx_filename_template = "transit_{period}_{access_mode}_TRN_{set}_{period}.omx"
        omx_filename_template = _os.path.join(self._root_dir, self.config.household.transit_demand_file)
        matrix_name_template = "{access_mode}_TRN_{set}_{period}"
        emme_matrix_name_template = "TRN_{set}_{period}"
        #with _m.logbook_trace("Importing demand matrices for period %s" % period):
        for set_num in _all_sets:
            demand = None
            for access_mode in _all_access_modes:
                omx_filename_path = omx_filename_template.format(
                    period=period_name, access_mode=access_mode, set=set_num
                )
                matrix_name = matrix_name_template.format(
                    period=period_name, access_mode=access_mode, set=set_num
                )
                with _emme_tools.OMX(omx_filename_path) as file_obj:
                    access_demand = file_obj[matrix_name].read()
                if demand is None:
                    demand = access_demand
                else:
                    demand += access_demand

            shape = total_demand.shape
            # pad external zone values with 0
            if shape != (num_zones, num_zones):
                demand = np.pad(
                    demand, ((0, num_zones - shape[0]), (0, num_zones - shape[1]))
                )
            demand_name = emme_matrix_name_template.format(period=period_name, set=set_num)
            matrix = emmebank.matrix(demand_name)
            apply_msa_demand = self.config.transit.get("apply_msa_demand")
            if msa_iteration <= 1:
                if not matrix:
                    ident = emmebank.available_matrix_identifier("FULL")
                    matrix = emmebank.create_matrix(ident)
                    matrix.name = demand_name
                # matrix.description = ?
            elif apply_msa_demand:
                # Load prev demand and MSA average
                prev_demand = matrix.get_numpy_data(scenario.id)
                demand = prev_demand + (1.0 / msa_iteration) * (
                    demand - prev_demand
                )
            matrix.set_numpy_data(demand, scenario.id)
            
    def create_empty_demand_matrices(self, period_name, scenario):
        emme_matrix_name_template = "TRN_{set}_{period}"
        # with _m.logbook_trace("Create empty demand matrices for period %s" % period):
        for set_num in _all_sets:
            demand_name = emme_matrix_name_template.format(period=period_name, set=set_num)
            matrix = emmebank.matrix(demand_name)
            if not matrix:
                ident = emmebank.available_matrix_identifier("FULL")
                matrix = emmebank.create_matrix(ident)
                matrix.name = demand_name
            else:
                matrix.initialize(0)
            matrix.description = f"{period_name} {set_num} (all access modes)"[:80]

    def assign_and_skim(self,
        scenario,
        period,
        assignment_only=False,
        use_fares=False,
        use_ccr=False,
    ):
        emmebank = scenario.emmebank
        # TODO: double check value of time from $/min to $/hour is OK
        network = scenario.get_network()
        # network = scenario.get_partial_network(
        #     element_types=["TRANSIT_LINE", "TRANSIT_SEGMENT"], include_attributes=True)

        with self._emme_manager.logbook_trace("Transit assignment and skims for period %s" % period.name):
            self.run_assignment(
                scenario,
                period,
                network,
                use_fares,
                use_ccr,
            )

            if not assignment_only:
                with self._emme_manager.logbook_trace("Skims for Local-only (set1)"):
                    self.run_skims(
                        scenario,
                        "BUS",
                        period,
                        _local_modes,
                        network,
                        use_fares,
                        use_ccr,
                    )
                with self._emme_manager.logbook_trace("Skims for Premium-only (set2)"):
                    self.run_skims(
                        scenario,
                        "PREM",
                        period,
                        _premium_modes,
                        network,
                        use_fares,
                        use_ccr,
                    )
                with _m.logbook_trace("Skims for Local+Premium (set3)"):
                    self.run_skims(
                        scenario,
                        "ALLPEN",
                        period,
                        _local_modes + _premium_modes,
                        network,
                        use_fares,
                        use_ccr,
                    )
                    self.mask_allpen(period)
                self.mask_transfers(scenario, period)
                # report(scenario, period)

    def run_assignment(
        scenario,
        period,
        network,
        num_processors,
        use_fares=False,
        use_ccr=False,
    ):

        # REVIEW: separate method into smaller steps
        #     - specify class structure in config
        #     - 
        params = self.config.transit
        base_spec = {
            "type": "EXTENDED_TRANSIT_ASSIGNMENT",
            "modes": [],
            "demand": "",  # demand matrix specified below
            "waiting_time": {
                "effective_headways": params["effective_headway_source"],
                "headway_fraction": 0.5,
                "perception_factor": params["initial_wait_perception_factor"],
                "spread_factor": 1.0,
            },
            "boarding_cost": {"global": {"penalty": 0, "perception_factor": 1}},
            "boarding_time": {"global": {"penalty": 10, "perception_factor": 1}},
            "in_vehicle_cost": None,
            "in_vehicle_time": {"perception_factor": params["in_vehicle_perception_factor"]},
            "aux_transit_time": {"perception_factor": params["walk_perception_factor"]},
            "aux_transit_cost": None,
            "journey_levels": [],
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
        if use_fares:
            # fare attributes
            fare_perception = 60 / params["value_of_time"]
            base_spec["boarding_cost"] = {
                "on_segments": {
                    "penalty": "@board_cost",
                    "perception_factor": fare_perception,
                }
            }
            base_spec["in_vehicle_cost"] = {
                "penalty": "@invehicle_cost",
                "perception_factor": fare_perception,
            }

            fare_modes = _defaultdict(lambda: set([]))
            for line in network.transit_lines():
                fare_modes[line["#src_mode"]].add(line.mode.id)

            def get_fare_modes(src_modes):
                out_modes = set([])
                for mode in src_modes:
                    out_modes.update(fare_modes[mode])
                return list(out_modes)

            local_modes = get_fare_modes(_local_modes)
            premium_modes = get_fare_modes(_premium_modes)
            project_dir = _os.path.dirname(_os.path.dirname(scenario.emmebank.path))
            with open(
                _os.path.join(
                    project_dir, "Specifications", "%s_BUS_journey_levels.ems" % period.name
                ),
                "r",
            ) as f:
                local_journey_levels = _json.load(f)["journey_levels"]
            with open(
                _os.path.join(
                    project_dir, "Specifications", "%s_PREM_journey_levels.ems" % period.name
                ),
                "r",
            ) as f:
                premium_modes_journey_levels = _json.load(f)["journey_levels"]
            with open(
                _os.path.join(
                    project_dir, "Specifications", "%s_ALLPEN_journey_levels.ems" % period.name
                ),
                "r",
            ) as f:
                journey_levels = _json.load(f)["journey_levels"]
            # add transfer wait perception penalty
            for jls in local_journey_levels, premium_modes_journey_levels, journey_levels:
                for level in jls[1:]:
                    level["waiting_time"] = {
                        "headway_fraction": 0.5,
                        "effective_headways": effective_headway_source,
                        "spread_factor": 1,
                        "perception_factor": xfer_perception_factor
                    }
                # add in the correct value of time parameter
                for level in jls:
                    if level["boarding_cost"]:
                        level["boarding_cost"]["on_segments"][
                            "perception_factor"
                        ] = fare_perception

            mode_attr = '["#src_mode"]'
        else:
            local_modes = list(_local_modes)
            premium_modes = list(_premium_modes)
            local_journey_levels = get_jl_xfer_penalty(
                local_modes, 
                params["effective_headway_source"],
                params["transfer_wait_perception_factor"]
            )
            premium_modes_journey_levels = get_jl_xfer_penalty(
                premium_modes, 
                params["effective_headway_source"],
                params["transfer_wait_perception_factor"]
            )
            journey_levels = get_jl_xfer_penalty(
                local_modes + premium_modes, 
                params["effective_headway_source"],
                params["transfer_wait_perception_factor"]
            )
            mode_attr = ".mode.id"

        skim_parameters = OrderedDict(
            [
                (
                    "BUS",
                    {
                        "modes": _walk_modes + local_modes,
                        "journey_levels": local_journey_levels,
                    },
                ),
                (
                    "PREM",
                    {
                        "modes": _walk_modes + premium_modes,
                        "journey_levels": premium_modes_journey_levels,
                    },
                ),
                (
                    "ALLPEN",
                    {
                        "modes": _walk_modes + local_modes + premium_modes,
                        "journey_levels": journey_levels,
                    },
                ),
            ]
        )

        if use_ccr:
            assign_transit = modeller.tool(
                "inro.emme.transit_assignment.capacitated_transit_assignment"
            )
            #  assign all 3 classes of demand at the same time
            specs = []
            names = []
            demand_matrix_template = "mfTRN_{set}_{period}"
            for mode_name, parameters in skim_parameters.items():
                spec = _copy(base_spec)
                spec["modes"] = parameters["modes"]
                demand_matrix = demand_matrix_template.format(
                    set=_set_dict[mode_name], period=period.name
                )
                # TODO: need to raise on zero demand matrix?
                # if emmebank.matrix(demand_matrix).get_numpy_data(scenario.id).sum() == 0:
                #     continue  # don't include if no demand
                spec["demand"] = demand_matrix
                spec["journey_levels"] = parameters["journey_levels"]
                specs.append(spec)
                names.append(mode_name)
            func = {
                "segment": {
                    "type": "CUSTOM",
                    "python_function": _segment_cost_function.format(
                        period.duration
                    ),
                    "congestion_attribute": "us3",
                    "orig_func": False,
                },
                "headway": {
                    "type": "CUSTOM",
                    "python_function": _headway_cost_function.format(mode_attr),
                },
                "assignment_period": period.duration,
            }
            stop = {
                "max_iterations": 3,  # changed from 10 for testing
                "relative_difference": 0.01,
                "percent_segments_over_capacity": 0.01,
            }
            assign_transit(
                specs,
                congestion_function=func,
                stopping_criteria=stop,
                class_names=names,
                scenario=scenario,
                log_worksheets=False,
            )
        else:
            assign_transit = modeller.tool(
                "inro.emme.transit_assignment.extended_transit_assignment"
            )
            add_volumes = False
            for mode_name, parameters in skim_parameters.items():
                spec = _copy(base_spec)
                spec["modes"] = parameters["modes"]
                # spec["demand"] = 'ms1' # zero demand matrix
                spec["demand"] = "mfTRN_{set}_{period}".format(
                    set=_set_dict[mode_name], period=period.name
                )
                spec["journey_levels"] = parameters["journey_levels"]
                assign_transit(
                    spec, class_name=mode_name, add_volumes=add_volumes, scenario=scenario
                )
                add_volumes = True

    def run_skims(self,
        scenario,
        name,
        period,
        valid_modes,
        network,
        use_fares=False,
        use_ccr=False,
    ):
        # REVIEW: separate method into smaller steps
        #     - specify class structure in config
        #     - specify skims by name
        params = self.config.transit
        modeller = self._emme_manager.modeller
        emmebank = scenario.emmebank
        num_processors = self._num_processors
        matrix_calc = modeller.tool("inro.emme.matrix_calculation.matrix_calculator")
        network_calc = modeller.tool("inro.emme.network_calculation.network_calculator")
        create_extra = modeller.tool(
            "inro.emme.data.extra_attribute.create_extra_attribute"
        )
        matrix_results = modeller.tool(
            "inro.emme.transit_assignment.extended.matrix_results"
        )
        path_analysis = modeller.tool(
            "inro.emme.transit_assignment.extended.path_based_analysis"
        )
        strategy_analysis = modeller.tool(
            "inro.emme.transit_assignment.extended.strategy_based_analysis"
        )

        class_name = name
        skim_name = "%s_%s" % (period, name)
        with self._emme_manager.logbook_trace(
            "First and total wait time, number of boardings, fares, total walk time"
        ):
            # First and total wait time, number of boardings, fares, total walk time, in-vehicle time
            spec = {
                "type": "EXTENDED_TRANSIT_MATRIX_RESULTS",
                "actual_first_waiting_times": 'mf"%s_FIRSTWAIT"' % skim_name,
                "actual_total_waiting_times": 'mf"%s_TOTALWAIT"' % skim_name,
                "by_mode_subset": {
                    "modes": [
                        m.id
                        for m in network.modes()
                        if m.type in ["TRANSIT", "AUX_TRANSIT"]
                    ],
                    "avg_boardings": 'mf"%s_XFERS"' % skim_name,
                    # "actual_in_vehicle_times": 'mf"%s_TOTALIVTT"' % skim_name,
                    "actual_aux_transit_times": 'mf"%s_TOTALWALK"' % skim_name,
                },
            }
            if use_fares:
                spec["by_mode_subset"]["actual_in_vehicle_costs"] = (
                    'mf"%s_IN_VEHICLE_COST"' % skim_name
                )
                spec["by_mode_subset"]["actual_total_boarding_costs"] = (
                    'mf"%s_FARE"' % skim_name
                )
            matrix_results(
                spec,
                class_name=class_name,
                scenario=scenario,
                num_processors=num_processors,
            )

        with self._emme_manager.logbook_trace("In-vehicle time by mode"):
            mode_combinations = [
                ("LB", "b"),
                ("EB", "x"),
                ("FR", "f"),
                ("LR", "l"),
                ("HR", "h"),
                ("CR", "r"),
            ]
            # map to used modes in apply fares case
            fare_modes = _defaultdict(lambda: set([]))
            if use_fares:
                for line in network.transit_lines():
                    fare_modes[line["#src_mode"]].add(line.mode.id)
            else:
                fare_modes = dict((m, [m]) for m in valid_modes)
            # set to fare_modes and filter out unused modes
            mode_combinations = [
                (n, list(fare_modes[m])) for n, m in mode_combinations if m in valid_modes
            ]

            total_ivtt_expr = []
            if use_ccr:
                scenario.create_extra_attribute("TRANSIT_SEGMENT", "@mode_timtr")
                try:
                    for mode_name, modes in mode_combinations:
                        network.create_attribute("TRANSIT_SEGMENT", "@mode_timtr")
                        for line in network.transit_lines():
                            if line.mode.id in modes:
                                for segment in line.segments():
                                    # segment["@mode_timtr"] = segment["@base_timtr"]
                                    # segment["@mode_timtr"] = segment["@trantime_final"]
                                    segment["@mode_timtr"] = segment["@timtr"]
                        mode_timtr = network.get_attribute_values(
                            "TRANSIT_SEGMENT", ["@mode_timtr"]
                        )
                        network.delete_attribute("TRANSIT_SEGMENT", "@mode_timtr")
                        scenario.set_attribute_values(
                            "TRANSIT_SEGMENT", ["@mode_timtr"], mode_timtr
                        )
                        ivtt = 'mf"%s_%sIVTT"' % (skim_name, mode_name)
                        total_ivtt_expr.append(ivtt)
                        spec = get_strat_spec({"in_vehicle": "@mode_timtr"}, ivtt)
                        strategy_analysis(
                            spec,
                            class_name=class_name,
                            scenario=scenario,
                            num_processors=num_processors,
                        )
                finally:
                    scenario.delete_extra_attribute("@mode_timtr")
            else:
                for mode_name, modes in mode_combinations:
                    ivtt = 'mf"%s_%sIVTT"' % (skim_name, mode_name)
                    total_ivtt_expr.append(ivtt)
                    spec = {
                        "type": "EXTENDED_TRANSIT_MATRIX_RESULTS",
                        "by_mode_subset": {"modes": modes, "actual_in_vehicle_times": ivtt},
                    }
                    matrix_results(
                        spec,
                        class_name=class_name,
                        scenario=scenario,
                        num_processors=num_processors,
                    )

        with self._emme_manager.logbook_trace(
            "Calculate total IVTT, number of transfers, transfer walk and wait times"
        ):
            spec_list = [
                {  # sum total ivtt across all modes
                    "type": "MATRIX_CALCULATION",
                    "constraint": None,
                    "result": 'mf"%s_TOTALIVTT"' % skim_name,
                    "expression": "+".join(total_ivtt_expr),
                },
                {  # convert number of boardings to number of transfers
                    "type": "MATRIX_CALCULATION",
                    "constraint": {
                        "by_value": {
                            "od_values": 'mf"%s_XFERS"' % skim_name,
                            "interval_min": 0,
                            "interval_max": 9999999,
                            "condition": "INCLUDE",
                        }
                    },
                    "result": 'mf"%s_XFERS"' % skim_name,
                    "expression": "(%s_XFERS - 1).max.0" % skim_name,
                },
                {  # transfer walk time = total - access - egress
                    "type": "MATRIX_CALCULATION",
                    "constraint": None,
                    "result": 'mf"%s_XFERWALK"' % skim_name,
                    "expression": "({name}_TOTALWALK - 0.66).max.0".format(name=skim_name),
                },
                {
                    "type": "MATRIX_CALCULATION",
                    "constraint": {
                        "by_value": {
                            "od_values": 'mf"%s_TOTALWAIT"' % skim_name,
                            "interval_min": 0,
                            "interval_max": 9999999,
                            "condition": "INCLUDE",
                        }
                    },
                    "result": 'mf"%s_XFERWAIT"' % skim_name,
                    "expression": "({name}_TOTALWAIT - {name}_FIRSTWAIT).max.0".format(
                        name=skim_name
                    ),
                },
            ]
            if use_fares:
                spec_list.append(
                    {  # sum in-vehicle cost and boarding cost to get the fare paid
                        "type": "MATRIX_CALCULATION",
                        "constraint": None,
                        "result": 'mf"%s_FARE"' % skim_name,
                        "expression": "(%s_FARE + %s_IN_VEHICLE_COST)"
                        % (skim_name, skim_name),
                    }
                )
            matrix_calc(spec_list, scenario=scenario, num_processors=num_processors)

        if use_ccr:
            with self._emme_manager.logbook_trace("Calculate CCR skims"):
                create_extra(
                    "TRANSIT_SEGMENT",
                    "@eawt",
                    "extra added wait time",
                    overwrite=True,
                    scenario=scenario,
                )
                # create_extra("TRANSIT_SEGMENT", "@crowding_factor", "crowding factor along segments", overwrite=True, scenario=scenario)
                create_extra(
                    "TRANSIT_SEGMENT",
                    "@capacity_penalty",
                    "capacity penalty at boarding",
                    overwrite=True,
                    scenario=scenario,
                )
                network = scenario.get_partial_network(
                    ["TRANSIT_LINE", "TRANSIT_SEGMENT"], include_attributes=True
                )
                attr_map = {
                    "TRANSIT_SEGMENT": ["@phdwy", "transit_volume", "transit_boardings"],
                    "TRANSIT_VEHICLE": ["seated_capacity", "total_capacity"],
                    "TRANSIT_LINE": ["headway"],
                }
                if use_fares:
                    # only if use_fares, otherwise will use .mode.id
                    attr_map["TRANSIT_LINE"].append("#src_mode")
                    mode_name = '["#src_mode"]'
                else:
                    mode_name = ".mode.id"
                for domain, attrs in attr_map.items():
                    values = scenario.get_attribute_values(domain, attrs)
                    network.set_attribute_values(domain, attrs, values)

                enclosing_scope = {"network": network, "scenario": scenario}
                # code = compile(_segment_cost_function, "segment_cost_function", "exec")
                # exec(code, enclosing_scope)
                code = compile(
                    _headway_cost_function.format(mode_name),
                    "headway_cost_function",
                    "exec",
                )
                exec(code, enclosing_scope)
                calc_eawt = enclosing_scope["calc_eawt"]
                hdwy_fraction = 0.5  # fixed in assignment spec

                # NOTE: assume assignment period is 1 hour
                for segment in network.transit_segments():
                    headway = segment.line.headway
                    veh_cap = line.vehicle.total_capacity
                    # capacity = 60.0 * veh_cap / line.headway
                    capacity = 60.0 * _hours_in_period[period] * veh_cap / line.headway
                    transit_volume = segment.transit_volume
                    vcr = transit_volume / capacity
                    segment["@eawt"] = calc_eawt(segment, vcr, headway)
                    # segment["@crowding_penalty"] = calc_segment_cost(transit_volume, capacity, segment)
                    segment["@capacity_penalty"] = (
                        max(segment["@phdwy"] - segment["@eawt"] - headway, 0)
                        * hdwy_fraction
                    )

                values = network.get_attribute_values(
                    "TRANSIT_SEGMENT", ["@eawt", "@capacity_penalty"]
                )
                scenario.set_attribute_values(
                    "TRANSIT_SEGMENT", ["@eawt", "@capacity_penalty"], values
                )

                # # Link unreliability
                # spec = get_strat_spec({"in_vehicle": "ul1"}, "%s_LINKREL" % skim_name)
                # strategy_analysis(spec, class_name=class_name, scenario=scenario, num_processors=num_processors)

                # Crowding penalty
                spec = get_strat_spec({"in_vehicle": "@ccost"}, "%s_CROWD" % skim_name)
                strategy_analysis(
                    spec,
                    class_name=class_name,
                    scenario=scenario,
                    num_processors=num_processors,
                )

                # skim node reliability, Extra added wait time (EAWT)
                spec = get_strat_spec({"boarding": "@eawt"}, "%s_EAWT" % skim_name)
                strategy_analysis(
                    spec,
                    class_name=class_name,
                    scenario=scenario,
                    num_processors=num_processors,
                )

                # skim capacity penalty
                spec = get_strat_spec(
                    {"boarding": "@capacity_penalty"}, "%s_CAPPEN" % skim_name
                )
                strategy_analysis(
                    spec,
                    class_name=class_name,
                    scenario=scenario,
                    num_processors=num_processors,
                )

    def mask_allpen(self, period):
        # Reset skims to 0 if not both local and premium
        localivt_skim = self._matrix_cache.get_data(f"{period}_ALLPEN_LBIVTT")
        totalivt_skim = self._matrix_cache.get_data(f"{period}_ALLPEN_TOTALIVTT")
        has_premium = numpy.greater((totalivt_skim - localivt_skim), 0)
        has_both = numpy.greater(localivt_skim, 0) * has_premium
        for skim in _skim_names:
            mat_name = f"{period}_ALLPEN_{skim}"
            data = self._matrix_cache.get_data(mat_name)
            self._matrix_cache.set_data(mat_name, data * has_both)

    def mask_transfers(self, period):
        # Reset skims to 0 if number of transfers is greater than max_transfers
        max_transfers = self.config.transit.max_transfers
        for skim_set in ["BUS", "PREM", "ALLPEN"]:
            xfers = self._matrix_cache.get_data(f"{period}_{skim_set}_XFERS")
            xfer_mask = np.less_equal(xfers, max_transfers)
            for skim in _skim_names:
                mat_name = f"{period}_{skim_set}_{skim}"
                data = self._matrix_cache.get_data(mat_name)
                self._matrix_cache.set_data(mat_name, data * xfer_mask)

    def export_skims(self, period, scenario):
        """Export skims to OMX files by period."""
        # NOTE: skims in separate file by period
        matrices = []
        for skim_set in ["BUS", "PREM", "ALLPEN"]:
            for skim in _skim_names:
                matrices.append(f"{period}_{skim_set}_{skim}")
        omx_file_path = _join(
            self.root_dir, 
            self.config.transit.output_skims_path.format(period=period))
        os.makedirs(omx_file_path, exist_ok=True)
        with _emme_tools.OMX(
            omx_file_path, "w", scenario, matrix_cache=self._matrix_cache, mask_max_value=1e7
        ) as omx_file:
            omx_file.write_matrices(matrices)
        self._matrix_cache.clear()

    def export_boardings_by_line(self, emme_app, output_transit_boardings_file):
        # TODO: untested
        project = emme_app.project
        table = project.new_network_table("TRANSIT_LINE")
        column = _worksheet.Column()

        # Creating total boardings by line table
        column.expression = "line"
        column.name = "line_name"
        table.add_column(0, column)

        column.expression = "description"
        column.name = "description"
        table.add_column(1, column)

        column.expression = "ca_board_t"
        column.name = "total_boardings"
        table.add_column(2, column)

        column.expression = "#src_mode"
        column.name = "mode"
        table.add_column(3, column)

        column.expression = "@mode"
        column.name = "line_mode"
        table.add_column(4, column)

        table.export(output_transit_boardings_file)
        table.close()

    def report(self, scenario, period):
        # TODO: untested
        text = ['<div class="preformat">']
        matrices = []
        for skim_set in ["BUS", "PREM", "ALLPEN"]:
            for skim in _skim_names:
                matrices.append(f"{period}_{skim_set}_{skim}")
        num_zones = len(scenario.zone_numbers)
        num_cells = num_zones ** 2
        text.append(
            "Number of zones: %s. Number of O-D pairs: %s. "
            "Values outside -9999999, 9999999 are masked in summaries.<br>"
            % (num_zones, num_cells)
        )
        text.append(
            "%-25s %9s %9s %9s %13s %9s" % ("name", "min", "max", "mean", "sum", "mask num")
        )
        for name in matrices:
            data = self._matrix_cache.get_data(name)
            data = numpy.ma.masked_outside(data, -9999999, 9999999)
            stats = (
                name,
                data.min(),
                data.max(),
                data.mean(),
                data.sum(),
                num_cells - data.count(),
            )
            text.append("%-25s %9.4g %9.4g %9.4g %13.7g %9d" % stats)
        text.append("</div>")
        title = "Transit impedance summary for period %s" % period
        report = _m.PageBuilder(title)
        report.wrap_html("Matrix details", "<br>".join(text))
        self._emme_manager.logbook_write(title, report.render())


def get_jl_xfer_penalty(modes, effective_headway_source, xfer_perception_factor):
    level_rules = [{
        "description": "",
        "destinations_reachable": True,
        "transition_rules": [{"mode": m, "next_journet_level": 1} for m in modes],
    },
    {
        "description": "",
        "destinations_reachable": True,
        "transition_rules": [{"mode": m, "next_journet_level": 1} for m in modes],
        "waiting_time": {
            "headway_fraction": 0.5,
            "effective_headways": effective_headway_source,
            "spread_factor": 1,
            "perception_factor": xfer_perception_factor
        }
    }]
    return level_rules



def get_strat_spec(components, matrix_name):
    spec = {
        "trip_components": components,
        "sub_path_combination_operator": "+",
        "sub_strategy_combination_operator": "average",
        "selected_demand_and_transit_volumes": {
            "sub_strategies_to_retain": "ALL",
            "selection_threshold": {"lower": -999999, "upper": 999999},
        },
        "analyzed_demand": None,
        "constraint": None,
        "results": {"strategy_values": matrix_name},
        "type": "EXTENDED_TRANSIT_STRATEGY_ANALYSIS",
    }
    return spec