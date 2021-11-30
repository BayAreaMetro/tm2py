"""Performs transit assignment and generates transit skims.

"""

from collections import defaultdict as _defaultdict, OrderedDict
from copy import deepcopy as _copy
from contextlib import contextmanager as _context
import json as _json
import numpy as np
import os

from tm2py.core.component import Component as _Component, Controller as _Controller
import tm2py.core.emme as _emme_tools
from tm2py.core.logging import LogStartEnd

# TODO: imports from skim_transit_network.py, to be reviewed
import inro.modeller as _m
import inro.emme.desktop.worksheet as _worksheet


# TODO: should express these in the config
# TODO: or make global lists tuples
# _all_access_modes = ["WLK", "PNR", "KNRTNC", "KNRPRV"]
_all_access_modes = ["WLK", "PNR", "KNR"]
_all_sets = ["set1", "set2", "set3"]
_set_dict = {"BUS": "set1", "PREM": "set2", "ALLPEN": "set3"}

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

    @LogStartEnd("transit assignment and skims")
    def run(self):
        """Run transit assignment and skims."""
        project_path = os.path.join(self.root_dir, self.config.emme.project_path)
        self._emme_manager = _emme_tools.EmmeManager()
        # Initialize Emme desktop if not already started
        emme_app = self._emme_manager.project(project_path)
        self._emme_manager.init_modeller(emme_app)
        emmebank_path = os.path.join(self.root_dir, self.config.emme.transit_database_path)
        self._emmebank = emmebank = self._emme_manager.emmebank(emmebank_path)
        ref_scenario = self._emmebank.scenario(self.config.periods[0].emme_scenario_id)
        period_names = [time.name for time in self.config.periods]
        self.initialize_skim_matrices(period_names, ref_scenario)
        # Run assignment and skims for all specified periods
        for period in self.config.periods:
            scenario = emmebank.scenario(period.emme_scenario_id)
            with self._setup(scenario, period):
                if self.controller.iteration >= 1:
                    self.import_demand_matrices(period.name, scenario)
                    use_ccr = False
                else:
                    self.create_empty_demand_matrices(period.name, scenario)
                    use_ccr = False

                network = scenario.get_network()
                self.update_auto_times(network, period)
                if self.config.transit.get("override_connector_times", False):
                    self.update_connector_times(scenario, network, period)
                # TODO: could set attribute_values instead of full publish
                scenario.publish_network(network)

                use_fares = self.config.transit.use_fares
                self.assign_and_skim(
                    scenario,
                    network,
                    period=period,
                    assignment_only=False,
                    use_fares=use_fares,
                    use_ccr=use_ccr,
                )
                self.export_skims(period.name, scenario)

                # if use_ccr and save_iter_flows:
                #     self.save_per_iteration_flows(scenario)

                # if output_transit_boardings:
                #     desktop.data_explorer().replace_primary_scenario(scenario)
                #     output_transit_boardings_file = _os.path.join(
                #          _os.getcwd(), args.trn_path, "boardings_by_line_{}.csv".format(period))
                #     export_boardings_by_line(emme_app, output_transit_boardings_file)
                if self.config.transit.get("output_stop_usage_path"):
                    self.export_connector_flows(scenario, period)

    @_context
    def _setup(self, scenario, period):
        with self.logger.log_start_end(f"period {period.name}"):
            with self._emme_manager.logbook_trace(f"Transit assignments for period {period.name}"):
                self._matrix_cache = _emme_tools.MatrixCache(scenario)
                self._skim_matrices = []
                try:
                    yield
                finally:
                    self._matrix_cache.clear()
                    self._matrix_cache = None

    @LogStartEnd("prepare network attributes and update times from auto network")
    def update_auto_times(self, transit_network, period):
        auto_emmebank = self._emme_manager.emmebank(os.path.join(self.root_dir, self.config.emme.highway_database_path))
        auto_scenario = auto_emmebank.scenario(period.emme_scenario_id)
        if auto_scenario.has_traffic_results:
            # TODO: partial network load
            auto_network = auto_scenario.get_network()
            link_lookup = {}
            for auto_link in auto_network.links():
                link_lookup[auto_link["#link_id"]] = auto_link
            for tran_link in transit_network.links():
                auto_link = link_lookup.get(tran_link["#link_id"])
                if not auto_link:
                    continue
                # TODO: may need to remove "reliability" factor in future versions of VDF definition
                auto_time = auto_link.auto_time
                if auto_time >= 0:
                    tran_link["@trantime"] = auto_time

        # set us1 (segment data1), used in ttf expressions, from @trantime
        for segment in transit_network.transit_segments():
            if segment['@schedule_time'] <= 0 and segment.link is not None:
                segment.data1 = segment["@trantime_seg"] = segment.link["@trantime"]

    def update_connector_times(self, scenario, network, period):
        # walk time attributes per skim set
        connector_attrs = {1: "@walk_time_bus", 2: "@walk_time_prem", 3: "@walk_time_all"}
        for attr_name in connector_attrs.values():
            if scenario.extra_attribute(attr_name) is None:
                scenario.create_extra_attribute("LINK", attr_name)
            # delete attribute in network object to reinitialize to default values
            if attr_name in network.attributes("LINK"):
                network.delete_attribute("LINK", attr_name)
            network.create_attribute("LINK", attr_name, 9999)
        period_name = period.name.lower()

        # lookup adjacent real stop to account for connector splitting
        connectors = _defaultdict(lambda: {})
        for zone in network.centroids():
            taz_id = int(zone["@taz_id"])
            for link in zone.outgoing_links():
                stop_id = int(link.j_node["#node_id"])
                connectors[taz_id][stop_id] = link
            for link in zone.incoming_links():
                stop_id = int(link.i_node["#node_id"])
                connectors[stop_id][taz_id] = link
        with open(os.path.join(self.root_dir, self.config.transit.input_connector_access_times_path), 'r') as f:
            header = [x.strip() for x in next(f).split(",")]
            for line in f:
                tokens = line.split(",")
                data = dict(zip(header, tokens))
                if data["time_period"].lower() == period_name:
                    taz = int(data["from_taz"])
                    stop = int(data["to_stop"])
                    connector = connectors[taz][stop]
                    attr_name = connector_attrs[int(data["skim_set"])]
                    connector[attr_name] = float(data["est_walk_min"])
        with open(os.path.join(self.root_dir, self.config.transit.input_connector_egress_times_path), 'r') as f:
            header = [x.strip() for x in next(f).split(",")]
            for line in f:
                tokens = line.split(",")
                data = dict(zip(header, tokens))
                if data["time_period"].lower() == period_name:
                    taz = int(data["to_taz"])
                    stop = int(data["from_stop"])
                    connector = connectors[stop][taz]
                    attr_name = connector_attrs[int(data["skim_set"])]
                    connector[attr_name] = float(data["est_walk_min"])
        # NOTE: publish in calling setup function ...
        # attrs = connector_attrs.values()
        # values = network.get_attribute_values("LINK", attrs)
        # scenario.set_attribute_values("LINK", attrs, values)

    @LogStartEnd("initialize matrices")
    def initialize_skim_matrices(self, time_periods, scenario):
        with self._emme_manager.logbook_trace("Create and initialize matrices"):
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
            matrices = [("ms", "zero", "zero")]
            emmebank = scenario.emmebank
            for period in time_periods:
                for set_name, set_desc in skim_sets:
                    for name, desc in tmplt_matrices:
                        matrices.append(("mf", f"{period}_{set_name}_{name}", f"{period} {set_desc}: {desc}"))
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
                matrix = emmebank.matrix(f'{mtype}"{name}"')
                if matrix:
                    emmebank.delete_matrix(matrix)
                create_matrix(mtype, name, desc, scenario=scenario, overwrite=True)

    @LogStartEnd("Import transit demand")
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
        # omx_filename_template = "transit_{period}_{access_mode}_TRN_{set}_{period}.omx"
        omx_filename_template = os.path.join(self.root_dir, self.config.household.transit_demand_file)
        matrix_name_template = "{access_mode}_SET_{period}"
        emme_matrix_name_template = "TRN_{set}_{period}"
        # with _m.logbook_trace("Importing demand matrices for period %s" % period):
        for set_num in _all_sets:
            demand = None
            omx_filename_path = omx_filename_template.format(
                period=period_name, set=set_num
            )
            with _emme_tools.OMX(omx_filename_path) as file_obj:
                for access_mode in _all_access_modes:
                    matrix_name = matrix_name_template.format(
                        period=period_name, access_mode=access_mode
                    )
                    access_demand = file_obj.read(matrix_name.upper())
                    if demand is None:
                        demand = access_demand
                    else:
                        demand += access_demand

            shape = demand.shape
            # pad external zone values with 0
            if shape != (num_zones, num_zones):
                demand = np.pad(
                    demand, ((0, num_zones - shape[0]), (0, num_zones - shape[1]))
                )
            demand_name = emme_matrix_name_template.format(period=period_name, set=set_num)
            matrix = emmebank.matrix(f'mf"{demand_name}"')
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
        emmebank = scenario.emmebank
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
                        network,
                        period,
                        assignment_only=False,
                        use_fares=False,
                        use_ccr=False,
                        ):
        # TODO: double check value of time from $/min to $/hour is OK
        # network = scenario.get_network()
        # network = scenario.get_partial_network(
        #     element_types=["TRANSIT_LINE", "TRANSIT_SEGMENT"], include_attributes=True)
        mode_types = {"LOCAL": [], "PREMIUM": [], "WALK": []}
        for mode in self.config.transit.modes:
            if mode.type in ["WALK", "ACCESS", "EGRESS"]:
                mode_types["WALK"].append(mode.id)
            elif mode.type in ["LOCAL", "PREMIUM"]:
                mode_types[mode.type].append(mode.id)
        with self._emme_manager.logbook_trace("Transit assignment and skims for period %s" % period.name):
            self.run_assignment(
                scenario,
                period,
                network,
                mode_types,
                use_fares,
                use_ccr,
            )

            if not assignment_only:
                with self._emme_manager.logbook_trace("Skims for Local-only (set1)"):
                    self.run_skims(
                        scenario,
                        "BUS",
                        period,
                        mode_types["LOCAL"],
                        network,
                        use_fares,
                        use_ccr,
                    )
                with self._emme_manager.logbook_trace("Skims for Premium-only (set2)"):
                    self.run_skims(
                        scenario,
                        "PREM",
                        period,
                        mode_types["PREMIUM"],
                        network,
                        use_fares,
                        use_ccr,
                    )
                with _m.logbook_trace("Skims for Local+Premium (set3)"):
                    self.run_skims(
                        scenario,
                        "ALLPEN",
                        period,
                        mode_types["LOCAL"] + mode_types["PREMIUM"],
                        network,
                        use_fares,
                        use_ccr,
                    )
                    self.mask_allpen(period.name)
                self.mask_transfers(period.name)
                # report(scenario, period)

    def run_assignment(
            self,
            scenario,
            period,
            network,
            mode_types,
            use_fares=False,
            use_ccr=False,
    ):

        # REVIEW: separate method into smaller steps
        #     - specify class structure in config
        #     - 
        params = self.config.transit
        modeller = self._emme_manager.modeller
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
            "boarding_time": {"global": {
                "penalty": params["initial_boarding_penalty"], "perception_factor": 1}
            },
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

            local_modes = get_fare_modes(mode_types["LOCAL"])
            premium_modes = get_fare_modes(mode_types["PREMIUM"])
            project_dir = os.path.dirname(os.path.dirname(scenario.emmebank.path))
            with open(
                    os.path.join(
                        project_dir, "Specifications", "%s_BUS_journey_levels.ems" % period.name
                    ),
                    "r",
            ) as f:
                local_journey_levels = _json.load(f)["journey_levels"]
            with open(
                    os.path.join(
                        project_dir, "Specifications", "%s_PREM_journey_levels.ems" % period.name
                    ),
                    "r",
            ) as f:
                premium_modes_journey_levels = _json.load(f)["journey_levels"]
            with open(
                    os.path.join(
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
                        "effective_headways": params["effective_headway_source"],
                        "spread_factor": 1,
                        "perception_factor": params["transfer_wait_perception_factor"]
                    }
                    if "transfer_boarding_penalty" in params:
                        level["boarding_time"] = {"global": {
                            "penalty": params["transfer_boarding_penalty"], "perception_factor": 1}
                        }
                # add in the correct value of time parameter
                for level in jls:
                    if level["boarding_cost"]:
                        level["boarding_cost"]["on_segments"]["perception_factor"] = fare_perception

            mode_attr = '["#src_mode"]'
        else:
            local_modes = list(mode_types["LOCAL"])
            premium_modes = list(mode_types["PREMIUM"])
            local_journey_levels = get_jl_xfer_penalty(
                local_modes,
                params["effective_headway_source"],
                params["transfer_wait_perception_factor"],
                params.get("transfer_boarding_penalty")
            )
            premium_modes_journey_levels = get_jl_xfer_penalty(
                premium_modes,
                params["effective_headway_source"],
                params["transfer_wait_perception_factor"],
                params.get("transfer_boarding_penalty")
            )
            journey_levels = get_jl_xfer_penalty(
                local_modes + premium_modes,
                params["effective_headway_source"],
                params["transfer_wait_perception_factor"],
                params.get("transfer_boarding_penalty")
            )
            mode_attr = ".mode.id"

        skim_parameters = OrderedDict(
            [
                (
                    "BUS",
                    {
                        "modes": mode_types["WALK"] + local_modes,
                        "journey_levels": local_journey_levels,
                    },
                ),
                (
                    "PREM",
                    {
                        "modes": mode_types["WALK"] + premium_modes,
                        "journey_levels": premium_modes_journey_levels,
                    },
                ),
                (
                    "ALLPEN",
                    {
                        "modes": mode_types["WALK"] + local_modes + premium_modes,
                        "journey_levels": journey_levels,
                    },
                ),
            ]
        )
        if self.config.transit.get("override_connector_times", False):
            skim_parameters["BUS"]["aux_transit_cost"] = {
                "penalty": "@walk_time_bus", "perception_factor": params["walk_perception_factor"]
            }
            skim_parameters["PREM"]["aux_transit_cost"] = {
                "penalty": "@walk_time_prem", "perception_factor": params["walk_perception_factor"]
            }
            skim_parameters["ALLPEN"]["aux_transit_cost"] = {
                "penalty": "@walk_time_all", "perception_factor": params["walk_perception_factor"]
            }

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
                # Optional aux_transit_cost, used for walk time on connectors, set if override_connector_times
                spec["aux_transit_cost"] = parameters.get("aux_transit_cost")
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
                # Optional aux_transit_cost, used for walk time on connectors, set if override_connector_times
                spec["aux_transit_cost"] = parameters.get("aux_transit_cost")
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
        modeller = self._emme_manager.modeller
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

        override_connectors = self.config.transit.get("override_connector_times", False)
        class_name = name
        skim_name = "%s_%s" % (period.name, name)
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
            if override_connectors:
                spec["by_mode_subset"]["actual_aux_transit_times"] = 'mf"%s_XFERWALK"' % skim_name
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
                    "result": f'mf"{skim_name}_TOTALIVTT"',
                    "expression": "+".join(total_ivtt_expr),
                },
                {  # convert number of boardings to number of transfers
                    "type": "MATRIX_CALCULATION",
                    "constraint": {
                        "by_value": {
                            "od_values": f'mf"{skim_name}_XFERS"',
                            "interval_min": 0,
                            "interval_max": 9999999,
                            "condition": "INCLUDE",
                        }
                    },
                    "result": f'mf"{skim_name}_XFERS"',
                    "expression": f'(mf"{skim_name}_XFERS" - 1).max.0',
                },
                {
                    "type": "MATRIX_CALCULATION",
                    "constraint": {
                        "by_value": {
                            "od_values": f'mf"{skim_name}_TOTALWAIT"',
                            "interval_min": 0,
                            "interval_max": 9999999,
                            "condition": "INCLUDE",
                        }
                    },
                    "result": f'mf"{skim_name}_XFERWAIT"',
                    "expression": f'(mf"{skim_name}_TOTALWAIT" - mf"{skim_name}_FIRSTWAIT").max.0',
                },
            ]
            if use_fares:
                # sum in-vehicle cost and boarding cost to get the fare paid
                spec_list.append({
                    "type": "MATRIX_CALCULATION",
                    "constraint": None,
                    "result": f'mf"{skim_name}_FARE"',
                    "expression": f'(mf"{skim_name}_FARE" + mf"{skim_name}_IN_VEHICLE_COST)"'})

            if override_connectors:
                # walk time skim updates for TAZ based assignment: XFERWALK contains the connector times,
                # and TOTALWALK contains the xfer (real) walk times, swap matrix results
                spec_list.append({  # total walk time = transfer walk plus connector walk
                    "type": "MATRIX_CALCULATION",
                    "constraint": None,
                    "result": f'mf"{skim_name}_TOTALWALK"',
                    "expression": f'(mf"{skim_name}_TOTALWALK" + mf"{skim_name}_XFERWALK").max.0',
                })
                spec_list.append({  # transfer walk time = total walk time - connector walk time
                    "type": "MATRIX_CALCULATION",
                    "constraint": None,
                    "result": f'mf"{skim_name}_XFERWALK"',
                    "expression": f'(mf"{skim_name}_TOTALWALK" - mf"{skim_name}_XFERWALK").max.0',
                })
            else:  # For TAP-based assignment, nominal walk time on connectors of 0.33
                spec_list.append({  # transfer walk time = total - access - egress
                    "type": "MATRIX_CALCULATION",
                    "constraint": None,
                    "result": f'mf"{skim_name}_XFERWALK"',
                    "expression": f'(mf"{skim_name}_TOTALWALK" - 0.66).max.0',
                })
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
                # create_extra("TRANSIT_SEGMENT", "@crowding_factor",
                # "crowding factor along segments", overwrite=True, scenario=scenario)
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

                for segment in network.transit_segments():
                    headway = segment.line.headway
                    veh_cap = line.vehicle.total_capacity
                    # capacity = 60.0 * veh_cap / line.headway
                    capacity = 60.0 * period.duration * veh_cap / line.headway
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
                spec = get_strat_spec({"in_vehicle": "@ccost"}, f'mf"{skim_name}_CROWD"')
                strategy_analysis(
                    spec,
                    class_name=class_name,
                    scenario=scenario,
                    num_processors=num_processors,
                )

                # skim node reliability, Extra added wait time (EAWT)
                spec = get_strat_spec({"boarding": "@eawt"}, f'mf"{skim_name}_EAWT"')
                strategy_analysis(
                    spec,
                    class_name=class_name,
                    scenario=scenario,
                    num_processors=num_processors,
                )

                # skim capacity penalty
                spec = get_strat_spec(
                    {"boarding": "@capacity_penalty"}, f'mf"{skim_name}_CAPPEN"'
                )
                strategy_analysis(
                    spec,
                    class_name=class_name,
                    scenario=scenario,
                    num_processors=num_processors,
                )

    def mask_allpen(self, period):
        # Reset skims to 0 if not both local and premium
        localivt_skim = self._matrix_cache.get_data(f'mf"{period}_ALLPEN_LBIVTT"')
        totalivt_skim = self._matrix_cache.get_data(f'mf"{period}_ALLPEN_TOTALIVTT"')
        has_premium = np.greater((totalivt_skim - localivt_skim), 0)
        has_both = np.greater(localivt_skim, 0) * has_premium
        for skim in _skim_names:
            mat_name = f'mf"{period}_ALLPEN_{skim}"'
            data = self._matrix_cache.get_data(mat_name)
            self._matrix_cache.set_data(mat_name, data * has_both)

    def mask_transfers(self, period):
        # Reset skims to 0 if number of transfers is greater than max_transfers
        max_transfers = self.config.transit.max_transfers
        for skim_set in ["BUS", "PREM", "ALLPEN"]:
            xfers = self._matrix_cache.get_data(f'mf"{period}_{skim_set}_XFERS"')
            xfer_mask = np.less_equal(xfers, max_transfers)
            for skim in _skim_names:
                mat_name = f'mf"{period}_{skim_set}_{skim}"'
                data = self._matrix_cache.get_data(mat_name)
                self._matrix_cache.set_data(mat_name, data * xfer_mask)

    def export_skims(self, period, scenario):
        """Export skims to OMX files by period."""
        # NOTE: skims in separate file by period
        matrices = []
        for skim_set in ["BUS", "PREM", "ALLPEN"]:
            for skim in _skim_names:
                matrices.append(f'mf"{period}_{skim_set}_{skim}"')
        omx_file_path = os.path.join(
            self.root_dir,
            self.config.transit.output_skim_path.format(period=period))
        os.makedirs(os.path.dirname(omx_file_path), exist_ok=True)
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

    def export_connector_flows(self, scenario, period):
        # export boardings and alightings by stop (connector) and TAZ
        modeller = self._emme_manager.modeller
        network_results = modeller.tool(
            "inro.emme.transit_assignment.extended.network_results"
        )
        create_extra = modeller.tool(
            "inro.emme.data.extra_attribute.create_extra_attribute"
        )
        skim_sets = ["BUS", "PREM", "ALLPEN"]
        names = []
        for name in skim_sets:
            attr_name = f"@aux_volume_{name}".lower()
            create_extra("LINK", attr_name, overwrite=True, scenario=scenario)
            spec = {
                "type": "EXTENDED_TRANSIT_NETWORK_RESULTS",
                "on_links": {"aux_transit_volumes": attr_name}
            }
            network_results(spec, class_name=name, scenario=scenario)
            names.append((name, attr_name))

        # TODO: optimization: partial network to only load links and certain attributes
        network = scenario.get_network()
        path_tmplt = os.path.join(self.root_dir, self.config.transit.output_stop_usage_path)
        with open(path_tmplt.format(period=period.name), "w") as f:
            f.write(",".join(["mode", "taz", "stop", "boardings", "alightings"]))
            for zone in network.centroids():
                taz_id = zone["@taz_id"]
                for link in zone.outgoing_links():
                    stop_id = link.j_node["#node_id"]
                    for name, attr_name in names:
                        boardings = link[attr_name]
                        alightings = link.reverse_link[attr_name] if link.reverse_link else 0
                        f.write(",".join([name, taz_id, stop_id, boardings, alightings]))
                for link in zone.incoming_links():
                    if link.reverse_link:  # already exported
                        continue
                    stop_id = link.i_node["#node_id"]
                    for name, attr_name in names:
                        f.write(",".join([name, taz_id, stop_id, 0, link[attr_name]]))

    def report(self, scenario, period):
        # TODO: untested
        text = ['<div class="preformat">']
        matrices = []
        for skim_set in ["BUS", "PREM", "ALLPEN"]:
            for skim in _skim_names:
                matrices.append(f'mf"{period}_{skim_set}_{skim}"')
        num_zones = len(scenario.zone_numbers)
        num_cells = num_zones * num_zones
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
            data = np.ma.masked_outside(data, -9999999, 9999999)
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


def get_jl_xfer_penalty(modes, effective_headway_source, xfer_perception_factor, xfer_boarding_penalty):
    level_rules = [{
        "description": "",
        "destinations_reachable": True,
        "transition_rules": [{"mode": m, "next_journey_level": 1} for m in modes],
    },
        {
            "description": "",
            "destinations_reachable": True,
            "transition_rules": [{"mode": m, "next_journey_level": 1} for m in modes],
            "waiting_time": {
                "headway_fraction": 0.5,
                "effective_headways": effective_headway_source,
                "spread_factor": 1,
                "perception_factor": xfer_perception_factor
            }

        }]

    if xfer_boarding_penalty is not None:
        level_rules[1]["boarding_time"] = {"global": {
            "penalty": xfer_boarding_penalty, "perception_factor": 1}
        }
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
