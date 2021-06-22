"""Highway assignment module docsting


NOTES: 
The following are details in the implementation which are specified 
in the config.properties file or in the traffic_config list in the 
run function (to be moved to the config.properties).
    The traffic assignment is run for the following periods:
        (1) EA: early AM, 3 am to 6 am; 
        (2) AM: AM peak period, 6 am to 10 am; 
        (3) MD: midday, 10 am to 3 pm; 
        (4) PM: PM peak period, 3 pm to 7 pm; and, 
        (5) EV: evening, 7 pm to 3 am.  

    Ten vehicle classes are tracked in each assignment, with value-toll-eligible 
    and not-value-toll-eligible classes, with the skims noted.
        (1) da, mode "d", drive alone, no value toll
                skims: time, dist, freeflowtime, bridgetoll_da
        (2) sr2, mode "e", shared ride 2, no value toll
                skims: time, dist, freeflowtime, bridgetoll_sr2, hovdist
        (3) sr3, mode "f", shared ride 3+, no value toll
                skims: time, dist, freeflowtime, bridgetoll_sr3, hovdist
        (4) truck, mode "t", very small, small, and medium trucks, no value toll
                skims: time, dist, freeflowtime, 
                       bridgetoll_vsm, bridgetoll_sml, bridgetoll_med
        (5) lrgtrk, mode "l", large trucks, no value toll
                skims: time, dist, freeflowtime, bridgetoll_lrg
        (6) datoll, mode "D", drive alone, value toll eligible
                skims: time, dist, freeflowtime, bridgetoll_da, valuetoll_da, tolldist
        (7) sr2toll, mode "E", shared ride 2, value toll eligible
                skims: time, dist, freeflowtime, bridgetoll_sr2, valuetoll_sr2, tolldist
        (8) sr3toll, mode "F", shared ride 3+, value toll eligible
                skims: time, dist, freeflowtime, bridgetoll_sr3, valuetoll_sr3, tolldist
        (9) trucktoll, mode "T", very small, small, and medium trucks, value toll eligible
                skims: time, dist, freeflowtime, 
                       bridgetoll_vsm, bridgetoll_sml, bridgetoll_med,
                       valuetoll_vsm, valuetoll_sml, valuetoll_med
        (10) lrgtrktoll, mode "L", large trucks, value toll eligible
                skims: time, dist, freeflowtime, 

    Note that the "truck" and "trucktoll" classes combine very small, small and medium trucks
    The skims are stored in the Emmebank and exported to OMX with names with the following
    convention:
        period_class_skim

    Four types of trips are assigned: 
        (a) personal, inter-regional travel, file "household" 
        (b) personal, intra-regional travel, file "internal_external"; 
        (c) commercial travel, file "commercial"
        (d) air passenger travel, file "air_passenger"

    Separate trip tables are read in by the script for each of these travel types. 

The Emme network must have the following attributes set:
    - "length" in feet
    - "type"  the facility type 
    - "vdf", volume delay function
    - "@free_flow_speed" the free flow speed (in miles per hour)
    - "@tollXX_YY"
    - "@bridgetoll_YY"
    - "@valuetoll_YY"
    - modes
    - "@mazseq"
    - "#county"

 Network results: 
     TODO       

 Notes: 
    - Output matrices are in miles, minutes, and cents (2010 dollars) and are stored as real values;
    - Intrazonal distance/time is one half the distance/time to the nearest neighbor;
    - Intrazonal bridge and value tolls are assumed to be zero

"""

import array as _array
from collections import defaultdict as _defaultdict
from contextlib import contextmanager as _context
from math import sqrt as _sqrt
import numpy as _numpy
import os as _os
from tm2py.core.component import Component as _Component

# import tm2py.core.tools as _tools
import tm2py.core.emme as _emme_tools


_join, _dir = _os.path.join, _os.path.dirname


class HighwayAssignment(_Component):
    """docstring for traffic assignment"""

    def __init__(self, controller):
        super().__init__(controller)
        self._relative_gap = self.config.emme.highway_assignment.relative_gap
        self._max_iterations = self.config.emme.highway_assignment.max_iterations
        self._num_processors = self.config.emme.number_of_processors
        # TODO: num_processors needs to be parsed to an int
        self._modeller = None
        self._matrix_cache = None

    def run(self, root=None):
        """docstring for traffic assignment run"""

        # TODO: scenario structure, project relative to current dir?
        if root is None:
            root = _os.getcwd()
        project_path = _join(root, "mtc_emme", "mtc_emme.emp")
        self._emme_manager = _emme_tools.EmmeProjectCache()
        self._project = self._emme_manager.project(project_path)
        self._modeller = self._emme_manager.modeller(self._project)
        self._emmebank = self._modeller.emmebank

        msa_iteration = self.controller.iteration
        # List of assignment classes
        # TODO: move to config
        traffic_config = [
            {  # 0
                "name": "da",
                "description": "drive alone",
                "mode": "d",
                "demand": [
                    {"file": "household", "name": "SOV_GP_{period}"},
                    {"file": "air_passenger", "name": "DA"},
                    {"file": "internal_external", "name": "DA"},
                ],
                "excluded_links": ["is_toll_da", "is_hov"],
                "value_of_time": 18.93,  # $ / hr
                "operating_cost": 17.23,  # cents / mile
                "toll": "@bridgetoll_da",
                "skims": ["time", "dist," "freeflowtime", "bridgetoll_da"],
                # available skims: time, dist, bridgetoll_{}, valuetoll_{},
                #                  freeflowtime, hovdist, tolldist
            },
            {  # 1
                "name": "sr2",
                "description": "shared ride 2",
                "mode": "e",
                "demand": [
                    {
                        "file": "household",
                        "name": "SR2_GP_{period}",
                        "factor": 1 / 1.75,
                    },
                    {
                        "file": "household",
                        "name": "SR2_HOV_{period}",
                        "factor": 1 / 1.75,
                    },
                    {"file": "air_passenger", "name": "SR2"},
                    {"file": "internal_external", "name": "SR2"},
                ],
                "excluded_links": ["is_toll_s2", "is_hov3"],
                "value_of_time": 18.93,  # $ / hr
                "operating_cost": 17.23,  # cents / mile
                "toll": "@bridgetoll_sr2",
                "skims": ["time", "dist", "freeflowtime", "bridgetoll_sr2", "hovdist"],
            },
            {  # 2
                "name": "sr3",
                "description": "shared ride 3+",
                "mode": "f",
                "demand": [
                    {"file": "household", "name": "SR3_GP_{period}", "factor": 1 / 2.5},
                    {
                        "file": "household",
                        "name": "SR3_HOV_{period}",
                        "factor": 1 / 2.5,
                    },
                    {"file": "air_passenger", "name": "SR3"},
                    {"file": "internal_external", "name": "SR3"},
                ],
                "excluded_links": ["is_toll_s3"],
                "value_of_time": 18.93,  # $ / hr
                "operating_cost": 17.23,  # cents / mile
                "toll": "@bridgetoll_sr3",
                "skims": ["time", "dist", "freeflowtime", "bridgetoll_sr3", "hovdist"],
            },
            {  # 3
                "name": "truck",
                "description": "truck",
                "mode": "t",
                "demand": [
                    {"file": "commercial", "name": "VSTRUCK"},
                    {"file": "commercial", "name": "STRUCK"},
                    {"file": "commercial", "name": "MTRUCK"},
                ],
                "excluded_links": ["is_toll_truck", "is_hov"],
                "value_of_time": 37.87,  # $ / hr
                "operating_cost": 31.28,  # cents / mile
                "toll": "@bridgetoll_sml",
                "skims": [
                    "time",
                    "dist",
                    "freeflowtime",
                    "bridgetoll_vsm",
                    "bridgetoll_sml",
                    "bridgetoll_med",
                ],
            },
            {  # 4
                "name": "lrgtrk",
                "description": "large truck",
                "mode": "l",
                "demand": [
                    {"file": "commercial", "name": "CTRUCK", "factor": 2.0},
                ],
                "excluded_links": ["is_toll_truck", "is_auto_only"],
                "value_of_time": 37.87,  # $ / hr
                "operating_cost": 31.28,  # cents / mile
                "toll": "@bridgetoll_lrg",
                "passenger_car_equivalent": 2.0,
                "skims": [
                    "time",
                    "dist",
                    "freeflowtime",
                    "bridgetoll_lrg",
                ],
            },
            {  # 5
                "name": "datoll",
                "description": "drive alone toll",
                "mode": "D",
                "excluded_links": ["is_hov"],
                "value_of_time": 18.93,  # $ / hr
                "operating_cost": 17.23,  # cents / mile
                "toll": "@toll_da",
                "skims": [
                    "time",
                    "dist",
                    "freeflowtime",
                    "bridgetoll_da",
                    "valuetoll_da",
                    "tolldist",
                ],
            },
            {  # 6
                "name": "sr2toll",
                "description": "shared ride 2 toll",
                "mode": "E",
                "demand": [
                    {
                        "file": "household",
                        "name": "SR2_PAY_{period}",
                        "factor": 1 / 1.75,
                    },
                    {"file": "air_passenger", "name": "SR2TOLL"},
                    {"file": "internal_external", "name": "SR2TOLL"},
                ],
                "excluded_links": ["is_hov3"],
                "value_of_time": 18.93,  # $ / hr
                "operating_cost": 17.23,  # cents / mile
                "toll": "@toll_sr2",
                "toll_factor": 1 / 1.75,
                "skims": [
                    "time",
                    "dist",
                    "freeflowtime",
                    "bridgetoll_sr2",
                    "valuetoll_sr2",
                    "hovdist",
                    "tolldist",
                ],
            },
            {  # 7
                "name": "sr3toll",
                "description": "shared ride 3+ toll",
                "mode": "F",
                "demand": [
                    {
                        "file": "household",
                        "name": "SR3_PAY_{period}",
                        "factor": 1 / 2.5,
                    },
                    {"file": "air_passenger", "name": "SR3TOLL"},
                    {"file": "internal_external", "name": "SR3TOLL"},
                ],
                "excluded_links": [],
                "value_of_time": 18.93,  # $ / hr
                "operating_cost": 17.23,  # cents / mile
                "toll": "@toll_sr3",
                "toll_factor": 1 / 2.5,
                "skims": [
                    "time",
                    "dist",
                    "freeflowtime",
                    "bridgetoll_sr3",
                    "valuetoll_sr3",
                    "hovdist",
                    "tolldist",
                ],
            },
            {  # 8
                "name": "trucktoll",
                "description": "truck toll",
                "mode": "T",
                "demand": [
                    {"file": "commercial", "name": "VSTRUCKTOLL"},
                    {"file": "commercial", "name": "STRUCKTOLL"},
                    {"file": "commercial", "name": "MTRUCKTOLL"},
                ],
                "excluded_links": ["is_hov"],
                "value_of_time": 37.87,  # $ / hr
                "operating_cost": 31.28,  # cents / mile
                "toll": "@toll_sml",
                "skims": [
                    "time",
                    "dist",
                    "freeflowtime",
                    "bridgetoll_vsm",
                    "bridgetoll_sml",
                    "bridgetoll_med",
                    "valuetoll_vsm",
                    "valuetoll_sml",
                    "valuetoll_med",
                ],
            },
            {  # 9
                "name": "lrgtrktoll",
                "description": "large truck toll",
                "mode": "L",
                "demand": [
                    {"file": "commercial", "name": "CTRUCKTOLL", "factor": 2.0},
                ],
                "excluded_links": ["is_auto_only", "is_hov"],
                "value_of_time": 37.87,  # $ / hr
                "operating_cost": 31.28,  # cents / mile
                "passenger_car_equivalent": 2.0,
                "toll": "@toll_lrg",
                "skims": [
                    "time",
                    "dist",
                    "freeflowtime",
                    "bridgetoll_lrg",
                    "valuetoll_lrg",
                ],
            },
        ]
        for period in self.config.model.periods:
            scenario_id = self.config.emme.scenario_ids[period]
            scenario = self._emmebank.scenario(scenario_id)
            with self._setup(scenario):
                # TODO: skipping import_demand until integrated in workflow
                # self._import_demand(scenario, period, traffic_config, msa_iteration)
                if msa_iteration > 1:  # skip for first global iteration
                    # TODO: hardcoded these for now, to come from config
                    vot = 18.93
                    operating_cost = 17.23
                    maz_assign = AssignMAZSPDemand(
                        controller,
                        modeller,
                        scenario,
                        root,
                        vot,
                        operating_cost,
                        modes=["x", "d"],
                    )
                    maz_assign.run()
                self._assign_and_skim(period, scenario, traffic_config)
                for class_config in traffic_config:
                    self._set_intrazonal_values(
                        period, scenario, class_config["name"], class_config["skims"]
                    )
                self._export_skims(period, scenario)

    @_context
    def _setup(self, scenario):
        """Setup and teardown"""
        self._matrix_cache = _emme_tools.MatrixCache(scenario)
        self._skim_matrices = []
        with self._emme_manager.logbook_trace("Traffic assignments"):
            try:
                yield
            finally:
                self._matrix_cache.clear()
                self._matrix_cache = None

    def _import_demand(self, scenario, period, traffic_config, msa_iteration):
        root = _join("..", "demand_matrices", "highway")
        # TODO: set file paths in config
        omx_files = {
            "household": _emme_tools.OMX(
                _join(root, "household", f"TAZ_Demand_{period}.omx")
            ),
            "air_passenger": _emme_tools.OMX(
                _join(root, "air_passenger", f"tripsAirPax{period}.omx")
            ),
            "internal_external": _emme_tools.OMX(
                _join(root, "internal_external", f"tripsIx{period}.omx")
            ),
            "commercial": _emme_tools.OMX(
                _join(root, "commercial", f"tripstrk{period}.omx")
            ),
        }

        def load_demand(file_config):
            file_ref = file_config["file"]
            name = file_config["name"]
            factor = file_config.get("factor")
            demand = omx_files[file_ref][name].read()
            if factor is not None:
                demand = factor * demand
            shape = demand.shape
            # pad external zone values with 0
            if shape != (num_zones, num_zones):
                demand = numpy.pad(
                    demand, ((0, num_zones - shape[0]), (0, num_zones - shape[1]))
                )

        try:
            num_zones = len(scenario.zone_numbers)
            for file_obj in omx_files.values():
                file_obj.open()
            for class_config in traffic_config:
                demand = load_demand(class_config["demand"][0])
                for file_config in class_config["demand"[1:]]:
                    demand = demand + load_demand(file_config)
            demand_name = f'{period}_{traffic_config["name"]}'
            if msa_iteration == 1:
                if emmebank.matrix(demand_name):
                    emmebank.delete_matrix(emmebank.matrix(demand_name))
                ident = emmebank.available_matrix_identifier("FULL")
                matrix = emmebank.create_matrix(ident)
                matrix.name = demand_name
                matrix.description = f'{period} {traffic_config["description"]} demand'
            else:
                matrix = emmebank.matrix(demand_name)
                prev_demand = matrix.get_numpy_data(scenario.id)
                demand = prev_demand + (1.0 / msa_iteration) * (demand - prev_demand)
            matrix.set_numpy_data(demand, scenario.id)
        finally:
            for file_obj in omx_files.values():
                file_obj.close()

    # def validate_inputs(self):

    # def trace_od_pair(self):

    def _assign_and_skim(self, period, scenario, traffic_config):
        modeller = self._modeller
        traffic_assign = modeller.tool(
            "inro.emme.traffic_assignment.sola_traffic_assignment"
        )
        matrix_calc = modeller.tool("inro.emme.matrix_calculation.matrix_calculator")
        create_attribute = modeller.tool(
            "inro.emme.data.extra_attribute.create_extra_attribute"
        )
        net_calc = _emme_tools.NetworkCalculator(scenario, modeller)

        # prepare network attributes for skimming
        create_attribute(
            "LINK", "@hov_length", "length HOV lanes", overwrite=True, scenario=scenario
        )
        net_calc(
            "@hov_length",
            "length * (@useclass >= 2 && @useclass <= 3)",
        )
        create_attribute(
            "LINK", "@toll_length", "length tolls", overwrite=True, scenario=scenario
        )
        # non-bridge toll facilities
        net_calc(
            "@toll_length",
            f"length * (@valuetoll_da > 0",
        )
        # create Emme format specification with traffic class definitions
        # and path analysis (skims)
        assign_spec = self._base_spec()
        for class_config in traffic_config:
            emme_class_spec = self._prepare_traffic_class(
                class_config, scenario, period
            )
            assign_spec["classes"].append(emme_class_spec)

        # Run assignment
        traffic_assign(assign_spec, scenario, chart_log_interval=2)

        # Subtrack the non-time costs from generalize cost to get the raw travel time skim
        for emme_class_spec in assign_spec["classes"]:
            od_travel_times = emme_class_spec["results"]["od_travel_times"][
                "shortest_paths"
            ]
            if od_travel_times is not None:
                cost = emme_class_spec["path_analyses"][0]["results"]["od_values"]
                factor = emme_class_spec["generalized_cost"]["perception_factor"]
                gencost_data = self._matrix_cache.get_data(od_travel_times)
                cost_data = self._matrix_cache.get_data(cost)
                time_data = gencost_data - (factor * cost_data)
                self._matrix_cache.set_data(od_travel_times, time_data)

    def _set_intrazonal_values(self, period, class_name, skims, scenario):
        for skim_name in skims:
            name = f"{period}_{class_name}_{skim_name}"
            matrix = self._emmebank.matrix(name)
            if skim_name in ["time", "distance", "freeflowtime", "hovdist", "tolldist"]:
                data = self._matrix_cache.get_data(matrix)
                # NOTE: sets values for external zones as well
                _numpy.fill_diagonal(data, _numpy.inf)
                data[numpy.diag_indices_from(data)] = 0.5 * numpy.nanmin(data, 1)
                self._matrix_cache.set_data(matrix, data)
            # TODO: double check fill zero diagonals for toll
            # else:
            #     data = self._matrix_cache.get_data(matrix)
            #     numpy.fill_diagonal(data, 0.0)

    def _export_skims(self, period, scenario):
        root = _dir(_dir(self._emmebank.path))
        omx_file_path = _join(root, f"{period}_traffic_skims.omx")
        with _emme_tools.OMX(
            omx_file_path, "w", scenario, matrix_cache=self._matrix_cache
        ) as omx_file:
            omx_file.write_matrices(self._skim_matrices)
        self._skim_matrices = []

    def _base_spec(self, background_traffic=True):
        base_spec = {
            "type": "SOLA_TRAFFIC_ASSIGNMENT",
            "background_traffic": None,
            "classes": [],
            "stopping_criteria": {
                "max_iterations": self._max_iterations,
                "best_relative_gap": 0.0,
                "relative_gap": self._relative_gap,
                "normalized_gap": 0.0,
            },
            "performance_settings": {"number_of_processors": self._num_processors},
        }
        # TODO: consider background transit vehicles per-period
        #       (assignment period length) PCE?
        # NOTE: mazmazvol as background traffic in link.data1 ("ul1")
        if background_traffic:
            base_spec["background_traffic"] = {
                "link_component": "ul1",
                "turn_component": None,
                "add_transit_vehicles": False,
            }
        return base_spec

    def _prepare_traffic_class(self, class_config, scenario, period):
        modeller = self._modeller
        create_attribute = modeller.tool(
            "inro.emme.data.extra_attribute.create_extra_attribute"
        )
        create_matrix = modeller.tool("inro.emme.data.matrix.create_matrix")
        net_calc = _emme_tools.NetworkCalculator(scenario, modeller)

        name = class_config["name"]
        name_lower = name.lower()
        description = class_config["description"]
        operating_cost = class_config["operating_cost"]
        toll = class_config["toll"]
        toll_factor = class_config.get("toll_factor")
        skims = class_config.get("skims", [])

        link_cost = f"@cost_{name_lower}"
        create_attribute("LINK", link_cost, overwrite=True, scenario=scenario)
        if toll_factor is None:
            cost_expression = f"length * {operating_cost} + {toll}"
        else:
            cost_expression = f"length * {operating_cost} + {toll} * {toll_factor}"
        net_calc(link_cost, cost_expression)

        link_flow = create_attribute(
            "LINK",
            f"@flow_{name_lower}",
            f"{period} {description} link volume",
            0,
            overwrite=True,
            scenario=scenario,
        )
        skim_matrices = []
        class_analysis = []
        # time skim is special case, get total generialized cost and link costs
        # then calculate time = gen_cost - (oper_cost + toll)
        if "time" in skims:
            # total generalized cost results from od_travel_time
            od_travel_times = f"{period}_{name}_time"
            skim_matrices.append(od_travel_times)
            # also get non-time costs
            total_cost = f"{period}_{name}_cost"
            skim_matrices.append(total_cost)
            class_analysis.append(self._analysis_spec(total_cost, link_cost))
            skims.remove("time")
        else:
            od_travel_times = None
            total_cost = None

        for skim_type in class_config["skims"]:
            if "_" in skim_type:
                skim_type, group = skim_type.split("_")
            else:
                group = ""
            analysis_link = {
                "dist": "length",  # NOTE: length must be in miles
                "hovdist": "@hov_length",
                "tolldist": "@toll_length",
                "freeflowtime": "@free_flow_time",
                "bridgetoll": f"@bridgetoll_{group}",
                "valuetoll": f"@valuetoll_{group}",
            }
            link_attr = analysis_link[skim_type]
            if group:
                matrix_name = f"{period}_{name}_{skim_type}{group}"
            else:
                matrix_name = f"{period}_{name}_{skim_type}"
            class_analysis.append(self._analysis_spec(matrix_name, link_attr))
            skim_matrices.append(matrix_name)

        # create / initialize skim matrices
        for matrix_name in skim_matrices:
            if self._emmebank.matrix(matrix_name):
                self._emmebank.delete_matrix(self._emmebank.matrix(matrix_name))
            matrix = create_matrix("mf", matrix_name, scenario=scenario, overwrite=True)
            self._skim_matrices.append(matrix)

        emme_class_spec = {
            "mode": class_config["mode"],
            "demand": f'mf"{period}_{name}"',
            "generalized_cost": {
                "link_costs": link_cost,  # cost in $0.01
                "perception_factor": 0.6
                / class_config["value_of_time"],  # $/hr -> min/$0.01
            },
            "results": {
                "link_volumes": link_flow.id,
                "od_travel_times": {"shortest_paths": od_travel_times},
            },
            "path_analyses": class_analysis,
        }
        return emme_class_spec

    @staticmethod
    def _analysis_spec(matrix_name, link_attr):
        analysis_spec = {
            "link_component": link_attr,
            "turn_component": None,
            "operator": "+",
            "selection_threshold": {"lower": None, "upper": None},
            "path_to_od_composition": {
                "considered_paths": "ALL",
                "multiply_path_proportions_by": {
                    "analyzed_demand": False,
                    "path_value": True,
                },
            },
            "results": {
                "od_values": matrix_name,
                "selected_link_volumes": None,
                "selected_turn_volumes": None,
            },
        }
        return analysis_spec


class AssignMAZSPDemand(_Component):
    """TODO: docsting"""

    def __init__(
        self, controller, modeller, scenario, root, vot, operating_cost, modes
    ):
        """docstring for traffic assignment run"""
        super().__init__(controller)
        self._num_processors = self.config.emme.number_of_processors
        self._modeller = modeller
        self._scenario = scenario
        self._root_dir = root
        self._vot = vot
        self._operating_cost = operating_cost
        self._modes = modes
        # bins: performance parameter, number of distance bins
        #       for shortest path calculation
        self._bin_edges = []
        self._debug_maz_pairs = None  # run for small number of O-D pairs to test

    def run(self):
        root_dir = r"..\demand_matrices\highway\maz_demand"
        with self._setup():
            # Step 1:
            self._prepare_network()
            # Step 2: Import
            county_sets = {
                1: ["San Francisco", "San Mateo", "Santa Clara"],
                2: ["Alameda", "Contra Costa"],
                3: ["Solano", "Napa", "Sonoma", "Marin"],
            }
            for i in range(1, 4):
                mazseq = self._get_county_mazs(county_sets[i])
                omx_file_path = _join(root_dir, f"auto_AM_MAZ_AUTO_{i}_AM.omx")
                with _emme_tools.OMX(omx_file_path, "r") as omx_file:
                    demand_array = omx_file.read_array("/matrices/M0")
                self._process_demand(demand_array, mazseq)
                del demand_array
            demand_bins = self._group_demand()
            for i, demand_group in enumerate(demand_bins):
                self._find_roots_and_leaves(demand_group["demand"])
                self._run_shortest_path(i, demand_group["dist"])
                self._assign_flow(i, demand_group["demand"])
            self._save_attr_values("LINK", ["@link_cost_maz"])

    @_context
    def _setup(self):
        self._mazs = None
        self._demand = _defaultdict(lambda: [])
        self._max_dist = 0
        self._network = None
        self._root_index = None
        self._leaf_index = None
        try:
            yield
        finally:
            self._mazs = None
            self._demand = None
            self._network = None
            self._root_index = None
            self._leaf_index = None
            # TODO: delete attributes @link_cost_maz, @maz_root, @maz_leaf

    def _prepare_network(self):
        modeller = self._modeller
        create_attribute = modeller.tool(
            "inro.emme.data.extra_attribute.create_extra_attribute"
        )
        shortest_paths_tool = modeller.tool(
            "inro.emme.network_calculation.shortest_path"
        )
        net_calc = _emme_tools.NetworkCalculator(self._scenario, modeller)
        create_attribute(
            "LINK",
            "@link_cost_maz",
            "total link cost for MAZ-MAZ SP assign",
            overwrite=True,
            scenario=self._scenario,
        )
        create_attribute("NODE", "@maz_root", overwrite=True, scenario=self._scenario)
        create_attribute("NODE", "@maz_leaf", overwrite=True, scenario=self._scenario)
        if self._scenario.has_traffic_results:
            time_attr = "timau"
        else:
            time_attr = "@free_flow_time"
        vot = self._vot
        op_cost = self._operating_cost
        net_calc("@link_cost_maz", f"{time_attr} + 0.6 / {vot} * (length * {op_cost})")
        net_calc("ul1", "0")
        self._network = self._scenario.get_partial_network(
            ["NODE", "LINK"], include_attributes=False
        )
        self._network.create_attribute("LINK", "temp_flow")
        attrs_to_read = [
            ("NODE", ["@mazseq", "x", "y", "#county"]),
            ("LINK", ["@link_cost_maz"]),
        ]
        for domain, attrs in attrs_to_read:
            self._read_attr_values(domain, attrs)

    def _get_county_mazs(self, counties):
        network = self._network
        # NOTE: every maz must have a valid #county
        if self._mazs is None:
            self._mazs = _defaultdict(lambda: [])
            for node in network.nodes():
                if node["@mazseq"]:
                    self._mazs[node["#county"]].append(node)
        mazs = []
        for county in counties:
            mazs.extend(self._mazs[county])
        return sorted(mazs, key=lambda n: n["@mazseq"])

    def _process_demand(self, data, mazseq):
        # report.append(len(mazseq), data.shape)
        origins, destinations = data.nonzero()
        for o, d in zip(origins, destinations):
            # skip intra-maz demand
            if p == q:
                continue
            p = mazseq[o]
            q = mazseq[d]
            dist = _sqrt((q.x - p.x) ** 2 + (q.y - p.y) ** 2)
            if dist > self._max_dist:
                self._max_dist = dist
            self._demand[p].append({"p": p, "q": q, "dem": data[o][d], "dist": dist})

    def _group_demand(self):
        # group demand from same origin into distance bins by furthest
        # distance destination to limit shortest path search radius
        # using linear grouping (based on maximum dist)
        # AND for smallest group split into counties
        # TODO: report on : size of bins: num origins, num demand pairs
        #       runtime for each sp call
        counties = {
            "San Francisco": 1,
            "San Mateo": 2,
            "Santa Clara": 3,
            "Alameda": 4,
            "Contra Costa": 5,
            "Solano": 6,
            "Napa": 7,
            "Sonoma": 8,
            "Marin": 9,
        }
        # NOTE: < 1.0 mile divided by county, > 0.5 divided by distance
        # demand =
        bin_edges = [0.0, 1.0, 1.5, 2.0, 4.0, 6.0, 10.0, 16.0, self._max_dist / 5280]
        demand = [{"dist": d, "demand": []} for i, d in enumerate(bin_edges[1:])]
        # demand_county = [{"dist": 0.5, "demand": []} for c, i in counties.items()]
        count = 0
        for data in self._demand.values():
            max_dist = max(d["dist"] for d in data) / 5280.0
            for i, b in enumerate(bin_edges[1:]):
                if max_dist < b:
                    demand[i]["demand"].extend(data)
                    break
            count += len(data)
            # for testing, evaluate a small number of maz pairs
            if self._debug_maz_pairs and count > self._debug_maz_pairs:
                break
        demand = [x for x in demand if x["demand"]]
        # for x in demand:
        #     print(x['dist'], len(x["demand"]))
        return demand

    def _find_roots_and_leaves(self, demand):
        network = self._network
        attrs_to_init = [("NODE", ["@maz_root", "@maz_leaf"]), ("LINK", ["maz_cost"])]
        for domain, attrs in attrs_to_init:
            for name in attrs:
                if name in network.attributes(domain):
                    network.delete_attribute(domain, name)
                network.create_attribute(domain, name)
        root_maz_ids = {}
        leaf_maz_ids = {}
        for data in demand:
            p, q = data["p"], data["q"]
            root_maz_ids[p.number] = p["@maz_root"] = p["@mazseq"]
            leaf_maz_ids[q.number] = q["@maz_leaf"] = q["@mazseq"]
        self._root_index = {p: i for i, p in enumerate(sorted(root_maz_ids.keys()))}
        self._leaf_index = {q: i for i, q in enumerate(sorted(leaf_maz_ids.keys()))}
        self._save_attr_values("NODE", ["@maz_root", "@maz_leaf"])
        # forbid egress from MAZ nodes which are not demand roots /
        #        access to MAZ nodes which are not demand leafs
        network.copy_attribute("LINK", "@link_cost_maz", "temp_link_cost")
        for node in network.nodes():
            if node["@mazseq"]:
                for link in node.outgoing_links():
                    link["temp_link_cost"] = 1e20
                for link in node.incoming_links():
                    link["temp_link_cost"] = 1e20
        for node_id in root_maz_ids.keys():
            node = network.node(node_id)
            for link in node.outgoing_links():
                link["temp_link_cost"] = link["@link_cost_maz"]
        for node_id in leaf_maz_ids.keys():
            node = network.node(node_id)
            for link in node.incoming_links():
                link["temp_link_cost"] = link["@link_cost_maz"]
        self._save_attr_values("LINK", ["temp_link_cost"], ["@link_cost_maz"])
        network.delete_attribute("LINK", "temp_link_cost")

    def _run_shortest_path(self, bin_no, max_radius):
        shortest_paths_tool = self._modeller.tool(
            "inro.emme.network_calculation.shortest_path"
        )
        # TODO: temp binary path files ?
        # TODO: path file directory
        # TODO: review buffer size
        max_radius = max_radius * 5280 + 100  # add some buffer for rounding error
        shortest_paths_tool(
            modes=self._modes,
            roots_attribute="@maz_root",
            leafs_attribute="@maz_leaf",
            link_cost_attribute="@link_cost_maz",
            num_processors=self._num_processors,
            direction="FORWARD",
            paths_file=_join(self._root_dir, f"shortest_paths_{bin_no}.ebp"),
            export_format_paths="BINARY",
            through_leaves=False,
            through_centroids=False,
            max_radius=max_radius,
        )

    def _assign_flow(self, bin_no, demand):
        # TODO: add logging of process and debugging
        # NOTE: can add report details with log levels
        with open(
            _join(self._root_dir, f"shortest_paths_{bin_no}.ebp"), "rb"
        ) as paths_file:
            # read first 4 integers from file
            header = _array.array("I")
            header.fromfile(paths_file, 4)
            file, direction, roots_nb, leafs_nb = header
            # read list of positions by p, q index, for list of path node IDs in file
            path_indicies = _array.array("I")
            path_indicies.fromfile(paths_file, roots_nb * leafs_nb + 1)
            # for all p-q pairs with demand, load path from file
            assigned = 0
            not_assigned = 0
            offset = roots_nb * leafs_nb + 1 + 4
            bytes_read = offset
            for data in demand:
                # get file position based on p, q index
                p_index = self._root_index[data["p"].number]
                q_index = self._leaf_index[data["q"].number]
                index = p_index * leafs_nb + q_index
                start = path_indicies[index]
                end = path_indicies[index + 1]

                # no path found, likely an error
                if start == end:
                    not_assigned += 1
                    continue
                paths_file.seek((start + offset) * 4)
                # load sequence of Node IDs which define the path
                path = _array.array("I")
                path.fromfile(paths_file, end - start)
                bytes_read += end - start
                path_iter = iter(path)
                i_node = next(path_iter)
                for j_node in path_iter:
                    link = self._network.link(i_node, j_node)
                    link["temp_flow"] += data["dem"]
                    i_node = j_node
                assigned += 1

        # print(
        #     "bin %s, total %s, assign %s, not assign %s, bytes %s"
        #     % (bin_no, len(demand), assigned, not_assigned, bytes_read))
        self._save_attr_values("LINK", ["temp_flow"], ["data1"])

    def _read_attr_values(self, domain, src_names, dst_names=None):
        self._copy_attr_values(
            domain, self._scenario, self._network, src_names, dst_names
        )

    def _save_attr_values(self, domain, src_names, dst_names=None):
        self._copy_attr_values(
            domain, self._network, self._scenario, src_names, dst_names
        )

    def _copy_attr_values(self, domain, src, dst, src_names, dst_names=None):
        if dst_names is None:
            dst_names = src_names
        values = src.get_attribute_values(domain, src_names)
        dst.set_attribute_values(domain, dst_names, values)
