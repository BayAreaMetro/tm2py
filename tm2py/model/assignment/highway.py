"""Performs equalibrium traffic assignment and generates resulting skims.


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
    and not-value-toll-eligible classes, with the skims noted below.
        (1) da, mode "d", drive alone, no value toll
                skims: time, dist, freeflowtime, bridgetoll_da
        (2) sr2, mode "e", shared ride 2, no value toll
                skims: time, dist, freeflowtime, bridgetoll_sr2, hovdist
        (3) sr3, mode "f", shared ride 3+, no value toll
                skims: time, dist, freeflowtime, bridgetoll_sr3, hovdist
        (4) truck, mode "t", very small, small, and medium trucks, no value toll
                skims: time, dist, freeflowtime,
                       bridgetollvsm, bridgetollsml, bridgetollmed
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
                       bridgetollvsm, bridgetollsml, bridgetollmed,
                       valuetollvsm, valuetollsml, valuetollmed
        (10) lrgtrktoll, mode "L", large trucks, value toll eligible
                skims: time, dist, freeflowtime,

    Note that the "truck" and "trucktoll" classes combine very small, small and medium trucks

    The skims are stored in the Emmebank and exported to OMX with names with the following
    convention:
        period_class_skim

    Four types of trips are assigned:
        (a) personal, inter-regional travel, file "household";
        (b) personal, intra-regional travel, file "internal_external";
        (c) commercial travel, file "commercial";
        (d) air passenger travel, file "air_passenger";

    Separate trip tables are read in by the script for each of these travel types.

The Emme network must have the following attributes set:
    - "length" in feet
    - "type"  the facility type
    - "vdf", volume delay function (volume delay functions must also be setup)
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

from contextlib import contextmanager as _context
import os as _os
import numpy as _numpy

from tm2py.core.component import Component as _Component, Controller as _Controller
import tm2py.core.emme as _emme_tools
from tm2py.model.assignment.highwaymaz import AssignMAZSPDemand as _AssignMAZSPDemand


_join, _dir = _os.path.join, _os.path.dirname


class HighwayAssignment(_Component):
    """docstring for traffic assignment"""

    def __init__(self, controller: _Controller, root_dir: str):
        """Run highway assignment and skims.

        Args:
            controller: parent Controller object
            root_dir (str): root directory containing Emme project, demand matrix root directory.
        """
        super().__init__(controller)
        self._num_processors = _emme_tools.parse_num_processors(
            self.config.emme.number_of_processors
        )
        if root_dir is None:
            self._root_dir = _os.getcwd()
        self._matrix_cache = None
        self._emme_manager = None
        self._emmebank = None
        self._skim_matrices = []
        self._omx_files = {}

    @property
    def _modeller(self):
        return self._emme_manager.modeller()

    def run(self):
        """Run highway assignment and skims."""
        project_path = _join(self._root_dir, "mtc_emme", "mtc_emme.emp")
        self._emme_manager = _emme_tools.EmmeProjectCache()
        project = self._emme_manager.project(project_path)
        self._modeller = self._emme_manager.init_modeller(project)
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
                "skims": ["time", "dist", "freeflowtime", "bridgetoll_da"],
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
                # NOTE to consider: should import and avg demand be separate step?
                self._import_demand(scenario, period, traffic_config, msa_iteration)
                # skip for first global iteration
                if msa_iteration > 1:
                    modes = ["x", "d"]
                    maz_assign = _AssignMAZSPDemand(
                        self.controller, scenario, period, modes
                    )
                    maz_assign.run()
                else:
                    # Initialize ul1 to 0 (MAZ-MAZ background traffic)
                    net_calc = _emme_tools.NetworkCalculator(scenario)
                    net_calc("ul1", "0")
                self._assign_and_skim(period, scenario, traffic_config)
                self._export_skims(period, scenario)

    @_context
    def _setup(self, scenario):
        self._matrix_cache = _emme_tools.MatrixCache(scenario)
        self._skim_matrices = []
        with self._emme_manager.logbook_trace("Traffic assignments"):
            try:
                yield
            finally:
                self._matrix_cache.clear()
                self._matrix_cache = None

    def _import_demand(self, scenario, period, traffic_config, msa_iteration):
        emmebank = scenario.emmebank
        num_zones = len(scenario.zone_numbers)
        with self._open_omx_files(period):
            for class_config in traffic_config:
                demand = self._read_demand(class_config["demand"][0], num_zones)
                for file_config in class_config["demand"[1:]]:
                    demand = demand + self._read_demand(file_config, num_zones)
            demand_name = f'{period}_{traffic_config["name"]}'
            matrix = emmebank.matrix(demand_name)
            if msa_iteration == 1:
                if matrix:
                    emmebank.delete_matrix(matrix)
                ident = emmebank.available_matrix_identifier("FULL")
                matrix = emmebank.create_matrix(ident)
                matrix.name = demand_name
                matrix.description = f'{period} {traffic_config["description"]} demand'
            else:
                prev_demand = matrix.get_numpy_data(scenario.id)
                demand = prev_demand + (1.0 / msa_iteration) * (demand - prev_demand)
            matrix.set_numpy_data(demand, scenario.id)

    @_context
    def _open_omx_files(self, period):
        root = _join(self._root_dir, "demand_matrices", "highway")
        # TODO: set file paths in config
        self._omx_files = {
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
        try:
            for file_obj in self._omx_files.values():
                file_obj.open()
            yield
        finally:
            for file_obj in self._omx_files.values():
                file_obj.close()
            self._omx_files = {}

    def _read_demand(self, file_config, num_zones):
        file_ref = file_config["file"]
        name = file_config["name"]
        factor = file_config.get("factor")
        demand = self._omx_files[file_ref][name].read()
        if factor is not None:
            demand = factor * demand
        shape = demand.shape
        # pad external zone values with 0
        if shape != (num_zones, num_zones):
            demand = _numpy.pad(
                demand, ((0, num_zones - shape[0]), (0, num_zones - shape[1]))
            )
        return demand

    # def validate_inputs(self):

    # def trace_od_pair(self):

    def _assign_and_skim(self, period, scenario, traffic_config):
        traffic_assign = self._modeller.tool(
            "inro.emme.traffic_assignment.sola_traffic_assignment"
        )
        create_attribute = self._modeller.tool(
            "inro.emme.data.extra_attribute.create_extra_attribute"
        )
        net_calc = _emme_tools.NetworkCalculator(scenario)

        # prepare network attributes for skimming
        create_attribute(
            "LINK", "@hov_length", "length HOV lanes", overwrite=True, scenario=scenario
        )
        net_calc("@hov_length", "length * (@useclass >= 2 && @useclass <= 3)")
        create_attribute(
            "LINK", "@toll_length", "length tolls", overwrite=True, scenario=scenario
        )
        # non-bridge toll facilities
        net_calc("@toll_length", "length * (@valuetoll_da > 0)")
        # create Emme format specification with traffic class definitions
        # and path analyses (skims)
        assign_spec = self._base_spec()
        for class_config in traffic_config:
            emme_class_spec = self._prepare_traffic_class(
                class_config, scenario, period
            )
            assign_spec["classes"].append(emme_class_spec)

        # Run assignment
        traffic_assign(assign_spec, scenario, chart_log_interval=1)

        # Subtrack the non-time costs from generalize cost to get the raw travel time skim
        for emme_class_spec in assign_spec["classes"]:
            self._calc_time_skim(emme_class_spec)
        # Set intra-zonals for time and dist to be 1/2 nearest neighbour
        for class_config in traffic_config:
            self._set_intrazonal_values(
                period, class_config["name"], class_config["skims"]
            )

    def _calc_time_skim(self, emme_class_spec):
        od_travel_times = emme_class_spec["results"]["od_travel_times"]["shortest_paths"]
        if od_travel_times is not None:
            # Total link costs is always the first analysis
            cost = emme_class_spec["path_analyses"][0]["results"]["od_values"]
            factor = emme_class_spec["generalized_cost"]["perception_factor"]
            gencost_data = self._matrix_cache.get_data(od_travel_times)
            cost_data = self._matrix_cache.get_data(cost)
            time_data = gencost_data - (factor * cost_data)
            self._matrix_cache.set_data(od_travel_times, time_data)

    def _set_intrazonal_values(self, period, class_name, skims):
        for skim_name in skims:
            name = f"{period}_{class_name}_{skim_name}"
            matrix = self._emmebank.matrix(name)
            if skim_name in ["time", "distance", "freeflowtime", "hovdist", "tolldist"]:
                data = self._matrix_cache.get_data(matrix)
                # NOTE: sets values for external zones as well
                _numpy.fill_diagonal(data, _numpy.inf)
                data[_numpy.diag_indices_from(data)] = 0.5 * _numpy.nanmin(data, 1)
                self._matrix_cache.set_data(matrix, data)

    def _export_skims(self, period, scenario):
        root = _dir(_dir(self._emmebank.path))
        omx_file_path = _join(root, f"{period}_traffic_skims.omx")
        with _emme_tools.OMX(
            omx_file_path, "w", scenario, matrix_cache=self._matrix_cache
        ) as omx_file:
            omx_file.write_matrices(self._skim_matrices)
        self._skim_matrices = []
        self._matrix_cache.clear()

    def _base_spec(self):
        relative_gap = self.config.emme.highway_assignment.relative_gap
        max_iterations = self.config.emme.highway_assignment.max_iterations
        # NOTE: mazmazvol as background traffic in link.data1 ("ul1")
        #       to consider: background transit vehicles per-period
        #       (assignment period length) PCE?
        base_spec = {
            "type": "SOLA_TRAFFIC_ASSIGNMENT",
            "background_traffic": {
                "link_component": "ul1",
                "turn_component": None,
                "add_transit_vehicles": False,
            },
            "classes": [],
            "stopping_criteria": {
                "max_iterations": max_iterations,
                "best_relative_gap": 0.0,
                "relative_gap": relative_gap,
                "normalized_gap": 0.0,
            },
            "performance_settings": {"number_of_processors": self._num_processors},
        }
        return base_spec

    def _prepare_traffic_class(self, class_config, scenario, period):
        create_attribute = self._modeller.tool(
            "inro.emme.data.extra_attribute.create_extra_attribute"
        )
        net_calc = _emme_tools.NetworkCalculator(scenario)

        name = class_config["name"]
        name_lower = name.lower()
        op_cost = class_config["operating_cost"]
        toll = class_config["toll"]
        toll_factor = class_config.get("toll_factor")
        link_cost = f"@cost_{name_lower}"
        create_attribute("LINK", link_cost, overwrite=True, scenario=scenario)
        if toll_factor is None:
            cost_expression = f'length * {op_cost} + {toll}'
        else:
            cost_expression = f'length * {op_cost} + {toll} * {toll_factor}'
        net_calc(link_cost, cost_expression)
        link_flow = create_attribute(
            "LINK",
            f"@flow_{name_lower}",
            f'{period} {class_config["description"]} link volume',
            0,
            overwrite=True,
            scenario=scenario,
        )

        class_analysis, od_travel_times = self._prepare_path_analyses(
            class_config["skims"], scenario, period, name, link_cost)
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

    def _prepare_path_analyses(self, skim_names, scenario, period, name, link_cost):
        create_matrix = self._modeller.tool("inro.emme.data.matrix.create_matrix")
        skim_names = skim_names[:]
        skim_matrices = []
        class_analysis = []
        # time skim is special case, get total generialized cost and link costs
        # then calculate time = gen_cost - (oper_cost + toll)
        if "time" in skim_names:
            # total generalized cost results from od_travel_time
            od_travel_times = f"{period}_{name}_time"
            skim_matrices.append(od_travel_times)
            # also get non-time costs
            total_cost = f"{period}_{name}_cost"
            skim_matrices.append(total_cost)
            class_analysis.append(self._analysis_spec(total_cost, link_cost))
            skim_names.remove("time")
        else:
            od_travel_times = None

        for skim_type in skim_names:
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

        return class_analysis, od_travel_times

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
