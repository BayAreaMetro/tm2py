"""Performs equalibrium traffic assignment and generates resulting skims.

The traffic assignment runs according to the list of assignment classes
in the controller.config. Each classes is specified using the following
schema. All items are required unless otherwise (no validation at present).
Note that some network preparation steps (such as the setting of link.modes)
are completed in the create_emme_network component.
    "name": short (e.g. 2-3 character) unique reference name for the class.
        used in attribute and matrix names
    "description": longer text used in attribute and matrix descriptions
    "emme_mode": single character mode, used to generate link.modes to
        identify subnetwork, generated from "exclued_links" keywords
    "demand": list of OMX file and matrix keyname references
        "file": reference name for relative file path of source OMX file
        "name": name of matrix in the OMX file, can include "{period}"
            placeholder
        "factor": optional, multiplicative factor to generate PCEs from
            trucks or convert person-trips to vehicle-trips for HOVs
    "excluded_links": list of keywords to identify links to exclude from
        this class' available subnetwork (generate link.modes)
        Options are:
            - "is_toll_da": has a value (non-bridge) toll for drive alone
            - "is_sr2": is reserved for shared ride 2+ (@useclass in 2,3)
            - "is_sr3": is reserved for shared ride 3+ (@useclass == 3)
            - "is_toll_sr2": has a value (non-bridge) toll for shared ride 2
            - "is_toll_sr3": has a value (non-bridge) toll for shared ride 3+
            - "is_toll_truck": has a value (non-bridge) toll for trucks
            - "is_auto_only": is reserved for autos (non-truck) (@useclass != 1)
    "value_of_time": value of time for this class in $ / hr
    "operating_cost": vehicle operating cost in cents / mile
    "toll": additional toll cost link attribute (in cents)
    "toll_factor": optional, factor to apply to toll values in cost calculation
    "pce": optional, passenger car equivalent to convert assigned demand in
        PCE units to vehicles for total assigned vehicle calculations
    "skims": list of skim matrices to generate
        Options are:
            "time": travel time only in minutes
            "dist": distance in miles
            "hovdist": distance on HOV (sr2 or sr3+) facilities
            "tolldist": distance on toll (@valuetoll_da > 0) facilities
            "freeflowtime": free flow travel time in minutes
            "bridgetoll_YY": bridge tolls, where YY is one of the class groups
            "valuetoll_YY": other, non-bridge tolls, where YY is one of the class groups

The available class groups for the skim / attribute names are:
"da", "sr2", "sr3", "vsm", sml", "med", "lrg"

Example single class config, as a Python dictionary:
    {
        "name": "da",
        "description": "drive alone",
        "emme_mode": "d",
        "demand": [
            {"file": "household", "name": "SOV_GP_{period}"},
            {"file": "air_passenger", "name": "DA"},
            {"file": "internal_external", "name": "DA"},
        ],
        "excluded_links": ["is_toll_da", "is_sr2"],
        "value_of_time": 18.93,  # $ / hr
        "operating_cost": 17.23,  # cents / mile
        "toll": "@bridgetoll_da",
        "skims": ["time", "dist", "freeflowtime", "bridgetoll_da"],
    }

Other relevant parameters from the config are
    emme.highway.relative_gap: target relative gap stopping criteria
    emme.highway.max_iterations: maximum iterations stopping criteria
    emme.highway.demand_files: mapping giving short names for relative
        paths to OMX files for demand import. Can use {period} placeholder
    emme.num_processors: number of processors as integer or "MAX" or "MAX-N"
    emme.scenario_ids: mapping of period ID to scenario number

The Emme network must have the following attributes available:
    Link:
    - "length" in feet
    - "type", the facility type
    - "vdf", volume delay function (volume delay functions must also be setup)
    - "@useclass", vehicle-class restrictions classification, auto-only, HOV only
    - "@free_flow_time", the free flow time (in minutes)
    - "@tollXX_YY", the toll for period XX and class subgroup (see truck
        class) named YY, used together with @tollbooth to generate @bridgetoll_YY
        and @valuetoll_YY
    - "@tollbooth", label to separate bridgetolls from valuetolls
    - modes: must be set on links and match the specified mode codes in
        the traffic config
    Node:
    - "@mazseq": the MAZ identifiers (used in the highwaymaz assignment)
    - "#county": the county name

 Network results:
    - "@bridgetoll_YY", the bridge toll for period XX and class subgroup
        (see truck class) named YY
    - "@valuetoll_YY", the "value", non-bridge toll for period XX and class
        subgroup (see truck class) named YY
    - @flow_YY: link PCE flows per class, where YY is the class name in the config
    - timau: auto travel time
    - volau: total assigned flow in PCE

 Notes:
    - Output matrices are in miles, minutes, and cents (2010 dollars) and are stored as real values;
    - Intrazonal distance/time is one half the distance/time to the nearest neighbor;
    - Intrazonal bridge and value tolls are assumed to be zero

The following are the default example setup which are specified
in the config.properties file.
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
        (4) trk, mode "t", very small, small, and medium trucks, no value toll
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
        (9) trktoll, mode "T", very small, small, and medium trucks, value toll eligible
                skims: time, dist, freeflowtime,
                       bridgetollvsm, bridgetollsml, bridgetollmed,
                       valuetollvsm, valuetollsml, valuetollmed
        (10) lrgtrktoll, mode "L", large trucks, value toll eligible
                skims: time, dist, freeflowtime,

    Note that the "truck" and "trucktoll" classes combine very small, small and
    medium trucks

    The skims are stored in the Emmebank and exported to OMX with names with
    the following convention:
        period_class_skim, e.g. am_da_bridgetoll_da

    Four types of trips are assigned:
        (a) personal, inter-regional travel, file "household";
        (b) personal, intra-regional travel, file "internal_external";
        (c) commercial travel, file "commercial";
        (d) air passenger travel, file "air_passenger";

    The trip tables are read in by the script for each of these
    travel types.
"""

from contextlib import contextmanager as _context
import os as _os

# from typing import List, Union, Any, Dict
import numpy as _numpy

from tm2py.core.component import Component as _Component, Controller as _Controller
import tm2py.core.emme as _emme_tools
from tm2py.model.assignment.highwaymaz import AssignMAZSPDemand as _AssignMAZSPDemand

_join, _dir = _os.path.join, _os.path.dirname


class HighwayAssignment(_Component):
    """Highway assignment and skims"""

    def __init__(self, controller: _Controller, root_dir: str = None):
        """Highway assignment and skims.

        Args:
            controller: parent Controller object
            root_dir (str): root directory containing Emme project, demand matrices
        """
        super().__init__(controller)
        self._num_processors = _emme_tools.parse_num_processors(
            self.config.emme.num_processors
        )
        if root_dir is None:
            self._root_dir = _os.getcwd()
        else:
            self._root_dir = root_dir
        self._matrix_cache = None
        self._emme_manager = None
        self._emmebank = None
        self._skim_matrices = []

    @property
    def _modeller(self):
        return self._emme_manager.modeller

    def run(self):
        """Run highway assignment and skims."""
        project_path = _join(self._root_dir, "mtc_emme", "mtc_emme.emp")
        self._emme_manager = _emme_tools.EmmeProjectCache()
        self._emme_manager.project(project_path)
        self._emmebank = self._modeller.emmebank
        # Run assignment and skims for all specified periods
        for period in self.config.periods:
            scenario_id = self.config.emme.scenario_ids[period["name"]]
            scenario = self._emmebank.scenario(scenario_id)
            with self._setup(scenario):
                # Import demand from specified OMX files
                # Will also MSA average demand if msa_iteration > 1
                import_demand = ImportDemand(
                    self.controller, self._root_dir, scenario, period["name"]
                )
                import_demand.run()
                # skip for first global iteration
                if self.controller.iteration > 1:
                    # non-auto mode x on MAZ connectors plus drive alone
                    # mode d, requires review after network creation workflow
                    modes = ["x", "d"]
                    maz_assign = _AssignMAZSPDemand(
                        self.controller, scenario, period["name"], modes
                    )
                    maz_assign.run()
                else:
                    # Initialize ul1 to 0 (MAZ-MAZ background traffic)
                    net_calc = _emme_tools.NetworkCalculator(scenario)
                    net_calc("ul1", "0")
                self._assign_and_skim(period["name"], scenario)
                if self.config.run.verify:
                    self.verify(period["name"], scenario)
                self._export_skims(period["name"], scenario)

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

    def _assign_and_skim(self, period, scenario):
        """Runs Emme SOLA assignment with path analyses (skims)."""
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
        for class_config in self.config.emme.highway.classes:
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
        for class_config in self.config.emme.highway.classes:
            self._set_intrazonal_values(
                period, class_config["name"], class_config["skims"]
            )

    def _calc_time_skim(self, emme_class_spec):
        """Cacluate the matrix skim time=gen_cost-per_fac*link_costs"""
        od_travel_times = emme_class_spec["results"]["od_travel_times"][
            "shortest_paths"
        ]
        if od_travel_times is not None:
            # Total link costs is always the first analysis
            cost = emme_class_spec["path_analyses"][0]["results"]["od_values"]
            factor = emme_class_spec["generalized_cost"]["perception_factor"]
            gencost_data = self._matrix_cache.get_data(od_travel_times)
            cost_data = self._matrix_cache.get_data(cost)
            time_data = gencost_data - (factor * cost_data)
            self._matrix_cache.set_data(od_travel_times, time_data)

    def _set_intrazonal_values(self, period, class_name, skims):
        """Set the intrazonal values to 1/2 nearest neighbour for time and distance skims."""
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
        """Export skims to OMX files by period."""
        root = _dir(_dir(self._emmebank.path))
        omx_file_path = _join(root, f"traffic_skims_{period}.omx")
        with _emme_tools.OMX(
            omx_file_path, "w", scenario, matrix_cache=self._matrix_cache
        ) as omx_file:
            omx_file.write_matrices(self._skim_matrices)
        self._skim_matrices = []
        self._matrix_cache.clear()

    def _base_spec(self):
        """Generate template Emme SOLA assignment specification"""
        relative_gap = self.config.emme.highway.relative_gap
        max_iterations = self.config.emme.highway.max_iterations
        # NOTE: mazmazvol as background traffic in link.data1 ("ul1")
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
        """Prepare attributes and matrices and path analyses specs by class."""
        create_attribute = self._modeller.tool(
            "inro.emme.data.extra_attribute.create_extra_attribute"
        )
        net_calc = _emme_tools.NetworkCalculator(scenario)

        name = class_config["name"]
        name_lower = name
        op_cost = class_config["operating_cost"]
        toll = class_config["toll"]
        toll_factor = class_config.get("toll_factor")
        create_attribute(
            "LINK",
            f"@cost_{name_lower}",
            f'{period} {class_config["description"]} total costs'[:40],
            overwrite=True,
            scenario=scenario,
        )
        if toll_factor is None:
            cost_expression = f"length * {op_cost} + {toll}"
        else:
            cost_expression = f"length * {op_cost} + {toll} * {toll_factor}"
        net_calc(f"@cost_{name_lower}", cost_expression)
        create_attribute(
            "LINK",
            f"@flow_{name_lower}",
            f'{period} {class_config["description"]} link volume'[:40],
            0,
            overwrite=True,
            scenario=scenario,
        )

        class_analysis, od_travel_times = self._prepare_path_analyses(
            class_config["skims"], scenario, period, name
        )
        emme_class_spec = {
            "mode": class_config["emme_mode"],
            "demand": f'mf"{period}_{name}"',
            "generalized_cost": {
                "link_costs": f"@cost_{name_lower}",  # cost in $0.01
                # $/hr -> min/$0.01
                "perception_factor": 0.6 / class_config["value_of_time"],
            },
            "results": {
                "link_volumes": f"@flow_{name_lower}",
                "od_travel_times": {"shortest_paths": od_travel_times},
            },
            "path_analyses": class_analysis,
        }
        return emme_class_spec

    def _prepare_path_analyses(self, skim_names, scenario, period, name):
        """Prepare the path analysis specification and matrices for all skims"""
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
            skim_matrices.append(f"{period}_{name}_cost")
            class_analysis.append(
                self._analysis_spec(f"{period}_{name}_cost", f"@cost_{name}".lower())
            )
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
            if group:
                matrix_name = f"{period}_{name}_{skim_type}{group}"
            else:
                matrix_name = f"{period}_{name}_{skim_type}"
            class_analysis.append(
                self._analysis_spec(matrix_name, analysis_link[skim_type])
            )
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
        """Template path analysis spec"""
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

    def verify(self, period, scenario):
        """Run post-process verification steps 
        """
        # calc_vmt
        net_calc = _emme_tools.NetworkCalculator(scenario)
        class_vehs = []
        for class_config in self.config.emme.highway.classes:
            name = class_config.name.lower()
            pce = class_config.get("pce", 1.0)
            class_vehs.append(f"@flow_{[name]}*{[pce]}")
        if not scenario.extra_attribute("@total_vehicles"):
            scenario.create_extra_attribute("LINK", "@total_vehicles")
        net_calc.add_calc("@total_vehicles", "+".join(class_vehs))
        net_calc.add_calc(result=None, expression="length * @total_vehicles")
        reports = net_calc.run()
        total_vmt = reports[1]["sum"]
        # TODO: specifiy acceptable VMT range
        # assert min_vmt[period] <= total_vmt <= max_vmt[period]

        # check skim matrices for infinities
        for matrix in self._skim_matrices:
            data = self._matrix_cache.get_data(matrix)
            


class ImportDemand(_Component):
    """Import and average highway assignment demand from OMX files to Emme database"""

    def __init__(
        self,
        controller: _Controller,
        root_dir: str,
        scenario: _emme_tools.EmmeScenario,
        period: str,
    ):
        """Import and average highway demand.

        Demand is imported from OMX files based on reference file paths and OMX
        matrix names in highway assignment config (emme.highway.classes).
        The demand is average using MSA with the current demand matrices if the
        controller.iteration > 1.

        Args:
            controller: parent Controller object
            root_dir (str): root directory containing Emme project, demand matrices
            scenario: Emme scenario object for reference zone system
            period: time period ID
        """
        super().__init__(controller)
        self._root_dir = root_dir
        self._scenario = scenario
        self._period = period
        self._omx_files = {}

    def run(self):
        """Run demand import from OMX files and average"""
        scenario = self._scenario
        period = self._period
        traffic_config = self.config.emme.highway.classes
        msa_iteration = self.controller.iteration
        emmebank = scenario.emmebank
        num_zones = len(scenario.zone_numbers)
        with self._setup():
            for class_config in traffic_config:
                demand = self._read_demand(class_config["demand"][0], num_zones)
                for file_config in class_config["demand"][1:]:
                    demand = demand + self._read_demand(file_config, num_zones)
                demand_name = f'{period}_{class_config["name"]}'
                matrix = emmebank.matrix(demand_name)
                if msa_iteration <= 1:
                    if matrix:
                        emmebank.delete_matrix(matrix)
                    ident = emmebank.available_matrix_identifier("FULL")
                    matrix = emmebank.create_matrix(ident)
                    matrix.name = demand_name
                    matrix.description = (
                        f'{period} {class_config["description"]} demand'
                    )
                else:
                    # Load prev demand and MSA average
                    prev_demand = matrix.get_numpy_data(scenario.id)
                    demand = prev_demand + (1.0 / msa_iteration) * (
                        demand - prev_demand
                    )
                matrix.set_numpy_data(demand, scenario.id)

    @_context
    def _setup(self):
        demand_files = self.config.emme.highway.demand_files
        for name, path in demand_files.items():
            self._omx_files[name] = _emme_tools.OMX(
                _join(self._root_dir, path.format(period=self._period))
            )
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
        name = file_config["name"].format(period=self._period.upper())
        factor = file_config.get("factor")
        demand = self._omx_files[file_ref].read(name)
        if factor is not None:
            demand = factor * demand
        shape = demand.shape
        # pad external zone values with 0
        if shape != (num_zones, num_zones):
            demand = _numpy.pad(
                demand, ((0, num_zones - shape[0]), (0, num_zones - shape[1]))
            )
        return demand
