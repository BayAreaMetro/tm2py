"""Highway assignment and skim component.

Performs equilibrium traffic assignment and generates resulting skims.
The assignmend is configured using the "highway" table in the source config.
See the config documentation for details. The traffic assignment runs according
to the list of assignment classes under highway.classes.

Other relevant parameters from the config are:
- emme.num_processors: number of processors as integer or "MAX" or "MAX-N"
- time_periods[].emme_scenario_id: Emme scenario number to use for each period
- time_periods[].highway_capacity_factor

The Emme network must have the following attributes available:

Link - attributes:
- "length" in feet
- "vdf", volume delay function (volume delay functions must also be setup)
- "@useclass", vehicle-class restrictions classification, auto-only, HOV only
- "@free_flow_time", the free flow time (in minutes)
- "@tollXX_YY", the toll for period XX and class subgroup (see truck
    class) named YY, used together with @tollbooth to generate @bridgetoll_YY
    and @valuetoll_YY
- "@maz_flow", the background traffic MAZ-to-MAZ SP assigned flow from highway_maz,
    if controller.iteration > 0
- modes: must be set on links and match the specified mode codes in
    the traffic config

 Network results - attributes:
- @flow_XX: link PCE flows per class, where XX is the class name in the config
- timau: auto travel time
- volau: total assigned flow in PCE

Notes:
- Output matrices are in miles, minutes, and cents (2010 dollars) and are stored/
as real values;
- Intrazonal distance/time is one half the distance/time to the nearest neighbor;
- Intrazonal bridge and value tolls are assumed to be zero
"""

from __future__ import annotations

import os
from contextlib import contextmanager as _context
from typing import TYPE_CHECKING, Dict, List, Union

import numpy as np

from tm2py import tools
from tm2py.components.component import Component
from tm2py.components.demand.demand import PrepareHighwayDemand
from tm2py.emme.manager import EmmeScenario
from tm2py.emme.matrix import MatrixCache, OMXManager
from tm2py.emme.network import NetworkCalculator
from tm2py.logger import LogStartEnd

if TYPE_CHECKING:
    from tm2py.controller import RunController

    EmmeHighwayAnalysisSpec = Dict[
        str,
        Union[
            str,
            bool,
            None,
            Dict[
                str,
                Union[str, bool, None, Dict[str, Union[str, bool, None]]],
            ],
        ],
    ]
    EmmeHighwayClassSpec = Dict[
        str,
        Union[
            str,
            Dict[str, Union[str, float, Dict[str, str]]],
            List[EmmeHighwayAnalysisSpec],
        ],
    ]
    EmmeTrafficAssignmentSpec = Dict[
        str,
        Union[str, Union[str, bool, None, float, List[EmmeHighwayClassSpec]]],
    ]


class HighwayAssignment(Component):
    """Highway assignment and skims.

    Args:
        controller: parent RunController object
    """

    def __init__(self, controller: RunController):
        """Constructor for HighwayAssignment components.

        Args:
            controller (RunController): Reference to current run controller.
        """
        super().__init__(controller)
        self._num_processors = tools.parse_num_processors(
            self.config.emme.num_processors
        )
        self._matrix_cache = None
        self._skim_matrices = []

    @LogStartEnd("Highway assignment and skims", level="STATUS")
    def run(self):
        """Run highway assignment."""
        demand = PrepareHighwayDemand(self.controller)
        emmebank_path = self.get_abs_path(self.config.emme.highway_database_path)
        emmebank = self.controller.emme_manager.emmebank(emmebank_path)
        if self.controller.iteration >= 1:
            demand.run()
        else:
            demand.create_zero_matrix(emmebank)
        for time in self.time_period_names():
            scenario = self.get_emme_scenario(emmebank, time)
            with self._setup(scenario, time):
                iteration = self.controller.iteration
                assign_classes = [
                    AssignmentClass(c, time, iteration)
                    for c in self.config.highway.classes
                ]
                if iteration > 0:
                    self._copy_maz_flow(scenario)
                else:
                    self._reset_background_traffic(scenario)
                self._create_skim_matrices(scenario, assign_classes)
                assign_spec = self._get_assignment_spec(assign_classes)
                # self.logger.log_dict(assign_spec, level="DEBUG")
                with self.logger.log_start_end(
                    "Run SOLA assignment with path analyses", level="INFO"
                ):
                    assign = self.controller.emme_manager.tool(
                        "inro.emme.traffic_assignment.sola_traffic_assignment"
                    )
                    assign(assign_spec, scenario, chart_log_interval=1)

                # Subtract non-time costs from gen cost to get the raw travel time
                for emme_class_spec in assign_spec["classes"]:
                    self._calc_time_skim(emme_class_spec)
                # Set intra-zonal for time and dist to be 1/2 nearest neighbour
                for class_config in self.config.highway.classes:
                    self._set_intrazonal_values(
                        time,
                        class_config["name"],
                        class_config["skims"],
                    )
                self._export_skims(scenario, time)

    @_context
    def _setup(self, scenario: EmmeScenario, time_period: str):
        """Setup and teardown for Emme Matrix cache and list of skim matrices.

        Args:
            scenario: Emme scenario object
            time_period: time period name
        """
        self._matrix_cache = MatrixCache(scenario)
        self._skim_matrices = []
        msg = f"Highway assignment for period {time_period}"
        with self.logger.log_start_end(msg, level="STATUS"):
            try:
                yield
            finally:
                self._matrix_cache.clear()
                self._matrix_cache = None
                self._skim_matrices = []

    def _copy_maz_flow(self, scenario: EmmeScenario):
        """Copy maz_flow from MAZ demand assignment to ul1 for background traffic.

        Args:
            scenario: Emme scenario object
        """
        self.logger.log_time(
            "Copy @maz_flow to ul1 for background traffic", indent=True, level="DETAIL"
        )
        net_calc = NetworkCalculator(scenario)
        net_calc("ul1", "@maz_flow")

    def _reset_background_traffic(self, scenario: EmmeScenario):
        """Set ul1 for background traffic to 0 (no maz-maz flow).

        Args:
            scenario: Emme scenario object
        """
        self.logger.log_time(
            "Set ul1 to 0 for background traffic", indent=True, level="DETAIL"
        )
        net_calc = NetworkCalculator(scenario)
        net_calc("ul1", "0")

    def _create_skim_matrices(
        self, scenario: EmmeScenario, assign_classes: List[AssignmentClass]
    ):
        """Create matrices to store skim results in Emme database.

        Also add the matrices to list of self._skim_matrices.

        Args:
            scenario: Emme scenario object
            assign_classes: list of AssignmentClass objects
        """
        create_matrix = self.controller.emme_manager.tool(
            "inro.emme.data.matrix.create_matrix"
        )

        with self.logger.log_start_end("Creating skim matrices", level="DETAIL"):
            for klass in assign_classes:
                for matrix_name in klass.skim_matrices:
                    matrix = scenario.emmebank.matrix(f'mf"{matrix_name}"')
                    if not matrix:
                        matrix = create_matrix(
                            "mf", matrix_name, scenario=scenario, overwrite=True
                        )
                        self.logger.log(
                            f"Create matrix name: {matrix_name}, id: {matrix.id}",
                            level="DEBUG",
                        )
                    self._skim_matrices.append(matrix)

    def _get_assignment_spec(
        self, assign_classes: List[AssignmentClass]
    ) -> EmmeTrafficAssignmentSpec:
        """Generate template Emme SOLA assignment specification.

        Args:
            assign_classes: list of AssignmentClass objects

        Returns
            Emme specification for SOLA traffic assignment

        """
        relative_gap = self.config.highway.relative_gap
        max_iterations = self.config.highway.max_iterations
        # NOTE: mazmazvol as background traffic in link.data1 ("ul1")
        base_spec = {
            "type": "SOLA_TRAFFIC_ASSIGNMENT",
            "background_traffic": {
                "link_component": "ul1",
                "turn_component": None,
                "add_transit_vehicles": False,
            },
            "classes": [klass.emme_highway_class_spec for klass in assign_classes],
            "stopping_criteria": {
                "max_iterations": max_iterations,
                "best_relative_gap": 0.0,
                "relative_gap": relative_gap,
                "normalized_gap": 0.0,
            },
            "performance_settings": {"number_of_processors": self._num_processors},
        }
        return base_spec

    def _calc_time_skim(self, emme_class_spec: EmmeHighwayClassSpec):
        """Calculate the real time skim =gen_cost-per_fac*link_costs.

        Args:
            emme_class_spec: dictionary of the per-class spec sub-section from the
                Emme SOLA assignment spec, classes list
        """
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

    def _set_intrazonal_values(
        self, time_period: str, class_name: str, skims: List[str]
    ):
        """Set the intrazonal values to 1/2 nearest neighbour for time and distance skims.

        Args:
            time_period: time period name (from config)
            class_name: highway class name (from config)
            skims: list of requested skims (from config)
        """
        for skim_name in skims:
            matrix_name = f"mf{time_period}_{class_name}_{skim_name}"
            if skim_name in ["time", "distance", "freeflowtime", "hovdist", "tolldist"]:
                data = self._matrix_cache.get_data(matrix_name)
                # NOTE: sets values for external zones as well
                np.fill_diagonal(data, np.inf)
                data[np.diag_indices_from(data)] = 0.5 * np.nanmin(data, 1)
                self._matrix_cache.set_data(matrix_name, data)

    def _export_skims(self, scenario: EmmeScenario, time_period: str):
        """Export skims to OMX files by period.

        Args:
            scenario: Emme scenario object
            time_period: time period name
        """
        # NOTE: skims in separate file by period
        omx_file_path = self.get_abs_path(
            self.config.highway.output_skim_path.format(period=time_period)
        )
        os.makedirs(os.path.dirname(omx_file_path), exist_ok=True)
        with OMXManager(
            omx_file_path, "w", scenario, matrix_cache=self._matrix_cache
        ) as omx_file:
            omx_file.write_matrices(self._skim_matrices)


class AssignmentClass:
    """Highway assignment class, represents data from config and conversion to Emme specs."""

    def __init__(self, class_config, time_period, iteration):
        """Constructor of Highway Assignment class.

        Args:
            class_config (_type_): _description_
            time_period (_type_): _description_
            iteration (_type_): _description_
        """
        self.class_config = class_config
        self.time_period = time_period
        self.iteration = iteration
        self.name = class_config["name"].lower()
        self.skims = class_config.get("skims", [])

    @property
    def emme_highway_class_spec(self) -> EmmeHighwayClassSpec:
        """Construct and return Emme traffic assignment class specification.

        Converted from input config (highway.classes), see Emme Help for
        SOLA traffic assignment for specification details.
        Adds time_period as part of demand and skim matrix names.

        Returns:
            A nested dictionary corresponding to the expected Emme traffic
            class specification used in the SOLA assignment.
        """
        if self.iteration == 0:
            demand_matrix = 'ms"zero"'
        else:
            demand_matrix = f'mf"{self.time_period}_{self.name}"'
        class_spec = {
            "mode": self.class_config.mode_code,
            "demand": demand_matrix,
            "generalized_cost": {
                "link_costs": f"@cost_{self.name.lower()}",  # cost in $0.01
                # $/hr -> min/$0.01
                "perception_factor": 0.6 / self.class_config.value_of_time,
            },
            "results": {
                "link_volumes": f"@flow_{self.name.lower()}",
                "od_travel_times": {
                    "shortest_paths": f"mf{self.time_period}_{self.name}_time"
                },
            },
            "path_analyses": self.emme_class_analysis,
        }
        return class_spec

    @property
    def emme_class_analysis(self) -> List[EmmeHighwayAnalysisSpec]:
        """Construct and return a list of path analyses specs which generate the required skims.

        Returns:
            A list of nested dictionaries corresponding to the Emme path analysis
            (per-class) specification used in the SOLA assignment.
        """
        class_analysis = []
        if "time" in self.skims:
            class_analysis.append(
                self.emme_analysis_spec(
                    f"@cost_{self.name}".lower(),
                    f"mf{self.time_period}_{self.name}_cost",
                )
            )
        for skim_type in self.skims:
            if skim_type == "time":
                continue
            if "_" in skim_type:
                skim_type, group = skim_type.split("_")
            else:
                group = ""
            matrix_name = f"mf{self.time_period}_{self.name}_{skim_type}{group}"
            class_analysis.append(
                self.emme_analysis_spec(
                    self.skim_analysis_link_attribute(skim_type, group),
                    matrix_name,
                )
            )
        return class_analysis

    @property
    def skim_matrices(self) -> List[str]:
        """Returns: List of skim matrix names for this class."""
        skim_matrices = []
        if "time" in self.skims:
            skim_matrices.extend(
                [
                    f"{self.time_period}_{self.name}_time",
                    f"{self.time_period}_{self.name}_cost",
                ]
            )
        for skim_type in self.skims:
            if skim_type == "time":
                continue
            if "_" in skim_type:
                skim_type, group = skim_type.split("_")
            else:
                group = ""
            skim_matrices.append(f"{self.time_period}_{self.name}_{skim_type}{group}")
        return skim_matrices

    @staticmethod
    def emme_analysis_spec(link_attr: str, matrix_name: str) -> EmmeHighwayAnalysisSpec:
        """Returns Emme highway class path analysis spec.

        See Emme Help for SOLA assignment for full specification details.
        Args:
            link_attr: input link attribute for which to sum values along the paths
            matrix_name: full matrix name to store the result of the path analysis

        Returns:
            The nested dictionary specification which will generate the skim
            of link attribute values.
        """
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

    @staticmethod
    def skim_analysis_link_attribute(skim: str, group: str) -> str:
        """Return the link attribute name for the specified skim type and group.

        Args:
            skim: name of skim requested, one of dist, hovdist, tolldist, freeflowtime,
                bridgetoll, or valuetoll
            group: subgroup name for the bridgetoll or valuetoll, corresponds to one of
                the names from config.highway.tolls.dst_vehicle_group_names
        Returns:
            A string of the link attribute name used in the analysis.
        """
        lookup = {
            "dist": "length",  # NOTE: length must be in miles
            "hovdist": "@hov_length",
            "tolldist": "@toll_length",
            "freeflowtime": "@free_flow_time",
            "bridgetoll": f"@bridgetoll_{group}",
            "valuetoll": f"@valuetoll_{group}",
        }
        return lookup[skim]
