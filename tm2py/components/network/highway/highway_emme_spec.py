"""EMME Specification builders for the Highway assignment and skim component.

Generates EMME SOLA assignment specifications and related data
(lists of demand matrices, skims matrices)

"""

from typing import TYPE_CHECKING, Dict, List, Union

from tm2py.emme.manager import parse_num_processors


if TYPE_CHECKING:
    from tm2py.config import HighwayConfig

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


class AssignmentSpecBuilder:
    """Highway assignment specification builder, represents data from config
    and conversion to EMME SOLA specification.
    """

    def __init__(
        self,
        time: str,
        iteration: int,
        warmstart: bool,
        highway_config: "HighwayConfig",
        num_processors: Union[int, str],
    ):
        """Constructor of Highway Assignment class.

        Args:
            time (_type_): _description_
            iteration (_type_): global iteration number (should normally come from controller)
            warmstart (bool): True if assigning warmstart demand (should normally come from config)
            highway_config (HighwayConfig object): the highway config (config.highway)
            num_processors (int, str): number of processors to use in the assignment as an integer,
                or reference of MAX in pattern like MAX-N or MAX/N
        """
        self._time = time
        self._iteration = iteration
        self._warmstart = warmstart
        self._num_processors = parse_num_processors(num_processors)

        self._assign_classes = [
            AssignmentClass(c, time, iteration, warmstart)
            for c in highway_config.classes
        ]
        # get the corresponding relative gap for the current global iteration
        relative_gaps = highway_config.relative_gaps
        relative_gap = None
        if relative_gaps and isinstance(relative_gaps, tuple):
            for item in relative_gaps:
                if item["global_iteration"] == iteration:
                    relative_gap = item["relative_gap"]
                    break
            if relative_gap is None:
                raise ValueError(
                    f"RelativeGapConfig: Must specifify a value for global iteration {iteration}"
                )
        self._relative_gap = relative_gap
        self._network_acceleration = highway_config.network_acceleration
        self._max_iterations = highway_config.max_iterations

    @property
    def demand_matrices(self) -> List[str]:
        """The list of unique demand matrices as strings of EMME matrix IDs."""
        demand_matrices = set([])
        for klass in self._assign_classes:
            demand_matrices.add(klass.demand_matrix)
        return list(demand_matrices)

    @property
    def skim_matrices(self) -> List[str]:
        """The list of unique skim matrices as strings of EMME matrix IDs."""
        skim_matrices = []
        for klass in self._assign_classes:
            skim_matrices.extend(klass.skim_matrices)
        return skim_matrices

    @property
    def assignment_spec(self) -> "EmmeTrafficAssignmentSpec":
        """The Emme SOLA assignment specification for the highway.

        Returns
            Emme specification for SOLA traffic assignment

        """
        classes = [klass.spec for klass in self._assign_classes]
        # NOTE: mazmazvol as background traffic in link.data1 ("ul1")
        base_spec = {
            "type": "SOLA_TRAFFIC_ASSIGNMENT",
            "background_traffic": {
                "link_component": "ul1",
                "turn_component": None,
                "add_transit_vehicles": False,
            },
            "classes": classes,
            "stopping_criteria": {
                "max_iterations": self._max_iterations,
                "best_relative_gap": 0.0,
                "relative_gap": self._relative_gap,
                "normalized_gap": 0.0,
            },
            "performance_settings": {
                "number_of_processors": self._num_processors,
                "network_acceleration": self._network_acceleration,
            },
        }
        return base_spec


class AssignmentClass:
    """Highway assignment class, represents data from config and conversion to Emme specs."""

    def __init__(self, class_config, time_period, iteration, warmstart):
        """Constructor of Highway Assignment class.

        Args:
            class_config (_type_): _description_
            time_period (str): _description_
            iteration (int): _description_
            warmstart (bool): True if assigning warmstart demand
        """
        self.class_config = class_config
        self.time_period = time_period
        self.iteration = iteration
        self.warmstart = warmstart
        self.name = class_config["name"].lower()
        self.skims = class_config.get("skims", [])

    @property
    def spec(self) -> "EmmeHighwayClassSpec":
        """Construct and return Emme traffic assignment class specification.

        Converted from input config (highway.classes), see Emme Help for
        SOLA traffic assignment for specification details.
        Adds time_period as part of demand and skim matrix names.

        Returns:
            A nested dictionary corresponding to the expected Emme traffic
            class specification used in the SOLA assignment.
        """
        class_spec = self.spec_no_analysis
        class_spec["path_analyses"] = self.emme_class_analysis
        return class_spec

    @property
    def spec_no_analysis(self) -> "EmmeHighwayClassSpec":
        """Construct and return Emme traffic assignment class specification.

        Converted from input config (highway.classes), see Emme Help for
        SOLA traffic assignment for specification details.
        Adds time_period as part of demand and skim matrix names.

        Returns:
            A nested dictionary corresponding to the expected Emme traffic
            class specification used in the SOLA assignment.
        """
        class_spec = {
            "mode": self.class_config.mode_code,
            "demand": self.demand_matrix,
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
        }
        return class_spec

    @property
    def emme_class_analysis(self) -> List["EmmeHighwayAnalysisSpec"]:
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
                matrix_name = f"mf{self.time_period}_{self.name}_{skim_type}_{group}"
            else:
                group = ""
                matrix_name = f"mf{self.time_period}_{self.name}_{skim_type}"
            class_analysis.append(
                self.emme_analysis_spec(
                    self.skim_analysis_link_attribute(skim_type, group),
                    matrix_name,
                )
            )
        return class_analysis

    @property
    def demand_matrix(self) -> str:
        """Returns: The demand matrix name for this class."""
        if self.iteration == 0:
            if not self.warmstart:
                demand_matrix = 'ms"zero"'
            else:
                demand_matrix = f'mf"{self.time_period}_{self.name}"'
        else:
            demand_matrix = f'mf"{self.time_period}_{self.name}"'
        return demand_matrix

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
                skim_matrices.append(
                    f"{self.time_period}_{self.name}_{skim_type}_{group}"
                )
            else:
                group = ""
                skim_matrices.append(f"{self.time_period}_{self.name}_{skim_type}")
        return skim_matrices

    @staticmethod
    def emme_analysis_spec(
        link_attr: str, matrix_name: str
    ) -> "EmmeHighwayAnalysisSpec":
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
            "rlbty": "@reliability_sq",
            "autotime": "@auto_time",
        }
        return lookup[skim]
