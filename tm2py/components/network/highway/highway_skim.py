from ...component import Component
from ....controller import RunController


class HighwaySkim(Component):
    """Highway network preparation"""

    def __init__(self, controller: RunController):
        """Highway assignment and skims.
        Args:
            controller: parent RunController object
        """
        super().__init__(controller)
        self.iteration = controller.iteration.copy()

    def write_omx(self, period, scenario):
        """Export skims to OMX files by period."""
        root = _dir(_dir(self._emmebank.path))
        omx_file_path = _join(root, f"traffic_skims_{period}.omx")
        with _emme_tools.OMX(
            omx_file_path, "w", scenario, matrix_cache=self._matrix_cache
        ) as omx_file:
            omx_file.write_matrices(self._skim_matrices)
        self._skim_matrices = []
        self._matrix_cache.clear()

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


class HighwayAnalysis:
    @staticmethod
    def _emme_analysis_spec(matrix_name, link_attr):
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

    @staticmethod
    def _skim_analysis_link_attribute(skim: str, group) -> str:
        lookup = {
            "dist": "length",  # NOTE: length must be in miles
            "hovdist": "@hov_length",
            "tolldist": "@toll_length",
            "freeflowtime": "@free_flow_time",
            "bridgetoll": f"@bridgetoll_{group}",
            "valuetoll": f"@valuetoll_{group}",
        }
        return lookup[skim]
