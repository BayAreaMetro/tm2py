from typing import Union, Collection

from .highway_network import PrepareNetwork
from .highway_skim import HighwayAnalysis
from ...component import Component
from ...demand import PrepareDemand
from ....controller import RunController


class HighwayAssignment(Component):
    """Highway assignment"""

    def __init__(self, controller: RunController):
        """Highway assignment and skims.
        Args:
            controller: parent RunController object
        """
        super().__init__(controller)
        self.demand = PrepareDemand(self.controller)
        self.network = PrepareNetwork(self.controller)
        self.local_traffic = Maz
        self.iteration = controller.iteration.copy()

        self.time_periods = [time.short_name for time in self.config.time_periods]
        self.scenario_id_to_time_periods = {
            i + 1: tp for i, tp in enumerate(self.transit_time_periods)
        }

        self.time_periods_to_scenario_id = {
            v: k for k, v in self.scenario_id_to_time_periods.items()
        }

        self.traffic_assign = self._modeller.tool(
            "inro.emme.traffic_assignment.sola_traffic_assignment"
        )

    @property
    def assignment_classes(self, time_period):
        config_classes = self.config.highway.classes
        classes = [AssignmentClass(class_config) for class_config in config_classes]

    def assignment_spec(self, time_period):
        spec = self.config.highway.assignment
        spec["classes"] = [
            ac.emme_highway_class_spec(time_period) for ac in self.assignment_classes
        ]
        return spec

    def run(self, time_period: Union[Collection[str], str] = None):
        """Run highway assignment"""
        if not time_period:
            time_period = self.time_periods
        if type(time_period) is Collection:
            for tp in time_period:
                self.run(time_period=tp)

        _scenario_id = self.time_periods_to_scenario_id[time_period]
        scenario = self._modeller._emmebank.scenario(_scenario_id)

        self.network.run(
            time_period=time_period, assignment_classes=self.assignment_classes
        )
        self.demand.run(time_period=time_period)

        self.local_traffic.run(time_period=time_period)

        self.traffic_assign(
            self.assign_spec(time_period), scenario, chart_log_interval=1
        )


class AssignmentClass:
    def __init__(self, class_config):
        self.class_config
        self.name = class_config["name"].lower()
        self.skims = class_config.get("skims", [])

    @property
    def cost_expression(self):
        op_cost = self.class_config["operating_cost"]
        toll = self.class_config.get("toll", 0)
        toll_factor = self.class_config.get("toll_factor", 1)
        cost_exp = f"length * {op_cost} + {toll} * {toll_factor}"
        return cost_exp

    @property
    def emme_highway_class_spec(self, time_period):
        """[summary]

        Args:
            time_period ([type]): [description]

        Returns:
            [type]: [description]
        """
        class_spec = {
            "mode": self.class_config["code"],
            "demand": f'mf"{time_period}_{self.name}"',
            "generalized_cost": {
                "link_costs": f"@cost_{self.name}",  # cost in $0.01
                # $/hr -> min/$0.01
                "perception_factor": 0.6 / self.class_config["value_of_time"],
            },
            "results": {
                "link_volumes": f"@flow_{self.name}",
                "od_travel_times": {
                    "shortest_paths": f"{time_period}_{self.name}_time"
                },
            },
            "path_analyses": self.emme_class_analysis(time_period),
        }
        return class_spec

    @property
    def emme_class_analysis(self, time_period):
        """[summary]

        Args:
            time_period ([type]): [description]

        Returns:
            [type]: [description]
        """
        class_analysis = []
        if "time" in self.skims:
            class_analysis.append(
                HighwayAnalysis._emme_analysis_spec(
                    f"{time_period}_{self.name}_cost", f"@cost_{self.name}".lower()
                )
            )
        for skim_type in self.skims:
            if "_" in skim_type:
                skim_type, group = skim_type.split("_")
                matrix_name = f"{time_period}_{self.name}_{skim_type}{group}"
            else:
                group = ""
                matrix_name = f"{time_period}_{self.name}_{skim_type}"

            class_analysis.append(
                HighwayAnalysis._emme_analysis_spec(
                    matrix_name,
                    HighwayAnalysis._skim_analysis_link_attribute(skim_type, group),
                )
            )
        return class_analysis
