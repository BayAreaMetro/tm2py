import os
from typing import Mapping

from ....logger import Logger
from ...component import Component
from ....controller import Controller

class PrepareNetwork(Component):
    """Highway network preparation"""

    def __init__(self, controller: Controller):
        """Highway assignment and skims.
        Args:
            controller: parent Controller object
        """
        super().__init__(controller)
        self.iteration = controller.iteration.copy()
        self.create_attribute = self._modeller.tool(
            "inro.emme.data.extra_attribute.create_extra_attribute"
        )


    def add_highway_class(self, time_period, class_config: Mapping):
        self.create_attribute(
            "LINK",
            f'@cost_{class_config["short_name"]}',
            f'{time_period} {class_config["description"]} total costs'[:40],
            overwrite=True,
            scenario=scenario,
        )
        net_calc(f"@cost_{name_lower}", cost_expression)

        create_attribute(
            "LINK",
            f"@flow_{name_lower}",
            f'{period} {class_config["description"]} link volume'[:40],
            0,
            overwrite=True,
            scenario=scenario,
        )
        net_calc(f"@cost_{class_config['short_name']}", class_config['cost_expression')


class PrepareAssignmentClasses(Component):
    def __init__(self, controller: Controller):
        """Highway assignment and skims.
        Args:
            controller: parent Controller object
        """
        super().__init__(controller)
        
