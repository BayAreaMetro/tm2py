import os

from contextlib import contextmanager

import numpy as npm

import inro.emme.database.emmebank as emmebank

from ...component import Component
from ....controller import RunController
from ....emme.matrix import matrices_to_omx


class TransitAssignment(Component):
    """Run transit assignment."""

    def __init__(self, controller: RunController):
        """Run transit assignment and skims.
        Args:
            controller: parent Controller object
            root_dir (str): root directory containing Emme project, demand matrices
        """
        super().__init__(controller)
        self.time_periods = [
            time.short_name for time in self.config.time_periods
        ]
        self.scenario_id_to_time_periods = {
            i + 1: tp for i, tp in enumerate(self.time_periods)
        }

    def run_time_period(self, demand, tp_id: int, time_period: str):
        """[summary]

        Args:
            tp_id ([type]): [description]
            tp ([type]): [description]
        """
        pass

    def run(self):
        """Run transit assignment and skims."""
        for tp_id, tp in self.scenario_id_to_time_periods.items():
            demand = None
            self.run_time_period(demand, tp_id, tp)
