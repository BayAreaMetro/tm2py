import os

from contextlib import contextmanager

import numpy as npm

import inro.emme.database.emmebank as emmebank

from .component import Component
from ..controller import RunController
from ..emme.matrix import matrices_to_omx


class TransitSkim(Component):
    def __init__(self, time_period, tp_id, controller: RunController):
        super().__init__(controller)
        self.time_period = time_period
        self.tp_id = tp_id

    def run(self):
        pass

    def write(self):
        """[summary]"""
        output_omx_file = os.path.join(
            self.controller.run_dir,
            self.controller.config.dir.skims,
            self.controller.config.transit.skim_file.format(skim_name=self.name),
        )

        matrices_to_omx(
            omx_file=output_omx_file,
            scenario=self.tp_id,
            big_to_zero=True,
        )

    @property
    def name(self):
        return self.config.transit.skim_name.format(
            access_mode="",
            transit_set="",
            time_period=self.time_period,
            iteration=self.controller.iteration,
        )


class TransitAssignment(Component):
    """Run transit assignment."""

    def __init__(self, controller: RunController):
        """Run transit assignment and skims.
        Args:
            controller: parent Controller object
            root_dir (str): root directory containing Emme project, demand matrices
        """
        super().__init__(controller)
        self.transit_time_periods = [
            time.short_name for time in self.config.time_periods
        ]
        self.scenario_id_to_time_periods = {
            i + 1: tp for i, tp in enumerate(self.transit_time_periods)
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
