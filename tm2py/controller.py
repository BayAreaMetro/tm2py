"""tktk
"""

import argparse
import itertools
import os
from typing import Union, List

from .config import Configuration
from .emme.manager import EmmeManager
from .logger import Logger
from . import components


# pylint: disable=too-many-instance-attributes


class RunController:
    """Main operational interface for model runs.

    Provide one or more config files in TOML (*.toml) format, and a run directory.
    If the run directory is not provided the root directory of the first config_file is used.
    """

    def __init__(self, config_file: Union[List[str], str] = None, run_dir: str = None):
        if not isinstance(config_file, list):
            config_file = [config_file]
        self._run_dir = run_dir
        if self._run_dir is None:
            self._run_dir = os.path.dirname(config_file[0])

        self.config = Configuration(config_file)
        self.logger = Logger(self)

        self.top_sheet = None
        self.trace = None

        self._emme_manager = EmmeManager()
        self._emme_manager.project(self.run_dir)

        self.component_map = {
            "highway": components.network.highway.highway_assign.HighwayAssignment(
                self
            ),
        }
        self.completed_components = []
        self._iteration = None
        self._component = None
        self._queued_components = []
        self._queue_components()

    @property
    def run_dir(self):
        """Shortcut to access run directory"""
        return self._run_dir

    @property
    def queued_components(self):
        """List of all component objects"""
        return list(self._queued_components)

    @property
    def iteration(self):
        """Current iteration of model"""
        return self._iteration

    @property
    def component(self):
        """Current component of model"""
        return self._component

    def run(self):
        """Main interface to run model"""
        self.validate_inputs()
        for iteration, component in self._queued_components:
            self._iteration = iteration
            self._component = component
            component.run()
            self.completed_components.append((iteration, component))

    def _queue_components(self):
        """[summary]"""
        if self.config.run.start_iteration == 0:
            self._queued_components += [
                (0, self.component_map[c_name])
                for c_name in self.config.run.initial_components
            ]
        self._queued_components += list(
            itertools.product(
                range(
                    max(1, self.config.run.start_iteration),
                    self.config.run.end_iteration + 1,
                ),
                self.config.run.global_iteration_components,
            )
        )

        if self.config.run.start_component:
            start_index = [
                idx
                for idx, c in enumerate(self._queued_components)
                if self.config.run.start_component in c[1]
            ][0]
            self._queued_components = self._queued_components[start_index:]

    def validate_inputs(self):
        """Validate input state prior to run"""
        for _, component in self._queued_components:
            component.validate_inputs()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Main: run MTC TM2PY")
    parser.add_argument("-s", "--scenario", help=r"Scenario config file path")
    args = parser.parse_args()
    controller = RunController(args.scenario)
    controller.run()
