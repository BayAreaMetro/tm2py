"""RunController - model operation controller
"""

import argparse
import itertools
import os
from typing import Union, List

from tm2py.config import Configuration
from tm2py.emme.manager import EmmeManager
from tm2py.logger import Logger
from tm2py.components.network.highway.highway_assign import HighwayAssignment
from tm2py.components.network.highway.highway_network import PrepareNetwork
from tm2py.components.network.highway.highway_maz import AssignMAZSPDemand, SkimMAZCosts


# pylint: disable=too-many-instance-attributes


class RunController:
    """Main operational interface for model runs.

    Provide one or more config files in TOML (*.toml) format, and a run directory.
    If the run directory is not provided the root directory of the first config_file is used.
    """

    def __init__(self, config_file: Union[List[str], str] = None, run_dir: str = None):
        if not isinstance(config_file, list):
            config_file = [config_file]
        if run_dir is None:
            run_dir = os.path.abspath(os.path.dirname(config_file[0]))
        self._run_dir = run_dir

        self.config = Configuration(config_file)
        self.logger = Logger(self)
        self.top_sheet = None
        self.trace = None

        self.component_map = {
            "prepare_network_highway": PrepareNetwork(self),
            "highway": HighwayAssignment(self),
            "highway_maz_assign": AssignMAZSPDemand(self),
            "highway_maz_skim": SkimMAZCosts(self),
        }
        self.completed_components = []
        self._emme_manager = None
        self._iteration = None
        self._component = None
        self._queued_components = []
        self._queue_components()

    @property
    def run_dir(self):
        """The root run directory of the model run"""
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

    @property
    def emme_manager(self) -> EmmeManager:
        """Cached Emme Manager object"""
        if self._emme_manager is None:
            self._emme_manager = EmmeManager()
            project = self._emme_manager.project(
                os.path.join(self.run_dir, self.config.emme.project_path)
            )
            # Initialize Modeller to use Emme assignment tools and other APIs
            self._emme_manager.modeller(project)
        return self._emme_manager

    def run(self):
        """Main interface to run model"""
        self._iteration = None
        self.validate_inputs()
        for iteration, component in self._queued_components:
            if self._iteration != iteration:
                self.logger.log_time(f"Start iteration {iteration}")
            self._iteration = iteration
            self._component = component
            component.run()
            self.completed_components.append((iteration, component))

    def _queue_components(self):
        """Add components per iteration to queue according to input Config"""
        if self.config.run.start_iteration == 0:
            self._queued_components += [
                (0, self.component_map[c_name])
                for c_name in self.config.run.initial_components
            ]
        iteration_nums = range(
            max(1, self.config.run.start_iteration), self.config.run.end_iteration + 1
        )
        iteration_components = [
            self.component_map[c_name]
            for c_name in self.config.run.global_iteration_components
        ]
        self._queued_components += list(
            itertools.product(iteration_nums, iteration_components)
        )
        self._queued_components += [
            (self.config.run.end_iteration + 1, self.component_map[c_name])
            for c_name in self.config.run.final_components
        ]

        if self.config.run.start_component:
            start_component = self.component_map[self.config.run.start_component]
            start_index = [
                idx
                for idx, c in enumerate(self._queued_components)
                if start_component == c[1]
            ][0]
            self._queued_components = self._queued_components[start_index:]

    def validate_inputs(self):
        """Validate input state prior to run"""
        already_validated_components = set()
        for _, component in self._queued_components:
            if component not in already_validated_components:
                component.validate_inputs()
                already_validated_components.add(component)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Main: run MTC TM2PY")
    parser.add_argument("-s", "--scenario", help=r"Scenario config file path")
    parser.add_argument("-m", "--model", help=r"Model config file path")
    args = parser.parse_args()
    controller = RunController([args.scenario, args.model])
    controller.run()
