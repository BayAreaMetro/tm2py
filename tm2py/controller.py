"""tktk
"""

import argparse
import itertools
import os

from .config import Configuration
from .logger import Logger
from .emme.cache import EmmeProjectCache
from .compute import parse_num_processors
import components

class RunController:
    """docstring for Controller class"""

    def __init__(self, run_config_file: str = None):
        if not run_config_file:
            config_file = os.path.join(os.getcwd(), "config.toml")

        self.config = Configuration(config_file)
        self.logger = Logger(self)

        self.top_sheet = None
        self.trace = None

        self._emme_bank = None
        self._emme_manager = EmmeProjectCache()
        self._emme_manager.project(self.run_dir)
        self._emmebank_dir = os.path.join(
            self.run_dir, "mtc_emme_transit", "Database", "emmebank"
        )

        self.num_processors = parse_num_processors(self.config.num_processors)

        self.completed_components = []

        self._queued_components = []
        self._iteration = None
        self._component = None
        self._queue_components()


    @property
    def _modeller(self):
        return self._emme_manager.modeller

    @property
    def run_dir(self):
        "shortcut to access run directory"
        return self.config.run_dir

    @property
    def emme_bank(self):
        return self._emme_bank

    @property
    def queued_components(self):
        """List of all component objects"""
        return self._queued_components

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
        for i, c in self.queued_components():
            self._iteration = i
            self._component = c
            components.c.run()

    def _queue_components(self):
        """[summary]"""
        if self.config.start_iteration == 0:
            self._queued_components += [
                (0, self.component_map[c_name])
                for c_name in self.config.initialize_components
            ]
        self._queued_components += list(
            itertools.product(
                range(
                    max(1, self.config.start_iteration),
                    self.config.run.global_iterations + 1,
                ),
                self.config.global_iteration_components,
            )
        )

        if self.config.start_component:
            start_index = [
                idx
                for idx, c in enumerate(self._queued_components)
                if self.config.start_component in c[1]
            ][0]
            self._queued_components = self._queued_components[start_index:]

    def validate_inputs(self):
        """Validate input state prior to run"""
        for component in self.components.values():
            component.validate_inputs()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Main: run MTC TM2PY")
    parser.add_argument("-a", "--argument", help=r"An argument")
    args = parser.parse_args()

    controller = RunController()
    controller.argument = args.argument
    controller.run()
