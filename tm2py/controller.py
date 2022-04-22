"""RunController - model operation controller.

Main interface to start a TM2PY model run. Provide one or more configuration
files in .toml format (by convention a scenario.toml and a model.toml)

  Typical usage example:
  from tm2py.controller import RunController
  controller = RunController(
    [r"example_union\\scenario.toml", r"example_union\\model.toml"])
  controller.run()

  Or from the command-line:
  python <path>\\tm2py\\tm2py\\controller.py –s scenario.toml –m model.toml

"""

import itertools
import os
from typing import Union, List

from tm2py.config import Configuration
from tm2py.emme.manager import EmmeManager
from tm2py.logger import Logger
from tm2py.components.component import Component
from tm2py.components.network.highway.highway_assign import HighwayAssignment
from tm2py.components.network.highway.highway_network import PrepareNetwork
from tm2py.components.network.highway.highway_maz import AssignMAZSPDemand, SkimMAZCosts

# mapping from names referenced in config.run to imported classes
# NOTE: component names also listed as literal in tm2py.config for validation
component_cls_map = {
    "prepare_network_highway": PrepareNetwork,
    "highway": HighwayAssignment,
    "highway_maz_assign": AssignMAZSPDemand,
    "highway_maz_skim": SkimMAZCosts,
}

# pylint: disable=too-many-instance-attributes


class RunController:
    """Main operational interface for model runs.

    Provide one or more config files in TOML (*.toml) format, and a run directory.
    If the run directory is not provided the root directory of the first config_file is used.

    Properties:
        config: root Configuration object
        logger: logger object
        top_sheet: placeholder for top sheet functionality (not implemented yet)
        trace: placeholder for trace functionality (not implemented yet)
        run_dir: root run directory for the model run
        iteration: current running (or last started) iteration
        component: current running (or last started) Component object
        emme_manager: EmmeManager object for centralized Emme-related (highway and
            transit assignments and skims) utilities.
        complete_components: list of components which have completed, tuple of
            (iteration, name, Component object)
    """

    def __init__(self, config_file: Union[List[str], str] = None, run_dir: str = None):
        if not isinstance(config_file, list):
            config_file = [config_file]
        if run_dir is None:
            run_dir = os.path.abspath(os.path.dirname(config_file[0]))
        self._run_dir = run_dir

        self.config = Configuration.load_toml(config_file)
        self.logger = Logger(self)
        self.top_sheet = None
        self.trace = None
        self.completed_components = []

        # mapping from defined names referenced in config to Component objects
        self._component_map = {k: v(self) for k, v in component_cls_map.items()}
        self._emme_manager = None
        self._iteration = None
        self._component = None
        self._queued_components = []
        self._queue_components()

    @property
    def run_dir(self) -> str:
        """The root run directory of the model run"""
        return self._run_dir

    @property
    def iteration(self) -> int:
        """Current iteration of model"""
        return self._iteration

    @property
    def component(self) -> Component:
        """Current component of model"""
        return self._component

    @property
    def emme_manager(self) -> EmmeManager:
        """Cached Emme Manager object"""
        if self._emme_manager is None:
            self._init_emme_manager()
        return self._emme_manager

    def _init_emme_manager(self):
        """Initialize Emme manager, start Emme desktop App, and initialize Modeller"""
        self._emme_manager = EmmeManager()
        project = self._emme_manager.project(
            os.path.join(self.run_dir, self.config.emme.project_path)
        )
        # Initialize Modeller to use Emme assignment tools and other APIs
        self._emme_manager.modeller(project)

    def run(self):
        """Main interface to run model"""
        self._iteration = None
        self.validate_inputs()
        for iteration, name, component in self._queued_components:
            if self._iteration != iteration:
                self.logger.log_time(f"Start iteration {iteration}")
            self._iteration = iteration
            self._component = component
            component.run()
            self.completed_components.append((iteration, name, component))

    def _queue_components(self):
        """Add components per iteration to queue according to input Config"""
        self._queued_components = []
        if self.config.run.start_iteration == 0:
            self._queued_components += [
                (0, c_name, self._component_map[c_name])
                for c_name in self.config.run.initial_components
            ]
        iteration_nums = range(
            max(1, self.config.run.start_iteration), self.config.run.end_iteration + 1
        )
        iteration_components = [
            self._component_map[c_name]
            for c_name in self.config.run.global_iteration_components
        ]
        self._queued_components += list(
            itertools.product(
                iteration_nums,
                iteration_components,
                self.config.run.global_iteration_components,
            )
        )
        self._queued_components += [
            (self.config.run.end_iteration + 1, self._component_map[c_name])
            for c_name in self.config.run.final_components
        ]

        if self.config.run.start_component:
            start_index = [
                idx
                for idx, c in enumerate(self._queued_components)
                if self.config.run.start_component == c[1]
            ][0]
            self._queued_components = self._queued_components[start_index:]

    def validate_inputs(self):
        """Validate input state prior to run"""
        already_validated_components = set()
        for _, name, component in self._queued_components:
            if name not in already_validated_components:
                component.validate_inputs()
                already_validated_components.add(name)
