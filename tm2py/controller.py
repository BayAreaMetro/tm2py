"""RunController - model operation controller.

Main interface to start a TM2PY model run. Provide one or more configuration
files in .toml format (by convention a scenario.toml and a model.toml)

  Typical usage example:
  from tm2py.controller import RunController
  controller = RunController(
    ["scenario.toml", "model.toml"])
  controller.run()

  Or from the command-line:
  `python <path>/tm2py/tm2py/controller.py –s scenario.toml –m model.toml`

"""

import itertools
import os
from pathlib import Path
from typing import Collection, List, Tuple, Union

from tm2py.components.component import Component
from tm2py.components.network.highway.highway_assign import HighwayAssignment
from tm2py.components.network.highway.highway_maz import AssignMAZSPDemand, SkimMAZCosts
from tm2py.components.network.highway.highway_network import PrepareNetwork
from tm2py.config import Configuration
from tm2py.emme.manager import EmmeManager
from tm2py.logger import Logger

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

    Internal properties:
        _emme_manager: EmmeManager object, cached on first access
        _iteration: current iteration
        _component: current running / last run Component
        _component_name: name of the current / last run component
        _queued_components: list of iteration, name, Component
    """

    def __init__(
        self,
        config_file: Union[Collection[Union[str, Path]], str, Path] = None,
        run_dir: Union[Path, str] = None,
    ):
        """Constructor for RunController class.

        Args:
            config_file: Single or list of config file locations as strings or Path objects.
                Defaults to None.
            run_dir: Model run directory as a Path object or string. If not provided, defaults
                to the directory of the first config_file.
        """
        if run_dir is None:
            run_dir = Path(os.path.abspath(os.path.dirname(config_file[0])))

        self._run_dir = Path(run_dir)

        self.config = Configuration.load_toml(config_file)
        # NOTE: Logger opens log file on __enter__ (in run), not ready for logging yet
        # Logger uses self.config.logging
        self.logger = Logger(self)
        self.top_sheet = None
        self.trace = None
        self.completed_components = []

        # mapping from defined names referenced in config to Component objects
        self._component_map = {k: v(self) for k, v in component_cls_map.items()}
        self._validated_components = set()
        self._emme_manager = None
        self._iteration = None
        self._component = None
        self._component_name = None
        self._queued_components = []

    @property
    def run_dir(self) -> Path:
        """The root run directory of the model run"""
        return self._run_dir

    @property
    def run_iterations(self) -> List[int]:
        """List of iterations for this model run."""
        return range(
            max(1, self.config.run.start_iteration), self.config.run.end_iteration + 1
        )

    @property
    def iteration(self) -> int:
        """Current iteration of model."""
        return self._iteration

    @property
    def iter_component(self) -> Tuple[int, str]:
        """Tuple of the current iteration and component name."""
        return self._iteration, self._component_name

    def component(self) -> Component:
        """Current component of model."""
        return self._component

    @property
    def emme_manager(self) -> EmmeManager:
        """Cached Emme Manager object."""
        if self._emme_manager is None:
            self._init_emme_manager()
        return self._emme_manager

    def _init_emme_manager(self):
        """Initialize Emme manager, start Emme desktop App, and initialize Modeller."""
        self._emme_manager = EmmeManager()
        project = self._emme_manager.project(
            os.path.join(self.run_dir, self.config.emme.project_path)
        )
        # Initialize Modeller to use Emme assignment tools and other APIs
        self._emme_manager.modeller(project)

    def get_abs_path(self, rel_path: Union[Path, str]) -> Path:
        """Get the absolute path from the root run directory given a relative path."""
        if not isinstance(rel_path, Path):
            rel_path = Path(rel_path)
        return os.path.join(self.run_dir, rel_path)

    def validate_inputs(self):
        """Validate inputs files are correct, raise if an error is found."""
        # TODO
        pass

    def run(self):
        """Main interface to run model."""
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
        """Add components per iteration to queue according to input Config."""
        try:
            assert not self._queued_components
        except AssertionError:
            "Components already queued, returning without re-queuing."
            return

        if self.config.run.start_iteration == 0:
            for _c_name in self.config.run.initial_components:
                self._add_component_to_queue(0, _c_name)

        # Queue components which are run for each iteration
        _iteration_x_components = itertools.product(
            self.run_iterations, self.config.run.global_iteration_components
        )

        for _iteration, _c_name in _iteration_x_components:
            self._add_component_to_queue(_iteration, _c_name)

        # Queue components which are run after final iteration
        _finalizer_iteration = self.config.run.end_iteration + 1

        for c_name in self.config.run.final_components:
            self._add_component_to_queue(_finalizer_iteration, _c_name)

        # If start_component specified, remove things before its first occurance
        if self.config.run.start_component:
            _queued_c_names = [c.name for c in self._queued_components]
            _start_c_index = _queued_c_names.index(self.config.run.start_component)
            self._queued_components = self._queued_components[_start_c_index:]

    def _add_component_to_queue(self, iteration: int, component_name: str):
        """Add component to queue (self._queued_components), first validating its inputs.

        Args:
            iteration (int): iteration to add component to.
            component (Component): Component to add to queue.
        """
        _component = self._component_map[component_name]
        if component_name not in self._validated_components:
            _component.validate_inputs()
            self._validated_components.add(component_name)
        self._queued_components.append((self._iteration, component_name, _component))
