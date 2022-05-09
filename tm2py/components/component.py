"""Root component ABC."""

from __future__ import annotations

import os
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, List

from tm2py.emme.manager import EmmeScenario

if TYPE_CHECKING:
    from tm2py.controller import RunController


class Component(ABC):
    """Base component class for tm2py top-level inheritance.

    Example:
    ::
        class MyComponent(Component):

        def __init__(self, controller):
            super().__init__(controller)
            self._parameter = None

        def run(self):
            self._step1()
            self._step2()

        def _step1(self):
            pass

        def _step2(self):
            pass
    """

    def __init__(self, controller: RunController):
        """Model component template/abstract base class.

        Args:
            controller (RunController): Reference to the run controller object.
        """
        self._controller = controller
        self._trace = None

    @property
    def controller(self):
        """Parent controller."""
        return self._controller

    def get_abs_path(self, rel_path: str):
        """Get the absolute path from the root run directory given a relative path."""
        return os.path.join(self.controller.run_dir, rel_path)

    def get_emme_scenario(self, emmebank_path: str, time_period: str) -> EmmeScenario:
        """Get the Emme scenario object from the Emmebank at emmebank_path for the time_period ID.

        Args:
            emmebank_path: valid Emmebank path, absolute or relative to root run directory
            time_period: valid time_period ID

        Returns
            Emme Scenario object (see Emme API Reference)
        """
        if not os.path.isabs(emmebank_path):
            emmebank_path = self.get_abs_path(emmebank_path)
        emmebank = self.controller.emme_manager.emmebank(emmebank_path)
        scenario_id = {tp.name: tp.emme_scenario_id for tp in self.config.time_periods}[
            time_period
        ]
        return emmebank.scenario(scenario_id)

    @property
    def config(self):
        """Reference to configuration settings loaded from config files."""
        return self.controller.config

    @property
    def top_sheet(self):
        """Reference to top sheet."""
        return self.controller.top_sheet

    @property
    def logger(self):
        """Reference to logger."""
        return self.controller.logger

    @property
    def trace(self):
        """Reference to trace."""
        return self._trace

    def validate_inputs(self):
        """Validate inputs are correct at model initiation, raise on error"""

    @abstractmethod
    def run(self):
        """Run model component."""

    def report_progress(self):
        """Write progress to log file."""

    def test_component(self):
        """Run stand-alone component test."""

    def write_top_sheet(self):
        """Write key outputs to the model top sheet."""

    def verify(self):
        """Verify component outputs / results."""

    def time_period_names(self) -> List[str]:
        """Return input time_period name or names and return list of time_period names.

        Returns: list of string names of time periods
        """
        return [time.name for time in self.config.time_periods]
