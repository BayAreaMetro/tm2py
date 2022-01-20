""" tktk
"""
import os
from abc import ABC, abstractmethod

import tm2py.controller as _controller


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

    def __init__(self, controller: '_controller.RunController'):
        super().__init__()
        self._controller = controller
        self._trace = None

    @property
    def controller(self):
        """Parent controller"""
        return self._controller

    def get_abs_path(self, rel_path: str):
        """Get the absolute path from the root run directory given a relative path."""
        return os.path.join(self.controller.run_dir, rel_path)

    def get_emme_scenario(self, emmebank_path: str, time_period: str):
        """Get the Emme scenario object from the Emmebank at emmebank_path for the time_period ID.

        Args:
            emmebank_path: valid Emmebank path, relative to root run directory
            time_period: valid time_period ID
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
        """Configuration settings loaded from config files"""
        return self.controller.config

    @property
    def top_sheet(self):
        """docstring placeholder for top sheet"""
        return self.controller.top_sheet

    @property
    def logger(self):
        """docstring placeholder for logger"""
        return self.controller.logger

    @property
    def trace(self):
        """docstring placeholder for trace"""
        return self._trace

    def validate_inputs(self):
        """Validate inputs are correct at model initiation, fail fast if not"""

    @abstractmethod
    def run(self, time_periods=None):
        """Run model component"""

    def report_progress(self):
        """Write progress to log file"""

    def test_component(self):
        """Run stand-alone component test"""

    def write_top_sheet(self):
        """Write key outputs to the model top sheet"""

    def verify(self):
        """Verify component ouputs / results"""
