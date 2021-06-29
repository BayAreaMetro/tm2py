"""Component contains the abstract classes for the model Controller and Components.

"""

from abc import ABC, abstractmethod
from contextlib import contextmanager as _context


class Controller(ABC):
    """Base controller class for tm2py operating tm2py model."""
    def __init__(self):
        super().__init__()
        self._config = None
        self._logger = None
        self._top_sheet = None
        self._trace = None

    @_context
    def setup(self):
        """Placeholder setup and teardown"""
        try:
            yield
        finally:
            pass

    @property
    def config(self):
        """Return configuration interface"""
        return self._config

    @property
    def top_sheet(self):
        """Placeholder for topsheet interface"""
        return self._top_sheet

    @property
    def logger(self):
        """Placeholder for logger interface"""
        return self._logger

    @property
    def trace(self):
        """Placeholder for trace information"""
        return self._trace

    def validate_inputs(self):
        """Validate inputs are correct at model initiation, fail fast if not"""

    @abstractmethod
    def run(self):
        """Run model component"""

    def report_progress(self):
        """Write progress to log file"""

    def test_component(self):
        """Run stand-alone component test"""

    def write_top_sheet(self):
        """Write key outputs to the model top sheet"""


class Component(ABC):
    """Base component class for tm2py top-level inheritance"""

    def __init__(self, controller: Controller):
        super().__init__()
        self._controller = controller
        self._trace = None

    @property
    def controller(self):
        """Parent controller"""
        return self._controller

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
    def run(self):
        """Run model component"""

    def report_progress(self):
        """Write progress to log file"""

    def test_component(self):
        """Run stand-alone component test"""

    def write_top_sheet(self):
        """Write key outputs to the model top sheet"""
