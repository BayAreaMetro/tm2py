"""Root component ABC."""

from __future__ import annotations

import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, List, Union

from tm2py.emme.manager import Emmebank, EmmeScenario

if TYPE_CHECKING:
    from tm2py.controller import RunController


class FileFormatError(Exception):
    """Exception raised when a file is not in the expected format."""

    def __init__(self, f, *args):
        """Exception for invalid file formats."""
        super().__init__(args)
        self.f = f

    def __str__(self):
        """String representation for FileFormatError."""
        return f"The {self.f} is not a valid format."


class Component(ABC):
    """Template for Component class with several built-in methods.

    A component is a piece of the model that can be run independently (of other components) given
    the required input data and configuration.  It communicates information to other components via
    disk I/O (including the emmebank).

    Note: if the component needs data that is not written to disk, it would be considered a
    subcomponent.

    Abstract Methods – Each component class must have the following methods:
        __init___: constructor, which associates the RunController with the instantiated object
        run: run the component without any arguments
        validate_inputs: validate the inputs to the component
        report_progress: report progress to the user
        verify: verify the component's output
        write_top_sheet: write outputs to topsheet
        test_component: test the component

    Template Class methods - component classes inherit:
        get_abs_path: convenience method to get absolute path of the run directory


    Template Class Properties - component classes inherit:
        controller: RunController object
        config: Config object
        time_period_names: convenience property
        top_sheet: topsheet object
        logger: logger object
        trace: trace object

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

    @property
    def emme_manager(self):
        return self.controller.emme_manager

    def get_abs_path(self, path: Union[Path, str]) -> str:
        """Convenince method to get absolute path from run directory."""
        if not os.path.isabs(path):
            return self.controller.get_abs_path(path).__str__()
        else:
            return path

    @property
    def time_period_names(self) -> List[str]:
        """Return input time_period name or names and return list of time_period names.

        Implemented here for easy access for all components.

        Returns: list of uppercased string names of time periods
        """
        return self.controller.time_period_names

    @property
    def time_period_durations(self) -> dict:
        """Return mapping of time periods to durations in hours."""
        return self.controller.time_period_durations

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

    @abstractmethod
    def validate_inputs(self):
        """Validate inputs are correct at model initiation, raise on error."""

    @abstractmethod
    def run(self):
        """Run model component."""

    # @abstractmethod
    def report_progress(self):
        """Write progress to log file."""

    # @abstractmethod
    def verify(self):
        """Verify component outputs / results."""

    # @abstractmethod
    def write_top_sheet(self):
        """Write key outputs to the model top sheet."""


class Subcomponent(Component):
    """Template for sub-component class.

    A sub-component is a more loosly defined component that allows for input into the run()
    method.  It is used to break-up larger processes into smaller chunks which can be:
    (1) re-used across components (i.e toll choice)
    (2) updated/subbed in to a parent component(s) run method based on the expected API
    (3) easier to test, understand and debug.
    (4) more consistent with the algorithms we understand from transportation planning 101
    """

    def __init__(self, controller: RunController, component: Component):
        """Constructor for model sub-component abstract base class.

        Only calls the super class constructor.

        Args:
            controller (RunController): Reference to the run controller object.
            component (Component): Reference to the parent component object.
        """
        super().__init__(controller)
        self.component = component

    @abstractmethod
    def run(self, *args, **kwargs):
        """Run sub-component, allowing for multiple inputs.

        Allowing for inputs to the run() method is what differentiates a sub-component from
        a component.
        """
