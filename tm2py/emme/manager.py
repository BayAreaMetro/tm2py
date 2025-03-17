"""Module for Emme Manager for centralized management of Emme projects.

Centralized location for Emme API imports, which are automatically replaced
by unittest.Mock / MagicMock to support testing where Emme is not installed.

Contains EmmeManager class for access to common Emme-related procedures
(common-code / utility-type methods) and caching access to Emme project,
and Modeller.
"""

import json as _json
import multiprocessing
import os
import re
import shutil as _shutil
import subprocess as _subprocess
import sys
import time as _time
from abc import ABC, abstractmethod
from contextlib import contextmanager as _context
from pathlib import Path
from socket import error as _socket_error
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from typing_extensions import Literal

from ..tools import emme_context

emme_context()

import inro.emme.desktop.app as _app

if TYPE_CHECKING:
    from tm2py.config import EmmeConfig
    from tm2py.controller import RunController
    from tm2py.emme.manager import EmmeNetwork, EmmeScenario

# PyLint cannot build AST from compiled Emme libraries
# so disabling relevant import module checks
# pylint: disable=E0611, E0401, E1101
# Importing several Emme object types which are unused here, but so that
# the Emme API import are centralized within tm2py
from inro.emme.database.emmebank import Emmebank as Emmebank
from inro.emme.database.matrix import Matrix as EmmeMatrix  # pylint: disable=W0611
from inro.emme.database.scenario import Scenario as EmmeScenario
from inro.emme.network import Network as EmmeNetwork
from inro.emme.network.link import Link as EmmeLink  # pylint: disable=W0611
from inro.emme.network.mode import Mode as EmmeMode  # pylint: disable=W0611
from inro.emme.network.node import Node as EmmeNode  # pylint: disable=W0611
from inro.modeller import Modeller as EmmeModeller
from inro.modeller import logbook_trace, logbook_write

EmmeDesktopApp = _app.App

# "Emme Manager requires Emme to be installed unless running in a test environment."
# "Please install Emme and try again."

# Cache running Emme projects from this process (simple singleton implementation)
_EMME_PROJECT_REF = {}


def parse_num_processors(value: Union[str, int]) -> int:
    cpu_processors = multiprocessing.cpu_count()
    num_processors = 0
    if isinstance(value, str):
        if value.upper() == "MAX":
            num_processors = cpu_processors
        elif re.match("^[0-9]+$", value):
            num_processors = int(value)
        else:
            minus_processors = re.split(r"^MAX[/s]*-[/s]*", value.upper())
            num_processors = max(cpu_processors - int(minus_processors[1]), 1)
    else:
        num_processors = int(value)

    num_processors = min(cpu_processors, num_processors)
    num_processors = max(1, num_processors)
    return num_processors


class ProxyEmmebank:
    """Emmebank wrapper class."""

    def __init__(self, emme_manager, path: Union[str, Path]):
        self.emme_manager = emme_manager
        self.controller = self.emme_manager.controller
        self._path = Path(path)
        self._emmebank = None
        self._zero_matrix = None
        self.scenario_dict = {
            tp.name: tp.emme_scenario_id for tp in self.controller.config.time_periods
        }

    @property
    def emmebank(self) -> Emmebank:
        """The underlying reference Emmebank object
        (inro.emme.database.emmebank.Emmebank)"""
        if self._emmebank is None:
            self._emmebank = Emmebank(self.path)
        return self._emmebank

    @property
    def path(self) -> Path:
        """Return the path to the Emmebank."""
        if not self._path.exists():
            self._path = self.controller.get_abs_path(self._path)
        if not self._path.exists():
            raise (FileNotFoundError(f"Emmebank not found: {self._path}"))
        if not self._path.__str__().endswith("emmebank"):
            self._path = os.path.join(self._path, "emmebank")
        return self._path

    def change_dimensions(self, dimensions: Dict[str, int]):
        """Change the Emmebank dimensions as specified. See the Emme API help for details.

        Args:
            emmebank: the Emmebank object to change the dimensions
            dimensions: dictionary of the specified dimensions to set.
        """
        dims = self.emmebank.dimensions
        new_dims = dims.copy()
        new_dims.update(dimensions)
        if dims != new_dims:
            change_dimensions = self.emme_manager.tool(
                "inro.emme.data.database.change_database_dimensions"
            )
            change_dimensions(new_dims, self.emmebank, keep_backup=False)

    def scenario(self, time_period: str):
        """Return the EmmeScenario for the given time period.

        Args:
            time_period: valid time period abbreviation
        """
        _scenario_id = self.scenario_dict[time_period.lower()]
        return self.emmebank.scenario(_scenario_id)

    def create_matrix(
        self,
        name: str,
        matrix_type: Literal["SCALAR", "FULL", "ORIGIN", "DESTINATION"] = "FULL",
        description: str = None,
        default_value: float = 0,
    ):
        """Create ms"zero" matrix for zero-demand assignments."""
        prefix = {"SCALAR": "ms", "FULL": "mf", "ORIGIN": "mo", "DESTINATION": "md"}[
            matrix_type
        ]
        matrix = self.emmebank.matrix(f'{prefix}"{name}"')
        if matrix is None:
            ident = self.emmebank.available_matrix_identifier(matrix_type)
            matrix = self.emmebank.create_matrix(ident, default_value=default_value)
            matrix.name = name
            matrix.description = name if description is None else description
        elif matrix_type == "SCALAR":
            matrix.data = default_value
        return matrix

    def create_zero_matrix(self):
        """Create ms"zero" matrix for zero-demand assignments."""
        for matrix in self.emmebank.matrices():
            if matrix.name == "zero":
                self.emmebank.delete_matrix(matrix)
        if self._zero_matrix is None:
            self._zero_matrix = self.create_matrix(
                "zero", "SCALAR", "zero demand matrix", 0
            )
        return self._zero_matrix


class EmmeManagerLight:
    """Base class for EMME access point.

    Support access to EMME-related APIs separately from the controller / config
    in tm2py. Should only be used for launching separate processes for running
    EMME tasks.
    """

    def __init__(self, project_path):
        self.project_path = os.path.normcase(os.path.abspath(project_path))
        self._project = None
        self._modeller = None

    @property
    def project(self) -> EmmeDesktopApp:
        """Return already open Emme project, or open new Desktop session if not found.

        Args:
            project_path: valid path to Emme project *.emp file

        Returns:
            Emme Desktop App object, see Emme API Reference, Desktop section for details.
        """
        if self._project is None:
            # lookup if already in opened projects
            self._project = _EMME_PROJECT_REF.get(self.project_path)

        if self._project is not None:
            try:  # Check if the Emme window was closed
                self._project.current_window()
            except _socket_error:
                self._project = None
        else:
            # if window is not opened in this process, start a new one
            self._project = _app.start_dedicated(
                visible=True, user_initials="mtc", project=self.project_path
            )
            _EMME_PROJECT_REF[self.project_path] = self._project
        return self._project

    @property
    def modeller(self) -> EmmeModeller:
        """Initialize and return Modeller object.

        If Modeller has not already been initialized it will do so on
        specified Emme project, or the first Emme project opened if not provided.
        If already initialized Modeller will reference whichever project was used
        first.

        Args:
            emme_project: open 'Emme Desktop' application (inro.emme.desktop.app)

        Returns:
            Emme Modeller object, see Emme API Reference, Modeller section for details.
        """
        # pylint: disable=E0611, E0401, E1101
        if self._modeller is None:
            try:
                self._modeller = EmmeModeller(self.project)
            except (AssertionError, RuntimeError):
                self._modeller = EmmeModeller()
                self._project = self._modeller.desktop
        return self._modeller

    def tool(self, namespace: str):
        """Return the Modeller tool at namespace.

        Returns:
            Corresponding Tool object, see Emme Help for full details.
        """
        return self.modeller.tool(namespace)

    @property
    def matrix_calculator(self):
        "Shortcut to matrix calculator."
        return self.tool("inro.emme.matrix_calculation.matrix_calculator")

    @property
    def matrix_results(self):
        "Shortcut to matrix results."
        return self.tool("inro.emme.transit_assignment.extended.matrix_results")

    @staticmethod
    def copy_attribute_values(
        src,
        dst,
        src_attributes: Dict[str, List[str]],
        dst_attributes: Optional[Dict[str, List[str]]] = None,
    ):
        """Copy network/scenario attribute values from src to dst.

        Args:
            src: Emme scenario object or Emme Network object
            dst: Emme scenario object or Emme Network object
            src_attributes: dictionary or Emme network domain to list of attribute names
                NODE, LINK, TURN, TRANSIT_LINE, TRANSIT_SEGMENT
            dst_attributes: Optional, names to use for the attributes in the dst object,
                if not specified these are the same as src_attributes
        """
        for domain, src_attrs in src_attributes.items():
            if src_attrs:
                dst_attrs = src_attrs
                if dst_attributes is not None:
                    dst_attrs = dst_attributes.get(domain, src_attrs)
                values = src.get_attribute_values(domain, src_attrs)
                dst.set_attribute_values(domain, dst_attrs, values)

    @staticmethod
    @_context
    def temp_attributes_and_restore(
        scenario: "EmmeScenario", attributes: List[List[str]]
    ):
        """Create temp extra attribute and network field, and backup values and state and restore.

        Allows the use of temporary attributes which may conflict with existing attributes.
        The temp created attributes are deleted at the end, and if there were pre-existing
        attributes with the same names the values are restored.

        Note that name conflicts may still arise in the shorthand inheritance systems
        for the network hierarchy tree (@node attribute reserves -> @nodei, @nodej, etc,
        see Emme help Network calculations for full list) which will raise an error in the
        Emme API.

        Args:
            scenario: Emme scenario object
            attributes: list of attribute details, where details is a list of 3 items
                for extra attributes and 4 for network fields: domain, name, description[, atype]
        """
        attrs_to_delete = []
        fields_to_delete = []
        attrs_to_restore = dict(
            (d, []) for d in ["NODE", "LINK", "TURN", "TRANSIT_LINE", "TRANSIT_SEGMENT"]
        )
        for details in attributes:
            domain, name, desc = details[:3]
            attr = scenario.extra_attribute(name)
            field = scenario.network_field(domain, name)
            if attr or field:
                attrs_to_restore[domain].append(name)
            elif name.startswith("@"):
                attr = scenario.create_extra_attribute(domain, name)
                attr.description = desc
                attrs_to_delete.append(name)
            else:
                atype = details[3]
                field = scenario.create_nertwork_field(domain, name, atype)
                field.description = desc
                fields_to_delete.append((domain, name))
        backup = []
        for domain, names in attrs_to_restore.items():
            if names:
                backup.append(
                    (domain, names, scenario.get_attribute_values(domain, names))
                )
        try:
            yield
        finally:
            for name in attrs_to_delete:
                scenario.delete_extra_attribute(name)
            for domain, name in fields_to_delete:
                scenario.delete_network_field(domain, name)
            for domain, names, values in backup:
                scenario.set_attribute_values(domain, names, values)

    def get_network(
        self, scenario: "EmmeScenario", attributes: Dict[str, List[str]] = None
    ) -> "EmmeNetwork":
        """Read partial Emme network from the scenario for the domains and attributes specified.

        Optimized load of network object from scenario (disk / emmebank) for only the
        domains specified, and only reads the attributes specified. The attributes is a
        dictionary with keys for the required domains, and values as lists of the
        attributes required by domain.

        Wrapper for scenario.get_partial_network followed by scenario.get_attribute_values
        and network.set_attribute_values.

        Args:
            scenario: Emme scenario object, see Emme API reference
            attributes: dictionary of domain names to lists of attribute names

        Returns:
            Emme Network object, see Emme API Reference, Network section for details.
        """
        if attributes is None:
            return scenario.get_network()
        network = scenario.get_partial_network(
            attributes.keys(), include_attributes=False
        )
        self.copy_attribute_values(scenario, network, attributes)
        return network

    def add_database(self, path):
        path = os.path.realpath(path)
        for db in self.project.data_explorer().databases():
            if os.path.realpath(db.path) == path:
                if not db.is_open:
                    db.open()
                return
        db = self.project.data_explorer().add_database(path)
        db.open()
        self.project.project.save()

    @staticmethod
    def logbook_write(name: str, value: str = None, attributes: Dict[str, Any] = None):
        """Write an entry to the Emme Logbook at the current nesting level.

        Wrapper for inro.modeller.logbook_write.

        Args:
            name: The title of the logbook entry
            attributes: Optional. A Python dictionary of key-value pairs to be
                        displayed in the logbook entry detailed view.
            value: Optional. An HTML string value to be displayed in main detail
                   pane of the logbook entry
        """
        # pylint: disable=E0611, E0401, E1101
        attributes = attributes if attributes else {}
        logbook_write(name, value=value, attributes=attributes)

    @staticmethod
    @_context
    def logbook_trace(name: str, value: str = None, attributes: Dict[str, Any] = None):
        """Write an entry to the Modeller logbook and create a nest in the Logbook.

        Wrapper for inro.modeller.logbook_trace. Used in the with statement, e.g.

        ```
        with _emme_tools.logbook_trace('My nest'):
            _emme_tools.logbook_write('This entry is nested')
        ```

        Args:
            name: The title of the logbook entry
            attributes: Optional. A Python dictionary of key-value pairs to be
                        displayed in the logbook entry detailed view.
            value: Optional. An HTML string value to be displayed in main detail
                   pane of the logbook entry.
        """
        # pylint: disable=E0611, E0401, E1101
        attributes = attributes if attributes else {}
        with logbook_trace(name, value=value, attributes=attributes):
            yield

    def close(self):
        """Close all open cached Emme project(s).

        Should be called at the end of the model process / Emme assignments.
        """
        self._project.close()


class EmmeManager(EmmeManagerLight):
    """Centralized cache for a single Emme project and related calls.

    Leverages EmmeConfig.

    Wraps Emme Desktop API (see Emme API Reference for additional details on the Emme objects).
    """

    def __init__(self, controller: "RunController", emme_config: "EmmeConfig"):
        """The EmmeManager constructor.

        Maps an Emme project path to Emme Desktop API object for reference
        (projects are opened only once).
        """
        self.controller = controller
        self.config = emme_config
        project_path = self.controller.get_abs_path(self.config.project_path)
        super().__init__(project_path)

        # see if works without os.path.normcase(os.path.realpath(project_path))
        self.highway_database_path = self.controller.get_abs_path(
            self.config.highway_database_path
        )
        self.transit_database_path = self.controller.get_abs_path(
            self.config.transit_database_path
        )
        self.active_north_database_path = self.controller.get_abs_path(
            self.config.active_north_database_path
        )
        self.active_south_database_path = self.controller.get_abs_path(
            self.config.active_south_database_path
        )

        self._highway_emmebank = None
        self._transit_emmebank = None
        self._active_north_emmebank = None
        self._active_south_emmebank = None
        self._num_processors = None

        # Initialize Modeller to use Emme assignment tools and other APIs
        # inializing now will raise error sooner in case of EMME configuration
        # issues (rather than waiting to get to the assignments)
        self._modeller = self.modeller

    @property
    def highway_emmebank(self) -> ProxyEmmebank:
        if self._highway_emmebank is None:
            self._highway_emmebank = ProxyEmmebank(self, self.highway_database_path)
        return self._highway_emmebank

    @property
    def transit_emmebank(self) -> ProxyEmmebank:
        if self._transit_emmebank is None:
            self._transit_emmebank = ProxyEmmebank(self, self.transit_database_path)
        return self._transit_emmebank

    @property
    def active_north_emmebank(self) -> ProxyEmmebank:
        if self._active_north_emmebank is None:
            self._active_north_emmebank = ProxyEmmebank(
                self, self.active_north_database_path
            )
        return self._active_north_emmebank

    @property
    def active_south_emmebank(self) -> ProxyEmmebank:
        if self._active_south_emmebank is None:
            self._active_south_emmebank = ProxyEmmebank(
                self, self.active_south_database_path
            )
        return self._active_south_emmebank

    @property
    def num_processors(self):
        """Convert input value (parse if string) to number of processors.

        Must be an int or string as 'MAX' or 'MAX-X', capped between 1 and the maximum available
        processors.

        Returns:
            An int of the number of processors to use.

        """
        if self._num_processors is None:
            self._num_processors = parse_num_processors(self.config.num_processors)
        return self._num_processors
