"""Module for Emme Manager for centralized management of Emme projects

Centralized location for Emme API imports, which are automatically replaced
by unittest.Mock / MagicMock to support testing where Emme is not installed.

Contains EmmeManager class for access to common Emme-related procedures
(common-code / utility-type methods) and caching access to Emme project,
and Modeller.
"""

from contextlib import contextmanager as _context
import os
from socket import error as _socket_error
from typing import Any, Dict, List, Union

try:
    # skip Emme import to support testing where Emme is not installed
    # Note: some imports unused (W0611 message), so as to be available
    #       to other tools, all Emme imports in one place, and can
    #       be replaced with a Mock for testing
    # PyLint cannot build AST from compiled Emme libraries
    # so disabling relevant import module checks
    # pylint: disable=E0611, E0401, E1101
    from inro.emme.database.emmebank import Emmebank
    from inro.emme.network import Network as EmmeNetwork
    from inro.emme.database.scenario import Scenario as EmmeScenario
    from inro.emme.database.matrix import Matrix as EmmeMatrix  # pylint: disable=W0611
    from inro.emme.network.node import Node as EmmeNode  # pylint: disable=W0611
    import inro.emme.desktop.app as _app
    from inro.modeller import Modeller as EmmeModeller, logbook_write, logbook_trace

    EmmeDesktopApp = _app.App
except ModuleNotFoundError:
    if "PYTEST_CURRENT_TEST" not in os.environ:
        raise
    # if running from pytest replace objects with Mocks
    # pylint: disable=C0103
    from unittest.mock import Mock, MagicMock
    from numpy import zeros

    EmmeNetwork = Mock()
    EmmeNetwork.links = MagicMock(return_value=[])
    EmmeNetwork.nodes = MagicMock(return_value=[])
    EmmeScenario = Mock()
    EmmeScenario.get_network = MagicMock(return_value=EmmeNetwork)
    EmmeScenario.get_partial_network = MagicMock(return_value=EmmeNetwork)
    EmmeScenario.zone_numbers = list(range(43))

    EmmeMatrix = Mock()
    EmmeMatrix.get_numpy_data = MagicMock(return_value=zeros([43, 43]))
    matrix_ids = iter(range(99999))
    type(EmmeMatrix).name = property(
        fget=lambda s: "test" + str(next(matrix_ids)), fset=lambda s, v: None
    )
    EmmeMatrix.description = "testtest"

    EmmebankMock = Mock()
    EmmebankMock.matrix = Mock(return_value=EmmeMatrix)
    EmmebankMock.scenario = Mock(return_value=EmmeScenario)
    EmmebankMock.path = ""
    EmmeScenario.emmebank = EmmebankMock
    Emmebank = Mock(return_value=EmmebankMock)

    EmmeDesktopApp = Mock()
    EmmeModeller = Mock()
    EmmeNode = Mock()
    logbook_write = Mock()
    logbook_trace = MagicMock()
    _app = Mock()

# Cache running Emme projects from this process (simple singleton implementation)
_EMME_PROJECT_REF = {}


class EmmeManager:
    """Centralized cache for Emme project and related calls for traffic and transit assignments.

    Wraps Emme Desktop API (see Emme API Reference for additional details on the Emme objects).
    """

    def __init__(self):
        # mapping of Emme project path to Emme Desktop API object for reference
        # (projects are opened only once)
        self._project_cache = _EMME_PROJECT_REF

    def close_all(self):
        """
        Close all open cached Emme project(s).

        Should be called at the end of the model process / Emme assignments.
        """
        while self._project_cache:
            _, app = self._project_cache.popitem()
            app.close()

    def create_project(self, project_dir: str, name: str) -> EmmeDesktopApp:
        """Create, open and return Emme project

        Args:
            project_dir: path to Emme root directory for new Emme project
            name: name for the Emme project

        Returns:
            Emme Desktop App object, see Emme API Reference, Desktop section for details.
        """
        emp_path = _app.create_project(project_dir, name)
        return self.project(emp_path)

    def project(self, project_path: str) -> EmmeDesktopApp:
        """Return already open Emme project, or open new Desktop session if not found.

        Args:
            project_path: valid path to Emme project *.emp file

        Returns:
            Emme Desktop App object, see Emme API Reference, Desktop section for details.
        """
        project_path = os.path.normcase(os.path.realpath(project_path))
        emme_project = self._project_cache.get(project_path)
        if emme_project:
            try:  # Check if the Emme window was closed
                emme_project.current_window()
            except _socket_error:
                emme_project = None
        # if window is not opened in this process, start a new one
        if emme_project is None:
            if not os.path.isfile(project_path):
                raise Exception(f"Emme project path does not exist {project_path}")
            emme_project = _app.start_dedicated(
                visible=True, user_initials="inro", project=project_path
            )
            self._project_cache[project_path] = emme_project
        return emme_project

    @staticmethod
    def emmebank(path: str) -> Emmebank:
        """Open and return the Emmebank at path.

        Args:
            path: valid system path pointing to an Emmebank file
        Returns:
            Emmebank object, see Emme API Reference, Database section for details.
        """
        if not path.endswith("emmebank"):
            path = os.path.join(path, "emmebank")
        return Emmebank(path)

    def change_emmebank_dimensions(
        self, emmebank: Emmebank, dimensions: Dict[str, int]
    ):
        """Change the Emmebank dimensions as specified. See the Emme API help for details.

        Args:
            emmebank: the Emmebank object to change the dimensions
            dimensions: dictionary of the specified dimensions to set.
        """
        dims = emmebank.dimensions
        new_dims = dims.copy()
        new_dims.update(dimensions)
        if dims != new_dims:
            change_dimensions = self.tool(
                "inro.emme.data.database.change_database_dimensions"
            )
            change_dimensions(new_dims, emmebank, keep_backup=False)

    def modeller(self, emme_project: EmmeDesktopApp = None) -> EmmeModeller:
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
        try:
            return EmmeModeller()
        except AssertionError as error:
            if emme_project is None:
                if self._project_cache:
                    emme_project = next(iter(self._project_cache.values()))
                else:
                    raise Exception(
                        "modeller not yet initialized and no cached Emme project,"
                        " emme_project arg must be provided"
                    ) from error
            return EmmeModeller(emme_project)

    def tool(self, namespace: str):
        """Return the Modeller tool at namespace.

        Returns:
            Corresponding Tool object, see Emme Help for full details.
        """
        return self.modeller().tool(namespace)

    @staticmethod
    @_context
    def temp_attributes_and_restore(
        scenario: EmmeScenario, attributes: List[List[str]]
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

    @staticmethod
    def copy_attr_values(
        domain: str,
        src: Union[EmmeScenario, EmmeNetwork],
        dst: Union[EmmeScenario, EmmeNetwork],
        src_names: List[str],
        dst_names: List[str] = None,
    ):
        """Copy attribute values between Emme scenario (on disk) and network (in memory).

        Args:
            domain: attribute domain, one of "NODE", "LINK", "TURN", "TRANSIT_LINE",
                "TRANSIT_SEGMENT"
            src: source Emme scenario or network to load values from
            dst: destination Emme scenario or network to save values to
            src_names: names of the attributes for loading values
            dst_names: optional, names of the attributes to save values as, defaults
                to using the src_names if not specified

        Returns:
            Emme Modeller object, see Emme API Reference, Modeller section for details.
        """
        if dst_names is None:
            dst_names = src_names
        values = src.get_attribute_values(domain, src_names)
        dst.set_attribute_values(domain, dst_names, values)

    def get_network(
        self, scenario: EmmeScenario, attributes: Dict[str, List[str]] = None
    ) -> EmmeNetwork:
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
        for domain, attrs in attributes.items():
            if attrs:
                self.copy_attr_values(domain, scenario, network, attrs)
        return network

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
