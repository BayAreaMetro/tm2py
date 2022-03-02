"""Module for Emme Manager for centralized management of Emme projects"""

from contextlib import contextmanager as _context
import os
from socket import error as _socket_error
from typing import Any, Dict, List, Union

try:
    # skip Emme import to support testing where Emme is not installed

    # PyLint cannot build AST from compiled Emme libraries
    # so disabling relevant import module checks
    # pylint: disable=E0611, E0401, E1101
    from inro.emme.database.emmebank import Emmebank
    from inro.emme.database.scenario import Scenario as EmmeScenario
    import inro.emme.desktop.app as _app
    import inro.modeller as _m

    EmmeDesktopApp = _app.App
    EmmeModeller = _m.Modeller
except ModuleNotFoundError:
    # pylint: disable=C0103
    Emmebank = None
    EmmeScenario = None
    EmmeDesktopApp = None
    EmmeModeller = None

# Cache running Emme projects from this process (simple singleton implementation)
_EMME_PROJECT_REF = {}


class EmmeManager:
    """Centralized cache for Emme project and related calls for traffic and transit assignments.

    Wraps Emme Desktop API (see Emme API Reference for additional details on the Emme objects).
    """

    def __init__(self):
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
        """
        emp_path = _app.create_project(project_dir, name)
        return self.project(emp_path)

    def project(self, project_path: str) -> EmmeDesktopApp:
        """Return already open Emme project, or open new Desktop session if not found.

        Args:
            project_path: valid path to Emme project *.emp file
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

    def prepare_zero_matrix(self, emmebank: Union[Emmebank, str]):
        """Create zero "demand" matrix from assignments.

        Creates a new matrix named "zero" in the specified emmebank if it does
        not already exist.

        Args:
            emmebank: the Emmebank object or path to change the dimensions
        """
        if not isinstance(emmebank, Emmebank):
            emmebank = self.emmebank(emmebank)
        zero_matrix = emmebank.matrix('ms"zero"')
        if zero_matrix is None:
            ident = emmebank.available_matrix_identifier("SCALAR")
            zero_matrix = emmebank.create_matrix(ident)
            zero_matrix.name = "zero"
            zero_matrix.description = "zero demand matrix"
        zero_matrix.data = 0

    def modeller(self, emme_project: EmmeDesktopApp = None) -> EmmeModeller:
        """Initialize and return Modeller object.

        If Modeller has not already been initialized it will do so on
        specified Emme project, or the first Emme project opened if not provided.
        If already initialized Modeller will reference whichever project was used
        first.

        Args:
            emme_project: open 'Emme Desktop' application (inro.emme.desktop.app)
        """
        # pylint: disable=E0611, E0401, E1101
        try:
            return _m.Modeller()
        except AssertionError as error:
            if emme_project is None:
                if self._project_cache:
                    emme_project = next(iter(self._project_cache.values()))
                else:
                    raise Exception(
                        "modeller not yet initialized and no cached Emme project,"
                        " emme_project arg must be provided"
                    ) from error
            return _m.Modeller(emme_project)

    def tool(self, namespace: str):
        """Return the Modeller tool at namespace."""
        return self.modeller().tool(namespace)

    @staticmethod
    def copy_attribute_values(src, dst, attributes: Dict[str, List[str]]):
        """Copy network/scenario attribute values from src to dst.

        Args:
            src: Emme scenario object or Emme Network object
            dst: Emme scenario object or Emme Network object
            attributes: dictionary or Emme network domain to list of attribute names
                NODE, LINK, TURN, TRANSIT_LINE, TRANSIT_SEGMENT
        """
        for domain, attrs in attributes.items():
            values = src.get_attribute_values(domain, attrs)
            dst.set_attribute_values(domain, attrs, values)

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
        _m.logbook_write(name, value=value, attributes=attributes)

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
        with _m.logbook_trace(name, value=value, attributes=attributes):
            yield
