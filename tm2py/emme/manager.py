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
from inro.emme.database.emmebank import Emmebank as Emmebank, create as _create_emmebank
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
    """Process input value as integer or formatted string into
       number of processors to use. Caps between 1 and maximum
       available according to multiprocessing.cpu_count().

       Returns integer number of processors to use.

       value: either an integer or string as "MAX" or "MAX-N" or "MAX/N"
    """
    cpu_processors = multiprocessing.cpu_count()
    num_processors = 0
    if isinstance(value, str):
        value = value.upper().strip()
        if value == "MAX":
            num_processors = cpu_processors
        elif re.match("^[0-9]+$", value):
            num_processors = int(value)
        elif re.match(r"^MAX[\s]*-[\s]*\d+$", value):
            minus_processors = re.split(r"^MAX[\s]*-[\s]*", value)
            num_processors = max(cpu_processors - int(minus_processors[1]), 1)
        else:
            fraction = re.split(r"^MAX[\s]*/[\s]*", value)
            num_processors = max(int(cpu_processors / int(fraction[1])), 1)
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
    EMME tasks, i.e. highway or transit assignments.
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
        "Add new EMMEBANK database at path"
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
        Args:
            controller: the parent RunController of tm2py
            emme_config: the config.emme entry of tm2py
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

        Must be an int or string as 'MAX' or 'MAX-X' or 'MAX/N',
        capped between 1 and the maximum available processors.

        Returns:
            An int of the number of processors to use.

        """
        if self._num_processors is None:
            self._num_processors = parse_num_processors(self.config.num_processors)
        return self._num_processors


class BaseAssignmentLauncher(ABC):
    """
    Manages Emme-related data (matrices and scenarios) for multiple time periods
    and kicks off assignment in a subprocess.
    """

    def __init__(self, emmebank: Emmebank, iteration: int):
        self._primary_emmebank = emmebank
        self._iteration = iteration

        self._times = []
        self._scenarios = []
        self._assign_specs = []
        self._demand_matrices = []
        self._skim_matrices = []
        self._omx_file_paths = []

        self._process = None

    @abstractmethod
    def get_assign_script_path(self) -> str:
        raise NotImplemented

    @abstractmethod
    def get_config(self) -> Dict:
        raise NotImplemented

    @abstractmethod
    def get_result_attributes(self) -> List[str]:
        raise NotImplemented

    def add_run(
        self,
        time: str,
        scenario_id: str,
        assign_spec: Dict,
        demand_matrices: List[str],
        skim_matrices: List[str],
        omx_file_path: str,
    ):
        """Add new time period run along with the required scenario, assignment specification
           lists of demand and skims matrices and resulting output omx file path.

        Args:
            time (str): time period ID
            scenario_id (str): EMME scenario ID
            assign_spec (Dict): EMME assignment specification
            demand_matrices (List[str]): list of demand matrix IDs
            skim_matrices (List[str]): list of skim matrix IDs
            omx_file_path (str): complete absolute path for the output of the skim matrices to omx
        """
        self._times.append(time)
        self._scenarios.append(self._primary_emmebank.scenario(scenario_id))
        self._assign_specs.append(assign_spec)
        self._demand_matrices.append(demand_matrices)
        self._skim_matrices.append(skim_matrices)
        self._omx_file_paths.append(omx_file_path)

    @property
    def times(self):
        return self._times

    def setup(self):
        """Create separate Emme project and emmebank and
        copies time period scenario(s), functions and demand matrices.

        """
        self._setup_run_project()
        with self._setup_run_emmebank() as run_emmebank:
            self._copy_scenarios(run_emmebank)
            self._copy_functions(run_emmebank)
            self._copy_demand_matrices(run_emmebank)

    def run(self):
        """"""
        _time.sleep(2)
        python_path = sys.executable
        config_path = os.path.join(self._run_emmebank_dir, "config.json")
        with open(config_path, "w", encoding="utf8") as f:
            _json.dump(self.get_config(), f, indent=4)
        script_path = self.get_assign_script_path()
        command = " ".join([python_path, script_path, "--config", f'"{config_path}"'])

        self._process = _subprocess.Popen(command, shell=True)

    @property
    def is_running(self) -> bool:
        "Returns true if the subprocess is running"
        if self._process:
            return self._process.poll() is None
        return False

    def teardown(self):
        """Copy back reulting skims and link attributes (flow, times) from
        completed .

        NOTE: does not delete duplicate EMME project files.
        """
        ref_scenario_id = self._scenarios[0].id
        dst_emmebank = self._primary_emmebank
        with Emmebank(self._run_emmebank_path) as src_emmebank:
            for matrix_list in self._skim_matrices:
                for matrix_name in matrix_list:
                    src_matrix = src_emmebank.matrix(matrix_name)
                    self.__copy_matrix(dst_emmebank, src_matrix, ref_scenario_id)
            for scenario in self._scenarios:
                attrs = self.get_result_attributes(scenario.id)
                if attrs:
                    values = src_emmebank.scenario(scenario).get_attribute_values(
                        "LINK", attrs
                    )
                    scenario.set_attribute_values("LINK", attrs, values)

        self._process = None

    def delete_run_project(self):
        "Remove all files and folders under target run project directory"
        if os.path.exists(self._run_project_dir):
            _shutil.rmtree(self._run_project_dir)

    @property
    def _run_project_root(self):
        return os.path.dirname(os.path.dirname(self._primary_emmebank.path))

    @property
    def _run_project_name(self):
        return "Remote run " + str(" ".join(self._times))

    @property
    def _run_project_dir(self):
        return os.path.join(self._run_project_root, self._run_project_name)

    @property
    def _run_project_path(self):
        return os.path.join(self._run_project_dir, self._run_project_name + ".emp")

    @property
    def _run_emmebank_dir(self):
        return os.path.join(self._run_project_dir, "Database")

    @property
    def _run_emmebank_path(self):
        return os.path.join(self._run_emmebank_dir, "emmebank")

    def _setup_run_project(self):
        self.delete_run_project()
        _app.create_project(self._run_project_root, self._run_project_name)

    @_context
    def _setup_run_emmebank(self):
        src_emmebank = self._primary_emmebank
        dst_db_dir = self._run_emmebank_dir
        if os.path.exists(dst_db_dir):
            _shutil.rmtree(dst_db_dir)
        os.mkdir(dst_db_dir)
        dimensions = src_emmebank.dimensions
        dimensions["scenarios"] = len(self._scenarios)
        run_emmebank = _create_emmebank(self._run_emmebank_path, dimensions)
        try:
            run_emmebank.title = src_emmebank.title
            run_emmebank.coord_unit_length = src_emmebank.coord_unit_length
            run_emmebank.unit_of_length = src_emmebank.unit_of_length
            run_emmebank.unit_of_cost = src_emmebank.unit_of_cost
            run_emmebank.unit_of_energy = src_emmebank.unit_of_energy
            run_emmebank.use_engineering_notation = (
                src_emmebank.use_engineering_notation
            )
            run_emmebank.node_number_digits = src_emmebank.node_number_digits
            # set extra function parameters ep1...ep3, el1...el9
            for c, upper in [("l", 10), ("p", 4)]:
                for i in range(1, upper):
                    v = getattr(src_emmebank.extra_function_parameters, f"e{c}{i}")
                    setattr(run_emmebank.extra_function_parameters, f"e{c}{i}", v)

            yield run_emmebank

        finally:
            run_emmebank.dispose()

    def _copy_scenarios(self, dst_emmebank):
        for src_scen in self._scenarios:
            dst_scen = dst_emmebank.create_scenario(src_scen.id)
            dst_scen.title = src_scen.title
            for attr in sorted(src_scen.extra_attributes(), key=lambda x: x._id):
                dst_attr = dst_scen.create_extra_attribute(
                    attr.type, attr.name, attr.default_value
                )
                dst_attr.description = attr.description
            for field in src_scen.network_fields():
                dst_scen.create_network_field(
                    field.type, field.name, field.atype, field.description
                )
            dst_scen.has_traffic_results = src_scen.has_traffic_results
            dst_scen.has_transit_results = src_scen.has_transit_results
            dst_scen.publish_network(src_scen.get_network())

    def _copy_functions(self, run_emmebank):
        for src_func in self._primary_emmebank.functions():
            dst_func = run_emmebank.create_function(src_func.id, src_func.expression)

    def _copy_demand_matrices(self, run_emmebank):
        ref_scenario_id = self._scenarios[0].id
        for matrix_list in self._demand_matrices:
            for matrix_id in matrix_list:
                src_matrix = self._primary_emmebank.matrix(matrix_id)
                self.__copy_matrix(run_emmebank, src_matrix, ref_scenario_id)

    @staticmethod
    def __copy_matrix(emmebank, src_matrix, scenario_id):
        dst_matrix = emmebank.matrix(src_matrix.name)
        if not dst_matrix:
            ident = emmebank.available_matrix_identifier(src_matrix.type)
            dst_matrix = emmebank.create_matrix(ident)
            dst_matrix.name = src_matrix.name
            dst_matrix.description = src_matrix.description
        if src_matrix.type == "SCALAR":
            dst_matrix.data = src_matrix.data
        else:
            dst_matrix.set_data(src_matrix.get_data(scenario_id), scenario_id)
        return dst_matrix
