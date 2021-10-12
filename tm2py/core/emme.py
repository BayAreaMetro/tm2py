"""Emme related shared code and core components.

"""


from collections import defaultdict as _defaultdict
from contextlib import contextmanager as _context
import heapq
import multiprocessing as _multiprocessing
import re as _re
import os
from socket import error as _socket_error
from typing import List, Union, Any, Dict, Callable

import numpy as _np
import openmatrix as _omx

# PyLint cannot build AST from compiled Emme libraries
# so disabling relevant import module checks
# pylint: disable=E0611, E0401, E1101
from inro.emme.database.emmebank import Emmebank, change_dimensions
from inro.emme.database.scenario import Scenario as EmmeScenario
from inro.emme.database.matrix import Matrix as EmmeMatrix
from inro.emme.network.node import Node as EmmeNetworkNode
from inro.emme.network.link import Link as EmmeNetworkLink
import inro.emme.desktop.app as _app
import inro.modeller as _m

NumpyArray = _np.array
EmmeDesktopApp = _app.App
EmmeModeller = _m.Modeller

# Cache running Emme projects from this process (simple singleton implementation)
_EMME_PROJECT_REF = {}
_INF = 1e400

class EmmeManager:
    """Centralized cache for Emme project and related calls for traffic and transit assignments.

    Wraps Emme Desktop API (see Emme API Reference, )
    """

    def __init__(self):
        self._project_cache = _EMME_PROJECT_REF

    def close_all(self):
        """
        Close open emme project(s).

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
        if not path.endswith("emmebank"):
            path = os.path.join(path, "emmebank")
        return Emmebank(path)

    @staticmethod
    def change_emmebank_dimensions(emmebank, dimensions):
        dims = emmebank.dimensions
        new_dims = dims.copy()
        new_dims.update(dimensions)
        if dims != new_dims:
            change_dimensions(emmebank.path, new_dims, keep_backup=False)

    @staticmethod
    def init_modeller(emme_project: EmmeDesktopApp) -> EmmeModeller:
        """Initialize and return Modeller object.

        If Modeller has not already been initialized it will do so on
        specified Emme project. If already initialized Modeller will use
        the existing project.

        Args:
            emme_project: open 'Emme Desktop' application (inro.emme.desktop.app)
        """
        try:
            return _m.Modeller()
        except AssertionError:
            return _m.Modeller(emme_project)

    @property
    def modeller(self) -> EmmeModeller:
        """Return Modeller object.

        If not already initialized the first opened Emme project will be used
        to do so.
        """
        try:
            return _m.Modeller()
        except AssertionError:
            if self._project_cache:
                emme_project = next(iter(self._project_cache.values()))
                return _m.Modeller(emme_project)
            raise

    def tool(self, namespace: str):
        return self.modeller.tool(namespace)

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
        attributes = attributes if attributes else {}
        with _m.logbook_trace(name, value=value, attributes=attributes):
            yield


class NetworkCalculator:
    """Simple alternative interface to the Emme Network calculator

    Used to generate the standard network calculator specification (dictionary)
    from argument inputs. Useful when NOT (commonly) using selection or
    aggregation options, and mostly running link expression calculations
    """

    def __init__(self, scenario: EmmeScenario, modeller: EmmeModeller = None):
        """

        Args:
            scenario:
            modeller:
        """
        self._scenario = scenario
        if modeller is None:
            emme_manager = EmmeManager()
            modeller = emme_manager.modeller
        self._network_calc = modeller.tool(
            "inro.emme.network_calculation.network_calculator"
        )
        self._specs = []

    def __call__(
        self,
        result: str,
        expression: str,
        selections: Union[str, Dict[str, str]] = None,
        aggregation: Dict[str, str] = None,
    ):
        """Run a network calculation in the scenario, see the Emme help for more.

        Args:
            result: Name of network attribute
            expression: Calculation expression
            selections: Selection expression nest. Defaults to {"link": "all"} if
                        not specified, and is used as a link selection expression
                        if specified as a string.
            aggregation: Aggregation operators if aggregating between network domains.
        """
        spec = self._format_spec(result, expression, selections, aggregation)
        return self._network_calc(spec, self._scenario)

    def add_calc(
        self,
        result: str,
        expression: str,
        selections: Union[str, dict] = None,
        aggregation: [dict] = None,
    ):
        """Add calculation to list of network calclations to run.

        Args:
            result: Name of network attribute
            expression: Calculation expression
            selections: Selection expression nest. Defaults to {"link": "all"} if
                        not specified, and is used as a link selection expression
                        if specified as a string.
            aggregation: Aggregation operators if aggregating between network domains.
        """
        self._specs.append(
            self._format_spec(result, expression, selections, aggregation)
        )

    def run(self):
        """Run accumulated network calculations all at once."""
        reports = self._network_calc(self._specs, self._scenario)
        self._specs = []
        return reports

    @staticmethod
    def _format_spec(result, expression, selections, aggregation):
        spec = {
            "result": result,
            "expression": expression,
            "aggregation": aggregation,
            "type": "NETWORK_CALCULATION",
        }
        if selections is not None:
            if isinstance(selections, str):
                selections = {"link": selections}
            spec["selections"] = selections
        else:
            spec["selections"] = {"link": "all"}
        return spec


class MatrixCache:
    """Write through cache of Emme matrix data via Numpy arrays"""

    def __init__(self, scenario: EmmeScenario):
        """

        Args:
            scenario: reference scenario for the active Emmebank and matrix zone system
        """
        self._scenario = scenario
        self._emmebank = scenario.emmebank
        self._timestamps = {}
        self._data = {}

    def get_data(self, matrix: [str, EmmeMatrix]) -> NumpyArray:
        """Get Emme matrix data as numpy array.

        Args:
            matrix: Emme matrix object or unique name / ID for Emme matrix in Emmebank
        """
        if isinstance(matrix, str):
            matrix = self._emmebank.matrix(matrix)
        timestamp = matrix.timestamp
        prev_timestamp = self._timestamps.get(matrix)
        if prev_timestamp is None or (timestamp != prev_timestamp):
            self._timestamps[matrix] = matrix.timestamp
            self._data[matrix] = matrix.get_numpy_data(self._scenario.id)
        return self._data[matrix]

    def set_data(self, matrix: [str, EmmeMatrix], data: NumpyArray):
        """Set numpy array to Emme matrix (write through cache).

        Args:
            matrix: Emme matrix object or unique name / ID for Emme matrix in Emmebank
            data: Numpy array, must match the scenario zone system
        """
        if isinstance(matrix, str):
            matrix = self._emmebank.matrix(matrix)
        matrix.set_numpy_data(data, self._scenario.id)
        self._timestamps[matrix] = matrix.timestamp
        self._data[matrix] = data

    def clear(self):
        """Clear the cache."""
        self._timestamps = {}
        self._data = {}


class OMX:
    """Wrapper for the OMX interface to write from Emme matrices and numpy arrays."""

    def __init__(
        self,
        file_path: str,
        mode: str = "r",
        scenario: EmmeScenario = None,
        omx_key: str = "NAME",
        matrix_cache: MatrixCache = None,
        mask_max_value: float = None
    ):  # pylint: disable=R0913
        """Write from Emmebank or Matrix Cache to OMX file, or read from OMX to Numpy.

        Also supports with statement.

        Args:
            file_path: path of OMX file
            mode: "r", "w" or "a"
            scenario: Emme scenario object for zone system and reference
                Emmebank
            omx_key: "ID_NAME", "NAME", "ID", format for generating
                OMX key from Emme matrix data
            matrix_cache: optional, Matrix Cache to support write data
                from cache (instead of always reading from Emmmebank)
            mask_max_value: optional, max value above which to write
                zero instead ("big to zero" behavior)
        """
        self._file_path = file_path
        self._mode = mode
        self._scenario = scenario
        self._omx_key = omx_key
        self._mask_max_value = mask_max_value
        self._omx_file = None
        self._emme_matrix_cache = matrix_cache
        self._read_cache = {}

    def _generate_name(self, matrix):
        if self._omx_key == "ID_NAME":
            return f"{matrix.id}_{matrix.name}"
        if self._omx_key == "NAME":
            return matrix.name
        if self._omx_key == "ID":
            return matrix.id
        raise Exception("invalid omx_key: {0}".format(self._omx_key))

    def open(self):
        """Open the OMX file."""
        self._omx_file = _omx.open_file(self._file_path, self._mode)

    def close(self):
        """Close the OMX file."""
        if self._omx_file is not None:
            self._omx_file.close()
        self._omx_file = None
        self._read_cache = {}

    def __enter__(self):
        self.open()
        if self._mode in ["a", "w"] and self._scenario is not None:
            try:
                self._omx_file.create_mapping(
                    "zone_number", self._scenario.zone_numbers
                )
            except LookupError:
                pass
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def write_matrices(self, matrices: List[Union[EmmeMatrix, str]]):
        """Write the list of emme matrices to OMX file.

        Args:
            matrices: list of Emme matrix objects or names / IDs
                of matrices in Emmebank, or dictionary of 
                name: Emme matrix object/ Emme matrix ID
        """
        if isinstance(matrices, dict):
            for key, matrix in matrices.iteritems():
                self.write_matrix(matrix, key)
        else:
            for matrix in matrices:
                self.write_matrix(matrix)

    def write_matrix(self, matrix: [str, EmmeMatrix], name=None):
        """Write Emme matrix (as name or ID or Emme matrix object).

        Args:
            matrix: Emme matrix object or name / ID of matrix in Emmebank
            name: optional name to use for OMX key, if not specified the
                omx_key format will be used to generate a name from the
                Emme matrix data
        """
        if self._mode not in ["a", "w"]:
            raise Exception("{0}: open in read-only mode".format(self._file_path))
        if not isinstance(matrix, EmmeMatrix):
            matrix = self._scenario.emmebank.matrix(matrix)
        if name is None:
            name = self._generate_name(matrix)
        if self._emme_matrix_cache:
            numpy_array = self._emme_matrix_cache.get_data(matrix)
        else:
            numpy_array = matrix.get_numpy_data(self._scenario.id)
        if matrix.type == "DESTINATION":
            n_zones = len(numpy_array)
            numpy_array = _np.resize(numpy_array, (1, n_zones))
        elif matrix.type == "ORIGIN":
            n_zones = len(numpy_array)
            numpy_array = _np.resize(numpy_array, (n_zones, 1))
        attrs = {"description": matrix.description}
        self.write_array(numpy_array, name, attrs)

    def write_clipped_array(
        self,
        numpy_array: NumpyArray,
        name: str,
        a_min: float,
        a_max: float = None,
        attrs: Dict[str, str] = None,
    ):  # pylint: disable=R0913
        """Write array with min and max values capped.

        Args:
            numpy_array: Numpy array
            name: name to use for the OMX key
            a_min: minimum value to clip array data
            a_max: optional maximum value to clip array data
            attrs: additional attribute key value pairs to write to OMX file
        """
        if a_max is not None:
            numpy_array = numpy_array.clip(a_min, a_max)
        else:
            numpy_array = numpy_array.clip(a_min)
        self.write_array(numpy_array, name, attrs)

    def write_array(
        self, numpy_array: NumpyArray, name: str, attrs: Dict[str, str] = None
    ):
        """Write array with name and optional attrs to OMX file.

        Args:
            numpy_array:
            name:
            attrs:
        """
        if self._mode not in ["a", "w"]:
            raise Exception("{0}: open in read-only mode".format(self._file_path))
        shape = numpy_array.shape
        if len(shape) == 2:
            chunkshape = (1, shape[0])
        else:
            chunkshape = None
        if self._mask_max_value:
            numpy_array[numpy_array > self._mask_max_value] = 0
        numpy_array = numpy_array.astype(dtype="float64", copy=False)
        self._omx_file.create_matrix(
            name, obj=numpy_array, chunkshape=chunkshape, attrs=attrs
        )

    def read(self, name: str) -> NumpyArray:
        """Read OMX data as numpy array (standard interface).

        Caches matrix data (arrays) already read from disk.

        Args:
            name: name of OMX matrix
        """
        if name in self._read_cache:
            return self._read_cache[name]
        data = self._omx_file[name].read()
        self._read_cache[name] = data
        return data

    def read_hdf5(self, path: str) -> NumpyArray:
        """Read data directly from PyTables interface.

        Support for hdf5 formats that don't have full OMX compatibility.

        Args:
            path: hdf5 reference path to matrix data
        """
        return self._omx_file.get_node(path).read()


def parse_num_processors(value: [str, int, float]):
    """Parse input value string "MAX-X" to number of available processors.

    Used with Emme procedures (traffic and transit assignments, matrix
    caculator, etc.) Does not raise any specific errors.

    Args:
        value: int, float or string; string value can be "X" or "MAX-X"
    """
    max_processors = _multiprocessing.cpu_count()
    if isinstance(value, str):
        value = value.upper()
        if value == "MAX":
            return max_processors
        if _re.match("^[0-9]+$", value):
            return int(value)
        result = _re.split(r"^MAX[\s]*-[\s]*", value)
        if len(result) == 2:
            return max(max_processors - int(result[1]), 1)
    else:
        return int(value)
    return value


def find_path(
        orig_node: EmmeNetworkNode,
        dest_node: EmmeNetworkNode,
        filter_func: Callable,
        cost_func: Callable) -> List[EmmeNetworkLink]:
    """Find and return the shortest path (sequence of links) between two nodes in Emme network.

        Args:
            orig_node: origin Emme node object
            dest_node: desination Emme node object
            filter_func: callable function which accepts an Emme network link and returns True if included and False
                if excluded. E.g. lambda link: mode in link.modes
            cost_func: callable function which accepts an Emme network link and returns the cost value for the link.
    """
    visited = set([])
    visited_add = visited.add
    costs = _defaultdict(lambda: _INF)
    back_links = {}
    heap = []
    pop, push = heapq.heappop, heapq.heappush
    outgoing = None
    link_found = False
    for outgoing in orig_node.outgoing_links():
        if filter_func(outgoing):
            back_links[outgoing] = None
            if outgoing.j_node == dest_node:
                link_found = True
                break
            cost_to_link = cost_func(outgoing)
            costs[outgoing] = cost_to_link
            push(heap, (cost_to_link, outgoing))
    try:
        while not link_found:
            cost_to_link, link = pop(heap)
            if link in visited:
                continue
            visited_add(link)
            for outgoing in link.j_node.outgoing_links():
                if not filter_func(outgoing):
                    continue
                if outgoing in visited:
                    continue
                outgoing_cost = cost_to_link + cost_func(outgoing)
                if outgoing_cost < costs[outgoing]:
                    back_links[outgoing] = link
                    costs[outgoing] = outgoing_cost
                    push(heap, (outgoing_cost, outgoing))
                if outgoing.j_node == dest_node:
                    link_found = True
                    break
    except IndexError:
        pass  # IndexError if heap is empty
    if not link_found or outgoing is None:
        raise NoPathFound("No path found between %s and %s" % (orig_node, dest_node))
    prev_link = outgoing
    route = []
    while prev_link:
        route.append(prev_link)
        prev_link = back_links[prev_link]
    return list(reversed(route))


class NoPathFound(Exception):
    pass
