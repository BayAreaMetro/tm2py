"""Emme (Traffic and transit assginment) related shared code and core components.

"""

from contextlib import contextmanager as _context
from inro.emme.database.scenario import Scenario as EmmeScenario
from inro.emme.database.matrix import Matrix as EmmeMatrix
import inro.emme.desktop.app as _app
import inro.modeller as _m
from numpy import array as NumpyArray
import openmatrix as _omx
from socket import error as _socket_error


EmmeDesktopApp = _app.App

# Cache running Emme projects from this process (simple singleton implementation)
_EMME_PROJECT_REF = {}


class EmmeProjectCache:
    """Centralized cache to keep same Emme project for traffic and transit assignments."""

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

    def create_project(self, project_path: str, name: str) -> EmmeDesktopApp:
        """Create, open and return Emme project"""
        project_path = _app.create_project(project_path, name)
        return self.project(project_path)

    def project(self, project_path: str) -> EmmeDesktopApp:
        """Return already open Emme project, or open new Desktop session if not found."""
        emme_project = self._project_cache.get(project_path)
        if emme_project:
            try:  # Check if the Emme window was closed
                emme_project.current_window()
            except _socket_error:
                emme_project = None
        # if window is not opened in this process, start a new one
        if emme_project is None:
            emme_project = _app.start_dedicated(
                visible=True, user_initials="inro", project=project_path
            )
            self._project_cache[project_path] = emme_project
        return emme_project

    def modeller(self, emme_project: EmmeDesktopApp) -> _m.Modeller:
        """Return Modeller object"""
        try:
            return _m.Modeller()
        except AssertionError:
            return _m.Modeller(emme_project)

    def logbook_write(self, name, value=None, attributes={}):
        _m.logbook_write(name, value, attributes)

    @_context
    def logbook_trace(self, name, value=None, attributes={}):
        with _m.logbook_trace(name, value, attributes):
            yield


class NetworkCalculator:
    """Simple alternative interface to the Emme Network calculator, wraps the spec dictionary."""

    def __init__(self, scenario: EmmeScenario, modeller=None):
        self._scenario = scenario
        if modeller is None:
            modeller = _m.Modeller()
        self._network_calc = modeller.tool(
            "inro.emme.network_calculation.network_calculator"
        )

    def __call__(self, result: str, expression: str, selections=None, aggregation=None):
        """Run a network calculation in the scenario, see the Emme help for the Network Calculator for more

        Note that selections defaults to "link": "all" if not specified, to a link selection expression if specified as a string
        """
        spec = {
            "result": result,
            "expression": expression,
            "aggregation": aggregation,
            "type": "NETWORK_CALCULATION",
        }
        if selections is not None:
            if isinstance(selections, basestring):
                selections = {"link": selections}
            spec["selections"] = selections
        else:
            spec["selections"] = {"link": "all"}
        return self._network_calc(spec, self._scenario)


class MatrixCache:
    """Write through cache of Emme matrix data as Numpy arrays"""

    def __init__(self, scenario: EmmeScenario):
        self._scenario = scenario
        self._emmebank = scenario.emmebank
        self._timestamps = {}
        self._data = {}

    def get_data(self, matrix: [str, EmmeMatrix]) -> NumpyArray:
        """Get Emme matrix data as numpy array"""
        if not isinstance(matrix, EmmeMatrix):
            matrix = self._emmebank.matrix(matrix)
        timestamp = matrix.timestamp
        prev_timestamp = self._timestamps.get(matrix)
        if prev_timestamp is None or (timestamp != prev_timestamp):
            self._timestamps[matrix] = matrix.timestamp
            self._data[matrix] = matrix.get_numpy_data(self._scenario.id)
        return self._data[matrix]

    def set_data(self, matrix: [str, EmmeMatrix], data: NumpyArray):
        """Set numpy array to Emme matrix (write through cache)"""
        if not isinstance(matrix, EmmeMatrix):
            matrix = self._emmebank.matrix(matrix)
        matrix.set_numpy_data(data, self._scenario.id)
        self._timestamps[matrix] = matrix.timestamp
        self._data[matrix] = data

    def clear(self):
        self._timestamps = {}
        self._data = {}


class OMX:
    """Wrapper for the OMX interface with support for write from Emme matrices (from Emmebank) and numpy arrays."""

    def __init__(
        self,
        file_path: str,
        mode: str = "r",
        scenario: EmmeScenario = None,
        omx_key: str = "NAME",
        matrix_cache: MatrixCache = None,
    ):
        self._file_path = file_path
        self._mode = mode
        self._scenario = scenario
        self._omx_key = omx_key
        self._omx_file = None
        self._matrix_cache = matrix_cache

    def _generate_name(self, matrix):
        if self._omx_key == "ID_NAME":
            return f"{matrix.id}_{matrix.name}"
        if self._omx_key == "NAME":
            return matrix.name
        if self._omx_key == "ID":
            return matrix.id

    def open(self):
        self._omx_file = _omx.open_file(self._file_path, self._mode)

    def close(self):
        if self._omx_file is not None:
            self._omx_file.close()
        self._omx_file = None

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

    def write_matrices(self, matrices):
        """Write the list of emme matrices to OMX file"""
        if isinstance(matrices, dict):
            for key, matrix in matrices.iteritems():
                self.write_matrix(matrix, key)
        else:
            for matrix in matrices:
                self.write_matrix(matrix)

    def write_matrix(self, matrix: [str, EmmeMatrix], name=None):
        """Write Emme matrix (as name or ID or Emme matrix object)"""
        if self._mode not in ["a", "w"]:
            raise Exception("{f} open in read mode".format(self._file_path))
        if not isinstance(matrix, EmmeMatrix):
            matrix = self._scenario.emmebank.matrix(matrix)
        if name is None:
            name = self._generate_name(matrix)
        if self._matrix_cache:
            numpy_array = self._matrix_cache.get_data(matrix)
        else:
            numpy_array = matrix.get_numpy_data(self._scenario.id)
        if matrix.type == "DESTINATION":
            n_zones = len(numpy_array)
            numpy_array = _numpy.resize(numpy_array, (1, n_zones))
        elif matrix.type == "ORIGIN":
            n_zones = len(numpy_array)
            numpy_array = _numpy.resize(numpy_array, (n_zones, 1))
        attrs = {"description": matrix.description}
        self.write_array(name, numpy_array, attrs)

    def write_clipped_array(
        self, numpy_array: NumpyArray, name: str, a_min: float, a_max=None, attrs={}
    ):
        """Write array with min and max values capped"""
        if a_max is not None:
            numpy_array = numpy_array.clip(a_min, a_max)
        else:
            numpy_array = numpy_array.clip(a_min)
        self.write_array(name, numpy_array, attrs)

    def write_array(self, name: str, numpy_array: NumpyArray, attrs={}):
        """Write array with name and optional attrs to OMX file"""
        if self._mode not in ["a", "w"]:
            raise Exception("{f} open in read mode".format(self._file_path))
        shape = numpy_array.shape
        if len(shape) == 2:
            chunkshape = (1, shape[0])
        else:
            chunkshape = None
        attrs["source"] = "Emme"
        numpy_array = numpy_array.astype(dtype="float64", copy=False)
        omx_matrix = self._omx_file.create_matrix(
            name, obj=numpy_array, chunkshape=chunkshape, attrs=attrs
        )

    def read(self, name: str) -> NumpyArray:
        """Read OMX data as numpy array (standard interface)."""
        return self._omx_file[name].read()

    def read_array(self, path: str) -> NumpyArray:
        """Read data directly from PyTables interface. Support for hdf5 formats that don't have full OMX compatibility."""
        return self._omx_file.get_node(path).read()
