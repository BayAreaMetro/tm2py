"""Module for Emme-related matrix management.

Contains the MatrixCache class for write through matrix data management of Emme
matrices (in Emmebank) to avoid repeated read-from-disk of skim matrices
during post-assignment processing and export to OMX.

Contains the OMXManager which is a thin wrapper on the openmatrix (OMX)
library for transfer between Emme (emmebank) <-> OMX files. Integrates with
the MatrixCache to support easy write from Emmebank without re-reading data
from disk.
"""

from typing import Dict, List, Union

import openmatrix as _omx
from numpy import array as NumpyArray
from numpy import exp, resize

from tm2py.emme.manager import EmmeMatrix, EmmeScenario


class MatrixCache:
    """Write through cache of Emme matrix data via Numpy arrays."""

    def __init__(self, scenario: EmmeScenario):
        """Contructor for MatrixCache class.

        Args:
            scenario (EmmeScenario): EmmeScenario reference scenario for the active Emmebank
                and matrix zone system
        """
        self._scenario = scenario
        self._emmebank = scenario.emmebank
        # mapping from matrix object to last read/write timestamp for cache invalidation
        self._timestamps = {}
        # cache of Emme matrix data, key: matrix object, value: numpy array of data
        self._data = {}

    def get_data(self, matrix: Union[str, EmmeMatrix]) -> NumpyArray:
        """Get Emme matrix data as numpy array.

        Args:
            matrix: Emme matrix object or unique name / ID for Emme matrix in Emmebank

        Returns:
            The Numpy array of values for this matrix / matrix ID.
        """
        if isinstance(matrix, str):
            matrix = self._emmebank.matrix(matrix)
        timestamp = matrix.timestamp
        prev_timestamp = self._timestamps.get(matrix)
        if prev_timestamp is None or (timestamp != prev_timestamp):
            self._timestamps[matrix] = matrix.timestamp
            self._data[matrix] = matrix.get_numpy_data(self._scenario.id)
        return self._data[matrix]

    def set_data(self, matrix: Union[str, EmmeMatrix], data: NumpyArray):
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


# disable too-many-instance-attributes recommendation
# pylint: disable=R0902
class OMXManager:
    """Wrapper for the OMX interface to write from Emme matrices and numpy arrays.

    Write from Emmebank or Matrix Cache to OMX file, or read from OMX to Numpy.
    Supports "with" statement.
    """

    def __init__(
        self,
        file_path: str,
        mode: str = "r",
        scenario: EmmeScenario = None,
        omx_key: str = "NAME",
        matrix_cache: MatrixCache = None,
        mask_max_value: float = None,
    ):  # pylint: disable=R0913
        """The OMXManager constructor.

        Args:
            file_path (str): path of OMX file
            mode (str, optional): "r", "w" or "a". Defaults to "r".
            scenario (EmmeScenario, optional): _description_. Defaults to None.
            omx_key (str, optional): "ID_NAME", "NAME", "ID", format for generating
            OMX key from Emme matrix data. Defaults to "NAME".
            matrix_cache (MatrixCache, optional): Matrix Cache to support write data
            from cache (instead of always reading from Emmmebank). Defaults to None.
            mask_max_value (float, optional): max value above which to write
            zero instead ("big to zero" behavior). Defaults to None.
        """
        self._file_path = file_path
        self._mode = mode
        self._scenario = scenario
        self._omx_key = omx_key
        self._mask_max_value = mask_max_value
        self._omx_file = None
        self._emme_matrix_cache = matrix_cache
        self._read_cache = {}

    def _generate_name(self, matrix: EmmeMatrix) -> str:
        if self._omx_key == "ID_NAME":
            return f"{matrix.id}_{matrix.name}"
        if self._omx_key == "NAME":
            return matrix.name
        if self._omx_key == "ID":
            return matrix.id
        raise Exception(f"invalid omx_key: {self._omx_key}")

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
        """Allows for context-based usage using 'with' statement."""
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
        """Allows for context-based usage using 'with' statement."""
        self.close()

    def write_matrices(self, matrices: List[Union[EmmeMatrix, str]]):
        """Write the list of emme matrices to OMX file.

        Args:
            matrices: list of Emme matrix objects or names / IDs
                of matrices in Emmebank, or dictionary of
                name: Emme matrix object/ Emme matrix ID
        """
        if isinstance(matrices, dict):
            for key, matrix in matrices.items():
                self.write_matrix(matrix, key)
        else:
            for matrix in matrices:
                self.write_matrix(matrix)

    def write_matrix(self, matrix: Union[str, EmmeMatrix], name=None):
        """Write Emme matrix (as name or ID or Emme matrix object).

        Args:
            matrix: Emme matrix object or name / ID of matrix in Emmebank
            name: optional name to use for OMX key, if not specified the
                omx_key format will be used to generate a name from the
                Emme matrix data
        """
        if self._mode not in ["a", "w"]:
            raise Exception(f"{self._file_path}: open in read-only mode")
        if isinstance(matrix, str):
            matrix = self._scenario.emmebank.matrix(matrix)
        if name is None:
            name = self._generate_name(matrix)
        if self._emme_matrix_cache:
            numpy_array = self._emme_matrix_cache.get_data(matrix)
        else:
            numpy_array = matrix.get_numpy_data(self._scenario.id)
        if matrix.type == "DESTINATION":
            n_zones = len(numpy_array)
            numpy_array = resize(numpy_array, (1, n_zones))
        elif matrix.type == "ORIGIN":
            n_zones = len(numpy_array)
            numpy_array = resize(numpy_array, (n_zones, 1))
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
            numpy_array:: Numpy array
            name: name to use for the OMX key
            attrs: additional attribute key value pairs to write to OMX file
        """
        if self._mode not in ["a", "w"]:
            raise Exception(f"{self._file_path}: open in read-only mode")
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

        Returns:
            Numpy array from OMX file
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

        Returns:
            Numpy array from OMX file
        """
        return self._omx_file.get_node(path).read()


class TollChoiceCalculator:
    """Implements toll choice calculations.

    Centralized implementation of Toll Choice calculations common to
    Commercial and Internal-external sub models. Loads input skims
    from OMXManager

    Properties:
        value_of_time: value of time to use in the utility expression
        coeff_time: coefficient of time value used in the utility expression
        operating_cost_per_mile: operating cost value in cents per mile
            for converting distance to cost
    """

    def __init__(
        self, value_of_time: float, coeff_time: float, operating_cost_per_mile: float
    ):
        """Constructor for TollChoiceCalculator.

        Args:
            value_of_time (float): value of time to use in the utility expression
            coeff_time (float): coefficient of time value used in the utility expression
            operating_cost_per_mile (float): operating cost value in cents per mile
                for converting distance to cost
        """
        self.value_of_time = value_of_time
        self.coeff_time = coeff_time
        self.operating_cost_per_mile = operating_cost_per_mile
        self._omx_manager = None

    def set_omx_manager(self, omx_manager: OMXManager):
        """Set the OMX manager for referencing the skim matrices.

        Args:
            omx_manager: OMXManager which accesses the skim file for reading
                the skim matrices
        """
        self._omx_manager = omx_manager

    def calc_exp_util(
        self,
        time_name: str,
        dist_name: str,
        toll_names: List[str],
        toll_factor: float = 1.0,
    ) -> NumpyArray:
        """Calculate the exp(utils) for the time, distance and costs skims.

        Loads the referenced skim matrices and calculates the result as:
        exp(coeff_time * time + coeff_cost * (op_cost * dist + cost)))

        coeff_cost = coeff_time / vot * 0.6

        Args:
            time_name: Name of the time skim matrix in the OMX file
            dist_name: Name of the distance skim matrix in the OMX file
            toll_names: List of names of the the toll skim matrix in the OMX file
            toll_factor: Optional factor to apply to the tolls

        Returns:
            A numpy array with the calculated exp(util) result.
        """
        vot = self.value_of_time
        k_ivtt = self.coeff_time
        op_cost = self.operating_cost_per_mile
        k_cost = (k_ivtt / vot) * 0.6
        time = self._omx_manager.read(time_name)
        dist = self._omx_manager.read(dist_name)
        toll = self._omx_manager.read(toll_names[0])
        for name in toll_names[1:]:
            toll += self._omx_manager.read(name)
        if toll_factor != 1:
            toll = toll * toll_factor
        e_util = exp(k_ivtt * time + k_cost * (op_cost * dist + toll))
        return e_util

    def mask_non_available(
        self,
        toll_cost_name: str,
        nontoll_time: str,
        prob_nontoll: NumpyArray,
    ):
        """Mask the nontoll probability matrix.

        Set to 1.0 if no toll path toll cost, or to 0.0 if no nontoll time.

        Args:
            toll_cost_name: Name of toll available cost (toll) skim matrix
            nontoll_time: Name of the time for non-toll skim matrix in the OMX file
            prob_nontoll: numpy array of calculated probability for non-toll
        """
        toll_tollcost = self._omx_manager.read(toll_cost_name)
        nontoll_time = self._omx_manager.read(nontoll_time)
        prob_nontoll[(toll_tollcost == 0) | (toll_tollcost > 999999)] = 1.0
        prob_nontoll[(nontoll_time == 0) | (nontoll_time > 999999)] = 0.0
