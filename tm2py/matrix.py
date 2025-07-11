"""Module with helpful matrix helper functions."""

from typing import Collection, Dict, Mapping, Optional, Union

import numpy as np
import pandas as pd

from tm2py.components.component import Subcomponent
from tm2py.config import MatrixFactorConfig

NumpyArray = np.array


def create_matrix_factors(
    matrix_factors: Collection[MatrixFactorConfig],
    default_matrix: NumpyArray,
    periods: Optional[float] = None,
) -> NumpyArray:
    adj_matrix = default_matrix
    for adj in matrix_factors:
        if adj.factor is not None:
            _i_factor = adj.factor / 2.00
            _j_factor = adj.factor / 2.00
        else:
            _i_factor = adj.i_factor
            _j_factor = adj.j_factor

        if adj.as_growth_rate:
            _i_factor = pow(_i_factor, periods)
            _j_factor = pow(_j_factor, periods)

        if not adj.zone_index:
            adj_matrix *= _i_factor
            adj_matrix *= _j_factor
        else:
            zone_index = [z for z in adj.zone_index if z < adj_matrix.shape[0]]
            adj_matrix[zone_index, :] *= _i_factor
            adj_matrix[:, zone_index] *= _j_factor

    return adj_matrix


def factor_matrix(
    matrix: Union[NumpyArray, pd.DataFrame],
    matrix_factors: Collection[MatrixFactorConfig],
    periods: Optional[float] = None,
    decimals: Optional[int] = 2,
) -> Union[NumpyArray, pd.DataFrame]:
    """Factor a matrix based on a MatrixFactorConfig and return factored matrix.

    Args:
        matrix (Union[NumpyArray, pd.DataFrame]): A numpy array or pandas dataframe to factor.
        config (MatrixFactorConfig): A collection of instances of MatrixFactorConfig
        periods (Optional[float]): Time (usually in years) used to determine overall growth
            from an annual growth rate. Required if config contains annual_growth_rate.

    Returns:
        Union[NumpyArray, pd.DataFrame]: Matrix factored per config and if growth rate, years.
    """

    _default_matrix = np.ones(matrix.shape)

    adj_matrix = create_matrix_factors(
        matrix_factors,
        _default_matrix,
        periods,
    )

    return np.around(matrix * adj_matrix, decimals=decimals)


def redim_matrix(matrix: NumpyArray, num_zones: int) -> NumpyArray:
    """Pad numpy array with zeros to match expected square shape of zonexzone dimensions.

    Args:
        matrix: NumpyArray to redimension
        num_zones: expected shape to redimension to
    """
    _shape = matrix.shape
    if _shape < (num_zones, num_zones):
        matrix = np.pad(
            matrix, ((0, num_zones - _shape[0]), (0, num_zones - _shape[1]))
        )
    elif _shape > (num_zones, num_zones):
        ValueError(
            f"Provided matrix is larger ({_shape}) than the \
            specified number of zones: {num_zones}"
        )

    return matrix
