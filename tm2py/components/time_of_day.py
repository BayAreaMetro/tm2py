"""Module with helpful matrix helper functions."""

from typing import Collection, Dict, Mapping, Optional, Union
from unicodedata import decimal

import numpy as np
import pandas as pd

from tm2py.components.component import Component, Subcomponent
from tm2py.config import TimeSplitConfig
from tm2py.logger import LogStartEnd

NumpyArray = np.array


class TimePeriodSplit(Subcomponent):
    def __init__(
        self,
        controller,
        component: Component,
        split_configs: Collection[TimeSplitConfig],
    ):
        super().__init__(controller, component)
        self.split_configs = split_configs

    def validate_inputs(self):
        # TODO
        pass

    @staticmethod
    def split_matrix(matrix, split_config: TimeSplitConfig):
        if isinstance(matrix, dict):
            _split_demand = {}
            for key, value in matrix.items():
                if split_config.production and split_config.attraction:
                    prod, attract = (
                        0.5 * split_config.production,
                        0.5 * split_config.attraction,
                    )
                    _split_demand[key] = prod * value + attract * value.T
                else:
                    _split_demand[key] = split_config.od * value

                _split_demand[key] = np.around(_split_demand[key], decimals=2)
        else:
            if split_config.production and split_config.attraction:
                prod, attract = (
                    0.5 * split_config.production,
                    0.5 * split_config.attraction,
                )
                _split_demand = prod * matrix + attract * matrix.T
            else:
                _split_demand = split_config.od * matrix

            _split_demand = np.around(_split_demand, decimals=2)

        return _split_demand

    @LogStartEnd()
    def run(self, demand: NumpyArray) -> Dict[str, NumpyArray]:
        """Split a demand matrix according to a TimeOfDaySplitConfig.

        Right now supports simple factoring of demand. If TimeOfDaySplitConfig has productions
        and attractions, will balance the matrix to product an OD matrix. If has origins and
        destinations, wont balance.

        Args:
            matrix (NumpyArray): matrix to split.
            split_configs (Collection[TimeOfDaySplitConfig]): List of TimeOfDaySplitConfigs to use.

        Returns:
            Dict[str, NumpyArray]: _description_
        """
        matrix_dict = {}
        for _split in self.split_configs:
            matrix_dict[_split.time_period] = TimePeriodSplit.split_matrix(
                demand, _split
            )

        return matrix_dict
