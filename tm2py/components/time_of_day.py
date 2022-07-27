"""Module with helpful matrix helper functions."""

from typing import Collection, Dict, Mapping, Optional, Union

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
        if split_config.productions and split_config.attractions:
            prod, attract = 0.5 * split_config.production, 0.5 * split_config.attraction
            _split_demand = prod * matrix + attract * matrix.T
        else:
            _split_demand = split_config.od * matrix

        return np.around(_split_demand, decimals=2)

    @LogStartEnd()
    def run(
        demand: NumpyArray, split_configs: Collection[TimeSplitConfig]
    ) -> Dict[str, NumpyArray]:
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
        for _split in split_configs:
            matrix_dict[_split.time_period] = TimePeriodSplit.split_matrix(
                demand, _split
            )

        return matrix_dict
