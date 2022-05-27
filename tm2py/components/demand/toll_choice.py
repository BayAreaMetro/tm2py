"""Toll Choice Model."""
import itertools
import os
from math import exp
from typing import TYPE_CHECKING, List, Optional

import numpy as np
import openmatrix as _omx
import pandas as pd

from tm2py.components.component import Component, Subcomponent
from tm2py.emme.matrix import OMXManager
from tm2py.logger import LogStartEnd
from tm2py.tools import df_to_omx, interpolate_dfs

NumpyArray = np.array

if TYPE_CHECKING:
    from tm2py.controller import RunController


class TollChoiceCalculator(Subcomponent):
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
        self,
        controller: 'RunController',
        component: Component,
    ):
        """Constructor for TollChoiceCalculator.

        Args:
            controller: RunController object
            component: Component which contains this subcomponent
        """
        self.value_of_time = None
        self.coeff_time = None
        self.operating_cost_per_mile = None
        self._omx_manager = None
        self._skim_dir = None

    @property
    def skim_dir(self):
        """Return the directory where the skim matrices are located."""
        return self._skim_dir

    @skim_dir.setter
    def skim_dir(self, value):
        """Set the directory where the skim matrices are located.

        If the directory is different from previous directory, initialize on OMX manager
        to manage skims.
        """
        if not os.path.isdir(value):
            raise ValueError(f"{value} is not a valid skim directory")
        if value != self._skim_dir:
            self._omx_manager = OMXManager(value)
            self._skim_dir = value

    @property
    def omx_manager(self):
        """Access to self._omx_manager."""
        return self._omx_manager

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
