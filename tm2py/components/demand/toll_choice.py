"""Toll Choice Model."""
import itertools
import os
from ast import Num
from math import exp
from typing import TYPE_CHECKING, Collection, Dict, List, Mapping, Optional

import numpy as np
import openmatrix as _omx
import pandas as pd

from tm2py.components.component import Component, Subcomponent
from tm2py.components.network.skims import get_omx_skim_as_numpy, get_summed_skims
from tm2py.config import ChoiceClassConfig, TollChoiceConfig
from tm2py.emme.matrix import OMXManager
from tm2py.logger import LogStartEnd
from tm2py.omx import df_to_omx
from tm2py.tools import interpolate_dfs

NumpyArray = np.array

if TYPE_CHECKING:
    from tm2py.controller import RunController

DEFAULT_PROPERTY_SKIM_TOLL = {
    "time": ["time"],
    "distance": ["dist"],
    "cost": ["bridgetoll", "valuetoll"],
}

DEFAULT_PROPERTY_SKIM_NOTOLL = {
    "time": ["time"],
    "distance": ["dist"],
    "cost": ["bridgetoll"],
}


class TollChoiceCalculator(Subcomponent):
    """Implements toll choice calculations.

    Centralized implementation of Toll Choice calculations common to
    Commercial and Internal-external sub models. Loads input skims
    from OMXManager

    This subcomponent should be able to be configured solely within:

    TollChoiceConfig:
        classes: List[ChoiceClassConfig]
        value_of_time: float
        operating_cost_per_mile: float
        utility: Optional[List[CoefficientConfig]] = Field(default=None)

    ChoiceClassConfig:
        name: str
        skim_mode_notoll: Optional[str] = Field(default="da")
        skim_mode_toll: Optional[str] = Field(default="datoll")
        property_factors: Optional[List[CoefficientConfig]] = Field(default=None)

    CoefficientConfig:
        name: str
        coeff: Optional[float] = Field(default=None)

    Properties:
        classes (Dict[str,TollClassConfig]): convenience access to TollChoiceConfig
        utility (Dict[str,float]): access to all utility factors by property

    """

    def __init__(
        self,
        controller: "RunController",
        component: Component,
        config: TollChoiceConfig,
    ):
        """Constructor for TollChoiceCalculator.

        Args:
            controller: RunController object
            component: Component which contains this subcomponent
            config: TollChoiceConfig Instance
        """
        super().__init__(controller, component)

        self.config = config
        self._class_configs = None

        # Copy out parts of config that we want to update/manipulate

        # Fill in blanks with defaults
        DEFAULT_PROPERTY_SKIM_TOLL.update(self.config.property_to_skim_toll)
        self.property_to_skim_toll = DEFAULT_PROPERTY_SKIM_TOLL
        DEFAULT_PROPERTY_SKIM_NOTOLL.update(self.config.property_to_skim_notoll)
        self.property_to_skim_notoll = DEFAULT_PROPERTY_SKIM_NOTOLL

        self.utility = {x.property: x.coeff for x in config.utility}
        # set utility for cost using value of time and distance using operating cost per mile
        self.utility["cost"] = TollChoiceCalculator.calc_cost_coeff(
            self.utility["time"], config.value_of_time
        )

        self.utility["distance"] = TollChoiceCalculator.calc_dist_coeff(
            self.utility["cost"],
            config.operating_cost_per_mile,
        )

        self.toll_skim_suffix = ""

        self._omx_manager = None
        self._skim_dir = None

        self.skim_dir = self.get_abs_path(
            self.controller.config.highway.output_skim_path
        )

    @property
    def classes(self):
        self.classes = {c.name: c for c in self.config.classes}

    @property
    def class_config(self):
        if not self._class_configs:
            self._class_configs = {c.name: c for c in self.config.classes}
        return self._class_configs

    @staticmethod
    def calc_cost_coeff(time_coeff: float, value_of_time: float) -> float:
        """Calculate cost coefficient from time coefficient and value of time."""
        # FIXME why is 0.6 here?
        return (time_coeff / value_of_time) * 0.6

    @staticmethod
    def calc_dist_coeff(cost_coeff: float, operating_cost_per_mile: float) -> float:
        """Calculate coefficient on distance skim from cost coefficient and operating cost."""
        return cost_coeff * operating_cost_per_mile

    @staticmethod
    def calc_cost_coeff(time_coeff: float, value_of_time: float) -> float:
        """Calculate cost coefficient from time coefficient and value of time."""
        # FIXME why is 0.6 here?
        return (time_coeff / value_of_time) * 0.6

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
            os.makedirs(value)
            self.controller.logger.debug(f"Creating directory {value}")
        if value != self._skim_dir:
            self._omx_manager = OMXManager(value)
            self._skim_dir = value

    @property
    def omx_manager(self):
        """Access to self._omx_manager."""
        return self._omx_manager

    def validate_inputs(self):
        """Validate inputs."""
        if self.utility.get("cost"):
            raise ValueError(
                "Cost utility for toll choice should be set using value\
                of time config."
            )

        if self.utility.get("distance"):
            raise ValueError(
                "Distance utility for toll choice should be set using\
                operating cost config."
            )

    def run(
        self, demand: NumpyArray, class_name: str, time_period: str
    ) -> Dict[str, NumpyArray]:
        """Split demand into toll / non toll based on time period and class name.

        Args:
            demand (NumpyArray): Zone-by-zone demand to split into toll/non-toll
            class_name (str): class name to find classConfig
            time_period (str): Time period to use for calculating impedances

        Returns:
            Dict[str,NumpyArray]: Dictionary mapping "toll" and "non toll" to NumpyArrays with
                demand assigned to each.
        """

        prob_nontoll = self.calc_nontoll_prob(time_period, class_name)

        split_demand = {
            "non toll": prob_nontoll * demand,
            "toll": (1 - prob_nontoll) * demand,
        }

        return split_demand

    def calc_nontoll_prob(
        self,
        time_period: str,
        class_name: str,
    ) -> NumpyArray:
        """Calculates the non-toll probability using binary logit model, masking non avail options.

        Args:
            time_period (str): time period abbreviation
            class_name (str): _description_

        Returns:
            NumpyArray: Probability of choosing non-toll option for a given class and time period.
        """

        e_util_nontoll = self.calc_exp_util(
            self.property_to_skim_notoll,
            self.class_config[class_name],
            time_period,
        )

        e_util_toll = self.calc_exp_util(
            self.property_to_skim_toll,
            self.class_config[class_name],
            time_period,
            toll=True,
        )

        prob_nontoll = e_util_nontoll / (e_util_toll + e_util_nontoll)

        prob_nontoll = self.mask_non_available(
            prob_nontoll,
            time_period,
            self.class_config[class_name].skim_mode,
            self.class_config[class_name].veh_group_name,
        )

        return prob_nontoll

    def calc_exp_util(
        self,
        prop_to_skim: Mapping[str, Collection[str]],
        choice_class_config: ChoiceClassConfig,
        time_period: str,
        toll: Optional[bool] = False,
    ) -> NumpyArray:
        """Calculate the exp(utils) for the time, distance and costs skims.

        Loads the referenced skim matrices and calculates the result as:
        exp(coeff_time * time + coeff_cost * (op_cost * dist + cost)))

        Args:
            prop_to_skim: mapping of a property (used in coeffs) to a set of skim properties to sum
            choice_class_config: A ChoiceClassConfig instance
            time_period: time period abbrevation

        Returns:
            A numpy array with the calculated exp(util) result.
        """
        _util_sum = []
        property_factors = {}
        if choice_class_config.property_factors is not None:
            property_factors = {
                x.property: x.coeff for x in choice_class_config.property_factors
            }
        for prop, skim_prop_list in prop_to_skim.items():
            if not toll:
                _skim_values = get_summed_skims(
                    self.controller,
                    property=skim_prop_list,
                    mode=choice_class_config.skim_mode,
                    veh_group_name=choice_class_config.veh_group_name,
                    time_period=time_period,
                    omx_manager=self._omx_manager,
                )
            else:
                _skim_values = get_summed_skims(
                    self.controller,
                    property=skim_prop_list,
                    mode=choice_class_config.skim_mode + "toll",
                    veh_group_name=choice_class_config.veh_group_name,
                    time_period=time_period,
                    omx_manager=self._omx_manager,
                )
            _util = self.utility[prop] * _skim_values * property_factors.get(prop, 1)
            _util_sum.append(_util)

        self._omx_manager.close()  # can comment out

        return np.exp(np.add(*_util_sum))

    def mask_non_available(
        self,
        prob_nontoll,
        time_period,
        skim_mode,
        veh_group_name,
        prop_toll_cost="valuetoll",
        prop_nontoll_time="time",
    ) -> NumpyArray:
        """Mask the nontoll probability matrix.

        Set to 1.0 if no toll path toll cost, or to 0.0 if no nontoll time.

        Args:
            prob_nontoll: numpy array of calculated probability for non-toll
            time_period: time period abbreviation
            skim_mode: skim mode for getting skims
            prop_toll_cost: the property to use to see if toll option is available
            prop_nontoll_time: the property to use to see if a non-toll option is available
        """

        nontoll_time = get_omx_skim_as_numpy(
            self.controller,
            skim_mode,
            veh_group_name,
            time_period,
            prop_nontoll_time,
            omx_manager=self._omx_manager,
        )

        toll_tollcost = get_omx_skim_as_numpy(
            self.controller,
            skim_mode + "toll",
            veh_group_name,
            time_period,
            prop_toll_cost,
            omx_manager=self._omx_manager,
        )

        prob_nontoll[(toll_tollcost == 0) | (toll_tollcost > 999999)] = 1.0
        prob_nontoll[(nontoll_time == 0) | (nontoll_time > 999999)] = 0.0

        self._omx_manager.close()

        return prob_nontoll
