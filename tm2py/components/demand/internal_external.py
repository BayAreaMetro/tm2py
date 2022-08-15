"""Module containing Internal <-> External trip model."""

from __future__ import annotations

import itertools
import os
from collections import defaultdict
from typing import Dict

import numpy as np
import openmatrix as _omx

from tm2py.components.component import Component, Subcomponent
from tm2py.components.demand.toll_choice import TollChoiceCalculator
from tm2py.components.time_of_day import TimePeriodSplit
from tm2py.emme.matrix import OMXManager
from tm2py.logger import LogStartEnd
from tm2py.matrix import create_matrix_factors
from tm2py.omx import omx_to_dict

NumpyArray = np.array


class InternalExternal(Component):
    """Develop Internal <-> External trip tables from land use and impedances.

    1. Grow demand from base year using static rates ::ExternalDemand
    2. Split by time of day using static factors ::TimePeriodSplit
    3. Apply basic toll binomial choice model: ::ExternalTollChoice

    Governed by InternalExternalConfig:
        highway_demand_file:
        input_demand_file:
        input_demand_matrixname_tmpl:
        modes:
        reference_year:
        annual_growth_rate: List[MatrixFactorConfig]
        time_of_day: TimeOfDayConfig
        toll_choice: TollChoiceConfig
        special_gateway_adjust: Optional[List[MatrixFactorConfig]]
    """

    def __init__(self, controller: "RunController"):
        super().__init__(controller)
        self.config = self.controller.config.internal_external

        self.sub_components = {
            "demand forecast": ExternalDemand(controller, self),
            "time of day": TimePeriodSplit(controller, self, self.config.time_of_day),
            "toll choice": ExternalTollChoice(controller, self),
        }

    @property
    def classes(self):
        return self.config.modes

    def validate_inputs(self):
        """Validate inputs to component."""
        ## TODO
        pass

    @LogStartEnd()
    def run(self):
        """Run internal/external travel demand component."""

        daily_demand = self.sub_components["demand forecast"].run()
        period_demand = self.sub_components["time_of_day"].run(daily_demand)
        class_demands = self.sub_components["toll choice"].run(period_demand)
        self._export_results(class_demands)

    @LogStartEnd()
    def _export_results(self, demand: Dict[str, Dict[str, NumpyArray]]):
        """Export assignable class demands to OMX files by time-of-day."""
        path_tmplt = self.get_abs_path(
            self.config.internal_external.highway_demand_file
        )
        os.makedirs(os.path.dirname(path_tmplt), exist_ok=True)
        for period, matrices in demand.items():
            with OMXManager(path_tmplt.format(period=period), "w") as output_file:
                for name, data in matrices.items():
                    output_file.write_array(data, name)


class ExternalDemand(Subcomponent):
    """Forecast of daily internal<->external demand based on growth from a base year.

    Create a daily matrix that includes internal/external, external/internal,
    and external/external passenger vehicle travel (based on Census 2000 journey-to-work flows).
    These trip tables are based on total traffic counts, which include trucks, but trucks are
    not explicitly segmented from passenger vehicles.  This short-coming is a hold-over from
    BAYCAST and will be addressed in the next model update.

    The row and column totals are taken from count station data provided by Caltrans.  The
    BAYCAST 2006 IX matrix is used as the base matrix and scaled to match forecast year growth
    assumptions. The script generates estimates for the model forecast year; the growth rates
    were discussed with neighboring MPOs as part of the SB 375 target setting process.

     Input:  (1)  Station-specific assumed growth rates for each forecast year (the lack of
                  external/external movements through the region allows simple factoring of
                  cells without re-balancing);
             (2)  An input base matrix derived from the Census journey-to-work data.

     Output: (1) Four-table, forecast-year specific trip tables containing internal/external,
                 external/internal, and external/external vehicle (xxx or person xxx) travel.


    Governed by class DemandGrowth Config:
    ```
        highway_demand_file:
        input_demand_file:
        input_demand_matrixname_tmpl:
        modes:
        reference_year:
        annual_growth_rate:
        special_gateway_adjust:
    ```
    """

    def __init__(self, controller, component):

        super().__init__(controller, component)
        self.config = self.component.config.demand
        # Loaded lazily
        self._base_demand = None

    @property
    def year(self):
        return self.controller.config.scenario.year

    @property
    def modes(self):
        return self.component.classes

    @property
    def input_demand_file(self):
        return self.get_abs_path(self.config.input_demand_file)

    @property
    def base_demand(self):
        if self._base_demand is None:
            self._load_base_demand()
        return self._base_demand

    def validate_inputs(self):
        # TODO
        pass

    def _load_base_demand(self):
        """Load reference matrices from .omx to self._base_demand

        input file template: self.config.internal_external.input_demand_matrixname_tmpl
        modes: self.config.internal_external.modes
        """
        _mx_name_tmpl = self.config.input_demand_matrixname_tmpl
        _matrices = {m: _mx_name_tmpl.format(mode=m.upper()) for m in self.modes}

        self.base_demand = omx_to_dict(self.input_demand_file, matrices=_matrices)

    def run(self, base_demand: Dict[str, NumpyArray] = None) -> Dict[str, NumpyArray]:
        """Calculate adjusted demand based on scenario year and growth rates.

        Steps:
        - 1.1 apply special factors to certain gateways based on ID
        - 1.2 apply gateway-specific annual growth rates to results of step 1
           to generate year specific forecast

        Args:
            demand: dictionary of input daily demand matrices (numpy arrays)

        Returns:
             Dictionary of Numpy matrices of daily PA by class mode
        """
        # Build adjustment matrix to be applied to all input matrices
        # special gateway adjustments based on zone index
        if base_demand is None:
            base_demand = self.base_demand
        _num_years = self.year - self.config.reference_year
        _adj_matrix = np.ones(base_demand["da"].shape)

        _adj_matrix = create_matrix_factors(
            _adj_matrix,
            self.config.special_factor_adjust,
        )

        _adj_matrix = create_matrix_factors(
            _adj_matrix,
            self.config.annual_growth_rate,
            periods=_num_years,
        )

        daily_prod_attract = dict(
            (_mode, _demand * _adj_matrix) for _mode, _demand in base_demand.items()
        )
        return daily_prod_attract


class ExternalTollChoice(Subcomponent):
    """Toll choice
    -----------
    Apply a binomial choice model for drive alone, shared ride 2, and shared ride 3
    internal/external personal vehicle travel.

    Input:  (1) Time-period-specific origin/destination matrices of drive alone, shared ride 2,
                and share ride 3+ internal/external trip tables.
            (2) Skims providing the time and cost for value toll and non-value toll paths for each

                traffic_skims_{period}.omx, where {period} is the time period ID,
                {class} is the class name da, sr2, sr2, with the following matrix names
                  Non-value-toll paying time: {period}_{class}_time,
                  Non-value-toll distance: {period}_{class}_dist,
                  Non-value-toll bridge toll is: {period}_{class}_bridgetoll_{class},
                  Value-toll paying time is: {period}_{class}toll_time,
                  Value-toll paying distance is: {period}_{class}toll_dist,
                  Value-toll bridge toll is: {period}_{class}toll_bridgetoll_{class},
                  Value-toll value toll is: {period}_{class}toll_valuetoll_{class},

     Output: Five, six-table trip matrices, one for each time period.  Two tables for each vehicle
             class representing value-toll paying path trips and non-value-toll paying path trips

    Governed by TollClassConfig:

        ```
        classes:
        value_of_time:
        operating_cost_per_mile:
        property_to_skim_toll:
        property_to_skim_notoll:
        utility:
        ```
    """

    def __init__(self, controller, component):
        super().__init__(controller, component)

        self.config = self.component.config.toll_choice

        self.sub_components = {
            "toll choice calculator": TollChoiceCalculator(
                controller, component, self.config
            ),
        }

        # shortcut
        self._toll_choice = self.sub_components["toll choice calculator"]
        self._toll_choice.toll_skim_suffix = "trk"

    def validate_inputs(self):
        # TODO
        pass

    @LogStartEnd()
    def run(
        self, period_demand: Dict[str, Dict[str, NumpyArray]]
    ) -> Dict[str, Dict[str, NumpyArray]]:
        """Binary toll / non-toll choice model by class.

        input: result of _ix_time_of_day
        skims:
            traffic_skims_{period}.omx, where {period} is the time period ID,
            {class} is the class name da, sr2, sr2, with the following matrix names
              Non-value-toll paying time: {period}_{class}_time,
              Non-value-toll distance: {period}_{class}_dist,
              Non-value-toll bridge toll is: {period}_{class}_bridgetoll_{class},
              Value-toll paying time is: {period}_{class}toll_time,
              Value-toll paying distance is: {period}_{class}toll_dist,
              Value-toll bridge toll is: {period}_{class}toll_bridgetoll_{class},
              Value-toll value toll is: {period}_{class}toll_valuetoll_{class},

        STEPS:
        3.1: For each time of day, for each da, sr2, sr3, calculate
             - utility of toll and nontoll
             - probability of toll / nontoll
             - split demand into toll and nontoll matrices

        """

        _time_class_combos = itertools.product(
            self.time_period_names, self.component.classes
        )

        class_demands = defaultdict()
        for _time_period, _class in _time_class_combos:

            _split_demand = self._toll_choice.run(period_demand, _class, _time_period)

            class_demands[_time_period][_class] = _split_demand["no toll"]
            class_demands[_time_period][f"{_class}toll"] = _split_demand["no toll"]
        return class_demands
