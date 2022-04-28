"""Module containing Internal <-> External trip model.

"""

from __future__ import annotations

import os
from typing import Dict
import numpy as np
import openmatrix as _omx

from tm2py.components.component import Component
from tm2py.emme.matrix import OMXManager, TollChoiceCalculator
from tm2py.logger import LogStartEnd


NumpyArray = np.array

# NOTE: for reference, the default factor tables, now specified via the config:
# special_gateway_adjust = {
#     4693: 1.020228,
#     4695: 1.242555,
#     4696: 0.848518,
#     4698: 1.673817,
# }
# annual_growth_rate = {
#     4688: 1.005,
#     4689: 1.005,
#     4690: 1.005,
#     4691: 1.005,
#     4692: 1.005,
#     4693: 1.010,
#     4694: 1.010,
#     4695: 1.010,
#     4696: 1.010,
#     4697: 0,
#     4698: 1.010,
#     4699: 1.010,
#     4700: 1.015,
#     4701: 1.010,
#     4702: 1.010,
#     4703: 1.010,
#     4704: 1.005,
#     4705: 1.005,
#     4706: 1.005,
#     4707: 1.005,
#     4708: 1.005,
# }
# time_of_day_split = {
#     "ea": {"production": 0.15329, "attraction": 0.06440},
#     "am": {"production": 0.26441, "attraction": 0.17540},
#     "md": {"production": 0.25720, "attraction": 0.26950},
#     "pm": {"production": 0.21490, "attraction": 0.29824},
#     "ev": {"production": 0.11020, "attraction": 0.19246},
# }


class InternalExternal(Component):
    """Create Internal <-> External trip tables.

    Create daily demand (growth forecast)
    -------------------------------------
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

    Time of day split
    -----------------
    Apply diurnal factors to the daily estimate of internal/external personal vehicle trips.
    A separate sketch process is used to generate the internal/external trip table, which is
    independent of land use patterns and congestion in the current implementation. The
    internal/external trip table includes only passenger vehicles, and does not include trucks.

    The diurnal factors are taken from the BAYCAST model.  The entire estimation of
    internal/external travel demand is taken from the BAYCAST model and is not improved for
    Travel Model One.

    Input:  (1)  A daily trip table, containing trips in production/attraction format for: drive
                 alone (i.e. single-occupant personal passenger vehicle), shared ride 2
                 (double-occupant personal passenger vehicle), and shared ride 3+
                 (three-or-more-occupants personal passenger vehicle).

     Output: (1)  time-of-day-specific estimates of internal/external demand, each
                  time-of-day-specific matrices containing four tables, one for drive alone (da),
                  one for shared ride 2 (sr2), and one for shared ride 3 (sr3).

    Toll choice
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
             class representing value-toll paying path trips and non-value-toll paying path trips.
    """

    @LogStartEnd()
    def run(self):
        """docstring for component run"""
        input_demand = self._load_data()
        daily_demand = self._growth_forecast(input_demand)
        period_demand = self._time_of_day(daily_demand)
        class_demands = self._toll_choice(period_demand)
        self._export_results(class_demands)

    def _load_data(self) -> Dict[str, NumpyArray]:
        """Load reference matrices from .omx

        input file: config.internal_external.input_demand_file,
            default is inputs\\nonres\\ixDaily2006x4.may2208.new.omx
        """
        file_path = self.get_abs_path(self.config.internal_external.input_demand_file)
        omx_file = _omx.open_file(file_path)
        demand = {
            "da": omx_file["IX_Daily_DA"].read(),
            "sr2": omx_file["IX_Daily_SR2"].read(),
            "sr3": omx_file["IX_Daily_SR3"].read(),
        }
        omx_file.close()
        return demand

    def _growth_forecast(self, demand: Dict[str, NumpyArray]) -> Dict[str, NumpyArray]:
        """Calculate adjusted demand based on scenario year and growth rates.

        STEPS:
        1.1 apply special factors to certain gateways based on ID
          NOTE: these should be expressed such that we can put
                them in the config later
        1.2 apply gateway-specific annual growth rates to results of step 1
           to generate year specific forecast

        Args:
            demand: dictionary of input daily demand matrices (numpy arrays)

        Returns:
             Dictionary of Numpy matrices of daily PA by class mode
        """

        # Build adjustment matrix to be applied to all input matrices
        # special gateway adjustments based on zone index
        year = int(self.config.scenario.year)
        adj_matrix = np.ones(demand["da"].shape)
        for entry in self.config.internal_external.special_factor_adjust:
            index = entry.zone_index
            factor = entry.factor
            adj_matrix[index, :] *= factor
            adj_matrix[:, index] *= factor
        num_years = year - int(self.config.internal_external.reference_year)
        # apply total growth from annual growth rates at each gateway
        for entry in self.config.internal_external.annual_growth_rate:
            index = entry.zone_index
            factor = pow(entry.factor, num_years)
            adj_matrix[index, :] *= factor
            adj_matrix[:, index] *= factor
        daily_prod_attract = dict((k, v * adj_matrix) for k, v in demand.items())
        return daily_prod_attract

    @LogStartEnd()
    def _time_of_day(
        self, daily_prod_attract: Dict[str, NumpyArray]
    ) -> Dict[str, Dict[str, NumpyArray]]:
        """Calculate time of day matrices

        input: results of _ix_forecast, daily PA matrices
        STEPS:
        2.1 apply time of day factors to convert 3 PA matrices into 15
            time of day O-D matrices

        Args:
            daily_prod_attract: dictionary of numpy arrays of daily PA totals
                with growth factors applied (factored to the scenario year)

        Returns:
            Nested dictionary by period and class name of Numpy matrices of
            time period demand
        """
        period_demand = {}
        for entry in self.config.internal_external.time_of_day_split:
            class_demand = {}
            for class_name, demand in daily_prod_attract.items():
                prod, attract = 0.5 * entry.production, 0.5 * entry.attraction
                class_demand[class_name] = prod * demand + attract * demand.T
            period_demand[entry.time_period] = class_demand
        return period_demand

    @LogStartEnd()
    def _toll_choice(
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
        toll_factors = {
            "sr2": self.config.internal_external.shared_ride_2_toll_factor,
            "sr3": self.config.internal_external.shared_ride_3_toll_factor,
        }
        calculator = TollChoiceCalculator(
            value_of_time=self.config.internal_external.value_of_time,
            coeff_time=self.config.internal_external.toll_choice_time_coefficient,
            operating_cost_per_mile=self.config.internal_external.operating_cost_per_mile,
        )
        class_demand = {}
        for period, demands in period_demand.items():
            skim_path_tmplt = self.get_abs_path(self.config.highway.output_skim_path)
            with OMXManager(skim_path_tmplt.format(period=period)) as skims:
                calculator.set_omx_manager(skims)
                split_demand = {}
                for name, total_trips in demands.items():
                    e_util_nontoll = calculator.calc_exp_util(
                        f"{period}_{name}_time",
                        f"{period}_{name}_dist",
                        [f"{period}_{name}_bridgetoll{name}"],
                        toll_factors.get(name, 1.0),
                    )
                    e_util_toll = calculator.calc_exp_util(
                        f"{period}_{name}toll_time",
                        f"{period}_{name}toll_dist",
                        [
                            f"{period}_{name}_bridgetoll{name}",
                            f"{period}_{name}toll_valuetoll{name}",
                        ],
                        toll_factors.get(name, 1.0),
                    )
                    prob_nontoll = e_util_nontoll / (e_util_toll + e_util_nontoll)
                    calculator.mask_non_available(
                        f"{period}_{name}toll_valuetoll{name}",
                        f"{period}_{name}_time",
                        prob_nontoll,
                    )
                    split_demand[name] = prob_nontoll * total_trips
                    split_demand[f"{name}toll"] = (1 - prob_nontoll) * total_trips

                class_demand[period] = split_demand
        return class_demand

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
