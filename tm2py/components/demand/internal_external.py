"""

NOTES from .job scripts for reference. To be edited / replaced with updated docs.

IxForecasts.job

 TP+ script to create a daily matrix that includes internal/external, external/internal, and external/external
 passenger vehicle travel (based on Census 2000 journey-to-work flows).  These trip tables are based on total 
 traffic counts, which include trucks, but trucks are not explicitly segmented from passenger vehicles.  This 
 short-coming is a hold-over from BAYCAST and will be addressed in the next model update.

 The row and column totals are taken from count station data provided by Caltrans.  The BAYCAST 2006 IX matrix is
 used as the base matrix and scaled to match forecast year growth assumptions. The script generates estimates 
 for the model forecast year; the growth rates were discussed with neighboring MPOs as part of the SB 375 target 
 setting process. 

 Input:  (1)  Station-specific assumed growth rates for each forecast year (the lack of external/external movements
              through the region allows simple factoring of cells without re-balancing);
         (2)  An input base matrix derived from the Census journey-to-work data. 

 Output: (1) Four-table, forecast-year specific trip tables containing internal/external, external/internal, and 
             external/external vehicle (xxx or person xxx) travel. 

IxTimeOfDay.job

 TP+ script to apply diurnal factors to the daily estimate of internal/external personal vehicle trips.  A separate 
 sketch process is used to generate the internal/external trip table, which is independent of land use patterns 
 and congestion in the current implementation. The internal/external trip table includes only passenger vehicles,
 and does not include trucks.

 The five time periods modeled are: (a) early AM, 3 am to 6 am; (b) AM peak period, 6 am to 10 am; (c) midday, 
 10 am to 3 pm; (d) PM peak period, 3 pm to 7 pm; and, (e) evening, 7 pm to 3 am the next day.

 The diurnal factors are taken from the BAYCAST model.  The entire estimation of internal/external travel demand
 is taken from the BAYCAST model and is not improved for Travel Model One. 

 Input:  (1)  A daily trip table, containing trips in production/attraction format for the following modes, drive
              alone (i.e. single-occupant personal passenger vehicle), shared ride 2 (double-occupant personal
              passenger vehicle), and shared ride 3+ (three-or-more-occupants personal passenger vehicle). The 
              tables must be named "ix_daily_da", "ix_daily_sr2", and "ix_daily_sr3".

 Output: (1)  Five time-of-day-specific estimates of internal/external demand, each time-of-day-specific matrices
              containing four tables, one for drive alone (da), one for shared ride 2 (sr2), and one for shared ride
              3 (sr3).


IxTollChoice.job

 TP+ script to apply a binomial choice model for drive alone, shared ride 2, and shared ride 3 internal/external
 personal vehicle travel.  Two loops are used. The first cycles through the five time periods and the second 
 cycles through the three vehicle classes.

 The time periods are: (a) early AM, 3 am to 6 am; (b) AM peak period, 7 am to 10 am; (c) midday, 10 am to 3 pm; 
 (d) PM peak period, 3 pm to 7 pm; and, (e) evening, 7 pm to 3 am the next day.  A separate value toll paying 
 versus no value toll paying path choice model is applied to each of the fifteen time period/vehicle type combinations.

 Input:  (1) Time-period-specific origin/destination matrices of drive alone, shared ride 2, and share ride 3+ 
             internal/external trip tables. 
         (2) Skims providing the time and cost for value toll and non-value toll paths for each; the tables must
             have the following names:

             (a) Non-value-toll paying time: TIMEXXX;
             (b) Non-value-toll distance: DISTXXX
             (c) Non-value-toll bridge toll is: BTOLLXXX;
             (d) Value-toll paying time is: TOLLTIMEXXX;
             (e) Value-toll paying distance is: TOLLDISTXXX;
             (f) Value-toll bridge toll is: TOLLBTOLLXXX;
             (g) Value-toll value toll is: TOLLVTOLLXXX,
                 where XXX is DA, S2, or S3.

 Output: Five, six-table trip matrices, one for each time period.  Two tables for each vehicle class
         representing value-toll paying path trips and non-value-toll paying path trips. 

 Notes:  (1)  TOLLCLASS is a code, 1 through 10 are reserved for bridges; 11 and up is reserved for value toll
              facilities. 
         (2)  All costs should be coded in year 2000 cents
         (3)  The in-vehicle time coefficient is taken from the work trip mode choice model. 
"""


import os
import math
import numpy as np
import openmatrix as _omx

from tm2py.components.component import Component
from tm2py.emme.matrix import OMXManager
from tm2py.logger import LogStartEnd

_special_gateway_adjust = {
    4693: 1.020228,
    4695: 1.242555,
    4696: 0.848518,
    4698: 1.673817,
}
_annual_growth_rate = {
    4688: 1.005,
    4689: 1.005,
    4690: 1.005,
    4691: 1.005,
    4692: 1.005,
    4693: 1.010,
    4694: 1.010,
    4695: 1.010,
    4696: 1.010,
    4697: 0,
    4698: 1.010,
    4699: 1.010,
    4700: 1.015,
    4701: 1.010,
    4702: 1.010,
    4703: 1.010,
    4704: 1.005,
    4705: 1.005,
    4706: 1.005,
    4707: 1.005,
    4708: 1.005,
}
_time_of_day_split = {
    "ea": {"production": 0.15329, "attraction": 0.06440},
    "am": {"production": 0.26441, "attraction": 0.17540},
    "md": {"production": 0.25720, "attraction": 0.26950},
    "pm": {"production": 0.21490, "attraction": 0.29824},
    "ev": {"production": 0.11020, "attraction": 0.19246},
}
# True to reference old names generated by cube assignments, False to use Emme
# naming and structure. For testing only, to be removed.
use_old_skims = False


class InternalExternal(Component):
    """docstring for component"""

    def __init__(self, controller):
        super().__init__(controller)
        self._parameter = None

    @LogStartEnd()
    def run(self):
        """docstring for component run"""
        input_demand = self._load_data()
        daily_demand = self._growth_forecast(input_demand)
        period_demand = self._time_of_day(daily_demand)
        class_demands = self._toll_choice(period_demand)
        self._export_results(class_demands)

    def _load_data(self):
        # input: inputs\nonres\ixDaily2006x4.may2208.new.omx
        # load matrices from .omx
        file_path = self.get_abs_path(self.config.internal_external.input_demand_file)
        omx_file = _omx.open_file(file_path)
        demand = {
            "da": omx_file["IX_Daily_DA"].read(),
            "sr2": omx_file["IX_Daily_SR2"].read(),
            "sr3": omx_file["IX_Daily_SR3"].read(),
        }
        omx_file.close()
        return demand

    def _growth_forecast(self, demand):
        # STEPS:
        # 1.1 apply special factors to certain gateways based on ID
        #   NOTE: these should be expressed such that we can put
        #         them in the config later
        # 1.2 apply gateway-specific annual growth rates to results of step 1
        #    to generate year specific forecast
        #
        # return numpy matrices or pandas dataframe to pass into next step

        # Build adjustment matrix to be applied to all input matrices
        # special gateway adjustments based on zone index
        year = int(self.config.scenario.year)
        adj_matrix = np.ones(demand["da"].shape)
        for index, factor in _special_gateway_adjust.items():
            adj_matrix[index, :] *= factor
            adj_matrix[:, index] *= factor
        num_years = year - int(self.config.internal_external.reference_year)
        # apply total growth from annual growth rates at each gateway
        for index, rate in _annual_growth_rate.items():
            factor = pow(rate, num_years)
            adj_matrix[index, :] *= factor
            adj_matrix[:, index] *= factor
        daily_prod_attract = {
            "da": demand["da"] * adj_matrix,
            "sr2": demand["sr2"] * adj_matrix,
            "sr3": demand["sr3"] * adj_matrix,
        }
        return daily_prod_attract

    @LogStartEnd()
    def _time_of_day(self, daily_prod_attract):
        # input: results of _ix_forecast, daily PA matrices
        # STEPS:
        # 2.1 apply time of day factors to convert 3 PA matrices into 15
        #     time of day O-D matrices
        # return result array(s)
        period_demand = {}
        for period in [p.name for p in self.config.periods]:
            factor_map = _time_of_day_split[period]
            class_demand = {}
            for class_name, demand in daily_prod_attract.items():
                p, a = 0.5 * factor_map["production"], 0.5 * factor_map["attraction"]
                class_demand[class_name] = p * demand + a * demand.T
            period_demand[period] = class_demand
        return period_demand

    @LogStartEnd()
    def _toll_choice(self, period_demand):
        # input: result of _ix_time_of_day
        #        skims:
        #           REFERENCE version, from Cube Output:
        #             HWYSKIMYY_taz.omx, where YY is time period,
        #                                      XX is the class DA, S2, S3
        #               (a) Non-value-toll paying time: TIMEXX
        #               (b) Non-value-toll distance: DISTXX
        #               (c) Non-value-toll bridge toll is: BTOLLXX
        #               (d) Value-toll paying time is: TOLLTIMEXX
        #               (e) Value-toll paying distance is: TOLLDISTXX
        #               (f) Value-toll bridge toll is: TOLLBTOLLXX
        #               (g) Value-toll value toll is: TOLLVTOLLXX
        #           Emme output version
        #             traffic_skims_{period}.omx, where {period} is the time period ID,
        #                                               {class} is the class name da, sr2, sr2
        #               (a) Non-value-toll paying time: {period}_{class}_time,
        #               (b) Non-value-toll distance: {period}_{class}_dist,
        #               (c) Non-value-toll bridge toll is: {period}_{class}_bridgetoll_{class},
        #               (d) Value-toll paying time is: {period}_{class}toll_time,
        #               (e) Value-toll paying distance is: {period}_{class}toll_dist,
        #               (f) Value-toll bridge toll is: {period}_{class}toll_bridgetoll_{class},
        #               (g) Value-toll value toll is: {period}_{class}toll_valuetoll_{class},
        #
        # STEPS:
        # 3.1: For each time of day, for each da, sr2, sr3, calculate
        #      - utility of toll and notoll
        #      - probabability of toll / notoll
        #      - split demand into toll and notoll matrices
        # 3.2 Export resulting demand to .omx files
        #
        #   demand_matrices\highway\internal_external\tripsIx{period}.omx,
        #   with matrix names: DA, SR2, SR3, DATOLL, SR2TOLL, SR3TOLL
        # reproduce the calculation in IxTollChoice.job
        # read parameters from .block file

        # setup all the coefficient
        # model_coefficient_in_vehicle = -0.022 / 0.25
        k_ivtt = self.config.internal_external.toll_choice_time_coefficient
        value_of_time = self.config.internal_external.value_of_time
        k_cost = (k_ivtt / value_of_time) * 0.6
        sr2_tollf = self.config.internal_external.shared_ride_2_toll_factor
        sr3_tollf = self.config.internal_external.shared_ride_3_toll_factor
        op_cost = self.config.internal_external.operating_cost_per_mile
        class_demand = {}
        for period, demands in period_demand.items():
            # skim_file_path = os.path.join('skims', 'HWYSKIM' + period + '_taz.omx')
            skim_path_tmplt = self.get_abs_path(self.config.highway.output_skim_path)
            with OMXManager(skim_path_tmplt.format(period=period)) as skims:
                split_demand = {}
                for name, total_trips in demands.items():
                    if use_old_skims:
                        uname = {"da": "DA", "sr2": "S2", "sr3": "S3"}[name]
                        nontoll_time = skims.read(f"TIME{uname}")
                        nontoll_dist = skims.read(f"DIST{uname}")
                        nontoll_bridgecost = skims.read(f"BTOLL{uname}")
                        toll_time = skims.read(f"TOLLTIME{uname}")
                        toll_dist = skims.read(f"TOLLDIST{uname}")
                        toll_bridgecost = skims.read(f"TOLLBTOLL{uname}")
                        toll_tollcost = skims.read(f"TOLLVTOLL{uname}")
                    else:
                        nontoll_time = skims.read(f"{period}_{name}_time")
                        nontoll_dist = skims.read(f"{period}_{name}_dist")
                        nontoll_bridgecost = skims.read(
                            f"{period}_{name}_bridgetoll{name}"
                        )
                        toll_time = skims.read(f"{period}_{name}toll_time")
                        toll_dist = skims.read(f"{period}_{name}toll_dist")
                        toll_bridgecost = skims.read(
                            f"{period}_{name}toll_bridgetoll{name}"
                        )
                        toll_tollcost = skims.read(
                            f"{period}_{name}toll_valuetoll{name}"
                        )

                    e_util_nontoll = np.exp(
                        k_ivtt * nontoll_time
                        + k_cost * (op_cost * nontoll_dist + nontoll_bridgecost)
                    )
                    e_util_toll = np.exp(
                        k_ivtt * toll_time
                        + k_cost
                        * (op_cost * toll_dist + toll_bridgecost + toll_tollcost)
                    )
                    prob_nontoll = e_util_nontoll / (e_util_toll + e_util_nontoll)
                    prob_nontoll[(toll_tollcost == 0) | (toll_tollcost > 999999)] = 1.0
                    prob_nontoll[(nontoll_time == 0) | (nontoll_time > 999999)] = 0.0
                    split_demand[name] = prob_nontoll * total_trips
                    split_demand[f"{name}toll"] = (1 - prob_nontoll) * total_trips

                class_demand[period] = split_demand
        return class_demand

    @LogStartEnd()
    def _export_results(self, demand):
        """Export assignable class demands to OMX files by time-of-day."""
        path_tmplt = self.get_abs_path(
            self.config.internal_external.highway_demand_file
        )
        os.makedirs(os.path.dirname(path_tmplt), exist_ok=True)
        for period, matrices in demand.items():
            with OMXManager(path_tmplt.format(period=period), "w") as output_file:
                for name, data in matrices.items():
                    output_file.write_array(data, name)
