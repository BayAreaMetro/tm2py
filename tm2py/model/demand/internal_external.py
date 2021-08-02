"""TODO: add module docsting, 

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


# from contextlib import contextmanager as _context
import os as _os

# import numpy as _numpy
# import pandas as _pandas
from tm2py.core.component import Component as _Component

# import tm2py.core.tools as _tools
# import tm2py.core.emme as _emme_tools


_join, _dir = _os.path.join, _os.path.dirname


class InternalExternal(_Component):
    """docstring for component"""

    def __init__(self, controller):
        super().__init__(controller)
        self._parameter = None

    def run(self):
        """docstring for component run"""
        year = self._config.model.year
        pa_matrices = self._ix_forecast()
        od_matrices = self._ix_time_of_day(pa_matrices)
        demand = self._ix_toll_choice(od_matrices)
        self._export_results(demand)

    def _ix_forecast(self):

        # input: nonres\ixDaily2006x4.may2208.new.omx
        # STEPS:
        # 1.0 load input matrix
        # 1.1 apply special factors to certain gateways based on ID
        #   NOTE: these should be expressed such that we can put
        #         them in the config later
        # 1.2 apply gateway-specific annual growth rates to results of step 1
        #    to generate year specific forecast
        #
        # return numpy matrices or pandas dataframe to pass into next step
        pass

    def _ix_time_of_day(self, pa_matrices):
        # input: results of _ix_forecast, daily PA matrices
        # STEPS:
        # 2.1 apply time of day factors to convert 3 PA matrices into 15
        #     time of day O-D matrices
        # return result array(s) / dataframe(s)
        pass

    def _ix_toll_choice(self, od_matrices):
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
        pass

    def _export_results(self, demand):
        # Export results to OMX files
        pass
