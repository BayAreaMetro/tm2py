"""TODO: review module docsting 

NOTES from .job scripts for reference. To be edited / replaced with updated docs.


TruckTripGeneration.job

 TP+ script to apply BAYCAST truck model.  The truck trip generation models for small trucks (two-axle, six tire),
 medium trucks (three-axle), and large or combination (four or more axle) trucks are taken directly from the study: 
 "I-880 Intermodal Corridor Study: Truck Travel in the San Francisco Bay Area", prepared by Barton Aschman in 
 December 1992.  The coefficients are on page 223 of this report.

 The very small truck generation model is based on the Phoenix four-tire truck model documented in the TMIP Quick
 Response Freight Manual. 

 Both models use linear regression models to generate trip ends, balancing attractions to productions. 

 Note that certain production models previously used SIC-based employment categories.  To both maintain consistency
 with the BAYCAST truck model and update the model to use NAICS-based employment categories, new regression models
 were estimated relating the NAICS-based employment data with the SIC-based-predicted trips.  The goal here is not
 to create a new truck model, but to mimic the old model with the available data.  Please see the excel spreadsheet
 TruckModel.xlsx for details.  The NAICS-based model results replicate the SIC-based model results quite well. 

 Input:  (1) MAZ csv data file with the emkplyment and household counts. A python script will be run which will 
             aggregate the data by zone and column to match what is needed for this model, namely:
                (a) TOTEMP, total employment (same regardless of classification system)
                (b) RETEMPN, retail trade employment per the NAICS classification system
                (c) FPSEMPN, financial and professional services employment per the NAICS classification system
                (d) HEREMPN, health, educational, and recreational employment per the NAICS classification system
                (e) OTHEMPN, other employment per the NAICS classification system
                (f) AGREMPN, agricultural employment per the NAICS classificatin system
                (g) MWTEMPN, manufacturing, warehousing, and transportation employment per the NAICS classification system
                (h) TOTHH, total households.

 Output: (1) An ASCII file containing the following fields: (a) zone number; (b) very small truck trip productions;
             (c) very small truck trip attractions; (d) small truck trip productions; (e) small truck trip attractions;
             (f) medium truck trip productions; (g) medium truck trip attractions; (h) large truck trip productions;
             and, (i) large truck trip attractions. 

 Notes:  (1) These scripts do not update the BAYCAST truck model; rather, the model is simply implemented in a
             manner consistent with the Travel Model One implementation.  
         (2) Combined Chuck's calibration adjustments into the NAICS-based model coefficients.  




TruckTripDistribution.job

 TP+ script to apply BAYCAST's trip distribution model.  The implementation is identical to the BAYCAST implementation,
 though scripts and input files have been consolidated.  A simple gravity model is used to distribute the truck
 trips, with separate friction factors used for each class of truck.  The four truck types are: very small trucks 
 (two-axle, four-tire), small trucks (two-axle, six-tire), medium trucks (three-axle), and large or combination 
 (four or more axle) trucks.

 A blended travel time is used as the impedance measure, specifically the weighted average of the AM travel time
 (one-third weight) and the midday travel time (two-thirds weight). 

 Input:  (1) Level-of-service matrices for the AM peak period (6 am to 10 am) and midday period (10 am to 3 pm)
             which contain truck-class specific estimates of congested travel time (in minutes) using the following
             table names:(a) timeVSM, which is the time for very small trucks; (b) timeSML, which is the time for 
             small trucks; (c) timeMED, which is the time for medium trucks; and, (d) timeLRG, which is the time
             for large trucks.
         (2) Trip generation results in ASCII format with the following fields (each 12 columns wide): (a) zone 
             number; (b) very small truck trip productions; (c) very small truck trip attractions; (d) small truck
             trip productions; (e) small truck trip attractions; (f) medium truck trip productions; (g) medium 
             truck trip attractions; (h) large truck trip productions; and, (i) large truck trip attractions. 
         (3) A matrix of k-factors, as calibrated by Chuck Purvis.  Note the very small truck model does not use
             k-factors; the small, medium, and large trucks use the same k-factors. 
         (4) A table of friction factors in ASCII format with the following fields (each 12 columns wide): (a)
             impedance measure (blended travel time); (b) friction factors for very small trucks; (c) friction
             factors for small trucks; (d) friction factors for medium trucks; and, (e) friction factors for large
             trucks. 

 Output: (1) A four-table production/attraction trip table matrix of daily class-specific truck trips (in units 
             of trips x 100, to be consistent with the previous application)with a table for (a) very small trucks,
             (b) small trucks, (c) medium trucks, and (d) large trucks.

 Notes:  (1) These scripts do not update the BAYCAST truck model; rather, the model is simply implemented in a
             manner consistent with the Travel Model One implementation. 

 See also: (1) TruckTripGeneration.job, which applies the generation model.
           (2) TruckTimeOfDay.job, which applies diurnal factors to the daily trips generated here. 
           (3) TruckTollChoice.job, which applies a toll/no toll choice model for trucks.



TruckTimeOfDay.job

 TP+ script to segment daily estimates of truck flows into time-period-specific flows.  The time periods are: 
 early AM, 3 am to 6 am; AM peak, 6 am to 10 am; midday, 10 am to 3 pm; PM peak, 3 pm to 7 pm; and evening, 
 7 pm to 3 am the next day. The four truck types are: very small trucks (two-axle, four-tire), small trucks 
 (two-axle, six-tire), medium trucks (three-axle), and large or combination (four or more axle) trucks.

 The diurnal factors are taken from the BAYCAST-90 model with adjustments made during calibration to the very
 small truck values to better match counts. 

 Input:   A four-table production/attraction trip table matrix of daily class-specific truck trips (in units 
          of trips x 100, to be consistent with the previous application)with a table for (a) very small trucks,
          (b) small trucks, (c) medium trucks, and (d) large trucks.

 Output: Five, time-of-day-specific trip table matrices, each containing the following four tables: (a) vstruck,
         for very small trucks, (b) struck, for small trucks, (c) mtruck, for medium trucks, and (d) ctruck,
         for combination truck. 

 Notes:  (1) These scripts do not update the BAYCAST truck model; rather, the model is simply implemented in a
             manner consistent with the Travel Model One implementation



TruckTollChoice.job

 TP+ script to apply a binomial choice model for very small, small, medium, and large trucks.  Two loops are used.
 The first cycles through the five time periods and the second cycles through the four types of commercial vehicles.
 The time periods are: (a) early AM, before 6 am; (b) AM peak period, 7 am to 10 am; (c) midday, 10 am to 3 pm; 
 (d) PM peak period, 3 pm to 7 pm; and, (e) evening, after 7 pm.  The four types of commercial vehicles are: 
 very small, small, medium, and large.  A separate value toll paying versus no value toll paying path choice
 model is applied to each of the twenty time period/vehicle type combinations.

 Input:  (1) Origin/destination matrix of very small, small, medium, and large truck trips
         (2) Skims providing the time and cost for value toll and non-value toll paths for each; the tables must
             have the following names:
             (a) Non-value-toll paying time: TIMEXXX;
            (b) Non-value-toll distance: DISTXXX
             (c) Non-value-toll bridge toll is: BTOLLXXX;
             (d) Value-toll paying time is: TOLLTIMEXXX;
             (e) Value-toll paying distance is: TOLLDISTXXX;
         (f) Value-toll bridge toll is: TOLLBTOLLXXX;
         (g) Value-toll value toll is: TOLLVTOLLXXX,
          where XXX is VSM, SML, MED, or LRG (vehicle type).

 Output: Five, eight-table trip tables.  One trip table for each time period.  Two tables for each vehicle class
         representing value-toll paying path trips and non-value-toll paying path trips. 

 Notes:  (1)  TOLLCLASS is a code, 1 through 10 are reserved for bridges; 11 and up is reserved for value toll
              facilities. 
         (2)  All costs should be coded in year 2000 cents
         (3)  The 2-axle fee is used for very small trucks
         (4)  The 2-axle fee is used for small trucks
         (5)  The 3-axle fee is used for medium trucks
         (6)  The average of the 5-axle and 6-axle fee is used for large trucks (about the midpoint of the fee
              schedule).
         (7)  The in-vehicle time coefficient is taken from the work trip mode choice model. 


"""


# from contextlib import contextmanager as _context
import os as _os
# import numpy as _numpy
# import pandas as _pandas
from tm2py.core.component import Component as _Component

# import tm2py.core.tools as _tools
# import tm2py.core.emme as _emme_tools


_join, _dir = _os.path.join, _os.path.dirname


class Truck(_Component):
    """docstring for component"""

    def __init__(self, controller):
        super().__init__(controller)
        self._parameter = None

    def run(self):
        """docstring for component run"""
        self._generation()
        self._distribution()
        self._time_of_day()
        self._toll_choice()
        self._export_results()

    def _generation(self):
        # load "maz_data.csv" as dataframe, copy truck_taz_data.py
        # apply the generation models 
        # balance attractions and productions
        # sum linked and unlinked trips
        # return vectors of productiond and attactions (numpy or pandas)
        pass

    def _distribution(self):
        # input: the production / attraction vectors
        # load nonres\truck_kfactors_taz.csv 
        # load nonres\truckFF.dat
        # compute blended truck time as an average 1/3 AM and 2/3 MD
        #     NOTE: Cube outputs skims\COM_HWYSKIMAM_taz.tpp, skims\COM_HWYSKIMMD_taz.tpp
        #           are in the highway_skims_{period}.omx files in Emme version 
        #           with updated matrix names, {period}_trk_time, {period}_lrgtrk_time. 
        #           Also, there will no longer be separate very small, small and medium
        #           truck times, as they are assigned together as the same class.         
        #           There is only the trk_time.
        # Apply friction factors and kfactors to produce balancing matrix
        # apply the gravity models using friction factors from nonres\truckFF.dat
        # (note the very small trucks do not use the K-factors)
        #     Can use Emme matrix balancing for this - important note: reference
        #     matrices by name and ensure names are unique
        #     May want to use temporary matrices
        #     See core/emme.py
        # scale the trips by 100 and bucket round 
        #     (maybe use Matrix controlled rounding Emme tool and precision 0.01)
        # return the daily truck trip matrices
        pass

    def _time_of_day(self):
        # input: the DailyTruckTrips for the four truck classes
        # convert to O/D (m + m.T)/2 and divide by 100
        # apply time-of-day factors by class
        # return the time-of-day matrices

    def _toll_choice(self):
        # input: time-of-day matrices
        # skims: skims\COM_HWYSKIM@token_period@_taz.tpp -> traffic_skims_{period}.omx
        #        NOTE matrix name changes in Emme version, using {period}_{class}_{skim}
        #        format
        # for each class and time-of-day
        #    utility of toll and notoll
        #    probabability of toll / notoll
        #    split demand into toll and notoll matrices

    def _export_results(self):
        # Export results to OMX files by time-of-day
        # nonres\tripstrk{period}.omx
        # VSTRUCK,    STRUCK,    MTRUCK,    CTRUCK,
        # VSTRUCKTOLL,STRUCKTOLL,MTRUCKTOLL,CTRUCKTOLL
        pass