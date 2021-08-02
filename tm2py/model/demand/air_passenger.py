"""TODO: review module docsting 
Build the airport trip matrices (these are independent of level-of-service)
    Run the airport model
        "nonres/BuildAirPax.job"
            input: nonres/@token_year@_@token_toFrom@@token_airport@.csv
            output: five time-of-day-specific tables for DA, SR2, SR3, DATOLL, SR2TOLL, SR3TOLL
            Looks like a pandas application

NOTES FROM BuildAirPaxMatrices.job

 TP+ script that creates air passenger vehicle trip tables for the Bay Area's three major airports, namely SFO,
 OAK, and SJC.  Geoff Gosling, a consultant, created vehicle trip tables segmented by time of day, travel mode,
 and access/egress direction (i.e. to the airport or from the airport) for years 2007 and 2035.  The tables are
 based on a 2006 Air Passenger survey, which was conducted at SFO and OAK (but not SJC).  The five time
 periods correspond to the time periods used by the travel model and are as follows: (a) early AM, before 6 am; 
 (b) AM peak period, 6 am to 10 am; (c) midday, 10 am to 3 pm; (d) PM peak period, 3 pm to 7 pm; and, (e) evening, 
 after 7 pm.  The travel modes are as follows: (a) escort (drive alone, shared ride 2, and shared ride 3+); (b) 
 park (da, sr2, & sr3+); (c) rental car (da, sr2, & sr3+); (d) taxi ((da, sr2, & sr3+); (e) limo (da, sr2, & sr3+);
 (f) shared ride van (all assumed to be sr3); (g) hotel shuttle (all assumed to be sr3); and, (h) charter bus (all
 assumed to be sr3).  The shared ride van, hotel shuttle, and charter bus modes are assumed to have no deadhead travel. 
 The return escort trip is included, as are the deadhead limo and taxi trips. 

 The scripts reads in csv files adapted from Mr. Gosling's Excel files, then creates a Cube matrix for each of the
 file.  Next, a highway-assignment ready matrix file is creating for each time-of-day interval.  Here, we 
 assume that no air passengers use HOT lanes (probably not exactly true in certain future year scenarios, but the 
 assumption is made here as a simplification).  Simple linear interpolations are used to estimate vehicle demand in
 years other than 2007 and 2035, including 2015, 2020, 2025, 2030, and 2040.  The 2007 table is used for years 2000,
 2005, and 2010.

 Transit travel to the airports is not included in these vehicle trip tables.


 Input:  (A)  Year-, access/egress-, and airport-specific database file with 90 columns of data for each TAZ.  There
              are 18 columns for each time-of-day interval as follows:
            (1)   Escort, drive alone
            (2)   Escort, shared ride 2
            (3)   Escort, shared ride 3+
            (4)   Park, drive alone
            (5)   Park, shared ride 2
            (6)   Park, shared ride 3+
            (7)   Rental car, drive alone
            (8)   Rental car, shared ride 2
            (9)   Rental car, shared ride 3+
            (10)  Taxi, drive alone
            (11)  Taxi, shared ride 2
            (12)  Taxi, shared ride 3+
            (13)  Limo, drive alone
            (14)  Limo, shared ride 2
            (15)  Limo, shared ride 3+
            (16)  Shared ride van, shared ride 3+
            (17)  Hotel shuttle, shared ride 3+
            (18)  Charter bus, shared ride 3+


 Output: (A) Five time-of-day-specific tables, each containing origin/destination vehicle matrices for the following modes:
           (1) drive alone (DA)
           (2) shared ride 2 (SR2)
           (3) shared ride 3+ (SR3)
           (4) drive alone and willing to pay a value toll (DATOLL)
           (5) shared ride 2 and willing to pay a value toll (SR2TOLL)
           (6) shared ride 3+ and willing to pay a value toll (SR3TOLL)

"""


# from contextlib import contextmanager as _context
import os as _os
import pandas as _pandas
from tm2py.core.component import Component as _Component

# import tm2py.core.tools as _tools
# import tm2py.core.emme as _emme_tools


_join, _dir = _os.path.join, _os.path.dirname


class AirPassenger(_Component):
    """docstring for component"""

    def __init__(self, controller):
        super().__init__(controller)
        self._parameter = None

    def run(self):
        """docstring for component run"""
        year = self._config.model.year
        self._load_demand()
        self._sum_demand()
        self._interpolate()
        self._export_result()

    def _load_demand(self):
        years = ["2007", "2035"]
        airports = ["SFO", "OAK", "SJC"]
        to_from = ["to", "from"]
        # TODO: load each .csv into pandas data frame and return dataframe(s)
        # nonres\@token_year@_@token_toFrom@@token_airport@.csv
        # nonres\@token_year@_@token_toFrom@@token_airport@_seq.csv

    def _sum_demand(self, dataframes):
        # TODO: aggregate demand columns from data frames into the
        #       assignable classes for each year (2007 and 2035)
        pass

    def _interpolate(self):
        # TODO: interpolate between the defined years
        pass
        # token_scale = '(%MODEL_YEAR% - 2007)/(2035 - %MODEL_YEAR%)'

    def _export_result(self):
        # TODO: resulting demand for the model year
        pass
