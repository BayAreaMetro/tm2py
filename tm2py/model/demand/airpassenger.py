"""Build the airport trip matrices.

    input: nonres/{year}_{tofrom}{airport}.csv
    output: five time-of-day-specific OMX files with matrices DA, SR2, SR3

NOTES:

These are independent of level-of-service).

Creates air passenger vehicle trip tables for the Bay Area's three major
airports, namely SFO, OAK, and SJC.  Geoff Gosling, a consultant, created
vehicle trip tables segmented by time of day, travel mode, and access/egress
direction (i.e. to the airport or from the airport) for years 2007 and 2035.
The tables are based on a 2006 Air Passenger survey, which was conducted
at SFO and OAK (but not SJC).  The five time periods correspond to the time
periods used by the travel model and are as follows:
    (a) early AM, before 6 am
    (b) AM peak period, 6 am to 10 am
    (c) midday, 10 am to 3 pm
    (d) PM peak period, 3 pm to 7 pm
    (e) evening,  after 7 pm

The travel modes are as follows:
    (a) escort (drive alone, shared ride 2, and shared ride 3+)
    (b) park (da, sr2, & sr3+)
    (c) rental car (da, sr2, & sr3+)
    (d) taxi ((da, sr2, & sr3+)
    (e) limo (da, sr2, & sr3+)
    (f) shared ride van (all assumed to be sr3);
    (g) hotel shuttle (all assumed to be sr3); and,
    (h) charter bus (all assumed to be sr3).

The shared ride van, hotel shuttle, and charter bus modes are assumed to
have no deadhead travel. The return escort trip is included, as are the
deadhead limo and taxi trips.

The scripts reads in csv files adapted from Mr. Gosling's Excel files,
and creates a highway-assignment ready OMX matrix file for each time-of-day
interval.

Assumes that no air passengers use HOT lanes (probably not exactly true
in certain future year scenarios, but the assumption is made here as a
simplification).  Simple linear interpolations are used to estimate vehicle
demand in years other than 2007 and 2035, including 2015, 2020, 2025, 2030,
and 2040.  The 2007 table is used for years 2000, 2005, and 2010.

Transit travel to the airports is not included in these vehicle trip tables.

Input:
    Year-, access/egress-, and airport-specific database file with 90 columns
    of data for each TAZ.  There are 18 columns for each time-of-day interval
    as follows:
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


 Output:
 Five time-of-day-specific tables, each containing origin/destination vehicle
 matrices for the following modes:
           (1) drive alone (DA)
           (2) shared ride 2 (SR2)
           (3) shared ride 3+ (SR3)
"""


import os as _os
import numpy as _numpy
import openmatrix as _omx
import pandas as _pandas

from tm2py.core.component import Component as _Component, Controller as _Controller


_join, _dir = _os.path.join, _os.path.dirname


class AirPassenger(_Component):
    """Builds the airport trip matrices."""

    def __init__(self, controller: _Controller, root_dir: str = None):
        """Build the airport trip matrices.

        Args:
            controller: parent Controller object
            root_dir (str): root directory containing Emme project, demand matrices
        """
        super().__init__(controller)
        if root_dir is None:
            self._root_dir = _os.getcwd()
        else:
            self._root_dir = root_dir
        self._periods = ["EA", "AM", "MD", "PM", "EV"]
        self._access_modes = ["ES", "PK", "RN", "TX", "LI"]
        self._s3_access_modes = self._access_modes + ["VN", "HT", "CH"]
        self._assign_classes = ["DA", "S2", "S3"]

    def run(self):
        """Build the airport trip matrices"""
        input_demand = self._load_demand()
        aggr_demand = self._aggregate_demand(input_demand)
        demand = self._interpolate(aggr_demand)
        self._export_result(demand)

    def _load_demand(self):
        """Loads demand from the CSV files into pandas dataframe."""

        def rename_columns(name):
            if name in ["ORIG", "DEST"]:
                return name
            return f"{name}_{year}"

        input_demand = []
        for year in ["2007", "2035"]:
            input_dataframes = []
            for airport in ["SFO", "OAK", "SJC"]:
                for direction in ["to", "from"]:
                    file_name = f"{year}_{direction}{airport}.csv"
                    file_path = _join(self._root_dir, "nonres", file_name)
                    input_df = _pandas.read_csv(file_path)
                    input_df.rename(columns=rename_columns, inplace=True)
                    input_dataframes.append(input_df)
            data = _pandas.concat(input_dataframes)
            input_demand.append(data)
        demand = _pandas.merge(
            input_demand[0], input_demand[1], how="outer", on=["ORIG", "DEST"]
        )
        grouped = demand.groupby(["ORIG", "DEST"]).sum()
        return grouped

    def _aggregate_demand(self, input_demand):
        """Aggregate demand into the assignable classes for each year (2007 and 2035)."""
        aggr_demand = _pandas.DataFrame()
        for year in self._ref_years:
            for period in self._periods:
                for assign in self._assign_classes:
                    if assign == "S3":
                        acces_modes = self._s3_access_modes
                    else:
                        acces_modes = self._access_modes
                    data = input_demand[
                        [f"{period}_{access}_{assign}_{year}" for access in acces_modes]
                    ]
                    aggr_demand[f"{period}_{assign}_{year}"] = data.sum(axis=1)
        return aggr_demand

    def _interpolate(self, aggr_demand):
        """Interpolate for the model year assuming linear growth between the reference years."""
        year = str(self.config.model.year)
        if year in self._ref_years:
            # no interpolation is needed
            columns = [c for c in aggr_demand.columns if c.endswith(year)]
            demand = aggr_demand[columns].copy()

            def rename_columns(name):
                return name.replace(f"_{year}", "")

            demand.rename(columns=rename_columns, inplace=True)
            return demand

        # In the cube .job script, the formula is:
        # token_scale = (%MODEL_YEAR% - 2007)/(2035 - %MODEL_YEAR%)
        # and it should be: token_scale = (%MODEL_YEAR% - 2007)/(2035 - 2007)
        # scale = float((int(year) - 2007)) / (2035 - int(year))
        scale = (int(year) - 2007) / (2035 - 2007)
        demand = _pandas.DataFrame()
        for period in self._periods:
            for assign in self._assign_classes:
                name = f"{period}_{assign}"
                demand[name] = (1 - scale) * aggr_demand[
                    f"{name}_2007"
                ] + scale * aggr_demand[f"{name}_2035"]
        return demand

    def _export_result(self, demand):
        """Export resulting model year demand to OMX files by period."""
        # dropping ORIG and DEST index for calculation of numpy array index
        demand = demand.reset_index()
        # get all used Zone IDs to produce index and zone mapping in OMX file
        zone_ids = sorted(set(demand["ORIG"]).union(set(demand["DEST"])))
        num_zones = len(zone_ids)
        zone_map = dict((z, i) for i, z in enumerate(zone_ids))
        # calculate index of entries in numpy array list
        demand["INDEX"] = demand.apply(
            lambda r: zone_map[r["ORIG"]] * len(zone_ids) + zone_map[r["DEST"]], axis=1
        )
        for period in self._periods:
            file_path = _join(
                self._root_dir,
                "demand_matrices",
                "highway",
                "air_passenger",
                f"tripsAirPax{period}.omx",
            )
            omx_file = _omx.open_file(file_path, "w")
            try:
                omx_file.create_mapping("zone_number", zone_ids)
                for name in self._assign_classes:
                    array = _numpy.zeros(shape=(num_zones, num_zones))
                    # Insert values at the calculated indices in array
                    _numpy.put(
                        array,
                        demand["INDEX"].to_numpy(),
                        demand[f"{period}_{name}"].to_numpy(),
                    )
                    omx_file.create_matrix(name, obj=array)
            finally:
                omx_file.close()
