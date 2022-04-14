"""Module contining the AirPassenger class which builds the airport trip matrices."""


from __future__ import annotations
import os
from typing import TYPE_CHECKING

import numpy as np
import openmatrix as _omx
import pandas as pd

from tm2py.components.component import Component
from tm2py.logger import LogStartEnd

if TYPE_CHECKING:
    from tm2py.controller import RunController


class AirPassenger(Component):
    """Builds the airport trip matrices.

    input: nonres/{year}_{tofrom}{airport}.csv
    output: five time-of-day-specific OMX files with matrices DA, SR2, SR3

    NOTES:

    These are independent of level-of-service.

    Note that the reference names, years, file paths and other key details
    are controlled via the config, air_passenger section. See the
    AirPassengerConfig doc for details on specifying these inputs.

    The following details are based on the default config values.

    Creates air passenger vehicle trip tables for the Bay Area's three major
    airports, namely SFO, OAK, and SJC.  Geoff Gosling, a consultant, created
    vehicle trip tables segmented by time of day, travel mode, and access/egress
    direction (i.e. to the airport or from the airport) for years 2007 and 2035.
    The tables are based on a 2006 Air Passenger survey, which was conducted
    at SFO and OAK (but not SJC).

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
    and 2040.

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

    Internal properties:
        _periods
        _start_year
        _end_year
        _mode_groups:
        _out_names:
    """

    def __init__(self, controller: RunController):
        """Build the airport trip matrices.

        Args:
            controller: parent Controller object
        """
        super().__init__(controller)
        self._periods = [p["name"].upper() for p in self.config.periods]
        self._start_year = None
        self._end_year = None
        self._mode_groups = {}
        self._out_names = {}

    @LogStartEnd()
    def run(self):
        """Build the airport trip matrices"""
        self._periods = [p["name"].upper() for p in self.config.periods]
        self._start_year = self.config.air_passenger.reference_start_year
        self._end_year = self.config.air_passenger.reference_end_year
        self._mode_groups = {}
        self._out_names = {}
        for group in self.config.air_passenger.demand_aggregation:
            self._mode_groups[group["src_group_name"]] = group["access_modes"]
            self._out_names[group["src_group_name"]] = group["result_class_name"]

        input_demand = self._load_demand()
        aggr_demand = self._aggregate_demand(input_demand)
        demand = self._interpolate(aggr_demand)
        self._export_result(demand)

    def _load_demand(self) -> pd.DataFrame:
        """Loads demand from the CSV files into pandas dataframe."""

        def rename_columns(name):
            if name in ["ORIG", "DEST"]:
                return name
            return f"{name}_{year}"

        input_data_folder = self.config.air_passenger.input_demand_folder
        input_demand = []
        for year in [self._start_year, self._end_year]:
            input_dataframes = []
            for airport in self.config.air_passenger.airport_names:
                for direction in ["to", "from"]:
                    file_name = f"{year}_{direction}{airport}.csv"
                    file_path = os.path.join(
                        self.controller.run_dir, input_data_folder, file_name
                    )
                    input_df = pd.read_csv(file_path)
                    input_df.rename(columns=rename_columns, inplace=True)
                    input_dataframes.append(input_df)
            data = pd.concat(input_dataframes)
            input_demand.append(data)
        demand = pd.merge(
            input_demand[0], input_demand[1], how="outer", on=["ORIG", "DEST"]
        )
        grouped = demand.groupby(["ORIG", "DEST"]).sum()
        return grouped

    def _aggregate_demand(self, input_demand: pd.DataFrame) -> pd.DataFrame:
        """Aggregate demand into the assignable classes for each year."""
        aggr_demand = pd.DataFrame()
        for year in [self._start_year, self._end_year]:
            for period in self._periods:
                for group, access_modes in self._mode_groups.items():
                    data = input_demand[
                        [f"{period}_{access}_{group}_{year}" for access in access_modes]
                    ]
                    aggr_demand[f"{period}_{group}_{year}"] = data.sum(axis=1)
        return aggr_demand

    def _interpolate(self, aggr_demand: pd.DataFrame) -> pd.DataFrame:
        """Interpolate for the model year assuming linear growth between the reference years."""
        start_year = int(self._start_year)
        end_year = int(self._end_year)
        year = str(self.config.scenario.year)
        if year in [start_year, end_year]:
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
        scale = float(int(year) - start_year) / (end_year - start_year)
        demand = pd.DataFrame()
        for period in self._periods:
            for group in self._mode_groups:
                name = f"{period}_{group}"
                demand[name] = (1 - scale) * aggr_demand[
                    f"{name}_{start_year}"
                ] + scale * aggr_demand[f"{name}_{end_year}"]
        return demand

    def _export_result(self, demand: pd.DataFrame):
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
        path_tmplt = os.path.join(
            self.controller.run_dir, self.config.air_passenger.highway_demand_file
        )
        os.makedirs(os.path.dirname(path_tmplt), exist_ok=True)
        for period in self._periods:
            file_path = path_tmplt.format(period=period)
            omx_file = _omx.open_file(file_path, "w")
            try:
                omx_file.create_mapping("zone_number", zone_ids)
                for name in self._mode_groups:
                    array = np.zeros(shape=(num_zones, num_zones))
                    # Insert values at the calculated indices in array
                    np.put(
                        array,
                        demand["INDEX"].to_numpy(),
                        demand[f"{period}_{name}"].to_numpy(),
                    )
                    omx_file.create_matrix(self._out_names[name], obj=array)
            finally:
                omx_file.close()
