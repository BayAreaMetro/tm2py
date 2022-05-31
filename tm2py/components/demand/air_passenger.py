"""Module containing the AirPassenger class which builds the airport trip matrices."""


from __future__ import annotations

import itertools
import os
from typing import TYPE_CHECKING

import numpy as np
import openmatrix as _omx
import pandas as pd

from tm2py.components.component import Component
from tm2py.logger import LogStartEnd
from tm2py.tools import df_to_omx, interpolate_dfs

if TYPE_CHECKING:
    from tm2py.controller import RunController


class AirPassenger(Component):
    """Builds the airport trip matrices.

    input: nonres/{year}_{tofrom}{airport}.csv
    output: five time-of-day-specific OMX files with matrices DA, SR2, SR3

    Notes:
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
        self._periods = [p.upper() for p in self.time_period_names]
        self._start_year = None
        self._end_year = None
        self._mode_groups = {}
        self._out_names = {}

    def validate_inputs(self):
        """Validate the inputs."""
        # TODO
        pass

    @LogStartEnd()
    def run(self):
        """Run the Air Passenger Demand model to generate the demand matrices.

        Steps:
            1. Load the demand data from the CSV files.
            2. Aggregate the demand data into the assignable classes.
            3. Create the demand matrices be interpolating the demand data.
            4. Write the demand matrices to OMX files.
        """
        self._start_year = self.config.air_passenger.reference_start_year
        self._end_year = self.config.air_passenger.reference_end_year
        self._mode_groups = {}
        self._out_names = {}
        for group in self.config.air_passenger.demand_aggregation:
            self._mode_groups[group["src_group_name"]] = group["access_modes"]
            self._out_names[group["src_group_name"]] = group["result_class_name"]

        input_demand = self._load_air_pax_demand()
        aggr_demand = self._aggregate_demand(input_demand)

        demand = interpolate_dfs(
            aggr_demand,
            [int(self._start_year), int(self._end_year)],
            int(self.config.scenario.year),
        )
        self._export_result(demand)

    def _load_air_pax_demand(self) -> pd.DataFrame:
        """Loads demand from the CSV files into single pandas dataframe.

        Uses the following configs to determine the input file names and paths:
        - self.config.air_passenger.input_demand_folder
        - self.config.air_passenger.airport_names
        - self.config.air_passenger.reference_start_year
        - self.config.air_passenger.reference_end_year

        Using the pattern: f"{year}_{direction}{airport}.csv"

        Returns: pandas dataframe with the following columns:
            (1) airport
            (2) time_of_day
            (3) access_mode
            (4) demand
        """

        _start_demand_df = self._get_air_demand_for_year(self._start_year)
        _end_demand_df = self._get_air_demand_for_year(self._end_year)

        _air_pax_demand_df = pd.merge(
            _start_demand_df,
            _end_demand_df,
            how="outer",
            suffixes=(f"_{self._start_year}", f"_{self._end_year}"),
            on=["ORIG", "DEST"],
        )

        _grouped_air_pax_demand_df = _air_pax_demand_df.groupby(["ORIG", "DEST"]).sum()
        return _grouped_air_pax_demand_df

    def _get_air_demand_for_year(self, year: str) -> pd.DataFrame:
        """Creates a dataframe of concatenated data from CSVs for all airport x direction combos.

        Args:
            year (str): year of demand

        Returns:
            pd.DataFrame: concatenation of all CSVs that were read in as a dataframe
        """

        input_data_folder = self.config.air_passenger.input_demand_folder

        _airport_direction = itertools.product(
            self.config.air_passenger.airport_names,
            ["to", "from"],
        )
        demand_df = None
        for airport, direction in _airport_direction:

            _file_name = self.config.air_passenger.input_demand_filename_tmpl.format(
                airport=airport, year=year, direction=direction
            )
            _file_path = self.controller.run_dir / input_data_folder / _file_name
            _df = pd.read_csv(_file_path)
            if demand_df is not None:
                demand_df = pd.concat([demand_df, _df])
            else:
                demand_df = _df

        return demand_df

    def _aggregate_demand(self, input_demand: pd.DataFrame) -> pd.DataFrame:
        """Aggregate demand into the assignable classes for each year.

        Args:
            input_demand: pandas dataframe with the following columns: TODO
        """
        aggr_demand = pd.DataFrame()

        _year_tp_group_accessmode = itertools.product(
            [self._start_year, self._end_year],
            self._periods,
            self._mode_groups.items(),
        )

        # TODO This should be done entirely in pandas using group-by
        for _year, _period, (_group, _access_modes) in _year_tp_group_accessmode:
            data = input_demand[
                [f"{_period}_{_access}_{_group}_{_year}" for _access in _access_modes]
            ]
            aggr_demand[f"{_period}_{_group}_{_year}"] = data.sum(axis=1)

        return aggr_demand

    def _export_result(self, demand_df: pd.DataFrame):
        """Export resulting model year demand to OMX files by period."""
        path_tmplt = os.path.join(
            self.controller.run_dir, self.config.air_passenger.highway_demand_file
        )
        os.makedirs(os.path.dirname(path_tmplt), exist_ok=True)

        for period in self._periods:
            file_path = path_tmplt.format(period=period)
            df_to_omx(
                demand_df,
                {group: f"{period}_{group}" for group in self._mode_groups},
                file_path,
                orig_column="ORIG",
                dest_column="DEST",
            )
