"""Demand loading from OMX to Emme database."""

from __future__ import annotations

import itertools
from abc import ABC
from typing import TYPE_CHECKING, Dict, List, Union
import pathlib

import numpy as np
import pandas as pd

from tm2py.components.component import Component, Subcomponent
from tm2py.emme.manager import Emmebank
from tm2py.emme.matrix import OMXManager
from tm2py.logger import LogStartEnd
from tm2py.matrix import redim_matrix
from collections import defaultdict


if TYPE_CHECKING:
    from tm2py.controller import RunController

NumpyArray = np.array


class EmmeDemand:
    """Abstract base class to import and average demand."""

    def __init__(self, controller: RunController):
        """Constructor for PrepareDemand class.

        Args:
            controller (RunController): Run controller for the current run.
        """
        self.controller = controller
        self._emmebank = None
        self._scenario = None
        self._source_ref_key = None

    @property
    def logger(self):
        """Reference to logger."""
        return self.controller.logger

    def _read(
        self, path: str, name: str, num_zones, factor: float = None
    ) -> NumpyArray:
        """Read matrix array from OMX file at path with name, and multiple by factor (if specified).

        Args:
            path: full path to OMX file
            name: name of the OMX matrix / key
            factor: optional factor to apply to matrix
        """
        with OMXManager(path, "r") as omx_file:
            demand = omx_file.read(name)
            omx_file.close()
        if factor is not None:
            demand = factor * demand
        demand = self._redim_demand(demand, num_zones)
        # self.logger.log(f"{name} sum: {demand.sum()}", level=3)
        return demand

    @staticmethod
    def _redim_demand(demand, num_zones):
        _shape = demand.shape
        if _shape < (num_zones, num_zones):
            demand = np.pad(
                demand, ((0, num_zones - _shape[0]), (0, num_zones - _shape[1]))
            )
        elif _shape > (num_zones, num_zones):
            ValueError(
                f"Provided demand matrix is larger ({_shape}) than the \
                specified number of zones: {num_zones}"
            )

        return demand

    def _save_demand(
        self,
        name: str,
        demand: NumpyArray,
        description: str = None,
        apply_msa: bool = False,
    ):
        """Save demand array to Emme matrix with name, optional description.

        Matrix will be created if it does not exist and the model is on iteration 0.

        Args:
            name: name of the matrix in the Emmebank
            demand: NumpyArray, demand array to save
            description: str, optional description to use in the Emmebank
            apply_msa: bool, default False: use MSA on matrix with current array
                values if model is on iteration >= 1
        """
        matrix = self._emmebank.emmebank.matrix(f'mf"{name}"')
        msa_iteration = self.controller.iteration
        if not apply_msa or msa_iteration <= 1:
            if not matrix:
                ident = self._emmebank.emmebank.available_matrix_identifier("FULL")
                matrix = self._emmebank.emmebank.create_matrix(ident)
                matrix.name = name
                if description is not None:
                    matrix.description = description
        else:
            if not matrix:
                raise Exception(f"error averaging demand: matrix {name} does not exist")
            prev_demand = matrix.get_numpy_data(self._scenario.id)
            demand = prev_demand + (1.0 / msa_iteration) * (demand - prev_demand)
        self.logger.log(f"{name} sum: {demand.sum()}", level="DEBUG")
        matrix.set_numpy_data(demand, self._scenario.id)


def avg_matrix_msa(
    prev_avg_matrix: NumpyArray, this_iter_matrix: NumpyArray, msa_iteration: int
) -> NumpyArray:
    """Average matrices based on Method of Successive Averages (MSA).

    Args:
        prev_avg_matrix (NumpyArray): Previously averaged matrix
        this_iter_matrix (NumpyArray): Matrix for this iteration
        msa_iteration (int): MSA iteration

    Returns:
        NumpyArray: MSA Averaged matrix for this iteration.
    """
    if msa_iteration < 1:
        return this_iter_matrix
    result_matrix = prev_avg_matrix + (1.0 / msa_iteration) * (
        this_iter_matrix - prev_avg_matrix
    )
    return result_matrix


class PrepareHighwayDemand(EmmeDemand):
    """Import and average highway demand.

    Demand is imported from OMX files based on reference file paths and OMX
    matrix names in highway assignment config (highway.classes).
    The demand is average using MSA with the current demand matrices
    (in the Emmebank) if the controller.iteration > 1.

    Args:
        controller: parent RunController object
    """

    def __init__(self, controller: RunController):
        """Constructor for PrepareHighwayDemand.

        Args:
            controller (RunController): Reference to run controller object.
        """
        super().__init__(controller)
        self.controller = controller
        self.config = self.controller.config.highway
        self._highway_emmebank = None

    def validate_inputs(self):
        # TODO
        pass

    @property
    def highway_emmebank(self):
        if self._highway_emmebank == None:
            self._highway_emmebank = self.controller.emme_manager.highway_emmebank
            self._emmebank = self._highway_emmebank
        return self._highway_emmebank

    # @LogStartEnd("prepare highway demand")
    def run(self):
        """Open combined demand OMX files from demand models and prepare for assignment."""

        self.highway_emmebank.create_zero_matrix()
        for time in self.controller.time_period_names:
            for klass in self.config.classes:
                self._prepare_demand(klass.name, klass.description, klass.demand, time)

    def _prepare_demand(
        self,
        name: str,
        description: str,
        demand_config: List[Dict[str, Union[str, float]]],
        time_period: str,
    ):
        """Load demand from OMX files and save to Emme matrix for highway assignment.

        Average with previous demand (MSA) if the current iteration > 1

        Args:
            name (str): the name of the highway assignment class
            description (str): the description for the highway assignment class
            demand_config (dict): the list of file cross-reference(s) for the demand to be loaded
                {"source": <name of demand model component>,
                 "name": <OMX key name>,
                 "factor": <factor to apply to demand in this file>}
            time_period (str): the time time_period ID (name)
        """
        self._scenario = self.highway_emmebank.scenario(time_period)
        num_zones = len(self._scenario.zone_numbers)
        demand = self._read_demand(demand_config[0], time_period, num_zones)
        for file_config in demand_config[1:]:
            demand = demand + self._read_demand(file_config, time_period, num_zones)
        demand_name = f"{time_period}_{name}"
        description = f"{time_period} {description} demand"
        self._save_demand(demand_name, demand, description, apply_msa=True)

    def _read_demand(self, file_config, time_period, num_zones):
        # Load demand from cross-referenced source file,
        # the named demand model component under the key highway_demand_file
        source = file_config["source"]
        name = file_config["name"].format(period=time_period.upper())
        path = self.controller.get_abs_path(
            self.controller.config[source].highway_demand_file
        ).__str__()
        return self._read(
            path.format(period=time_period, iter=self.controller.iteration),
            name,
            num_zones,
        )

    @LogStartEnd("Prepare household demand matrices.")
    def prepare_household_demand(self):
        """Prepares highway and transit household demand matrices from trip lists produced by CT-RAMP."""
        iteration = self.controller.iteration

        # Create folders if they don't exist
        pathlib.Path(
            self.controller.get_abs_path(
                self.controller.config.household.highway_demand_file
            )
        ).parents[0].mkdir(parents=True, exist_ok=True)
        pathlib.Path(
            self.controller.get_abs_path(
                self.controller.config.household.transit_demand_file
            )
        ).parents[0].mkdir(parents=True, exist_ok=True)
        #    pathlib.Path(self.controller.get_abs_path(self.controller.config.household.active_demand_file)).parents[0].mkdir(parents=True, exist_ok=True)

        indiv_trip_file = (
            self.controller.config.household.ctramp_indiv_trip_file.format(
                iteration=iteration
            )
        )
        joint_trip_file = (
            self.controller.config.household.ctramp_joint_trip_file.format(
                iteration=iteration
            )
        )
        it_full, jt_full = pd.read_csv(indiv_trip_file), pd.read_csv(joint_trip_file)

        # Add time period, expanded count
        time_period_start = dict(
            zip(
                [c.name.upper() for c in self.controller.config.time_periods],
                [c.start_period for c in self.controller.config.time_periods],
            )
        )
        # the last time period needs to be filled in because the first period may or may not start at midnight
        time_periods_sorted = sorted(
            time_period_start, key=lambda x: time_period_start[x]
        )  # in upper case
        first_period = time_periods_sorted[0]
        periods_except_last = time_periods_sorted[:-1]
        breakpoints = [time_period_start[tp] for tp in time_periods_sorted]
        it_full["time_period"] = (
            pd.cut(
                it_full.stop_period,
                breakpoints,
                right=False,
                labels=periods_except_last,
            )
            .cat.add_categories(time_periods_sorted[-1])
            .fillna(time_periods_sorted[-1])
            .astype(str)
        )
        jt_full["time_period"] = (
            pd.cut(
                jt_full.stop_period,
                breakpoints,
                right=False,
                labels=periods_except_last,
            )
            .cat.add_categories(time_periods_sorted[-1])
            .fillna(time_periods_sorted[-1])
            .astype(str)
        )
        it_full["eq_cnt"] = 1 / it_full.sampleRate
        it_full["eq_cnt"] = np.where(
            it_full["trip_mode"].isin([3, 4, 5]),
            0.5 * it_full["eq_cnt"],
            np.where(
                it_full["trip_mode"].isin([6, 7, 8]),
                0.35 * it_full["eq_cnt"],
                it_full["eq_cnt"],
            ),
        )
        jt_full["eq_cnt"] = jt_full.num_participants / jt_full.sampleRate
        zp_cav = self.controller.config.household.OwnedAV_ZPV_factor
        zp_tnc = self.controller.config.household.TNC_ZPV_factor

        maz_taz_df = pd.read_csv(
            self.controller.get_abs_path(self.controller.config.scenario.landuse_file),
            usecols=["MAZ", "TAZ"],
        )
        it_full = it_full.merge(
            maz_taz_df, left_on="orig_mgra", right_on="MAZ", how="left"
        ).rename(columns={"TAZ": "orig_taz"})
        it_full = it_full.merge(
            maz_taz_df, left_on="dest_mgra", right_on="MAZ", how="left"
        ).rename(columns={"TAZ": "dest_taz"})
        jt_full = jt_full.merge(
            maz_taz_df, left_on="orig_mgra", right_on="MAZ", how="left"
        ).rename(columns={"TAZ": "orig_taz"})
        jt_full = jt_full.merge(
            maz_taz_df, left_on="dest_mgra", right_on="MAZ", how="left"
        ).rename(columns={"TAZ": "dest_taz"})
        it_full["trip_mode"] = np.where(
            it_full["trip_mode"] == 14, 13, it_full["trip_mode"]
        )
        jt_full["trip_mode"] = np.where(
            jt_full["trip_mode"] == 14, 13, jt_full["trip_mode"]
        )

        num_zones = self.num_internal_zones
        OD_full_index = pd.MultiIndex.from_product(
            [range(1, num_zones + 1), range(1, num_zones + 1)]
        )

        def combine_trip_lists(it, jt, trip_mode):
            # combines individual trip list and joint trip list
            combined_trips = pd.concat(
                [it[(it["trip_mode"] == trip_mode)], jt[(jt["trip_mode"] == trip_mode)]]
            )
            combined_sum = combined_trips.groupby(["orig_taz", "dest_taz"])[
                "eq_cnt"
            ].sum()
            return combined_sum.reindex(OD_full_index, fill_value=0).unstack().values

        def create_zero_passenger_trips(
            trips, deadheading_factor, trip_modes=[1, 2, 3]
        ):
            zpv_trips = trips.loc[
                (trips["avAvailable"] == 1) & (trips["trip_mode"].isin(trip_modes))
            ]
            zpv_trips["eq_cnt"] = zpv_trips["eq_cnt"] * deadheading_factor
            zpv_trips = zpv_trips.rename(
                columns={"dest_taz": "orig_taz", "orig_taz": "dest_taz"}
            )
            return zpv_trips

        # create zero passenger trips for auto modes
        if it_full["avAvailable"].sum() > 0:
            it_zpav_trp = create_zero_passenger_trips(
                it_full, zp_cav, trip_modes=[1, 2, 3]
            )
            it_zptnc_trp = create_zero_passenger_trips(it_full, zp_tnc, trip_modes=[9])
            # Combining zero passenger trips to trip files
            it_full = pd.concat(
                [it_full, it_zpav_trp, it_zptnc_trp], ignore_index=True
            ).reset_index(drop=True)

        if jt_full["avAvailable"].sum() > 0:
            jt_zpav_trp = create_zero_passenger_trips(
                jt_full, zp_cav, trip_modes=[1, 2, 3]
            )
            jt_zptnc_trp = create_zero_passenger_trips(jt_full, zp_tnc, trip_modes=[9])
            # Combining zero passenger trips to trip files
            jt_full = pd.concat(
                [jt_full, jt_zpav_trp, jt_zptnc_trp], ignore_index=True
            ).reset_index(drop=True)

        # read properties from config

        mode_name_dict = self.controller.config.household.ctramp_mode_names
        income_segment_config = self.controller.config.household.income_segment

        if income_segment_config["enabled"]:
            # This only affects highway trip tables.

            hh_file = self.controller.config.household.ctramp_hh_file.format(
                iteration=iteration
            )
            hh = pd.read_csv(hh_file, usecols=["hh_id", "income"])
            it_full = it_full.merge(hh, on="hh_id", how="left")
            jt_full = jt_full.merge(hh, on="hh_id", how="left")

            suffixes = income_segment_config["segment_suffixes"]

            it_full["income_seg"] = pd.cut(
                it_full["income"],
                right=False,
                bins=income_segment_config["cutoffs"] + [float("inf")],
                labels=suffixes,
            ).astype(str)

            jt_full["income_seg"] = pd.cut(
                jt_full["income"],
                right=False,
                bins=income_segment_config["cutoffs"] + [float("inf")],
                labels=suffixes,
            ).astype(str)
        else:
            it_full["income_seg"] = ""
            jt_full["income_seg"] = ""
            suffixes = [""]

        # groupby objects for combinations of time period - income segmentation, used for highway modes only
        it_grp = it_full.groupby(["time_period", "income_seg"])
        jt_grp = jt_full.groupby(["time_period", "income_seg"])

        for time_period in time_periods_sorted:
            self.logger.debug(
                f"Producing household demand matrices for period {time_period}"
            )

            highway_out_file = OMXManager(
                self.controller.get_abs_path(
                    self.controller.config.household.highway_demand_file
                )
                .__str__()
                .format(period=time_period, iter=self.controller.iteration),
                "w",
            )
            transit_out_file = OMXManager(
                self.controller.get_abs_path(
                    self.controller.config.household.transit_demand_file
                )
                .__str__()
                .format(period=time_period),
                "w",
            )
            # active_out_file = OMXManager(
            #    self.controller.get_abs_path(self.controller.config.household.active_demand_file).__str__().format(period=time_period), 'w')

            # hsr_trips_file = _omx.open_file(
            #    self.controller.get_abs_path(self.controller.config.household.hsr_demand_file).format(year=self.controller.config.scenario.year, period=time_period))

            # interregional_trips_file = _omx.open_file(
            #   self.controller.get_abs_path(self.controller.config.household.interregional_demand_file).format(year=self.controller.config.scenario.year, period=time_period))

            highway_out_file.open()
            transit_out_file.open()
            # active_out_file.open()

            # Transit and active modes: one matrix per time period per mode
            it = it_full[it_full.time_period == time_period]
            jt = jt_full[jt_full.time_period == time_period]

            for trip_mode in mode_name_dict:
                #                if trip_mode in [9,10]:
                #                    matrix_name =  mode_name_dict[trip_mode]
                #                    self.logger.debug(f"Writing out mode {mode_name_dict[trip_mode]}")
                #                    active_out_file.write_array(numpy_array=combine_trip_lists(it,jt, trip_mode), name = matrix_name)

                if trip_mode == 11:
                    matrix_name = "WLK_TRN_WLK"
                    self.logger.debug(f"Writing out mode WLK_TRN_WLK")
                    # other_trn_trips = np.array(hsr_trips_file[matrix_name])+np.array(interregional_trips_file[matrix_name])
                    transit_out_file.write_array(
                        numpy_array=(combine_trip_lists(it, jt, trip_mode)),
                        name=matrix_name,
                    )

                elif trip_mode in [12, 13]:
                    it_outbound, it_inbound = it[it.inbound == 0], it[it.inbound == 1]
                    jt_outbound, jt_inbound = jt[jt.inbound == 0], jt[jt.inbound == 1]

                    matrix_name = f"{mode_name_dict[trip_mode].upper()}_TRN_WLK"
                    # other_trn_trips = np.array(hsr_trips_file[matrix_name])+np.array(interregional_trips_file[matrix_name])
                    self.logger.debug(
                        f"Writing out mode {mode_name_dict[trip_mode].upper() + '_TRN_WLK'}"
                    )
                    transit_out_file.write_array(
                        numpy_array=(
                            combine_trip_lists(it_outbound, jt_outbound, trip_mode)
                        ),
                        name=matrix_name,
                    )

                    matrix_name = f"WLK_TRN_{mode_name_dict[trip_mode].upper()}"
                    # other_trn_trips = np.array(hsr_trips_file[matrix_name])+np.array(interregional_trips_file[matrix_name])
                    self.logger.debug(
                        f"Writing out mode {'WLK_TRN_' + mode_name_dict[trip_mode].upper()}"
                    )
                    transit_out_file.write_array(
                        numpy_array=(
                            combine_trip_lists(it_inbound, jt_inbound, trip_mode)
                        ),
                        name=matrix_name,
                    )

            # Highway modes: one matrix per suffix (income class) per time period per mode
            for suffix in suffixes:
                highway_cache = {}

                if (time_period, suffix) in it_grp.groups.keys():
                    it = it_grp.get_group((time_period, suffix))
                else:
                    it = pd.DataFrame(None, columns=it_full.columns)

                if (time_period, suffix) in jt_grp.groups.keys():
                    jt = jt_grp.get_group((time_period, suffix))
                else:
                    jt = pd.DataFrame(None, columns=jt_full.columns)

                for trip_mode in sorted(mode_name_dict):
                    # Python preserves keys in the order they are inserted but
                    # mode_name_dict originates from TOML, which does not guarantee
                    # that the ordering of keys is preserved.  See
                    # https://github.com/toml-lang/toml/issues/162

                    if trip_mode in [
                        1,
                        2,
                        3,
                        4,
                        5,
                        6,
                        7,
                        8,
                        9,
                        10,
                        15,
                        16,
                        17,
                    ]:  # currently hard-coded based on Travel Mode trip mode codes
                        highway_cache[mode_name_dict[trip_mode]] = combine_trip_lists(
                            it, jt, trip_mode
                        )
                        out_mode = f"{mode_name_dict[trip_mode].upper()}"
                        matrix_name = (
                            f"{out_mode}_{suffix}_{time_period.upper()}"
                            if suffix
                            else f"{out_mode}_{time_period.upper()}"
                        )
                        highway_out_file.write_array(
                            numpy_array=highway_cache[mode_name_dict[trip_mode]],
                            name=matrix_name,
                        )

                    elif trip_mode in [15, 16]:
                        # identify the correct mode split factors for da, sr2, sr3
                        self.logger.debug(
                            f"Splitting ridehail trips into shared ride trips"
                        )
                        ridehail_split_factors = defaultdict(float)
                        splits = self.controller.config.household.rideshare_mode_split
                        for key in splits:
                            out_mode_split = self.controller.config.household.__dict__[
                                f"{key}_split"
                            ]
                            for out_mode in out_mode_split:
                                ridehail_split_factors[out_mode] += (
                                    out_mode_split[out_mode] * splits[key]
                                )

                        ridehail_trips = combine_trip_lists(it, jt, trip_mode)
                        for out_mode in ridehail_split_factors:
                            matrix_name = f"{out_mode}_{suffix}" if suffix else out_mode
                            self.logger.debug(f"Writing out mode {out_mode}")
                            highway_cache[out_mode] += (
                                (ridehail_trips * ridehail_split_factors[out_mode])
                                .astype(float)
                                .round(2)
                            )
                            highway_out_file.write_array(
                                numpy_array=highway_cache[out_mode], name=matrix_name
                            )

            highway_out_file.close()
            transit_out_file.close()
            # active_out_file.close()

    @property
    def num_internal_zones(self):
        df = pd.read_csv(
            self.controller.get_abs_path(self.controller.config.scenario.landuse_file),
            usecols=[self.controller.config.scenario.landuse_index_column],
        )
        return len(df["TAZ"].unique())

    @property
    def num_total_zones(self):
        self._emmebank_path = self.controller.get_abs_path(
            self.controller.config.emme.highway_database_path
        )
        self._emmebank = self.controller.emme_manager.emmebank(self._emmebank_path)
        time_period = self.controller.config.time_periods[0].name
        scenario = self.get_emme_scenario(
            self._emmebank.path, time_period
        )  # any scenario id works
        return len(scenario.zone_numbers)


class PrepareTransitDemand(EmmeDemand):
    """Import transit demand.

    Demand is imported from OMX files based on reference file paths and OMX
    matrix names in transit assignment config (transit.classes).
    The demand is average using MSA with the current demand matrices (in the
    Emmebank) if transit.apply_msa_demand is true if the
    controller.iteration > 1.

    """

    def __init__(self, controller: "RunController"):
        """Constructor for PrepareTransitDemand.

        Args:
            controller: RunController object.
        """
        super().__init__(controller)
        self.controller = controller
        self.config = self.controller.config.transit
        self._transit_emmebank = None

    def validate_inputs(self):
        """Validate the inputs."""
        # TODO

    @property
    def transit_emmebank(self):
        if not self._transit_emmebank:
            self._transit_emmebank = self.controller.emme_manager.transit_emmebank
            self._emmebank = self._transit_emmebank
        return self._transit_emmebank

    @LogStartEnd("Prepare transit demand")
    def run(self):
        """Open combined demand OMX files from demand models and prepare for assignment."""
        self._source_ref_key = "transit_demand_file"
        self.transit_emmebank.create_zero_matrix()
        _time_period_tclass = itertools.product(
            self.controller.time_period_names, self.config.classes
        )
        for _time_period, _tclass in _time_period_tclass:
            self._prepare_demand(
                _tclass.skim_set_id, _tclass.description, _tclass.demand, _time_period
            )

    def _prepare_demand(
        self,
        name: str,
        description: str,
        demand_config: List[Dict[str, Union[str, float]]],
        time_period: str,
    ):
        """Load demand from OMX files and save to Emme matrix for transit assignment.

        Average with previous demand (MSA) if the current iteration > 1 and
        config.transit.apply_msa_demand is True

        Args:
            name (str): the name of the transit assignment class in the OMX files, usually a number
            description (str): the description for the transit assignment class
            demand_config (dict): the list of file cross-reference(s) for the demand to be loaded
                {"source": <name of demand model component>,
                 "name": <OMX key name>,
                 "factor": <factor to apply to demand in this file>}
            time_period (str): the time _time_period ID (name)
        """
        self._scenario = self.transit_emmebank.scenario(time_period)
        num_zones = len(self._scenario.zone_numbers)
        demand = self._read_demand(demand_config[0], time_period, name, num_zones)
        for file_config in demand_config[1:]:
            demand = demand + self._read_demand(
                file_config, time_period, name, num_zones
            )
        demand_name = f"TRN_{name}_{time_period}"
        description = f"{time_period} {description} demand"
        apply_msa = self.config.apply_msa_demand
        self._save_demand(demand_name, demand, description, apply_msa=apply_msa)

    def _read_demand(self, file_config, time_period, skim_set, num_zones):
        # Load demand from cross-referenced source file,
        # the named demand model component under the key highway_demand_file
        if (
            self.controller.config.warmstart.warmstart
            and self.controller.iteration == 0
        ):
            source = file_config["source"]
            path = self.controller.get_abs_path(
                self.controller.config[source].transit_demand_file
            ).__str__()
        else:
            source = file_config["source"]
            path = self.controller.get_abs_path(
                self.controller.config[source].transit_demand_file
            ).__str__()
        name = file_config["name"]
        return self._read(
            path.format(
                period=time_period,
                # set=skim_set,
                # iter=self.controller.iteration
            ),
            name,
            num_zones,
        )
