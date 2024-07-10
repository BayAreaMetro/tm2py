"""Commercial vehicle / truck model module."""

from __future__ import annotations

import itertools
import os
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

import numpy as np
import pandas as pd

from tm2py.components.component import Component, Subcomponent
from tm2py.components.demand.toll_choice import TollChoiceCalculator
from tm2py.components.network.skims import get_blended_skim
from tm2py.emme.matrix import MatrixCache, OMXManager
from tm2py.logger import LogStartEnd
from tm2py.tools import zonal_csv_to_matrices

if TYPE_CHECKING:
    from tm2py.controller import RunController

NumpyArray = np.array


# mployment category mappings, grouping into larger categories
_land_use_aggregation = {
    "AGREMPN": ["ag"],
    "RETEMPN": ["ret_loc", "ret_reg"],
    "FPSEMPN": ["fire", "info", "lease", "prof", "serv_bus"],
    "HEREMPN": [
        "art_rec",
        "eat",
        "ed_high",
        "ed_k12",
        "ed_oth",
        "health",
        "hotel",
        "serv_pers",
        "serv_soc",
    ],
    "MWTEMPN": [
        "logis",
        "man_bio",
        "man_hvy",
        "man_lgt",
        "man_tech",
        "natres",
        "transp",
        "util",
    ],
    "OTHEMPN": ["constr", "gov"],
    "TOTEMP": ["emp_total"],
    "TOTHH": ["HH"],
}


class CommercialVehicleModel(Component):
    """Commercial Vehicle demand model.

    Generates truck demand matrices from:
        - land use
        - highway network impedances
        - parameters

    Segmented into four truck types:
        (1) very small trucks (two-axle, four-tire),
        (2) small trucks (two-axle, six-tire),
        (3) medium trucks (three-axle),
        (4) large or combination (four or more axle) trucks.

    Input:  (1) MAZ csv data file with the employment and household counts.
            (2) Skims
            (3) K-Factors
            (4)
    Output:

    Notes:
    (1) Based on the BAYCAST truck model, no significant updates.
    (2) Combined Chuck's calibration adjustments into the NAICS-based model coefficients.
    """

    def __init__(self, controller: RunController):
        """Constructor for the CommercialVehicleTripGeneration component.

        Args:
            controller (RunController): Run controller for model run.
        """
        super().__init__(controller)

        self.config = self.controller.config.truck
        self.sub_components = {
            "trip generation": CommercialVehicleTripGeneration(controller, self),
            "trip distribution": CommercialVehicleTripDistribution(controller, self),
            "time of day": CommercialVehicleTimeOfDay(controller, self),
            "toll choice": CommercialVehicleTollChoice(controller, self),
        }

        self.trk_impedances = {imp.name: imp for imp in self.config.impedances}

        # Emme matrix management (lazily evaluated)
        self._matrix_cache = None

        # Interim Results
        self.total_tripends_df = None
        self.daily_demand_dict = None
        self.trkclass_tp_demand_dict = None
        self.trkclass_tp_toll_demand_dict = None

    @property
    def purposes(self):
        return list(
            set([trk_class.purpose for trk_class in self.config.trip_gen.classes])
        )

    @property
    def classes(self):
        return [trk_class.name for trk_class in self.config.classes]

    def validate_inputs(self):
        """Validate the inputs."""
        # TODO

    @LogStartEnd()
    def run(self):
        """Run commercial vehicle model."""
        self.total_tripends_df = self.sub_components["trip generation"].run()
        self.daily_demand_dict = self.sub_components["trip distribution"].run(
            self.total_tripends_df
        )
        self.trkclass_tp_demand_dict = self.sub_components["time of day"].run(
            self.daily_demand_dict
        )
        self.trkclass_tp_toll_demand_dict = self.sub_components["toll choice"].run(
            self.trkclass_tp_demand_dict
        )
        self._export_results_as_omx(self.trkclass_tp_toll_demand_dict)

    @property
    def emmebank(self):
        """Reference to highway assignment Emmebank.

        TODO
            This should really be in the controller?
            Or part of network.skims?
        """
        self._emmebank = self.controller.emme_manager.highway_taz_emmebank
        return self._emmebank

    @property
    def emme_scenario(self):
        """Return emme scenario from emmebank.

        Use first valid scenario for reference Zone IDs.

        TODO
            This should really be in the controller?
            Or part of network.skims?
        """
        _ref_scenario_name = self.controller.config.time_periods[0].name
        return self.emmebank.scenario(_ref_scenario_name)

    @property
    def matrix_cache(self):
        """Access to MatrixCache to Emmebank for given emme_scenario."""
        if self._matrix_cache is None:
            self._matrix_cache = MatrixCache(self.emme_scenario)
        return self._matrix_cache

    @LogStartEnd(level="DEBUG")
    def _export_results_as_omx(self, class_demand):
        """Export assignable class demands to OMX files by time-of-day."""
        outdir = self.get_abs_path(self.config.output_trip_table_directory)
        os.makedirs(os.path.dirname(outdir), exist_ok=True)
        for period, matrices in class_demand.items():
            with OMXManager(
                os.path.join(
                    outdir, self.config.outfile_trip_table_tmp.format(period=period)
                ),
                "w",
            ) as output_file:
                for name, data in matrices.items():
                    output_file.write_array(data, name)


class CommercialVehicleTripGeneration(Subcomponent):
    """Commercial vehicle (truck) Trip Generation for 4 sizes of truck.

    The four truck types are:
        (1) very small trucks (two-axle, four-tire),
        (2) small trucks (two-axle, six-tire),
        (3) medium trucks (three-axle),
        (4) large or combination (four or more axle) trucks.

    Input:  (1) MAZ csv data file with the employment and household counts.
    Ouput:  Trips by 4 truck sizes

    Trip generation
    ---------------
    Use linear regression models to generate trip ends,
    balancing attractions to productions. Based on BAYCAST truck model.

    The truck trip generation models for small trucks (two-axle, six tire),
    medium trucks (three-axle), and large or combination (four or more axle)
    trucks are taken directly from the study: "I-880 Intermodal Corridor Study:
    Truck Travel in the San Francisco Bay Area", prepared by Barton Aschman in
    December 1992.  The coefficients are on page 223 of this report.

    The very small truck generation model is based on the Phoenix four-tire
    truck model documented in the TMIP Quick Response Freight Manual.

    Note that certain production models previously used SIC-based employment
    categories.  To both maintain consistency with the BAYCAST truck model and
    update the model to use NAICS-based employment categories, new regression
    models were estimated relating the NAICS-based employment data with the
    SIC-based-predicted trips.  The goal here is not to create a new truck
    model, but to mimic the old model with the available data.  Please see
    the excel spreadsheet TruckModel.xlsx for details.  The NAICS-based model
    results replicate the SIC-based model results quite well.
    """

    def __init__(self, controller: RunController, component: Component):
        """Constructor for the CommercialVehicleTripGeneration component.

        Args:
            controller (RunController): Run controller for model run.
            component (Component): Parent component of sub-component
        """
        super().__init__(controller, component)
        self.config = self.component.config.trip_gen

    def validate_inputs(self):
        """Validate the inputs."""
        # TODO
        pass

    @LogStartEnd()
    def run(self):
        """Run commercial vehicle trip distribution."""
        _landuse_df = self._aggregate_landuse()
        _unbalanced_tripends_df = self._generate_trip_ends(_landuse_df)
        _balanced_tripends_df = self._balance_pa(_unbalanced_tripends_df)
        total_tripends_df = self._aggregate_by_class(_balanced_tripends_df)
        return total_tripends_df

    @LogStartEnd(level="DEBUG")
    def _aggregate_landuse(self) -> pd.DataFrame:
        """Aggregates landuse data from input CSV by MAZ to TAZ and employment groups.

        TOTEMP, total employment (same regardless of classification system)
        RETEMPN, retail trade employment per the NAICS classification system
        FPSEMPN, financial and professional services employment per NAICS
        HEREMPN, health, educational, and recreational employment per  NAICS
        OTHEMPN, other employment per the NAICS classification system
        AGREMPN, agricultural employment per the NAICS classificatin system
        MWTEMPN, manufacturing, warehousing, and transportation employment per NAICS
        TOTHH, total households
        """
        maz_data_file = self.get_abs_path(
            self.controller.config.scenario.maz_landuse_file
        )
        maz_input_data = pd.read_csv(maz_data_file)
        zones = self.component.emme_scenario.zone_numbers
        maz_input_data = maz_input_data[maz_input_data["TAZ_ORIGINAL"].isin(zones)]
        taz_input_data = maz_input_data.groupby(["TAZ_ORIGINAL"]).sum()
        taz_input_data = taz_input_data.sort_values(by="TAZ_ORIGINAL")
        # combine categories
        taz_landuse = pd.DataFrame()
        for total_column, sub_categories in _land_use_aggregation.items():
            taz_landuse[total_column] = taz_input_data[sub_categories].sum(axis=1)
        taz_landuse.reset_index(inplace=True)
        return taz_landuse

    @LogStartEnd(level="DEBUG")
    def _generate_trip_ends(self, landuse_df: pd.DataFrame) -> pd.DataFrame:
        """Generate productions and attractions by class based on landuse and truck trip rates.

        Args:
            landuse_df (pd.DataFrame): DataFrame with aggregated landuse data.
                Expected columns for landuse are: AGREMPN, RETEMPN, FPSEMPN, HEREMPN,
                MWTEMPN, OTHEMPN, TOTEMP, TOTHH

        Returns:
            pd.DataFrame: DataFrame with unbalanced production and attraction trip ends.
        """
        tripends_df = pd.DataFrame()

        _class_pa = itertools.product(
            self.config.classes,
            ["production_formula", "attraction_formula"],
        )

        # TODO Do this with multi-indexing rather than relying on column naming

        for _c, _pa in _class_pa:
            _trip_type = _c.purpose
            _trk_class = _c.name

            if _pa.endswith("_formula"):
                _pa_short = _pa.split("_")[0]

            # linked trips (non-garage-based) - attractions (equal productions)
            if (_trip_type == "linked") & (_pa_short == "attraction"):
                tripends_df[f"{_trip_type}_{_trk_class}_{_pa_short}s"] = tripends_df[
                    f"{_trip_type}_{_trk_class}_productions"
                ]
            else:
                _constant = _c[_pa].constant
                _multiplier = _c[_pa].multiplier

                land_use_rates = pd.DataFrame(_c[_pa].land_use_rates).T
                land_use_rates = land_use_rates.rename(
                    columns=land_use_rates.loc["property"]
                ).drop("property", axis=0)

                _rate_trips_df = landuse_df.mul(land_use_rates.iloc[0])
                _trips_df = _rate_trips_df * _multiplier + _constant

                tripends_df[f"{_trip_type}_{_trk_class}_{_pa_short}s"] = _trips_df.sum(
                    axis=1
                ).round()

        return tripends_df

    @LogStartEnd(level="DEBUG")
    def _balance_pa(self, tripends_df: pd.DataFrame) -> pd.DataFrame:
        """Balance production and attractions.

        Args:
            tripends_df (pd.DataFrame): DataFrame with unbalanced production and attraction
                trip ends.

        Returns:
            pd.DataFrame: DataFrame with balanced production and attraction trip ends.
        """

        for _c in self.config.classes:
            _trip_type = _c.purpose
            _trk_class = _c.name
            _balance_to = _c.balance_to

            _tots = {
                "attractions": tripends_df[
                    f"{_trip_type}_{_trk_class}_attractions"
                ].sum(),
                "productions": tripends_df[
                    f"{_trip_type}_{_trk_class}_productions"
                ].sum(),
            }

            # if productions OR attractions are zero, fill one with other
            if not _tots["attractions"]:
                tripends_df[f"{_trip_type}_{_trk_class}_attractions"] = tripends_df[
                    f"{_trip_type}_{_trk_class}_productions"
                ]

            elif not _tots["productions"]:
                tripends_df[f"{_trip_type}_{_trk_class}_productions"] = tripends_df[
                    f"{_trip_type}_{_trk_class}_attractions"
                ]

            # otherwise balance based on sums
            elif _balance_to == "productions":
                tripends_df[f"{_trip_type}_{_trk_class}_attractions"] = tripends_df[
                    f"{_trip_type}_{_trk_class}_attractions"
                ] * (_tots["productions"] / _tots["attractions"])

            elif _balance_to == "attractions":
                tripends_df[f"{_trip_type}_{_trk_class}_productions"] = tripends_df[
                    f"{_trip_type}_{_trk_class}_productions"
                ] * (_tots["attractions"] / _tots["productions"])
            else:
                raise ValueError(f"{_balance_to} is not a valid balance_to value")
        return tripends_df

    @LogStartEnd(level="DEBUG")
    def _aggregate_by_class(self, tripends_df: pd.DataFrame) -> pd.DataFrame:
        """Sum tripends by class across trip purpose.

        Args:
            tripends_df (pd.DataFrame): DataFrame with balanced production and attraction

        Returns:
            pd.DataFrame: DataFrame with aggregated tripends by truck class. Returned columns are:
                vsmtrk_prod, vsmtrk_attr,
                smltrk_prod, smltrk_attr,
                medtrk_prod, medtrk_attr,
                lrgtrk_prod, lrgtrk_attr
        """
        agg_tripends_df = pd.DataFrame()

        _class_pa = itertools.product(
            self.component.classes,
            ["productions", "attractions"],
        )

        for _trk_class, _pa in _class_pa:
            _sum_cols = [
                c for c in tripends_df.columns if c.endswith(f"_{_trk_class}_{_pa}")
            ]
            agg_tripends_df[f"{_trk_class}_{_pa}"] = pd.Series(
                tripends_df[_sum_cols].sum(axis=1)
            )

        agg_tripends_df.round(decimals=7)

        self.logger.log(agg_tripends_df.describe().to_string(), level="DEBUG")

        return agg_tripends_df


class CommercialVehicleTripDistribution(Subcomponent):
    """Commercial vehicle (truck) Trip Distribution for 4 sizes of truck.

    The four truck types are:
        (1) very small trucks (two-axle, four-tire),
        (2) small trucks (two-axle, six-tire),
        (3) medium trucks (three-axle),
        (4) large or combination (four or more axle) trucks.

    Input:  (1) Trips by 4 truck sizes
            (2) highway skims for truck, time, distance, bridgetoll and value toll
            (3) friction factors lookup table
            (4) k-factors matrix
    Ouput:  Trips origin and destination matrices by 4 truck sizes

    A simple gravity model is used to distribute the truck trips, with
    separate friction factors used for each class of truck.

    A blended travel time is used as the impedance measure, specifically the weighted average
    of the AM travel time (one-third weight) and the midday travel time (two-thirds weight).

    Input:
        Level-of-service matrices for the AM peak period (6 am to 10 am) and midday
        period (10 am to 3 pm) which contain truck-class specific estimates of
        congested travel time (in minutes)

        A matrix of k-factors, as calibrated by Chuck Purvis.  Note the very small truck model
        does not use k-factors; the small, medium, and large trucks use the same k-factors.

        A table of friction factors in text format with the following fields, space separated:
        - impedance measure (blended travel time);
        - friction factors for very small trucks;
        - friction factors for small trucks;
        - friction factors for medium trucks; and,
        - friction factors for large trucks.

    Notes on distribution steps:
        load nonres/truck_kfactors_taz.csv
        load nonres/truckFF.dat
        Apply friction factors and kfactors to produce balancing matrix
        apply the gravity models using friction factors from nonres/truckFF.dat
        (note the very small trucks do not use the K-factors)
        Can use Emme matrix balancing for this - important note: reference
        matrices by name and ensure names are unique
        Trips rounded to 0.01, causes some instability in results

    Results: four total daily trips by truck type

    Notes:
    (1) Based on the BAYCAST truck model, no significant updates.
    (2) Combined Chuck's calibration adjustments into the NAICS-based model coefficients.

    """

    def __init__(self, controller: RunController, component: Component):
        """Constructor for the CommercialVehicleTripDistribution component.

        Args:
            controller (RunController): Run controller for model run.
            component (Component): Parent component of sub-component
        """
        super().__init__(controller, component)

        self.config = self.component.config.trip_dist
        self._k_factors = None
        self._blended_skims = {}
        self._friction_factors = None
        self._friction_factor_matrices = {}

        self._class_config = None

    @property
    def class_config(self):
        if not self._class_config:
            self._class_config = {c.name: c for c in self.config.classes}

        return self._class_config

    @property
    def k_factors(self):
        """Zone-to-zone values of truck K factors.

        Returns:
             NumpyArray: Zone-to-zone values of truck K factors.
        """
        if self._k_factors is None:
            self._k_factors = self._load_k_factors()
        return self._k_factors

    def _load_k_factors(self):
        """Loads k-factors from self.config.truck.k_factors_file csv file.

        Returns:
            NumpyArray: Zone-to-zone values of truck K factors.

        """
        """return zonal_csv_to_matrices(
            self.get_abs_path(self.config.k_factors_file),
            i_column="I_taz_tm2_v2_2",
            j_column="J_taz_tm2_v2_2",
            value_columns="truck_k",
            fill_zones=True,
            default_value=0,
            max_zone=max(self.component.emme_scenario.zone_numbers),
        )["truck_k"].values"""
        data = pd.read_csv(self.get_abs_path(self.config.k_factors_file))
        zones = np.unique(data["I_taz_tm2_v2_2"])
        num_data_zones = len(zones)
        row_index = np.searchsorted(zones, data["I_taz_tm2_v2_2"])
        col_index = np.searchsorted(zones, data["J_taz_tm2_v2_2"])
        k_factors = np.zeros((num_data_zones, num_data_zones))
        k_factors[row_index, col_index] = data["truck_k"]
        num_zones = len(self.component.emme_scenario.zone_numbers)
        padding = ((0, num_zones - num_data_zones), (0, num_zones - num_data_zones))
        k_factors = np.pad(k_factors, padding)

        return k_factors

    def blended_skims(self, mode: str):
        """Get blended skim. Creates it if doesn't already exist.

        Args:
            mode (str): Mode for skim

        Returns:
            _type_: _description_
        """
        if mode not in self._blended_skims:
            self._blended_skims[mode] = get_blended_skim(
                self.controller,
                mode=mode,
                blend=self.component.trk_impedances[mode]["time_blend"],
            )
        return self._blended_skims[mode]

    def friction_factor_matrices(
        self, trk_class: str, k_factors: Union[None, NumpyArray] = None
    ) -> NumpyArray:
        """Zone to zone NumpyArray of impedances for a given truck class.

        Args:
            trk_class (str): Truck class abbreviated name
            k_factors (Union[None,NumpyArray]): If not None, gives an zone-by-zone array of
                k-factors--additive impedances to be added on top of friciton factors.
                Defaults to None.

        Returns:
            NumpyArray: Zone-by-zone matrix of friction factors
        """
        if trk_class not in self._friction_factor_matrices.keys():
            self._friction_factor_matrices[
                trk_class
            ] = self._calculate_friction_factor_matrix(
                trk_class,
                self.class_config[trk_class].impedance,
                self.k_factors,
                self.class_config[trk_class].use_k_factors,
            )

        return self._friction_factor_matrices[trk_class]

    @LogStartEnd(level="DEBUG")
    def _calculate_friction_factor_matrix(
        self,
        segment_name,
        blended_skim_name: str,
        k_factors: Union[None, NumpyArray] = None,
        use_k_factors: bool = False,
    ):
        """Calculates friction matrix by interpolating time; optionally multiplying by k_factors.

        Args:
            segment_name: Name of the segment to calculate the friction factors for (i.e. vstruck)
            blended_skim_name (str): Name of blended skim
            k_factors (Union[None,NumpyArray): Optional k-factors matrix

        Returns:
            friction_matrix NumpyArray: friction matrix for a truck class
        """
        _friction_matrix = np.interp(
            self.blended_skims(blended_skim_name),
            self.friction_factors["time"].tolist(),
            self.friction_factors[segment_name],
        )

        if use_k_factors:
            if k_factors is not None:
                _friction_matrix = _friction_matrix * k_factors

        return _friction_matrix

    @property
    def friction_factors(self):
        """Table of friction factors for each time band by truck class.

        Returns:
            pd.DataFrame: DataFrame of friction factors read from disk.
        """
        if self._friction_factors is None:
            self._friction_factors = self._read_ffactors()
        return self._friction_factors

    def _read_ffactors(self) -> pd.DataFrame:
        """Load friction factors lookup tables from csv file to dataframe.

        Reads from file: config.truck.friction_factors_file with following assumed column order:
            time: Time
            vsmtrk: Very Small Truck FF
            smltrk: Small Truck FF
            medtrk: Medium Truck FF
            lrgtrk: Large Truck FF
        """
        _file_path = self.get_abs_path(self.config.friction_factors_file)
        return pd.read_csv(_file_path)

    def validate_inputs(self):
        """Validate the inputs."""
        # TODO
        pass

    @LogStartEnd()
    def run(self, tripends_df) -> Dict[str, NumpyArray]:
        """Run commercial vehicle trip distribution."""
        daily_demand_dict = {
            tc: self._distribute_ods(tripends_df, tc) for tc in self.component.classes
        }

        return daily_demand_dict

    @LogStartEnd(level="DEBUG")
    def _distribute_ods(
        self,
        tripends_df: pd.DataFrame,
        trk_class: str,
        orig_factor: float = 0.5,
        dest_factor: float = 0.5,
    ) -> NumpyArray:
        """Distribute a trip ends for a given a truck class.

        Args:
            tripends_df: dataframe with trip ends as "{trk_class}_prod" and{trk_class}_attr".
            trk_class: name of truck class to distribute.
            orig_factor (float, optional): Amount to factor towards origins. Defaults to 0.5.
            dest_factor (float, optional): Amount to factor towards destinations. Defaults to 0.5.

        Returns:
            NumpyArray: Distributed trip ends for given truck class
        """
        if orig_factor + dest_factor != 1.0:
            raise ValueError(
                "orig_factor ({orig_factor}) and dest_factor ({dest_factor}) must\
                sum to 1.0"
            )

        _prod_attr_matrix = self._matrix_balancing(
            tripends_df[f"{trk_class}_productions"].to_numpy(),
            tripends_df[f"{trk_class}_attractions"].to_numpy(),
            trk_class,
        )
        daily_demand = (
            orig_factor * _prod_attr_matrix
            + dest_factor * _prod_attr_matrix.transpose()
        )

        self.logger.log(
            f"{trk_class}, prod sum: {_prod_attr_matrix.sum()}, "
            f"daily sum: {daily_demand.sum()}",
            level="DEBUG",
        )

        return daily_demand

    def _matrix_balancing(
        self,
        orig_totals: NumpyArray,
        dest_totals: NumpyArray,
        trk_class: str,
    ) -> NumpyArray:
        """Distribute origins and destinations based on friction factors for a givein truck class.

        Args:
            orig_totals: Total demand for origins as a numpy array
            dest_totals: Total demand for destinations as a numpy array
            trk_class (str): Truck class name



        """
        matrix_balancing = self.controller.emme_manager.modeller.tool(
            "inro.emme.matrix_calculation.matrix_balancing"
        )
        matrix_round = self.controller.emme_manager.modeller.tool(
            "inro.emme.matrix_calculation.matrix_controlled_rounding"
        )

        # Transfer numpy to emmebank
        _ff_emme_mx_name = self.component.matrix_cache.set_data(
            f"{trk_class}_friction",
            self.friction_factor_matrices(trk_class),
            matrix_type="FULL",
        ).name

        _orig_tots_emme_mx_name = self.component.matrix_cache.set_data(
            f"{trk_class}_prod", orig_totals, matrix_type="ORIGIN"
        ).name

        _dest_tots_emme_mx_name = self.component.matrix_cache.set_data(
            f"{trk_class}_attr", dest_totals, matrix_type="DESTINATION"
        ).name

        # Create a destination matrix for output to live in Emmebank
        _result_emme_mx_name = self.component.matrix_cache.get_or_init_matrix(
            f"{trk_class}_daily_demand"
        ).name

        spec = {
            "od_values_to_balance": _ff_emme_mx_name,
            "origin_totals": _orig_tots_emme_mx_name,
            "destination_totals": _dest_tots_emme_mx_name,
            "allowable_difference": 0.01,
            "max_relative_error": self.config.max_balance_relative_error,
            "max_iterations": self.config.max_balance_iterations,
            "results": {"od_balanced_values": _result_emme_mx_name},
            "performance_settings": {
                "allowed_memory": None,
                "number_of_processors": self.controller.num_processors,
            },
            "type": "MATRIX_BALANCING",
        }
        matrix_balancing(spec, scenario=self.component.emme_scenario)

        matrix_round(
            _result_emme_mx_name,
            _result_emme_mx_name,
            min_demand=0.01,
            values_to_round="ALL_NON_ZERO",
            scenario=self.component.emme_scenario,
        )

        return self.component.matrix_cache.get_data(_result_emme_mx_name)


class CommercialVehicleTimeOfDay(Subcomponent):
    """Commercial vehicle (truck) Time of Day Split for 4 sizes of truck.

    Input:  Trips origin and destination matrices by 4 truck sizes
    Ouput:  20 trips origin and destination matrices by 4 truck sizes by 5 times periods

    Note:
        The diurnal factors are taken from the BAYCAST-90 model with adjustments made
    during calibration to the very small truck values to better match counts.
    """

    def __init__(self, controller: RunController, component: Component):
        """Constructor for the CommercialVehicleTimeOfDay component.

        Args:
            controller (RunController): Run controller for model run.
            component (Component): Parent component of sub-component
        """
        super().__init__(controller, component)

        self.config = self.component.config.time_of_day

        self.split_factor = "od"
        self._class_configs = None
        self._class_period_splits = None

    @property
    def time_periods(self):
        return self.controller.config.time_periods

    @property
    def classes(self):
        return [trk_class.name for trk_class in self.config.classes]

    @property
    def class_configs(self):
        if not self._class_configs:
            self._class_configs = {c.name: c for c in self.config.classes}
        return self._class_configs

    @property
    def class_period_splits(self):
        """Returns split fraction dictonary mapped to [time period class][time period]."""
        if not self._class_period_splits:
            self._class_period_splits = {
                c_name: {c.time_period: c for c in config.time_period_split}
                for c_name, config in self.class_configs.items()
            }

        return self._class_period_splits

    def validate_inputs(self):
        """Validate the inputs."""
        # TODO
        pass

    @LogStartEnd()
    def run(
        self, daily_demand: Dict[str, NumpyArray]
    ) -> Dict[str, Dict[str, NumpyArray]]:
        """Splits the daily demand by time of day based on factors in the config.

        Uses self.config.truck.classes.{class_name}.time_of_day_split to split the daily demand.

        #TODO use TimePeriodSplit
        Args:
            daily_demand: dictionary of truck type name to numpy array of
                truck type daily demand

        Returns:
             Nested dictionary of truck class: time period name => numpy array of demand
        """
        trkclass_tp_demand_dict = defaultdict(dict)

        _class_timeperiod = itertools.product(self.classes, self.time_period_names)

        for _t_class, _tp in _class_timeperiod:
            trkclass_tp_demand_dict[_t_class][_tp] = np.around(
                self.class_period_splits[_t_class][_tp.lower()][self.split_factor]
                * daily_demand[_t_class],
                decimals=2,
            )

        return trkclass_tp_demand_dict


class CommercialVehicleTollChoice(Subcomponent):
    """Commercial vehicle (truck) toll choice.

    A binomial choice model for very small, small, medium, and large trucks.
    A separate value toll paying versus no value toll paying path choice
    model is applied to each of the twenty time period and vehicle type combinations.

    Input:  (1) Trip tables by time of day and truck class
            (2) Skims providing the time and cost for value toll and non-value toll paths
            for each; the matrix names in the OMX files are:
                "{period}_{cls_name}_time"
                "{period}_{cls_name}_dist"
                "{period}_{cls_name}_bridgetoll{grp_name}"
                "{period}_{cls_name}toll_time"
                "{period}_{cls_name}toll_dist"
                "{period}_{cls_name}toll_bridgetoll{grp_name}"
                "{period}_{cls_name}toll_valuetoll{grp_name}"
            Where period is the assignment period, cls_name is the truck assignment
            class name (as very small, small and medium truck are assigned as the
            same class) and grp_name is the truck type name (as the tolls are
            calculated separately for very small, small and medium).

    Results: a total of forty demand matrices, by time of day, truck type and toll/non-toll.

    Notes:  (1)  TOLLCLASS is a code, 1 through 10 are reserved for bridges; 11 and up is
                 reserved for value toll facilities.
            (2)  All costs should be coded in year 2000 cents
            (3)  The 2-axle fee is used for very small trucks
            (4)  The 2-axle fee is used for small trucks
            (5)  The 3-axle fee is used for medium trucks
            (6)  The average of the 5-axle and 6-axle fee is used for large trucks
                 (about the midpoint of the fee schedule).
            (7)  The in-vehicle time coefficient is from the work trip mode choice model.
    """

    def __init__(self, controller, component):
        """Constructor for Commercial Vehicle Toll Choice.

        Also calls Subclass __init__().

        Args:
            controller: model run controller
            component: parent component
        """
        super().__init__(controller, component)

        self.config = self.component.config.toll_choice

        self.sub_components = {
            "toll choice calculator": TollChoiceCalculator(
                controller,
                self,
                self.config,
            ),
        }

        # shortcut
        self._toll_choice = self.sub_components["toll choice calculator"]
        self._toll_choice.toll_skim_suffix = "trk"

    def validate_inputs(self):
        """Validate the inputs."""
        # TODO
        pass

    @LogStartEnd()
    def run(self, trkclass_tp_demand_dict):
        """Split per-period truck demands into nontoll and toll classes.

        Uses OMX skims output from highway assignment: traffic_skims_{period}.omx"""

        _tclass_time_combos = itertools.product(
            self.time_period_names, self.config.classes
        )

        class_demands = defaultdict(dict)
        for _time_period, _tclass in _tclass_time_combos:
            _split_demand = self._toll_choice.run(
                trkclass_tp_demand_dict[_tclass.name][_time_period],
                _tclass.name,
                _time_period,
            )

            class_demands[_time_period][_tclass.name] = _split_demand["non toll"]
            class_demands[_time_period][f"{_tclass.name}toll"] = _split_demand["toll"]
        return class_demands
