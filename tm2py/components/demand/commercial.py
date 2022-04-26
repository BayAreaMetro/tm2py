"""Commercial vehicle / truck model module.
"""
from __future__ import annotations

import os
from typing import Dict, List, TYPE_CHECKING
import numpy as np
import pandas as pd

from tm2py.components.component import Component
from tm2py.emme.matrix import OMXManager
from tm2py.logger import LogStartEnd
from tm2py.tools import parse_num_processors

if TYPE_CHECKING:
    from tm2py.controller import RunController


# employment category mappings, grouping into larger categories
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
_time_of_day_split = {
    "ea": {"vsmtrk": 0.0235, "smltrk": 0.0765, "medtrk": 0.0665, "lrgtrk": 0.1430},
    "am": {"vsmtrk": 0.0700, "smltrk": 0.2440, "medtrk": 0.2930, "lrgtrk": 0.2320},
    "md": {"vsmtrk": 0.6360, "smltrk": 0.3710, "medtrk": 0.3935, "lrgtrk": 0.3315},
    "pm": {"vsmtrk": 0.1000, "smltrk": 0.2180, "medtrk": 0.1730, "lrgtrk": 0.1750},
    "ev": {"vsmtrk": 0.1705, "smltrk": 0.0905, "medtrk": 0.0740, "lrgtrk": 0.1185},
}


class CommercialVehicleModel(Component):
    """Commercial vehicle (truck) demand model for 4 sizes of truck, toll choice, and time of day.

    Based on the BAYCAST truck model, note that there are no significant updates or changes.

    The four truck types are: very small trucks (two-axle, four-tire),
    small trucks (two-axle, six-tire), medium trucks (three-axle),
    and large or combination (four or more axle) trucks.

    Input:  (1) MAZ csv data file with the employment and household counts.
            (2) highway skims for truck, time, distance, bridgetoll and value toll
            (3) friction factors lookup table
            (4) k-factors matrix
    Ouput:  Trips by time-of-day for 4 truck sizes X 2 types, toll and nontoll

    Notes:
    (1) Based on the BAYCAST truck model, no significant updates.
    (2) Combined Chuck's calibration adjustments into the NAICS-based model coefficients.

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


    Trip distribution
    -----------------
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

    Results: four total daily trips by truck type


    Time of day
    -----------
    Segment daily estimates of truck flows into time-period-specific flows.
    The diurnal factors are taken from the BAYCAST-90 model with adjustments made
    during calibration to the very small truck values to better match counts.

    Input: four total daily trips by truck type

    Results: five time of days to four truck types each, for twenty trip tables


    Toll choice
    -----------
    A binomial choice model for very small, small, medium, and large trucks.
    A separate value toll paying versus no value toll paying path choice
    model is applied to each of the twenty time period and vehicle type combinations.

    Input:  (1) Trip tables by time of day and trip type
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

    def __init__(self, controller: RunController):
        super().__init__(controller)
        self._num_processors = parse_num_processors(self.config.emme.num_processors)
        self._scenario = None

    @LogStartEnd()
    def run(self):
        """Run truck sub-model to generate assignable truck class demand."""
        # future note: should not round intermediate results
        # future note: could use skim matrix cache from assignment (in same process)
        #              (save disk read time, minor optimization)
        self._setup_emme()
        taz_landuse = self._aggregate_landuse()
        trip_ends = self._generation(taz_landuse)
        daily_demand = self._distribution(trip_ends)
        period_demand = self._time_of_day(daily_demand)
        class_demands = self._toll_choice(period_demand)
        self._export_results(class_demands)

    def _setup_emme(self):
        """Create matrices for balancing."""
        # Note: using the highway assignment Emmebank
        emmebank = self.controller.emme_manager.emmebank(
            self.get_abs_path(self.config.emme.highway_database_path)
        )
        # use first valid scenario for reference Zone IDs
        ref_scenario_id = self.config.periods[0].emme_scenario_id
        self._scenario = emmebank.scenario(ref_scenario_id)
        # matrix names, internal to this class and Emme database
        # (used in _matrix_balancing method)
        # NOTE: possible improvement: could use temporary matrices (delete when finished)
        #       prefer retaining matrices for now for transparency
        matrices = {
            "FULL": [
                ("vsmtrk_friction", "very small truck friction factors"),
                ("smltrk_friction", "small truck friction factors"),
                ("medtrk_friction", "medium truck friction factors"),
                ("lrgtrk_friction", "large truck friction factors"),
                ("vsmtrk_daily_demand", "very small truck daily demand"),
                ("smltrk_daily_demand", "small truck daily demand"),
                ("medtrk_daily_demand", "medium truck daily demand"),
                ("lrgtrk_daily_demand", "large truck daily demand"),
            ],
            "ORIGIN": [
                ("vsmtrk_prod", "very small truck daily productions"),
                ("smltrk_prod", "small truck daily productions"),
                ("medtrk_prod", "medium truck daily productions"),
                ("lrgtrk_prod", "large truck daily productions"),
            ],
            "DESTINATION": [
                ("vsmtrk_attr", "very small truck daily attractions"),
                ("smltrk_attr", "small truck daily attractions"),
                ("medtrk_attr", "medium truck daily attractions"),
                ("lrgtrk_attr", "large truck daily attractions"),
            ],
        }
        for matrix_type, matrix_names in matrices.items():
            for name, desc in matrix_names:
                matrix = emmebank.matrix(name)
                if not matrix:
                    ident = emmebank.available_matrix_identifier(matrix_type)
                    matrix = emmebank.create_matrix(ident)
                    matrix.name = name
                matrix.description = desc

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
        maz_data_file = self.get_abs_path(self.config.scenario.maz_landuse_file)
        maz_input_data = pd.read_csv(maz_data_file)
        taz_input_data = maz_input_data.groupby(["TAZ_ORIGINAL"]).sum()
        taz_input_data = taz_input_data.sort_values(by="TAZ")
        # combine categories
        taz_landuse = pd.DataFrame()
        for total_column, sub_categories in _land_use_aggregation.items():
            taz_landuse[total_column] = taz_input_data[sub_categories].sum(axis=1)
        taz_landuse.reset_index(inplace=True)
        return taz_landuse

    @LogStartEnd(level="DEBUG")
    def _generation(self, landuse: pd.DataFrame) -> pd.DataFrame:
        """Run truck trip generation on input landuse dataframe.

        This step applies simple generation models, balances attractions
        and productions, sums linked and unlinked trips and returns vectors
        of production and attactions as a pandas dataframe.

        Expected columns for landuse are: AGREMPN, RETEMPN, FPSEMPN, HEREMPN,
        MWTEMPN, OTHEMPN, TOTEMP, TOTHH

        Returned columns are: vsmtrk_prod, vsmtrk_attr, smltrk_prod,
        smltrk_attr, medtrk_prod, medtrk_attr, lrgtrk_prod, lrgtrk_attr
        """

        link_trips = pd.DataFrame()
        # linked trips (non-garage-based) - productions
        # (very small updated with NAICS coefficients)
        link_trips["vsmtrk_prod"] = (
            0.96
            * (
                0.95409 * landuse.RETEMPN
                + 0.54333 * landuse.FPSEMPN
                + 0.50769 * landuse.HEREMPN
                + 0.63558 * landuse.OTHEMPN
                + 1.10181 * landuse.AGREMPN
                + 0.81576 * landuse.MWTEMPN
                + 0.26565 * landuse.TOTHH
            )
        ).round(decimals=0)
        link_trips["smltrk_prod"] = (0.0324 * landuse.TOTEMP).round()
        link_trips["medtrk_prod"] = (0.0039 * landuse.TOTEMP).round()
        link_trips["lrgtrk_prod"] = (0.0073 * landuse.TOTEMP).round()
        # linked trips (non-garage-based) - attractions (equal productions)
        link_trips["vsmtrk_attr"] = link_trips["vsmtrk_prod"]
        link_trips["smltrk_attr"] = link_trips["smltrk_prod"]
        link_trips["medtrk_attr"] = link_trips["medtrk_prod"]
        link_trips["lrgtrk_attr"] = link_trips["lrgtrk_prod"]
        garage_trips = pd.DataFrame()
        # garage-based - productions (updated NAICS coefficients)
        garage_trips["smltrk_prod"] = (
            0.02146 * landuse.RETEMPN
            + 0.02424 * landuse.FPSEMPN
            + 0.01320 * landuse.HEREMPN
            + 0.04325 * landuse.OTHEMPN
            + 0.05021 * landuse.AGREMPN
            + 0.01960 * landuse.MWTEMPN
        ).round()
        garage_trips["medtrk_prod"] = (
            0.00102 * landuse.RETEMPN
            + 0.00147 * landuse.FPSEMPN
            + 0.00025 * landuse.HEREMPN
            + 0.00331 * landuse.OTHEMPN
            + 0.00445 * landuse.AGREMPN
            + 0.00165 * landuse.MWTEMPN
        ).round()
        garage_trips["lrgtrk_prod"] = (
            0.00183 * landuse.RETEMPN
            + 0.00482 * landuse.FPSEMPN
            + 0.00274 * landuse.HEREMPN
            + 0.00795 * landuse.OTHEMPN
            + 0.01125 * landuse.AGREMPN
            + 0.00486 * landuse.MWTEMPN
        ).round()
        # garage-based - attractions
        garage_trips["smltrk_attr"] = (0.0234 * landuse.TOTEMP).round()
        garage_trips["medtrk_attr"] = (0.0046 * landuse.TOTEMP).round()
        garage_trips["lrgtrk_attr"] = (0.0136 * landuse.TOTEMP).round()
        # balance attractions to productions (applies to garage trips only)
        for name in ["smltrk", "medtrk", "lrgtrk"]:
            total_attract = garage_trips[name + "_attr"].sum()
            total_prod = garage_trips[name + "_prod"].sum()
            garage_trips[name + "_attr"] = garage_trips[name + "_attr"] * (
                total_prod / total_attract
            )

        trip_ends = pd.DataFrame(
            {
                "vsmtrk_prod": link_trips["vsmtrk_prod"],
                "vsmtrk_attr": link_trips["vsmtrk_attr"],
                "smltrk_prod": link_trips["smltrk_prod"] + garage_trips["smltrk_prod"],
                "smltrk_attr": link_trips["smltrk_attr"] + garage_trips["smltrk_attr"],
                "medtrk_prod": link_trips["medtrk_prod"] + garage_trips["medtrk_prod"],
                "medtrk_attr": link_trips["medtrk_attr"] + garage_trips["medtrk_attr"],
                "lrgtrk_prod": link_trips["lrgtrk_prod"] + garage_trips["lrgtrk_prod"],
                "lrgtrk_attr": link_trips["lrgtrk_attr"] + garage_trips["lrgtrk_attr"],
            }
        )
        trip_ends.round(decimals=7)
        for name, value in trip_ends:
            self.logger.log(f"{name}: {value.sum()}", level="DEBUG")
        return trip_ends

    @LogStartEnd(level="DEBUG")
    def _distribution(self, trip_ends: pd.DataFrame) -> Dict[str, np.array]:
        """Run trip distribution model for 4 truck types using Emme matrix balancing.

        Notes on distribution steps:
            load nonres\truck_kfactors_taz.csv
            load nonres\truckFF.dat
            Apply friction factors and kfactors to produce balancing matrix
            apply the gravity models using friction factors from nonres\truckFF.dat
            (note the very small trucks do not use the K-factors)
            Can use Emme matrix balancing for this - important note: reference
            matrices by name and ensure names are unique
            Trips rounded to 0.01, causes some instability in results

        Args:
            trip_ends: pandas dataframe of the production / attraction vectors

        Returns:
             Pandas dataframe of the the daily truck trip matrices
        """
        # load skims from OMX file
        trk_blend_time = self._get_blended_time("trk")
        lrgtrk_blend_time = self._get_blended_time("lrgtrk")
        # note that very small, small and medium are assigned as one class
        # and share the same output skims
        ffactors = self._load_ff_lookup_tables()
        k_factors = self._load_k_factors()
        friction_calculations = [
            {"name": "vsmtrk", "time": trk_blend_time, "use_k_factors": False},
            {"name": "smltrk", "time": trk_blend_time, "use_k_factors": True},
            {"name": "medtrk", "time": trk_blend_time, "use_k_factors": True},
            {"name": "lrgtrk", "time": lrgtrk_blend_time, "use_k_factors": True},
        ]
        daily_demand = {}
        for spec in friction_calculations:
            name = spec["name"]
            # lookup friction factor values from table with interpolation
            # and multiply by k-factors (no k-factors for very small truck)
            friction_matrix = np.interp(spec["time"], ffactors["time"], ffactors[name])
            if spec["use_k_factors"]:
                friction_matrix = friction_matrix * k_factors
            # run matrix balancing
            prod_attr_matrix = self._matrix_balancing(
                friction_matrix,
                trip_ends[f"{name}_prod"],
                trip_ends[f"{name}_attr"],
                name,
            )
            daily_demand[name] = (
                0.5 * prod_attr_matrix + 0.5 * prod_attr_matrix.transpose()
            )
            self.logger.log(
                f"{name}, prod sum: {prod_attr_matrix.sum()}, "
                f"daily sum: {daily_demand[name].sum()}",
                level="DEBUG",
            )
        return daily_demand

    def _get_blended_time(self, name: str) -> np.array:
        """Blend AM and MD truck times for distribution calculations.

        Uses 1/3 * AM + 2/3 * MD, sources skims from generated OMX files
        """
        skim_path_tmplt = self.get_abs_path(self.config.highway.output_skim_path)
        with OMXManager(skim_path_tmplt.format(period="AM"), "r") as am_file:
            am_time = am_file.read(f"am_{name}_time")
        with OMXManager(skim_path_tmplt.format(period="MD"), "r") as md_file:
            md_time = md_file.read(f"md_{name}_time")
        # blend truck times
        # compute blended truck time as an average 1/3 AM and 2/3 MD
        #     NOTE: Cube outputs skims\COM_HWYSKIMAM_taz.tpp, skims\COM_HWYSKIMMD_taz.tpp
        #           are in the highway_skims_{period}.omx files in Emme version
        #           with updated matrix names, {period}_trk_time, {period}_lrgtrk_time.
        #           Also, there will no longer be separate very small, small and medium
        #           truck times, as they are assigned together as the same class.
        #           There is only the trk_time.
        blend_time = 1 / 3.0 * am_time + 2 / 3.0 * md_time
        return blend_time

    @LogStartEnd(level="DEBUG")
    def _time_of_day(
        self, daily_demand: Dict[str, Dict[str, np.array]]
    ) -> Dict[str, np.array]:
        """Apply period factors to convert daily demand totals to per-period demands.

        Args:
            daily_demand: dictionary of truck type name to numpy array of
                truck type daily demand

        Returns:
             Nested dictionary of period name to truck type name to numpy array
             of per period demand.
        """
        period_demand = {}
        for period in [p.name for p in self.config.periods]:
            factor_map = _time_of_day_split[period]
            demand = {}
            for name, factor in factor_map.items():
                demand[name] = np.around(factor * daily_demand[name], decimals=2)
            period_demand[period] = demand
        return period_demand

    @LogStartEnd(level="DEBUG")
    def _toll_choice(
        self, period_demand: Dict[str, Dict[str, np.array]]
    ) -> Dict[str, Dict[str, np.array]]:
        """Split per-period truck demands into nontoll and toll classes.

        Args:
            period_demand: nested dictionary of period name to truck type name to numpy array
                of per period demand.

        Returns:
             Nested dictionary of period name to truck type name, either with toll
              of without toll, to numpy array assignable demand.
        """
        # input: time-of-day matrices
        # skims: skims\COM_HWYSKIM@token_period@_taz.tpp -> traffic_skims_{period}.omx
        #        NOTE matrix name changes in Emme version, using {period}_{class}_{skim}
        #        format

        class_demand = {}
        for period, demands in period_demand.items():
            skim_path_tmplt = self.get_abs_path(self.config.highway.output_skim_path)
            with OMXManager(skim_path_tmplt.format(period=period)) as skims:
                split_demand = {}
                for name, total_trips in demands.items():
                    cls_name = "trk" if name != "lrgtrk" else "lrgtrk"
                    grp_name = name[:3]

                    e_util_nontoll = self._get_exp_util(
                        skims,
                        f"{period}_{cls_name}_time",
                        f"{period}_{cls_name}_dist",
                        [f"{period}_{cls_name}_bridgetoll{grp_name}"],
                    )
                    e_util_toll = self._get_exp_util(
                        skims,
                        f"{period}_{cls_name}toll_time",
                        f"{period}_{cls_name}toll_dist",
                        [
                            f"{period}_{cls_name}toll_bridgetoll{grp_name}",
                            f"{period}_{cls_name}toll_valuetoll{grp_name}",
                        ],
                    )
                    prob_nontoll = e_util_nontoll / (e_util_toll + e_util_nontoll)
                    self._mask_non_available(
                        skims,
                        f"{period}_{cls_name}toll_valuetoll{grp_name}",
                        f"{period}_{cls_name}_time",
                        prob_nontoll,
                    )
                    split_demand[name] = prob_nontoll * total_trips
                    split_demand[f"{name}toll"] = (1 - prob_nontoll) * total_trips

                class_demand[period] = split_demand
        return class_demand

    def _get_exp_util(
        self, skims: OMXManager, time_name: str, dist_name: str, cost_names: List[str]
    ):
        """Calculate the exp(utils) for the truck times, distance and costs skims."""
        truck_vot = self.config.truck.value_of_time
        k_ivtt = self.config.truck.toll_choice_time_coefficient
        op_cost = self.config.truck.operating_cost_per_mile
        k_cost = (k_ivtt / truck_vot) * 0.6
        time = skims.read(time_name)
        dist = skims.read(dist_name)
        cost = skims.read(cost_names[0])
        for name in cost_names[1:]:
            cost += skims.read(name)
        e_util = np.exp(k_ivtt * time + k_cost * (op_cost * dist + cost))
        return e_util

    @staticmethod
    def _mask_non_available(
        skims: OMXManager,
        toll_cost_name: str,
        nontoll_time: str,
        prob_nontoll: np.array,
    ):
        toll_tollcost = skims.read(toll_cost_name)
        nontoll_time = skims.read(nontoll_time)
        prob_nontoll[(toll_tollcost == 0) | (toll_tollcost > 999999)] = 1.0
        prob_nontoll[(nontoll_time == 0) | (nontoll_time > 999999)] = 0.0

    @LogStartEnd(level="DEBUG")
    def _export_results(self, class_demand):
        """Export assignable class demands to OMX files by time-of-day."""
        path_tmplt = self.get_abs_path(self.config.truck.highway_demand_file)
        os.makedirs(os.path.dirname(path_tmplt), exist_ok=True)
        for period, matrices in class_demand.items():
            with OMXManager(path_tmplt.format(period=period), "w") as output_file:
                for name, data in matrices.items():
                    output_file.write_array(data, name)

    def _matrix_balancing(
        self,
        friction_matrix: np.array,
        orig_totals: np.array,
        dest_totals: np.array,
        name: str,
    ) -> np.array:
        """Run Emme matrix balancing tool using input arrays."""
        matrix_balancing = self.controller.emme_manager.tool(
            "inro.emme.matrix_calculation.matrix_balancing"
        )
        matrix_round = self.controller.emme_manager.tool(
            "inro.emme.matrix_calculation.matrix_controlled_rounding"
        )
        od_values_name = f"{name}_friction"
        orig_totals_name = f"{name}_prod"
        dest_totals_name = f"{name}_attr"
        result_name = f"{name}_daily_demand"
        # save O-D friction, prod and dest total values to Emmebank matrix
        self._save_to_emme_matrix(od_values_name, friction_matrix)
        self._save_to_emme_matrix(orig_totals_name, orig_totals)
        self._save_to_emme_matrix(dest_totals_name, dest_totals)
        spec = {
            "od_values_to_balance": od_values_name,
            "origin_totals": orig_totals_name,
            "destination_totals": dest_totals_name,
            "allowable_difference": 0.01,
            "max_relative_error": self.config.truck.max_balance_relative_error,
            "max_iterations": self.config.truck.max_balance_iterations,
            "results": {"od_balanced_values": result_name},
            "performance_settings": {
                "allowed_memory": None,
                "number_of_processors": self._num_processors,
            },
            "type": "MATRIX_BALANCING",
        }
        matrix_balancing(spec, scenario=self._scenario)
        matrix_round(
            result_name,
            result_name,
            min_demand=0.01,
            values_to_round="ALL_NON_ZERO",
            scenario=self._scenario,
        )
        matrix = self._scenario.emmebank.matrix(result_name)
        return matrix.get_numpy_data(self._scenario.id)

    def _save_to_emme_matrix(self, name: str, data: np.array):
        """Save numpy data to Emme matrix (in Emmebank) with specified name."""
        num_zones = len(self._scenario.zone_numbers)
        # reshape (e.g. pad externals) with zeros
        shape = data.shape
        if shape != [num_zones] * data.ndim:
            padding = [(0, num_zones - dim_shape) for dim_shape in shape]
            data = np.pad(data, padding)
        matrix = self._scenario.emmebank.matrix(name)
        matrix.set_numpy_data(data, self._scenario.id)

    def _load_ff_lookup_tables(self) -> Dict[str, List[float]]:
        """Load friction factors lookup tables from file [config.truck.friction_factors_file]."""
        #   time is in column 0, very small FF in 1, small FF in 2,
        #   medium FF in 3, and large FF in 4
        factors = {"time": [], "vsmtrk": [], "smltrk": [], "medtrk": [], "lrgtrk": []}
        file_path = self.get_abs_path(self.config.truck.friction_factors_file)
        with open(file_path, "r", encoding="utf8") as truck_ff:
            for line in truck_ff:
                tokens = line.split()
                for key, token in zip(factors.keys(), tokens):
                    factors[key].append(float(token))
        return factors

    def _load_k_factors(self) -> np.array:
        """Load k-factors table from CSV file [config.truck.k_factors_file]."""
        # NOTE: loading from this text format to numpy is pretty slow (~10 seconds)
        #       would be better to use a different format
        data = pd.read_csv(self.get_abs_path(self.config.truck.k_factors_file))
        zones = np.unique(data["I_taz_tm2_v2_2"])
        num_data_zones = len(zones)
        row_index = np.searchsorted(zones, data["I_taz_tm2_v2_2"])
        col_index = np.searchsorted(zones, data["J_taz_tm2_v2_2"])
        k_factors = np.zeros((num_data_zones, num_data_zones))
        k_factors[row_index, col_index] = data["truck_k"]
        num_zones = len(self._scenario.zone_numbers)
        padding = ((0, num_zones - num_data_zones), (0, num_zones - num_data_zones))
        k_factors = np.pad(k_factors, padding)
        return k_factors
