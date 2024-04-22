"""Methods to handle observed data for the Acceptance Criteria summaries from a tm2py model run."""

from tm2py.acceptance.canonical import Canonical

import numpy as np
import os
import geopandas as gpd
import pandas as pd
import toml


class Observed:
    c: Canonical

    observed_dict: dict
    observed_file: str

    ctpp_2012_2016_df: pd.DataFrame
    census_2010_geo_df: pd.DataFrame
    census_2017_zero_vehicle_hhs_df: pd.DataFrame
    census_tract_centroids_gdf: gpd.GeoDataFrame

    RELEVANT_PEMS_OBSERVED_YEARS_LIST = [2014, 2015, 2016]
    RELEVANT_BRIDGE_TRANSACTIONS_YEARS_LIST = [2014, 2015, 2016]
    RELEVANT_PEMS_VEHICLE_CLASSES_FOR_LARGE_TRUCK = [6, 7, 8, 9, 10, 11, 12]

    ohio_rmse_standards_df = pd.DataFrame(
        [
            [250, 24, 200],
            [1000, 100, 100],
            [2000, 200, 62],
            [3000, 300, 54],
            [4000, 400, 48],
            [5000, 500, 45],
            [6250, 625, 42],
            [7750, 775, 39],
            [9250, 925, 36],
            [11250, 1125, 34],
            [13750, 1375, 31],
            [16250, 1625, 30],
            [18750, 1875, 28],
            [22500, 2250, 26],
            [30000, 3000, 24],
            [45000, 4500, 21],
            [65000, 6500, 18],
            [97500, 9750, 12],
        ],
        columns=[
            "daily_volume_midpoint",
            "hourly_volume_midpoint",
            "desired_percent_rmse",
        ],
    )

    reduced_traffic_counts_df = pd.DataFrame(
        {
            "model_link_id": pd.Series(dtype="int"),
            "source": pd.Series(dtype="str"),
            "station_id": pd.Series(dtype="str"),
            "vehicle_class": pd.Series(dtype="str"),
            "time_period": pd.Series(dtype="str"),
            "observed_flow": pd.Series(dtype="float"),
            "odot_flow_category_daily": pd.Series(dtype="str"),
            "odot_flow_category_hourly": pd.Series(dtype="str"),
            "odot_maximum_error": pd.Series(dtype="float"),
            "key_location": pd.Series(dtype="str"),
        }
    )

    key_arterials_df = pd.DataFrame(
        [
            ["San Pablo", "Alameda", 123, "XB", np.nan],
            ["19th Ave", "San Francisco", 1, "XB", np.nan],
            ["El Camino Real", "San Mateo", 82, "XB", np.nan],
            ["El Camino Real", "Santa Clara", 82, "XB", np.nan],
            ["Mission Blvd", "Alameda", 238, "XB", np.nan],
            ["Ygnacio Valley Road", "Contra Costa", "XB", np.nan, np.nan],
            ["Hwy 12", "Solano", 12, "XB", "409485_W"],
            ["Hwy 37", "Marin", 37, "XB", "402038_W"],
            ["Hwy 29", "Napa", 29, "XB", "401796_N"],
            ["CA 128", "Sonoma", 128, "XB", np.nan],
        ],
        columns=[
            "name",
            "county",
            "route",
            "direction",
            "pems_station_id",
        ],
    )

    bridges_df = pd.DataFrame(
        [
            ["Antioch Bridge", "NB", np.nan],
            ["Antioch Bridge", "NB", np.nan],
            ["Benecia-Martinez Bridge", "NB", "402541_N"],
            ["Benecia-Martinez Bridge", "SB", "402412_S"],
            ["Carquinez Bridge", "WB", "401638_W"],
            ["Carquinez Bridge", "EB", np.nan],
            ["Dumbarton Bridge", "WB", "400841_W"],
            ["Dumbarton Bridge", "EB", np.nan],
            ["Richmond-San Rafael Bridge", "WB", np.nan],
            ["Richmond-San Rafael Bridge", "EB", np.nan],
            ["San Francisco-Oakland Bay Bridge", "WB", "404917_W"],
            ["San Francisco-Oakland Bay Bridge", "EB", "404906_E"],
            ["San Mateo-Hayward Bridge", "WB", "400071_W"],
            ["San Mateo-Hayward Bridge", "EB", "400683_E"],
            ["Golden Gate Bridge", "NB", np.nan],
            ["Golden Gate Bridge", "SB", np.nan],
        ],
        columns=["name", "direction", "pems_station_id"],
    )

    observed_bridge_transactions_df: pd.DataFrame

    observed_bart_boardings_df: pd.DataFrame
    RELEVANT_BART_OBSERVED_YEARS_LIST = [2014, 2015, 2016]

    florida_transit_guidelines_df = pd.DataFrame(
        [
            [0, 1.50],  # low end of volume range, maximum error as percentage
            [1000, 1.00],
            [2000, 0.65],
            [5000, 0.35],
            [10000, 0.25],
            [np.inf, 0.20],
        ],
        columns=["boardings", "threshold"],
    )

    reduced_transit_on_board_df = pd.DataFrame(
        {
            "survey_tech": pd.Series(dtype="str"),
            "survey_operator": pd.Series(dtype="str"),
            "survey_route": pd.Series(dtype="str"),
            "survey_boardings": pd.Series(dtype="float"),
        }
    )

    reduced_transit_on_board_access_df: pd.DataFrame
    reduced_transit_spatial_flow_df: pd.DataFrame
    reduced_transit_district_flows_by_technology_df: pd.DataFrame

    def _load_configs(self):
        with open(self.observed_file, "r", encoding="utf-8") as toml_file:
            self.observed_dict = toml.load(toml_file)

        return

    def __init__(
        self,
        canonical: Canonical,
        observed_file: str,
        on_board_assign_summary: bool = False,
    ) -> None:
        self.c = canonical
        self.observed_file = observed_file
        self._load_configs()

        if not on_board_assign_summary:
            self._validate()
        elif on_board_assign_summary:
            self._reduce_observed_rail_access_summaries()

    def _validate(self):
        if self.reduced_transit_on_board_df.empty:
            self.reduce_on_board_survey()

        if self.reduced_traffic_counts_df.empty:
            self.reduce_traffic_counts()

        self.reduce_bridge_transactions()

        self._reduce_observed_rail_access_summaries()
        self._reduce_observed_rail_flow_summaries()
        self._make_district_to_district_transit_flows_by_technology()
        self._reduce_ctpp_2012_2016()
        self._reduce_census_zero_car_households()
        self._reduce_observed_bart_boardings()

        assert sorted(
            self.ctpp_2012_2016_df.residence_county.unique().tolist()
        ) == sorted(self.c.county_names_list)
        assert sorted(self.ctpp_2012_2016_df.work_county.unique().tolist()) == sorted(
            self.c.county_names_list
        )

        self._make_census_geo_crosswalk()

        return

    def _join_florida_thresholds(self, input_df: pd.DataFrame) -> pd.DataFrame:
        df = self.florida_transit_guidelines_df.copy()
        df["high"] = df["boardings"].shift(-1)
        df["low"] = df["boardings"]
        df = df.drop(["boardings"], axis="columns")
        df = df.rename(columns={"threshold": "florida_threshold"})

        all_df = (
            input_df[input_df["time_period"] == self.c.ALL_DAY_WORD]
            .copy()
            .reset_index(drop=True)
        )
        other_df = (
            input_df[input_df["time_period"] != self.c.ALL_DAY_WORD]
            .copy()
            .reset_index(drop=True)
        )
        other_df["florida_threshold"] = np.nan

        vals = all_df.survey_boardings.values
        high = df.high.values
        low = df.low.values

        i, j = np.where((vals[:, None] >= low) & (vals[:, None] <= high))

        return_df = pd.concat(
            [
                all_df.loc[i, :].reset_index(drop=True),
                df.loc[j, :].reset_index(drop=True),
            ],
            axis=1,
        )

        return_df = return_df.drop(["high", "low"], axis="columns")

        return pd.concat([return_df, other_df], axis="rows", ignore_index=True)

    def _join_standard_route_id(self, input_df: pd.DataFrame) -> pd.DataFrame:
        df = self.c.standard_transit_to_survey_df.copy()

        df["survey_agency"] = df["survey_agency"].map(
            self.c.canonical_agency_names_dict
        )
        join_df = df[~df["survey_agency"].isin(self.c.rail_operators_vector)].copy()

        join_all_df = join_df.copy()
        join_time_of_day_df = join_df.copy()

        # for daily, aggregate across time of day
        join_all_df = self.c.aggregate_line_names_across_time_of_day(
            join_all_df, "standard_line_name"
        )
        join_all_df = (
            join_all_df.groupby(
                [
                    "survey_agency",
                    "survey_route",
                    "standard_route_id",
                    "daily_line_name",
                    "canonical_operator",
                    "standard_route_short_name",
                ]
            )
            .agg({"standard_route_long_name": "first"})
            .reset_index()
        )

        join_all_df["standard_line_name"] = "N/A -- Daily Record"
        join_all_df["standard_headsign"] = "N/A -- Daily Record"

        all_df = pd.merge(
            input_df[input_df["time_period"] == self.c.ALL_DAY_WORD],
            join_all_df,
            how="left",
            left_on=["survey_operator", "survey_route"],
            right_on=["survey_agency", "survey_route"],
        )

        # by time of day
        join_time_of_day_df = (
            join_time_of_day_df.groupby(
                [
                    "survey_agency",
                    "survey_route",
                    "standard_route_id",
                    "standard_line_name",
                    "canonical_operator",
                    "standard_route_short_name",
                ]
            )
            .agg({"standard_route_long_name": "first", "standard_headsign": "first"})
            .reset_index()
        )

        df = (
            join_time_of_day_df["standard_line_name"]
            .str.split(pat="_", expand=True)
            .copy()
        )
        df["time_period"] = df[3].str.lower()
        join_time_of_day_df = pd.concat(
            [join_time_of_day_df, df["time_period"]], axis="columns"
        )

        join_time_of_day_df = self.c.aggregate_line_names_across_time_of_day(
            join_time_of_day_df, "standard_line_name"
        )

        time_of_day_df = pd.merge(
            input_df[input_df["time_period"] != self.c.ALL_DAY_WORD],
            join_time_of_day_df,
            how="left",
            left_on=["survey_operator", "survey_route", "time_period"],
            right_on=["survey_agency", "survey_route", "time_period"],
        )

        # observed records are not by direction, so we need to scale the boardings by 2 when the cases match
        time_of_day_df["survey_boardings"] = np.where(
            (time_of_day_df["standard_route_id"].isna())
            | (time_of_day_df["survey_operator"].isin(self.c.rail_operators_vector)),
            time_of_day_df["survey_boardings"],
            time_of_day_df["survey_boardings"] / 2.0,
        )

        return pd.concat([all_df, time_of_day_df], axis="rows", ignore_index=True)

    def reduce_on_board_survey(self, read_file_from_disk=True):
        """Reduces the on-board survey, summarizing boardings by technology, operator, route, and time of day.
        Result is stored in the reduced_transit_on_board_df DataFrame and written to disk in the `reduced_summaries_file`
        in the observed configuration.
        """

        if not self.c.canonical_agency_names_dict:
            self.c._make_canonical_agency_names_dict()

        file_root = self.observed_dict["remote_io"]["obs_folder_root"]
        in_file = self.observed_dict["transit"]["on_board_survey_file"]
        out_file = self.observed_dict["transit"]["reduced_summaries_file"]

        if os.path.isfile(out_file) and read_file_from_disk:
            self.reduced_transit_on_board_df = pd.read_csv(
                os.path.join(file_root, out_file),
                dtype=self.reduced_transit_on_board_df.dtypes.to_dict(),
            )
        else:
            in_df = pd.read_csv(os.path.join(file_root, in_file))
            out_df = in_df.copy()
            out_df["survey_operator"] = out_df["survey_operator"].map(
                self.c.canonical_agency_names_dict
            )
            out_df = self._join_florida_thresholds(out_df)
            out_df = self._join_standard_route_id(out_df)
            out_df.to_csv(os.path.join(file_root, out_file))
            self.reduced_transit_on_board_df = out_df

        return

    def _reduce_census_zero_car_households(self):
        file_root = self.observed_dict["remote_io"]["obs_folder_root"]
        in_file = self.observed_dict["census"]["vehicles_by_block_group_file"]

        df = pd.read_csv(os.path.join(file_root, in_file), skiprows=1)

        df = df[
            ["Geography", "Estimate!!Total", "Estimate!!Total!!No vehicle available"]
        ]
        df = df.rename(
            columns={
                "Geography": "geoid",
                "Estimate!!Total": "total_households",
                "Estimate!!Total!!No vehicle available": "observed_zero_vehicle_households",
            }
        )

        df["observed_zero_vehicle_household_share"] = (
            df["observed_zero_vehicle_households"] / df["total_households"]
        )

        self.census_2017_zero_vehicle_hhs_df = df.copy()

        return

    def _reduce_observed_bart_boardings(self):
        file_root = self.observed_dict["remote_io"]["obs_folder_root"]
        in_file = self.observed_dict["transit"]["bart_boardings_file"]

        df = pd.read_csv(os.path.join(file_root, in_file))

        assert "BART" in self.c.canonical_station_names_dict.keys()

        df["boarding"] = df["orig_name"].map(
            self.c.canonical_station_names_dict["BART"]
        )
        df["alighting"] = df["dest_name"].map(
            self.c.canonical_station_names_dict["BART"]
        )

        a_df = df[df.year.isin(self.RELEVANT_BART_OBSERVED_YEARS_LIST)].copy()

        sum_df = (
            a_df.groupby(["boarding", "alighting"])
            .agg({"avg_trips": "mean"})
            .reset_index()
        )
        sum_df = sum_df.rename(columns={"avg_trips": "observed"})

        self.observed_bart_boardings_df = sum_df.copy()

        return

    def _make_census_geo_crosswalk(self):
        file_root = self.observed_dict["remote_io"]["obs_folder_root"]
        pickle_file = self.observed_dict["census"]["census_geographies_pickle"]
        tract_geojson_file = self.observed_dict["census"][
            "census_tract_centroids_geojson"
        ]

        if os.path.exists(os.path.join(file_root, pickle_file)) and os.path.exists(
            os.path.join(file_root, tract_geojson_file)
        ):
            self.census_2010_geo_df = pd.read_pickle(
                os.path.join(file_root, pickle_file)
            )

            self.census_tract_centroids_gdf = gpd.read_file(
                os.path.join(file_root, tract_geojson_file)
            )
            return

        else:
            file_root = self.observed_dict["remote_io"]["obs_folder_root"]
            in_file = self.observed_dict["census"]["census_geographies_shapefile"]
            gdf = gpd.read_file(os.path.join(file_root, in_file))

            self._make_tract_centroids(gdf, os.path.join(file_root, tract_geojson_file))

            self.census_2010_geo_df = (
                pd.DataFrame(gdf)
                .rename(
                    columns={
                        "TRACTCE10": "tract",
                        "COUNTYFP10": "county_fips",
                        "STATEFP10": "state_fips",
                        "GEOID10": "blockgroup",
                    }
                )[["state_fips", "county_fips", "tract", "blockgroup"]]
                .copy()
            )

            self.census_2010_geo_df.to_pickle(os.path.join(file_root, pickle_file))

            return

    def _make_tract_centroids(
        self, input_gdf: gpd.GeoDataFrame, out_file: str
    ) -> gpd.GeoDataFrame:
        t_gdf = input_gdf.dissolve(by="TRACTCE10")
        c_gdf = t_gdf.to_crs(3857).centroid.to_crs(4326).reset_index()
        df = pd.DataFrame(t_gdf.reset_index())[["TRACTCE10", "COUNTYFP10", "STATEFP10"]]
        df["tract"] = df["STATEFP10"] + df["COUNTYFP10"] + df["TRACTCE10"]
        df["tract"] = df.tract.str.slice(start=1)  # remove leading zero
        r_df = c_gdf.merge(df, left_on="TRACTCE10", right_on="TRACTCE10")
        return_df = r_df[["tract", 0]].rename(columns={0: "geometry"}).copy()

        self.census_tract_centroids_gdf = gpd.GeoDataFrame(
            return_df, geometry="geometry"
        )

        self.census_tract_centroids_gdf.to_file(out_file)

        return

    def _reduce_ctpp_2012_2016(self):
        file_root = self.observed_dict["remote_io"]["obs_folder_root"]
        in_file = self.observed_dict["census"]["ctpp_2012_2016_file"]

        df = pd.read_csv(os.path.join(file_root, in_file), skiprows=2)

        a_df = df[(df["Output"] == "Estimate")].copy()
        a_df = a_df.rename(
            columns={
                "RESIDENCE": "residence_county",
                "WORKPLACE": "work_county",
                "Workers 16 and Over": "observed_flow",
            }
        )
        a_df = a_df[["residence_county", "work_county", "observed_flow"]]
        a_df["residence_county"] = a_df["residence_county"].str.replace(
            " County, California", ""
        )
        a_df["work_county"] = a_df["work_county"].str.replace(" County, California", "")
        a_df["observed_flow"] = a_df["observed_flow"].str.replace(",", "").astype(int)

        self.ctpp_2012_2016_df = a_df

        return

    def _reduce_observed_rail_access_summaries(self):
        root_dir = self.observed_dict["remote_io"]["obs_folder_root"]
        in_file = self.observed_dict["transit"]["reduced_access_summary_file"]

        df = pd.read_csv(os.path.join(root_dir, in_file))

        assert "operator" in df.columns
        df["operator"] = df["operator"].map(self.c.canonical_agency_names_dict)

        assert "boarding_station" in df.columns
        for operator in self.c.canonical_station_names_dict.keys():
            df.loc[df["operator"] == operator, "boarding_station"] = df.loc[
                df["operator"] == operator, "boarding_station"
            ].map(self.c.canonical_station_names_dict[operator])

        self.reduced_transit_on_board_access_df = df.copy()

        return

    def _reduce_observed_rail_flow_summaries(self):
        root_dir = self.observed_dict["remote_io"]["obs_folder_root"]
        in_file = self.observed_dict["transit"]["reduced_flow_summary_file"]

        df = pd.read_csv(os.path.join(root_dir, in_file))

        self.reduced_transit_spatial_flow_df = df.copy()

        return

    def _make_district_to_district_transit_flows_by_technology(self):
        o_df = self.reduced_transit_spatial_flow_df.copy()
        o_df = o_df[o_df["time_period"] == "am"].copy()

        tm2_district_dict = self.c.taz_to_district_df.set_index("taz_tm2")[
            "district_tm2"
        ].to_dict()
        o_df["orig_district"] = o_df["orig_taz"].map(tm2_district_dict)
        o_df["dest_district"] = o_df["dest_taz"].map(tm2_district_dict)

        for prefix in self.c.transit_technology_abbreviation_dict.keys():
            o_df["{}".format(prefix.lower())] = (
                o_df["is_{}_in_path".format(prefix.lower())] * o_df["observed_trips"]
            )

        agg_dict = {"observed_trips": "sum"}
        for prefix in self.c.transit_technology_abbreviation_dict.keys():
            agg_dict["{}".format(prefix.lower())] = "sum"

        sum_o_df = (
            o_df.groupby(["orig_district", "dest_district"]).agg(agg_dict).reset_index()
        )

        long_sum_o_df = sum_o_df.melt(
            id_vars=["orig_district", "dest_district"],
            var_name="tech",
            value_name="observed",
        )
        long_sum_o_df["tech"] = np.where(
            long_sum_o_df["tech"] == "observed_trips", "total", long_sum_o_df["tech"]
        )

        self.reduced_transit_district_flows_by_technology_df = long_sum_o_df.copy()

        return

    def _join_tm2_node_ids(self, input_df: pd.DataFrame) -> pd.DataFrame:
        df = input_df.copy()
        nodes_df = self.c.standard_to_emme_node_crosswalk_df.copy()

        df = (
            pd.merge(df, nodes_df, how="left", left_on="A", right_on="model_node_id")
            .rename(
                columns={
                    "emme_node_id": "emme_a_node_id",
                }
            )
            .drop(["model_node_id"], axis=1)
        )

        df = (
            pd.merge(df, nodes_df, how="left", left_on="B", right_on="model_node_id")
            .rename(
                columns={
                    "emme_node_id": "emme_b_node_id",
                }
            )
            .drop(["model_node_id"], axis=1)
        )

        return df

    def _join_ohio_standards(self, input_df: pd.DataFrame) -> pd.DataFrame:
        df = self.ohio_rmse_standards_df.copy()

        df["upper"] = (
            df["daily_volume_midpoint"].shift(-1) - df["daily_volume_midpoint"]
        ) / 2
        df["lower"] = (
            df["daily_volume_midpoint"].shift(1) - df["daily_volume_midpoint"]
        ) / 2
        df["low"] = df["daily_volume_midpoint"] + df["lower"]
        df["low"] = np.where(df["low"].isna(), 0, df["low"])
        df["high"] = df["daily_volume_midpoint"] + df["upper"]
        df["high"] = np.where(df["high"].isna(), np.inf, df["high"])

        df = df.drop(
            ["daily_volume_midpoint", "hourly_volume_midpoint", "upper", "lower"],
            axis="columns",
        )

        df = df.rename(
            columns={
                "desired_percent_rmse": "odot_maximum_error",
            }
        )

        vals = input_df.observed_flow.values
        high = df.high.values
        low = df.low.values

        i, j = np.where((vals[:, None] >= low) & (vals[:, None] <= high))

        return_df = pd.concat(
            [
                input_df.loc[i, :].reset_index(drop=True),
                df.loc[j, :].reset_index(drop=True),
            ],
            axis=1,
        )

        return_df["low_hourly"] = return_df["low"] / 10
        return_df["high_hourly"] = return_df["high"] / 10
        return_df["odot_flow_category_daily"] = (
            return_df["low"].astype("str") + "-" + return_df["high"].astype("str")
        )
        return_df["odot_flow_category_hourly"] = (
            return_df["low_hourly"].astype("str")
            + "-"
            + return_df["high_hourly"].astype("str")
        )

        return_df = return_df.drop(
            ["high", "low", "high_hourly", "low_hourly"], axis="columns"
        )

        return return_df

    def _identify_key_arterials_and_bridges(
        self, input_df: pd.DataFrame
    ) -> pd.DataFrame:
        df1 = self.key_arterials_df.copy()
        df2 = self.bridges_df.copy()
        df = pd.concat([df1, df2])[["name", "direction", "pems_station_id"]].rename(
            columns={
                "name": "key_location_name",
                "direction": "key_location_direction",
                "pems_station_id": "station_id",
            }
        )
        df = df.dropna().copy()
        out_df = pd.merge(input_df, df, how="left", on="station_id")

        return out_df

    def _reduce_truck_counts(self) -> pd.DataFrame:
        file_root = self.observed_dict["remote_io"]["obs_folder_root"]
        in_file = self.observed_dict["roadway"]["pems_truck_count_file"]

        in_df = pd.read_csv(os.path.join(file_root, in_file))

        df = in_df[in_df.year.isin(self.RELEVANT_PEMS_OBSERVED_YEARS_LIST)].copy()
        df = df[
            df["Vehicle.Class"].isin(self.RELEVANT_PEMS_VEHICLE_CLASSES_FOR_LARGE_TRUCK)
        ].copy()
        df = df.rename(
            columns={
                "Census.Station.Identifier": "station",
                "Freeway.Direction": "direction",
                "Station.Type": "type",
            }
        )
        df["station_id"] = df["station"].astype(str) + "_" + df["direction"]
        df = df[["station_id", "type", "year", "time_period", "median_flow"]].copy()

        return_df = (
            df.groupby(["station_id", "type", "time_period"])["median_flow"]
            .median()
            .reset_index()
        )
        return_df = return_df.rename(columns={"median_flow": "observed_flow"})

        return_df["vehicle_class"] = self.c.LARGE_TRUCK_VEHICLE_TYPE_WORD

        return return_df

    def reduce_bridge_transactions(self, read_file_from_disk=True):
        """
        Prepares observed bridge transaction data for Acceptance Comparisons.

        Args:
            read_file_from_disk (bool, optional): If `False`, will do calculations from source data. Defaults to True.
        """

        time_period_dict = {
            "3 am": "ea",
            "4 am": "ea",
            "5 am": "ea",
            "6 am": "am",
            "7 am": "am",
            "8 am": "am",
            "9 am": "am",
            "10 am": "md",
            "11 am": "md",
            "Noon": "md",
            "1 pm": "md",
            "2 pm": "md",
            "3 pm": "pm",
            "4 pm": "pm",
            "5 pm": "pm",
            "6 pm": "pm",
            "7 pm": "ev",
            "8 pm": "ev",
            "9 pm": "ev",
            "10 pm": "ev",
            "11 pm": "ev",
            "Mdnt": "ev",
            "1 am": "ev",
            "2 am": "ev",
        }

        file_root = self.observed_dict["remote_io"]["obs_folder_root"]
        in_file = self.observed_dict["roadway"]["bridge_transactions_file"]
        out_file = self.observed_dict["roadway"]["reduced_transactions_file"]

        if os.path.isfile(out_file) and read_file_from_disk:
            return_df = pd.read_pickle(
                os.path.join(file_root, out_file),
            )
        else:
            in_df = pd.read_csv(
                os.path.join(file_root, in_file), sep="\t", encoding="utf-16"
            )
            df = in_df[
                in_df["Year"].isin(self.RELEVANT_BRIDGE_TRANSACTIONS_YEARS_LIST)
            ].copy()
            df = df[df["Lane Designation"] == "all"].copy()
            hourly_median_df = (
                df.groupby(["Plaza Name", "Hour beginning"])["transactions"]
                .agg("median")
                .reset_index()
            )
            daily_df = (
                hourly_median_df.groupby(["Plaza Name"])["transactions"]
                .agg("sum")
                .reset_index()
                .rename(columns={"Plaza Name": "plaza_name"})
            )
            daily_df["time_period"] = self.c.ALL_DAY_WORD

            df = hourly_median_df.copy()
            df["time_period"] = hourly_median_df["Hour beginning"].map(time_period_dict)
            time_period_df = (
                df.groupby(["Plaza Name", "time_period"])["transactions"]
                .agg("sum")
                .reset_index()
                .rename(columns={"Plaza Name": "plaza_name"})
            )
            return_df = pd.concat([time_period_df, daily_df]).reset_index(drop=True)

            return_df.to_pickle(os.path.join(file_root, out_file))

        self.observed_bridge_transactions_df = return_df

        return

    def reduce_traffic_counts(self, read_file_from_disk=True):
        """
        Prepares observed traffic count data for Acceptance Comparisons by computing daily counts,
        joining with the TM2 link cross walk, and joining the Ohio Standards database.

        Args:
            read_file_from_disk (bool, optional): If `False`, will do calculations from source data. Defaults to True.
        """

        pems_df = self._reduce_pems_counts(read_file_from_disk=read_file_from_disk)
        caltrans_df = self._reduce_caltrans_counts()
        self.reduced_traffic_counts_df = pd.concat([pems_df, caltrans_df])

        return

    def _reduce_pems_counts(self, read_file_from_disk=True):
        """
        Prepares observed traffic count data for Acceptance Comparisons by computing daily counts,
        joining with the TM2 link cross walk, and joining the Ohio Standards database.

        Args:
            read_file_from_disk (bool, optional): If `False`, will do calculations from source data. Defaults to True.
        """

        file_root = self.observed_dict["remote_io"]["obs_folder_root"]
        in_file = self.observed_dict["roadway"]["pems_traffic_count_file"]
        out_file = self.observed_dict["roadway"]["reduced_pems_summaries_file"]

        if os.path.isfile(out_file) and read_file_from_disk:
            return_df = pd.read_pickle(
                os.path.join(file_root, out_file),
            )
        else:
            in_df = pd.read_csv(os.path.join(file_root, in_file))
            df = in_df[in_df.year.isin(self.RELEVANT_PEMS_OBSERVED_YEARS_LIST)].copy()
            df["station_id"] = df["station"].astype(str) + "_" + df["direction"]
            df = df[["station_id", "type", "year", "time_period", "median_flow"]].copy()
            median_across_years_all_vehs_df = (
                df.groupby(["station_id", "type", "time_period"])["median_flow"]
                .median()
                .reset_index()
            )
            median_across_years_all_vehs_df = median_across_years_all_vehs_df.rename(
                columns={"median_flow": "observed_flow"}
            )
            median_across_years_all_vehs_df[
                "vehicle_class"
            ] = self.c.ALL_VEHICLE_TYPE_WORD
            median_across_years_trucks_df = self._reduce_truck_counts()

            median_across_years_df = pd.concat(
                [median_across_years_all_vehs_df, median_across_years_trucks_df],
                axis="rows",
                ignore_index=True,
            )

            all_day_df = (
                median_across_years_df.groupby(["station_id", "type", "vehicle_class"])[
                    "observed_flow"
                ]
                .sum()
                .reset_index()
            )
            all_day_df["time_period"] = self.c.ALL_DAY_WORD

            out_df = pd.concat(
                [all_day_df, median_across_years_df], axis="rows", ignore_index=True
            )

            out_df = pd.merge(
                self.c.pems_to_link_crosswalk_df, out_df, how="left", on="station_id"
            )

            out_df = self._join_tm2_node_ids(out_df)

            # take median across multiple stations on same link
            median_df = (
                out_df.groupby(
                    [
                        "A",
                        "B",
                        "emme_a_node_id",
                        "emme_b_node_id",
                        "time_period",
                        "vehicle_class",
                    ]
                )["observed_flow"]
                .agg("median")
                .reset_index()
            )
            join_df = out_df[
                [
                    "emme_a_node_id",
                    "emme_b_node_id",
                    "time_period",
                    "station_id",
                    "type",
                    "vehicle_class",
                ]
            ].copy()
            return_df = pd.merge(
                median_df,
                join_df,
                how="left",
                on=["emme_a_node_id", "emme_b_node_id", "time_period", "vehicle_class"],
            ).reset_index(drop=True)

            # return_df = return_df.rename(columns = {"model_link_id" : "standard_link_id"})
            return_df = self._join_ohio_standards(return_df)
            return_df = self._identify_key_arterials_and_bridges(return_df)

            return_df["source"] = "PeMS"

            return_df.to_pickle(os.path.join(file_root, out_file))

        return return_df

    def _reduce_caltrans_counts(self):
        file_root = self.observed_dict["remote_io"]["obs_folder_root"]
        in_file = self.observed_dict["roadway"]["caltrans_count_file"]

        in_df = pd.read_csv(os.path.join(file_root, in_file))
        temp_df = (
            in_df[
                [
                    "2015 Traffic AADT",
                    "2015 Truck AADT",
                    "IModelNODE",
                    "JModelNODE",
                    "Calt_stn2",
                ]
            ]
            .copy()
            .rename(
                columns={
                    "IModelNODE": "A",
                    "JModelNODE": "B",
                    "Calt_stn2": "station_id",
                }
            )
        )
        out_cars_df = (
            temp_df.copy()
            .rename(columns={"2015 Traffic AADT": "observed_flow"})
            .drop(columns=["2015 Truck AADT"])
        )
        out_cars_df["vehicle_class"] = self.c.ALL_VEHICLE_TYPE_WORD

        out_trucks_df = (
            temp_df.copy()
            .rename(columns={"2015 Truck AADT": "observed_flow"})
            .drop(columns=["2015 Traffic AADT"])
        )
        out_trucks_df["vehicle_class"] = self.c.LARGE_TRUCK_VEHICLE_TYPE_WORD

        out_df = pd.concat([out_cars_df, out_trucks_df]).reset_index(drop=True)
        out_df = out_df[out_df["observed_flow"].notna()]

        # convert to one-way flow
        out_df["observed_flow"] = out_df["observed_flow"] / 2.0

        return_df = self._join_tm2_node_ids(out_df)
        return_df["time_period"] = self.c.ALL_DAY_WORD
        return_df["source"] = "Caltrans"

        return_df = self._join_ohio_standards(return_df)

        return return_df
