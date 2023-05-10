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
            "pems_station_id": pd.Series(dtype="int"),
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
            ["San Pablo", "Alameda", 123, np.nan],
            ["19th Ave", "San Franscisco", 1, "401180_N"],
            ["El Camino Real", "San Mateo", 82, np.nan],
            ["El Camino Real", "Santa Clara", 82, np.nan],
            ["Mission Blvd", "Alameda", 238, "400646_N"],
            ["Ygnacio Valley Road", "Contra Costa", np.nan, np.nan],
            ["Hwy 12", "Solano", 12, "409485_W"],
            ["Hwy 37", "Marin", 37, "402038_W"],
            ["Hwy 29", "Napa", 29, "402864_N"],
            ["CA 128", "Sonoma", 128, np.nan],
        ],
        columns=[
            "name",
            "county",
            "route",
            "pems_station_id",
        ],
    )

    bridges_df = pd.DataFrame(
        [
            ["Antioch Bridge", np.nan],
            ["Benecia-Martinez Bridge", "402156_N"],
            ["Carquinez Bridge", "401638_W"],
            ["Dumbarton Bridge", "400841_W"],
            ["Richmond-San Rafael Bridge", np.nan],
            ["San Francisco-Oakland Bay Bridge", "402827_W"],
            ["San Mateo-Hayward Bridge", "401272_W"],
            ["Golden Gate Bridge", np.nan],
        ],
        columns=["name", "pems_station_id"],
    )

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

    def __init__(self, canonical: Canonical, observed_file: str) -> None:
        self.c = canonical
        self.observed_file = observed_file
        self._load_configs()
        self._validate()

    def _validate(self):

        if self.reduced_transit_on_board_df.empty:
            self.reduce_on_board_survey()

        if self.reduced_traffic_counts_df.empty:
            self.reduce_traffic_counts()

        # self._reduce_observed_rail_access_summaries()
        # self._reduce_observed_rail_flow_summaries()
        # self._make_district_to_district_transit_flows_by_technology()
        # self._reduce_ctpp_2012_2016()
        # self._reduce_census_zero_car_households()
        # self._reduce_observed_bart_boardings()

        # assert sorted(
        #     self.ctpp_2012_2016_df.residence_county.unique().tolist()
        # ) == sorted(self.c.county_names_list)
        # assert sorted(self.ctpp_2012_2016_df.work_county.unique().tolist()) == sorted(
        #     self.c.county_names_list
        # )

        # self._make_census_geo_crosswalk()

        return

    def _join_florida_thresholds(self, input_df: pd.DataFrame) -> pd.DataFrame:

        df = self.florida_transit_guidelines_df.copy()
        df["high"] = df["boardings"].shift(-1)
        df["low"] = df["boardings"]
        df = df.drop(["boardings"], axis="columns")
        df = df.rename(columns={"threshold": "florida_threshold"})

        all_df = input_df[input_df["time_period"] == self.c.ALL_DAY_WORD].copy()
        other_df = input_df[input_df["time_period"] != self.c.ALL_DAY_WORD].copy()
        other_df["florida_threshold"] = np.nan

        vals = all_df.survey_boardings.values
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

        return pd.concat([all_df, time_of_day_df], axis="rows", ignore_index=True)

    def reduce_on_board_survey(self, read_file_from_disk=True):
        """Reduces the on-board survey, summarizing boardings by technology, operator, route, and time of day.
        Result is stored in the reduced_transit_on_board_df DataFrame and written to disk in the `reduced_summaries_file`
        in the observed configuration.
        """

        # TODO: replace with the summary from the on-board survey repo once the crosswalk is updated

        if not self.c.canonical_agency_names_dict:
            self._make_canonical_agency_names_dict()

        time_period_dict = {
            "EARLY AM": "ea",
            "AM PEAK": "am",
            "MIDDAY": "md",
            "PM PEAK": "pm",
            "EVENING": "ev",
            "NIGHT": "ev",
        }

        file_root = self.observed_dict["remote_io"]["obs_folder_root"]
        in_file = self.observed_dict["transit"]["on_board_survey_file"]
        out_file = self.observed_dict["transit"]["reduced_summaries_file"]

        if os.path.isfile(out_file) and read_file_from_disk:
            self.reduced_transit_on_board_df = pd.read_csv(
                os.path.join(file_root, out_file),
                dtype=self.reduced_transit_on_board_df.dtypes.to_dict(),
            )
        else:
            in_df = pd.read_feather(os.path.join(file_root, in_file))
            temp_df = in_df[
                (in_df["weekpart"].isna()) | (in_df["weekpart"] != "WEEKEND")
            ].copy()
            temp_df["time_period"] = temp_df["day_part"].map(time_period_dict)
            temp_df["route"] = np.where(
                temp_df["operator"].isin(self.c.rail_operators_vector),
                temp_df["operator"],
                temp_df["route"],
            )

            all_day_df = (
                temp_df.groupby(["survey_tech", "operator", "route"])["boarding_weight"]
                .sum()
                .reset_index()
            )
            all_day_df["time_period"] = self.c.ALL_DAY_WORD

            time_of_day_df = (
                temp_df.groupby(["survey_tech", "operator", "route", "time_period"])[
                    "boarding_weight"
                ]
                .sum()
                .reset_index()
            )

            out_df = pd.concat(
                [all_day_df, time_of_day_df], axis="rows", ignore_index=True
            )

            out_df = out_df.rename(
                columns={
                    "operator": "survey_operator",
                    "route": "survey_route",
                    "boarding_weight": "survey_boardings",
                }
            )
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

            # TODO: geopandas cannot read remote files, fix
            temp_file_root = "."
            in_file = "tl_2010_06_bg10.shp"

            gdf = gpd.read_file(os.path.join(temp_file_root, in_file))

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

        tm1_district_dict = self.c.taz_to_district_df.set_index("taz_tm1")[
            "district_tm1"
        ].to_dict()
        o_df["orig_district"] = o_df["orig_taz"].map(tm1_district_dict)
        o_df["dest_district"] = o_df["dest_taz"].map(tm1_district_dict)

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

    def _join_tm2_link_ids(self, input_df: pd.DataFrame) -> pd.DataFrame:

        df = self.c.pems_to_link_crosswalk_df.copy()
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

        df[["emme_a_node_id", "emme_b_node_id"]] = (
            df[["emme_a_node_id", "emme_b_node_id"]].fillna(0).astype(int)
        )

        return_df = pd.merge(
            df,
            input_df,
            how="left",
            on="pems_station_id",
        )

        return return_df

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
        df = pd.concat([df1, df2])[["name", "pems_station_id"]]
        df = df.dropna().copy()
        out_df = pd.merge(input_df, df, how="left", on="pems_station_id")
        out_df.rename(columns={"name": "key_location"})

        return out_df

    def reduce_traffic_counts(self, read_file_from_disk=True):
        """
        Prepares observed traffic count data for Acceptance Comparisons by computing daily counts,
        joining with the TM2 link cross walk, and joining the Ohio Standards database.

        Args:
            read_file_from_disk (bool, optional): If `False`, will do calculations from source data. Defaults to True.
        """

        file_root = self.observed_dict["remote_io"]["obs_folder_root"]
        in_file = self.observed_dict["roadway"]["pems_traffic_count_file"]
        out_file = self.observed_dict["roadway"]["reduced_summaries_file"]

        if os.path.isfile(out_file) and read_file_from_disk:
            self.reduced_traffic_counts_df = pd.read_csv(
                os.path.join(file_root, out_file),
                dtype=self.reduced_traffic_counts_df.dtypes.to_dict(),
            )
        else:
            in_df = pd.read_csv(os.path.join(file_root, in_file))
            df = in_df[in_df.year.isin(self.RELEVANT_PEMS_OBSERVED_YEARS_LIST)].copy()
            df["pems_station_id"] = df["station"].astype(str) + "_" + df["direction"]
            df = df[["pems_station_id", "year", "time_period", "median_flow"]].copy()
            median_across_years_df = (
                df.groupby(["pems_station_id", "time_period"])["median_flow"]
                .median()
                .reset_index()
            )
            median_across_years_df = median_across_years_df.rename(
                columns={"median_flow": "observed_flow"}
            )

            all_day_df = (
                median_across_years_df.groupby(["pems_station_id"])["observed_flow"]
                .sum()
                .reset_index()
            )
            all_day_df["time_period"] = self.c.ALL_DAY_WORD

            out_df = pd.concat(
                [all_day_df, median_across_years_df], axis="rows", ignore_index=True
            )

            out_df = self._join_ohio_standards(out_df)
            out_df = self._join_tm2_link_ids(out_df)
            out_df = self._identify_key_arterials_and_bridges(out_df)

            out_df.to_csv(os.path.join(file_root, out_file))

            self.reduced_traffic_counts_df = out_df

            return
