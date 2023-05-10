"""Methods to handle simulation results for the Acceptance Criteria summaries from a tm2py model run."""

from tm2py.acceptance.canonical import Canonical

import numpy as np
import os
import geopandas as gpd
import itertools
import openmatrix as omx
import pandas as pd
import toml


class Simulated:

    c: Canonical

    scenario_dict: dict
    model_dict: dict

    scenario_file: str
    model_file: str

    model_time_periods = []
    model_morning_capacity_factor: float

    simulated_roadway_am_shape_gdf: gpd.GeoDataFrame

    simulated_roadway_assignment_results_df = pd.DataFrame

    transit_access_mode_dict = {}
    transit_mode_dict = {}

    simulated_boardings_df: pd.DataFrame
    simulated_home_work_flows_df: pd.DataFrame
    simulated_maz_data_df: pd.DataFrame
    simulated_transit_segments_gdf: gpd.GeoDataFrame
    simulated_transit_access_df: pd.DataFrame
    simulated_zero_vehicle_hhs_df: pd.DataFrame
    simulated_station_to_station_df: pd.DataFrame
    simulated_transit_demand_df: pd.DataFrame
    simulated_transit_tech_in_vehicle_times_df: pd.DataFrame
    simulated_transit_district_to_district_by_tech_df: pd.DataFrame

    HEAVY_RAIL_NETWORK_MODE_DESCR: str
    COMMUTER_RAIL_NETWORK_MODE_DESCR: str

    standard_transit_stops_df: pd.DataFrame
    standard_transit_shapes_df: pd.DataFrame
    standard_transit_routes_df: pd.DataFrame
    standard_nodes_gdf: gpd.GeoDataFrame
    standard_to_emme_transit_nodes_df: pd.DataFrame

    def _load_configs(self):

        with open(self.scenario_file, "r", encoding="utf-8") as toml_file:
            self.scenario_dict = toml.load(toml_file)

        with open(self.model_file, "r", encoding="utf-8") as toml_file:
            self.model_dict = toml.load(toml_file)

        return

    def _get_model_time_periods(self):
        if not self.model_time_periods:
            for time_dict in self.model_dict["time_periods"]:
                self.model_time_periods.append(time_dict["name"])

        return

    def _get_morning_commute_capacity_factor(self):
        for time_dict in self.model_dict["time_periods"]:
            if time_dict["name"] == "am":
                self.model_morning_capacity_factor = time_dict[
                    "highway_capacity_factor"
                ]

        return

    def __init__(
        self, canonical: Canonical, scenario_file: str, model_file: str
    ) -> None:
        self.c = canonical
        self.scenario_file = scenario_file
        self.model_file = model_file
        self._load_configs()
        self._get_model_time_periods()
        self._get_morning_commute_capacity_factor()
        self._validate()

    def _validate(self):

        # self._make_transit_mode_dict()
        # self._make_simulated_maz_data()

        # self._read_standard_transit_stops()
        # self._read_standard_transit_shapes()
        # self._read_standard_transit_routes()
        # self._read_standard_node()

        # self._read_transit_demand()
        # self._make_transit_technology_in_vehicle_table_from_skims()
        # self._make_district_to_district_transit_summaries()

        # self._reduce_simulated_transit_boardings()
        # self._reduce_simulated_transit_shapes()
        # self._reduce_simulated_home_work_flows()
        # self._reduce_simulated_zero_vehicle_households()
        # self._reduce_simulated_station_to_station()
        # self._reduce_simulated_rail_access_summaries()

        self._reduce_simulated_roadway_assignment_outcomes()

        # assert sorted(
        #     self.simulated_home_work_flows_df.residence_county.unique().tolist()
        # ) == sorted(self.c.county_names_list)
        # assert sorted(
        #     self.simulated_home_work_flows_df.work_county.unique().tolist()
        # ) == sorted(self.c.county_names_list)

        return

    def _get_operator_name_from_line_name(
        self, input_df: pd.DataFrame, input_column_name: str, output_column_name: str
    ) -> pd.DataFrame:

        df = input_df[input_column_name].str.split(pat="_", expand=True).copy()
        df[output_column_name] = df[1]
        return_df = pd.concat([input_df, df[output_column_name]], axis="columns")

        return return_df

    def _reduce_simulated_transit_shapes(self):

        file_prefix = "boardings_by_segment_"
        file_root = self.scenario_dict["scenario"]["root_dir"]

        # AM for now, just to get the shapes
        # TODO: problem with remote read in, use . for now
        file_root = "."
        time_period = "am"
        gdf = gpd.read_file(
            os.path.join(file_root, file_prefix + time_period + ".geojson")
        )

        gdf["first_row_in_line"] = gdf.groupby("line").cumcount() == 0

        # Compute v/c ratio, excluding pnr dummy routes
        df = pd.DataFrame(gdf.drop(columns=["geometry"]))
        a_df = df[~(df["line"].str.contains("pnr_"))].reset_index().copy()
        a_df["am_segment_capacity_total"] = (
            a_df["capt"] * self.model_morning_capacity_factor
        )
        a_df["am_segment_capacity_seated"] = (
            a_df["caps"] * self.model_morning_capacity_factor
        )
        a_df["am_segment_vc_ratio_total"] = (
            a_df["voltr"] / a_df["am_segment_capacity_total"]
        )
        a_df["am_segment_vc_ratio_seated"] = (
            a_df["voltr"] / a_df["am_segment_capacity_seated"]
        )
        a_df = a_df.rename(columns={"voltr": "am_segment_volume"})

        sum_df = (
            a_df.groupby(["line"])
            .agg({"am_segment_vc_ratio_total": "mean"})
            .reset_index()
        )
        sum_df.columns = [
            "line",
            "mean_am_segment_vc_ratio_total",
        ]

        a_gdf = pd.merge(
            gdf,
            sum_df,
            on="line",
            how="left",
        )

        self.simulated_transit_segments_gdf = pd.merge(
            a_gdf,
            a_df[
                [
                    "line",
                    "i_node",
                    "j_node",
                    "am_segment_volume",
                    "am_segment_capacity_total",
                    "am_segment_capacity_seated",
                    "am_segment_vc_ratio_total",
                    "am_segment_vc_ratio_seated",
                ]
            ],
            on=["line", "i_node", "j_node"],
            how="left",
        )

        return

    def _join_coordinates_to_stations(self, input_df, input_column_name):

        station_list = input_df[input_column_name].unique().tolist()

        x_df = self.c.standard_to_emme_transit_nodes_df.copy()
        n_gdf = self.standard_nodes_gdf.copy()

        df = x_df[x_df["emme_node_id"].isin(station_list)].copy().reset_index(drop=True)
        n_trim_df = n_gdf[["model_node_id", "X", "Y"]].copy()
        df = pd.merge(df, n_trim_df, how="left", on="model_node_id")
        df = (
            df[["emme_node_id", "X", "Y"]]
            .copy()
            .rename(
                columns={
                    "emme_node_id": input_column_name,
                    "X": "boarding_lon",
                    "Y": "boarding_lat",
                }
            )
        )
        return_df = pd.merge(input_df, df, how="left", on=input_column_name)

        return return_df

    def _read_standard_node(self):

        root_dir = self.scenario_dict["scenario"]["root_dir"]

        # TODO fix geopandas relative
        # in_file = os.path.join(root_dir, "inputs", "network", "standard", "node.geojson")
        in_file = os.path.join(root_dir, "node.feather")

        gdf = gpd.read_feather(in_file)

        self.standard_nodes_gdf = gdf

        return

    def _read_standard_transit_stops(self):

        root_dir = self.scenario_dict["scenario"]["root_dir"]
        in_file = os.path.join(root_dir, "inputs", "network", "standard", "stops.txt")

        df = pd.read_csv(in_file)

        self.standard_transit_stops_df = df.copy()

        return

    def _read_standard_transit_shapes(self):

        root_dir = self.scenario_dict["scenario"]["root_dir"]
        in_file = os.path.join(root_dir, "inputs", "network", "standard", "shapes.txt")

        df = pd.read_csv(in_file)

        self.standard_transit_shapes_df = df

        return

    def _read_standard_transit_routes(self):

        root_dir = self.scenario_dict["scenario"]["root_dir"]
        in_file = os.path.join(root_dir, "inputs", "network", "standard", "routes.txt")

        df = pd.read_csv(in_file)

        self.standard_transit_routes_df = df

        return

    def _reduce_simulated_home_work_flows(self):

        # if self.simulated_maz_data_df.empty:
        #    self._make_simulated_maz_data()

        root_dir = self.scenario_dict["scenario"]["root_dir"]
        in_file = os.path.join(root_dir, "ctramp_output", "wsLocResults_3.csv")

        df = pd.read_csv(in_file)

        b_df = (
            pd.merge(
                df[["HHID", "HomeMGRA", "WorkLocation"]].copy(),
                self.simulated_maz_data_df[["MAZSEQ", "CountyName"]].copy(),
                how="left",
                left_on="HomeMGRA",
                right_on="MAZSEQ",
            )
            .rename(columns={"CountyName": "residence_county"})
            .drop(columns=["MAZSEQ"])
        )

        c_df = (
            pd.merge(
                b_df,
                self.simulated_maz_data_df[["MAZSEQ", "CountyName"]].copy(),
                how="left",
                left_on="WorkLocation",
                right_on="MAZSEQ",
            )
            .rename(columns={"CountyName": "work_county"})
            .drop(columns=["MAZSEQ"])
        )

        d_df = (
            c_df.groupby(["residence_county", "work_county"])
            .size()
            .reset_index()
            .rename(columns={0: "simulated_flow"})
        )

        self.simulated_home_work_flows_df = d_df

        return

    def _make_simulated_maz_data(self):

        root_dir = self.scenario_dict["scenario"]["root_dir"]
        in_file = os.path.join(root_dir, "inputs", "landuse", "maz_data.csv")

        df = pd.read_csv(in_file)

        index_file = os.path.join(root_dir, "hwy", "mtc_final_network_zone_seq.csv")

        index_df = pd.read_csv(index_file)
        join_df = index_df.rename(columns={"N": "MAZ_ORIGINAL"})[
            ["MAZ_ORIGINAL", "MAZSEQ"]
        ].copy()

        self.simulated_maz_data_df = pd.merge(
            df,
            join_df,
            how="left",
            on="MAZ_ORIGINAL",
        )

        return

    def _reduce_simulated_rail_access_summaries(self):

        if not self.transit_mode_dict:
            self._make_transit_mode_dict()

        file_prefix = "transit_segment_"
        file_root = self.scenario_dict["scenario"]["root_dir"]

        # AM for now
        time_period = "am"
        df = pd.read_csv(
            os.path.join(file_root, "trn", file_prefix + time_period + ".csv"),
            dtype={"stop_name": str, "mdesc": str},
            low_memory=False,
        )

        rail_df = df[
            df["mdesc"].isin(
                [
                    self.HEAVY_RAIL_NETWORK_MODE_DESCR,
                    self.COMMUTER_RAIL_NETWORK_MODE_DESCR,
                ]
            )
        ].copy()

        a_df = rail_df["line"].str.split(pat="_", expand=True).copy()
        rail_df["operator"] = a_df[1]
        rail_df["operator"] = rail_df["operator"].map(
            self.c.canonical_agency_names_dict
        )

        i_df = (
            rail_df[["operator", "i_node"]]
            .rename(columns={"i_node": "boarding"})
            .copy()
        )
        i_df["boarding"] = i_df["boarding"].astype(int)
        i_df = i_df[i_df["boarding"] > 0].copy()
        i_df = i_df.reset_index(drop=True)

        j_df = (
            rail_df[["operator", "j_node"]]
            .rename(columns={"j_node": "boarding"})
            .copy()
        )
        j_df = j_df[j_df["boarding"] != "None"].copy()
        j_df["boarding"] = j_df["boarding"].astype(int)
        j_df = j_df[j_df["boarding"] > 0].copy()
        j_df = j_df.reset_index(drop=True)

        i_j_df = pd.concat([i_df, j_df]).drop_duplicates()
        names_df = self._get_station_names_from_standard_network(i_j_df)
        if "level_0" in names_df.columns:
            names_df = names_df.drop(columns=["level_0", "index"])
        station_list = names_df.boarding.astype(str).unique().tolist()

        access_df = df.copy()
        access_df = access_df[access_df["j_node"].isin(station_list)].copy()
        access_df = access_df[
            ["i_node", "board_ptw", "board_wtp", "board_ktw", "board_wtk", "board_wtw"]
        ].copy()
        access_df["i_node"] = access_df.i_node.astype(int)

        access_dict = {
            "board_ptw": self.c.PARK_AND_RIDE_ACCESS_WORD,
            "board_wtp": self.c.WALK_ACCESS_WORD,
            "board_ktw": self.c.KISS_AND_RIDE_ACCESS_WORD,
            "board_wtk": self.c.WALK_ACCESS_WORD,
            "board_wtw": self.c.WALK_ACCESS_WORD,
        }
        long_df = pd.melt(
            access_df,
            id_vars=["i_node"],
            value_vars=[
                "board_ptw",
                "board_wtp",
                "board_ktw",
                "board_wtk",
                "board_wtw",
            ],
            var_name="mode",
            value_name="boardings",
        )
        long_df["access_mode"] = long_df["mode"].map(access_dict)

        join_df = pd.merge(
            long_df,
            names_df,
            how="left",
            left_on=["i_node"],
            right_on=["boarding"],
        ).reset_index(drop=True)

        out_df = (
            join_df.groupby(
                [
                    "operator",
                    "i_node",
                    "boarding_name",
                    "boarding_standard_node_id",
                    "access_mode",
                ]
            )
            .agg({"boardings": "sum"})
            .reset_index()
        )
        out_df = out_df.rename(
            columns={"boardings": "simulated_boardings", "i_node": "boarding"}
        )

        out_df["time_period"] = "am"

        self.simulated_transit_access_df = out_df

        return

    def _reduce_simulated_station_to_station(self):

        if self.model_time_periods is None:
            self._get_model_time_periods()

        root_dir = self.scenario_dict["scenario"]["root_dir"]

        path_list = [
            "WLK_TRN_WLK",
            "WLK_TRN_KNR",
            "KNR_TRN_WLK",
            "WLK_TRN_PNR",
            "PNR_TRN_WLK",
        ]
        operator_list = ["bart", "caltrain"]

        df = pd.DataFrame(
            {
                "operator": pd.Series(dtype=str),
                "time_period": pd.Series(dtype=str),
                "boarding": pd.Series(dtype=str),
                "alighting": pd.Series(dtype=str),
                "simulated": pd.Series(dtype=str),
            }
        )

        for (operator, time_period, path) in itertools.product(
            operator_list, self.model_time_periods, path_list
        ):
            input_file_name = os.path.join(
                root_dir,
                "trn",
                f"{operator}_station_to_station_{path}_{time_period}.txt",
            )
            file = open(input_file_name, "r")
            while True:
                line = file.readline()
                if not line:
                    break
                if line[0] == "c" or line[0] == "t" or line[0] == "d" or line[0] == "a":
                    continue
                else:
                    data = line.split()
                    subject_df = pd.DataFrame(
                        [[operator, time_period, data[0], data[1], data[2]]],
                        columns=[
                            "operator",
                            "time_period",
                            "boarding",
                            "alighting",
                            "simulated",
                        ],
                    )
                    df = df.append(subject_df).reset_index(drop=True)

            df["boarding"] = df["boarding"].astype(int)
            df["alighting"] = df["alighting"].astype(int)
            df["simulated"] = df["simulated"].astype(float)

            file.close()

        a_df = self._get_station_names_from_standard_network(
            df, operator_list=["BART", "Caltrain"]
        )
        sum_df = (
            a_df.groupby(["operator", "boarding_name", "alighting_name"])
            .agg({"simulated": "sum", "boarding_lat": "first", "boarding_lon": "first"})
            .reset_index()
        )

        self.simulated_station_to_station_df = sum_df.copy()

        return

    def _join_tm2_mode_codes(self, input_df):

        df = self.c.gtfs_to_tm2_mode_codes_df.copy()

        return_df = pd.merge(
            input_df,
            df,
            how="left",
            on="tm2_mode",
        )

        return return_df

    def _reduce_simulated_transit_boardings(self):

        file_prefix = "boardings_by_line_"
        file_root = self.scenario_dict["scenario"]["root_dir"]

        c_df = pd.DataFrame()
        for time_period in self.model_time_periods:

            df = pd.read_csv(
                os.path.join(file_root, file_prefix + time_period + ".csv")
            )
            df["time_period"] = time_period
            c_df = c_df.append(df)

        c_df = self._join_tm2_mode_codes(c_df)
        c_df["operator"] = c_df["operator"].map(self.c.canonical_agency_names_dict)
        c_df = self.c.aggregate_line_names_across_time_of_day(c_df, "line_name")

        time_period_df = (
            c_df.groupby(
                [
                    "daily_line_name",
                    "line_name",
                    "tm2_mode",
                    "line_mode",
                    "operator",
                    "technology",
                    "fare_system",
                    "time_period",
                ]
            )
            .agg({"total_boarding": np.sum, "total_hour_cap": np.sum})
            .reset_index()
        )

        daily_df = (
            time_period_df.groupby(
                [
                    "daily_line_name",
                    "tm2_mode",
                    "line_mode",
                    "operator",
                    "technology",
                    "fare_system",
                ]
            )
            .agg({"total_boarding": np.sum, "total_hour_cap": np.sum})
            .reset_index()
        )
        daily_df["time_period"] = self.c.ALL_DAY_WORD
        daily_df["line_name"] = "N/A -- Daily Record"

        self.simulated_boardings_df = pd.concat(
            [daily_df, time_period_df], axis="rows", ignore_index=True
        )

        return

    def _reduce_simulated_zero_vehicle_households(self):

        root_dir = self.scenario_dict["scenario"]["root_dir"]
        in_file = os.path.join(root_dir, "ctramp_output", "householdData_3.csv")

        df = pd.read_csv(in_file)

        sum_df = df.groupby(["home_mgra", "autos"]).size().reset_index(name="count")
        sum_df["vehicle_share"] = sum_df["count"] / sum_df.groupby("home_mgra")[
            "count"
        ].transform("sum")

        self.simulated_zero_vehicle_hhs_df = (
            sum_df[sum_df["autos"] == 0]
            .rename(
                columns={
                    "home_mgra": "maz",
                    "vehicle_share": "simulated_zero_vehicle_share",
                    "count": "simulated_households",
                }
            )[["maz", "simulated_zero_vehicle_share", "simulated_households"]]
            .copy()
        )

        return

    def _reduce_simulated_zero_vehicle_households(self):

        root_dir = self.scenario_dict["scenario"]["root_dir"]
        in_file = os.path.join(root_dir, "ctramp_output", "householdData_3.csv")

        df = pd.read_csv(in_file)

        sum_df = df.groupby(["home_mgra", "autos"]).size().reset_index(name="count")
        sum_df["vehicle_share"] = sum_df["count"] / sum_df.groupby("home_mgra")[
            "count"
        ].transform("sum")

        self.simulated_zero_vehicle_hhs_df = (
            sum_df[sum_df["autos"] == 0]
            .rename(
                columns={
                    "home_mgra": "maz",
                    "vehicle_share": "simulated_zero_vehicle_share",
                    "count": "simulated_households",
                }
            )[["maz", "simulated_zero_vehicle_share", "simulated_households"]]
            .copy()
        )

        return

    def _get_station_names_from_standard_network(
        self,
        input_df: pd.DataFrame,
        operator_list: list = None,
    ) -> pd.DataFrame:
        """_summary_

        Takes a dataframe with columns "boarding" (and, optionally, "alighting") that contains the node numbers of BART and Caltrain stations from the simulation. It then uses
        the standard network stops file (`stops.txt`) to get the station names and lat/lon coordinates for each subject station. The canonical station name is returned.

        Args:
            input_df (pd.DataFrame): A pandas dataframe with columns "boarding" and/or "alighting" that contain the node numbers of rail stations from the simulation.

        Returns:
            pd.DataFrame: The input_df with the canonical station name and lat/lon coordinates added as columns. If "boarding" and "alighting" columns are present, then name and coordinate columns are added for both the boarding and alighting station.
        """

        x_df = self.c.standard_to_emme_transit_nodes_df.copy()

        stations_list = []
        if "boarding" in input_df.columns:
            stations_list.extend(input_df["boarding"].unique().astype(int).tolist())

        if "alighting" in input_df.columns:
            stations_list.extend(input_df["alighting"].unique().astype(int).tolist())

        assert (
            len(stations_list) > 0
        ), "No boarding or alighting columns found in input_df."

        stations_list = list(set(stations_list))

        x_trim_df = (
            x_df[x_df["emme_node_id"].isin(stations_list)].copy().reset_index(drop=True)
        )

        station_df = self.standard_transit_stops_df[
            ["stop_name", "stop_lat", "stop_lon", "model_node_id"]
        ].copy()

        if "boarding" in input_df.columns:
            r_df = (
                pd.merge(
                    input_df,
                    x_trim_df,
                    left_on="boarding",
                    right_on="emme_node_id",
                    how="left",
                )
                .rename(columns={"model_node_id": "boarding_standard_node_id"})
                .reset_index(drop=True)
            )
            r_df = r_df.drop(columns=["emme_node_id"])

            r_df = (
                pd.merge(
                    r_df,
                    station_df,
                    left_on="boarding_standard_node_id",
                    right_on="model_node_id",
                    how="left",
                )
                .rename(
                    columns={
                        "stop_name": "boarding_name",
                        "stop_lat": "boarding_lat",
                        "stop_lon": "boarding_lon",
                    }
                )
                .reset_index(drop=True)
            )
            r_df = r_df.drop(columns=["model_node_id"])

        if "alighting" in input_df.columns:
            r_df = (
                pd.merge(
                    r_df,
                    x_trim_df,
                    left_on="alighting",
                    right_on="emme_node_id",
                    how="left",
                )
                .rename(columns={"model_node_id": "alighting_standard_node_id"})
                .reset_index(drop=True)
            )
            r_df = r_df.drop(columns=["emme_node_id"])

            r_df = (
                pd.merge(
                    r_df,
                    station_df,
                    left_on="alighting_standard_node_id",
                    right_on="model_node_id",
                    how="left",
                )
                .rename(
                    columns={
                        "stop_name": "alighting_name",
                        "stop_lat": "alighting_lat",
                        "stop_lon": "alighting_lon",
                    }
                )
                .reset_index(drop=True)
            )
            r_df = r_df.drop(columns=["model_node_id"])

        r_df["operator"] = r_df["operator"].map(self.c.canonical_agency_names_dict)

        return_df = None
        if operator_list is None:
            operator_list = self.c.canonical_station_names_dict.keys()
        for operator in operator_list:
            if operator in r_df["operator"].unique().tolist():
                sub_df = r_df[r_df["operator"] == operator].copy()

                if "boarding" in input_df.columns:
                    sub_df["boarding_name"] = sub_df["boarding_name"].map(
                        self.c.canonical_station_names_dict[operator]
                    )

                if "alighting" in input_df.columns:
                    sub_df["alighting_name"] = sub_df["alighting_name"].map(
                        self.c.canonical_station_names_dict[operator]
                    )

                if return_df is None:
                    return_df = sub_df.copy()
                else:
                    return_df = pd.concat([return_df, sub_df]).reset_index()

        return return_df

    def _make_transit_mode_dict(self):

        transit_mode_dict = {}
        for lil_dict in self.model_dict["transit"]["modes"]:
            add_dict = {lil_dict["mode_id"]: lil_dict["name"]}
            transit_mode_dict.update(add_dict)

        # TODO: this will be in model_config in TM2.2
        transit_mode_dict.update({"p": "pnr"})

        self.HEAVY_RAIL_NETWORK_MODE_DESCR = transit_mode_dict["h"]
        self.COMMUTER_RAIL_NETWORK_MODE_DESCR = transit_mode_dict["r"]

        access_mode_dict = {
            transit_mode_dict["w"]: self.c.WALK_ACCESS_WORD,
            transit_mode_dict["a"]: self.c.WALK_ACCESS_WORD,
            transit_mode_dict["e"]: self.c.WALK_ACCESS_WORD,
            transit_mode_dict["p"]: self.c.PARK_AND_RIDE_ACCESS_WORD,
        }
        access_mode_dict.update(
            {
                "knr": self.c.KISS_AND_RIDE_ACCESS_WORD,
                "bike": self.c.BIKE_ACCESS_WORD,
                self.c.WALK_ACCESS_WORD: self.c.WALK_ACCESS_WORD,
                self.c.BIKE_ACCESS_WORD: self.c.BIKE_ACCESS_WORD,
                self.c.PARK_AND_RIDE_ACCESS_WORD: self.c.PARK_AND_RIDE_ACCESS_WORD,
                self.c.KISS_AND_RIDE_ACCESS_WORD: self.c.KISS_AND_RIDE_ACCESS_WORD,
            }
        )

        self.transit_mode_dict = transit_mode_dict
        self.transit_access_mode_dict = access_mode_dict

        return

    def _make_transit_technology_in_vehicle_table_from_skims(self):

        path_list = [
            "WLK_TRN_WLK",
            "PNR_TRN_WLK",
            "WLK_TRN_PNR",
            "KNR_TRN_WLK",
            "WLK_TRN_KNR",
        ]
        path_list = ["WLK_TRN_WLK"]
        tech_list = self.c.transit_technology_abbreviation_dict.keys()

        skim_dir = os.path.join(self.scenario_dict["scenario"]["root_dir"], "skims")

        running_df = None
        for (path, time_period) in itertools.product(
            path_list, self.model_time_periods
        ):
            filename = os.path.join(
                skim_dir, "trnskm{}_{}.omx".format(time_period, path)
            )
            if os.path.exists(filename):
                omx_handle = omx.open_file(filename)

                # IVT
                matrix_name = "IVT"
                if matrix_name in omx_handle.listMatrices():
                    ivt_df = self._make_dataframe_from_omx(
                        omx_handle[matrix_name], matrix_name
                    )
                    ivt_df = ivt_df[ivt_df[matrix_name] > 0.0].copy()

                # Transfers to get boardings from trips
                matrix_name = "BOARDS"
                if matrix_name in omx_handle.listMatrices():
                    boards_df = self._make_dataframe_from_omx(
                        omx_handle[matrix_name], matrix_name
                    )
                    boards_df = boards_df[boards_df[matrix_name] > 0.0].copy()

                path_time_df = pd.merge(
                    ivt_df, boards_df, on=["origin", "destination"], how="left"
                )
                path_time_df["path"] = path
                path_time_df["time_period"] = time_period

                for tech in tech_list:
                    matrix_name = "IVT{}".format(tech)
                    if matrix_name in omx_handle.listMatrices():
                        df = self._make_dataframe_from_omx(
                            omx_handle[matrix_name], matrix_name
                        )
                        df = df[df[matrix_name] > 0.0].copy()
                        col_name = "{}".format(tech.lower())
                        df[col_name] = df[matrix_name]
                        df = df.drop(columns=[matrix_name]).copy()
                        path_time_df = pd.merge(
                            path_time_df, df, how="left", on=["origin", "destination"]
                        )

                if running_df is None:
                    running_df = path_time_df.copy()
                else:
                    running_df = pd.concat(
                        [running_df, path_time_df], axis="rows", ignore_index=True
                    ).reset_index(drop=True)

                omx_handle.close()

        self.simulated_transit_tech_in_vehicle_times_df = running_df.fillna(0).copy()

        return

    def _read_transit_demand(self):

        # TODO: placeholder demand for now
        path_list = [
            "WLK_TRN_WLK",
            "PNR_TRN_WLK",
            "WLK_TRN_PNR",
            "KNR_TRN_WLK",
            "WLK_TRN_KNR",
        ]
        dem_dir = os.path.join(self.scenario_dict["scenario"]["root_dir"], "demand")

        time_period = "am"
        filename = os.path.join(
            dem_dir, "trn_demand_v12_trim_{}.omx".format(time_period)
        )
        omx_handle = omx.open_file(filename)

        running_df = None
        for path in path_list:
            if path in omx_handle.listMatrices():
                df = self._make_dataframe_from_omx(omx_handle[path], path)
                df = df[df[path] > 0.0].copy()
                df = df.rename(columns={path: "simulated_flow"})
                df["path"] = path
                df["time_period"] = time_period

                if running_df is None:
                    running_df = df.copy()
                else:
                    running_df = pd.concat(
                        [running_df, df], axis="rows", ignore_index=True
                    ).reset_index(drop=True)

        omx_handle.close()

        self.simulated_transit_demand_df = running_df.fillna(0).copy()

        return

    def _make_dataframe_from_omx(self, input_mtx: omx, column_name: str):
        """_summary_

        Args:
            input_mtx (omx): _description_
            column_name (str): _description_
        """
        df = pd.DataFrame(input_mtx)
        df = df.add_prefix("dest_")
        df["id"] = df.index
        df = pd.wide_to_long(df, "dest_", "id", "destination")
        df = df.reset_index().rename(columns={"dest_": column_name, "id": "origin"})
        df["origin"] = df["origin"] + 1
        df["destination"] = df["destination"] + 1

        return df

    def _make_district_to_district_transit_summaries(self):

        # TODO: placeholder with link21 demand, so using link21 taz to district mapping for now
        link21_district_dict = self.c.taz_to_district_df.set_index("taz_link21")[
            "district_tm1"
        ].to_dict()

        s_dem_df = self.simulated_transit_demand_df.copy()
        s_path_df = self.simulated_transit_tech_in_vehicle_times_df.copy()

        s_dem_sum_df = (
            s_dem_df.groupby(["origin", "destination"])
            .agg({"simulated_flow": "sum"})
            .reset_index()
        )
        s_df = s_dem_sum_df.merge(
            s_path_df,
            left_on=["origin", "destination"],
            right_on=["origin", "destination"],
        )

        for tech in self.c.transit_technology_abbreviation_dict.keys():
            column_name = "simulated_{}_flow".format(tech.lower())
            s_df[column_name] = (
                s_df["simulated_flow"]
                * s_df["BOARDS"]
                * s_df["{}".format(tech.lower())]
                / s_df["IVT"]
            )

        s_df["orig_district"] = s_df["origin"].map(link21_district_dict)
        s_df["dest_district"] = s_df["destination"].map(link21_district_dict)

        agg_dict = {"simulated_flow": "sum"}
        rename_dict = {"simulated_flow": "total"}
        for tech in self.c.transit_technology_abbreviation_dict.keys():
            agg_dict["simulated_{}_flow".format(tech.lower())] = "sum"
            rename_dict["simulated_{}_flow".format(tech.lower())] = tech.lower()

        sum_s_df = (
            s_df.groupby(["orig_district", "dest_district"]).agg(agg_dict).reset_index()
        )

        long_sum_s_df = sum_s_df.melt(
            id_vars=["orig_district", "dest_district"],
            var_name="tech",
            value_name="simulated",
        )
        long_sum_s_df["tech"] = long_sum_s_df["tech"].map(rename_dict)

        self.simulated_transit_district_to_district_by_tech_df = long_sum_s_df.copy()

        return

    def _reduce_simulated_traffic_flow(self):

        file_root = self.scenario_dict["scenario"]["root_dir"]
        time_of_day_df = pd.DataFrame()
        time_periods = ["AM"]  # temporary

        # for time_period in self.model_time_periods:
        for time_period in time_periods:

            gdf = gpd.read_file(
                os.path.join(file_root + time_period + "/" + "emme_links.shp")
            )
            df = gdf[
                ["ID", "@flow_da", "@flow_lrgt", "@flow_s2", "@flow_s3", "@flow_trk"]
            ]
            df = df.rename(columns={"ID": "model_link_id"})
            df["time_period"] = time_period
            time_of_day_df = time_of_day_df.append(
                df
            )  # one file with selected vars for all the time periods

        time_of_day_df["simulated_flow_auto"] = time_of_day_df[
            ["@flow_da", "@flow_s2", "@flow_s3"]
        ].sum(
            axis=1
        )  # include @flow_lrgt?
        time_of_day_df = time_of_day_df.rename(
            columns={"@flow_trk": "simulated_flow_truck"}
        )
        time_of_day_df["simulated_flow"] = time_of_day_df[
            ["simulated_flow_auto", "simulated_flow_truck", "@flow_lrgt"]
        ].sum(axis=1)

        all_day_df = (
            time_of_day_df.groupby(
                ["model_link_id"]
            )  # summarize all timeperiod flow variables to daily
            .sum()
            .reset_index()
        )
        all_day_df["time_period"] = self.c.ALL_DAY_WORD

        # combine
        out_df = pd.concat([time_of_day_df, all_day_df], axis="rows", ignore_index=True)

        # remove unneeded columns
        out_df = out_df[
            [
                "model_link_id",
                "time_period",
                "simulated_flow_auto",
                "simulated_flow_truck",
                "simulated_flow",
            ]
        ]

        # out_df.to_csv(os.path.join(file_root, out_file))
        out_df.to_csv("simulated_traffic_flow_temp.csv")

        self.simulated_traffic_flow_df = out_df  # model_link_id, time_period (which includes each of the timeperiods and daily) and flow vars (including simulated_flow)

        return

    def _reduce_simulated_roadway_assignment_outcomes(self):

        file_root = self.scenario_dict["scenario"]["root_dir"]

        # step 1: get the shape
        shape_period = "am"
        gdf = gpd.read_file(
            os.path.join(file_root + shape_period + "/" + "emme_links.shp")
        )
        self.simulated_roadway_am_shape_gdf = (
            gdf[["INODE", "JNODE", "#link_id", "geometry"]]
            .copy()
            .rename(
                columns={
                    "INODE": "emme_a_node_id",
                    "JNODE": "emme_b_node_id",
                    "#link_id": "standard_link_id",
                }
            )
        )

        join_standard_id_df = pd.DataFrame(
            self.simulated_roadway_am_shape_gdf.drop(columns="geometry").copy()
        )

        # step 2: fetch the roadway volumes
        across_df = pd.DataFrame()
        for t in ["am"]: #self.model_time_periods:
            if t != shape_period:
                gdf = gpd.read_file(
                    os.path.join(file_root + t.upper() + "/" + "emme_links.shp")
                )

            df = pd.DataFrame(gdf)[
                [
                    "INODE",
                    "JNODE",
                    "LENGTH",
                    "TIMAU",
                    "@lanes",
                    "@useclass",
                    "@capacity",
                    "@managed",
                    "@tollbooth",
                    "@tollseg",
                    "@ft",
                    "@flow_da",
                    "@flow_s2",
                    "@flow_s3",
                    "@flow_lrgt",
                    "@flow_trk",
                    "@ffs"
                ]
            ]
            df = df.rename(
                columns={
                    "INODE": "emme_a_node_id",
                    "JNODE": "emme_b_node_id",
                    "LENGTH": "distance_in_miles",
                    "TIMAU": "time_in_minutes",
                    "@managed": "managed",
                    "@tollbooth": "tollbooth",
                    "@tollseg": "tollseg",
                    "@ft": "ft",
                    "@useclass": "useclass",
                    "@capacity": "capacity",
                    "@lanes": "lanes",
                    "@flow_da": "flow_da",
                    "@flow_s2": "flow_s2",
                    "@flow_s3": "flow_s3",
                    "@flow_lrgt": "flow_lrgt",
                    "@flow_trk": "flow_trk",
                }
            )

            df["time_period"] = t
            df["speed_mph"] = np.where(df["distance_in_miles"] > 0, df["distance_in_miles"]/(df["time_in_minutes"]/60.0), df["@ffs"]) 
            df["flow_total"] = df[
                [col for col in df.columns if col.startswith("flow_")]
            ].sum(axis=1)

            # join managed lane flows to general purpose
            managed_df = df[df["managed"] == 1].copy()
            managed_df = pd.merge(
                managed_df,
                join_standard_id_df,
                how="left",
                on=["emme_a_node_id", "emme_b_node_id"],
            )
            managed_df["join_link_id"] = (
                managed_df["standard_link_id"] - self.c.MANAGED_LANE_OFFSET
            )
            managed_df = managed_df[
                [
                    "join_link_id",
                    "flow_da",
                    "flow_s2",
                    "flow_s3",
                    "flow_lrgt",
                    "flow_trk",
                    "flow_total",
                    "time_period",
                    "lanes",
                    "capacity",
                    "speed_mph",
                ]
            ].copy()
            managed_df = managed_df.rename(
                columns={
                    "join_link_id": "standard_link_id",
                    "lanes": "m_lanes",
                    "flow_da": "m_flow_da",
                    "flow_s2": "m_flow_s2",
                    "flow_s3": "m_flow_s3",
                    "flow_lrgt": "m_flow_lrgt",
                    "flow_trk": "m_flow_trk",
                    "flow_total": "m_flow_total",
                    "speed_mph": "m_speed_mph",
                    "capacity": "m_capacity",
                }
            )

            df = pd.merge(
                df,
                join_standard_id_df,
                how="left",
                on=["emme_a_node_id", "emme_b_node_id"],
            )
            df = pd.merge(
                df, managed_df, how="left", on=["standard_link_id", "time_period"]
            )
            df.fillna(
                {
                    "m_flow_da": 0,
                    "m_flow_s2": 0,
                    "m_flow_s3": 0,
                    "m_flow_lrgt": 0,
                    "m_flow_trk": 0,
                    "m_flow_total": 0,
                    "m_lanes": 0,
                    "m_speed_mph": 0,
                    "m_capacity": 0,
                },
                inplace=True,
            )

            variable_list = [
                "flow_da",
                "flow_s2",
                "flow_s3",
                "flow_lrgt",
                "flow_trk",
                "flow_total",
                "lanes",
                "capacity",
            ]
            for variable in variable_list:
                m_variable = "m_" + variable
                df[variable] = df[variable] + df[m_variable]

            # can now drop managed lane entries
            df = df[df["managed"] == 0].copy()

            if len(across_df.index) == 0:
                across_df = df.copy()
            else:
                across_df = pd.concat([across_df, df], axis="rows")

        daily_df = (
            df.groupby(["emme_a_node_id", "emme_b_node_id"])
            .agg(
                {
                    "flow_da": "sum",
                    "flow_s2": "sum",
                    "flow_s3": "sum",
                    "flow_trk": "sum",
                    "flow_lrgt": "sum",
                    "flow_total": "sum",
                    "m_flow_da": "sum",
                    "m_flow_s2": "sum",
                    "m_flow_s3": "sum",
                    "m_flow_lrgt": "sum",
                    "m_flow_trk": "sum",
                    "m_flow_total": "sum",
                }
            )
            .reset_index(drop=False)
        )
        daily_df["time_period"] = self.c.ALL_DAY_WORD
        daily_df["flow_total"] = daily_df[
            [col for col in daily_df.columns if col.startswith("flow_")]
        ].sum(axis=1)

        self.simulated_roadway_assignment_results_df = pd.concat(
            [across_df, daily_df], axis="rows"
        ).reset_index(drop=True)

        return
