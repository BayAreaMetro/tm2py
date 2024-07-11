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

    taz_to_district_df: pd.DataFrame

    simulated_boardings_df: pd.DataFrame
    simulated_home_work_flows_df: pd.DataFrame
    simulated_maz_data_df: pd.DataFrame
    simulated_transit_segments_gdf: gpd.GeoDataFrame
    simulated_transit_access_df: pd.DataFrame
    simulated_zero_vehicle_hhs_df: pd.DataFrame
    reduced_simulated_zero_vehicle_hhs_df: pd.DataFrame
    simulated_station_to_station_df: pd.DataFrame
    simulated_transit_demand_df: pd.DataFrame
    simulated_transit_tech_in_vehicle_times_df: pd.DataFrame
    simulated_transit_district_to_district_by_tech_df: pd.DataFrame
    am_segment_simulated_boardings_df: pd.DataFrame

    HEAVY_RAIL_NETWORK_MODE_DESCR: str
    COMMUTER_RAIL_NETWORK_MODE_DESCR: str

    standard_transit_stops_df: pd.DataFrame
    standard_transit_shapes_df: pd.DataFrame
    standard_transit_routes_df: pd.DataFrame
    standard_nodes_gdf: gpd.GeoDataFrame
    standard_to_emme_transit_nodes_df: pd.DataFrame

    simulated_bridge_details_df = pd.DataFrame(
        [
            ["Bay Bridge", "WB", 3082623, True],
            ["Bay Bridge", "EB", 3055540, False],
            ["San Mateo Bridge", "WB", 1034262, True],
            ["San Mateo Bridge", "EB", 3164437, False],
            ["Dumbarton Bridge", "WB", 3098698, True],
            ["Dumbarton Bridge", "EB", 3133722, False],
            ["Richmond Bridge", "WB", 4062186, True],
            ["Richmond Bridge", "EB", 4074386, False],
            ["Carquinez Bridge", "WB", 5051527, False],
            ["Carquinez Bridge", "EB", 5051375, True],
            ["Benicia Bridge", "SB", 4074831, False],
            ["Benicia Bridge", "NB", 5004246, True],
            ["Antioch Bridge", "SB", 26067351, False],
            ["Antioch Bridge", "NB", 26067350, True],
        ],
        columns=["plaza_name", "direction", "standard_link_id", "pay_toll"],
    )

    network_shapefile_names_dict = {
        "ea": "Scenario_11",
        "am": "Scenario_12",
        "md": "Scenario_13",
        "pm": "Scenario_14",
        "ev": "Scenario_15",
    }

    def _load_configs(self, scenario: bool = True, model: bool = True):
        if scenario:
            with open(self.scenario_file, "r", encoding="utf-8") as toml_file:
                self.scenario_dict = toml.load(toml_file)

        if model:
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
        self,
        canonical: Canonical,
        scenario_file: str = None,
        model_file: str = None,
        on_board_assign_summary: bool = False,
    ) -> None:
        self.c = canonical
        self.scenario_file = scenario_file
        self._load_configs(scenario=True, model=False)

        if not on_board_assign_summary:
            self.model_file = model_file
            self._load_configs()
            self._get_model_time_periods()
            self._get_morning_commute_capacity_factor()
            self._validate()

    def reduce_on_board_assignment_boardings(self, time_period_list: list = ["am"]):
        self.model_time_periods = time_period_list
        self._reduce_simulated_transit_boardings()

        return

    def _validate(self):
        self._make_transit_mode_dict()
        self._make_simulated_maz_data()
        self._read_standard_transit_stops()
        self._read_standard_transit_shapes()
        self._read_standard_transit_routes()
        self._read_standard_node()
        self._read_transit_demand()
        self._make_transit_technology_in_vehicle_table_from_skims()
        self._make_district_to_district_transit_summaries()
        self._reduce_simulated_transit_by_segment()
        self._reduce_simulated_transit_boardings()
        self._reduce_simulated_transit_shapes()
        self._reduce_simulated_home_work_flows()
        self._reduce_simulated_zero_vehicle_households()
        self._reduce_simulated_station_to_station()
        self._reduce_simulated_rail_access_summaries()

        assert sorted(
            self.simulated_home_work_flows_df.residence_county.unique().tolist()
        ) == sorted(self.c.county_names_list)
        assert sorted(
            self.simulated_home_work_flows_df.work_county.unique().tolist()
        ) == sorted(self.c.county_names_list)
        self._reduce_simulated_roadway_assignment_outcomes()

        return

    def _add_model_link_id(self):
        df = pd.read_csv(
            os.path.join("acceptance", "crosswalks", "transit_link_id_mapping_am.csv")
        )

        df = df.rename(
            columns={
                "LINK_ID": "#link_id",
            }
        )

        return df

    def _reduce_simulated_transit_by_segment(self):
        file_prefix = "transit_segment_"

        time_period = "am"  # am only

        df = pd.read_csv(
            os.path.join("output_summaries", file_prefix + time_period + ".csv"),
            low_memory=False,
        )

        a_df = df[~(df["line"].str.contains("pnr_"))].reset_index().copy()

        # remove nodes from line name and add node fields
        a_df["line_long"] = df["line"].copy()
        temp = a_df["line_long"].str.split(pat="-", expand=True)
        a_df["LINE_ID"] = temp[0]
        a_df = a_df.rename(columns={"i_node": "INODE", "j_node": "JNODE"})
        a_df = a_df[~(a_df["JNODE"] == "None")].reset_index().copy()
        a_df["JNODE"] = a_df["JNODE"].astype("float").astype("Int64")
        df = a_df[["LINE_ID", "line", "INODE", "JNODE", "board"]]

        self.am_segment_simulated_boardings_df = df

    def _get_operator_name_from_line_name(
        self, input_df: pd.DataFrame, input_column_name: str, output_column_name: str
    ) -> pd.DataFrame:
        df = input_df[input_column_name].str.split(pat="_", expand=True).copy()
        df[output_column_name] = df[1]
        return_df = pd.concat([input_df, df[output_column_name]], axis="columns")

        return return_df

    def _reduce_simulated_transit_shapes(self):
        file_prefix = "boardings_by_segment_"
        time_period = "am"  # am only
        gdf = gpd.read_file(
            os.path.join("output_summaries", file_prefix + time_period + ".geojson")
        )

        gdf["first_row_in_line"] = gdf.groupby("LINE_ID").cumcount() == 0

        df = pd.DataFrame(gdf.drop(columns=["geometry"]))

        # Add am boards
        a_df = self.am_segment_simulated_boardings_df

        df = pd.merge(df, a_df, how="left", on=["LINE_ID", "INODE", "JNODE"])

        # Compute v/c ratio, excluding pnr dummy routes
        a_df = df[~(df["LINE_ID"].str.contains("pnr_"))].reset_index().copy()
        a_df["am_segment_capacity_total"] = (
            a_df["capt"] * self.model_morning_capacity_factor
        )
        a_df["am_segment_capacity_seated"] = (
            a_df["caps"] * self.model_morning_capacity_factor
        )
        a_df["am_segment_vc_ratio_total"] = (
            a_df["VOLTR"] / a_df["am_segment_capacity_total"]
        )
        a_df["am_segment_vc_ratio_seated"] = (
            a_df["VOLTR"] / a_df["am_segment_capacity_seated"]
        )
        a_df = a_df.rename(columns={"VOLTR": "am_segment_volume"})

        sum_df = (
            a_df.groupby(["LINE_ID"])
            .agg({"am_segment_vc_ratio_total": "mean"})
            .reset_index()
        )
        sum_df.columns = [
            "LINE_ID",
            "mean_am_segment_vc_ratio_total",
        ]

        a_gdf = pd.merge(
            gdf,
            sum_df,
            on="LINE_ID",
            how="left",
        )

        crosswalk_df = self._add_model_link_id()

        b_gdf = pd.merge(
            a_gdf,
            crosswalk_df,
            on=["INODE", "JNODE", "LINE_ID"],
            how="left",
        )

        self.simulated_transit_segments_gdf = pd.merge(
            b_gdf,
            a_df[
                [
                    "LINE_ID",
                    "INODE",
                    "JNODE",
                    "am_segment_volume",
                    "am_segment_capacity_total",
                    "am_segment_capacity_seated",
                    "am_segment_vc_ratio_total",
                    "am_segment_vc_ratio_seated",
                    "board",
                ]
            ],
            on=["LINE_ID", "INODE", "JNODE"],
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
        in_file = os.path.join("inputs", "trn", "standard", "v12_node.geojson")
        gdf = gpd.read_file(in_file, driver="GEOJSON")

        self.standard_nodes_gdf = gdf

        return

    def _read_standard_transit_stops(self):
        in_file = os.path.join("inputs", "trn", "standard", "v12_stops.txt")

        df = pd.read_csv(in_file)

        self.standard_transit_stops_df = df.copy()

        return

    def _read_standard_transit_shapes(self):
        in_file = os.path.join("inputs", "trn", "standard", "v12_shapes.txt")

        df = pd.read_csv(in_file)

        self.standard_transit_shapes_df = df

        return

    def _read_standard_transit_routes(self):
        in_file = os.path.join("inputs", "trn", "standard", "v12_routes.txt")

        df = pd.read_csv(in_file)

        self.standard_transit_routes_df = df

        return

    def _reduce_simulated_home_work_flows(self):
        # if self.simulated_maz_data_df.empty:
        #    self._make_simulated_maz_data()

        in_file = os.path.join("ctramp_output", "wsLocResults_1.csv")

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
        in_file = self.scenario_dict["scenario"]["maz_landuse_file"]

        df = pd.read_csv(os.path.join(root_dir, in_file))

        index_file = os.path.join("inputs", "landuse", "mtc_final_network_zone_seq.csv")

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

        self._make_taz_district_crosswalk()

        return

    def _make_taz_district_crosswalk(self):
        df = self.simulated_maz_data_df[["TAZ_ORIGINAL", "DistID"]].copy()
        df = df.rename(columns={"TAZ_ORIGINAL": "taz", "DistID": "district"})
        self.taz_to_district_df = df.drop_duplicates().reset_index(drop=True)

        return

    def _reduce_simulated_rail_access_summaries(self):
        if not self.transit_mode_dict:
            self._make_transit_mode_dict()

        file_prefix = "transit_segment_"

        out_df = pd.DataFrame()

        for time_period in self.model_time_periods:
            df = pd.read_csv(
                os.path.join("output_summaries", file_prefix + time_period + ".csv"),
                dtype={"stop_name": str, "mdesc": str},
                low_memory=False,
            )

            rail_df = df[df["mdesc"].isin(["HVY", "COM"])].copy()

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
            access_df["board_ptw"] = (
                access_df["initial_board_ptw"] + access_df["direct_transfer_board_ptw"]
            )
            access_df["board_wtp"] = (
                access_df["initial_board_wtp"] + access_df["direct_transfer_board_wtp"]
            )
            access_df["board_ktw"] = (
                access_df["initial_board_ktw"] + access_df["direct_transfer_board_ktw"]
            )
            access_df["board_wtk"] = (
                access_df["initial_board_wtk"] + access_df["direct_transfer_board_wtk"]
            )
            access_df["board_wtw"] = (
                access_df["initial_board_wtw"] + access_df["direct_transfer_board_wtw"]
            )
            access_df = access_df[
                [
                    "i_node",
                    "board_ptw",
                    "board_wtp",
                    "board_ktw",
                    "board_wtk",
                    "board_wtw",
                ]
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
            long_df["access_mode"] = long_df["mode"].map(access_dict)  # check

            join_df = pd.merge(
                long_df,
                names_df,
                how="left",
                left_on=["i_node"],
                right_on=["boarding"],
            ).reset_index(drop=True)

            running_df = (
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
            running_df = running_df.rename(
                columns={"boardings": "simulated_boardings", "i_node": "boarding"}
            )

            running_df["time_period"] = time_period

            out_df = pd.concat([out_df, running_df], axis="rows", ignore_index=True)

        self.simulated_transit_access_df = out_df

        return

    def _reduce_simulated_station_to_station(self):
        # if self.model_time_periods is None:
        #     self._get_model_time_periods()

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

        for operator, time_period, path in itertools.product(
            operator_list, self.model_time_periods, path_list
        ):
            input_file_name = os.path.join(
                "output_summaries",
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

                    df = pd.concat([df, subject_df], axis="rows", ignore_index=True)

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
        i_df = input_df["line_name"].str.split(pat="_", expand=True).copy()
        i_df["tm2_operator"] = i_df[0]
        i_df["tm2_operator"] = (
            pd.to_numeric(i_df["tm2_operator"], errors="coerce")
            .fillna(0)
            .astype(np.int64)
        )
        j_df = pd.concat([input_df, i_df["tm2_operator"]], axis="columns")

        return_df = pd.merge(
            j_df,
            df,
            how="left",
            on=["tm2_operator", "tm2_mode"],
        )

        return return_df

    def _reduce_simulated_transit_boardings(self):
        file_prefix = "boardings_by_line_"

        c_df = pd.DataFrame()
        for time_period in self.model_time_periods:
            df = pd.read_csv(
                os.path.join("output_summaries", file_prefix + time_period + ".csv")
            )
            df["time_period"] = time_period
            c_df = pd.concat([c_df, df], axis="rows", ignore_index=True)

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
        in_file = os.path.join("ctramp_output", "householdData_1.csv")

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

        # prepare simulated data
        a_df = (
            pd.merge(
                self.simulated_zero_vehicle_hhs_df,
                self.simulated_maz_data_df[["MAZ_ORIGINAL", "MAZSEQ"]],
                left_on="maz",
                right_on="MAZSEQ",
                how="left",
            )
            .drop(columns=["maz", "MAZSEQ"])
            .rename(columns={"MAZ_ORIGINAL": "maz"})
        )

        a_df = pd.merge(
            a_df,
            self.c.census_2010_to_maz_crosswalk_df,
            how="left",
            on="maz",
        )

        a_df["product"] = a_df["simulated_zero_vehicle_share"] * a_df["maz_share"]

        b_df = (
            a_df.groupby("blockgroup")
            .agg({"product": "sum", "simulated_households": "sum"})
            .reset_index()
            .rename(columns={"product": "simulated_zero_vehicle_share"})
        )
        b_df["tract"] = b_df["blockgroup"].astype("str").str.slice(stop=-1)
        b_df["product"] = (
            b_df["simulated_zero_vehicle_share"] * b_df["simulated_households"]
        )

        c_df = (
            b_df.groupby("tract")
            .agg({"product": "sum", "simulated_households": "sum"})
            .reset_index()
        )
        c_df["simulated_zero_vehicle_share"] = (
            c_df["product"] / c_df["simulated_households"]
        )

        self.reduced_simulated_zero_vehicle_hhs_df = c_df

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
            input_df (pd.DataFrame): A pandas datting" columns are present, then name and coordinate columns are added for both the boarding and alighting station.aframe with columns "boarding" and/or "alighting" that contain the node numbers of rail stations from the simulation.

        Returns:
            pd.DataFrame: The input_df with the canonical station name and lat/lon coordinates added as columns. If "boarding" and "aligh
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

    def _make_transit_mode_dict(self):  # check
        transit_mode_dict = {}
        for lil_dict in self.model_dict["transit"]["modes"]:
            add_dict = {lil_dict["mode_id"]: lil_dict["name"]}
            transit_mode_dict.update(add_dict)

        # TODO: this will be in model_config in TM2.2
        transit_mode_dict.update({"p": "pnr"})

        # self.HEAVY_RAIL_NETWORK_MODE_DESCR = transit_mode_dict["h"]
        # self.COMMUTER_RAIL_NETWORK_MODE_DESCR = transit_mode_dict["r"]

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

        tech_list = self.c.transit_technology_abbreviation_dict.keys()

        skim_dir = os.path.join("skim_matrices", "transit")

        running_df = None
        for path, time_period in itertools.product(path_list, self.model_time_periods):
            filename = os.path.join(
                skim_dir, "trnskm{}_{}.omx".format(time_period.upper(), path)
            )
            if os.path.exists(filename):
                omx_handle = omx.open_file(filename)

                # IVT
                TIME_PERIOD = time_period.upper()
                matrix_name = TIME_PERIOD + "_" + path + "_IVT"
                if matrix_name in omx_handle.listMatrices():
                    ivt_df = self._make_dataframe_from_omx(
                        omx_handle[matrix_name], matrix_name
                    )
                    ivt_df = ivt_df[ivt_df[matrix_name] > 0.0].copy()
                    ivt_df.rename(columns={ivt_df.columns[2]: "ivt"}, inplace=True)

                # Transfers to get boardings from trips

                matrix_name = TIME_PERIOD + "_" + path + "_BOARDS"
                if matrix_name in omx_handle.listMatrices():
                    boards_df = self._make_dataframe_from_omx(
                        omx_handle[matrix_name], matrix_name
                    )
                    boards_df = boards_df[boards_df[matrix_name] > 0.0].copy()
                    boards_df.rename(
                        columns={boards_df.columns[2]: "boards"}, inplace=True
                    )

                path_time_df = pd.merge(
                    ivt_df, boards_df, on=["origin", "destination"], how="left"
                )
                path_time_df["path"] = path
                path_time_df["time_period"] = time_period

                for tech in tech_list:
                    matrix_name = TIME_PERIOD + "_" + path + "_IVT" + tech
                    if matrix_name in omx_handle.listMatrices():
                        df = self._make_dataframe_from_omx(
                            omx_handle[matrix_name], matrix_name
                        )
                        df = df[df[matrix_name] > 0.0].copy()
                        col_name = "{}".format(tech.lower())
                        df[col_name] = df[matrix_name]
                        df = df.drop(columns=[matrix_name]).copy()
                        path_time_df = pd.merge(
                            path_time_df,
                            df,
                            how="left",
                            on=["origin", "destination"],
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
        path_list = [
            "WLK_TRN_WLK",
            "PNR_TRN_WLK",
            "WLK_TRN_PNR",
            "KNR_TRN_WLK",
            "WLK_TRN_KNR",
        ]
        dem_dir = os.path.join("demand_matrices", "transit")

        out_df = pd.DataFrame()
        for time_period in self.model_time_periods:
            filename = os.path.join(dem_dir, "trn_demand_{}.omx".format(time_period))
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

            out_df = pd.concat([out_df, running_df], axis="rows", ignore_index=True)

            omx_handle.close()

        self.simulated_transit_demand_df = out_df.fillna(0).copy()

        return

    def _make_dataframe_from_omx(self, input_mtx: omx, core_name: str):
        """_summary_

        Args:
            input_mtx (omx): _description_
            core_name (str): _description_
        """
        a = np.array(input_mtx)

        df = pd.DataFrame(a)
        df = (
            df.unstack()
            .reset_index()
            .rename(
                columns={"level_0": "origin", "level_1": "destination", 0: core_name}
            )
        )
        df["origin"] = df["origin"] + 1
        df["destination"] = df["destination"] + 1

        return df

    def _make_district_to_district_transit_summaries(self):
        taz_district_dict = self.taz_to_district_df.set_index("taz")[
            "district"
        ].to_dict()

        s_dem_df = self.simulated_transit_demand_df.copy()
        s_path_df = self.simulated_transit_tech_in_vehicle_times_df.copy()

        s_dem_sum_df = (
            s_dem_df.groupby(["origin", "destination", "time_period"])
            .agg({"simulated_flow": "sum"})
            .reset_index()
        )
        s_df = s_dem_sum_df.merge(
            s_path_df,
            left_on=["origin", "destination", "time_period"],
            right_on=["origin", "destination", "time_period"],
        )

        s_df = s_df[s_df["time_period"] == "am"].copy()

        for tech in self.c.transit_technology_abbreviation_dict.keys():
            column_name = "simulated_{}_flow".format(tech.lower())
            s_df[column_name] = (
                s_df["simulated_flow"]
                * s_df["boards"]
                * s_df["{}".format(tech.lower())]
                / s_df["ivt"]
            )

        s_df["orig_district"] = s_df["origin"].map(taz_district_dict)
        s_df["dest_district"] = s_df["destination"].map(taz_district_dict)

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
        time_of_day_df = pd.DataFrame()

        for time_period in self.model_time_periods:
            emme_scenario = self.network_shapefile_names_dict[time_period]
            gdf = gpd.read_file(
                os.path.join("output_summaries", emme_scenario, "emme_links.shp")
            )
            df = gdf[
                [
                    "ID",
                    "@flow_da",
                    "@flow_lrgt",
                    "@flow_sr2",
                    "@flow_sr3",
                    "@flow_trk",
                    "@flow_dato",
                    "@flow_lrg0",
                    "@flow_sr2t",
                    "@flow_sr3t",
                    "@flow_trkt",
                ]
            ]
            df = df.rename(columns={"ID": "model_link_id"})
            df["time_period"] = time_period
            time_of_day_df = pd.concat(
                [time_of_day_df, df], axis="rows", ignore_index="True"
            )

            time_of_day_df["simulated_flow_auto"] = time_of_day_df[
                [
                    "@flow_da",
                    "@flow_sr2",
                    "@flow_sr3",
                    "@flow_dato",
                    "@flow_sr2t",
                    "@flow_sr3t",
                ]
            ].sum(axis=1)

            time_of_day_df["simulated_flow_truck"] = time_of_day_df[
                ["@flow_trk", "@flow_trkt"]
            ].sum(axis=1)
            time_of_day_df["simulated_flow"] = time_of_day_df[
                [
                    "simulated_flow_auto",
                    "simulated_flow_truck",
                    "@flow_lrgt",
                    "@flow_lrg0",
                ]
            ].sum(axis=1)

            all_day_df = time_of_day_df.groupby(["model_link_id"]).sum().reset_index()
            all_day_df["time_period"] = self.c.ALL_DAY_WORD

            out_df = pd.concat(
                [time_of_day_df, all_day_df], axis="rows", ignore_index=True
            )

        out_df = out_df[
            [
                "model_link_id",
                "time_period",
                "simulated_flow_auto",
                "simulated_flow_truck",
                "simulated_flow",
            ]
        ]

        self.simulated_traffic_flow_df = out_df

        return

    def _reduce_simulated_roadway_assignment_outcomes(self):
        # step 1: get the shape
        shape_period = "am"
        emme_scenario = self.network_shapefile_names_dict[shape_period]
        shape_gdf = gpd.read_file(
            os.path.join("output_summaries", emme_scenario, "emme_links.shp")
        )
        self.simulated_roadway_am_shape_gdf = (
            shape_gdf[["INODE", "JNODE", "#link_id", "geometry"]]
            .copy()
            .rename(
                columns={
                    "INODE": "emme_a_node_id",
                    "JNODE": "emme_b_node_id",
                    "#link_id": "standard_link_id",
                }
            )
        )

        # step 2: fetch the roadway volumes
        across_df = pd.DataFrame()
        for t in self.model_time_periods:
            if t == shape_period:
                gdf = shape_gdf
            else:
                emme_scenario = self.network_shapefile_names_dict[t]
                gdf = gpd.read_file(
                    os.path.join("output_summaries", emme_scenario, "emme_links.shp")
                )

            df = pd.DataFrame(gdf)[
                [
                    "INODE",
                    "JNODE",
                    "#link_id",
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
                    "@flow_sr2",
                    "@flow_sr3",
                    "@flow_lrgt",
                    "@flow_trk",
                    "@free_flow",
                    "@flow_dato",
                    "@flow_sr2t",
                    "@flow_sr3t",
                    "@flow_lrg0",
                    "@flow_trkt",
                ]
            ]
            df = df.rename(
                columns={
                    "INODE": "emme_a_node_id",
                    "JNODE": "emme_b_node_id",
                    "#link_id": "standard_link_id",
                    "LENGTH": "distance_in_miles",
                    "TIMAU": "time_in_minutes",
                    "@managed": "managed",
                    "@tollbooth": "tollbooth",
                    "@tollseg": "tollseg",
                    "@ft": "ft",
                    "@useclass": "useclass",
                    "@capacity": "capacity",
                    "@lanes": "lanes",
                }
            )

            df["flow_da"] = df["@flow_da"] + df["@flow_dato"]
            df["flow_s2"] = df["@flow_sr2"] + df["@flow_sr2t"]
            df["flow_s3"] = df["@flow_sr3"] + df["@flow_sr3t"]
            df["flow_lrgt"] = df["@flow_lrgt"] + df["@flow_lrg0"]
            df["flow_trk"] = df["@flow_trk"] + df["@flow_trkt"]

            df["time_period"] = t
            df["speed_mph"] = np.where(
                df["distance_in_miles"] > 0,
                df["distance_in_miles"] / (df["time_in_minutes"] / 60.0),
                df["@free_flow"],
            )
            df["flow_total"] = df[
                [col for col in df.columns if col.startswith("flow_")]
            ].sum(axis=1)

            # join managed lane flows to general purpose
            managed_df = df[df["managed"] == 1].copy()
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
            across_df.groupby(["emme_a_node_id", "emme_b_node_id", "standard_link_id"])
            .agg(
                {
                    "ft": "median",
                    "distance_in_miles": "median",
                    "lanes": "median",
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

        return_df = pd.concat([across_df, daily_df], axis="rows").reset_index(drop=True)

        self.simulated_roadway_assignment_results_df = return_df

        return
