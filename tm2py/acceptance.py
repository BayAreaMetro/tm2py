"""Methods to create Acceptance Criteria summaries from a tm2py model run."""

import numpy as np
import os
import geopandas as gpd
import pandas as pd
import toml


class Acceptance:

    scenario_dict: dict
    model_dict: dict
    observed_dict: dict

    scenario_file: str
    model_file: str
    observed_file: str

    output_transit_filename = "acceptance-transit-network.geojson"

    model_time_periods = []

    simulated_boardings_df: pd.DataFrame

    canonical_agency_names_dict = {}

    simulated_transit_segments_gdf: gpd.GeoDataFrame

    florida_transit_guidelines_df = pd.DataFrame(
        [
            [0, 1.50],
            [1000, 1.00],
            [2000, 0.65],
            [5000, 0.35],
            [10000, 0.25],
            [20000, 0.20],
        ],
        columns=["boardings", "threshold"],
    )

    # Reduced data specs
    reduced_transit_on_board_df = pd.DataFrame(
        {
            "survey_tech": pd.Series(dtype="str"),
            "survey_operator": pd.Series(dtype="str"),
            "survey_route": pd.Series(dtype="str"),
            "time_period": pd.Series(dtype="str"),
            "survey_boardings": pd.Series(dtype="float"),
        }
    )

    tableau_projection = "4326"

    # Output data specs
    road_network_gdf = gpd.GeoDataFrame(
        {
            "model_link_id": pd.Series(dtype="int"),
            "pems_station_id": pd.Series(dtype="int"),
            "time_period": pd.Series(dtype="str"),
            "observed_flow": pd.Series(dtype="float"),
            "simulated_flow": pd.Series(dtype="float"),
            "odot_flow_category": pd.Series(dtype="int"),
            "odot_maximum_error": pd.Series(dtype="float"),
            "line_string": pd.Series(dtype="str"),
        }
    )

    transit_network_gdf = gpd.GeoDataFrame(
        {
            "model_link_id": pd.Series(dtype="int"),
            "operator": pd.Series(dtype="str"),
            "route_short_name": pd.Series(dtype="str"),
            "trip_headsign": pd.Series(dtype="str"),
            "line_name": pd.Series(dtype="str"),
            "line_description": pd.Series(dtype="str"),
            "time_period": pd.Series(dtype="str"),
            "observed_boardings": pd.Series(dtype="float"),
            "simulated_boardings": pd.Series(dtype="float"),
            "florida_threshold": pd.Series(dtype="float"),
            "line_string": pd.Series(dtype="str"),
        }
    )

    compare_gdf = gpd.GeoDataFrame(
        {
            "criteria_number": pd.Series(dtype="int"),
            "criteria_name": pd.Series(dtype="str"),
            "dimension_01_name": pd.Series(dtype="str"),
            "dimension_01_value": pd.Series(dtype="float"),
            "dimension_02_name": pd.Series(dtype="str"),
            "dimension_02_value": pd.Series(dtype="float"),
            "dimension_03_name": pd.Series(dtype="str"),
            "dimension_03_value": pd.Series(dtype="float"),
            "observed_outcome": pd.Series(dtype="float"),
            "simulated_outcome": pd.Series(dtype="float"),
            "acceptance_threshold": pd.Series(dtype="float"),
            "point_string": pd.Series(dtype="str"),
        }
    )

    def _load_configs(self):

        with open(self.scenario_file, "r", encoding="utf-8") as toml_file:
            self.scenario_dict = toml.load(toml_file)

        with open(self.model_file, "r", encoding="utf-8") as toml_file:
            self.model_dict = toml.load(toml_file)

        with open(self.observed_file, "r", encoding="utf-8") as toml_file:
            self.observed_dict = toml.load(toml_file)

        return

    def _get_model_time_periods(self):
        if not self.model_time_periods:
            for time_dict in self.model_dict["time_periods"]:
                self.model_time_periods.append(time_dict["name"])

        return

    def __init__(self, scenario_file: str, model_file: str, observed_file: str) -> None:
        self.scenario_file = scenario_file
        self.model_file = model_file
        self.observed_file = observed_file
        self._load_configs()
        self._get_model_time_periods()

    def make_acceptance(self):

        self._validate()
        self._make_transit_network_comparisons()
        # _make_roadway_network_comparisons() method to build roadway network comparisons
        # _make_transit_network_comparisons()
        # _make_other_comparisons()

        return

    def _validate(self):

        self._validate_observed()
        self._validate_scenario()

        return

    def _validate_scenario(self):

        self._reduce_simulated_transit_boardings()
        self._reduce_simulated_transit_shapes()

        # is the model run complete?
        # are the key files present?

        return

    def _validate_observed(self):

        if not self.canonical_agency_names_dict:
            self._make_canonical_agency_names_dict()

        # transit on-board survey checks
        if self.reduced_transit_on_board_df.empty:
            self.reduce_on_board_survey()

        assert (
            self.model_time_periods.sort()
            is self.reduced_transit_on_board_df["time_period"]
            .value_counts()
            .tolist()
            .sort()
        )

        return

    def _write_roadway_network():
        """_summary_
        Method to set the geometry of the geopandas object and write the file to disk.
        """

        # set the geometry
        # make sure it is in the right projection
        # get the file location from the dictionary
        # write to disk

        return

    def _write_transit_network(self):

        file_root = self.observed_dict["remote_io"]["acceptance_output_folder_root"]
        # TODO: figure out how to get remote write, use . for now
        file_root = "."
        out_file = os.path.join(file_root, self.output_transit_filename)
        self.transit_network_gdf.to_file(out_file, driver="GeoJSON")

        return

    def _write_other_comparisons():

        # set the geometry
        # make sure it is in the right projection
        # get the file location from the dictionary
        # write to disk

        return

    def _make_roadway_network_comparisons():

        # placeholder
        # _write_roadway_network()

        return

    def _make_transit_network_comparisons(self):

        df = pd.merge(
            self.simulated_boardings_df,
            self.reduced_transit_on_board_df,
            how="left",
            left_on=["line_name", "time_period"],
            right_on=["standard_line_name", "time_period"],
        )

        g_df = pd.merge(
            df,
            self.simulated_transit_segments_gdf,
            how="left",
            left_on="line_name",
            right_on="line",
        )

        g_df = g_df.rename(
            columns={
                "#link_id": "model_link_id",
                "standard_route_short_name": "route_short_name",
                "standard_route_long_name": "route_long_name",
                "standard_headsign": "trip_headsign",
                "description": "line_description",
                "survey_boardings": "route_observed_boardings",
                "total_boarding": "route_simulated_boardings",
                "board": "segment_simulated_boardings",
            }
        )

        # only attach boardings to first link
        g_df["route_simulated_boardings"] = np.where(
            g_df["first_row_in_line"], g_df["route_simulated_boardings"], 0
        )
        g_df["route_observed_boardings"] = np.where(
            g_df["first_row_in_line"], g_df["route_observed_boardings"], 0
        )

        g_df = g_df[
            [
                "model_link_id",
                "operator",
                "route_short_name",
                "route_long_name",
                "trip_headsign",
                "line_name",
                "line_description",
                "time_period",
                "route_observed_boardings",
                "route_simulated_boardings",
                "segment_simulated_boardings",
                "florida_threshold",
                "geometry",
            ]
        ]

        self.transit_network_gdf = gpd.GeoDataFrame(
            g_df, crs="EPSG:" + self.tableau_projection, geometry="geometry"
        )

        self._write_transit_network()

        return

    def _make_other_comparisons():

        # separate method for each one

        return

    def _join_standard_route_id(self, input_df):

        file_root = self.observed_dict["remote_io"]["crosswalk_folder_root"]
        in_file = self.observed_dict["crosswalks"]["crosswalk_standard_survey_file"]

        df = pd.read_csv(os.path.join(file_root, in_file))

        df = df[
            [
                "survey_agency",
                "survey_route",
                "standard_route_id",
                "standard_line_name",
                "standard_headsign",
                "canonical_operator",
                "standard_route_short_name",
                "standard_route_long_name",
            ]
        ]

        return_df = pd.merge(
            input_df,
            df,
            how="outer",
            left_on=["survey_operator", "survey_route"],
            right_on=["survey_agency", "survey_route"],
        )

        return return_df

    def _join_florida_thresholds(self, input_df):

        df = self.florida_transit_guidelines_df.copy()
        df["high"] = df["boardings"].shift(-1)
        df["low"] = df["boardings"]
        df = df.drop(["boardings"], axis="columns")
        df = df.rename(columns={"threshold": "florida_threshold"})

        vals = input_df.survey_boardings.values
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

        return return_df

    def reduce_on_board_survey(self, read_file_from_disk=True):
        """Reduces the on-board survey, summarizing boardings by technology, operator, route, and time of day.
        Result is stored in the reduced_transit_on_board_df DataFrame and written to disk in the `reduced_summaries_file`
        in the observed configuration.
        """

        if not self.canonical_agency_names_dict:
            self._make_canonical_agency_names_dict()

        file_root = self.observed_dict["remote_io"]["obs_folder_root"]
        in_file = self.observed_dict["transit"]["on_board_survey_file"]
        out_file = self.observed_dict["transit"]["reduced_summaries_file"]

        if os.path.isfile(out_file) and read_file_from_disk:
            self.reduced_transit_on_board_df = pd.read_csv(
                os.path.join(file_root, out_file),
                dtype=self.reduced_transit_on_board_df.dtypes.to_dict(),
            )
        else:
            if not self.canonical_agency_names_dict:
                self._make_cononical_agency_names_dict()

            time_period_dict = {
                "EARLY AM": "ea",
                "AM PEAK": "am",
                "MIDDAY": "md",
                "PM PEAK": "pm",
                "EVENING": "ev",
                "NIGHT": "ev",
            }
            in_df = pd.read_feather(os.path.join(file_root, in_file))
            out_df = in_df[(in_df["weekpart"].isna()) | (in_df["weekpart"]!="WEEKEND")].copy()
            out_df["time_period"] = out_df["day_part"].map(time_period_dict)
            rail_operators_vector = ["BART", "Caltrain", "ACE"]
            out_df["route"] = np.where(
                out_df["operator"].isin(rail_operators_vector),
                out_df["operator"],
                out_df["route"],
            )
            out_df = (
                out_df.groupby(["survey_tech", "operator", "route", "time_period"])[
                    "boarding_weight"
                ]
                .sum()
                .reset_index()
            )

            out_df = out_df.rename(
                columns={
                    "operator": "survey_operator",
                    "route": "survey_route",
                    "boarding_weight": "survey_boardings",
                }
            )
            out_df = self._join_florida_thresholds(out_df)
            out_df = self._join_standard_route_id(out_df)
            out_df = self._fix_agency_names(out_df, "survey_operator")
            out_df.to_csv(os.path.join(file_root, out_file))
            self.reduced_transit_on_board_df = out_df

        return

    def _reduce_simulated_transit_boardings(self):

        file_prefix = "boardings_by_line_"
        file_root = self.scenario_dict["scenario"]["root_dir"]

        return_df = pd.DataFrame()
        for time_period in self.model_time_periods:

            df = pd.read_csv(
                os.path.join(file_root, file_prefix + time_period + ".csv")
            )
            df["time_period"] = time_period
            return_df = return_df.append(df)

        return_df = self._join_tm2_mode_codes(return_df)

        self.simulated_boardings_df = return_df

        return

    def _reduce_simulated_transit_shapes(self):

        file_prefix = "boardings_by_segment_"
        file_root = self.scenario_dict["scenario"]["root_dir"]

        # AM for now, just to get the shapes
        # TODO: problem with remote read in, use . for now
        # TODO: pickle this for faster i/o and check for it on disk?
        file_root = "."
        time_period = "am"
        gdf = gpd.read_file(
            os.path.join(file_root, file_prefix + time_period + ".geojson")
        )

        gdf["first_row_in_line"] = gdf.groupby("line").cumcount() == 0

        self.simulated_transit_segments_gdf = gdf

        return

    def _join_tm2_mode_codes(self, input_df):

        file_root = self.observed_dict["remote_io"]["crosswalk_folder_root"]
        in_file = self.observed_dict["crosswalks"]["standard_to_tm2_modes_file"]

        df = pd.read_csv(os.path.join(file_root, in_file))

        df = df[["TM2_mode", "agency_name", "TM2_line_haul_name"]]
        df = df.rename(
            columns={
                "TM2_mode": "tm2_mode",
                "agency_name": "operator",
                "TM2_line_haul_name": "technology",
            }
        )

        return_df = pd.merge(
            input_df,
            df,
            how="left",
            on="tm2_mode",
        )

        return return_df

    def _make_canonical_agency_names_dict(self):

        file_root = self.observed_dict["remote_io"]["crosswalk_folder_root"]
        in_file = self.observed_dict["crosswalks"]["canonical_agency_names_file"]

        df = pd.read_csv(os.path.join(file_root, in_file))

        a_df = df[["canonical_name"]].copy()
        a_df["temp"] = df["canonical_name"]
        a_dict = a_df.set_index("temp").to_dict()["canonical_name"]

        b_dict = df[(df["alternate_01"].notna())][["canonical_name", "alternate_01"]].set_index("alternate_01").to_dict()["canonical_name"]
        c_dict = df[(df["alternate_02"].notna())][["canonical_name", "alternate_02"]].set_index("alternate_02").to_dict()["canonical_name"]

        self.canonical_agency_names_dict = {**a_dict, **b_dict, **c_dict}

        return

    def _fix_agency_names(self, input_df, column_name):

        assert column_name in input_df.columns

        return_df = input_df.copy()

        return_df[column_name] = return_df[column_name].map(
            self.canonical_agency_names_dict
        )

        return return_df

