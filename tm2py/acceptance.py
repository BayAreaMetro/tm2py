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

    output_other_filename = "acceptance-other.csv"

    model_time_periods = []

    simulated_boardings_df: pd.DataFrame

    ctpp_2012_2016_df: pd.DataFrame

    simulated_home_work_flows_df: pd.DataFrame

    canonical_agency_names_dict = {}

    simulated_transit_segments_gdf: gpd.GeoDataFrame

    simulated_maz_data_df: pd.DataFrame

    rail_operators_vector = [
        "BART",
        "Caltrain",
        "ACE",
        "Sonoma-Marin Area Rail Transit",
    ]

    county_names_list = [
        "San Francisco",
        "San Mateo",
        "Santa Clara",
        "Alameda",
        "Contra Costa",
        "Solano",
        "Napa",
        "Sonoma",
        "Marin",
    ]

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

    # Reduced data specs
    reduced_transit_on_board_df = pd.DataFrame(
        {
            "survey_tech": pd.Series(dtype="str"),
            "survey_operator": pd.Series(dtype="str"),
            "survey_route": pd.Series(dtype="str"),
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
            "route_long_name": pd.Series(dtype="str"),
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

    def make_acceptance(self, make_transit=True, make_roadway=True, make_other=False):

        self._validate()
        if make_roadway:
            # self._make_roadway_acceptance()
            pass
        if make_transit:
            self._make_transit_network_comparisons()
        if make_other:
            self._make_other_comparisons()

        return

    def _validate(self):

        self._validate_observed()
        self._validate_scenario()

        return

    def _validate_scenario(self):

        self._reduce_simulated_transit_boardings()
        self._reduce_simulated_transit_shapes()
        self._make_simulated_maz_data()
        self._reduce_simulated_home_work_flows()

        assert sorted(
            self.simulated_home_work_flows_df.residence_county.unique().tolist()
        ) == sorted(self.county_names_list)
        assert sorted(
            self.simulated_home_work_flows_df.work_county.unique().tolist()
        ) == sorted(self.county_names_list)

        return

    def _validate_observed(self):

        if not self.canonical_agency_names_dict:
            self._make_canonical_agency_names_dict()

        if self.reduced_transit_on_board_df.empty:
            self.reduce_on_board_survey()

        self._reduce_ctpp_2012_2016()
        self._make_simulated_maz_data()

        assert sorted(
            self.ctpp_2012_2016_df.residence_county.unique().tolist()
        ) == sorted(self.county_names_list)
        assert sorted(self.ctpp_2012_2016_df.work_county.unique().tolist()) == sorted(
            self.county_names_list
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

    def _write_other_comparisons(self):

        file_root = self.observed_dict["remote_io"]["acceptance_output_folder_root"]
        out_file = os.path.join(file_root, self.output_other_filename)
        self.compare_gdf.to_csv(out_file)

        return

    def _make_roadway_network_comparisons():

        # placeholder
        # _write_roadway_network()

        return

    def _make_combined_line_name(self, input_df, input_column_name):

        df = input_df[input_column_name].str.split(pat="_", expand=True).copy()
        df["combined_line_name"] = df[0] + "_" + df[1] + "_" + df[2]
        return_df = pd.concat([input_df, df["combined_line_name"]], axis="columns")

        return return_df

    def _make_transit_network_comparisons(self):

        # outer merge for rail operators
        join_df = self.reduced_transit_on_board_df[
            self.reduced_transit_on_board_df["survey_operator"].isin(
                self.rail_operators_vector
            )
        ]
        join_df = join_df[
            [
                "survey_tech",
                "survey_operator",
                "survey_route",
                "survey_boardings",
                "florida_threshold",
            ]
        ].rename(columns={"survey_operator": "operator"})
        join_df["combined_line_name"] = "Missing"

        rail_df = pd.merge(
            self.simulated_boardings_df[
                self.simulated_boardings_df["operator"].isin(self.rail_operators_vector)
            ],
            join_df,
            how="outer",
            on=["operator", "combined_line_name"],
        )

        # left merge for non-rail operators
        non_df = pd.merge(
            self.simulated_boardings_df[
                ~self.simulated_boardings_df["operator"].isin(
                    self.rail_operators_vector
                )
            ],
            self.reduced_transit_on_board_df[
                ~self.reduced_transit_on_board_df["survey_operator"].isin(
                    self.rail_operators_vector
                )
            ],
            how="left",
            on=["combined_line_name"],
        )

        both_df = pd.concat([rail_df, non_df])

        both_df["operator"] = np.where(
            both_df["operator"].isnull(),
            both_df["survey_operator"],
            both_df["operator"],
        )
        both_df["technology"] = np.where(
            both_df["technology"].isnull(),
            both_df["survey_tech"],
            both_df["technology"],
        )

        # join the shape
        a_df = pd.DataFrame(self.simulated_transit_segments_gdf)
        a_df = self._make_combined_line_name(a_df, "line")
        b_df = a_df.groupby("combined_line_name").agg({"line": "first"}).reset_index()
        c_df = pd.DataFrame(
            self.simulated_transit_segments_gdf[
                self.simulated_transit_segments_gdf["line"].isin(b_df["line"])
            ].copy()
        )
        simple_shape_df = pd.merge(c_df, b_df, how="left", on="line")

        return_df = pd.merge(
            both_df,
            simple_shape_df,
            how="left",
            on="combined_line_name",
        )

        return_df = return_df.rename(
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

        # only attach route-level boardings to first link
        return_df["route_simulated_boardings"] = np.where(
            return_df["first_row_in_line"], return_df["route_simulated_boardings"], 0
        )
        return_df["route_observed_boardings"] = np.where(
            return_df["first_row_in_line"], return_df["route_observed_boardings"], 0
        )

        return_df = self._fix_technology_labels(return_df, "technology")

        return_df = return_df[
            [
                "model_link_id",
                "operator",
                "technology",
                "route_short_name",
                "route_long_name",
                "route_observed_boardings",
                "route_simulated_boardings",
                "segment_simulated_boardings",
                "florida_threshold",
                "geometry",
            ]
        ]

        self.transit_network_gdf = gpd.GeoDataFrame(
            return_df, crs="EPSG:" + self.tableau_projection, geometry="geometry"
        )

        self._write_transit_network()

        return

    def _make_other_comparisons(self):

        df = self._make_home_work_flow_comparisons()

        self.compare_gdf = df

        self._write_other_comparisons()

        return

    def _join_standard_route_id(self, input_df):

        file_root = self.observed_dict["remote_io"]["crosswalk_folder_root"]
        in_file = self.observed_dict["crosswalks"]["crosswalk_standard_survey_file"]

        df = pd.read_csv(os.path.join(file_root, in_file))

        df = self._fix_agency_names(df, "survey_agency")
        join_df = df[~df["survey_agency"].isin(self.rail_operators_vector)].copy()

        join_df = self._make_combined_line_name(join_df, "standard_line_name")
        join_df = (
            join_df.groupby(
                [
                    "survey_agency",
                    "survey_route",
                    "standard_route_id",
                    "combined_line_name",
                    "canonical_operator",
                    "standard_route_short_name",
                ]
            )
            .agg({"standard_route_long_name": "first"})
            .reset_index()
        )

        return_df = pd.merge(
            input_df,
            join_df,
            how="left",
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

            in_df = pd.read_feather(os.path.join(file_root, in_file))
            out_df = in_df[
                (in_df["weekpart"].isna()) | (in_df["weekpart"] != "WEEKEND")
            ].copy()
            out_df["route"] = np.where(
                out_df["operator"].isin(self.rail_operators_vector),
                out_df["operator"],
                out_df["route"],
            )
            out_df = (
                out_df.groupby(["survey_tech", "operator", "route"])["boarding_weight"]
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
            out_df = self._fix_agency_names(out_df, "survey_operator")
            # out_df = self._fix_technology_labels(out_df, "survey_tech")
            out_df = self._join_florida_thresholds(out_df)
            out_df = self._join_standard_route_id(out_df)
            out_df.to_csv(os.path.join(file_root, out_file))
            self.reduced_transit_on_board_df = out_df

        return

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
        c_df = self._fix_agency_names(c_df, "operator")
        c_df = self._make_combined_line_name(c_df, "line_name")
        return_df = (
            c_df.groupby(
                [
                    "combined_line_name",
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

        b_dict = (
            df[(df["alternate_01"].notna())][["canonical_name", "alternate_01"]]
            .set_index("alternate_01")
            .to_dict()["canonical_name"]
        )
        c_dict = (
            df[(df["alternate_02"].notna())][["canonical_name", "alternate_02"]]
            .set_index("alternate_02")
            .to_dict()["canonical_name"]
        )

        self.canonical_agency_names_dict = {**a_dict, **b_dict, **c_dict}

        return

    def _fix_agency_names(self, input_df, column_name):

        assert column_name in input_df.columns

        return_df = input_df.copy()

        return_df[column_name] = return_df[column_name].map(
            self.canonical_agency_names_dict
        )

        return return_df

    def _fix_technology_labels(self, input_df, column_name):

        assert column_name in input_df.columns

        r_df = input_df.copy()

        fix_dict = {
            "local": "Local Bus",
            "express": "Express Bus",
            "light": "Light Rail",
            "ferry": "Ferry",
            "heavy": "Heavy Rail",
            "commuter": "Commuter Rail",
        }

        for key, value in fix_dict.items():
            r_df[column_name] = np.where(
                r_df[column_name].str.lower().str.contains(key),
                value,
                r_df[column_name],
            )

        return r_df

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

    def _make_home_work_flow_comparisons(self):

        if self.ctpp_2012_2016_df.empty:
            self._reduce_ctpp_2012_2016()

        if self.simulated_home_work_flows_df.empty:
            self._reduce_simulated_home_work_flows()

        adjust_observed = (
            self.simulated_home_work_flows_df.simulated_flow.sum()
            / self.ctpp_2012_2016_df.observed_flow.sum()
        )
        j_df = self.ctpp_2012_2016_df.copy()
        j_df["observed_flow"] = j_df["observed_flow"] * adjust_observed

        df = pd.merge(
            self.simulated_home_work_flows_df,
            j_df,
            how="left",
            on=["residence_county", "work_county"],
        )

        df["criteria_number"] = 23
        df["acceptance_threshold"] = "Less than 15 percent RMSE"
        df[
            "criteria_name"
        ] = "Percent root mean square error in CTPP county-to-county worker flows"
        df["dimension_01_name"] = "residence_county"
        df["dimension_02_name"] = "work_county"
        df = df.rename(
            columns={
                "residence_county": "dimension_01_value",
                "work_county": "dimension_02_value",
                "observed_flow": "observed_outcome",
                "simulated_flow": "simulated_outcome",
            }
        )

        return df
