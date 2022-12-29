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

    model_time_periods = []

    # Reduced data specs
    reduced_transit_on_board_df = pd.DataFrame(
        {
            "survey_tech": pd.Series(dtype="str"),
            "operator": pd.Series(dtype="str"),
            "route": pd.Series(dtype="str"),
            "time_period": pd.Series(dtype="str"),
            "weight": pd.Series(dtype="float"),
        }
    )

    tableau_projection = 4326

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
            "route": pd.Series(dtype="str"),
            "direction": pd.Series(dtype="int"),
            "time_period": pd.Series(dtype="str"),
            "observed_passengers": pd.Series(dtype="float"),
            "simulated_passengers": pd.Series(dtype="float"),
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
        # _make_roadway_network_comparisons() method to build roadway network comparisons
        # _make_transit_network_comparisons()
        # _make_other_comparisons()

        return

    def _validate(self):

        # _validate_scenario()
        self._validate_observed()

        return

    def _validate_scenario():

        # is the model run complete?
        # are the key files present?

        return

    def _validate_observed(self):

        # are all the key files present?
        # do time period names align with the scenario config?

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

        # set the geometry
        # make sure it is in the right projection
        # get the file location from the dictionary
        # write to disk

        return

    def _write_transit_network():

        # set the geometry
        # make sure it is in the right projection
        # get the file location from the dictionary
        # write to disk

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

    def _make_transit_network_comparisons():

        return

    def _make_other_comparisons():

        # separate method for each one

        return

    def reduce_on_board_survey(self):
        """Reduces the on-board survey, summarizing boardings by technology, operator, route, and time of day. 
        Result is stored in the reduced_transit_on_board_df DataFrame and written to disk in the `reduced_summaries_file`
        in the observed configuration.
        """

        file_root = self.observed_dict["remote_io"]["folder_root"]
        in_file = self.observed_dict["transit"]["on_board_survey_file"]
        out_file = self.observed_dict["transit"]["reduced_summaries_file"]

        if os.path.isfile(out_file):
            self.reduced_transit_on_board_df = pd.read_csv(
                os.path.join(file_root, out_file),
                dtype=self.reduced_transit_on_board_df.dtypes.to_dict(),
            )
        else:
            time_period_dict = {
                "EARLY AM": "ea",
                "AM PEAK": "am",
                "MIDDAY": "md",
                "PM PEAK": "pm",
                "EVENING": "ev",
                "NIGHT": "ev",
            }
            in_df = pd.read_feather(os.path.join(file_root, in_file))
            out_df = in_df.loc[in_df["weekpart"] == "WEEKDAY"]
            out_df = out_df.loc[
                out_df["day_part"].isin(
                    ["EARLY AM", "AM PEAK", "MIDDAY", "PM PEAK", "EVENING", "NIGHT"]
                )
            ]
            out_df["time_period"] = out_df["day_part"].map(time_period_dict)
            out_df = (
                out_df.groupby(["survey_tech", "operator", "route", "time_period"])[
                    "weight"
                ]
                .sum()
                .reset_index()
            )
            out_df.to_csv(os.path.join(file_root, out_file))
            self.reduced_transit_on_board_df = out_df

        return
