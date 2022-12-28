"""Methods to create Acceptance Criteria summaries from a tm2py model run."""

from typing import Union, List

import os
import geopandas as gpd
import pandas as pd
import toml


class Acceptance:

    def _load_configs(self):
         
        with open(self.scenario_file, "r", encoding="utf-8") as toml_file:
            self.scenario_dict = toml.load(toml_file)
         
        with open(self.observed_file, "r", encoding="utf-8") as toml_file:
            self.observed_dict = toml.load(toml_file)
            
        return

    def __init__(self, scenario_file: str, observed_file: str) -> None:
        self.scenario_file = scenario_file
        self.observed_file = observed_file
        self._load_configs()

    scenario_dict: dict
    observed_dict: dict

    reduced_on_board_survey_df: pd.DataFrame

    tableau_projection = 4326

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

    def make_acceptance(self):

        self._load_configs()

        # _validate() method to validate the scenario and observed data to fail fast
        # _make_roadway_network_comparisons() method to build roadway network comparisons
        # _make_transit_network_comparisons()
        # _make_other_comparisons()

        return

    def _validate():

        # _validate_scenario()
        # _validate_observed()

        return

    def _validate_scenario():

        # is the model run complete?
        # are the key files present?

        return

    def _validate_observed(self):

        # are all the key files present?
        # do time period names align with the scenario config?

        if self.reduced_on_board_survey_df is None:
            self._reduce_on_board_survey_df()



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

    def _reduce_on_board_survey(self):

        file_root = self.observed_dict["folder_root"]
        in_file = self.observed_dict["transit"]["on_board_survey_file"]
        out_file = self.observed_dict["transit"]["reduced_summaries_file"]

        if os.path.isfile(out_file):
            self.reduced_on_board_survey_df = pd.read_csv(os.path.join(file_root, out_file))
        else:
            in_df = pd.read_csv(os.path.join(file_root, in_file))
            out_df = in_df.groupby(["operator", "route", "time_period"])["boarding_weight"].sum().reset_index()
            out_df.to_csv(os.path.join(file_root, out_file))

        return
