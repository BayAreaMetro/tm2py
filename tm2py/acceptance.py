"""Methods to create Acceptance Criteria summaries from a tm2py model run."""


from tm2py.config import Configuration
from typing import Union, List

import geopandas as gpd
import pandas as pd
import toml

class Acceptance:

    scenario_config: Configuration
    observed_dict: dict

    tableau_projection = 4326

    road_network_gdf = gpd.GeoDataFrame(
        {
            "model_link_id": pd.Series(dtype = "int"),
            "pems_station_id": pd.Series(dtype = "int"),
            "time_period": pd.Series(dtype="str"),
            "observed_flow": pd.Series(dtype="float"),
            "simulated_flow": pd.Series(dtype="float"),
            "odot_flow_category": pd.Series(dtype="int"),
            "odot_maximum_error": pd.Series(dtype="float"),
            "line_string": pd.Series(dtype="str") 

        }
    )

    transit_network_gdf = gpd.GeoDataFrame(
        {
            "model_link_id": pd.Series(dtype = "int"),
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
            "criteria_number": pd.Series(dtype = "int"),
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
    
    def make_acceptance(scenario_file: str, observed_config_file: str):

        scenario_config = Configuration.load_toml(path = scenario_file)

        with open(observed_config_file, "r", encoding="utf-8") as toml_file:
            observed_dict = toml.load(toml_file)

        # _validate() method to validate the scenario and observed data to fail fast
        # _make_roadway_network_comparisons() method to build roadway network comparisons
        # _make_transit_network_comparisons() 
        # _make_other_comparisons()

        return

    def _validate():

        #_validate_scenario()
        #_validate_observed()

        return

    def _validate_scenario():

        # is the model run complete?
        # are the key files present? 

        return

    def _validate_observed():

        # are all the key files present? 
        # do time period names align with the scenario config?

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
