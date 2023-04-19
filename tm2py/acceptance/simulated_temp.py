"""Methods to handle simulation results for the Acceptance Criteria summaries from a tm2py model run."""

from tm2py.acceptance.canonical_temp import Canonical

import numpy as np
import os
import geopandas as gpd
import itertools
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

    simulated_traffic_flow_gdf: gpd.GeoDataFrame

    simulated_traffic_flow_df = pd.DataFrame(
        { "model_link_id": pd.Series(dtype="int"),
          "time_period": pd.Series(dtype="str"),
          "simulated_flow_auto": pd.Series(dtype="float"),
          "simulated_flow_truck": pd.Series(dtype="float"),
          "simulated_flow": pd.Series(dtype="float")}
    )

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

    def __init__(self, canonical: Canonical, scenario_file: str, model_file: str) -> None:
        self.c = canonical
        self.scenario_file = scenario_file
        self.model_file = model_file
        self._load_configs()
        self._get_model_time_periods()
        self._get_morning_commute_capacity_factor()
        self._validate()

    def _validate(self):
        self._reduce_simulated_traffic_flow()
        return 


    def _reduce_simulated_traffic_flow(self): 
        file_root = self.scenario_dict["scenario"]["root_dir"]
        #out_file =
        time_of_day_df = pd.DataFrame()
        time_periods = ["AM"] # temporary

        #for time_period in self.model_time_periods:
        for time_period in time_periods:

            gdf = gpd.read_file(
                os.path.join(file_root +  time_period + "/" + "emme_links.shp")    
            )
            df = gdf[["ID","@flow_da","@flow_lrgt","@flow_s2","@flow_s3","@flow_trk"]]
            df = df.rename(columns={"ID": "model_link_id"})
            df["time_period"] = time_period
            time_of_day_df = time_of_day_df.append(df)  # one file with selected vars for all the time periods
        
        time_of_day_df["simulated_flow_auto"] = time_of_day_df[["@flow_da" , "@flow_s2" , "@flow_s3"]].sum(axis=1) # include @flow_lrgt?
        time_of_day_df = time_of_day_df.rename(columns={"@flow_trk":"simulated_flow_truck"})
        time_of_day_df["simulated_flow"] = time_of_day_df[["simulated_flow_auto","simulated_flow_truck", "@flow_lrgt"]].sum(axis=1)

        all_day_df = (time_of_day_df.groupby(["model_link_id"]) # summarize all timeperiod flow variables to daily
                .sum()
                .reset_index()
                )
        all_day_df["time_period"] =  self.c.ALL_DAY_WORD  


        # combine 
        out_df = pd.concat(
                [time_of_day_df, all_day_df], axis="rows", ignore_index=True
            )

        # remove unneeded columns
        out_df = out_df[["model_link_id","time_period","simulated_flow_auto","simulated_flow_truck","simulated_flow"]]

        #out_df.to_csv(os.path.join(file_root, out_file))
        out_df.to_csv("simulated_traffic_flow_temp.csv")

        self.simulated_traffic_flow_df = out_df  # model_link_id, time_period (which includes each of the timeperiods and daily) and flow vars (including simulated_flow)


