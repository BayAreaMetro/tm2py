"""Methods to create Acceptance Criteria summaries from a tm2py model run."""

from tm2py.acceptance.simulated_temp import Simulated
from tm2py.acceptance.observed_temp import Observed
from tm2py.acceptance.canonical_temp import Canonical

import numpy as np
import os
import geopandas as gpd
import itertools
import pandas as pd

class Acceptance:

    s: Simulated
    o: Observed
    c: Canonical

    acceptance_output_folder_root: str

    #output_transit_filename = "acceptance-transit-network.geojson"
    #output_other_filename = "acceptance-other.geojson"
    output_roadway_filename ="acceptance-roadway-network.geojson" 

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
            "geometry": pd.Series(dtype="str"),  
        }
    )

    def __init__(self, canonical: Canonical, simulated: Simulated, observed: Observed, output_file_root: str) -> None:
        self.c = canonical
        self.s = simulated
        self.o = observed
        self.acceptance_output_folder_root = output_file_root

    def make_acceptance(
        self, make_transit=False, make_roadway=True, make_other=False
    ):
        if make_roadway:
            self._make_roadway_network_comparisons()
        if make_transit:
            self._make_transit_network_comparisons()
        if make_other:
            self._make_other_comparisons()

        return


    def _write_roadway_network(self):

        file_root = self.acceptance_output_folder_root
        # TODO: figure out how to get remote write, use . for now
        file_root = "."
        out_file = os.path.join(file_root, self.output_roadway_filename)
        self.roadway_network_gdf.to_file(out_file, driver="GeoJSON")
        
        
        return
        


    def _calc_RMSE_roadway_comparison(self, input_df):

        df = input_df.copy()
        df["error"] = df["observed_flow"] - df["simulated_flow"]
        df["squared_error"] = df["error"]**2

        if 'dimension_02_name' in df.columns:
                mean_df = df.groupby(["dimension_01_value","dimension_02_value"])[["observed_flow","simulated_flow","squared_error","acceptance_threshold_value","dimension_01_value","dimension_02_value"]].mean().reset_index()
                add_df = df.groupby(["dimension_01_value","dimension_02_value"])[["criteria_number","criteria_name","acceptance_threshold","dimension_01_name","dimension_02_name"]].first().reset_index()
                summary_df = pd.merge(mean_df,
                                    add_df,
                                    how = "outer",
                                    on = ["dimension_01_value","dimension_02_value"])
        else:
                mean_df = df.groupby("dimension_01_value")[["observed_flow","simulated_flow","squared_error","acceptance_threshold_value","dimension_01_value"]].mean().reset_index()
                add_df = df.groupby("dimension_01_value")[["criteria_number","criteria_name","acceptance_threshold","dimension_01_name"]].first().reset_index()
                summary_df = pd.merge(mean_df,
                                    add_df,
                                    how = "outer",
                                    on = "dimension_01_value")
   
        summary_df["result"] = np.where(summary_df["percent_RMSE"] < summary_df["acceptance_threshold_value"],
        "Acceptable", "Not Acceptable")

        return summary_df

    def _make_roadway_network_comparisons(self):

        df = pd.merge(
            self.o.reduced_traffic_counts_df,
            self.s.simulated_traffic_flows_df, 
            how="left",
            on=["model_link_id", "time_period"]
        )


        df = df[
            [
                "model_link_id",
                "pems_station_id",
                "time_period",
                "observed_flow",
                "simulated_flow",
                "odot_flow_category",
                "odot_maximum_error",
                "key_arterial",
                "bridge",
               # "geometry",
            ]
        ]

        # for criteria 7
        criteria7_df = df.copy()
        criteria7_df["criteria_number"] = 7
        criteria7_df["criteria_name"] = "Percent root mean square error by volume category for daily outcomes"
        criteria7_df["acceptance_threshold"] = "OD0T threshold"
        criteria7_df["acceptance_threshold_value"] = criteria7_df["odot_maximum_error"]
        criteria7_df["dimension_01_name"] = "Volume Category"  
        criteria7_df["dimension_01_value"] = criteria7_df["odot_flow_category"]
        criteria7_df = criteria7_df[criteria7_df["time_period"] == self.c.ALL_DAY_WORD]

        criteria7_result_df = _calc_RMSE_roadway_comparison(criteria7_df)


        # for criteria 8 
        criteria8_df = df.copy()
        criteria8_df["criteria_number"] = 8
        criteria8_df["criteria_name"] = "Percent root mean square error by volume category for time-period specific outcomes"
        criteria8_df["acceptance_threshold"] = "OD0T threshold"
        criteria8_df["acceptance_threshold_value"] = criteria8_df["odot_maximum_error"]
        criteria8_df["dimension_01_name"] = "Volume Category"  
        criteria8_df["dimension_01_value"] = criteria8_df["odot_flow_category"]
        criteria8_df["dimension_02_name"] = "Time Period"
        criteria8_df["dimension_02_value"] = criteria8_df["time_period"]
        criteria8_df = criteria8_df[criteria8_df["time_period"] != self.c.ALL_DAY_WORD]

        criteria8_result_df = _calc_RMSE_roadway_comparison(criteria8_df)

        # for criteria 9
        criteria9_df = df.copy()
        criteria9_df["criteria_number"] = 9
        criteria9_df["criteria_name"] = "Percent error in daily vehicle miles traveled segmented by vehicle type (personal automobile, commercial automobile, medium truck, large truck)"
        criteria9_df["acceptance_threshold"] = "Less than 10 percent"
        criteria8_df["acceptance_threshold_value"] = 10
        criteria9_df["dimension_01_name"] = "Vehicle Type"
        #criteria9_df["dimension_01_value"] = ## simulated is by vehicle type but counts are not by vehicle type

        criteria9_result_df = _calc_RMSE_roadway_comparison(criteria9_df)

        # for criteria 10
        criteria10_df = df.copy()
        criteria10_df["criteria_number"] = 10
        criteria10_df["criteria_name"] = "Percent error in daily volume at each of the BATA-owned bridges plus the Golden Gate Bridge"
        criteria10_df["acceptance_threshold"] = "Less than 10 percent"
        criteria10_df["acceptance_threshold_value"] = 10
        criteria10_df["dimension_01_name"] = "Selected Bridges"
        criteria10_df["dimension_01_value"] = criteria10_df["bridge"]
        criteria10_df = criteria10_df["dimension_01_value"].notna()

        criteria10_result_df = _calc_RMSE_roadway_comparison(criteria10_df)

        # for criteria 11
        criteria11_df = df.copy()
        criteria11_df["criteria_number"] = 11
        criteria11_df["criteria_name"] = "Percent error in daily volume at a key location on selected arterials"
        criteria11_df["acceptance_threshold"] = "Less than 30 percent"
        criteria11_df["acceptance_threshold_value"] = 30
        criteria11_df["dimension_01_name"] = "Selected Arterials"
        criteria11_df["dimension_01_value"] = criteria11_df["key_arterial"]
        criteria11_df = criteria11_df["dimension_01_value"].notna
        
        criteria11_result_df = _calc_RMSE_roadway_comparison(criteria11_df)

        # for criteria 12  -- high quality data source?
        criteria12_df = df.copy()
        criteria12_df["criteria_number"] = 12
        criteria12_df["criteria_name"] = "Percent error in daily volume at a key location on selected arterials with high quality observed data"
        criteria12_df["acceptance_threshold"] = "Less than 30 percent"
        criteria12_df["acceptance_threshold_value"] = 30
        criteria12_df["dimension_01_name"] = "Selected Arterials"
        criteria12_df["dimension_01_value"] = criteria12_df["key_arterial"]

        criteria12_result_df = _calc_RMSE_roadway_comparison(criteria12_df)
         
        return_df = pd.concat([criteria7_result_df,criteria8_result_df,criteria9_result_df,criteria10_result_df,criteria11_result_df,criteria12_result_df])