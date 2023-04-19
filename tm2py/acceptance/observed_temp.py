"""Methods to handle observed data for the Acceptance Criteria summaries from a tm2py model run."""

from tm2py.acceptance.canonical_temp import Canonical

import numpy as np
import os
import geopandas as gpd
import pandas as pd
import toml


class Observed:

    c: Canonical

    observed_dict: dict
    observed_file: str

    RELEVANT_COUNT_OBSERVED_YEARS_LIST = [2014, 2015, 2016] 
    
    ohio_RMSE_standards_df = pd.DataFrame(  
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
        [97500, 9750, 12]
        ],
        columns = ["daily_volume_midpoint","hourly_volume_midpoint","desired_percent_RMSE"]
    )

    reduced_traffic_counts_df = pd.DataFrame(
        { "model_link_id": pd.Series(dtype="int"),
          "pems_station_id": pd.Series(dtype="int"),
          "time_period": pd.Series(dtype="str"),
          "observed_flow": pd.Series(dtype="float"),
          "odot_flow_category_daily":pd.Series(dtype="str"),
          "odot_flow_category_hourly":pd.Series(dtype="str"),
          "odot_maximum_error": pd.Series(dtype="float"),
          "key_location":pd.Series(dtype="str")}
    )
    
    key_arterials_df = pd.DataFrame(  
        [
        ["San Pablo", "Alameda", 123,np.nan], # NO PEMS
        ["19th Ave", "San Franscisco", 1, "401180_N"],
        ["El Camino Real", "San Mateo", 82,np.nan], # NO PEMS
        ["El Camino Real", "Santa Clara", 82,np.nan], # NO PEMS
        ["Mission Blvd", "Alameda", 238, "400646_N"],
        ["Ygnacio Valley Road", "Contra Costa",np.nan,np.nan], # NO PEMS
        ["Hwy 12", "Solano", 12, "409485_W"],
        ["Hwy 37", "Marin", 37, "402038_W"],
        ["Hwy 29", "Napa", 29, "402864_N"],
        ["CA 128", "Sonoma", 128,np.nan], # NO PEMS
        ],
        columns = ["name","county","route","pems_station_id",]
    )       


    bridges_df = pd.DataFrame(
        [
        ["Antioch Bridge",np.nan], # no pems 
        ["Benecia-Martinez Bridge","402156_N"], 
        ["Carquinez Bridge","401638_W"], 
        ["Dumbarton Bridge","400841_W"],
        ["Richmond-San Rafael Bridge",np.nan], # no pems
        ["San Francisco-Oakland Bay Bridge","402827_W"], 
        ["San Mateo-Hayward Bridge","401272_W"],
        ["Golden Gate Bridge",np.nan], # no pems
        ],
        columns = ["name","pems_station_id"]
    )

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
        if self.reduced_traffic_counts_df.empty:
            self.reduce_traffic_counts() 
        
        try:
            assert(self.key_arterials_df.shape[0] == self.key_arterials_df["pems_station_id"].count()),"Missing count data key arterials"
        except AssertionError as msg:
            print(msg)

        try:
            assert(self.bridges_df.shape[0] == self.bridges_df["pems_station_id"].count()),"Missing count data bridges"
        except AssertionError as msg:
            print(msg)


    # check if all key arterials and bridges have count stations
        
    def _join_tm2_link_ids(self, input_df: pd.DataFrame) -> pd.DataFrame:

            # assignment output uses emme nodes and station crosswalk uses model nodes (= standard nodes?)

            df = self.c.pems_crosswalk.copy()
            nodes_df = self.c.node_crosswalk.copy()

            df = pd.merge(
                df,
                nodes_df,
                how = "left",
                left_on = "A",
                right_on = "model_node_id"
            ).rename(
                columns={
                    "emme_node_id":"INODE",
                }
            ).drop(['model_node_id'], axis = 1)

            df = pd.merge(
                df,
                nodes_df,
                how = "left",
                left_on = "B",
                right_on = "model_node_id"
            ).rename(
                columns={
                    "emme_node_id":"JNODE",
                }
            ).drop(['model_node_id'], axis = 1)

            df[['INODE','JNODE']] = df[['INODE','JNODE']].fillna(0)
            df[['INODE','JNODE']] = df[['INODE','JNODE']].astype(int)
            df['INODE-JNODE'] = df['INODE'].astype(str) + "-" + df['JNODE'].astype(str)

            df = df[["pems_station_id", "INODE-JNODE"]]

            df = pd.merge(
                input_df,
                df,
                how="right",
                on=["INODE-JNODE"],
            )

            return_df = df[['ID',"pems_station_id"]]    # returns a bridge between the ID in the assignment file (INODE-JNODE) and the pems_station_id
            return_df = return_df.rename(columns={"ID": "model_link_id"})
            return return_df

    def _join_ohio_standards(self, input_df: pd.DataFrame) -> pd.DataFrame: 

        df = self.ohio_RMSE_standards_df.copy()
        
        df["upper"] = (df["daily_volume_midpoint"].shift(-1) - df["daily_volume_midpoint"])/2
        df["lower"] = (df["daily_volume_midpoint"].shift(1) - df["daily_volume_midpoint"])/2
        df["low"] = df["daily_volume_midpoint"] + df["lower"]
        df["low"] = np.where(df["low"].isna(), 0, df["low"])
        df["high"] =  df["daily_volume_midpoint"] + df["upper"]
        df["high"] = np.where(df["high"].isna(), np.inf,df["high"])

        df = df.drop(["daily_volume_midpoint","hourly_volume_midpoint","upper","lower"], axis="columns")
    
        df = df.rename(columns={
                "desired_percent_RMSE": "odot_maximum_error",
            })

        vals = input_df.median_flow.values # input df is daily flows as summarized by reduce_traffic_counts
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

        return_df["low_hourly"] = return_df["low"]/10
        return_df["high_hourly"] = return_df["high"]/10
        return_df["odot_flow_category_daily"] = return_df["low"].astype("str") + "-" + return_df["high"].astype("str") 
        return_df["odot_flow_category_hourly"] = return_df["low_hourly"].astype("str") + "-" + return_df["high_hourly"].astype("str") 

        return_df = return_df.drop(["high", "low","high_hourly","low_hourly"], axis="columns")

        return return_df

    def _join_selected_links(self, input_df: pd.DataFrame) -> pd.DataFrame:
        df1 = self.key_arterials_df.copy()
        df2 = self.bridges.copy()
        df = pd.concat(df1,df2)[["name","pems_station_id"]]
        df = df.dropna()
        out_df = pd.merge(input_df,
                    df,
                    how = "left",
                    on = "pems_station_id")
        out_df.rename(columns={"name": "key_location"})

        return out_df
                    

    def reduce_traffic_counts(self,read_file_from_disk=True):

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

                # filter relevant years
                df = in_df[df.year.isin(self.RELEVANT_COUNT_OBSERVED_YEARS_LIST)].copy()

                # remove unneeded variables
                df = df[["pems_station_id","year","time_period","median_flow"]]

                # summarize counts by time of day (for when there are counts for more than one of the selected years)
                time_of_day_df = (
                    df.groupby(["pems_station_id","time_period"])
                        [
                            "median_flow"
                        ]
                        .mean()
                        .reset_index()
                    )
                all_day_df = time_of_day_df.rename(columns={"median_flow": "observed_flow"})
                
                # aggregate time of day counts to daily
                all_day_df = (
                    time_of_day_df.groupby(["pems_station_id"])
                        [
                            "observed_flow"
                        ]
                        .sum()
                        .reset_index()
                    )

                all_day_df['time_period'] =  self.c.ALL_DAY_WORD   

                
                # combine 
                out_df = pd.concat(
                    [all_day_df, time_of_day_df], axis="rows", ignore_index=True
                )
            
                out_df = self._join_ohio_standards(out_df) # to attach flow category from standards and determine the applicable standard
                out_df = self._join__tm2_link_ids(out_df) # to translate pems_station_id  into model_link_id
                out_df = self._join_selected_links(out_df)# to tag selected arterials and bridges
                out_df.to_csv(os.path.join(file_root, out_file))
                self.reduced_traffic_counts_df = out_df  

                return

                
