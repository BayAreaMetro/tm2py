"""Methods to handle canonical names for the Acceptance Criteria summaries from a tm2py model run."""

import os
import pandas as pd
import toml


class Canonical:

    canonical_dict: dict
    canonical_file: str

    #census_2010_to_maz_crosswalk_df: pd.DataFrame

    #canonical_agency_names_dict = {}
    #canonical_station_names_dict = {}

    #gtfs_to_tm2_mode_codes_df: pd.DataFrame
    #standard_transit_to_survey_df: pd.DataFrame

    ALL_DAY_WORD = "daily"
    #WALK_ACCESS_WORD = "Walk"
    #PARK_AND_RIDE_ACCESS_WORD = "Park and Ride"
    #KISS_AND_RIDE_ACCESS_WORD = "Kiss and Ride"
    #BIKE_ACCESS_WORD = "Bike"



    def _load_configs(self):

        with open(self.canonical_file, "r", encoding="utf-8") as toml_file:
            self.canonical_dict = toml.load(toml_file)

        return

    def __init__(self, canonical_file: str) -> None:
        self.canonical_file = canonical_file
        self._load_configs()
        self._make_pems_crosswalk()  
        self._read_node_crosswalk()  

        return

    def _make_pems_crosswalk(self) -> pd.DataFrame:
        file_root = self.canonical_dict["remote_io"]["crosswalk_folder_root"]
        in_file = self.canonical_dict["crosswalks"]["pems_station_to_tm2_links_file"]

        df = pd.read_csv(os.path.join(file_root, in_file))

        df["pems_station_id"] = df["station"].astype(str) + "_" + df["direction"]

        assert(df["pems_station_id"].is_unique) # validate crosswalk - correct location?

        df = df[["pems_station_id","A","B"]]

        self.pems_crosswalk = df

        return

    def _read_node_crosswalk(self) -> pd.DataFrame:
        file_root = self.canonical_dict["remote_io"]["crosswalk_folder_root"]
        in_file = self.canonical_dict["crosswalks"]["standard_to_emme_nodes_file"] 

        df = pd.read_csv(os.path.join(file_root, in_file))

        self.node_crosswalk = df
        
        return

