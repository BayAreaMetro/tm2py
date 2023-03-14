"""Methods to handle canonical names for the Acceptance Criteria summaries from a tm2py model run."""

import os
import pandas as pd
import toml


class Canonical:

    canonical_dict: dict
    canonical_file: str

    census_2010_to_maz_crosswalk_df: pd.DataFrame

    canonical_agency_names_dict = {}
    canonical_station_names_dict = {}

    gtfs_to_tm2_mode_codes_df: pd.DataFrame
    standard_transit_to_survey_df: pd.DataFrame

    ALL_DAY_WORD = "daily"
    WALK_ACCESS_WORD = "Walk"
    PARK_AND_RIDE_ACCESS_WORD = "Park and Ride"
    KISS_AND_RIDE_ACCESS_WORD = "Kiss and Ride"
    BIKE_ACCESS_WORD = "Bike"

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

    def _load_configs(self):

        with open(self.canonical_file, "r", encoding="utf-8") as toml_file:
            self.canonical_dict = toml.load(toml_file)

        return

    def __init__(self, canonical_file: str) -> None:
        self.canonical_file = canonical_file
        self._load_configs()
        self._make_canonical_agency_names_dict()
        self._make_canonical_station_names_dict()
        # self._make_census_maz_crosswalk()
        self._read_standard_to_emme_transit()
        self._make_tm2_to_gtfs_mode_crosswalk()
        self._read_standard_transit_to_survey_crosswalk()

        return

    def _make_canonical_agency_names_dict(self):

        file_root = self.canonical_dict["remote_io"]["crosswalk_folder_root"]
        in_file = self.canonical_dict["crosswalks"]["canonical_agency_names_file"]

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

    def _make_canonical_station_names_dict(self):

        file_root = self.canonical_dict["remote_io"]["crosswalk_folder_root"]
        in_file = self.canonical_dict["crosswalks"]["canonical_station_names_file"]

        df = pd.read_csv(os.path.join(file_root, in_file))

        alt_list = list(df.columns)
        alt_list.remove("canonical")
        alt_list.remove("operator")

        running_operator_dict = {}
        for operator in df["operator"].unique():

            o_df = df[df["operator"] == operator].copy()

            a_df = o_df[["canonical"]].copy()
            a_df["temp"] = o_df["canonical"]
            a_dict = a_df.set_index("temp").to_dict()["canonical"]

            running_alt_dict = {**a_dict}

            for alt in alt_list:
                alt_dict = (
                    o_df[(o_df[alt].notna())][["canonical", alt]]
                    .set_index(alt)
                    .to_dict()["canonical"]
                )

                running_alt_dict = {**running_alt_dict, **alt_dict}

            operator_dict = {operator: running_alt_dict}

            running_operator_dict = {**running_operator_dict, **operator_dict}

        self.canonical_station_names_dict = running_operator_dict

        return

    def _make_census_maz_crosswalk(self):

        url_string = self.canonical_dict["crosswalks"]["block_group_to_maz_url"]
        self.census_2010_to_maz_crosswalk_df = pd.read_csv(url_string)

        return
    
    def _read_standard_to_emme_transit(self):

        root_dir = self.canonical_dict["remote_io"]["crosswalk_folder_root"]
        in_file = self.canonical_dict["crosswalks"]["standard_to_emme_transit_file"]

        x_df = pd.read_csv(os.path.join(root_dir, in_file))

        self.standard_to_emme_transit_nodes_df = x_df

        return
    
    def _make_tm2_to_gtfs_mode_crosswalk(self):

        file_root = self.canonical_dict["remote_io"]["crosswalk_folder_root"]
        in_file = self.canonical_dict["crosswalks"]["standard_to_tm2_modes_file"]

        df = pd.read_csv(os.path.join(file_root, in_file))

        df = df[["TM2_mode", "agency_name", "TM2_line_haul_name"]]
        df = df.rename(
            columns={
                "TM2_mode": "tm2_mode",
                "agency_name": "operator",
                "TM2_line_haul_name": "technology",
            }
        )

        self.gtfs_to_tm2_mode_codes_df = df
        
        return
    
    def _read_standard_transit_to_survey_crosswalk(self):

        file_root = self.canonical_dict["remote_io"]["crosswalk_folder_root"]
        in_file = self.canonical_dict["crosswalks"]["crosswalk_standard_survey_file"]

        df = pd.read_csv(os.path.join(file_root, in_file))

        self.standard_transit_to_survey_df = df

        return
    
    def aggregate_line_names_across_time_of_day(
        self, input_df: pd.DataFrame, input_column_name: str
    ) -> pd.DataFrame:

        df = input_df[input_column_name].str.split(pat="_", expand=True).copy()
        df["daily_line_name"] = df[0] + "_" + df[1] + "_" + df[2]
        return_df = pd.concat([input_df, df["daily_line_name"]], axis="columns")

        return return_df
