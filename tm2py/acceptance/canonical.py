"""Methods to handle canonical names for the Acceptance Criteria summaries from a tm2py model run."""

import os
import pandas as pd
import toml


class Canonical:
    canonical_dict: dict
    canonical_file: str
    scenario_dict: dict
    scenario_file: str

    census_2010_to_maz_crosswalk_df: pd.DataFrame

    canonical_agency_names_dict = {}
    canonical_station_names_dict = {}

    gtfs_to_tm2_mode_codes_df: pd.DataFrame
    standard_transit_to_survey_df: pd.DataFrame

    standard_to_emme_node_crosswalk_df: pd.DataFrame
    pems_to_link_crosswalk_df: pd.DataFrame
    taz_to_district_df: pd.DataFrame

    ALL_DAY_WORD = "daily"
    WALK_ACCESS_WORD = "Walk"
    PARK_AND_RIDE_ACCESS_WORD = "Park and Ride"
    KISS_AND_RIDE_ACCESS_WORD = "Kiss and Ride"
    BIKE_ACCESS_WORD = "Bike"

    ALL_VEHICLE_TYPE_WORD = "All Vehicles"
    LARGE_TRUCK_VEHICLE_TYPE_WORD = "Large Trucks"

    MANAGED_LANE_OFFSET = 10000000

    transit_technology_abbreviation_dict = {
        "LOC": "Local Bus",
        "EXP": "Express Bus",
        "LTR": "Light Rail",
        "FRY": "Ferry",
        "HVY": "Heavy Rail",
        "COM": "Commuter Rail",
    }

    rail_operators_vector = [
        "BART",
        "Caltrain",
        "ACE",
        "Sonoma-Marin Area Rail Transit",
        "SMART",
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

        with open(self.scenario_file, "r", encoding="utf-8") as toml_file:
            self.scenario_dict = toml.load(toml_file)
        


        return

    def __init__(
        self, canonical_file: str, scenario_file: str = None, on_board_assign_summary: bool = False
    ) -> None:
        self.canonical_file = canonical_file
        self.scenario_file = scenario_file
        self._load_configs()
        self._make_canonical_agency_names_dict()
        self._make_canonical_station_names_dict()
        self._read_standard_to_emme_transit()
        self._make_tm2_to_gtfs_mode_crosswalk()
        self._read_standard_transit_to_survey_crosswalk()
        self._make_simulated_maz_data()

        if not on_board_assign_summary:
            self._make_census_maz_crosswalk()
            self._read_pems_to_link_crosswalk()
            self._read_standard_to_emme_node_crosswalk()

        return

    def _make_simulated_maz_data(self):
        in_file = self.scenario_dict["scenario"]["maz_landuse_file"]

        df = pd.read_csv(in_file)

        index_file = os.path.join("inputs", "landuse", "mtc_final_network_zone_seq.csv")

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

        self._make_taz_district_crosswalk()

        return
        
    def _make_taz_district_crosswalk(self):

        df = self.simulated_maz_data_df[["TAZ_ORIGINAL", "DistID"]].copy()
        df = df.rename(columns={"TAZ_ORIGINAL": "taz", "DistID": "district"})
        self.taz_to_district_df = df.drop_duplicates().reset_index(drop=True)

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
        d_dict = (
            df[(df["alternate_03"].notna())][["canonical_name", "alternate_03"]]
            .set_index("alternate_03")
            .to_dict()["canonical_name"]
        )
        e_dict = (
            df[(df["alternate_04"].notna())][["canonical_name", "alternate_04"]]
            .set_index("alternate_04")
            .to_dict()["canonical_name"]
        )
        f_dict = (
            df[(df["alternate_05"].notna())][["canonical_name", "alternate_05"]]
            .set_index("alternate_05")
            .to_dict()["canonical_name"]
        )

        self.canonical_agency_names_dict = {
            **a_dict,
            **b_dict,
            **c_dict,
            **d_dict,
            **e_dict,
            **f_dict,
        }

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

        df = df[["TM2_mode", "TM2_operator", "agency_name", "TM2_line_haul_name"]]
        df = df.rename(
            columns={
                "TM2_mode": "tm2_mode",
                "TM2_operator": "tm2_operator",
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
        df = df[
            [
                "survey_route",
                "survey_agency",
                "survey_tech",
                "standard_route_id",
                "standard_line_name",
                "standard_operator",
                "standard_headsign",
                "standard_agency",
                "standard_route_short_name",
                "standard_route_long_name",
                "canonical_operator",
            ]
        ].drop_duplicates()

        self.standard_transit_to_survey_df = df

        return

    def aggregate_line_names_across_time_of_day(
        self, input_df: pd.DataFrame, input_column_name: str
    ) -> pd.DataFrame:
        df = input_df[input_column_name].str.split(pat="_", expand=True).copy()
        df["daily_line_name"] = df[0] + "_" + df[1] + "_" + df[2]
        return_df = pd.concat([input_df, df["daily_line_name"]], axis="columns")

        return return_df

    def _read_pems_to_link_crosswalk(self) -> pd.DataFrame:
        file_root = self.canonical_dict["remote_io"]["crosswalk_folder_root"]
        in_file = self.canonical_dict["crosswalks"]["pems_station_to_tm2_links_file"]

        df = pd.read_csv(os.path.join(file_root, in_file))
        df["station_id"] = df["station"].astype(str) + "_" + df["direction"]
        df = df[["station_id", "A", "B"]]

        self.pems_to_link_crosswalk_df = df

        return

    def _read_standard_to_emme_node_crosswalk(self) -> pd.DataFrame:
        file_root = self.canonical_dict["remote_io"]["crosswalk_folder_root"]
        in_file = self.canonical_dict["crosswalks"]["standard_to_emme_nodes_file"]

        df = pd.read_csv(os.path.join(file_root, in_file))

        self.standard_to_emme_node_crosswalk_df = df

        return
