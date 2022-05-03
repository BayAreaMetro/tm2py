"""Module containing the """

import os
import numpy as np
import pandas as pd

from tm2py.components.component import Component
from tm2py.config import TimePeriodConfig
from tm2py.emme.matrix import OMXManager
from tm2py.logger import LogStartEnd


MODE_NAME_MAP = {
    "b": "LOCAL_BUS",
    "f": "FERRY_SERVICE",
    "h": "HEAVY_RAIL",
    "l": "LIGHT_RAIL",
    "r": "COMMUTER_RAIL",
    "x": "EXPRESS_BUS",
}


class DriveAccessSkims(Component):
    """Joins the highway skims with the nearest TAP to support drive access to transit.

    The procedure is:
        Get closest maz (walk distance) for each TAP, from active mode skims
        Get taz corresponding to that maz (maz-taz lookup table)
        for each time period
            for each TAZ
                for each mode
                    find closest TAP (smallest gen cost+MAZ walk access, ignores transit time)
                    write row of FTAZ,MODE,PERIOD,TTAP,TMAZ,TTAZ,DTIME,DDIST,DTOLL,WDIST
    """

    @LogStartEnd()
    def run(self):
        results_path = self._init_results_file()
        maz_taz = self._maz_taz_correspondence()
        ped_dist = self._get_ped_dist()
        maz_taz_tap = maz_taz.merge(ped_dist, on="TMAZ")
        for period in self.config.time_periods:
            tap_modes = self._get_tap_modes(period)
            maz_ttaz_tap_modes = maz_taz_tap.merge(tap_modes, on="TTAP")
            drive_costs = self._get_drive_costs(period)
            taz_to_tap_costs = drive_costs.merge(maz_ttaz_tap_modes, on="TTAZ")
            closest_taps = self._get_closest_taps(taz_to_tap_costs, period)
            with open(results_path, "a", newline="", encoding="utf8") as output_file:
                closest_taps.to_csv(
                    output_file, header=False, index=False, float_format="%.5f"
                )

    def _init_results_file(self) -> str:
        """Initialize and write header to results file"""
        output_file_path = self.get_abs_path(
            self.config.highway.drive_access_output_skim_path
        )
        os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
        with open(output_file_path, "w", encoding="utf8") as output_file:
            output_file.write(
                "FTAZ,MODE,PERIOD,TTAP,TMAZ,TTAZ,DTIME,DDIST,DTOLL,WDIST\n"
            )
        return output_file_path

    def _maz_taz_correspondence(self) -> pd.DataFrame:
        """Load maz data (landuse file) which has the MAZ-> TAZ correspondence"""
        maz_data_file = self.get_abs_path(self.config.scenario.maz_landuse_file)
        maz_input_data = pd.read_csv(maz_data_file)
        # drop the other landuse columns

        # disable no-member error as Pandas returns either a parser object or a dataframe
        # depending upon the inputs to pd.read_csv, and in this case we get a
        # dataframe so in fact it has .columns
        # set the maz sequence numbers
        # pylint: disable=E1101
        maz_taz_mapping = maz_input_data.drop(
            columns=list(set(maz_input_data.columns) - {"MAZ_ORIGINAL", "TAZ_ORIGINAL"})
        )
        maz_taz_mapping["TMAZ"] = maz_taz_mapping.index + 1
        # Get taz seq numbers
        taz_ids = maz_taz_mapping["TAZ_ORIGINAL"].unique()
        taz_ids.sort()
        taz_seq = list(range(1, len(taz_ids) + 1))
        taz_seq_mapping = pd.DataFrame({"TAZ_ORIGINAL": taz_ids, "TTAZ": taz_seq})
        # Merge them back to get a table with the MAZ sequence and TAZ sequence (TMAZ and TTAZ)
        maz_taz = maz_taz_mapping.merge(taz_seq_mapping, on="TAZ_ORIGINAL")
        maz_taz.drop(columns=["MAZ_ORIGINAL", "TAZ_ORIGINAL"], inplace=True)
        return maz_taz

    def _get_ped_dist(self) -> pd.DataFrame:
        """Get walk distance from closest maz to tap"""
        # Load the shortest distance skims from the active mode skims results
        for skim_spec in self.config.active_modes.shortest_path_skims:
            if (
                skim_spec.mode == "walk"
                and skim_spec.roots == "MAZ"
                and skim_spec.leaves == "TAP"
            ):
                ped_skim_path = skim_spec["output"]
                break
        else:
            raise Exception(
                "No skim mode of WALK: MAZ->MAZ in active_modes.shortest_path_skims"
            )
        ped_dist = pd.read_csv(self.get_abs_path(ped_skim_path))
        ped_dist.rename(
            columns={
                "from_zone": "TMAZ",
                "to_zone": "TTAP",
                "shortest_path_cost": "WDIST",
            },
            inplace=True,
        )
        # Get closest MAZ to each TAZ
        # disable no-member error as Pandas returns either a parser object or a dataframe
        # depending upon the inputs to pd.read_csv, and in this case we get a
        # dataframe so in fact it has .sort_values
        # pylint: disable=E1101
        ped_dist.sort_values(["TTAP", "WDIST"], inplace=True)
        ped_dist.drop_duplicates("TTAP", inplace=True)
        return ped_dist

    def _get_tap_modes(self, period: TimePeriodConfig) -> pd.DataFrame:
        """Get the set of modes available from each TAP."""
        emmebank = self.controller.emme_manager.emmebank(
            self.get_abs_path(self.config.emme.transit_database_path)
        )
        # load Emme network for TAP<->available modes correspondence
        scenario = emmebank.scenario(period.emme_scenario_id)
        attrs_to_load = {
            "NODE": ["@tap_id"],
            "TRANSIT_LINE": [],
            "TRANSIT_SEGMENT": ["allow_alightings", "allow_boardings"],
        }
        if self.config.transit.use_fares:
            attrs_to_load["TRANSIT_LINE"].append("#src_mode")

            def process_stops(stops):
                modes = set()
                for stop in stops:
                    for seg in stop.outgoing_segments(include_hidden=True):
                        if seg.allow_alightings or seg.allow_boardings:
                            modes.add(MODE_NAME_MAP[seg.line["#src_mode"]])
                return modes
        else:
            def process_stops(stops):
                modes = set()
                for stop in stops:
                    for seg in stop.outgoing_segments(include_hidden=True):
                        if seg.allow_alightings or seg.allow_boardings:
                            modes.add(MODE_NAME_MAP[seg.line.mode.id])
                return modes

        network = self.controller.emme_manager.get_network(scenario, attrs_to_load)
        tap_ids = []
        tap_mode_ids = []
        for node in network.nodes():
            if node["@tap_id"] == 0:
                continue
            stops = set([])
            for link in node.outgoing_links():
                for next_link in link.j_node.outgoing_links():
                    stops.add(next_link.j_node)
            modes = process_stops(stops)
            if modes:
                tap_mode_ids.extend(modes)
                tap_ids.extend([node["@tap_id"]] * len(modes))
        tap_modes = pd.DataFrame({"TTAP": tap_ids, "MODE": tap_mode_ids})
        return tap_modes

    def _get_drive_costs(self, period: TimePeriodConfig) -> pd.DataFrame:
        """Load the drive costs from OMX matrix files, return as pandas dataframe."""
        emmebank = self.controller.emme_manager.emmebank(
            self.get_abs_path(self.config.emme.highway_database_path)
        )
        scenario = emmebank.scenario(period.emme_scenario_id)
        zone_numbers = scenario.zone_numbers
        network = self.controller.emme_manager.get_network(
            scenario, {"NODE": ["#county", "@taz_id"]}
        )
        externals = [
            n["@maz_id"]
            for n in network.nodes()
            if n["@maz_id"] > 0 and n["#county"] == "External"
        ]
        root_ids = np.repeat(zone_numbers, len(zone_numbers))
        leaf_ids = zone_numbers * len(zone_numbers)

        skim_src_file = self.get_abs_path(
            self.config.highway.output_skim_path.format(period=period.name)
        )
        with OMXManager(skim_src_file, "r") as src_file:
            drive_costs = pd.DataFrame(
                {
                    "FTAZ": root_ids,
                    "TTAZ": leaf_ids,
                    "DDIST": src_file.read(f"{period.name}_da_dist").flatten(),
                    "DTOLL": src_file.read(f"{period.name}_da_bridgetollda").flatten(),
                    "DTIME": src_file.read(f"{period.name}_da_time").flatten(),
                }
            )
        # drop externals
        drive_costs = drive_costs[~drive_costs["FTAZ"].isin(externals)]
        drive_costs = drive_costs[~drive_costs["TTAZ"].isin(externals)]
        # drop inaccessible zones
        drive_costs = drive_costs.query("DTIME > 0 & DTIME < 1e19")
        return drive_costs

    @staticmethod
    def _get_closest_taps(
            taz_to_tap_costs: pd.DataFrame, period: TimePeriodConfig
    ) -> pd.DataFrame:
        """Calculate the TAZ-> TAP drive cost, and get the closest TAP for each TAZ."""
        # cost = time + vot * (dist * auto_op_cost + toll)
        value_of_time = 18.93
        operating_cost_per_mile = 17.23
        auto_op_cost = operating_cost_per_mile  # / 5280 # correct for feet
        vot = 0.6 / value_of_time  # turn into minutes / cents
        walk_speed = 60.0 / 3.0
        taz_to_tap_costs["COST"] = (
            taz_to_tap_costs["DTIME"]
            + walk_speed * taz_to_tap_costs["WDIST"]
            + vot
            * (auto_op_cost * taz_to_tap_costs["DDIST"] + taz_to_tap_costs["DTOLL"])
        )
        # sort by mode, from taz and gen cost to get the closest TTAP to each FTAZ
        # for each mode, then drop subsequent rows
        closest_tap_costs = taz_to_tap_costs.sort_values(["MODE", "FTAZ", "COST"])
        closest_tap_costs.drop_duplicates(["MODE", "FTAZ"], inplace=True)
        closest_tap_costs["PERIOD"] = period.name
        columns = [
            "FTAZ",
            "MODE",
            "PERIOD",
            "TTAP",
            "TMAZ",
            "TTAZ",
            "DTIME",
            "DDIST",
            "DTOLL",
            "WDIST",
        ]
        return closest_tap_costs[columns]
