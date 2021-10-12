

from contextlib import contextmanager as _context
import numpy as np
import openmatrix as _omx
import os
import pandas as pd

from tm2py.core.component import Component as _Component, Controller as _Controller
import tm2py.core.emme as _emme_tools
from tm2py.core.logging import LogStartEnd


class DriveAccessSkims(_Component):
    """
    get closest maz (walk distance) for each TAP
    get taz corresponding to that maz (maz-taz lookup table)
    for each time period
        for each TAZ
            for each mode
                find closest TAP (smallest gen cost+MAZ walk access, ignores transit time)
                write row of FTAZ,MODE,PERIOD,TTAP,TMAZ,TTAZ,DTIME,DDIST,DTOLL,WDIST
    """

    @LogStartEnd()
    def run(self):
        # write header to output_file
        # TODO: config
        #       refactor into multiple methods
        #       additional profiling with full model (should be fast enough though)
        output_file_path = os.path.join(self.root_dir, r"skim_matrices\transit\drive_access\drive_maz_taz_tap.csv")
        os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
        with open(output_file_path, "w") as output_file:
            output_file.write("FTAZ,MODE,PERIOD,TTAP,TMAZ,TTAZ,DTIME,DDIST,DTOLL,WDIST\n")

        # TODO: update config
        # Load maz data (landuse file) which has the MAZ-> TAZ correspondance
        maz_data_file = os.path.join(self.root_dir, self.config.scenario.maz_landuse_file)
        maz_input_data = pd.read_csv(maz_data_file)
        # drop the other landuse columns
        maz_taz_mapping = maz_input_data.drop(columns=list(set(maz_input_data.columns) - set(['MAZ_ORIGINAL', 'TAZ_ORIGINAL'])))
        # set the maz sequence numbers 
        maz_taz_mapping["TMAZ"] = maz_taz_mapping.index + 1
        # Get taz seq numbers
        taz_ids = maz_taz_mapping["TAZ_ORIGINAL"].unique()
        taz_ids.sort()
        taz_seq = list(range(1, len(taz_ids) + 1))
        taz_seq_mapping = pd.DataFrame({"TAZ_ORIGINAL": taz_ids, "TTAZ": taz_seq})
        # Merge them back to get a table with the MAZ sequence and TAZ sequence (TMAZ and TTAZ)
        maz_taz = maz_taz_mapping.merge(taz_seq_mapping, on="TAZ_ORIGINAL")
        maz_taz.drop(columns=["MAZ_ORIGINAL", "TAZ_ORIGINAL"], inplace=True)

        # Get walk distance from closest maz to tap
        # Load the shortest distance skims from the active mode skims results
        for skim_spec in self.config.active_modes.shortest_path_skims:
           if skim_spec.mode == "walk" and skim_spec.roots == "MAZ" and skim_spec.leaves == "TAP":
               ped_skim_path = skim_spec["output"]
               break
        ped_dist = pd.read_csv(os.path.join(self.root_dir, ped_skim_path))
        ped_dist.rename(columns={"from_zone": "TMAZ", "to_zone": "TTAP", "shortest_path_cost": "WDIST"}, inplace=True)
        # Get closest MAZ to each TAZ
        ped_dist = ped_dist.sort_values(["TTAP", 'WDIST']).drop_duplicates("TTAP")
        maz_taz_tap = maz_taz.merge(ped_dist, on="TMAZ")
        emmebank = _emme_tools.EmmeManager.emmebank(
            os.path.join(self.root_dir, self.config.emme.transit_database_path))
        # create new output file and write header
        results_path = os.path.join(self.root_dir, r"skim_matrices\transit\drive_access\drive_maz_taz_tap.csv")
        os.makedirs(os.path.dirname(results_path), exist_ok=True)
        with open(results_path, "a", newline="") as output_file:
            output_file.write("FTAZ,MODE,PERIOD,TTAP,TMAZ,TTAZ,DTIME,DDIST,DTOLL,WDIST\n")

        for period in self.config.periods:
            # load Emme network for TAP<->available modes correspondence
            scenario = emmebank.scenario(period.emme_scenario_id)
            network = scenario.get_partial_network(["TRANSIT_LINE", "TRANSIT_SEGMENT"], include_attributes=False)
            # TODO: to be reviewed with new network
            attrs_to_load = {
                "NODE": ["@tap_id"],
                "TRANSIT_LINE": ["#src_mode"],
                "TRANSIT_SEGMENT": ["allow_alightings", "allow_boardings"]
            }
            for domain, attrs in attrs_to_load.items():
                values = scenario.get_attribute_values(domain, attrs)
                network.set_attribute_values(domain, attrs, values)
            mode_name_map = {
                'b': 'LOCAL_BUS',
                'f': 'FERRY_SERVICE',
                'h': 'HEAVY_RAIL',
                'l': 'LIGHT_RAIL',
                'r': 'COMMUTER_RAIL',
                'x': 'EXPRESS_BUS',
            }
            tap_ids = []
            tap_mode_ids = []
            taps_with_no_service = []
            for node in network.nodes():
                if node["@tap_id"] == 0:
                    continue
                stops = set([])
                for link in node.outgoing_links():
                    for next_link in link.j_node.outgoing_links():
                        stops.add(next_link.j_node)
                modes = set([])
                for stop in stops:
                    for seg in stop.outgoing_segments(include_hidden=True):
                        if seg.allow_alightings or seg.allow_boardings:
                            modes.add(mode_name_map[seg.line["#src_mode"]])
                if not modes:
                    taps_with_no_service.append(node["@tap_id"])
                else:
                    tap_mode_ids.extend(modes)
                    tap_ids.extend([node["@tap_id"]]*len(modes))
            tap_modes = pd.DataFrame({"TTAP": tap_ids, "MODE": tap_mode_ids})
            maz_ttaz_tap_modes = maz_taz_tap.merge(tap_modes, on="TTAP")

            skim_src_file = os.path.join(self.root_dir, self.config.highway.output_skim_path.format(period=period.name))
            src_file = _omx.open_file(skim_src_file)
            dist_name = f"{period.name}_da_dist"  # "DISTDA"
            toll_name = f"{period.name}_da_bridgetollda"  # "BTOLLDA"
            time_name = f"{period.name}_da_time"  # "TIMEDA"

            #taz_seq = list(range(1, len(taz_ids) + 1))
            #taz_seq

            root_ids = np.repeat(taz_seq, len(taz_seq))
            leaf_ids = taz_seq * len(taz_seq)
            # TODO: drop externals, use emme_tools omx reader
            drive_costs = pd.DataFrame(
                {
                    "FTAZ": root_ids,
                    "TTAZ": leaf_ids,
                    "DDIST": src_file[dist_name].read()[:4735,:4735].flatten(),
                    "DTOLL": src_file[toll_name].read()[:4735,:4735].flatten(),
                    "DTIME": src_file[time_name].read()[:4735,:4735].flatten(),
                }
            )
            src_file.close()
            # TODO: may need to drop 0's / 1e20 ?
            # drive_costs = drive_costs.query("DTIME > 0 & DTIME < 1e19")

            taz_to_tap_costs = drive_costs.merge(maz_ttaz_tap_modes, on="TTAZ")
            # time + vot*(dist * auto_op_cost + toll)
            # TODO config ... 
            value_of_time = 18.93
            operating_cost_per_mile = 17.23
            auto_op_cost = operating_cost_per_mile  #  / 5280 #correct for feet
            vot = 0.6 / value_of_time #turn into minutes / cents
            walk_speed = 60.0 / 3.0
            taz_to_tap_costs["COST"] = taz_to_tap_costs["DTIME"]  + walk_speed * taz_to_tap_costs["WDIST"] + vot * (auto_op_cost * taz_to_tap_costs["DDIST"] + taz_to_tap_costs["DTOLL"])
            # sort by mode, from taz and gen cost to get the closest TTAP to each FTAZ for each mode, drop subsequent rows
            nearest_tap_costs = taz_to_tap_costs.sort_values(["MODE", "FTAZ", "COST"]).drop_duplicates(["MODE", "FTAZ"])
            nearest_tap_costs["PERIOD"] = period.name
            results = nearest_tap_costs[["FTAZ", "MODE", "PERIOD", "TTAP", "TMAZ", "TTAZ", "DTIME", "DDIST", "DTOLL", "WDIST"]]
            # TODO: data types and trailing decimals
            with open(results_path, "a", newline="") as output_file:
                results.to_csv(output_file, header=False, index=False, float_format='%.5f')
