"""Geneates shortest path skims for walk and bike at MAZ, TAP or TAZ.

Compute zone-to-zone (root-to-leaf) walk distance and bicycle distances

Note: additional details in class docstring
"""

from __future__ import annotations

import os
from contextlib import contextmanager as _context
from typing import TYPE_CHECKING, List, Tuple

import pandas as pd
from numpy import array as NumpyArray
from numpy import repeat

from tm2py.components.component import Component
from tm2py.logger import LogStartEnd
from tm2py.tools import parse_num_processors

if TYPE_CHECKING:
    from tm2py.controller import RunController


ROOT_LEAF_ID_MAP = {"TAZ": "@taz_id", "TAP": "@tap_id", "MAZ": "@maz_id"}
SUBNETWORK_ID_MAP = {"walk": "@walk_link", "bike": "@bike_link"}
COUNTIES = [
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


class ActiveModesSkim(Component):
    """Generate shortest distance skims between network nodes (TAZs, TAPs, or MAZs).

    Details controlled in config.active_modes, e.g.:
        [active_modes]
        emme_scenario_id = 1
        [[active_modes.shortest_path_skims]]
        mode = "walk"
        roots = "MAZ"
        leaves = "MAZ"
        max_dist_miles = 3
        output = "skims\\ped_distance_maz_maz.txt"

        Input:  A scenario network containing the attributes


        Output: (1) Shortest path skims in csv format: from_zone,to_zone,dist
                skims\\ped_distance_maz_maz.txt
                skims\\ped_distance_maz_tap.txt
                skims\\bike_distance_maz_maz.txt
                skims\\bike_distance_maz_tap.txt
                skims\\bike_distance_taz_taz.txt
                skims\\ped_distance_tap_tap.txt

    Internal properties:
        _temp_scenario: temporary Emme scenario, deleted when component completes
        _network: in-memory network object
    """

    def __init__(self, controller: RunController):
        """Initialize active mode skim component.

        Args:
            controller: parent Controller object
        """
        super().__init__(controller)
        self.config = self.controller.config.active_modes
        self._temp_scenario = None
        self._network = None

    def validate_inputs(self):
        """Validate inputs files are correct, raise if an error is found."""
        # TODO
        pass

    @LogStartEnd("active mode skim")
    def run(self):
        """Run shortest path skim calculation for active modes."""
        skim_list = self.config.shortest_path_skims
        self._prepare_files()
        for emmebank_path in [
            self.controller.config.emme.active_south_database_path,
            self.controller.config.emme.active_north_database_path,
        ]:
            with self._setup(emmebank_path):
                mode_codes = self._prepare_network()
                for mode_id, spec in zip(mode_codes, skim_list):
                    for county in COUNTIES:
                        log_msg = (
                            f"skim for mode={spec['mode']}, roots={spec['roots']}, "
                            f"leaves={spec['leaves']} county={county}"
                        )
                        with self.logger.log_start_end(log_msg, level="DETAIL"):
                            roots, leaves = self._prepare_roots_leaves(
                                spec["roots"], spec["leaves"], county
                            )
                            if not roots or not leaves:
                                continue
                            distance_skim = self._run_shortest_path(
                                mode_id, spec.get("max_dist_miles")
                            )
                            self._export_results(
                                distance_skim, spec["output"], roots, leaves
                            )

    @_context
    def _setup(self, emmebank_path: str):
        """Create temp scenario for setting of modes on links and roots and leaves.

        Temp scenario is deleted on exit.
        """
        log_msg = f"Active modes shortest path skims {emmebank_path}"
        manager = self.controller.emme_manager
        with self.logger.log_start_end(log_msg, level="INFO"):
            if emmebank_path == self.controller.config.emme.active_north_database_path:
                emmebank = manager.active_north_emmebank
            else:
                emmebank = manager.active_south_emmebank
            min_dims = emmebank.emmebank.min_dimensions
            required_dims = {
                "scenarios": 2,
                "links": min(int(min_dims["links"] * 1.1), 2000000),
                "extra_attribute_values": int(min_dims["extra_attribute_values"] * 1.5),
            }
            emmebank.change_dimensions(required_dims)
            src_scenario = emmebank.emmebank.scenario(self.config.emme_scenario_id)
            for avail_id in range(9999, 1, -1):
                if not emmebank.emmebank.scenario(avail_id):
                    break
            self._temp_scenario = emmebank.emmebank.create_scenario(avail_id)
            try:
                self._temp_scenario.has_traffic_results = (
                    src_scenario.has_traffic_results
                )
                self._temp_scenario.has_transit_results = (
                    src_scenario.has_transit_results
                )
                # Load network topology from disk (scenario in emmebank)
                # Note: optimization to use get_partial_network, nodes and links
                #       only (instead of get_network), followed by loading
                #       only attribute values of interest (get_attribute_values)
                # self._network = src_scenario.get_partial_network(
                #     ["NODE", "LINK"], include_attributes=False
                # )
                self._network = src_scenario.get_network()
                # Attributes which are used in any step in this component
                # If additional attributes are required they must be added
                # to this list
                used_attributes = {
                    "NODE": list(ROOT_LEAF_ID_MAP.values())
                    + ["@roots", "@leaves", "#node_county", "x", "y"],
                    "LINK": list(SUBNETWORK_ID_MAP.values()) + ["length"],
                    "TURN": [],
                    "TRANSIT_LINE": [],
                    "TRANSIT_SEGMENT": [],
                }
                for domain, attrs in used_attributes.items():
                    attrs_to_load = []
                    # create required attributes in temp scenario and network object
                    for attr_id in attrs:
                        if attr_id not in self._network.attributes(domain):
                            # create attributes which do not exist
                            self._network.create_attribute(domain, attr_id)
                        else:
                            # only load attributes which already exist
                            attrs_to_load.append(attr_id)
                        if attr_id.startswith("@"):
                            self._temp_scenario.create_extra_attribute(domain, attr_id)
                        if attr_id.startswith("#"):
                            self._temp_scenario.create_network_field(
                                domain, attr_id, "STRING"
                            )
                    # load required attribute values from disk to network object
                    values = src_scenario.get_attribute_values(domain, attrs_to_load)
                    self._network.set_attribute_values(domain, attrs_to_load, values)

                    # delete unused extra attributes/ network field from network object
                    for attr_id in self._network.attributes(domain):
                        if attr_id not in attrs and attr_id.startswith(("@", "#")):
                            self._network.delete_attribute(domain, attr_id)
                self._network.publishable = True
                yield
            finally:
                emmebank.emmebank.delete_scenario(self._temp_scenario)
                self._network = None

    def _prepare_files(self):
        """Clear all output files and write new headers."""
        for skim_spec in self.config.shortest_path_skims:
            file_path = self.get_abs_path(skim_spec["output"])
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, "w", encoding="utf8") as output_file:
                pass
            # java expects no headers
            # from_zone,to_zone,to_zone,shortest_path_generalized_cost,shortest_path_distance_feet

    def _prepare_network(self):
        """Setup network modes, link adjustments for walk and bike skims.

        Set link modes for skim root, leaf and access mode combinations.
        Delete highway only links.
        Add reverse links for walk mode.
        """
        network = self._network

        # create reverse link for one-way walk links
        # also removed walk and bike access from connectors
        for link in network.links():
            if link[SUBNETWORK_ID_MAP["walk"]] and not link.reverse_link:
                reverse = network.create_link(link.j_node, link.i_node, link.modes)
                reverse.length = link.length
                reverse.vertices = link.vertices
                reverse[SUBNETWORK_ID_MAP["walk"]] = 1
            for attr in ROOT_LEAF_ID_MAP.values():
                if link.j_node[attr] or link.i_node[attr]:
                    for access_attr in SUBNETWORK_ID_MAP.values():
                        link[access_attr] = 0

        # create new modes for each skim: set node TAZ, MAZ, TAP attr to find connectors
        # note that the TAZ, MAZ, TAP connectors must not have walk or bike access
        # in order to prevent "shortcutting" via zones in shortest path building (removed above)
        mode_codes = []
        for spec in self.config.shortest_path_skims:
            mode = network.create_mode("AUX_AUTO", network.available_mode_identifier())
            mode_id_set = {mode.id}
            mode_codes.append(mode.id)
            # get network attribute names from parameters
            root_attr = ROOT_LEAF_ID_MAP[
                spec["roots"]
            ]  # TAZ, TAP or MAZ as root (origin)?
            leaf_attr = ROOT_LEAF_ID_MAP[
                spec["leaves"]
            ]  # TAZ, TAP or MAZ as leaf (dest)?
            network_attr = SUBNETWORK_ID_MAP[spec["mode"]]  # walk or bike mode
            # define network access and egress to "zones" and subnetwork
            # by setting the link.modes
            for link in network.links():
                if (
                    link.j_node[leaf_attr]
                    or link.i_node[root_attr]
                    or link[network_attr]
                ):
                    link.modes |= mode_id_set
        self._temp_scenario.publish_network(network)
        return mode_codes

    def _prepare_roots_leaves(
        self, root_type: str, leaf_type: str, county: str = None
    ) -> Tuple[List[int], List[int]]:
        """Set @roots and @leaves values for orig/dest nodes.

        Also return sequence of root and leaf IDs to match index for shortest
        path numpy array.
        """
        roots = []
        leaves = []
        for node in self._network.nodes():
            # filter to only origins by county (if used)
            if county and node["#node_county"] != county:
                node["@roots"] = 0
            else:
                node["@roots"] = node[ROOT_LEAF_ID_MAP[root_type]]
            node["@leaves"] = node[ROOT_LEAF_ID_MAP[leaf_type]]
            if node["@roots"]:
                roots.append(int(node["@roots"]))
            if node["@leaves"]:
                leaves.append(int(node["@leaves"]))
        # save root and leaf IDs back to scenario for SP calc
        values = self._network.get_attribute_values("NODE", ["@roots", "@leaves"])
        self._temp_scenario.set_attribute_values("NODE", ["@roots", "@leaves"], values)
        self.logger.log_time(
            f"num roots={len(roots)}, num leaves={len(leaves)}", level="DEBUG"
        )
        return roots, leaves

    def _run_shortest_path(self, mode_code: str, max_dist: float) -> NumpyArray:
        """Run Emme shortest path tool to get numpy array of distances."""
        shortest_paths = self.controller.emme_manager.tool(
            "inro.emme.network_calculation.shortest_path"
        )
        num_processors = parse_num_processors(
            self.controller.config.emme.num_processors
        )
        spec = {
            "type": "SHORTEST_PATH",
            "modes": [mode_code],
            "root_nodes": "@roots",
            "leaf_nodes": "@leaves",
            "link_cost": "length",
            "path_constraints": {
                "max_cost": max_dist,
                "uturn_allowed": False,
                "through_leaves": False,
                "through_centroids": False,
                "exclude_forbidden_turns": False,
            },
            "results": {
                "skim_output": {
                    "format": "OMX",
                    "return_numpy": True,
                    "analyses": [
                        {
                            "component": "SHORTEST_PATH_COST",
                            "operator": "+",
                            "name": "distance",
                            "description": "",
                        },
                    ],
                }
            },
            "performance_settings": {
                "number_of_processors": num_processors,
                "direction": "AUTO",
                "method": "STANDARD",
            },
        }
        results = shortest_paths(spec, scenario=self._temp_scenario)
        return results["distance"]

    def _export_results(
        self,
        distance_skim: NumpyArray,
        output: str,
        roots: List[int],
        leaves: List[int],
    ):
        """Export the distance skims for valid root/leaf pairs to csv."""
        # get the sequence of root / leaf (orig / dest) IDs
        root_ids = repeat(roots, len(leaves))
        leaf_ids = leaves * len(roots)
        distances = pd.DataFrame(
            {
                "root_ids": root_ids,
                "leaf_ids": leaf_ids,
                "leaf_ids_2": leaf_ids,
                "dist": distance_skim.flatten(),
                "dist_feet": distance_skim.flatten() * 5280,
            }
        )
        # convert node id to sequential (1-based) zone id
        # consistent with tm2.1 - java expects this
        zone_seq_file = self.get_abs_path(self.controller.config.scenario.zone_seq_file)
        zone_seq_df = pd.read_csv(zone_seq_file)
        taz_seq = dict(
            zip(
                zone_seq_df[zone_seq_df.TAZSEQ > 0].N,
                zone_seq_df[zone_seq_df.TAZSEQ > 0].TAZSEQ,
            )
        )
        maz_seq = dict(
            zip(
                zone_seq_df[zone_seq_df.MAZSEQ > 0].N,
                zone_seq_df[zone_seq_df.MAZSEQ > 0].MAZSEQ,
            )
        )
        tap_seq = dict(
            zip(
                zone_seq_df[zone_seq_df.TAPSEQ > 0].N,
                zone_seq_df[zone_seq_df.TAPSEQ > 0].TAPSEQ,
            )
        )
        ext_seq = dict(
            zip(
                zone_seq_df[zone_seq_df.EXTSEQ > 0].N,
                zone_seq_df[zone_seq_df.EXTSEQ > 0].EXTSEQ,
            )
        )
        taz_seq = {**taz_seq, **ext_seq}
        for c in ["root_ids", "leaf_ids", "leaf_ids_2"]:
            taz_bool = distances[c].isin(list(taz_seq.keys()))
            maz_bool = distances[c].isin(list(maz_seq.keys()))
            tap_bool = distances[c].isin(list(tap_seq.keys()))
            if taz_bool.any():
                distances[c] = distances[c].map(taz_seq)
                continue
            elif maz_bool.any():
                distances[c] = distances[c].map(maz_seq)
                continue
            elif tap_bool.any():
                distances[c] = distances[c].map(tap_seq)
                continue
            else:
                raise Exception(
                    "{} has N values not in the {} file".format(c, zone_seq_file)
                )
        # drop 0's / 1e20
        distances = distances.query("dist > 0 & dist < 1e19")
        # write remaining values to text file (append)
        with open(
            self.get_abs_path(output), "a", newline="", encoding="utf8"
        ) as output_file:
            distances.to_csv(
                output_file, header=False, index=False, float_format="%.5f"
            )
