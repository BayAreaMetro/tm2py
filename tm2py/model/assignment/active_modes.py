"""Geneates shortest path skims for walk and bike at MAZ, TAP or TAZ.

Compute zone-to-zone (root-to-leaf) walk distance and bicycle distances

Note: additional details in class docstring
"""


from contextlib import contextmanager as _context
import numpy as np
import os
import pandas as pd

from tm2py.core.component import Component as _Component, Controller as _Controller
import tm2py.core.emme as _emme_tools
from tm2py.core.logging import LogStartEnd


EmmeScenario = _emme_tools.EmmeScenario
_root_leaf_id_map = {"TAZ": "@taz_id", "TAP": "@tap_id", "MAZ": "@maz_id"}
_subnetwork_id_map = {"walk": "@walk_link", "bike": "@bike_link"}
_counties = [
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


class ActiveModesSkim(_Component):
    """Generate shortest distance skims between network nodes (TAZs, TAPs, or MAZs).

    Details controlled in config.active_modes
        [active_modes]
        emme_scenario_id = 1
        [[active_modes.shortest_path_skims]]
        mode = "walk"
        roots = "MAZ"
        leaves = "MAZ"
        max_dist_miles = 3
        output = "ped_distance_maz_maz.txt"

        Input:  A scenario network containing the attributes


        Output: (1) Shortest path skims in csv format: from_zone,to_zone,dist
                skims\\ped_distance_maz_maz.txt
                skims\\ped_distance_maz_tap.txt
                skims\\bike_distance_maz_maz.txt
                skims\\bike_distance_maz_tap.txt
                skims\\bike_distance_taz_taz.txt
                skims\\ped_distance_tap_tap.txt
    """

    def __init__(self, controller: _Controller):
        """Initialize active mode skim component.

        Args:
            controller: parent Controller object
        """
        super().__init__(controller)
        self._num_processors = _emme_tools.parse_num_processors(
            self.config.emme.num_processors
        )
        self._emme_manager = None
        self._temp_scenario = None
        self._network = None
        self._debug = False

    @LogStartEnd("active mode skim")
    def run(self):
        """Run shortest path skim calculation for active modes."""
        self._emme_manager = _emme_tools.EmmeManager()
        project_path = os.path.join(self.root_dir, self.config.emme.project_path)
        project = self._emme_manager.project(project_path)
        self._emme_manager.init_modeller(project)
        skim_list = self.config.active_modes.shortest_path_skims
        self._prepare_files(skim_list)
        for emmebank_path in self.config.emme.active_database_paths:
            with self._setup(emmebank_path):
                mode_codes = self._prepare_network(skim_list)
                for mode_id, spec in zip(mode_codes, skim_list):
                    log_msg = f"mode={spec['mode']}, roots={spec['roots']}, leaves={spec['leaves']}"
                    self.logger.log_time(f"skim for {log_msg}", indent=True)
                    with self._emme_manager.logbook_trace(log_msg):
                        for county in _counties:
                            with self.logger.log_start_end(f"county={county}"):
                                roots, leaves = self._prepare_roots_leaves(
                                    spec["roots"], spec["leaves"], county
                                )
                                self.logger.log_time(f"num roots={len(roots)}, num leaves={len(leaves)}", indent=True)
                                if not roots or not leaves:
                                    continue
                                distance_skim = self._run_shortest_path(
                                    mode_id, spec.get("max_dist_miles")
                                )
                                self._export_results(distance_skim, spec["output"], roots, leaves)

    @_context
    def _setup(self, emmebank_path):
        """Create temp scenario for setting of modes on links and roots and leaves.

        Temp scenario is deleted on exit.
        """
        msg = f"Active modes shortest path skims {emmebank_path}"
        with self._emme_manager.logbook_trace(msg):
            with self.logger.log_start_end(msg):
                emmebank = self._emme_manager.emmebank(
                    os.path.join(self.root_dir, emmebank_path))
                min_dims = emmebank.min_dimensions
                required_dims = {
                    "scenarios": 2,
                    "links": min(int(min_dims["links"] * 1.1), 2000000),
                    "extra_attribute_values": int(min_dims["extra_attribute_values"] * 1.5)
                }
                self._emme_manager.change_emmebank_dimensions(emmebank, required_dims)
                src_scenario = emmebank.scenario(self.config.active_modes.emme_scenario_id)
                for avail_id in range(9999, 1, -1):
                    if not emmebank.scenario(avail_id):
                        break
                self._temp_scenario = emmebank.create_scenario(avail_id)
                try:
                    self._temp_scenario.has_traffic_results = src_scenario.has_traffic_results
                    self._temp_scenario.has_transit_results = src_scenario.has_transit_results
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
                        "NODE": list(_root_leaf_id_map.values())
                        + ["@roots", "@leaves", "#node_county", "x", "y"],
                        "LINK": list(_subnetwork_id_map.values()) + ["length"],
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
                    if not self._debug:
                        emmebank.delete_scenario(self._temp_scenario)
                        self._network = None

    def _prepare_files(self, skim_list):
        """Clear all output files and write new headers."""
        for spec in skim_list:
            file_path = os.path.join(self.root_dir, spec["output"])
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, "w") as output_file:
                output_file.write("from_zone,to_zone,shortest_path_cost\n")

    def _prepare_network(self, skim_list):
        """Setup network modes, link adjustments for walk and bike skims.

        Set link modes for skim root, leaf and access mode combinations.
        Delete highway only links.
        Add reverse links for walk mode.
        """
        network = self._network

        # create reverse link for one-way walk links
        # also removed  walk and bike access from connectors
        # TODO: note that the TAZ, MAZ, TAP connectors have walk or bike access removed
        #       problem that we can't tell non-walk connectors apart (probably not a real problem)
        for link in network.links():
            if link[_subnetwork_id_map["walk"]] and not link.reverse_link:
                reverse = network.create_link(link.j_node, link.i_node, link.modes)
                reverse.length = link.length
                reverse.vertices = link.vertices
                reverse[_subnetwork_id_map["walk"]] = 1
            for attr in _root_leaf_id_map.values():
                if link.j_node[attr] or link.i_node[attr]:
                    for access_attr in _subnetwork_id_map.values():
                        link[access_attr] = 0

        # create new modes for each skim: set node TAZ, MAZ, TAP attr to find connectors
        # note that the TAZ, MAZ, TAP connectors must not have walk or bike access
        # in order to prevent "shortcutting" via zones in shortest path building (removed above)
        mode_codes = []
        for spec in skim_list:
            mode = network.create_mode("AUX_AUTO", network.available_mode_identifier())
            mode_id_set = set([mode.id])
            mode_codes.append(mode.id)
            # get network attribute names from parameters
            root_attr = _root_leaf_id_map[spec["roots"]]  # TAZ, TAP or MAZ as root (origin)?
            leaf_attr = _root_leaf_id_map[spec["leaves"]]  # TAZ, TAP or MAZ as leaf (dest)?
            network_attr = _subnetwork_id_map[spec["mode"]]  # walk or bike mode
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

    def _prepare_roots_leaves(self, root_type, leaf_type, county=None):
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
                node["@roots"] = node[_root_leaf_id_map[root_type]]
            node["@leaves"] = node[_root_leaf_id_map[leaf_type]]
            if node["@roots"]:
                roots.append(int(node["@roots"]))
            if node["@leaves"]:
                leaves.append(int(node["@leaves"]))
        # save root and leaf IDs back to scenario for SP calc
        values = self._network.get_attribute_values("NODE", ["@roots", "@leaves"])
        self._temp_scenario.set_attribute_values("NODE", ["@roots", "@leaves"], values)
        return roots, leaves

    def _run_shortest_path(self, mode_code, max_dist):
        """Run Emme shortest path tool to get numpy array of distances."""
        shortest_paths = self._emme_manager.modeller.tool(
            "inro.emme.network_calculation.shortest_path"
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
                "exclude_forbidden_turns": False
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
                            "description": ""
                        },
                    ]
                }
            },
            "performance_settings": {
                "number_of_processors": self._num_processors,
                "direction": "AUTO",
                "method": "STANDARD"
            }
        }
        results = shortest_paths(spec, scenario=self._temp_scenario)
        return results["distance"]

    def _export_results(self, distance_skim, output, roots, leaves):
        """Export the distance skims for valid root/leaf pairs to csv."""
        # get the sequence of root / leaf (orig / dest) IDs
        root_ids = np.repeat(roots, len(leaves))
        leaf_ids = leaves * len(roots)
        distances = pd.DataFrame(
            {
                "root_ids": root_ids,
                "leaf_ids": leaf_ids,
                "dist": distance_skim.flatten(),
            }
        )
        # drop 0's / 1e20
        distances = distances.query("dist > 0 & dist < 1e19")
        # write remaining values to text file (append)
        with open(os.path.join(self.root_dir, output), "a", newline="") as output_file:
            distances.to_csv(output_file, header=False, index=False, float_format='%.5f')
