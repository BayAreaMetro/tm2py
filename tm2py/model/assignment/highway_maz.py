"""Assigns MAZ-to-MAZ demand along shortest generalized cost path for nearby trips.

 MAZ to MAZ demand is read in 3 separate OMX matrices are as follows:
         (1) counties 1, 2, and 3 ("San Francisco", "San Mateo", "Santa Clara")
         (2) counties 4 and 5 ("Alameda", "Contra Costa")
         (3) counties 6, 7, 8, and 9 ("Solano", "Napa", "Sonoma", "Marin")

The demand is expected to be short distance (e.g. <0.5 miles), or within the
same TAZ. The demand is grouped into bins of origin -> all destinations, by
distance (straight-line) to furthest destination. This limits the size of the
shortest path calculated to the minimum required.
The bin edges have been predefined after testing as (in miles):
    [0.0, 0.9, 1.2, 1.8, 2.5, 5.0, 10.0, max_dist]


Input:
Emme network with:
    Link attributes:
        - time attribute, either timau (resulting VDF congested time)
          or @free_flow_time
    Node attributes: @maz_id, x, y, and #county
Demand matrices under /demand_matrices/highway/maz_demand"
    auto_{period}_MAZ_AUTO_1_{period}.omx
    auto_{period}_MAZ_AUTO_2_{period}.omx
    auto_{period}_MAZ_AUTO_3_{period}.omx
    NOTE: demand structured is fixed in current version
          this will be reviewed

Output:
The resulting MAZ-MAZ flows are saved in link data1 ("ul1") which is
used as background traffic in the general Highway assignment.
"""

import array as _array
from collections import defaultdict as _defaultdict
from contextlib import contextmanager as _context
from math import sqrt as _sqrt
import numpy as np
import os
import pandas as pd
import time as _time
from typing import List


from tm2py.core.component import Component as _Component, Controller as _Controller
import tm2py.core.emme as _emme_tools
from tm2py.core.logging import LogStartEnd


EmmeScenario = _emme_tools.EmmeScenario
_default_bin_edges = [0.0, 0.9, 1.2, 1.8, 2.5, 5.0, 10.0]
# Grouping of output demand files for MAZ-MAZ pairs
_county_sets = {
    1: ["San Francisco", "San Mateo", "Santa Clara"],
    2: ["Alameda", "Contra Costa"],
    3: ["Solano", "Napa", "Sonoma", "Marin"],
}
_counties = [
    "San Francisco", "San Mateo", "Santa Clara",
    "Alameda", "Contra Costa",
    "Solano", "Napa", "Sonoma", "Marin"
]
# Using text file format for now, can upgrade to binary format (faster) once
# compatiility with new networks is verified
_use_binary = False


class AssignMAZSPDemand(_Component):
    """MAZ-to-MAZ shortest-path highway assignment.

    Calculates shortest path between MAZs with demand in the Emme network
    and assigns flow.
    """

    # skip Too many instance attributes recommendation, it is OK as is
    # pylint: disable=R0902

    def __init__(
        self,
        controller: _Controller,
    ):
        """MAZ-to-MAZ shortest-path highway assignment.

        Args:
            controller: parent Controller object
        """
        super().__init__(controller)
        self._scenario = None
        # bins: performance parameter: crow-fly distance bins
        #       to limit shortest path calculation by origin to furthest destination
        #       semi-exposed for performance testing
        self._bin_edges = _default_bin_edges
        self._debug_report = []
        self._debug = False

        # Internal attributes to track data through the sequence of steps
        self._eb_dir = None
        self._modeller = None
        self._net_calc = None
        self._mazs = None
        self._demand = None
        self._max_dist = 0
        self._network = None
        self._root_index = None
        self._leaf_index = None

    def initialize_flow(self):
        emme_manager = _emme_tools.EmmeManager()
        emme_manager.project(os.path.join(self.root_dir, self.config.emme.project_path))
        modeller = emme_manager.modeller
        emmebank = emme_manager.emmebank(os.path.join(self.root_dir, self.config.emme.highway_database_path))
        for period in self.config.periods:
            scenario = emmebank.scenario(period.emme_scenario_id)
            net_calc = _emme_tools.NetworkCalculator(scenario, modeller)
            net_calc("ul1", "0")

    @LogStartEnd()
    def run(self):
        """Run MAZ-to-MAZ shortest path assignment."""
        emme_manager = _emme_tools.EmmeManager()
        emme_manager.project(os.path.join(self.root_dir, self.config.emme.project_path))
        self._modeller = emme_manager.modeller
        emmebank = emme_manager.emmebank(os.path.join(self.root_dir, self.config.emme.highway_database_path))
        self._eb_dir = os.path.dirname(emmebank.path)
        for period in self.config.periods:
            with self.logger.log_start_end(f"period {period.name}"):
                self._scenario = emmebank.scenario(period.emme_scenario_id)
                self._net_calc = _emme_tools.NetworkCalculator(self._scenario, self._modeller)
                # NOTE: demand structure to be reviewed
                file_path_tmplt = os.path.join(
                    self.root_dir, self.config.highway.maz_to_maz.input_maz_highway_demand_file)
                with self._setup(period.name):
                    self._prepare_network()
                    for i, names in _county_sets.items():
                        maz_ids = self._get_county_mazs(names)
                        omx_file_path = os.path.join(self.root_dir, file_path_tmplt.format(period=period.name, number=i))
                        with _emme_tools.OMX(omx_file_path, "r") as omx_file:
                            # NOTE: OMX matrices to be updated by WSP
                            demand_array = omx_file.read_hdf5("/matrices/M0")
                        self._process_demand(demand_array, maz_ids)
                        del demand_array

                    demand_bins = self._group_demand()
                    for i, demand_group in enumerate(demand_bins):
                        self._find_roots_and_leaves(demand_group["demand"])
                        self._run_shortest_path(i, demand_group["dist"], period.name)
                        self._assign_flow(i, demand_group["demand"], period.name)

    @_context
    def _setup(self, period):
        self._mazs = None
        self._demand = _defaultdict(lambda: [])
        self._max_dist = 0
        self._network = None
        self._root_index = None
        self._leaf_index = None
        self._debug_report = []
        try:
            yield
        # except Exception as error:
        #     print(error)
        #     import traceback
        #     traceback.print_exc()
        finally:
            if not self._debug:
                for attr in ["@link_cost_maz", "@maz_root", "@maz_leaf", "@link_cost"]:
                    self._scenario.delete_extra_attribute(attr)
                self._mazs = None
                self._demand = None
                self._network = None
                self._root_index = None
                self._leaf_index = None
                # delete sp path files
                for bin_no in range(len(self._bin_edges)):
                    file_path = os.path.join(self._eb_dir, f"sp_{period}_{bin_no}.ebp")
                    if os.path.exists(file_path):
                        try:
                            os.remove(file_path)
                        except Exception as error:
                            print(f"Error removing file sp_{period}_{bin_no}.ebp")
                            print(error)
            else:
                for item in self._debug_report:
                    self.logger.log(item)

    def _prepare_network(self):
        start_time = _time.time()
        modeller = self._modeller
        create_attribute = modeller.tool(
            "inro.emme.data.extra_attribute.create_extra_attribute"
        )
        # TODO: internal (temp) attbribute names may have a name conflict
        #       could auto-generate unique names on conflict
        attributes = [
            ("LINK", "@link_cost", "total cost MAZ-MAZ"),
            ("LINK", "@link_cost_maz", "cost MAZ-MAZ, unused MAZs blocked"),
            ("NODE", "@maz_root", "Flag for MAZs which are roots"),
            ("NODE", "@maz_leaf", "Flag for MAZs which are leaves"),
        ]
        for domain, name, desc in attributes:
            create_attribute(domain, name, desc, overwrite=True, scenario=self._scenario)
        if self._scenario.has_traffic_results:
            # TODO: for later review, this should not be necessary
            time_attr = "(@free_flow_time.max.timau)"
        else:
            time_attr = "@free_flow_time"
        vot = self.config.highway.maz_to_maz.value_of_time
        op_cost = self.config.highway.maz_to_maz.operating_cost_per_mile
        self._net_calc(
            "@link_cost", f"{time_attr} + 0.6 / {vot} * (length * {op_cost})"
        )
        self._net_calc("ul1", "0")
        self._network = self._scenario.get_partial_network(
            ["NODE", "LINK"], include_attributes=False
        )
        self._network.create_attribute("LINK", "temp_flow")
        attrs_to_read = [("NODE", ["@maz_id", "x", "y", "#county"])]
        for domain, attrs in attrs_to_read:
            self._read_attr_values(domain, attrs)
        self._debug_report.append(
            "    PREPARE NETWORK --- %.3f seconds ---" % (_time.time() - start_time)
        )

    def _get_county_mazs(self, counties):
        network = self._network
        # NOTE: every maz must have a valid #county
        if self._mazs is None:
            self._mazs = _defaultdict(lambda: [])
            for node in network.nodes():
                if node["@maz_id"]:
                    self._mazs[node["#county"]].append(node)
        mazs = []
        for county in counties:
            mazs.extend(self._mazs[county])
        return sorted(mazs, key=lambda n: n["@maz_id"])

    def _process_demand(self, data, maz_ids):
        start_time = _time.time()
        origins, destinations = data.nonzero()
        for orig, dest in zip(origins, destinations):
            # skip intra-maz demand
            if orig == dest:
                continue
            orig_node = maz_ids[orig]
            dest_node = maz_ids[dest]
            dist = _sqrt(
                (dest_node.x - orig_node.x) ** 2 + (dest_node.y - orig_node.y) ** 2
            )
            if dist > self._max_dist:
                self._max_dist = dist
            self._demand[orig_node].append(
                {
                    "orig": orig_node,
                    "dest": dest_node,
                    "dem": data[orig][dest],
                    "dist": dist,
                }
            )
        self._debug_report.append(
            "    PROCESS DEMAND --- %.3f seconds ---" % (_time.time() - start_time)
        )

    def _group_demand(self):
        start_time = _time.time()
        # group demand from same origin into distance bins by furthest
        # distance destination to limit shortest path search radius
        bin_edges = self._bin_edges[:]
        if bin_edges[-1] < self._max_dist / 5280.0:
            bin_edges.append(self._max_dist / 5280.0)

        demand_groups = [
            {"dist": edge, "demand": []} for i, edge in enumerate(bin_edges[1:])
        ]
        for data in self._demand.values():
            max_dist = max(entry["dist"] for entry in data) / 5280.0
            for group in demand_groups:
                if max_dist < group["dist"]:
                    group["demand"].extend(data)
                    break
        for group in demand_groups:
            self._debug_report.append(
                "       bin dist %.1f, size %s" % (group["dist"], len(group["demand"]))
            )
        # Filter out groups without any demend
        demand_groups = [group for group in demand_groups if group["demand"]]
        self._debug_report.append(
            "    GROUP DEMAND --- %.3f seconds ---" % (_time.time() - start_time)
        )
        return demand_groups

    def _find_roots_and_leaves(self, demand):
        start_time = _time.time()
        network = self._network
        attrs_to_init = [("NODE", ["@maz_root", "@maz_leaf"]), ("LINK", ["maz_cost"])]
        for domain, attrs in attrs_to_init:
            for name in attrs:
                if name in network.attributes(domain):
                    network.delete_attribute(domain, name)
                network.create_attribute(domain, name)
        root_maz_ids = {}
        leaf_maz_ids = {}
        for data in demand:
            o_node, d_node = data["orig"], data["dest"]
            root_maz_ids[o_node.number] = o_node["@maz_root"] = o_node["@maz_id"]
            leaf_maz_ids[d_node.number] = d_node["@maz_leaf"] = d_node["@maz_id"]
        self._root_index = {p: i for i, p in enumerate(sorted(root_maz_ids.keys()))}
        self._leaf_index = {q: i for i, q in enumerate(sorted(leaf_maz_ids.keys()))}
        self._save_attr_values("NODE", ["@maz_root", "@maz_leaf"])
        # forbid egress from MAZ nodes which are not demand roots /
        #        access to MAZ nodes which are not demand leafs
        self._net_calc.add_calc("@link_cost_maz", "@link_cost")
        self._net_calc.add_calc("@link_cost_maz", "1e20", "@maz_root=0 and !@maz_id=0")
        self._net_calc.add_calc(
            "@link_cost_maz", "1e20", "@maz_leafj=0 and !@maz_idj=0"
        )
        self._net_calc.run()
        self._debug_report.append(
            "    FIND ROOTS&LEAVES --- %.3f seconds ---" % (_time.time() - start_time)
        )

    def _run_shortest_path(self, bin_no, max_radius, period):
        start_time = _time.time()
        shortest_paths_tool = self._modeller.tool(
            "inro.emme.network_calculation.shortest_path"
        )
        max_radius = max_radius * 5280 + 100  # add some buffer for rounding error
        file_name = f"sp_{period}_{bin_no}.ebp"
        num_processors = _emme_tools.parse_num_processors(
            self.config.emme.num_processors)
        spec = {
            "type": "SHORTEST_PATH",
            "modes": [self.config.highway.maz_to_maz.mode_code],
            "root_nodes": "@maz_root",
            "leaf_nodes": "@maz_leaf",
            "link_cost": "@link_cost_maz",
            "path_constraints": {
                "max_radius": max_radius,
                "uturn_allowed": False,
                "through_leaves": False,
                "through_centroids": False,
                "exclude_forbidden_turns": False
            },
            "results": {
                "skim_output": {
                    "file": "",
                    "format": "TEXT",
                    "return_numpy": False,
                    "analyses": []
                },
                "path_output": {
                    "format": "BINARY" if _use_binary else "TEXT",
                    "file": os.path.join(self._eb_dir, file_name)
                }
            },
            "performance_settings": {
                "number_of_processors": num_processors,
                "direction": "FORWARD",
                "method": "STANDARD"
            }
        }
        shortest_paths_tool(spec, self._scenario)
        self._debug_report.append(
            "    RUN SP %s, %s --- %.3f seconds ---"
            % (bin_no, max_radius / 5280, _time.time() - start_time)
        )

    def _assign_flow(self, bin_no, demand, period):
        if _use_binary:
            self._assign_flow_binary(bin_no, demand, period)
        else:
            self._assign_flow_text(bin_no, demand, period)

    def _assign_flow_text(self, bin_no, demand, period):
        start_time = _time.time()
        file_name = f"sp_{period}_{bin_no}.ebp"
        paths = _defaultdict(lambda: {})
        with open(os.path.join(self._eb_dir, file_name), "r") as paths_file:
            for line in paths_file:
                nodes = [int(x) for x in line.split()]
                paths[nodes[0]][nodes[-1]] = nodes[1:]
        not_assigned, assigned = 0, 0
        for data in demand:
            orig, dest, dem = data["orig"].number, data["dest"].number, data["dem"]
            path = paths.get(orig, {}).get(dest)
            if path is None:
                not_assigned += dem
                continue
            i_node = orig
            for j_node in path:
                link = self._network.link(i_node, j_node)
                link["temp_flow"] += dem
                i_node = j_node
            assigned += dem

        self._debug_report.append(
            "    ASSIGN bin %s, total %s, assign %s, not assign %s  --- %.3f seconds ---"
            % (
                bin_no,
                len(demand),
                assigned,
                not_assigned,
                _time.time() - start_time,
            )
        )

    def _assign_flow_binary(self, bin_no, demand, period):
        start_time = _time.time()
        file_name = f"sp_{period}_{bin_no}.ebp"
        with open(os.path.join(self._eb_dir, file_name), "rb") as paths_file:
            # read set of path pointers by Orig-Dest sequence from file
            offset, leafs_nb, path_indicies = self._get_path_indices(paths_file)
            assigned = 0
            not_assigned = 0
            bytes_read = offset * 8
            # for all orig-dest pairs with demand, load path from file
            for data in demand:
                # get file position based on orig-dest index
                start, end = self._get_path_location(
                    data["orig"].number, data["dest"].number, leafs_nb, path_indicies
                )
                # no path found, disconnected zone
                if start == end:
                    not_assigned += data["dem"]
                    continue
                paths_file.seek(start * 4 + offset * 8)
                self._assign_path_flow(paths_file, start, end, data["dem"])
                assigned += data["dem"]
                bytes_read += (end - start) * 4

        self._debug_report.append(
            "    ASSIGN bin %s, total %s, assign %s, not assign %s, bytes %s  --- %.3f seconds ---"
            % (
                bin_no,
                len(demand),
                assigned,
                not_assigned,
                bytes_read,
                _time.time() - start_time,
            )
        )
        self._save_attr_values("LINK", ["temp_flow"], ["data1"])

    @staticmethod
    def _get_path_indices(paths_file):
        # read first 4 integers from file (Q=64-bit unsigned integers)
        header = _array.array("Q")
        header.fromfile(paths_file, 4)
        roots_nb, leafs_nb = header[2:4]
        # Load sequence of path indices (positions by orig-dest index),
        # pointing to list of path node IDs in file
        path_indicies = _array.array("Q")
        path_indicies.fromfile(paths_file, roots_nb * leafs_nb + 1)
        offset = roots_nb * leafs_nb + 1 + 4
        return offset, leafs_nb, path_indicies

    def _get_path_location(self, orig, dest, leafs_nb, path_indicies):
        p_index = self._root_index[orig]
        q_index = self._leaf_index[dest]
        index = p_index * leafs_nb + q_index
        start = path_indicies[index]
        end = path_indicies[index + 1]
        return start, end

    def _assign_path_flow(self, paths_file, start, end, demand):
        # load sequence of Node IDs which define the path (L=32-bit unsigned integers)
        path = _array.array("L")
        path.fromfile(paths_file, end - start)
        # proccess path to sequence of links and add flow
        path_iter = iter(path)
        i_node = next(path_iter)
        for j_node in path_iter:
            link = self._network.link(i_node, j_node)
            link["temp_flow"] += demand
            i_node = j_node

    def _read_attr_values(self, domain, src_names, dst_names=None):
        self._copy_attr_values(
            domain, self._scenario, self._network, src_names, dst_names
        )

    def _save_attr_values(self, domain, src_names, dst_names=None):
        self._copy_attr_values(
            domain, self._network, self._scenario, src_names, dst_names
        )

    @staticmethod
    def _copy_attr_values(domain, src, dst, src_names, dst_names=None):
        if dst_names is None:
            dst_names = src_names
        values = src.get_attribute_values(domain, src_names)
        dst.set_attribute_values(domain, dst_names, values)


class SkimMAZCosts(_Component):

    def __init__(
        self,
        controller: _Controller
    ):
        """MAZ-to-MAZ shortest-path skim of time, distance and toll

        Args:
            controller: parent Controller object
        """
        super().__init__(controller)
        self._scenario = None
        self._network = None
        self._modeller = None

    @LogStartEnd()
    def run(self):
        # MAZ access mode set on connectors plus drive alone mode
        ref_period = None
        ref_period_name = self.config.highway.maz_to_maz.skim_period
        for period in self.config.periods:
            if period.name == ref_period_name:
                ref_period = period
                break
        emme_manager = _emme_tools.EmmeManager()
        emme_manager.project(os.path.join(self.root_dir, self.config.emme.project_path))
        self._modeller = emme_manager.modeller
        emmebank = emme_manager.emmebank(os.path.join(self.root_dir, self.config.emme.highway_database_path))
        self._scenario = emmebank.scenario(ref_period.emme_scenario_id)

        # prepare output file and write header
        output = self.config.highway.maz_to_maz.output_skim_file
        os.makedirs(os.path.dirname(output), exist_ok=True)
        with open(os.path.join(self.root_dir, output), "w") as output_file:
            output_file.write("FROM_ZONE, TO_ZONE, COST, DISTANCE, BRIDGETOLL\n")

        self._prepare_network()
        for county in _counties:
            self._mark_roots(county)
            sp_values = self._run_shortest_path()
            self._export_results(sp_values)
        # TODO: setup ...
        self._network = None  # clear network obj ref for garbage collection
        for attr in ["@link_cost", "@maz_root"]:
            self._scenario.delete_extra_attribute(attr)

    @LogStartEnd()
    def _prepare_network(self):
        modeller = _emme_tools.EmmeManager().modeller
        create_attribute = modeller.tool(
            "inro.emme.data.extra_attribute.create_extra_attribute"
        )
        net_calc = _emme_tools.NetworkCalculator(self._scenario)
        # TODO: internal (temp) attribute names may have a name conflict
        #       could auto-generate unique names on conflict
        # TODO: should delete attributes when done
        # TODO: copy-paste with maz assignment script
        attributes = [
            ("LINK", "@link_cost", "total cost MAZ-MAZ"),
            ("NODE", "@maz_root", "selected roots (origins)")
        ]
        for domain, name, desc in attributes:
            create_attribute(domain, name, desc, overwrite=True, scenario=self._scenario)
        if self._scenario.has_traffic_results:
            time_attr = "(@free_flow_time.max.timau)"
        else:
            time_attr = "@free_flow_time"
        vot = self.config.highway.maz_to_maz.value_of_time
        op_cost = self.config.highway.maz_to_maz.operating_cost_per_mile
        net_calc(
            "@link_cost", f"{time_attr} + 0.6 / {vot} * (length * {op_cost})"
        )
        self._network = self._scenario.get_partial_network(["NODE"], include_attributes=False)
        attrs_to_read = [("NODE", ["@maz_id", "#county"])]
        for domain, attrs in attrs_to_read:
            values = self._scenario.get_attribute_values(domain, attrs)
            self._network.set_attribute_values(domain, attrs, values)

    def _mark_roots(self, county):
        # TODO: double check can't route through non-active roots
        #       (should be OK with through_leaves=False)
        for node in self._network.nodes():
            if node["@maz_id"] > 0 and node["#county"] == county:
                node["@maz_root"] = node["@maz_id"]
            else:
                node["@maz_root"] = 0
        values = self._network.get_attribute_values("NODE", ["@maz_root"])
        self._scenario.set_attribute_values("NODE", ["@maz_root"], values)

    def _run_shortest_path(self):
        shortest_paths_tool = self._modeller.tool(
            "inro.emme.network_calculation.shortest_path"
        )
        num_processors = _emme_tools.parse_num_processors(
            self.config.emme.num_processors)
        max_cost = float(self.config.highway.maz_to_maz.max_skim_cost)
        spec = {
            "type": "SHORTEST_PATH",
            "modes": [self.config.highway.maz_to_maz.mode_code],
            "root_nodes": "@maz_root",
            "leaf_nodes": "@maz_id",
            "link_cost": "@link_cost",
            "path_constraints": {
                "max_cost": max_cost,
                "uturn_allowed": False,
                "through_leaves": False,
                "through_centroids": False,
                "exclude_forbidden_turns": False
            },
            "results": {
                "skim_output": {
                    "return_numpy": True,
                    "analyses": [
                        {
                            "component": "SHORTEST_PATH_COST",
                            "operator": "+",
                            "name": "COST",
                            "description": ""
                        },
                        {
                            "component": "length",
                            "operator": "+",
                            "name": "DISTANCE",
                            "description": ""
                       },
                       {
                            "component": "@bridgetoll_da",
                            "operator": "+",
                            "name": "BRIDGETOLL",
                            "description": ""
                       }
                    ],
                    "format": "OMX"
                }
            },
            "performance_settings": {
                "number_of_processors": num_processors,
                "direction": "FORWARD",
                "method": "STANDARD"
            }
        }
        sp_values = shortest_paths_tool(spec, self._scenario)
        return sp_values

    def _export_results(self, sp_values):
        # get list of MAZ IDS
        roots = [node["@maz_root"] for node in self._network.nodes() if node["@maz_root"]]
        leaves = [node["@maz_id"] for node in self._network.nodes() if node["@maz_id"]]
        # build dataframe with output data and to/from MAZ ids
        root_ids = np.repeat(roots, len(leaves))
        leaf_ids = leaves * len(roots)
        result_df = pd.DataFrame(
            {
                "FROM_ZONE": root_ids,
                "TO_ZONE": leaf_ids,
                "COST": sp_values["COST"].flatten(),
                "DISTANCE": sp_values["DISTANCE"].flatten(),
                "BRIDGETOLL": sp_values["BRIDGETOLL"].flatten(),
            }
        )
        # drop 0's / 1e20
        result_df = result_df.query("COST > 0 & COST < 1e19")
        # write remaining values to text file 
        # FROM_ZONE,TO_ZONE,COST,DISTANCE,BRIDGETOLL
        output = self.config.highway.maz_to_maz.output_skim_file
        with open(os.path.join(self.root_dir, output), "a", newline="") as output_file:
            result_df.to_csv(output_file, header=False, index=False)
