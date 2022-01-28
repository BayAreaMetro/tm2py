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
    Node attributes: @maz_id, x, y, and #node_county
Demand matrices under /demand_matrices/highway/maz_demand"
    auto_{period}_MAZ_AUTO_1_{period}.omx
    auto_{period}_MAZ_AUTO_2_{period}.omx
    auto_{period}_MAZ_AUTO_3_{period}.omx
    NOTE: demand structured is fixed

Output:
The resulting MAZ-MAZ flows are saved in link @maz_flow which is
used as background traffic in the general Highway assignment.
"""
from __future__ import annotations

import array as _array
from collections import defaultdict as _defaultdict
from contextlib import contextmanager as _context
from math import sqrt as _sqrt
import os
from typing import Collection, Union, TYPE_CHECKING

import numpy as np
import pandas as pd
from tables import NoSuchNodeError

from tm2py.components.component import Component
from tm2py.emme.matrix import OMXManager
from tm2py.emme.network import NetworkCalculator
from tm2py.logger import LogStartEnd
from tm2py.tools import parse_num_processors

if TYPE_CHECKING:
    from tm2py.controller import RunController

_default_bin_edges = [0.0, 0.9, 1.2, 1.8, 2.5, 5.0, 10.0]
# Grouping of output demand files for MAZ-MAZ pairs
# moved to config
# _county_sets = {
#     1: ["San Francisco", "San Mateo", "Santa Clara"],
#     2: ["Alameda", "Contra Costa"],
#     3: ["Solano", "Napa", "Sonoma", "Marin"],
# }
# _counties = [
#     "San Francisco",
#     "San Mateo",
#     "Santa Clara",
#     "Alameda",
#     "Contra Costa",
#     "Solano",
#     "Napa",
#     "Sonoma",
#     "Marin",
# ]
# Using text file format for now, can upgrade to binary format (faster) once
# compatibility with new networks is verified
_USE_BINARY = False


class AssignMAZSPDemand(Component):
    """MAZ-to-MAZ shortest-path highway assignment.

    Calculates shortest path between MAZs with demand in the Emme network
    and assigns flow.
    """

    # skip Too many instance attributes recommendation, it is OK as is
    # pylint: disable=R0902

    def __init__(self, controller: RunController):
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
        self._mazs = None
        self._demand = None
        self._max_dist = 0
        self._network = None
        self._root_index = None
        self._leaf_index = None

    @LogStartEnd()
    def run(self, time_period: Union[Collection[str], str] = None):
        """Run MAZ-to-MAZ shortest path assignment.

        Args:
            time_period: list of str names of time_periods, or name of a single time_period
        """
        emme_manager = self.controller.emme_manager
        emmebank = emme_manager.emmebank(
            self.get_abs_path(self.config.emme.highway_database_path)
        )
        self._eb_dir = os.path.dirname(emmebank.path)
        county_groups = {}
        for group in self.config.highway.maz_to_maz.demand_county_groups:
            county_groups[group.number] = group.counties
        for time in self._process_time_period(time_period):
            with self.logger.log_start_end(f"period {time}"):
                self._scenario = self.get_emme_scenario(emmebank.path, time)
                with self._setup(time):
                    self._prepare_network()
                    for i, names in county_groups.items():
                        maz_ids = self._get_county_mazs(names)
                        if len(maz_ids) == 0:
                            self.logger.log(
                                f"warning: no mazs for counties {', '.join(names)}"
                            )
                            continue
                        self._process_demand(time, i, maz_ids)
                    demand_bins = self._group_demand()
                    for i, demand_group in enumerate(demand_bins):
                        self._find_roots_and_leaves(demand_group["demand"])
                        self._run_shortest_path(i, demand_group["dist"], time)
                        self._assign_flow(i, demand_group["demand"], time)

    @_context
    def _setup(self, period):
        self._mazs = None
        self._demand = _defaultdict(lambda: [])
        self._max_dist = 0
        self._network = None
        self._root_index = None
        self._leaf_index = None
        self._debug_report = []
        attributes = [
            ("LINK", "@link_cost", "total cost MAZ-MAZ"),
            ("LINK", "@link_cost_maz", "cost MAZ-MAZ, unused MAZs blocked"),
            ("NODE", "@maz_root", "Flag for MAZs which are roots"),
            ("NODE", "@maz_leaf", "Flag for MAZs which are leaves"),
        ]
        with self.controller.emme_manager.temp_attributes_and_restore(
            self._scenario, attributes
        ):
            try:
                yield
            finally:
                if not self._debug:
                    self._mazs = None
                    self._demand = None
                    self._network = None
                    self._root_index = None
                    self._leaf_index = None
                    # delete sp path files
                    for bin_no in range(len(self._bin_edges)):
                        file_path = os.path.join(
                            self._eb_dir, f"sp_{period}_{bin_no}.ebp"
                        )
                        if os.path.exists(file_path):
                            os.remove(file_path)
                else:
                    for item in self._debug_report:
                        self.logger.log(item)

    def _prepare_network(self):
        if self._scenario.has_traffic_results:
            time_attr = "(@free_flow_time.max.timau)"
        else:
            time_attr = "@free_flow_time"
        vot = self.config.highway.maz_to_maz.value_of_time
        op_cost = self.config.highway.maz_to_maz.operating_cost_per_mile
        net_calc = NetworkCalculator(self._scenario)
        net_calc("@link_cost", f"{time_attr} + 0.6 / {vot} * (length * {op_cost})")
        net_calc("ul1", "0")
        self._network = self._scenario.get_partial_network(
            ["NODE", "LINK"], include_attributes=False
        )
        self._network.create_attribute("LINK", "temp_flow")
        attrs_to_read = [("NODE", ["@maz_id", "x", "y", "#node_county"])]
        for domain, attrs in attrs_to_read:
            self._read_attr_values(domain, attrs)

    def _get_county_mazs(self, counties):
        network = self._network
        # NOTE: every maz must have a valid #node_county
        if self._mazs is None:
            self._mazs = _defaultdict(lambda: [])
            for node in network.nodes():
                if node["@maz_id"]:
                    self._mazs[node["#node_county"]].append(node)
        mazs = []
        for county in counties:
            mazs.extend(self._mazs[county])
        return sorted(mazs, key=lambda n: n["@maz_id"])

    def _process_demand(self, time, index, maz_ids):
        data = self._read_demand_array(time, index)
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

    def _read_demand_array(self, time, index):
        file_path_tmplt = self.get_abs_path(
            self.config.highway.maz_to_maz.demand_file
        )
        omx_file_path = self.get_abs_path(
            file_path_tmplt.format(period=time, number=index)
        )
        with OMXManager(omx_file_path, "r") as omx_file:
            try:
                demand_array = omx_file.read_hdf5("/matrices/M0")
            except NoSuchNodeError:
                demand_array = omx_file.read("M0")
        return demand_array

    def _group_demand(self):
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
                f"       bin dist {group['dist']}, size {len(group['demand'])}"
            )
        # Filter out groups without any demend
        demand_groups = [group for group in demand_groups if group["demand"]]
        return demand_groups

    def _find_roots_and_leaves(self, demand):
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
        net_calc = NetworkCalculator(self._scenario)
        net_calc.add_calc("@link_cost_maz", "@link_cost")
        net_calc.add_calc("@link_cost_maz", "1e20", "@maz_root=0 and !@maz_id=0")
        net_calc.add_calc("@link_cost_maz", "1e20", "@maz_leafj=0 and !@maz_idj=0")
        net_calc.run()

    def _run_shortest_path(self, bin_no, max_radius, period):
        shortest_paths_tool = self.controller.emme_manager.tool(
            "inro.emme.network_calculation.shortest_path"
        )
        max_radius = max_radius * 5280 + 100  # add some buffer for rounding error
        ext = "ebp" if _USE_BINARY else "txt"
        file_name = f"sp_{period}_{bin_no}.{ext}"
        num_processors = parse_num_processors(self.config.emme.num_processors)
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
                "exclude_forbidden_turns": False,
            },
            "results": {
                "skim_output": {
                    "file": "",
                    "format": "TEXT",
                    "return_numpy": False,
                    "analyses": [],
                },
                "path_output": {
                    "format": "BINARY" if _USE_BINARY else "TEXT",
                    "file": os.path.join(self._eb_dir, file_name),
                },
            },
            "performance_settings": {
                "number_of_processors": num_processors,
                "direction": "FORWARD",
                "method": "STANDARD",
            },
        }
        shortest_paths_tool(spec, self._scenario)

    def _assign_flow(self, bin_no, demand, period):
        if _USE_BINARY:
            self._assign_flow_binary(bin_no, demand, period)
        else:
            self._assign_flow_text(bin_no, demand, period)

    def _assign_flow_text(self, bin_no, demand, period):
        paths = self._load_text_format_paths(bin_no, period)
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
        self._debug_report.extend(
            [
                f"    ASSIGN bin {bin_no}: total: {len(demand)}",
                f"         assigned: {assigned}, not assigned: {not_assigned}",
            ]
        )

    def _load_text_format_paths(self, bin_no, period):
        paths = _defaultdict(lambda: {})
        with open(
            os.path.join(self._eb_dir, f"sp_{period}_{bin_no}.txt"),
            "r",
            encoding="utf8",
        ) as paths_file:
            for line in paths_file:
                nodes = [int(x) for x in line.split()]
                paths[nodes[0]][nodes[-1]] = nodes[1:]
        return paths

    def _assign_flow_binary(self, bin_no, demand, period):
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
        self._save_attr_values("LINK", ["temp_flow"], ["@maz_flow"])
        self._debug_report.append(
            f"    ASSIGN bin {bin_no}, total {len(demand)}, assign "
            f"{assigned}, not assign {not_assigned}, bytes {bytes_read}"
        )

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


class SkimMAZCosts(Component):
    """MAZ-to-MAZ shortest-path skim of time, distance and toll"""

    def __init__(self, controller: RunController):
        """MAZ-to-MAZ shortest-path skim of time, distance and toll
        Args:
            controller: parent RunController object
        """
        super().__init__(controller)
        self._scenario = None
        self._network = None
        self._modeller = None

    @LogStartEnd()
    def run(self):
        ref_period = None
        ref_period_name = self.config.highway.maz_to_maz.skim_period
        for period in self.config.time_periods:
            if period.name == ref_period_name:
                ref_period = period
                break
        if ref_period is None:
            raise Exception(
                "highway.maz_to_maz.skim_period: is not the name of an existing time_period"
            )
        self._scenario = self.get_emme_scenario(
            self.config.emme.highway_database_path, ref_period.name
        )
        # prepare output file and write header
        output = self.get_abs_path(self.config.highway.maz_to_maz.output_skim_file)
        os.makedirs(os.path.dirname(output), exist_ok=True)
        with open(output, "w", encoding="utf8") as output_file:
            output_file.write("FROM_ZONE, TO_ZONE, COST, DISTANCE, BRIDGETOLL\n")
        counties = []
        for group in self.config.highway.maz_to_maz.demand_county_groups:
            counties.extend(group.counties)
        with self._setup():
            self._prepare_network()
            for county in counties:
                num_roots = self._mark_roots(county)
                if num_roots == 0:
                    continue
                sp_values = self._run_shortest_path()
                self._export_results(sp_values)

    @_context
    def _setup(self):
        attributes = [
            ("LINK", "@link_cost", "total cost MAZ-MAZ"),
            ("NODE", "@maz_root", "selected roots (origins)"),
        ]
        with self.controller.emme_manager.temp_attributes_and_restore(
            self._scenario, attributes
        ):
            try:
                yield
            finally:
                self._network = None  # clear network obj ref to free memory

    @LogStartEnd()
    def _prepare_network(self):
        net_calc = NetworkCalculator(self._scenario)
        if self._scenario.has_traffic_results:
            time_attr = "(@free_flow_time.max.timau)"
        else:
            time_attr = "@free_flow_time"
        vot = self.config.highway.maz_to_maz.value_of_time
        op_cost = self.config.highway.maz_to_maz.operating_cost_per_mile
        net_calc("@link_cost", f"{time_attr} + 0.6 / {vot} * (length * {op_cost})")
        self._network = self._scenario.get_partial_network(
            ["NODE"], include_attributes=False
        )
        attrs_to_read = [("NODE", ["@maz_id", "#node_county"])]
        for domain, attrs in attrs_to_read:
            values = self._scenario.get_attribute_values(domain, attrs)
            self._network.set_attribute_values(domain, attrs, values)

    def _mark_roots(self, county):
        count_roots = 0
        for node in self._network.nodes():
            if node["@maz_id"] > 0 and node["#node_county"] == county:
                node["@maz_root"] = node["@maz_id"]
                count_roots += 1
            else:
                node["@maz_root"] = 0
        values = self._network.get_attribute_values("NODE", ["@maz_root"])
        self._scenario.set_attribute_values("NODE", ["@maz_root"], values)
        return count_roots

    def _run_shortest_path(self):
        shortest_paths_tool = self.controller.emme_manager.tool(
            "inro.emme.network_calculation.shortest_path"
        )
        num_processors = parse_num_processors(self.config.emme.num_processors)
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
                "exclude_forbidden_turns": False,
            },
            "results": {
                "skim_output": {
                    "return_numpy": True,
                    "analyses": [
                        {
                            "component": "SHORTEST_PATH_COST",
                            "operator": "+",
                            "name": "COST",
                            "description": "",
                        },
                        {
                            "component": "length",
                            "operator": "+",
                            "name": "DISTANCE",
                            "description": "",
                        },
                        {
                            "component": "@bridgetoll_da",
                            "operator": "+",
                            "name": "BRIDGETOLL",
                            "description": "",
                        },
                    ],
                    "format": "OMX",
                }
            },
            "performance_settings": {
                "number_of_processors": num_processors,
                "direction": "FORWARD",
                "method": "STANDARD",
            },
        }
        sp_values = shortest_paths_tool(spec, self._scenario)
        return sp_values

    def _export_results(self, sp_values):
        # get list of MAZ IDS
        roots = [
            node["@maz_root"] for node in self._network.nodes() if node["@maz_root"]
        ]
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
        output = self.get_abs_path(self.config.highway.maz_to_maz.output_skim_file)
        with open(output, "a", newline="", encoding="utf8") as output_file:
            result_df.to_csv(output_file, header=False, index=False)
