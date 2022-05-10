"""Assigns and skims MAZ-to-MAZ demand along shortest generalized cost path.

MAZ to MAZ demand is read in from separate OMX matrices as defined under
the config table highway.maz_to_maz.demand_county_groups,

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
Demand matrices under highway.maz_to_maz.demand_file,
and can have a placeholder
    auto_{period}_MAZ_AUTO_{number}_{period}.omx

Output:
The resulting MAZ-MAZ flows are saved in link @maz_flow which is
used as background traffic in the equilibrium Highway assignment.
"""

from __future__ import annotations

import array as _array
import os
from collections import defaultdict as _defaultdict
from contextlib import contextmanager as _context
from math import sqrt as _sqrt
from typing import TYPE_CHECKING, BinaryIO, Dict, List, Union

import numpy as np
import pandas as pd

from tm2py.components.component import Component
from tm2py.emme.manager import EmmeNode
from tm2py.emme.matrix import OMXManager
from tm2py.emme.network import NetworkCalculator
from tm2py.logger import LogStartEnd
from tm2py.tools import parse_num_processors

# from tables import NoSuchNodeError


if TYPE_CHECKING:
    from tm2py.controller import RunController

_default_bin_edges = [0.0, 0.9, 1.2, 1.8, 2.5, 5.0, 10.0]
# Using text file format for now, can upgrade to binary format (faster) once
# compatibility with new networks is verified
_USE_BINARY = False
NumpyArray = np.array


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
    def run(self):
        """Run MAZ-to-MAZ shortest path assignment."""
        emme_manager = self.controller.emme_manager
        emmebank = emme_manager.emmebank(
            self.get_abs_path(self.config.emme.highway_database_path)
        )
        self._eb_dir = os.path.dirname(emmebank.path)
        county_groups = {}
        for group in self.config.highway.maz_to_maz.demand_county_groups:
            county_groups[group.number] = group.counties
        for time in self.time_period_names():
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
                    self._set_link_cost_maz()
                    self._run_shortest_path(time, i, demand_group["dist"])
                    self._assign_flow(time, i, demand_group["demand"])

    @_context
    def _setup(self, time: str):
        """Context setup / teardown, initializes internal attributes.

        Args:
            time: name of the time period
        """
        self._mazs = None
        self._demand = _defaultdict(lambda: [])
        self._max_dist = 0
        self._network = None
        self._root_index = None
        self._leaf_index = None
        attributes = [
            ("LINK", "@link_cost", "total cost MAZ-MAZ"),
            ("LINK", "@link_cost_maz", "cost MAZ-MAZ, unused MAZs blocked"),
            ("NODE", "@maz_root", "Flag for MAZs which are roots"),
            ("NODE", "@maz_leaf", "Flag for MAZs which are leaves"),
        ]
        for domain, name, desc in attributes:
            self.logger.log(f"Create temp {domain} attr: {name}, {desc}", level="TRACE")
        with self.controller.emme_manager.temp_attributes_and_restore(
            self._scenario, attributes
        ):
            try:
                with self.logger.log_start_end(
                    f"MAZ assign for period {time} scenario {self._scenario}"
                ):
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
                            self._eb_dir, f"sp_{time}_{bin_no}.ebp"
                        )
                        if os.path.exists(file_path):
                            os.remove(file_path)

    def _prepare_network(self):
        """Calculate link cost (travel time + bridge tolls + operating cost) and load network.

        Reads Emme network from disk for later node lookups. Optimized to only load
        attribute values of interest, additional attributes must be added in
        order to be read from disk.
        """
        if self._scenario.has_traffic_results:
            time_attr = "(@free_flow_time.max.timau)"
        else:
            time_attr = "@free_flow_time"
        self.logger.log(f"Calculating link costs using time {time_attr}", level="DEBUG")
        vot = self.config.highway.maz_to_maz.value_of_time
        op_cost = self.config.highway.maz_to_maz.operating_cost_per_mile
        net_calc = NetworkCalculator(self._scenario)
        report = net_calc(
            "@link_cost", f"{time_attr} + 0.6 / {vot} * (length * {op_cost})"
        )
        self.logger.log_time("Link cost calculation report", level="TRACE")
        self.logger.log_dict(report, level="TRACE")
        self._network = self.controller.emme_manager.get_network(
            self._scenario, {"NODE": ["@maz_id", "x", "y", "#node_county"], "LINK": []}
        )
        self._network.create_attribute("LINK", "temp_flow")

    def _get_county_mazs(self, counties: List[str]) -> List[EmmeNode]:
        """Get all MAZ nodes which are located in one of these counties.

        Used the node attribute #node_county to identify the node location.
        Name must be an exact match. Catches a mapping of the county names
        to nodes so nodes are processed only once.

        Args:
            counties: list of county names

        Returns:
            List of MAZ nodes (Emme Node) which are in these counties.
        """
        self.logger.log_time(
            f"Processing county MAZs for {', '.join(counties)}", level="DETAIL"
        )
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
        self.logger.log(f"Num MAZs {len(mazs)}", level="DEBUG")
        return sorted(mazs, key=lambda n: n["@maz_id"])

    def _process_demand(self, time: str, index: int, maz_ids: List[EmmeNode]):
        """Loads the demand from file and groups by origin node.

        Sets the demand to self._demand for later processing, grouping the demand in
        a dictionary by origin node (Emme Node object) to list of dictionaries
        {"orig": orig_node, "dest": dest_node, "dem": demand, "dist": dist}

        Args:
            time: time period name
            index: group index of the demand file, used to find the file by name
            maz_ids: indexed list of MAZ ID nodes for the county group
                (active counties for this demand file)
        """
        self.logger.log_time(
            f"Process demand for time period {time} index {index}", level="DETAIL"
        )
        data = self._read_demand_array(time, index)
        origins, destinations = data.nonzero()
        self.logger.log(
            f"non-zero origins {len(origins)} destinations {len(destinations)}",
            level="DEBUG",
        )
        total_demand = 0
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
            demand = data[orig][dest]
            total_demand += demand
            self._demand[orig_node].append(
                {
                    "orig": orig_node,
                    "dest": dest_node,
                    "dem": demand,
                    "dist": dist,
                }
            )
        self.logger.log(f"Max distance found {self._max_dist}", level="DEBUG")
        self.logger.log(f"Total inter-zonal demand {total_demand}", level="DEBUG")

    def _read_demand_array(self, time: str, index: int) -> NumpyArray:
        """Load the demand from file with the specified time and index name.

        Args:
            time: time period name
            index: group index of the demand file, used to find the file by name
        """
        file_path_tmplt = self.get_abs_path(self.config.highway.maz_to_maz.demand_file)
        omx_file_path = self.get_abs_path(
            file_path_tmplt.format(period=time, number=index)
        )
        self.logger.log(f"Reading demand from {omx_file_path}", level="DEBUG")
        with OMXManager(omx_file_path, "r") as omx_file:
            demand_array = omx_file.read("M0")
        return demand_array

    def _group_demand(
        self,
    ) -> List[Dict[str, Union[float, List[Dict[str, Union[float, EmmeNode]]]]]]:
        """Process the demand loaded from files \
            and create groups based on the origin to the furthest destination with demand.

        Returns:
            List of dictionaries, containing the demand in the format
                {"orig": EmmeNode, "dest": EmmeNode, "dem": float (demand value)}

        """
        self.logger.log_time("Grouping demand in distance buckets", level="DETAIL")
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
            self.logger.log(
                f"bin dist {group['dist']}, size {len(group['demand'])}", level="DEBUG"
            )
        # Filter out groups without any demand
        demand_groups = [group for group in demand_groups if group["demand"]]
        return demand_groups

    def _find_roots_and_leaves(self, demand: List[Dict[str, Union[float, EmmeNode]]]):
        """Label available MAZ root nodes and leaf nodes for the path calculation.

        The MAZ nodes which are found as origins in the demand are "activated"
        by setting @maz_root to non-zero, and similarly the leaves have @maz_leaf
        set to non-zero.

        Args:
            demand: list of dictionaries, containing the demand in the format
                {"orig": EmmeNode, "dest": EmmeNode, "dem": float (demand value)}
        """
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
        self.controller.emme_manager.copy_attr_values(
            "NODE", self._network, self._scenario, ["@maz_root", "@maz_leaf"]
        )

    def _set_link_cost_maz(self):
        """Set link cost used in the shortest path forbidden using unavailable connectors.

        Copy the pre-calculated cost @link_cost to @link_cost_maz,
        setting value to 1e20 on connectors to unused zone leaves / from
        unused roots.
        """
        # forbid egress from MAZ nodes which are not demand roots /
        #        access to MAZ nodes which are not demand leafs
        net_calc = NetworkCalculator(self._scenario)
        net_calc.add_calc("@link_cost_maz", "@link_cost")
        net_calc.add_calc("@link_cost_maz", "1e20", "@maz_root=0 and !@maz_id=0")
        net_calc.add_calc("@link_cost_maz", "1e20", "@maz_leafj=0 and !@maz_idj=0")
        net_calc.run()

    @LogStartEnd(level="DETAIL")
    def _run_shortest_path(self, time: str, bin_no: int, max_radius: float):
        """Run the shortest path tool to generate paths between the marked nodes.

        Args:
            time: time period name
            bin_no: bin number (id) for this demand segment
            max_radius: max unit coordinate distance to limit search tree
        """
        shortest_paths_tool = self.controller.emme_manager.tool(
            "inro.emme.network_calculation.shortest_path"
        )
        max_radius = max_radius * 5280 + 100  # add some buffer for rounding error
        ext = "ebp" if _USE_BINARY else "txt"
        file_name = f"sp_{time}_{bin_no}.{ext}"
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

    def _assign_flow(
        self, time: str, bin_no: int, demand: List[Dict[str, Union[float, EmmeNode]]]
    ):
        """Assign the demand along the paths generated from the shortest path tool.

        Args:
            time: time period name
            bin_no: bin number (id) for this demand segment
            demand: list of dictionaries, containing the demand in the format
                {"orig": EmmeNode, "dest": EmmeNode, "dem": float (demand value)}
        """
        if _USE_BINARY:
            self._assign_flow_binary(time, bin_no, demand)
        else:
            self._assign_flow_text(time, bin_no, demand)

    def _assign_flow_text(
        self, time: str, bin_no: int, demand: List[Dict[str, Union[float, EmmeNode]]]
    ):
        """Assign the demand along the paths generated from the shortest path tool.

        The paths are read from a text format file, see Emme help for details.
        Demand is summed in self._network (in memory) using temp_flow attribute
        and written to scenario (Emmebank / disk) @maz_flow.

        Args:
            time: time period name
            bin_no: bin number (id) for this demand segment
            demand: list of dictionaries, containin the demand in the format
                {"orig": EmmeNode, "dest": EmmeNode, "dem": float (demand value)}
        """
        paths = self._load_text_format_paths(time, bin_no)
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
        self.logger.log_time(
            f"ASSIGN bin {bin_no}: total: {len(demand)}", level="DEBUG"
        )
        self.logger.log_time(
            f"assigned: {assigned}, not assigned: {not_assigned}", level="DEBUG"
        )

    def _load_text_format_paths(
        self, time: str, bin_no: int
    ) -> Dict[int, Dict[int, List[int]]]:
        """Load all paths from text file and return as nested dictionary.

        Args:
            time: time period name
            bin_no: bin number (id) for this demand segment

        Returns:
            All paths as a nested dictionary, path = paths[origin][destination],
            using the node IDs as integers.
        """
        paths = _defaultdict(lambda: {})
        with open(
            os.path.join(self._eb_dir, f"sp_{time}_{bin_no}.txt"),
            "r",
            encoding="utf8",
        ) as paths_file:
            for line in paths_file:
                nodes = [int(x) for x in line.split()]
                paths[nodes[0]][nodes[-1]] = nodes[1:]
        return paths

    def _assign_flow_binary(
        self, time: str, bin_no: int, demand: List[Dict[str, Union[float, EmmeNode]]]
    ):
        """Assign the demand along the paths generated from the shortest path tool.

        The paths are read from a binary format file, see Emme help for details.
        Demand is summed in self._network (in memory) using temp_flow attribute
        and written to scenario (Emmebank / disk) @maz_flow.

        Args:
            time: time period name
            bin_no: bin number (id) for this demand segment
            demand: list of dictionaries, containin the demand in the format
                {"orig": EmmeNode, "dest": EmmeNode, "dem": float (demand value)}
        """
        file_name = f"sp_{time}_{bin_no}.ebp"
        with open(os.path.join(self._eb_dir, file_name), "rb") as paths_file:
            # read set of path pointers by Orig-Dest sequence from file
            offset, leaves_nb, path_indicies = self._get_path_indices(paths_file)
            assigned = 0
            not_assigned = 0
            bytes_read = offset * 8
            # for all orig-dest pairs with demand, load path from file
            for data in demand:
                # get file position based on orig-dest index
                start, end = self._get_path_location(
                    data["orig"].number, data["dest"].number, leaves_nb, path_indicies
                )
                # no path found, disconnected zone
                if start == end:
                    not_assigned += data["dem"]
                    continue
                paths_file.seek(start * 4 + offset * 8)
                self._assign_path_flow(paths_file, start, end, data["dem"])
                assigned += data["dem"]
                bytes_read += (end - start) * 4
        self.controller.emme_manager.copy_attr_values(
            "LINK", self._network, self._scenario, ["temp_flow"], ["@maz_flow"]
        )
        self.logger.log_time(
            f"ASSIGN bin {bin_no}, total {len(demand)}, assign "
            f"{assigned}, not assign {not_assigned}, bytes {bytes_read}",
            level="DEBUG",
        )

    @staticmethod
    def _get_path_indices(paths_file: BinaryIO) -> [int, int, _array.array]:
        """Get the path header indices.

        See the Emme Shortest path tool doc for additional details on reading
        this file.

        Args:
            paths_file: binary file access to the generated paths file

        Returns:
            2 ints + array of ints: offset, leafs_nb, path_indicies
            offset: starting index to read the paths
            leafs_nb: number of leafs in the shortest path file
            path_indicies: array of the start index for each root, leaf path in paths_file.
        """
        # read first 4 integers from file (Q=64-bit unsigned integers)
        header = _array.array("Q")
        header.fromfile(paths_file, 4)
        roots_nb, leaves_nb = header[2:4]
        # Load sequence of path indices (positions by orig-dest index),
        # pointing to list of path node IDs in file
        path_indicies = _array.array("Q")
        path_indicies.fromfile(paths_file, roots_nb * leaves_nb + 1)
        offset = roots_nb * leaves_nb + 1 + 4
        return offset, leaves_nb, path_indicies

    def _get_path_location(
        self,
        orig: EmmeNode,
        dest: EmmeNode,
        leaves_nb: int,
        path_indicies: _array.array,
    ) -> [int, int]:
        """Get the location in the paths_file to read.

        Args:
            orig: Emme Node object, origin MAZ to query the path
            dest: Emme Node object, destination MAZ to query the path
            leaves_nb: number of leaves
            path_indicies: array of the start index for each root, leaf path in paths_file.

        Returns:
            Two integers, start, end
            start: starting index to read Node ID bytes from paths_file
            end: ending index to read bytes from paths_file
        """
        p_index = self._root_index[orig]
        q_index = self._leaf_index[dest]
        index = p_index * leaves_nb + q_index
        start = path_indicies[index]
        end = path_indicies[index + 1]
        return start, end

    def _assign_path_flow(
        self, paths_file: BinaryIO, start: int, end: int, demand: float
    ):
        """Add demand to link temp_flow for the path.

        Args:
            paths_file: binary file access to read path from
            start: starting index to read Node ID bytes from paths_file
            end: ending index to read bytes from paths_file
            demand: flow demand to add on link
        """
        # load sequence of Node IDs which define the path (L=32-bit unsigned integers)
        path = _array.array("L")
        path.fromfile(paths_file, end - start)
        # process path to sequence of links and add flow
        path_iter = iter(path)
        i_node = next(path_iter)
        for j_node in path_iter:
            link = self._network.link(i_node, j_node)
            link["temp_flow"] += demand
            i_node = j_node


class SkimMAZCosts(Component):
    """MAZ-to-MAZ shortest-path skim of time, distance and toll."""

    def __init__(self, controller: RunController):
        """MAZ-to-MAZ shortest-path skim of time, distance and toll.

        Args:
            controller: parent RunController object
        """
        super().__init__(controller)
        self._scenario = None
        self._network = None

    @LogStartEnd()
    def run(self):
        """Run shortest path skims for all available MAZ-to-MAZ O-D pairs.

        Runs a shortest path builder for each county, using a maz_skim_cost
        to limit the search. The valid gen cost (time + cost), distance and toll (drive alone)
        are written to CSV at the output_skim_file path:
        FROM_ZONE, TO_ZONE, COST, DISTANCE, BRIDGETOLL

        The following config inputs are used directly in this component. Note also
        that the network mode_code is prepared in the highway_network component
        using the excluded_links.

        config.highway.maz_to_maz:
            skim_period: name of the period used for the skim, must match one the
                defined config.time_periods
            demand_county_groups: used for the list of counties, creates a list out
                of all listed counties under [].counties
            output_skim_file: relative path to save the skims
            value_of_time: value of time used to convert tolls and auto operating cost
            operating_cost_per_mile: auto operating cost
            max_skim_cost: max cost value used to limit the shortest path search
            mode_code:

        config.emme.num_processors
        """
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
        """Creates the temp attributes used in the component."""
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

    @LogStartEnd(level="DEBUG")
    def _prepare_network(self):
        """Calculates the link cost in @link_cost and loads the network to self._network."""
        net_calc = NetworkCalculator(self._scenario)
        if self._scenario.has_traffic_results:
            time_attr = "(@free_flow_time.max.timau)"
        else:
            time_attr = "@free_flow_time"
        self.logger.log(f"Time attribute {time_attr}", level="DEBUG")
        vot = self.config.highway.maz_to_maz.value_of_time
        op_cost = self.config.highway.maz_to_maz.operating_cost_per_mile
        net_calc("@link_cost", f"{time_attr} + 0.6 / {vot} * (length * {op_cost})")
        self._network = self.controller.emme_manager.get_network(
            self._scenario, {"NODE": ["@maz_id", "#node_county"]}
        )

    def _mark_roots(self, county: str) -> int:
        """Mark the available roots in the county."""
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

    @LogStartEnd(level="DETAIL")
    def _run_shortest_path(self) -> Dict[str, NumpyArray]:
        """Run shortest paths tool and return dictionary of skim results name, numpy arrays.

        O-D pairs are limited by a max cost value from config.highway.maz_to_maz.max_skim_cost,
        from roots marked by @maz_root to all available leaves at @maz_id.

        Returns:
            A dictionary with keys "COST", "DISTANCE", and "BRIDGETOLL", and numpy
            arrays of SP values for available O-D pairs
        """
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

    def _export_results(self, sp_values: Dict[str, NumpyArray]):
        """Write matrix skims to CSV.

        The matrices are filtered to omit rows for which the COST is
        < 0 or > 1e19 (Emme uses 1e20 to indicate inaccessible zone pairs).

        sp_values: dictionary of matrix costs, with the three keys
            "COST", "DISTANCE", and "BRIDGETOLL" and Numpy arrays of values
        """
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
