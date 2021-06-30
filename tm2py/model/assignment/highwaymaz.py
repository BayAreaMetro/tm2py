"""Assigns MAZ-to-MAZ demand along shortest generalized cost path for nearby trips.

 MAZ to MAZ local networks are as follows:
         (1) counties 1, 2, and 3
         (2) counties 4 and 5
         (3) counties 6, 7, 8, and 9

 Input:  (1) hwy\\avgload@token_period@.net - TAZ output network for skimming by time period
         (2) hwy\\hwyparam.block - highway assignment generalized cost parameters
         (3) MAZ_Demand_@mazset@_@token_period@.mat - MAZ to MAZ auto demand for each local network

 Output: (1) maz_preload_@token_period@.net - Network by time period with link attribute MAZMAZVOL
             for copying over to the TAZ to TAZ highway assignment
"""

import array as _array
from collections import defaultdict as _defaultdict
from contextlib import contextmanager as _context
from math import sqrt as _sqrt
import os as _os
import time as _time


from tm2py.core.component import Component as _Component
import tm2py.core.emme as _emme_tools


_join, _dir = _os.path.join, _os.path.dirname


class AssignMAZSPDemand(_Component):
    """TODO: docstring"""

    def __init__(self, controller, scenario, period, modes):
        """docstring for traffic assignment run"""
        super().__init__(controller)
        self._num_processors = self.config.emme.number_of_processors
        self._scenario = scenario
        self._period = period
        self._modes = modes
        self._modeller = _emme_tools.EmmeProjectCache().modeller
        self._root_dir = _dir(scenario.emmebank.path)
        maz_assign_config = self.config.emme.highway_assignment.maz_assignment
        self._vot = maz_assign_config.value_of_time
        self._operating_cost = maz_assign_config.operating_cost
        # bins: performance parameter: crow-fly distance bins
        #       to limit shortest path calculation by origin to furthest destination
        self._bin_edges = [0.0, 0.9, 1.2, 1.8, 2.5, 5.0, 10.0]
        self._net_calc = _emme_tools.NetworkCalculator(self._scenario, self._modeller)
        self._debug_report = []
        self._debug = False

        self._mazs = None
        self._demand = None
        self._max_dist = 0
        self._network = None
        self._root_index = None
        self._leaf_index = None

    def run(self):
        """docstring for traffic assignment run"""
        root_dir = r"..\demand_matrices\highway\maz_demand"
        period = self._period
        with self._setup():
            self._prepare_network()
            county_sets = {
                1: ["San Francisco", "San Mateo", "Santa Clara"],
                2: ["Alameda", "Contra Costa"],
                3: ["Solano", "Napa", "Sonoma", "Marin"],
            }
            for i in range(1, 4):
                mazseq = self._get_county_mazs(county_sets[i])
                omx_file_path = _join(
                    root_dir, f"auto_{period}_MAZ_AUTO_{i}_{period}.omx"
                )
                with _emme_tools.OMX(omx_file_path, "r") as omx_file:
                    demand_array = omx_file.read_hdf5("/matrices/M0")
                self._process_demand(demand_array, mazseq)
                del demand_array

            demand_bins = self._group_demand()
            for i, demand_group in enumerate(demand_bins):
                self._find_roots_and_leaves(demand_group["demand"])
                self._run_shortest_path(i, demand_group["dist"])
                self._assign_flow(i, demand_group["demand"])

    @_context
    def _setup(self):
        self._mazs = None
        self._demand = _defaultdict(lambda: [])
        self._max_dist = 0
        self._network = None
        self._root_index = None
        self._leaf_index = None
        self._debug_report = []
        try:
            yield
        finally:
            if not self._debug:
                for attr in ["@link_cost_maz", "@maz_root", "@maz_leaf", "@link_cost"]:
                    self._scenario.delete_attribute(attr)
                self._mazs = None
                self._demand = None
                self._network = None
                self._root_index = None
                self._leaf_index = None

    def _prepare_network(self):
        start_time = _time.time()
        modeller = self._modeller
        create_attribute = modeller.tool(
            "inro.emme.data.extra_attribute.create_extra_attribute"
        )
        create_attribute(
            "LINK",
            "@link_cost",
            "total link cost for MAZ-MAZ SP assign",
            overwrite=True,
            scenario=self._scenario,
        )
        create_attribute(
            "LINK",
            "@link_cost_maz",
            "link cost MAZ-MAZ, unused MAZs blocked",
            overwrite=True,
            scenario=self._scenario,
        )
        create_attribute("NODE", "@maz_root", overwrite=True, scenario=self._scenario)
        create_attribute("NODE", "@maz_leaf", overwrite=True, scenario=self._scenario)
        if self._scenario.has_traffic_results:
            time_attr = "timau"
        else:
            time_attr = "@free_flow_time"
        vot = self._vot
        op_cost = self._operating_cost
        self._net_calc(
            "@link_cost", f"{time_attr} + 0.6 / {vot} * (length * {op_cost})"
        )
        self._net_calc("ul1", "0")
        self._network = self._scenario.get_partial_network(
            ["NODE", "LINK"], include_attributes=False
        )
        self._network.create_attribute("LINK", "temp_flow")
        attrs_to_read = [
            ("NODE", ["@mazseq", "x", "y", "#county"]),
            # ("LINK", ["@link_cost_maz"]),
        ]
        for domain, attrs in attrs_to_read:
            self._read_attr_values(domain, attrs)
        self._debug_report.append(
            "    PREPARE NETWORK --- %s seconds ---" % (_time.time() - start_time)
        )

    def _get_county_mazs(self, counties):
        network = self._network
        # NOTE: every maz must have a valid #county
        if self._mazs is None:
            self._mazs = _defaultdict(lambda: [])
            for node in network.nodes():
                if node["@mazseq"]:
                    self._mazs[node["#county"]].append(node)
        mazs = []
        for county in counties:
            mazs.extend(self._mazs[county])
        return sorted(mazs, key=lambda n: n["@mazseq"])

    def _process_demand(self, data, mazseq):
        start_time = _time.time()
        origins, destinations = data.nonzero()
        for orig, dest in zip(origins, destinations):
            # skip intra-maz demand
            if orig == dest:
                continue
            orig_node = mazseq[orig]
            dest_node = mazseq[dest]
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
            "    PROCESS DEMAND --- %s seconds ---" % (_time.time() - start_time)
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
                if max_dist < demand_groups["dist"]:
                    demand_groups["demand"].extend(data)
                    break
        for group in demand_groups:
            self._debug_report.append(
                "       bin dist %s, size %s" % (group["dist"], len(group["demand"]))
            )
        # Filter out groups without any demend
        demand_groups = [group for group in demand_groups if group["demand"]]
        self._debug_report.append(
            "    GROUP DEMAND --- %s seconds ---" % (_time.time() - start_time)
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
            o_node, d_node = data["p"], data["q"]
            root_maz_ids[o_node.number] = o_node["@maz_root"] = o_node["@mazseq"]
            leaf_maz_ids[d_node.number] = d_node["@maz_leaf"] = d_node["@mazseq"]
        self._root_index = {p: i for i, p in enumerate(sorted(root_maz_ids.keys()))}
        self._leaf_index = {q: i for i, q in enumerate(sorted(leaf_maz_ids.keys()))}
        self._save_attr_values("NODE", ["@maz_root", "@maz_leaf"])
        # forbid egress from MAZ nodes which are not demand roots /
        #        access to MAZ nodes which are not demand leafs
        self._net_calc.add_calc("@link_cost_maz", "@link_cost")
        self._net_calc.add_calc("@link_cost_maz", "1e20", "@maz_root=0 and !@mazseq=0")
        self._net_calc.add_calc(
            "@link_cost_maz", "1e20", "@maz_leafj=0 and !@mazseqj=0"
        )
        self._net_calc.run()
        self._debug_report.append(
            "    FIND ROOTS&LEAVES --- %s seconds ---" % (_time.time() - start_time)
        )

    def _run_shortest_path(self, bin_no, max_radius):
        start_time = _time.time()
        shortest_paths_tool = self._modeller.tool(
            "inro.emme.network_calculation.shortest_path"
        )
        # TODO: temp binary path files / path file directory
        max_radius = max_radius * 5280 + 100  # add some buffer for rounding error
        shortest_paths_tool(
            modes=self._modes,
            roots_attribute="@maz_root",
            leafs_attribute="@maz_leaf",
            link_cost_attribute="@link_cost_maz",
            num_processors=self._num_processors,
            direction="FORWARD",
            paths_file=_join(self._root_dir, f"shortest_paths_{bin_no}.ebp"),
            export_format_paths="BINARY",
            through_leaves=False,
            through_centroids=False,
            max_radius=max_radius,
        )
        self._debug_report.append(
            "    RUN SP %s, %s --- %s seconds ---"
            % (bin_no, max_radius / 5280, _time.time() - start_time)
        )

    def _assign_flow(self, bin_no, demand):
        start_time = _time.time()
        # NOTE: can add additional report details with log levels
        with open(
            _join(self._root_dir, f"shortest_paths_{bin_no}.ebp"), "rb"
        ) as paths_file:
            # read first 4 integers from file
            header = _array.array("I")
            header.fromfile(paths_file, 4)
            _, _, roots_nb, leafs_nb = header
            # read list of positions by orig-dest index, for list of path node IDs in file
            path_indicies = _array.array("I")
            path_indicies.fromfile(paths_file, roots_nb * leafs_nb + 1)
            assigned = 0
            not_assigned = 0
            offset = roots_nb * leafs_nb + 1 + 4
            bytes_read = offset
            # for all orig-dest pairs with demand, load path from file
            for data in demand:
                # get file position based on orig-dest index
                p_index = self._root_index[data["orig"].number]
                q_index = self._leaf_index[data["dest"].number]
                index = p_index * leafs_nb + q_index
                start = path_indicies[index]
                end = path_indicies[index + 1]

                # no path found, likely disconnected zone
                if start == end:
                    not_assigned += 1
                    continue
                paths_file.seek((start + offset) * 4)
                # load sequence of Node IDs which define the path
                path = _array.array("I")
                path.fromfile(paths_file, end - start)
                bytes_read += end - start
                path_iter = iter(path)
                i_node = next(path_iter)
                for j_node in path_iter:
                    link = self._network.link(i_node, j_node)
                    link["temp_flow"] += data["dem"]
                    i_node = j_node
                assigned += 1

        self._debug_report.append(
            "    ASSIGN bin %s, total %s, assign %s, not assign %s, bytes %s  --- %s seconds ---"
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
