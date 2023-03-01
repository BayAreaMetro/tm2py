"""Module for highway network preparation steps.

Creates required attributes and populates input values needed
for highway assignments. The toll values, VDFs, per-class cost
(tolls+operating costs), modes and skim link attributes are calculated.

The following link attributes are used as input:
    - "@capclass": link capclass index
    - "length": standard link length, in miles
    - "@tollbooth": label to separate bridgetolls from valuetolls
    - "@tollseg": toll segment, used to index toll value lookups from the toll file
        (under config.highway.tolls.file_path)
    - "@ft": functional class, used to assign VDFs

The following keys and tables are used from the config:
    highway.tolls.file_path: relative path to input toll file
    highway.tolls.src_vehicle_group_names: names used in tolls file for
        toll class values
    highway.tolls.dst_vehicle_group_names: corresponding names used in
        network attributes toll classes
    highway.tolls.valuetoll_start_tollbooth_code: index to split point bridge tolls
        (< this value) from distance value tolls (>= this value)
    highway.classes: the list of assignment classes, see the notes under
        highway_assign for detailed explanation
    highway.capclass_lookup: the lookup table mapping the link @capclass setting
        to capacity (@capacity), free_flow_speed (@free_flow_speec) and
        critical_speed (used to calculate @ja for akcelik type functions)
    highway.generic_highway_mode_code: unique (with other mode_codes) single
        character used to label entire auto network in Emme
    highway.maz_to_maz.mode_code: unique (with other mode_codes) single
        character used to label MAZ local auto network including connectors

The following link attributes are created (overwritten) and are subsequently used in
the highway assignments.
    - "@flow_XX": link PCE flows per class, where XX is the class name in the config
    - "@maz_flow": Assigned MAZ-to-MAZ flow

The following attributes are calculated:
    - vdf: volume delay function to use
    - "@capacity": total link capacity
    - "@ja": akcelik delay parameter
    - "@hov_length": length with HOV lanes
    - "@toll_length": length with tolls
    - "@bridgetoll_YY": the bridge toll for class subgroup YY
    - "@valuetoll_YY": the "value", non-bridge toll for class subgroup YY
    - "@cost_YY": total cost for class YY
"""

import os
from typing import TYPE_CHECKING, Dict, List, Set

import pandas as pd

from tm2py.components.component import Component, FileFormatError
from tm2py.emme.manager import EmmeNetwork, EmmeScenario
from tm2py.logger import LogStartEnd

import heapq as _heapq

if TYPE_CHECKING:
    from tm2py.controller import RunController


class PrepareNetwork(Component):
    """Highway network preparation."""

    def __init__(self, controller: "RunController"):
        """Constructor for PPrepareNetwork.

        Args:
            controller (RunController): Reference to run controller object.
        """
        super().__init__(controller)
        self.config = self.controller.config.highway
        self._emme_manager = self.controller.emme_manager
        self._highway_emmebank = None
        self._highway_scenarios = None

    @LogStartEnd("Prepare network attributes and modes")
    def run(self):
        """Run network preparation step."""
        for time in self.time_period_names:
            with self.controller.emme_manager.logbook_trace(
                f"prepare for highway assignment {time}"
            ):
                scenario = self.highway_emmebank.scenario(time)
                self._create_class_attributes(scenario, time)
                network = scenario.get_network()
                self._set_tolls(network, time)
                self._set_vdf_attributes(network, time)
                self._set_link_modes(network)
                self._calc_link_skim_lengths(network)
                self._calc_link_class_costs(network)
                self._calc_interchange_distance(network)
                self._calc_link_static_reliability(network)
                scenario.publish_network(network)

    @property
    def highway_emmebank(self):
        if not self._highway_emmebank:
            self._highway_emmebank = self.controller.emme_manager.highway_emmebank
        return self._highway_emmebank

    @property
    def highway_scenarios(self):
        if self._highway_scenarios is None:
            self._highway_scenarios = {
                tp: self.highway_emmebank.scenario(tp) for tp in self.time_period_names
            }
        return self._highway_scenarios

    def validate_inputs(self):
        """Validate inputs files are correct, raise if an error is found."""
        toll_file_path = self.get_abs_path(self.config.tolls.file_path)
        if not os.path.exists(toll_file_path):
            self.logger.log(
                f"Tolls file (config.highway.tolls.file_path) does not exist: {toll_file_path}",
                level="ERROR",
            )
            raise FileNotFoundError(f"Tolls file does not exist: {toll_file_path}")
        src_veh_groups = self.config.tolls.src_vehicle_group_names
        columns = ["fac_index"]
        for time in self.controller.config.time_periods:
            for vehicle in src_veh_groups:
                columns.append(f"toll{time.name.lower()}_{vehicle}")
        with open(toll_file_path, "r", encoding="UTF8") as toll_file:
            header = set(h.strip() for h in next(toll_file).split(","))
            missing = []
            for column in columns:
                if column not in header:
                    missing.append(column)
                    self.logger.log(
                        f"Tolls file missing column: {column}", level="ERROR"
                    )
        if missing:
            raise FileFormatError(
                f"Tolls file missing {len(missing)} columns: {', '.join(missing)}"
            )

    def _create_class_attributes(self, scenario: EmmeScenario, time_period: str):
        """Create required network attributes including per-class cost and flow attributes."""
        create_attribute = self.controller.emme_manager.tool(
            "inro.emme.data.extra_attribute.create_extra_attribute"
        )
        attributes = {
            "LINK": [
                ("@capacity", "total link capacity"),
                ("@ja", "akcelik delay parameter"),
                ("@maz_flow", "Assigned MAZ-to-MAZ flow"),
                ("@hov_length", "length with HOV lanes"),
                ("@toll_length", "length with tolls"),
                ("@intdist_down", "dist to the closest d-stream interchange"),
                ("@intdist_up", "dist from the closest upstream int"),
                ("@static_rel", "static reliability"),
                ("@reliability", "link total reliability"),
                ("@reliability_sq", "link total reliability variance"),
                ("@auto_time", "link total reliability"),
            ],
            "NODE": [
                ("@interchange", "interchange"),
            ]
        }
        # toll field attributes by bridge and value and toll definition
        dst_veh_groups = self.config.tolls.dst_vehicle_group_names
        for dst_veh in dst_veh_groups:
            for toll_type in "bridge", "value":
                attributes["LINK"].append(
                    (
                        f"@{toll_type}toll_{dst_veh}",
                        f"{toll_type} toll value for {dst_veh}",
                    )
                )
        # results for link cost and assigned flow
        for assign_class in self.config.classes:
            attributes["LINK"].append(
                (
                    f"@cost_{assign_class.name.lower()}",
                    f'{time_period} {assign_class["description"]} total costs'[:40],
                )
            )
            attributes["LINK"].append(
                (
                    f"@flow_{assign_class.name.lower()}",
                    f'{time_period} {assign_class["description"]} link volume'[:40],
                )
            )
        for domain, attrs in attributes.items():
            for name, desc in attrs:
                create_attribute(domain, name, desc, overwrite=True, scenario=scenario)

    def _set_tolls(self, network: EmmeNetwork, time_period: str):
        """Set the tolls in the network from the toll reference file."""
        toll_index = self._get_toll_indices()
        src_veh_groups = self.config.tolls.src_vehicle_group_names
        dst_veh_groups = self.config.tolls.dst_vehicle_group_names
        valuetoll_start_tollbooth_code = (
            self.config.tolls.valuetoll_start_tollbooth_code
        )
        for link in network.links():
            # set bridgetoll
            if (
                link["@tollbooth"] > 0
                and link["@tollbooth"] < valuetoll_start_tollbooth_code
            ):
                index = int(
                    link["@tollbooth"] * 1000
                    + link["@tollseg"] * 10
                    + link["@useclass"]
                )
                data_row = toll_index.get(index)
                if data_row is None:
                    self.logger.warn(
                        f"set tolls failed index lookup {index}, link {link.id}",
                        indent=True,
                    )
                    continue  # tolls will remain at zero
                for src_veh, dst_veh in zip(src_veh_groups, dst_veh_groups):
                    link[f"@bridgetoll_{dst_veh}"] = (
                        float(data_row[f"toll{time_period.lower()}_{src_veh}"]) * 100
                    )
            # set valuetoll
            elif link["@tollbooth"] >= valuetoll_start_tollbooth_code:
                data_row = toll_index.get(index)
                if data_row is None:
                    self.logger.warn(
                        f"set tolls failed index lookup {index}, link {link.id}",
                        indent=True,
                    )
                    continue  # tolls will remain at zero
                for src_veh, dst_veh in zip(src_veh_groups, dst_veh_groups):
                    link[f"@valuetoll_{dst_veh}"] = (
                        float(data_row[f"toll{time_period.lower()}_{src_veh}"])
                        * link.length
                        * 100
                    )
            else:
                continue

    def _get_toll_indices(self) -> Dict[int, Dict[str, str]]:
        """Get the mapping of toll lookup table from the toll reference file."""
        toll_file_path = self.get_abs_path(self.config.tolls.file_path)
        self.logger.debug(f"toll_file_path {toll_file_path}", indent=True)
        tolls = {}
        with open(toll_file_path, "r", encoding="UTF8") as toll_file:
            header = [h.strip() for h in next(toll_file).split(",")]
            for line in toll_file:
                data = dict(zip(header, line.split(",")))
                tolls[int(data["fac_index"])] = data
        return tolls

    def _set_vdf_attributes(self, network: EmmeNetwork, time_period: str):
        """Set capacity, VDF and critical speed on links."""
        capacity_map = {}
        critical_speed_map = {}
        for row in self.config.capclass_lookup:
            if row.get("capacity") is not None:
                capacity_map[row["capclass"]] = row.get("capacity")
            if row.get("critical_speed") is not None:
                critical_speed_map[row["capclass"]] = row.get("critical_speed")
        tp_mapping = {
            tp.name.upper(): tp.highway_capacity_factor
            for tp in self.controller.config.time_periods
        }
        period_capacity_factor = tp_mapping[time_period]
        akcelik_vdfs = [3, 4, 5, 7, 8, 10, 11, 12, 13, 14]
        for link in network.links():
            cap_lanehour = capacity_map[link["@capclass"]]
            link["@capacity"] = cap_lanehour * period_capacity_factor * link["@lanes"]
            link.volume_delay_func = int(link["@ft"])
            # re-mapping links with type 99 to type 7 "local road of minor importance"
            if link.volume_delay_func == 99:
                link.volume_delay_func = 7
            # num_lanes not used directly, but set for reference
            link.num_lanes = max(min(9.9, link["@lanes"]), 1.0)
            if link.volume_delay_func in akcelik_vdfs and link["@free_flow_speed"] > 0:
                dist = link.length
                critical_speed = critical_speed_map[link["@capclass"]]
                t_c = dist / critical_speed
                t_o = dist / link["@free_flow_speed"]
                link["@ja"] = 16 * (t_c - t_o) ** 2

    def _set_link_modes(self, network: EmmeNetwork):
        """Set the link modes based on the per-class 'excluded_links' set."""
        # first reset link modes (script run more than once)
        # "generic_highway_mode_code" must already be created (in import to Emme script)
        auto_mode = {network.mode(self.config.generic_highway_mode_code)}
        used_modes = {
            network.mode(assign_class.mode_code) for assign_class in self.config.classes
        }
        used_modes.add(network.mode(self.config.maz_to_maz.mode_code))
        for link in network.links():
            link.modes -= used_modes
            if link["@drive_link"]:
                link.modes |= auto_mode
        for mode in used_modes:
            if mode is not None:
                network.delete_mode(mode)

        # Create special access/egress mode for MAZ connectors
        maz_access_mode = network.create_mode(
            "AUX_AUTO", self.config.maz_to_maz.mode_code
        )
        maz_access_mode.description = "MAZ access"
        # create modes from class spec
        # (duplicate mode codes allowed provided the excluded_links is the same)
        mode_excluded_links = {}
        for assign_class in self.config.classes:
            if assign_class.mode_code in mode_excluded_links:
                if (
                    assign_class.excluded_links
                    != mode_excluded_links[assign_class.mode_code]
                ):
                    ex_links1 = mode_excluded_links[assign_class.mode_code]
                    ex_links2 = assign_class.excluded_links
                    raise Exception(
                        f"config error: highway.classes, duplicated mode codes "
                        f"('{assign_class.mode_code}') with different excluded "
                        f"links: {ex_links1} and {ex_links2}"
                    )
                continue
            mode = network.create_mode("AUX_AUTO", assign_class.mode_code)
            mode.description = assign_class.name
            mode_excluded_links[mode.id] = assign_class.excluded_links

        dst_veh_groups = self.config.tolls.dst_vehicle_group_names
        for link in network.links():
            modes = set(m.id for m in link.modes)
            if link.i_node["@maz_id"] + link.j_node["@maz_id"] > 0:
                modes.add(maz_access_mode.id)
                link.modes = modes
                continue
            if not link["@drive_link"]:
                continue
            exclude_links_map = {
                "is_sr": link["@useclass"] in [2, 3],
                "is_sr2": link["@useclass"] == 2,
                "is_sr3": link["@useclass"] == 3,
                "is_auto_only": link["@useclass"] in [2, 3, 4],
            }
            for dst_veh in dst_veh_groups:
                exclude_links_map[f"is_toll_{dst_veh}"] = (
                    link[f"@valuetoll_{dst_veh}"] > 0
                )
            self._apply_exclusions(
                self.config.maz_to_maz.excluded_links,
                maz_access_mode.id,
                modes,
                exclude_links_map,
            )
            for assign_class in self.config.classes:
                self._apply_exclusions(
                    assign_class.excluded_links,
                    assign_class.mode_code,
                    modes,
                    exclude_links_map,
                )
            link.modes = modes

    @staticmethod
    def _apply_exclusions(
        excluded_links_criteria: List[str],
        mode_code: str,
        modes_set: Set[str],
        link_values: Dict[str, bool],
    ):
        """Apply the exclusion criteria to set the link modes."""
        for criteria in excluded_links_criteria:
            if link_values[criteria]:
                return
        modes_set.add(mode_code)

    def _calc_link_skim_lengths(self, network: EmmeNetwork):
        """Calculate the length attributes used in the highway skims."""
        valuetoll_start_tollbooth_code = (
            self.config.tolls.valuetoll_start_tollbooth_code
        )
        for link in network.links():
            # distance in hov lanes / facilities
            if 2 <= link["@useclass"] <= 3:
                link["@hov_length"] = link.length
            else:
                link["@hov_length"] = 0
            # distance on non-bridge toll facilities
            if link["@tollbooth"] > valuetoll_start_tollbooth_code:
                link["@toll_length"] = link.length
            else:
                link["@toll_length"] = 0

    def _calc_link_class_costs(self, network: EmmeNetwork):
        """Calculate the per-class link cost from the tolls and operating costs."""
        for assign_class in self.config.classes:
            cost_attr = f"@cost_{assign_class.name.lower()}"
            op_cost = assign_class["operating_cost_per_mile"]
            toll_factor = assign_class.get("toll_factor")
            if toll_factor is None:
                toll_factor = 1.0
            for link in network.links():
                try:
                    toll_value = sum(
                        link[toll_attr] for toll_attr in assign_class["toll"]
                    )
                except:
                    link
                link[cost_attr] = link.length * op_cost + toll_value * toll_factor

    def _calc_interchange_distance(self, network: EmmeNetwork):
        """
        For highway reliability
        Calculate upstream and downstream interchange distance
        First, label the intersection nodes as nodes with freeway and freeway-to-freeway ramp
        """
        # input interchange nodes file
        # This is a file inherited from https://app.box.com/folder/148342877307, as implemented in the tm2.1
        interchange_nodes_file = self.get_abs_path(self.config.interchange_nodes_file)
        interchange_nodes_df = pd.read_csv(interchange_nodes_file)
        interchange_nodes_df = interchange_nodes_df[interchange_nodes_df.intx > 0]
        interchange_points = interchange_nodes_df["N"].tolist()
        network.create_attribute("NODE", "is_interchange")
        for node in network.nodes():
            if node["#node_id"] in interchange_points:
                node.is_interchange = True
                node["@interchange"] = node.is_interchange
        # The following approach is based on SANDAG's code
        # network.create_attribute("NODE", "is_interchange")
        # interchange_points = []
        # mode_c = network.mode('c')
        # for node in network.nodes():
        #     adj_links = list(node.incoming_links()) + list(node.outgoing_links())
        #     has_freeway_links = bool(
        #         [l for l in adj_links
        #             if l["@ft"] in [1,2] and mode_c in l.modes])
        #     has_ramp_links = bool(
        #         [l for l in adj_links
        #             if l["@ft"] == 3 and mode_c in l.modes])
        #     if has_freeway_links and has_ramp_links:
        #         node.is_interchange = True
        #         interchange_points.append(node)
        #     else:
        #         node.is_interchange = False
        # for node in network.nodes():
        #     node["@interchange"] = node.is_interchange
        
        mode_c = network.mode('c')
        for link in network.links():
            if link["@ft"] in [1,2] and mode_c in link.modes:
                link["@intdist_down"] = PrepareNetwork.interchange_distance(link, "DOWNSTREAM")
                link["@intdist_up"] = PrepareNetwork.interchange_distance(link, "UPSTREAM")
        
        network.delete_attribute("NODE", "is_interchange")
    
    @staticmethod
    def interchange_distance(orig_link, direction):
        visited = set([])
        visited_add = visited.add
        back_links = {}
        heap = []
        if direction == "DOWNSTREAM":
            get_links = lambda l: l.j_node.outgoing_links()
            check_far_node = lambda l: l.j_node.is_interchange
        elif direction == "UPSTREAM":
            get_links = lambda l: l.i_node.incoming_links()
            check_far_node = lambda l: l.i_node.is_interchange
        # Shortest path search for nearest interchange node along freeway
        for link in get_links(orig_link):
            _heapq.heappush(heap, (link["length"], link["#link_id"], link))
        interchange_found = False

        # Check first node
        if check_far_node(orig_link):
            interchange_found = True
            link_cost = 0.0
        
        try:
            while not interchange_found:
                link_cost, link_id, link = _heapq.heappop(heap)
                if link in visited:
                    continue
                visited_add(link)
                if check_far_node(link):
                    interchange_found = True
                    break
                get_links_return = get_links(link)
                for next_link in get_links_return:
                    if next_link in visited:
                        continue
                    next_cost = link_cost + next_link["length"]
                    _heapq.heappush(heap, (next_cost, next_link["#link_id"], next_link))
        except TypeError:
            # TypeError if the link type objects are compared in the tuples
            # case where the path cost are the same
            raise Exception("Path cost are the same, cannot compare Link objects")
        except IndexError:
            # IndexError if heap is empty
            # case where start / end of highway, dist = 99
            return 99
        return orig_link["length"] / 2.0 + link_cost

    def _calc_link_static_reliability(self, network: EmmeNetwork):
        """
        For highway reliability
        consists of: lane factor, interchange distance, speed factor
        differentiated by freeway, artertial, and others
        """
        # Static reliability parameters
        # freeway coefficients
        freeway_rel = {
            "intercept": 0.1078,
            "speed>70": 0.01393,
            "upstream": 0.011,
            "downstream": 0.0005445,
        }
        # arterial/ramp/other coefficients
        road_rel = {
            "intercept": 0.0546552,
            "lanes": {
                1: 0.0,
                2: 0.0103589,
                3: 0.0361211,
                4: 0.0446958,
                5: 0.0
            },
            "speed":  {
                "<35": 0,
                35: 0.0075674,
                40: 0.0091012,
                45: 0.0080996,
                50: -0.0022938,
                ">50": -0.0046211
            },
        }
        for link in network.links():
            # if freeway apply freeway parameters to this link
            if (link["@ft"] in [1,2]) and (link['@lanes'] > 0):
                high_speed_factor = freeway_rel["speed>70"] if link["@free_flow_speed"]>=70 else 0
                upstream_factor = freeway_rel["upstream"] * 1 / link["@intdist_up"]
                downstream_factor = freeway_rel["downstream"] * 1 / link["@intdist_down"]
                link["@static_rel"] = freeway_rel["intercept"] + high_speed_factor + upstream_factor + downstream_factor
            # arterial/ramp/other apply road parameters
            elif (link["@ft"] < 8) and (link["@lanes"] > 0):
                lane_factor = road_rel["lanes"].get(link["@lanes"],0)
                speed_bin = link["@free_flow_speed"]
                if speed_bin < 35:
                    speed_bin = "<35"
                elif speed_bin > 50:
                    speed_bin = ">50"
                speed_factor = road_rel["speed"][speed_bin]
                link["@static_rel"] = road_rel["intercept"] + lane_factor + speed_factor
            else:
                link["@static_rel"] = 0