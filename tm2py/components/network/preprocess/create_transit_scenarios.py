"""Module for creating the time-of-day scenario for transit assignments and skims.
"""
from __future__ import annotations

from collections import defaultdict as _defaultdict
import os
from typing import List, Union, Dict, Tuple, Set
import pandas as pd

from tm2py.components.component import Component as _Component
from tm2py.components.network.preprocess.apply_fares import ApplyFares
from tm2py.emme.manager import (
    EmmeNode,
    EmmeLink,
    EmmeTransitLine,
    EmmeNetwork,
    EmmeScenario,
    EmmeMode,
)
from tm2py.emme.network import copy_tod_scenario, IDGenerator
from tm2py.logger import LogStartEnd

_CNTYPE_SPEED_MAP = {
    "CRAIL": 45.0,
    "HRAIL": 40.0,
    "LRAIL": 30.0,
    "FERRY": 15.0,
}


class CreateTransitScenarios(_Component):
    """Create the per-time period scenarios for transit"""

    @LogStartEnd("Create transit time-of-day scenarios")
    def run(self):
        ref_scenario = self._create_base_scenario()
        all_period_names = self.time_period_names()
        for period in self.config.time_periods:
            with self.logger.log_start_end(f"period {period.name}"):
                scenario = copy_tod_scenario(
                    ref_scenario, period.name, period.emme_scenario_id, all_period_names
                )
                attributes = {
                    "TRANSIT_SEGMENT": [
                        "@schedule_time",
                        "@trantime_seg",
                        "@board_cost",
                        "@invehicle_cost",
                    ],
                }
                for domain, attrs in attributes.items():
                    for name in attrs:
                        attr = scenario.extra_attribute(name)
                        if attr is not None:
                            scenario.delete_extra_attribute(name)
                        scenario.create_extra_attribute(domain, name)

                network = scenario.get_network()
                self._remove_non_period_transit_lines(network, period.name)
                if self.config.transit.get("override_connector_times", False):
                    self.prepare_connectors(network, period)
                self.distribute_nntime(network)
                self.update_link_trantime(network)
                # self.calc_link_unreliability(network, period)
                if self.config.transit.use_fares:
                    self.apply_fares(scenario, network, period.name)
                if self.config.transit.get("split_connectors_to_prevent_walk", True):
                    self.split_tap_connectors_to_prevent_walk(network)
                # missing the input data files for apply station attributes
                # self.apply_station_attributes(input_dir, network)
                scenario.publish_network(network)

    @LogStartEnd("prepare base transit scenario")
    def _create_base_scenario(self):
        """Update base (all period) transit scenario.

        Includes the following steps:
            - update the emmebank dimensions to fit expected requirements
            - creates transit time functions
            - creates derived extra attributes for calculations
            - copies calculated attributes values from auto network
                @area_type, @capclass, @free_flow_speed, @free_flow_time
            - create modes and vehicles from transit config
            - set link times (@trantime), modes
            - set line vehicles and in vehicle time factors
        """
        emmebank_path = self.get_abs_path(self.config.emme.transit_database_path)
        emmebank = self.controller.emme_manager.emmebank(emmebank_path)
        required_dims = {
            "full_matrices": 9999,
            "scenarios": 6,
            "regular_nodes": 650000,
            "links": 1900000,
            "transit_vehicles": 200,
            "transit_segments": 1800000,
            "extra_attribute_values": 200000000,
        }
        self.controller.emme_manager.change_emmebank_dimensions(emmebank, required_dims)
        for ident in ["ft1", "ft2", "ft3"]:
            if emmebank.function(ident):
                emmebank.delete_function(ident)
        # for zero-cost links
        emmebank.create_function("ft1", "0")
        # segment travel time pre-calculated and stored in data1 (copied from @trantime_seg)
        emmebank.create_function("ft2", "us1")

        ref_scenario = emmebank.scenario(self.config.emme.all_day_scenario_id)
        self._create_attributes(ref_scenario)
        network = ref_scenario.get_network()
        self._copy_auto_attrs(network)
        modes = self._create_transit_modes(network)
        self._create_transit_vehicles(network)
        self._set_link_trantime(network)
        self._set_line_vehicle(network)
        self._set_link_modes(network, modes)
        ref_scenario.publish_network(network)
        return ref_scenario

    @staticmethod
    def _create_attributes(scenario: EmmeScenario):
        """Create calculated extra attributes in scenario"""
        attributes = {
            "LINK": [
                "@trantime",
                "@area_type",
                "@capclass",
                "@free_flow_speed",
                "@free_flow_time",
            ],
            "TRANSIT_LINE": ["@invehicle_factor"],
        }
        for domain, attrs in attributes.items():
            for name in attrs:
                if scenario.extra_attribute(name) is None:
                    scenario.create_extra_attribute(domain, name)

    def _copy_auto_attrs(self, network: EmmeNetwork):
        """copy link attributes from auto network to transit network"""
        manager = self.controller.emme_manager
        auto_emmebank_path = self.get_abs_path(self.config.emme.highway_database_path)
        auto_emmebank = manager.emmebank(auto_emmebank_path)
        auto_scenario = auto_emmebank.scenario(self.config.emme.all_day_scenario_id)

        copy_attrs = ["@area_type", "@capclass", "@free_flow_speed", "@free_flow_time"]
        auto_network = manager.get_network(auto_scenario, {"LINK": copy_attrs})
        link_lookup = {}
        for link in auto_network.links():
            link_lookup[link["#link_id"]] = link
        for link in network.links():
            auto_link = link_lookup.get(link["#link_id"])
            if not auto_link:
                continue
            for attr in copy_attrs:
                link[attr] = auto_link[attr]

    def _create_transit_modes(self, network: EmmeNetwork) -> Dict[str, Set[str]]:
        mode_table = self.config.transit.modes
        modes = {
            "walk": set(),
            "access": set(),
            "egress": set(),
            "local": set(),
            "premium": set(),
        }
        for mode_data in mode_table:
            mode = network.mode(mode_data["mode_id"])
            if mode is None:
                mode = network.create_mode(
                    mode_data["assign_type"], mode_data["mode_id"]
                )
            elif mode.type != mode_data["assign_type"]:
                raise Exception(
                    f"mode {mode_data['mode_id']} already exists with type "
                    f"{mode.type} instead of {mode_data['assign_type']}"
                )
            mode.description = mode_data["name"]
            if mode_data["assign_type"] == "AUX_TRANSIT":
                mode.speed = mode_data["speed_miles_per_hour"]
            if mode_data["type"] == "WALK":
                modes["walk"].add(mode.id)
            elif mode_data["type"] == "ACCESS":
                modes["access"].add(mode.id)
            elif mode_data["type"] == "EGRESS":
                modes["egress"].add(mode.id)
            elif mode_data["type"] == "LOCAL":
                modes["local"].add(mode.id)
            elif mode_data["type"] == "PREMIUM":
                modes["premium"].add(mode.id)
        return modes

    def _create_transit_vehicles(self, network: EmmeNetwork):
        """Create transit vehicles corresponding to transit config."""
        vehicle_table = self.config.transit.vehicles
        for veh_data in vehicle_table:
            vehicle = network.transit_vehicle(veh_data["vehicle_id"])
            if vehicle is None:
                vehicle = network.create_transit_vehicle(
                    veh_data["vehicle_id"], veh_data["mode"]
                )
            elif vehicle.mode.id != veh_data["mode"]:
                raise Exception(
                    f"vehicle {veh_data['vehicle_id']} already exists with "
                    f"mode {vehicle.mode.id} instead of {veh_data['mode']}"
                )
            vehicle.auto_equivalent = veh_data["auto_equivalent"]
            vehicle.seated_capacity = veh_data["seated_capacity"]
            vehicle.total_capacity = veh_data["total_capacity"]

    @staticmethod
    def _set_link_trantime(network: EmmeNetwork):
        """set fixed guideway times, and initial free flow auto link times"""
        for link in network.links():
            speed = _CNTYPE_SPEED_MAP.get(link["#cntype"])
            if speed is None:
                speed = link["@free_flow_speed"]
                if link["@ft"] == 1 and speed > 0:
                    link["@trantime"] = 60 * link.length / speed
                elif speed > 0:
                    link["@trantime"] = (
                        60 * link.length / speed + link.length * 5 * 0.33
                    )
            else:
                link["@trantime"] = 60 * link.length / speed
            # set TAP connector distance to 60 feet
            if link.i_node.is_centroid or link.j_node.is_centroid:
                link.length = 0.01  # 60.0 / 5280.0

    def _set_line_vehicle(self, network: EmmeNetwork):
        """Set transit line vehicle types and in-vehicle time factor"""
        in_vehicle_factors = {}
        default_in_vehicle_factor = self.config.transit.get(
            "in_vehicle_perception_factor", 1.0
        )
        for mode in self.config.transit.modes:
            in_vehicle_factors[mode.mode_id] = mode.get(
                "in_vehicle_perception_factor", default_in_vehicle_factor
            )
        for line in network.transit_lines():
            line_veh = network.transit_vehicle(line["#mode"])
            if line_veh is None:
                raise Exception(
                    f"line {line.id} requires vehicle ('#mode') "
                    f"{line['#mode']} which does not exist"
                )
            line_mode = line_veh.mode.id
            for seg in line.segments():
                seg.link.modes |= {line_mode}
            line.vehicle = line_veh
            # Set the perception factor from the mode table
            line["@invehicle_factor"] = in_vehicle_factors[line.vehicle.mode.id]

    def _set_link_modes(self, network: EmmeNetwork, modes: Dict[str, Set[str]]):
        """set link modes to the minimum set"""
        auto_mode = {self.config.highway.generic_highway_mode_code}
        for link in network.links():
            # get used transit modes on link
            link_modes = {seg.line.mode for seg in link.segments()}
            # add in available modes based on link type
            if link["@drive_link"]:
                link_modes |= modes["local"]
                link_modes |= auto_mode
            if link["@bus_only"]:
                link_modes |= modes["local"]
            if link["@rail_link"] and not modes:
                link_modes |= modes["premium"]
            # add access, egress or walk mode (auxilary transit modes)
            if link.i_node.is_centroid:
                link_modes |= modes["egress"]
            elif link.j_node.is_centroid:
                link_modes |= modes["access"]
            elif link["@walk_link"]:
                link_modes |= modes["walk"]
            if not link_modes:  # in case link is unused, give it the auto mode
                link.modes = auto_mode
            else:
                link.modes = link_modes

    @staticmethod
    def _remove_non_period_transit_lines(network: EmmeNetwork, period_name: str):
        """removed transit lines from other periods from per-period scenarios"""
        period_name = period_name.lower()
        for line in network.transit_lines():
            if line["#time_period"].lower() != period_name:
                network.delete_transit_line(line)

    def prepare_connectors(self, network: EmmeNetwork, period: str):
        """Prepare connectors for updating access / egress times"""
        for node in network.centroids():
            for link in node.outgoing_links():
                network.delete_link(link.i_node, link.j_node)
            for link in node.incoming_links():
                network.delete_link(link.i_node, link.j_node)
        access_modes = set()
        egress_modes = set()
        for mode_data in self.config.transit.modes:
            if mode_data["type"] == "ACCESS":
                access_modes.add(network.mode(mode_data["mode_id"]))
            if mode_data["type"] == "EGRESS":
                egress_modes.add(network.mode(mode_data["mode_id"]))
        tazs = dict((int(n["@taz_id"]), n) for n in network.centroids())
        nodes = dict((int(n["#node_id"]), n) for n in network.regular_nodes())
        self._create_connectors(
            self.get_abs_path(self.config.transit.input_connector_access_times_path),
            period.lower(),
            network,
            access_modes,
            tazs,
            "from_taz",
            nodes,
            "to_stop",
        )
        self._create_connectors(
            self.get_abs_path(self.config.transit.input_connector_egress_times_path),
            period.lower(),
            network,
            egress_modes,
            nodes,
            "from_stop",
            tazs,
            "to_taz",
        )

    # pylint: disable=R0913
    # Disable too many arguments, 8 is OK
    @staticmethod
    def _create_connectors(
        path: str,
        period_name: str,
        network: EmmeNetwork,
        modes: Set[EmmeMode],
        from_index: Dict[int, EmmeNode],
        from_name: str,
        to_index: Dict[int, EmmeNode],
        to_name: str,
    ):
        """Create connectors from references in connector times file."""
        with open(path, "r", encoding="utf8") as connectors:
            header = next(connectors).split(",")
            for line in connectors:
                tokens = line.split(",")
                data = dict(zip(header, tokens))
                if data["time_period"].lower() == period_name:
                    from_node = from_index[int(data[from_name])]
                    to_node = to_index[int(data[to_name])]
                    if network.link(from_node, to_node) is None:
                        network.create_link(from_node, to_node, modes)

    @staticmethod
    def distribute_nntime(network: EmmeNetwork):
        """Distribute in segment NNTIME (@nntime) to "@schedule_time" along route"""
        for line in network.transit_lines():
            total_nntime = sum(
                segment["@nntime"] for segment in line.segments(include_hidden=True)
            )
            if total_nntime == 0:
                continue
            total_length = 0
            segments_for_current_nntime = []
            for segment in line.segments(include_hidden=True):
                nntime = segment["@nntime"]
                if nntime > 0:
                    for nn_seg in segments_for_current_nntime:
                        nn_seg["@schedule_time"] = nntime * (
                            nn_seg.link.length / total_length
                        )
                    segments_for_current_nntime = []
                    total_length = 0
                segments_for_current_nntime.append(segment)
                total_length += segment.link.length if segment.link else 0

    @staticmethod
    def update_link_trantime(network: EmmeNetwork):
        """if nntime exists, use that for ivtt, else use the link trantime"""
        for line in network.transit_lines():
            for segment in line.segments(include_hidden=False):
                if segment["@schedule_time"] > 0:
                    segment.data1 = segment["@trantime_seg"] = segment["@schedule_time"]
                else:
                    segment.data1 = segment["@trantime_seg"] = segment.link["@trantime"]
                segment.transit_time_func = 2

    def split_tap_connectors_to_prevent_walk(self, network: EmmeNetwork):
        """Split tap connectors to create virtual TAP access/egress stops"""
        split_connectors = SplitTapConnectors(network, self.config.transit.modes)
        split_connectors.run()

    def apply_fares(self, scenario: EmmeScenario, network: EmmeNetwork, period):
        """Process input fare data."""
        apply_fares = ApplyFares(self.controller)
        apply_fares.scenario = scenario
        apply_fares.network = network
        apply_fares.period = period
        apply_fares.run()

    def apply_station_attributes(self, input_dir: str, network: EmmeNetwork):
        """Apply station attributes"""
        # reading input data
        station_tap_attributes_file = ""
        emme_node_id_xwalk_file = ""

        station_tap_attributes = pd.read_csv(
            os.path.join(input_dir, station_tap_attributes_file)
        )
        emme_node_id_xwalk = pd.read_csv(
            os.path.join(input_dir, emme_node_id_xwalk_file)
        )
        tap_to_pseudo_tap_xwalk_file = self.get_abs_path(
            os.path.join("inputs", "hwy", "tap_to_pseudo_tap_xwalk.csv")
        )
        tap_to_pseudo_tap_xwalk = pd.read_csv(
            tap_to_pseudo_tap_xwalk_file, names=["tap", "pseudo_tap"]
        )

        # buiding crosswalk between emme taps to their pseduo walk taps
        tap_and_pseudo_tap_nodes = emme_node_id_xwalk["OLD_NODE"].isin(
            tap_to_pseudo_tap_xwalk["tap"]
        ) | emme_node_id_xwalk["OLD_NODE"].isin(tap_to_pseudo_tap_xwalk["pseudo_tap"])

        emme_node_id_map = (
            emme_node_id_xwalk[tap_and_pseudo_tap_nodes]
            .set_index(["OLD_NODE"])["node_id"]
            .to_dict()
        )

        tap_to_pseudo_tap_xwalk["emme_tap"] = tap_to_pseudo_tap_xwalk["tap"].map(
            emme_node_id_map
        )
        tap_to_pseudo_tap_xwalk["emme_pseudo_tap"] = tap_to_pseudo_tap_xwalk[
            "pseudo_tap"
        ].map(emme_node_id_map)

        # only need to loop over taps matched to stations
        for tap_id in station_tap_attributes["tap"].unique():
            # modifying station platform times for lines departing from tap
            # stop_nodes_with_platform_time, walk_links_overridden = \
            self._modify_station_platform_times(
                network, tap_id, tap_to_pseudo_tap_xwalk
            )
        # self.logger.log(f"Number of nodes set with new platform "
        # f"time {len(stop_nodes_with_platform_time)}")
        # self.logger.log(f"Number of walk links set with new walk "
        # f"time {len(walk_links_overridden)}")

    @staticmethod
    def _modify_station_platform_times(network, tap_id, tap_to_pseudo_tap_xwalk):
        stop_nodes_with_platform_time = []
        walk_links_overridden = []
        max_station_walk_distance = 100
        tap = network.node(tap_id)
        for link in tap.outgoing_links():
            jnode = link.j_node
            modes_serviced = list(
                set(
                    str(segment.line["#src_mode"])
                    for segment in jnode.outgoing_segments()
                )
            )
            # setting tap station platform time on tap connector nodes
            # for stops servicing rail
            if any(x in modes_serviced for x in ["c", "l", "h"]):
                jnode["@stplatformtime"] = tap["@stplatformtime"]
                stop_nodes_with_platform_time.append(jnode.id)

        # walk transfer links are separated onto separate pseudo taps
        pseudo_tap_id = tap_to_pseudo_tap_xwalk.loc[
            tap_to_pseudo_tap_xwalk["emme_tap"] == tap_id, "emme_pseudo_tap"
        ]
        pseudo_tap = network.node(pseudo_tap_id)

        # setting bus walk times for outgoing walk transfer links
        for walk_link in pseudo_tap.outgoing_links():
            if walk_link["@feet"] < max_station_walk_distance:
                if tap["@stbuswalktime"] > 0:
                    walk_link["@walktime"] = tap["@stbuswalktime"]
                    walk_link.data2 = tap["@stbuswalktime"]  # ul2
                    walk_links_overridden.append(walk_link.id)
        # setting for incoming links
        for walk_link in pseudo_tap.incoming_links():
            if walk_link["@feet"] < max_station_walk_distance:
                if tap["@stbuswalktime"] > 0:
                    walk_link["@walktime"] = tap["@stbuswalktime"]
                    walk_link.data2 = tap["@stbuswalktime"]  # ul2
                    walk_links_overridden.append(walk_link.id)
        return stop_nodes_with_platform_time, walk_links_overridden


class SplitTapConnectors:
    """Tk tk

    Args
    """

    # pylint: disable=R0902
    def __init__(
        self, network: EmmeNetwork, mode_table: List[Dict[str, Union[str, float]]]
    ):

        # disable too many instance attributes
        self.network = network
        self.mode_table = mode_table
        self.new_node_id = IDGenerator(1, network)

        self.node_attributes = network.attributes("NODE")
        self.node_attributes.remove("x")
        self.node_attributes.remove("y")
        self.link_attributes_reset = ["length"]
        self.line_attributes = network.attributes("TRANSIT_LINE")
        self.seg_attributes = network.attributes("TRANSIT_SEGMENT")
        # attributes referring to in-vehicle components which should be set
        # to 0 on virtual stop segments
        self.seg_invehicle_attrs = [
            "@invehicle_cost",
            "data1",
            "@trantime_seg",
            "@schedule_time",
            "@nntime",
        ]

        self.tap_stops = None
        self.all_transit_modes = None
        self.walk_modes = None
        self.access_modes = None
        self.egress_modes = None

    def run(self):
        """Run split connectors and reroute transit lines"""
        network = self.network
        self.tap_stops = _defaultdict(lambda: [])
        self.process_modes()
        # Mark TAP adjacent stops and split TAP connectors
        for centroid in network.centroids():
            out_links = list(centroid.outgoing_links())
            for link in out_links:
                self.process_tap_connector(link, centroid)

        # re-route the transit lines through the new TAP-stops
        for line in network.transit_lines():
            self.reroute_line(line)

    def process_modes(self):
        """Prepare mode references for newly created links."""
        self.all_transit_modes = set(
            mode for mode in self.network.modes() if mode.type == "TRANSIT"
        )
        self.walk_modes = set()
        self.access_modes = set()
        self.egress_modes = set()
        for mode_data in self.mode_table:
            if mode_data["type"] == "WALK":
                self.walk_modes.add(self.network.mode(mode_data["mode_id"]))
            if mode_data["type"] == "ACCESS":
                self.access_modes.add(self.network.mode(mode_data["mode_id"]))
            if mode_data["type"] == "EGRESS":
                self.egress_modes.add(self.network.mode(mode_data["mode_id"]))

    def process_tap_connector(self, link: EmmeLink, centroid: EmmeNode):
        """Process the tap connector and if it is adjacent to a stop, split the link."""
        network = link.network
        real_stop = link.j_node
        has_adjacent_walk_links = False
        for stop_link in real_stop.outgoing_links():
            if stop_link == link.reverse_link:
                continue
            if self.walk_modes.intersection(stop_link.modes):
                has_adjacent_walk_links = True
            if self.egress_modes.intersection(stop_link.modes):
                has_adjacent_walk_links = True

        if has_adjacent_walk_links:
            link_length = link.length
            tap_stop = network.split_link(
                centroid,
                real_stop,
                next(self.new_node_id),
                include_reverse=True,
                proportion=0.5,
            )
            for attr in self.node_attributes:
                tap_stop[attr] = real_stop[attr]
            self.tap_stops[real_stop].append(tap_stop)
            for i_node, j_node in [
                (real_stop, tap_stop),
                (tap_stop, real_stop),
            ]:
                t_link = network.link(i_node, j_node)
                if t_link is None:
                    t_link = network.create_link(i_node, j_node, self.all_transit_modes)
                else:
                    t_link.modes = self.all_transit_modes
                for attr in self.link_attributes_reset:
                    t_link[attr] = 0
            egress_link = network.link(tap_stop, centroid)
            if egress_link:
                egress_link.modes = self.egress_modes
                egress_link.length = link_length
            access_link = network.link(centroid, tap_stop)
            if access_link:
                access_link.modes = self.access_modes
                access_link.length = link_length

    def reroute_line(self, line: EmmeTransitLine):
        """Process and line and reroute segments via virtual TAP-adjacent stops"""
        seg_data = self.get_segment_data(line)
        itinerary, tap_segments = self.get_new_itinerary(line)
        if tap_segments:
            # store line data for re-routing
            line_data = dict((k, line[k]) for k in self.line_attributes)
            line_data["id"] = line.id
            line_data["vehicle"] = line.vehicle
            # delete old line, then re-create on new, re-routed itinerary
            self.network.delete_transit_line(line)

            new_line = self.network.create_transit_line(
                line_data.pop("id"), line_data.pop("vehicle"), itinerary
            )
            # copy line attributes back
            for attr, value in line_data.items():
                new_line[attr] = value
            # copy segment attributes back
            for seg in new_line.segments(include_hidden=True):
                data = seg_data.get((seg.i_node, seg.j_node, seg.loop_index), {})
                for attr, value in data.items():
                    seg[attr] = value
            self.update_tap_segments(new_line, tap_segments)

    def get_segment_data(self, line: EmmeTransitLine) -> Dict:
        """store segment data for re-routing"""
        seg_data = {}
        for seg in line.segments(include_hidden=True):
            seg_data[(seg.i_node, seg.j_node, seg.loop_index)] = dict(
                (k, seg[k]) for k in self.seg_attributes
            )
        return seg_data

    def get_new_itinerary(
        self, line: EmmeTransitLine
    ) -> Tuple[List[int], List[Dict[str, Union[int, List[int]]]]]:
        """Get new itinerary rerouted up and down via the virtual tap adjacent stops.

        Returns new itinerary as list of node IDs, and list of tap segment references:
           {"access": list of <new TAP access segment index>,
            "egress": list of <new TAP egress segment index>,
            "real": <original, real incoming stop segment}
        """
        itinerary = []
        tap_segments = []
        for seg in line.segments(include_hidden=True):
            itinerary.append(seg.i_node.number)
            if seg.i_node in self.tap_stops and (
                seg.allow_boardings or seg.allow_alightings
            ):
                # insert tap_stop, real_stop loop after tap_stop
                real_stop = seg.i_node
                tap_access = []
                tap_egress = []
                for tap_stop in self.tap_stops[real_stop]:
                    itinerary.extend([tap_stop.number, real_stop.number])
                    tap_access.append(len(itinerary) - 3)
                    tap_egress.append(len(itinerary) - 2)
                real_seg = len(itinerary) - 1
                # track new segments to update stopping pattern
                tap_segments.append(
                    {"access": tap_access, "egress": tap_egress, "real": real_seg}
                )
        return itinerary, tap_segments

    def update_tap_segments(
        self,
        new_line: EmmeTransitLine,
        tap_segments: List[Dict[str, Union[int, List[int]]]],
    ):
        """Set boarding, alighting and dwell time on new tap access / egress segments"""
        for tap_ref in tap_segments:
            real_seg = new_line.segment(tap_ref["real"])
            for access_ref in tap_ref["access"]:
                access_seg = new_line.segment(access_ref)
                for k in self.seg_attributes:
                    access_seg[k] = real_seg[k]
                access_seg.allow_boardings = False
                access_seg.allow_alightings = False
                access_seg.transit_time_func = 1  # special 0-cost ttf
                for attr_name in self.seg_invehicle_attrs:
                    access_seg[attr_name] = 0
                access_seg.dwell_time = 0

            first_access_seg = new_line.segment(tap_ref["access"][0])
            first_access_seg.allow_alightings = real_seg.allow_alightings
            first_access_seg.dwell_time = real_seg.dwell_time

            for egress_ef in tap_ref["egress"]:
                egress_seg = new_line.segment(egress_ef)
                for k in self.seg_attributes:
                    egress_seg[k] = real_seg[k]
                egress_seg.allow_boardings = real_seg.allow_boardings
                egress_seg.allow_alightings = real_seg.allow_alightings
                egress_seg.transit_time_func = 1  # special 0-cost ttf
                for attr_name in self.seg_invehicle_attrs:
                    egress_seg[attr_name] = 0
                egress_seg.dwell_time = 0

            real_seg.allow_alightings = False
            real_seg.dwell_time = 0
