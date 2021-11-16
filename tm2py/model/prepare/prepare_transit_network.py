

from collections import defaultdict as _defaultdict
from copy import deepcopy as _copy
import json as _json
import math as _math
from scipy.optimize import nnls as _nnls
import shapely.geometry as _geom
import time as _time
import traceback as _traceback
import os
import pandas as pd

from tm2py.core.component import Component as _Component, Controller as _Controller
import tm2py.core.emme as _emme_tools
from tm2py.core.logging import LogStartEnd

EmmeScenario = _emme_tools.EmmeScenario


class PrepareTransitNetwork(_Component):

    def __init__(self, controller: _Controller):
        """Initialize active mode skim component.

        Args:
            controller: parent Controller object
        """
        super().__init__(controller)
        self._emme_manager = _emme_tools.EmmeManager()
        self._num_processors = _emme_tools.parse_num_processors(
            self.config.emme.num_processors
        )

    @LogStartEnd("prepare transit scenarios")
    def run(self):
        emmebank_path = os.path.join(self.root_dir, self.config.emme.transit_database_path)
        emmebank = self._emme_manager.emmebank(emmebank_path)

        #ref_scenario = emmebank.scenario(self.config.emme.all_day_scenario_id)
        #network = ref_scenario.get_network()

        for period in self.config.periods:
            with self.logger.log_start_end(f"period {period.name}"):
                scenario = emmebank.scenario(period.emme_scenario_id)
                if self.controller.iteration == 0:
                    attributes = {
                        "TRANSIT_SEGMENT": ["@schedule_time", "@trantime_seg"]
                    }
                    for domain, attrs in attributes.items():
                        for name in attrs:
                            attr = scenario.extra_attribute(name)
                            if attr and attr.type != domain:
                                scenario.delete_extra_attribute(name)
                                attr = None
                            if attr is None:
                                scenario.create_extra_attribute(domain, name)

                    network = scenario.get_network()
                    self.distribute_nntime(network)
                    self.update_link_trantime(network)
                    # self.calc_link_unreliability(network, period)
                    if self.config.transit.use_fares:
                        self.apply_fares(scenario, network, period.name)
                    else:
                        scenario.create_extra_attribute("TRANSIT_SEGMENT", "@board_cost")
                        network.create_attribute("TRANSIT_SEGMENT", "@board_cost")
                        scenario.create_extra_attribute("TRANSIT_SEGMENT", "@invehicle_cost")
                        network.create_attribute("TRANSIT_SEGMENT", "@invehicle_cost")
                    self.split_tap_connectors_to_prevent_walk(network)
                    # TODO: missing the input data files for apply station attributes
                    # self.apply_station_attributes(input_dir, network)
                else:
                    network = self.update_auto_times(period, scenario)

                scenario.publish_network(network)

    def distribute_nntime(self, network):
        for line in network.transit_lines():
            total_nntime = sum(segment["@nntime"] for segment in line.segments(include_hidden=True))
            if total_nntime == 0:
                continue
            total_length = 0
            segments_for_current_nntime = []
            for segment in line.segments(include_hidden=True):
                nntime = segment["@nntime"]
                if nntime > 0:
                    for nn_seg in segments_for_current_nntime:
                        nn_seg['@schedule_time'] = nntime * (nn_seg.link.length / total_length)
                    segments_for_current_nntime = []
                    total_length = 0
                segments_for_current_nntime.append(segment)
                total_length += segment.link.length if segment.link else 0

    @staticmethod
    def update_link_trantime(network):
        # if nntime exists, use that for ivtt, else use the link trantime
        for line in network.transit_lines():
            for segment in line.segments(include_hidden=False):
                if segment['@schedule_time'] > 0:
                    segment.data1 = segment['@trantime_seg'] = segment['@schedule_time']
                else:
                    segment.data1 = segment['@trantime_seg'] = segment.link['@trantime']
                segment.transit_time_func = 2

    def split_tap_connectors_to_prevent_walk(self, network):
        tap_stops = _defaultdict(lambda: [])
        new_node_id = self.init_node_id(network)
        all_transit_modes = set([mode for mode in network.modes() if mode.type == "TRANSIT"])
        node_attributes = network.attributes("NODE")
        node_attributes.remove("x")
        node_attributes.remove("y")
        link_attributes_reset = ["length"]

        mode_table = self.config.transit.modes
        walk_modes = set()
        access_modes = set()
        egress_modes = set()
        for mode_data in mode_table:
            if mode_data["type"] == "WALK":
                walk_modes.add(network.mode(mode_data["id"]))
            if mode_data["type"] == "ACCESS":
                access_modes.add(network.mode(mode_data["id"]))
            if mode_data["type"] == "EGRESS":
                egress_modes.add(network.mode(mode_data["id"]))

        # Mark TAP adjacent stops and split TAP connectors
        for centroid in network.centroids():
            out_links = list(centroid.outgoing_links())
            for link in out_links:
                real_stop = link.j_node
                has_adjacent_transfer_links = False
                has_adjacent_walk_links = False
                for stop_link in real_stop.outgoing_links():
                    if stop_link == link.reverse_link:
                        continue
                    if walk_modes.intersection(stop_link.modes):
                        has_adjacent_transfer_links = True
                    if egress_modes.intersection(stop_link.modes):
                        has_adjacent_walk_links = True

                if has_adjacent_transfer_links or has_adjacent_walk_links:
                    length = link.length
                    new_node_id += 1
                    tap_stop = network.split_link(
                        centroid, real_stop, new_node_id, include_reverse=True, proportion=0.5)
                    for attr in node_attributes:
                        tap_stop[attr] = real_stop[attr]
                    tap_stops[real_stop].append(tap_stop)
                    transit_access_link = network.link(real_stop, tap_stop)
                    for t_link in transit_access_link, transit_access_link.reverse_link:
                        t_link.modes = all_transit_modes
                        for attr in link_attributes_reset:
                            t_link[attr] = 0
                    egress_link = network.link(tap_stop, centroid)
                    egress_link.modes = egress_modes
                    egress_link.reverse_link.modes = access_modes
                    egress_link.length = length
                    egress_link.reverse_link.length = length

        line_attributes = network.attributes("TRANSIT_LINE")
        seg_attributes = network.attributes("TRANSIT_SEGMENT")
        # attributes referring to in-vehicle components which should be set to 0 on virtual stop segments
        seg_invehicle_attrs = ["@invehicle_cost", "data1", "@trantime_seg", "@schedule_time", "@nntime"]

        # re-route the transit lines through the new TAP-stops
        for line in network.transit_lines():
            # store segment data for re-routing
            seg_data = {}
            itinerary = []
            tap_segments = []
            for seg in line.segments(include_hidden=True):
                seg_data[(seg.i_node, seg.j_node, seg.loop_index)] = dict((k, seg[k]) for k in seg_attributes)
                itinerary.append(seg.i_node.number)
                if seg.i_node in tap_stops and (seg.allow_boardings or seg.allow_alightings):
                    # insert tap_stop, real_stop loop after tap_stop
                    real_stop = seg.i_node
                    tap_access = []
                    tap_egress = []
                    for tap_stop in tap_stops[real_stop]:
                        itinerary.extend([tap_stop.number, real_stop.number])
                        tap_access.append(len(itinerary) - 3)
                        tap_egress.append(len(itinerary) - 2)
                    real_seg = len(itinerary) - 1
                    # track new segments to update stopping pattern
                    tap_segments.append({
                        "access": tap_access,
                        "egress": tap_egress,
                        "real": real_seg
                    })

            if tap_segments:
                # store line data for re-routing
                line_data = dict((k, line[k]) for k in line_attributes)
                line_data["id"] = line.id
                line_data["vehicle"] = line.vehicle
                # delete old line, then re-create on new, re-routed itinerary
                network.delete_transit_line(line)

                new_line = network.create_transit_line(
                    line_data.pop("id"),
                    line_data.pop("vehicle"),
                    itinerary)
                # copy line attributes back
                for k, v in line_data.items():
                    new_line[k] = v
                # copy segment attributes back
                for seg in new_line.segments(include_hidden=True):
                    data = seg_data.get((seg.i_node, seg.j_node, seg.loop_index), {})
                    for k, v in data.items():
                        seg[k] = v
                # set boarding, alighting and dwell time on new tap access / egress segments
                for tap_ref in tap_segments:
                    real_seg = new_line.segment(tap_ref["real"])
                    for access_ref in tap_ref["access"]:
                        access_seg = new_line.segment(access_ref)
                        for k in seg_attributes:
                            access_seg[k] = real_seg[k]
                        access_seg.allow_boardings = False
                        access_seg.allow_alightings = False
                        access_seg.transit_time_func = 1  # special 0-cost ttf
                        for attr_name in seg_invehicle_attrs:
                            access_seg[attr_name] = 0
                        access_seg.dwell_time = 0

                    first_access_seg = new_line.segment(tap_ref["access"][0])
                    first_access_seg.allow_alightings = real_seg.allow_alightings
                    first_access_seg.dwell_time = real_seg.dwell_time

                    for egress_ef in tap_ref["egress"]:
                        egress_seg = new_line.segment(egress_ef)
                        for k in seg_attributes:
                            egress_seg[k] = real_seg[k]
                        egress_seg.allow_boardings = real_seg.allow_boardings
                        egress_seg.allow_alightings = real_seg.allow_alightings
                        egress_seg.transit_time_func = 1  # special 0-cost ttf
                        for attr_name in seg_invehicle_attrs:
                            egress_seg[attr_name] = 0
                        egress_seg.dwell_time = 0

                    real_seg.allow_alightings = False
                    real_seg.dwell_time = 0

    @staticmethod
    def init_node_id(network):
        new_node_id = max(n.number for n in network.nodes())
        new_node_id = _math.ceil(new_node_id / 10000.0) * 10000
        return new_node_id

    def apply_fares(self, scenario, network, period):
        apply_fares = ApplyFares(self.controller)
        apply_fares.scenario = scenario
        apply_fares.network = network
        apply_fares.period = period
        apply_fares.run()

    def apply_station_attributes(self, input_dir, network):
        # reading input data
        station_tap_attributes_file = ""
        emme_node_id_xwalk_file = ""
        max_station_walk_distance = 100

        station_tap_attributes = pd.read_csv(os.path.join(input_dir, station_tap_attributes_file))
        emme_node_id_xwalk = pd.read_csv(os.path.join(input_dir, emme_node_id_xwalk_file))
        tap_to_pseudo_tap_xwalk_file = os.path.join(self.root_dir, "inputs", 'hwy', 'tap_to_pseudo_tap_xwalk.csv')
        tap_to_pseudo_tap_xwalk = pd.read_csv(tap_to_pseudo_tap_xwalk_file, names=['tap', 'pseudo_tap'])

        # buiding crosswalk between emme taps to their pseduo walk taps
        tap_and_pseudo_tap_nodes = (
            emme_node_id_xwalk['OLD_NODE'].isin(tap_to_pseudo_tap_xwalk['tap'])
            | emme_node_id_xwalk['OLD_NODE'].isin(tap_to_pseudo_tap_xwalk['pseudo_tap']))

        emme_node_id_map = emme_node_id_xwalk[tap_and_pseudo_tap_nodes].set_index(
                ['OLD_NODE'])['node_id'].to_dict()

        tap_to_pseudo_tap_xwalk['emme_tap'] = tap_to_pseudo_tap_xwalk['tap'].map(emme_node_id_map)
        tap_to_pseudo_tap_xwalk['emme_pseudo_tap'] = tap_to_pseudo_tap_xwalk['pseudo_tap'].map(emme_node_id_map)

        # only need to loop over taps matched to stations
        station_taps = station_tap_attributes['tap'].unique()
        stop_nodes_with_platform_time = []
        walk_links_overridden = []

        for tap_id in station_taps:
            # modifying station platform times for lines departing from tap
            tap = network.node(tap_id)
            for link in tap.outgoing_links():
                jnode = link.j_node
                modes_serviced = list(set([str(segment.line['#src_mode']) for segment in jnode.outgoing_segments()]))
                # setting tap station platform time on tap connector nodes for stops servicing rail
                if any(x in modes_serviced for x in ['c', 'l', 'h']):
                    jnode['@stplatformtime'] = tap['@stplatformtime']
                    stop_nodes_with_platform_time.append(jnode.id)

            # walk transfer links are separated onto separate pseudo taps created in BuildTransitNetworks.job
            pseudo_tap_id = tap_to_pseudo_tap_xwalk.loc[tap_to_pseudo_tap_xwalk['emme_tap'] == tap_id, 'emme_pseudo_tap']
            pseudo_tap = network.node(pseudo_tap_id)

            # setting bus walk times for outgoing walk transfer links
            for walk_link in pseudo_tap.outgoing_links():
                if walk_link['@feet'] < max_station_walk_distance:
                    if tap['@stbuswalktime'] > 0:
                        walk_link['@walktime'] = tap['@stbuswalktime']
                        walk_link.data2 = tap['@stbuswalktime']  # ul2
                        walk_links_overridden.append(walk_link.id)
            # setting for incoming links
            for walk_link in pseudo_tap.incoming_links():
                if walk_link['@feet'] < max_station_walk_distance:
                    if tap['@stbuswalktime'] > 0:
                        walk_link['@walktime'] = tap['@stbuswalktime']
                        walk_link.data2 = tap['@stbuswalktime']  # ul2
                        walk_links_overridden.append(walk_link.id)

        #print "Number of nodes set with new platform time", len(stop_nodes_with_platform_time)
        #print "Number of walk links set with new walk time", len(walk_links_overridden)

    def update_auto_times(self, period, scenario):
        auto_emmebank = self._emme_manager.emmebank(os.path.join(self.root_dir, self.config.emme.highway_database_path))
        auto_scenario = auto_emmebank.scenario(period.emme_scenario_id)
        if auto_scenario.has_traffic_results:
            auto_network = auto_scenario.get_network()
            network = scenario.get_network()
            link_lookup = {}
            for link in auto_network.links():
                link_lookup[link["#link_id"]] = link
            for link in network.links():
                auto_link = link_lookup.get(link["#link_id"])
                if not auto_link:
                    continue
                # TODO: may need to remove "reliability" factor
                auto_time = auto_link.auto_time
                if auto_time > 0:
                    link["@trantime"] = auto_time

        return network

class ApplyFares(_Component):

    def __init__(self, controller: _Controller):
        """Initialize component.

        Args:
            controller: parent Controller object
        """
        super().__init__(controller)

        self.scenario = None
        self.network = None
        self.period = ""

        self.dot_far_file = os.path.join(self.root_dir, self.config.transit.fares_path)
        self.fare_matrix_file = os.path.join(self.root_dir, self.config.transit.fare_matrix_path)

        self._log = []

    def run(self):
        self._log = []
        faresystems = self.parse_dot_far_file()
        fare_matrices = self.parse_fare_matrix_file()

        try:
            # network = self.network = self.scenario.get_network()
            network = self.network
            self.create_attribute("TRANSIT_SEGMENT", "@board_cost", self.scenario, network)
            self.create_attribute("TRANSIT_SEGMENT", "@invehicle_cost", self.scenario, network)
            # identify the lines by faresystem
            for line in network.transit_lines():
                fs_id = int(line["#faresystem"])
                try:
                    fs_data = faresystems[fs_id]
                except KeyError:
                    self._log.append({
                        "type": "text",
                        "content": (f"Line {line.id} has #faresystem '{fs_id}' which was "
                                    "not found in fares.far table")})
                    continue
                fs_data["LINES"].append(line)
                fs_data["NUM LINES"] += 1
                fs_data["NUM SEGMENTS"] += len(list(line.segments()))
                # Set final hidden segment allow_boardings to False so that the boarding cost is not
                # calculated for this segment (has no next segment)
                line.segment(-1).allow_boardings = False

            self._log.append({"type": "header", "content": "Base fares by faresystem"})
            for fs_id, fs_data in faresystems.items():
                self._log.append(
                    {"type": "text", "content": "FAREZONE {}: {} {}".format(fs_id, fs_data["STRUCTURE"], fs_data["NAME"])})
                lines = fs_data["LINES"]
                fs_data["MODE_SET"] = set(l.mode.id for l in lines)
                fs_data["MODES"] = ", ".join(fs_data["MODE_SET"])
                if fs_data["NUM LINES"] == 0:
                    self._log.append(
                        {"type": "text2", "content": "No lines associated with this faresystem"})
                elif fs_data["STRUCTURE"] == "FLAT":
                    self.generate_base_board(lines, fs_data["IBOARDFARE"])
                elif fs_data["STRUCTURE"] == "FROMTO":
                    fare_matrix = fare_matrices[fs_data["FAREMATRIX ID"]]
                    self.generate_fromto_approx(network, lines, fare_matrix, fs_data)

            self.faresystem_distances(faresystems)
            faresystem_groups = self.group_faresystems(faresystems)
            journey_levels, mode_map = self.generate_transfer_fares(
                faresystems, faresystem_groups, network)
            self.save_journey_levels("ALLPEN", journey_levels)
            local_modes = []
            premium_modes = []
            for mode in self.config.transit.modes:
                if mode.assign_type == "LOCAL":
                    local_modes.extend(mode_map[mode.id])
                if mode.assign_type == "PREMIUM":
                    premium_modes.extend(mode_map[mode.id])
            local_levels = self.filter_journey_levels_by_mode(local_modes, journey_levels)
            self.save_journey_levels("BUS", local_levels)
            premium_levels = self.filter_journey_levels_by_mode(premium_modes, journey_levels)
            self.save_journey_levels("PREM", premium_levels)

        except Exception as error:
            self._log.append({"type": "text", "content": "error during apply fares"})
            self._log.append({"type": "text", "content": str(error)})
            self._log.append({"type": "text", "content": _traceback.format_exc()})
            raise
        finally:
            log_content = []
            header = ["NUMBER", "NAME", "NUM LINES", "NUM SEGMENTS", "MODES", "FAREMATRIX ID",
                      "NUM ZONES", "NUM MATRIX RECORDS"]
            for fs_id, fs_data in faresystems.items():
                log_content.append([str(fs_data.get(h, "")) for h in header])
            self._log.insert(0, {
                "content": log_content,
                "type": "table",
                "header": header,
                "title": "Faresystem data"
            })

            self.log_report()
            self.log_text_report()

        # self.scenario.publish_network(network)

        return journey_levels

    def parse_dot_far_file(self):
        data = {}
        numbers = []
        with open(self.dot_far_file, 'r') as f:
            for line in f:
                fs_data = {}
                word = []
                key = None
                for c in line:
                    if key == "FAREFROMFS":
                        word.append(c)
                    elif c == "=":
                        key = "".join(word)
                        word = []
                    elif c == ",":
                        fs_data[key.strip()] = "".join(word)
                        key = None
                        word = []
                    elif c == "\n":
                        pass
                    else:
                        word.append(c)
                fs_data[key.strip()] = "".join(word)

                fs_data["NUMBER"] = int(fs_data["FARESYSTEM NUMBER"])
                if fs_data["STRUCTURE"] != "FREE":
                    fs_data["FAREFROMFS"] = [float(x) for x in fs_data["FAREFROMFS"].split(",")]
                if fs_data["STRUCTURE"] == "FLAT":
                    fs_data["IBOARDFARE"] = float(fs_data["IBOARDFARE"])
                elif fs_data["STRUCTURE"] == "FROMTO":
                    fmi, one, farematrix_id = fs_data["FAREMATRIX"].split(".")
                    fs_data["FAREMATRIX ID"] = int(farematrix_id)
                fs_data["LINES"] = []
                fs_data["NUM LINES"] = 0
                fs_data["NUM SEGMENTS"] = 0
                numbers.append(fs_data["NUMBER"])

                data[fs_data["NUMBER"]] = fs_data
        for fs_data in data.values():
            if "FAREFROMFS" in fs_data:
                fs_data["FAREFROMFS"] = dict(zip(numbers, fs_data["FAREFROMFS"]))
        return data

    def parse_fare_matrix_file(self):
        data = _defaultdict(lambda: _defaultdict(dict))
        with open(self.fare_matrix_file, 'r') as f:
            for i, line in enumerate(f):
                if line:
                    tokens = line.split()
                    if len(tokens) != 4:
                        raise Exception("FareMatrix file line {}: expecting 4 values".format(i))
                    system, orig, dest, fare = tokens
                    data[int(system)][int(orig)][int(dest)] = float(fare)
        return data

    def generate_base_board(self, lines, board_fare):
        self._log.append(
            {"type": "text2", "content": "Set @board_cost to {} on {} lines".format(board_fare, len(lines))})
        for line in lines:
            for segment in line.segments():
                segment["@board_cost"] = board_fare

    def generate_fromto_approx(self, network, lines, fare_matrix, fs_data):
        network.create_attribute("LINK", "invehicle_cost")
        network.create_attribute("LINK", "board_cost")
        farezone_warning1 = "Warning: faresystem {} estimation: on line {}, node {} "\
            "does not have a valid @farezone ID. Using {} valid farezone {}."

        fs_data["NUM MATRIX RECORDS"] = 0
        valid_farezones = set(fare_matrix.keys())
        for mapping in fare_matrix.values():
            zones = list(mapping.keys())
            fs_data["NUM MATRIX RECORDS"] += len(zones)
            valid_farezones.update(set(zones))
        fs_data["NUM ZONES"] = len(valid_farezones)
        valid_fz_str = ", ".join([str(x) for x in valid_farezones])
        self._log.append(
            {"type": "text2", "content": "{} valid zones: {}".format(fs_data["NUM ZONES"], valid_fz_str)})

        valid_links = set([])
        zone_nodes = _defaultdict(lambda: set([]))
        for line in lines:
            prev_farezone = 0
            for seg in line.segments(include_hidden=True):
                if seg.link:
                    valid_links.add(seg.link)
                if seg.allow_alightings or seg.allow_boardings:
                    farezone = int(seg.i_node["@farezone"])
                    if farezone not in valid_farezones:
                        if prev_farezone == 0:
                            # DH added first farezone fix instead of exception
                            prev_farezone = list(valid_farezones)[0]
                            src_msg = "first"
                        else:
                            src_msg = "previous"
                        farezone = prev_farezone
                        self._log.append({
                            "type": "text3",
                            "content": farezone_warning1.format(fs_data["NUMBER"], line, seg.i_node, src_msg, prev_farezone)})
                    else:
                        prev_farezone = farezone
                    zone_nodes[farezone].add(seg.i_node)
        self._log.append(
            {"type": "text2", "content": "Farezone IDs and node count: %s" % (
                ", ".join(["%s: %s" % (k, len(v)) for k, v in zone_nodes.items()]))})

        # Two cases:
        #  - zone / area fares with boundary crossings, different FS may overlap:
        #         handle on a line-by-line bases with boarding and incremental segment costs
        #         for local and regional bus lines
        #  - station-to-station fares
        #         handle as an isolated system with the same costs on for all segments on a link
        #         and from boarding nodes by direction.
        #         Used mostly for BART, but also used Amtrack, some ferries and express buses
        #         Can support multiple boarding stops with same farezone provided it is an isolated leg,
        #         e.g. BART zone 85 Oakland airport connector (when operated a bus with multiple stops).

        count_single_node_zones = 0.0
        count_multi_node_zones = 0.0
        for zone, nodes in zone_nodes.items():
            if len(nodes) > 1:
                count_multi_node_zones += 1.0
            else:
                count_single_node_zones += 1.0
        # use station-to-station approximation if >90% of zones are single node
        is_area_fare = count_multi_node_zones / (count_multi_node_zones + count_single_node_zones) > 0.1

        if is_area_fare:
            self.zone_boundary_crossing_approx(lines, valid_farezones, fare_matrix, fs_data)
        else:
            self.station_to_station_approx(valid_farezones, fare_matrix, zone_nodes, valid_links, network)

        # copy costs from links to segments
        for line in lines:
            for segment in line.segments():
                segment["@invehicle_cost"] = max(segment.link.invehicle_cost, 0)
                segment["@board_cost"] = max(segment.link.board_cost, 0)
        network.delete_attribute("LINK", "invehicle_cost")
        network.delete_attribute("LINK", "board_cost")

    def zone_boundary_crossing_approx(self, lines, valid_farezones, fare_matrix, fs_data):
        farezone_warning1 = "Warning: no value in fare matrix for @farezone ID %s "\
           "found on line %s at node %s (using @farezone from previous segment in itinerary)"
        farezone_warning2 = "Warning: faresystem %s estimation on line %s: first node %s "\
            "does not have a valid @farezone ID. "
        farezone_warning3 = "Warning: no entry in farematrix %s from-to %s-%s: board cost "\
            "at segment %s set to %s."
        farezone_warning4 = "WARNING: the above issue has occurred more than once for the same line. "\
            "There is a feasible boarding-alighting on the this line with no fare defined in "\
            "the fare matrix."
        farezone_warning5 = "Warning: no entry in farematrix %s from-to %s-%s: "\
            "invehicle cost at segment %s set to %s"
        matrix_id = fs_data["FAREMATRIX ID"]

        self._log.append(
            {"type": "text2", "content": "Using zone boundary crossings approximation"})
        for line in lines:
            prev_farezone = 0
            same_farezone_missing_cost = False
            # Get list of stop segments
            stop_segments = [seg for seg in line.segments(include_hidden=True)
                             if (seg.allow_alightings or seg.allow_boardings)]
            for i, seg in enumerate(stop_segments):
                farezone = int(seg.i_node["@farezone"])
                if farezone not in valid_farezones:
                    self._log.append({
                        "type": "text3", "content": farezone_warning1 % (farezone, line, seg.i_node)})
                    if prev_farezone != 0:
                        farezone = prev_farezone
                        msg = "farezone from previous stop segment,"
                    else:
                        # DH added first farezone fix instead of exception
                        farezone = list(valid_farezones)[0]
                        self._log.append({
                            "type": "text3",
                            "content": farezone_warning2 % (fs_data["NUMBER"], line, seg.i_node)
                        })
                        msg = "first valid farezone in faresystem,"
                    self._log.append({
                            "type": "text3",
                            "content": "Using %s farezone %s" % (msg, farezone)
                        })
                if seg.allow_boardings:
                    # get the cost travelling within this farezone as base boarding cost
                    board_cost = fare_matrix.get(farezone, {}).get(farezone)
                    if board_cost is None:
                        # If this entry is missing from farematrix,
                        # use next farezone if both previous stop and next stop are in different farezones
                        next_seg = stop_segments[i+1]
                        next_farezone = next_seg.i_node["@farezone"]
                        if next_farezone != farezone and prev_farezone != farezone:
                            board_cost = fare_matrix.get(farezone, {}).get(next_farezone)
                    if board_cost is None:
                        # use the smallest fare found from this farezone as best guess
                        # as a reasonable boarding cost
                        board_cost = min(fare_matrix[farezone].values())
                        self._log.append({
                            "type": "text3",
                            "content": farezone_warning3 % (
                                matrix_id, farezone, farezone, seg, board_cost)})
                        if same_farezone_missing_cost == farezone:
                            self._log.append({"type": "text3", "content": farezone_warning4})
                        same_farezone_missing_cost = farezone
                    if seg.link:
                        seg.link.board_cost = max(board_cost, seg.link.board_cost)


                farezone = int(seg.i_node["@farezone"])
                # Set the zone-to-zone fare increment from the previous stop
                if prev_farezone != 0 and farezone != prev_farezone:
                    try:
                        invehicle_cost = fare_matrix[prev_farezone][farezone] - prev_seg.link.board_cost
                        prev_seg.link.invehicle_cost = max(invehicle_cost, prev_seg.link.invehicle_cost)
                    except KeyError:
                        self._log.append({
                            "type": "text3",
                            "content": farezone_warning5 % (matrix_id, prev_farezone, farezone, prev_seg, 0)})
                if farezone in valid_farezones:
                    prev_farezone = farezone
                prev_seg = seg

    def station_to_station_approx(self, valid_farezones, fare_matrix, zone_nodes, valid_links, network):
        network.create_attribute("LINK", "board_index", -1)
        network.create_attribute("LINK", "invehicle_index", -1)
        self._log.append(
            {"type": "text2", "content": "Using station-to-station least squares estimation"})
        index = 0
        farezone_area_index = {}
        for link in valid_links:
            farezone = link.i_node["@farezone"]
            if farezone not in valid_farezones:
                continue
            if len(zone_nodes[farezone]) == 1:
                link.board_index = index
                index += 1
                link.invehicle_index = index
                index += 1
            else:
                # in multiple station cases ALL boardings have the same index
                if farezone not in farezone_area_index:
                    farezone_area_index[farezone] = index
                    index += 1
                link.board_index = farezone_area_index[farezone]
                # only zone boundary crossing links get in-vehicle index
                if link.j_node["@farezone"] != farezone and link.j_node["@farezone"] in valid_farezones:
                    link.invehicle_index = index
                    index += 1

        A = []
        b = []
        pq_pairs = []

        def lookup_node(z):
            try:
                return next(iter(zone_nodes[z]))
            except StopIteration:
                return None

        for p in valid_farezones:
            q_costs = fare_matrix.get(p, {})
            orig_node = lookup_node(p)
            for q in valid_farezones:
                cost = q_costs.get(q, "n/a")
                dest_node = lookup_node(q)
                pq_pairs.append((p, q, orig_node, dest_node, cost))
                if q == p or orig_node is None or dest_node is None or cost == "n/a":
                    continue
                try:
                    path_links = _emme_tools.find_path(
                        orig_node, dest_node, lambda l: l in valid_links, lambda l: l.length)
                except _emme_tools.NoPathFound:
                    continue
                b.append(cost)
                a_indices = [0]*index

                a_indices[path_links[0].board_index] = 1
                for link in path_links:
                    if link.invehicle_index == -1:
                        continue
                    a_indices[link.invehicle_index] = 1
                A.append(a_indices)

        #x, res, rank, s = _np.linalg.lstsq(A, b, rcond=None)
        # Use scipy non-negative least squares solver
        x, rnorm = _nnls(A, b)
        result = [round(i, 2) for i in x]

        header = ["Boarding node", "J-node", "Farezone", "Board cost", "Invehicle cost"]
        table_content = []
        for link in valid_links:
            if link.board_index != -1:
                link.board_cost = result[link.board_index]
            if link.invehicle_index != -1:
                link.invehicle_cost = result[link.invehicle_index]
            if link.board_cost or link.invehicle_cost:
                table_content.append(
                    [link.i_node.id, link.j_node.id, int(link.i_node["@farezone"]), link.board_cost, link.invehicle_cost])

        self._log.append(
            {"type": "text2", "content": "Table of boarding and in-vehicle costs"})
        self._log.append({"content": table_content, "type": "table", "header": header})
        network.delete_attribute("LINK", "board_index")
        network.delete_attribute("LINK", "invehicle_index")

        # validation and reporting
        header = ["p/q"]
        table_content = []
        prev_p = None
        row = None
        for p, q, orig_node, dest_node, cost in pq_pairs:
            if prev_p != p:
                header.append(p)
                if row:
                    table_content.append(row)
                row = [p]
            cost = "$%.2f" % cost if isinstance(cost, float) else cost
            if orig_node is None or dest_node is None:
                row.append("%s, UNUSED" % (cost))
            else:
                try:
                    path_links = _emme_tools.find_path(
                        orig_node, dest_node, lambda l: l in valid_links, lambda l: l.length)
                    path_cost = (path_links[0].board_cost + sum(l.invehicle_cost for l in path_links))
                    row.append("%s, $%.2f" % (cost, path_cost))
                except _emme_tools.NoPathFound:
                    row.append("%s, NO PATH" % (cost))
            prev_p = p
        table_content.append(row)

        self._log.append(
            {"type": "text2", "content": "Table of origin station p to destination station q input cost, estimated cost"})
        self._log.append({"content": table_content, "type": "table", "header": header})

    def create_attribute(self, domain, name, scenario=None, network=None, atype=None):
        if scenario:
            if atype is None:
                if scenario.extra_attribute(name):
                    scenario.delete_extra_attribute(name)
                scenario.create_extra_attribute(domain, name)
            else:
                if scenario.network_field(domain, name):
                    scenario.delete_network_field(domain, name)
                scenario.create_network_field(domain, name, atype)
        if network:
            if name in network.attributes(domain):
                network.delete_attribute(domain, name)
            network.create_attribute(domain, name)

    def faresystem_distances(self, faresystems):
        max_xfer_dist = self.config.transit.fare_max_transfer_distance_miles * 5280.0
        self._log.append(
            {"type": "header", "content": "Faresystem distances"})
        self._log.append(
            {"type": "text2", "content": "Max transfer distance: %s" % max_xfer_dist})

        def bounding_rect(shape):
            if shape.bounds:
                x_min, y_min, x_max, y_max = shape.bounds
                return _geom.Polygon([(x_min, y_max), (x_max, y_max), (x_max, y_min), (x_min, y_min)])
            return _geom.Point()

        for fs_index, fs_data in enumerate(faresystems.values()):
            stops = set([])
            for line in fs_data["LINES"]:
                for stop in line.segments(True):
                    if stop.allow_alightings or stop.allow_boardings:
                        stops.add(stop.i_node)
            fs_data["shape"] = _geom.MultiPoint([(stop.x, stop.y) for stop in stops])
            #fs_data["bounding_rect"] = bounding_rect(fs_data["shape"])
            fs_data["NUM STOPS"] = len(fs_data["shape"])
            fs_data["FS_INDEX"] = fs_index

        # get distances between every pair of zone systems
        # determine transfer fares which are too far away to be used
        for fs_id, fs_data in faresystems.items():
            fs_data["distance"] = []
            fs_data["xfer_fares"] = xfer_fares = {}
            for fs_id2, fs_data2 in faresystems.items():
                if fs_data["NUM LINES"] == 0 or fs_data2["NUM LINES"] == 0:
                    distance = "n/a"
                elif fs_id == fs_id2:
                    distance = 0
                else:
                    # Get distance between bounding boxes as first approximation
                    #distance = fs_data["bounding_rect"].distance(fs_data2["bounding_rect"])
                    #if distance <= max_xfer_dist:
                        # if within tolerance get more precise distance between all stops
                    distance = fs_data["shape"].distance(fs_data2["shape"])
                fs_data["distance"].append(distance)

                if distance == "n/a" or distance > max_xfer_dist:
                    xfer = "TOO_FAR"
                elif fs_data2["STRUCTURE"] == "FREE":
                    xfer = 0.0
                elif fs_data2["STRUCTURE"] == "FROMTO":
                    # Transfering to the same FS in fare matrix is ALWAYS free
                    # for the farezone approximation
                    if fs_id == fs_id2:
                        xfer = 0.0
                        if fs_data2["FAREFROMFS"][fs_id] != 0:
                            self._log.append({
                                "type": "text3",
                                "content": "Warning: non-zero transfer within 'FROMTO' faresystem not supported"})
                    else:
                        xfer = "BOARD+%s" % fs_data2["FAREFROMFS"][fs_id]
                else:
                    xfer = fs_data2["FAREFROMFS"][fs_id]
                xfer_fares[fs_id2] = xfer

        distance_table = [["p/q"] + list(faresystems.keys())]
        for fs, fs_data in faresystems.items():
            distance_table.append([fs] + [("%.0f" % d if isinstance(d, float) else d) for d in fs_data["distance"]])
        self._log.append(
            {"type": "text2", "content": "Table of distance between stops in faresystems (feet)"})
        self._log.append({"content": distance_table, "type": "table"})

    def group_faresystems(self, faresystems):
        self._log.append(
            {"type": "header", "content": "Faresystem groups for ALL MODES"})
        def matching_xfer_fares(xfer_fares_list1, xfer_fares_list2):
            for xfer_fares1 in xfer_fares_list1:
                for xfer_fares2 in xfer_fares_list2:
                    for fs_id, fare1 in xfer_fares1.items():
                        fare2 = xfer_fares2[fs_id]
                        if fare1 != fare2 and (fare1 != "TOO_FAR" and fare2 != "TOO_FAR"):
                            return False
            return True

        # group faresystems together which have the same transfer-to pattern,
        # first pass: only group by matching mode patterns to minimize the number
        #             of levels with multiple modes
        group_xfer_fares_mode = []
        for fs_id, fs_data in faresystems.items():
            fs_modes = fs_data["MODE_SET"]
            if not fs_modes:
                continue
            xfers = fs_data["xfer_fares"]
            is_matched = False
            for xfer_fares_list, group, modes in group_xfer_fares_mode:
                # only if mode sets match
                if set(fs_modes) == set(modes):
                    is_matched = matching_xfer_fares([xfers], xfer_fares_list)
                    if is_matched:
                        group.append(fs_id)
                        xfer_fares_list.append(xfers)
                        modes.extend(fs_modes)
                        break
            if not is_matched:
                group_xfer_fares_mode.append(([xfers], [fs_id], list(fs_modes)))

        # second pass attempt to group together this set
        #   to minimize the total number of levels and modes
        group_xfer_fares = []
        for xfer_fares_list, group, modes in group_xfer_fares_mode:
            is_matched = False
            for xfer_fares_listB, groupB, modesB in group_xfer_fares:
                is_matched = matching_xfer_fares(xfer_fares_list, xfer_fares_listB)
                if is_matched:
                    xfer_fares_listB.extend(xfer_fares_list)
                    groupB.extend(group)
                    modesB.extend(modes)
                    break
            if not is_matched:
                group_xfer_fares.append((xfer_fares_list, group, modes))

        self._log.append(
            {"type": "header", "content": "Faresystems grouped by compatible transfer fares"})
        xfer_fares_table = [["p/q"] + list(faresystems.keys())]
        faresystem_groups = []
        i = 0
        for xfer_fares_list, group, modes in group_xfer_fares:
            xfer_fares = {}
            for fs_id in faresystems.keys():
                to_fares = [f[fs_id] for f in xfer_fares_list if f[fs_id] != "TOO_FAR"]
                fare = to_fares[0] if len(to_fares) > 0 else 0.0
                xfer_fares[fs_id] = fare
            faresystem_groups.append((group, xfer_fares))
            for fs_id in group:
                xfer_fares_table.append([fs_id] + list(faresystems[fs_id]["xfer_fares"].values()))
            i += 1
            self._log.append(
                {"type": "text2", "content": "Level %s faresystems: %s modes: %s" %
                    (i, ", ".join([str(x) for x in group]), ", ".join([str(m) for m in modes]))})

        self._log.append(
            {"type": "header", "content": "Transfer fares list by faresystem, sorted by group"})
        self._log.append({"content": xfer_fares_table, "type": "table"})

        return faresystem_groups

    def generate_transfer_fares(self, faresystems, faresystem_groups, network):
        self.create_attribute("MODE", "#orig_mode", self.scenario, network, "STRING")
        self.create_attribute("TRANSIT_LINE", "#src_mode", self.scenario, network, "STRING")
        self.create_attribute("TRANSIT_LINE", "#src_veh", self.scenario, network, "STRING")

        transit_modes = set([m for m in network.modes() if m.type == "TRANSIT"])
        mode_desc = {m.id: m.description for m in transit_modes}
        get_mode_id = network.available_mode_identifier
        get_vehicle_id = network.available_transit_vehicle_identifier

        meta_mode = network.create_mode("TRANSIT", get_mode_id())
        meta_mode.description = "Meta mode"
        for link in network.links():
            if link.modes.intersection(transit_modes):
                link.modes |= set([meta_mode])
        lines = _defaultdict(lambda: [])
        for line in network.transit_lines():
            lines[line.vehicle.id].append(line)
            line["#src_mode"] = line.mode.id
            line["#src_veh"] = line.vehicle.id
        for vehicle in network.transit_vehicles():
            temp_veh = network.create_transit_vehicle(get_vehicle_id(), vehicle.mode.id)
            veh_id = vehicle.id
            attributes = {a: vehicle[a] for a in network.attributes("TRANSIT_VEHICLE")}
            for line in lines[veh_id]:
                line.vehicle = temp_veh
            network.delete_transit_vehicle(vehicle)
            new_veh = network.create_transit_vehicle(veh_id, meta_mode.id)
            for a, v in attributes.items():
                new_veh[a] = v
            for line in lines[veh_id]:
                line.vehicle = new_veh
            network.delete_transit_vehicle(temp_veh)
        for link in network.links():
            link.modes -= transit_modes
        for mode in transit_modes:
            network.delete_mode(mode)

        # transition rules will be the same for every journey level
        transition_rules = []
        journey_levels = [
            {
                "description": "base",
                "destinations_reachable": True,
                "transition_rules": transition_rules,
                "waiting_time": None,
                "boarding_time": None,
                "boarding_cost": None
            }
        ]
        mode_map = _defaultdict(lambda: [])
        level = 1
        for fs_ids, xfer_fares in faresystem_groups:
            boarding_cost_id = "@from_level_%s" % level
            self.create_attribute("TRANSIT_SEGMENT", boarding_cost_id, self.scenario, network)
            journey_levels.append({
                "description": "Level_%s fs: %s" % (level, ",".join([str(x) for x in fs_ids])) ,
                "destinations_reachable": True,
                "transition_rules": transition_rules,
                "waiting_time": None,
                "boarding_time": None,
                "boarding_cost": {
                    "global": None, "at_nodes": None, "on_lines": None,
                    "on_segments": {"penalty": boarding_cost_id, "perception_factor": 1}
                },
            })

            level_modes = {}
            level_vehicles = {}
            for fs_id in fs_ids:
                fs_data = faresystems[fs_id]
                for line in fs_data["LINES"]:
                    level_mode = level_modes.get(line["#src_mode"])
                    if level_mode is None:
                        level_mode = network.create_mode("TRANSIT", get_mode_id())
                        level_mode.description = mode_desc[line["#src_mode"]]
                        level_mode["#orig_mode"] = line["#src_mode"]
                        transition_rules.append({"mode": level_mode.id, "next_journey_level": level})
                        level_modes[line["#src_mode"]] = level_mode
                        mode_map[line["#src_mode"]].append(level_mode.id)
                    for segment in line.segments():
                        segment.link.modes |= set([level_mode])
                    new_vehicle = level_vehicles.get(line.vehicle.id)
                    if new_vehicle is None:
                        new_vehicle = network.create_transit_vehicle(get_vehicle_id(), level_mode)
                        for a in network.attributes("TRANSIT_VEHICLE"):
                            new_vehicle[a] = line.vehicle[a]
                        level_vehicles[line.vehicle.id] = new_vehicle
                    line.vehicle = new_vehicle

            # set boarding cost on all lines
            # xferfares is a list of transfer fares, as a number or a string "BOARD+" + a number
            for line in network.transit_lines():
                to_faresystem = int(line["#faresystem"])
                try:
                    xferboard_cost = xfer_fares[to_faresystem]
                except KeyError:
                    continue  # line does not have a valid faresystem ID
                if xferboard_cost == "TOO_FAR":
                    pass  # use zero cost as transfer from this fs to line is impossible
                elif isinstance(xferboard_cost, str) and xferboard_cost.startswith("BOARD+"):
                    xferboard_cost = float(xferboard_cost[6:])
                    for segment in line.segments():
                        if segment.allow_boardings:
                            segment[boarding_cost_id] = max(xferboard_cost + segment["@board_cost"], 0)
                else:
                    for segment in line.segments():
                        if segment.allow_boardings:
                            segment[boarding_cost_id] = max(xferboard_cost, 0)
            level += 1

        for vehicle in network.transit_vehicles():
            if vehicle.mode == meta_mode:
                network.delete_transit_vehicle(vehicle)
        for link in network.links():
            link.modes -= set([meta_mode])
        network.delete_mode(meta_mode)
        self._log.append(
            {"type": "header", "content": "Mapping from original modes to modes for transition table"})
        for orig_mode, new_modes in mode_map.items():
            self._log.append(
                {"type": "text2", "content": "%s : %s" % (orig_mode, ", ".join(new_modes))})
        return journey_levels, mode_map

    def save_journey_levels(self, name, journey_levels):
        spec_dir = os.path.join(self.root_dir, os.path.dirname(self.config.emme.project_path), "Specifications")
        path = os.path.join(spec_dir, "%s_%s_journey_levels.ems" % (self.period, name))
        with open(path, 'w') as jl_spec_file:
            spec = {"type": "EXTENDED_TRANSIT_ASSIGNMENT", "journey_levels": journey_levels}
            _json.dump(spec, jl_spec_file, indent=4)

    def filter_journey_levels_by_mode(self, modes, journey_levels):
        # remove rules for unused modes from provided journey_levels
        # (restrict to provided modes)
        journey_levels = _copy(journey_levels)
        for level in journey_levels:
            rules = level["transition_rules"]
            rules = [_copy(r) for r in rules if r["mode"] in modes]
            level["transition_rules"] = rules
        # count level transition rules references to find unused levels
        num_levels = len(journey_levels)
        level_count = [0] * num_levels

        def follow_rule(next_level):
            level_count[next_level] += 1
            if level_count[next_level] > 1:
                return
            for rule in journey_levels[next_level]["transition_rules"]:
                follow_rule(rule["next_journey_level"])

        follow_rule(0)
        # remove unreachable levels
        # and find new index for transition rules for remaining levels
        level_map = {i: i for i in range(num_levels)}
        for level_id, count in reversed(list(enumerate(level_count))):
            if count == 0:
                for index in range(level_id, num_levels):
                    level_map[index] -= 1
                del journey_levels[level_id]
        # re-index remaining journey_levels
        for level in journey_levels:
            for rule in level["transition_rules"]:
                next_level = rule["next_journey_level"]
                rule["next_journey_level"] = level_map[next_level]
        return journey_levels

    def log_report(self):
        manager = _emme_tools.EmmeManager()
        emme_project = manager.project(os.path.join(self.root_dir, self.config.emme.project_path))
        manager.init_modeller(emme_project)
        report = _emme_tools.PageBuilder(title="Fare calculation report")
        try:
            for item in self._log:
                if item["type"] == "header":
                    report.add_html("<h3 style='margin-left:10px'>%s</h3>" % item["content"])
                elif item["type"] == "text":
                    report.add_html("<div style='margin-left:20px'>%s</div>" % item["content"])
                elif item["type"] == "text2":
                    report.add_html("<div style='margin-left:30px'>%s</div>" % item["content"])
                elif item["type"] == "text3":
                    report.add_html("<div style='margin-left:40px'>%s</div>" % item["content"])
                elif item["type"] == "table":
                    table_msg = []
                    if "header" in item:
                        table_msg.append("<tr>")
                        for label in item["header"]:
                            table_msg.append("<th>%s</th>" % label)
                        table_msg.append("</tr>")
                    for row in item["content"]:
                        table_msg.append("<tr>")
                        for cell in row:
                            table_msg.append("<td>%s</td>" % cell)
                        table_msg.append("</tr>")
                    title = "<h3>%s</h3>" % item["title"] if "title" in item else ""
                    report.add_html("""
                        <div style='margin-left:20px'>
                            %s
                            <table>%s</table>
                        </div>
                        <br>
                        """ % (title, "".join(table_msg)))

        except Exception as error:
            # no raise during report to avoid masking real error
            report.add_html("Error generating report")
            report.add_html(str(error))
            report.add_html(_traceback.format_exc())

        manager.logbook_write("Apply fares report %s" % self.period, report.render())

    def log_text_report(self):
        bank_dir = os.path.dirname(self.scenario.emmebank.path)
        timestamp = _time.strftime("%Y%m%d-%H%M%S")
        path = os.path.join(bank_dir, "apply_fares_report_%s_%s.txt" % (self.period, timestamp))
        with open(path, 'w') as report:
            try:
                for item in self._log:
                    if item["type"] == "header":
                        report.write("\n%s\n" % item["content"])
                        report.write("-" * len(item["content"]) + "\n\n")
                    elif item["type"] == "text":
                        report.write("    %s\n" % item["content"])
                    elif item["type"] == "text2":
                        report.write("        %s\n" % item["content"])
                    elif item["type"] == "text3":
                        report.write("            %s\n" % item["content"])
                    elif item["type"] == "table":
                        table_msg = []
                        cell_length = [0] * len(item["content"][0])
                        if "header" in item:
                            for i, label in enumerate(item["header"]):
                                cell_length[i] = max(cell_length[i], len(str(label)))
                        for row in item["content"]:
                            for i, cell in enumerate(row):
                                cell_length[i] = max(cell_length[i], len(str(cell)))
                        if "header" in item:
                            row_text = []
                            for label, length in zip(item["header"], cell_length):
                                row_text.append("%-*s" % (length, label))
                            table_msg.append(" ".join(row_text))
                        for row in item["content"]:
                            row_text = []
                            for cell, length in zip(row, cell_length):
                                row_text.append("%-*s" % (length, cell))
                            table_msg.append(" ".join(row_text))
                        if "title" in item:
                            report.write("%s\n" % item["title"])
                            report.write("-" * len(item["title"]) + "\n")
                        table_msg.extend(["", ""])
                        report.write("\n".join(table_msg))
            except Exception as error:
                # no raise during report to avoid masking real error
                report.write("Error generating report\n")
                report.write(str(error) + "\n")
                report.write(_traceback.format_exc())
