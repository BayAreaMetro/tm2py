"""Transit network preparation module."""

from __future__ import annotations

import json as _json
import os
import time as _time
import traceback as _traceback
from collections import defaultdict as _defaultdict
from copy import deepcopy as _copy
from typing import Dict

import pandas as pd
import shapely.geometry as _geom
from inro.modeller import PageBuilder
from scipy.optimize import nnls as _nnls
from typing_extensions import TYPE_CHECKING, Literal

from tm2py.components.component import Component
from tm2py.emme.manager import EmmeLink, EmmeNetwork, EmmeScenario
from tm2py.emme.network import NoPathFound, find_path
from tm2py.logger import LogStartEnd

if TYPE_CHECKING:
    from tm2py.controller import RunController


class PrepareTransitNetwork(Component):
    """Transit assignment and skim-related network preparation."""

    def __init__(self, controller: "RunController"):
        """Constructor for PrepareTransitNetwork class.

        Args:
            controller: The RunController instance.
        """
        super().__init__(controller)
        self.config = self.controller.config.transit
        self._emme_manager = self.controller.emme_manager
        self._transit_emmebank = None
        self._transit_networks = None
        self._transit_scenarios = None
        self._highway_emmebank = None
        self._highway_scenarios = None
        self._auto_emmebank = None
        self._auto_networks = None
        self._auto_scenarios = None
        self._access_connector_df = None
        self._egress_connector_df = None

    @LogStartEnd(
        "Prepare transit network attributes and update times from auto network."
    )
    def run(self):
        """Prepare transit network for assignment.

        Updates link travel times from auto network and
        (if using TAZ-connectors for assignment) update connector walk times.
        """
        if self.controller.iteration == 0:
            for period in self.controller.time_period_names:
                with self.logger.log_start_end(f"period {period}"):
                    scenario = self.transit_emmebank.scenario(period)
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

                    network = self.transit_networks[period]
                    if self.config.get(
                        "override_connectors", False
                    ):  # don't run prepare connector, connectors are created in lasso
                        self.prepare_connectors(network, period)
                    self.distribute_nntime(network)
                    self.update_link_trantime(network)
                    # self.calc_link_unreliability(network, period)
                    if self.config.use_fares:
                        self.apply_fares(scenario, network, period)
                    if self.config.get("split_connectors_to_prevent_walk", False):
                        self.split_tap_connectors_to_prevent_walk(network)
                    # TODO: missing the input data files for apply station attributes
                    # self.apply_station_attributes(input_dir, network)
                    scenario.publish_network(network)

        for time_period in self.time_period_names:
            self._update_auto_times(time_period)
            self._update_pnr_penalty(time_period)
            if self.config.override_connector_times:
                self._update_connector_times(time_period)

    def validate_inputs(self):
        """Validate the inputs."""
        # TODO

    @property
    def transit_emmebank(self):
        if not self._transit_emmebank:
            self._transit_emmebank = self.controller.emme_manager.transit_emmebank
        return self._transit_emmebank

    @property
    def highway_emmebank(self):
        if not self._highway_emmebank:
            self._highway_emmebank = self.controller.emme_manager.highway_emmebank
        return self._highway_emmebank

    @property
    def transit_scenarios(self):
        if self._transit_scenarios is None:
            self._transit_scenarios = {
                tp: self.transit_emmebank.scenario(tp) for tp in self.time_period_names
            }
        return self._transit_scenarios

    @property
    def highway_scenarios(self):
        if self._highway_scenarios is None:
            self._highway_scenarios = {
                tp: self.highway_emmebank.scenario(tp) for tp in self.time_period_names
            }
        return self._highway_scenarios

    @property
    def transit_networks(self):
        # if self._transit_networks is None:
        self._transit_networks = {
            tp: self.transit_scenarios[tp].get_network()
            for tp in self.time_period_names
        }
        return self._transit_networks

    @property
    def access_connector_df(self):
        if self._access_connector_df is None:
            self._access_connector_df = pd.read_csv(
                self.get_abs_path(self.config.input_connector_access_times_path)
            )
        return self._access_connector_df

    @property
    def egress_connector_df(self):
        if self._egress_connector_df is None:
            self._egress_connector_df = pd.read_csv(
                self.get_abs_path(self.config.input_connector_egress_times_path)
            )
        return self._egress_connector_df

    def _update_auto_times(self, time_period: str):
        """Update the auto travel times from the last auto assignment to the transit scenario.

        TODO Document steps more when understand them.

        Note: may need to remove "reliability" factor in future versions of VDF def

        Args:
            time_period: time period name abbreviation
        """

        _highway_link_dict = self._get_highway_links(time_period)
        _transit_link_dict = self._get_transit_links(time_period)

        for _link_id in _highway_link_dict.keys() & _transit_link_dict.keys():
            auto_time = _highway_link_dict[_link_id].auto_time
            area_type = _highway_link_dict[_link_id]["@area_type"]
            # use @valuetoll_dam (cents/mile) here to represent the drive alone toll
            sov_toll_per_mile = _highway_link_dict[_link_id]['@valuetoll_dam']
            link_length = _transit_link_dict[_link_id].length
            facility_type = _transit_link_dict[_link_id]['@ft']
            sov_toll = sov_toll_per_mile * link_length/100

            _transit_link_dict[_link_id]["@drive_toll"] = sov_toll 
            
            if auto_time > 0:
                # https://github.com/BayAreaMetro/travel-model-one/blob/master/model-files/scripts/skims/PrepHwyNet.job#L106
                tran_speed = 60 * link_length/auto_time
                if (facility_type<=4 or facility_type==8) and (tran_speed<6):
                    tran_speed = 6
                    _transit_link_dict[_link_id]["@trantime"] = 60 * link_length/tran_speed
                elif (tran_speed<3):
                    tran_speed = 3
                    _transit_link_dict[_link_id]["@trantime"] = 60 * link_length/tran_speed
                else:
                    _transit_link_dict[_link_id]["@trantime"] = auto_time
                # data1 is the auto time used in Mixed-Mode transit assigment
                _transit_link_dict[_link_id].data1 = (_transit_link_dict[_link_id]["@trantime"] + 
                                                      60*sov_toll/self.config.value_of_time)
                # bus time calculation
                if facility_type in [1,2,3,8]:
                    delayfactor = 0.0
                else:
                    if area_type in [0,1]: 
                        delayfactor = 2.46
                    elif area_type in [2,3]: 
                        delayfactor = 1.74
                    elif area_type==4:
                        delayfactor = 1.14
                    else:
                        delayfactor = 0.08
                bus_time = _transit_link_dict[_link_id]["@trantime"] + (delayfactor * link_length)
                _transit_link_dict[_link_id]["@trantime"] = bus_time                 

        # TODO document this! Consider copying to another method.
        # set us1 (segment data1), used in ttf expressions, from @trantime
        _transit_net = self._transit_networks[time_period]
        _transit_scenario = self.transit_scenarios[time_period]

        for segment in _transit_net.transit_segments():
            # ? why would we only do this is schedule time was negative -- ES
            if segment["@schedule_time"] <= 0 and segment.link is not None:
                # ? what is "data1" and why do we need to update all these?
                segment["data1"] = segment["@trantime_seg"] = segment.link["@trantime"]

        _update_attributes = {
            "TRANSIT_SEGMENT": ["@trantime_seg", "data1"],
            "LINK": ["@trantime", "@drive_toll"],
        }
        self.emme_manager.copy_attribute_values(
            _transit_net, _transit_scenario, _update_attributes
        )

    def _update_pnr_penalty(self, time_period: str):
        """Add the parking penalties to pnr parking lots.

        Args:
            time_period: time period name abbreviation
        """
        _transit_net = self._transit_networks[time_period]
        _transit_scenario = self.transit_scenarios[time_period]
        deflator = self.config.fare_2015_to_2000_deflator

        for segment in _transit_net.transit_segments():
            if "BART_acc" in segment.id:
                if "West Oakland" in segment.id:
                    segment["@board_cost"] = 12.4 * deflator        
                else:
                    segment["@board_cost"] = 3.0 * deflator
            elif "Caltrain_acc" in segment.id:
                segment["@board_cost"] = 5.5 * deflator

        _update_attributes = {
            "TRANSIT_SEGMENT": ["@board_cost"]
        }
        self.emme_manager.copy_attribute_values(
            _transit_net, _transit_scenario, _update_attributes
        )

    def _initialize_link_attribute(self, time_period, attr_name):
        """

        Delete attribute in network object to reinitialize to default values

        Args:
            network (_type_): _description_
            attre_name (_type_): _description_
        """
        _network = self._transit_networks[time_period]
        _scenario = self._transit_scenarios[time_period]
        if _scenario.extra_attribute(attr_name) is None:
            _scenario.create_extra_attribute("LINK", attr_name)
        if attr_name in _network.attributes("LINK"):
            _network.delete_attribute("LINK", attr_name)
        _network.create_attribute("LINK", attr_name, 9999)

    def _update_connector_times(self, time_period: str):
        """Set the connector times from the source connector times files.

        See also _process_connector_file

        Args:
            time_period: time period name abbreviation
        """
        # walk time attributes per skim set

        _scenario = self.transit_scenarios[time_period]
        _network = self.transit_networks[time_period]
        _update_node_attributes = {"NODE": ["@taz_id", "#node_id"]}

        # ? what purpose do all these copy attribute values serve? why wouldn't taz and node
        # ID already be there? -- ES
        self.emme_manager.copy_attribute_values(
            _scenario,
            _network,
            _update_node_attributes,
        )

        _transit_class_attr_map = {
            _tclass.skim_set_id: f"@walk_time_{_tclass.name.lower()}"
            for _tclass in self.config.classes
        }

        for attr_name in _transit_class_attr_map.values():
            self._initialize_link_attribute(time_period, attr_name)

        _connectors_df = self._get_centroid_connectors_as_df(time_period=time_period)
        _access_df = self.access_connector_df.loc[
            self.access_connector_df.time_period == time_period
        ]
        _egress_df = self.access_connector_df.loc[
            self.egress_connector_df.time_period == time_period
        ]

        # TODO check logic here. It was hard to follow previously.
        _walktime_attrs = [
            f"@walk_time_{_tclass.name.lower()}" for _tclass in self.comfig.classes
        ]

        _connectors_df.merge(_access_df[["A", "B"] + _walktime_attrs], on=["A", "B"])
        _connectors_df.merge(_egress_df[["A", "B"] + _walktime_attrs], on=["A", "B"])

        # messy because can't access EmmeLink attributes in dataframe-like method
        for row in _connectors_df.iterrows():
            for _attr in _walktime_attrs:
                row.link[_attr] = row[_attr]

        _connector_links = _connectors_df.link.tolist()

        self.emme_manager.copy_attribute_values(
            _network, _scenario, {"LINK": _connector_links}
        )

    def _get_centroid_connectors_as_df(self, time_period: str) -> pd.DataFrame:
        """Returns a datafra,e of centroid connector links and A and B ids.

        Args:
            time_period (str): time period abbreviation.

        Returns:
            pd.DataFrame: DataFrame of centroid connectors.
        """
        connectors_df = pd.DataFrame()
        # lookup adjacent stop ID (also accounts for connector splitting)
        _network = self.transit_networks[time_period]
        for zone in _network.centroids():
            taz_id = int(zone["@taz_id"])
            for link in zone.outgoing_links():
                connectors_df.append(
                    {"A": taz_id, "B": int(link.j_node["#node_id"]), "link": link},
                    ignore_index=True,
                )
            for link in zone.incoming_links():
                connectors_df.append(
                    {"A": int(link.i_node["#node_id"]), "B": taz_id, "link": link},
                    ignore_index=True,
                )
        return connectors_df

    def _get_transit_links(
        self,
        time_period: str,
    ):
        """Create dictionary of link ids mapped to attributes.

        Args:
            time_period (str): time period abbreviation
        """
        _transit_scenario = self.transit_scenarios[time_period]
        _transit_net = self.transit_networks[time_period]
        transit_attributes = {
            "LINK": ["#link_id", "@trantime", "@ft"],
            "TRANSIT_SEGMENT": ["@schedule_time", "@trantime_seg", "data1"],
        }
        self.emme_manager.copy_attribute_values(
            _transit_scenario, _transit_net, transit_attributes
        )
        _transit_link_dict = {
            tran_link["#link_id"]: tran_link for tran_link in _transit_net.links()
        }
        return _transit_link_dict

    def _get_highway_links(
        self,
        time_period: str,
    ):
        """Create dictionary of link ids mapped to auto travel times.

        Args:
            time_period (str): time period abbreviation
        """
        _highway_scenario = self.highway_scenarios[time_period]
        if not _highway_scenario.has_traffic_results:
            return {}
        _highway_net = _highway_scenario.get_partial_network(
            ["LINK"], include_attributes=False
        )
        travel_time_attributes = {"LINK": ["#link_id", 
                                           "auto_time",
                                           "@area_type",
                                           "@valuetoll_dam"]}
        self.emme_manager.copy_attribute_values(
            _highway_scenario, _highway_net, travel_time_attributes
        )
        # TODO can we just get the link attributes as a DataFrame and merge them?
        auto_link_dict = {
            auto_link["#link_id"]: auto_link
            for auto_link in _highway_net.links()
        }
        return auto_link_dict

    def prepare_connectors(self, network, period):
        for node in network.centroids():
            for link in node.outgoing_links():
                network.delete_link(link.i_node, link.j_node)
            for link in node.incoming_links():
                network.delete_link(link.i_node, link.j_node)
        period_name = period.lower()
        access_modes = set()
        egress_modes = set()
        for mode_data in self.controller.config.transit.modes:
            if mode_data["type"] == "ACCESS":
                access_modes.add(network.mode(mode_data["mode_id"]))
            if mode_data["type"] == "EGRESS":
                egress_modes.add(network.mode(mode_data["mode_id"]))
        tazs = dict((int(n["@taz_id"]), n) for n in network.centroids())
        nodes = dict((int(n["#node_id"]), n) for n in network.regular_nodes())
        with open(
            self.get_abs_path(self.config.input_connector_access_times_path), "r"
        ) as f:
            header = next(f).split(",")
            for line in f:
                tokens = line.split(",")
                data = dict(zip(header, tokens))
                if data["time_period"].lower() == period_name:
                    taz = tazs[int(data["from_taz"])]
                    stop = nodes[int(data["to_stop"])]
                    if network.link(taz, stop) is None:
                        connector = network.create_link(taz, stop, access_modes)
        with open(
            self.get_abs_path(self.config.input_connector_egress_times_path), "r"
        ) as f:
            header = next(f).split(",")
            for line in f:
                tokens = line.split(",")
                data = dict(zip(header, tokens))
                if data["time_period"].lower() == period_name:
                    taz = tazs[int(data["to_taz"])]
                    stop = nodes[int(data["from_stop"])]
                    if network.link(stop, taz) is None:
                        connector = network.create_link(stop, taz, egress_modes)

    def distribute_nntime(self, network):
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
    def update_link_trantime(network):
        # if nntime exists, use that for ivtt, else use the link trantime
        for line in network.transit_lines():
            for segment in line.segments(include_hidden=False):
                if segment["@schedule_time"] > 0:
                    segment.data1 = segment["@trantime_seg"] = segment["@schedule_time"]
                else:
                    segment.data1 = segment["@trantime_seg"] = segment.link["@trantime"]
                segment.transit_time_func = 2

    def split_tap_connectors_to_prevent_walk(self, network):
        tap_stops = _defaultdict(lambda: [])
        new_node_id = IDGenerator(1, network)
        all_transit_modes = set(
            [mode for mode in network.modes() if mode.type == "TRANSIT"]
        )
        node_attributes = network.attributes("NODE")
        node_attributes.remove("x")
        node_attributes.remove("y")
        link_attributes_reset = ["length"]

        mode_table = self.controller.config.transit.modes
        walk_modes = set()
        access_modes = set()
        egress_modes = set()
        for mode_data in mode_table:
            if mode_data["type"] == "WALK":
                walk_modes.add(network.mode(mode_data["mode_id"]))
            if mode_data["type"] == "ACCESS":
                access_modes.add(network.mode(mode_data["mode_id"]))
            if mode_data["type"] == "EGRESS":
                egress_modes.add(network.mode(mode_data["mode_id"]))

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
                    tap_stop = network.split_link(
                        centroid,
                        real_stop,
                        next(new_node_id),
                        include_reverse=True,
                        proportion=0.5,
                    )
                    for attr in node_attributes:
                        tap_stop[attr] = real_stop[attr]
                    tap_stops[real_stop].append(tap_stop)
                    transit_access_links = [
                        (real_stop, tap_stop),
                        (tap_stop, real_stop),
                    ]
                    for i_node, j_node in transit_access_links:
                        t_link = network.link(i_node, j_node)
                        if t_link is None:
                            t_link = network.create_link(
                                i_node, j_node, all_transit_modes
                            )
                        else:
                            t_link.modes = all_transit_modes
                        for attr in link_attributes_reset:
                            t_link[attr] = 0
                    egress_links = [
                        (network.link(tap_stop, centroid), egress_modes),
                        (network.link(centroid, tap_stop), access_modes),
                    ]
                    for t_link, modes in egress_links:
                        if t_link is None:
                            continue
                        t_link.modes = modes
                        t_link.length = length

        line_attributes = network.attributes("TRANSIT_LINE")
        seg_attributes = network.attributes("TRANSIT_SEGMENT")
        # attributes referring to in-vehicle components which should be set to 0 on virtual stop segments
        seg_invehicle_attrs = [
            "@invehicle_cost",
            "data1",
            "@trantime_seg",
            "@schedule_time",
            "@nntime",
        ]

        # re-route the transit lines through the new TAP-stops
        for line in network.transit_lines():
            # store segment data for re-routing
            seg_data = {}
            itinerary = []
            tap_segments = []
            for seg in line.segments(include_hidden=True):
                seg_data[(seg.i_node, seg.j_node, seg.loop_index)] = dict(
                    (k, seg[k]) for k in seg_attributes
                )
                itinerary.append(seg.i_node.number)
                if seg.i_node in tap_stops and (
                    seg.allow_boardings or seg.allow_alightings
                ):
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
                    tap_segments.append(
                        {"access": tap_access, "egress": tap_egress, "real": real_seg}
                    )

            if tap_segments:
                # store line data for re-routing
                line_data = dict((k, line[k]) for k in line_attributes)
                line_data["id"] = line.id
                line_data["vehicle"] = line.vehicle
                # delete old line, then re-create on new, re-routed itinerary
                network.delete_transit_line(line)

                new_line = network.create_transit_line(
                    line_data.pop("id"), line_data.pop("vehicle"), itinerary
                )
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

    def apply_fares(self, scenario, network, period):
        apply_fares = ApplyFares(self.controller)
        apply_fares.scenario = scenario
        apply_fares.network = network
        apply_fares.period = period
        apply_fares.run()


class IDGenerator(object):
    """Generate available Node IDs."""

    def __init__(self, start, network):
        """Return new Emme network attribute with details as defined."""
        self._number = start
        self._network = network

    def next(self):
        """Return the next valid node ID number."""
        while True:
            if self._network.node(self._number) is None:
                break
            self._number += 1
        return self._number

    def __next__(self):
        """Return the next valid node ID number."""
        return self.next()


class ApplyFares(Component):
    def __init__(self, controller: RunController):
        """Initialize component.

        Args:
            controller: parent Controller object
        """
        super().__init__(controller)

        self.scenario = None
        self.network = None
        self.period = ""
        self.config = self.controller.config.transit

        self.dot_far_file = self.get_abs_path(self.config.fares_path)
        self.fare_matrix_file = self.get_abs_path(self.config.fare_matrix_path)

        self._log = []

    def validate_inputs(self):
        # TODO
        pass

    def run(self):
        self._log = []
        faresystems = self.parse_dot_far_file()
        fare_matrices = self.parse_fare_matrix_file()

        try:
            # network = self.network = self.scenario.get_network()
            network = self.network
            self.create_attribute(
                "TRANSIT_SEGMENT", "@board_cost", self.scenario, network
            )
            self.create_attribute(
                "TRANSIT_SEGMENT", "@invehicle_cost", self.scenario, network
            )
            # identify the lines by faresystem
            for line in network.transit_lines():
                fs_id = int(line["#faresystem"])
                try:
                    fs_data = faresystems[fs_id]
                except KeyError:
                    self._log.append(
                        {
                            "type": "text",
                            "content": (
                                f"Line {line.id} has #faresystem '{fs_id}' which was "
                                "not found in fares.far table"
                            ),
                        }
                    )
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
                    {
                        "type": "text",
                        "content": "FAREZONE {}: {} {}".format(
                            fs_id, fs_data["STRUCTURE"], fs_data["NAME"]
                        ),
                    }
                )
                lines = fs_data["LINES"]
                fs_data["MODE_SET"] = set(l.mode.id for l in lines)
                fs_data["MODES"] = ", ".join(fs_data["MODE_SET"])
                if fs_data["NUM LINES"] == 0:
                    self._log.append(
                        {
                            "type": "text2",
                            "content": "No lines associated with this faresystem",
                        }
                    )
                elif fs_data["STRUCTURE"] == "FLAT":
                    self.generate_base_board(lines, fs_data["IBOARDFARE"])
                elif fs_data["STRUCTURE"] == "FROMTO":
                    fare_matrix = fare_matrices[fs_data["FAREMATRIX ID"]]
                    self.generate_fromto_approx(network, lines, fare_matrix, fs_data)

            self.faresystem_distances(faresystems)
            faresystem_groups = self.group_faresystems(faresystems)
            journey_levels, mode_map = self.generate_transfer_fares(
                faresystems, faresystem_groups, network
            )
            self.save_journey_levels("ALLPEN", journey_levels)
            # local_modes = []
            # premium_modes = []
            # for mode in self.config.modes:
            #     if mode.type == "LOCAL":
            #         local_modes.extend(mode_map[mode.mode_id])
            #     if mode.type == "PREMIUM":
            #         premium_modes.extend(mode_map[mode.mode_id])
            # local_levels = self.filter_journey_levels_by_mode(
            #     local_modes, journey_levels
            # )
            # self.save_journey_levels("BUS", local_levels)
            # premium_levels = self.filter_journey_levels_by_mode(
            #     premium_modes, journey_levels
            # )
            # self.save_journey_levels("PREM", premium_levels)

        except Exception as error:
            self._log.append({"type": "text", "content": "error during apply fares"})
            self._log.append({"type": "text", "content": str(error)})
            self._log.append({"type": "text", "content": _traceback.format_exc()})
            raise
        finally:
            log_content = []
            header = [
                "NUMBER",
                "NAME",
                "NUM LINES",
                "NUM SEGMENTS",
                "MODES",
                "FAREMATRIX ID",
                "NUM ZONES",
                "NUM MATRIX RECORDS",
            ]
            for fs_id, fs_data in faresystems.items():
                log_content.append([str(fs_data.get(h, "")) for h in header])
            self._log.insert(
                0,
                {
                    "content": log_content,
                    "type": "table",
                    "header": header,
                    "title": "Faresystem data",
                },
            )

            self.log_report()
            self.log_text_report()

        self.scenario.publish_network(network)

        return journey_levels

    def parse_dot_far_file(self):
        data = {}
        numbers = []
        with open(self.dot_far_file, "r") as f:
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
                    fs_data["FAREFROMFS"] = [
                        float(x) for x in fs_data["FAREFROMFS"].split(",")
                    ]
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
        with open(self.fare_matrix_file, "r") as f:
            for i, line in enumerate(f):
                if line:
                    tokens = line.split()
                    if len(tokens) != 4:
                        raise Exception(
                            "FareMatrix file line {}: expecting 4 values".format(i)
                        )
                    system, orig, dest, fare = tokens
                    data[int(system)][int(orig)][int(dest)] = float(fare)
        return data

    def generate_base_board(self, lines, board_fare):
        self._log.append(
            {
                "type": "text2",
                "content": "Set @board_cost to {} on {} lines".format(
                    board_fare, len(lines)
                ),
            }
        )
        for line in lines:
            for segment in line.segments():
                segment["@board_cost"] = board_fare

    def generate_fromto_approx(self, network, lines, fare_matrix, fs_data):
        network.create_attribute("LINK", "invehicle_cost")
        network.create_attribute("LINK", "board_cost")
        farezone_warning1 = (
            "Warning: faresystem {} estimation: on line {}, node {} "
            "does not have a valid @farezone ID. Using {} valid farezone {}."
        )

        fs_data["NUM MATRIX RECORDS"] = 0
        valid_farezones = set(fare_matrix.keys())
        for mapping in fare_matrix.values():
            zones = list(mapping.keys())
            fs_data["NUM MATRIX RECORDS"] += len(zones)
            valid_farezones.update(set(zones))
        fs_data["NUM ZONES"] = len(valid_farezones)
        valid_fz_str = ", ".join([str(x) for x in valid_farezones])
        self._log.append(
            {
                "type": "text2",
                "content": "{} valid zones: {}".format(
                    fs_data["NUM ZONES"], valid_fz_str
                ),
            }
        )

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
                        self._log.append(
                            {
                                "type": "text3",
                                "content": farezone_warning1.format(
                                    fs_data["NUMBER"],
                                    line,
                                    seg.i_node,
                                    src_msg,
                                    prev_farezone,
                                ),
                            }
                        )
                    else:
                        prev_farezone = farezone
                    zone_nodes[farezone].add(seg.i_node)
        self._log.append(
            {
                "type": "text2",
                "content": "Farezone IDs and node count: %s"
                % (", ".join(["%s: %s" % (k, len(v)) for k, v in zone_nodes.items()])),
            }
        )

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
        is_area_fare = (
            count_multi_node_zones / (count_multi_node_zones + count_single_node_zones)
            > 0.1
        )

        if is_area_fare:
            self.zone_boundary_crossing_approx(
                lines, valid_farezones, fare_matrix, fs_data
            )
        else:
            self.station_to_station_approx(
                valid_farezones, fare_matrix, zone_nodes, valid_links, network
            )
            # copy costs from links to segments
            for line in lines:
                for segment in line.segments():
                    segment["@invehicle_cost"] = max(segment.link.invehicle_cost, 0)
                    segment["@board_cost"] = max(segment.link.board_cost, 0)
        
        network.delete_attribute("LINK", "invehicle_cost")
        network.delete_attribute("LINK", "board_cost")

    def zone_boundary_crossing_approx(
        self, lines, valid_farezones, fare_matrix, fs_data
    ):
        farezone_warning1 = (
            "Warning: no value in fare matrix for @farezone ID %s "
            "found on line %s at node %s (using @farezone from previous segment in itinerary)"
        )
        farezone_warning2 = (
            "Warning: faresystem %s estimation on line %s: first node %s "
            "does not have a valid @farezone ID. "
        )
        farezone_warning3 = (
            "Warning: no entry in farematrix %s from-to %s-%s: board cost "
            "at segment %s set to %s."
        )
        farezone_warning4 = (
            "WARNING: the above issue has occurred more than once for the same line. "
            "There is a feasible boarding-alighting on the this line with no fare defined in "
            "the fare matrix."
        )
        farezone_warning5 = (
            "Warning: no entry in farematrix %s from-to %s-%s: "
            "invehicle cost at segment %s set to %s"
        )
        matrix_id = fs_data["FAREMATRIX ID"]

        self._log.append(
            {"type": "text2", "content": "Using zone boundary crossings approximation"}
        )
        for line in lines:
            prev_farezone = 0
            same_farezone_missing_cost = False
            # Get list of stop segments
            stop_segments = [
                seg
                for seg in line.segments(include_hidden=True)
                if (seg.allow_alightings or seg.allow_boardings)
            ]
            prev_seg = None
            for i, seg in enumerate(stop_segments):
                farezone = int(seg.i_node["@farezone"])
                if farezone not in valid_farezones:
                    self._log.append(
                        {
                            "type": "text3",
                            "content": farezone_warning1 % (farezone, line, seg.i_node),
                        }
                    )
                    if prev_farezone != 0:
                        farezone = prev_farezone
                        msg = "farezone from previous stop segment,"
                    else:
                        # DH added first farezone fix instead of exception
                        farezone = list(valid_farezones)[0]
                        self._log.append(
                            {
                                "type": "text3",
                                "content": farezone_warning2
                                % (fs_data["NUMBER"], line, seg.i_node),
                            }
                        )
                        msg = "first valid farezone in faresystem,"
                    self._log.append(
                        {
                            "type": "text3",
                            "content": "Using %s farezone %s" % (msg, farezone),
                        }
                    )
                if seg.allow_boardings:
                    # get the cost travelling within this farezone as base boarding cost
                    board_cost = fare_matrix.get(farezone, {}).get(farezone)
                    if board_cost is None:
                        # If this entry is missing from farematrix,
                        # use next farezone if both previous stop and next stop are in different farezones
                        if (
                            i == len(stop_segments) - 1
                        ):  # in case the last segment has missing fare
                            board_cost = min(fare_matrix[farezone].values())
                        else:
                            next_seg = stop_segments[i + 1]
                            next_farezone = next_seg.i_node["@farezone"]
                            if next_farezone != farezone and prev_farezone != farezone:
                                board_cost = fare_matrix.get(farezone, {}).get(
                                    next_farezone
                                )
                    if board_cost is None:
                        # use the smallest fare found from this farezone as best guess
                        # as a reasonable boarding cost
                        board_cost = min(fare_matrix[farezone].values())
                        self._log.append(
                            {
                                "type": "text3",
                                "content": farezone_warning3
                                % (matrix_id, farezone, farezone, seg, board_cost),
                            }
                        )
                        if same_farezone_missing_cost == farezone:
                            self._log.append(
                                {"type": "text3", "content": farezone_warning4}
                            )
                        same_farezone_missing_cost = farezone
                    seg["@board_cost"] = max(board_cost, seg["@board_cost"])

                farezone = int(seg.i_node["@farezone"])
                # Set the zone-to-zone fare increment from the previous stop
                if prev_farezone != 0 and farezone != prev_farezone:
                    try:
                        invehicle_cost = (
                            fare_matrix[prev_farezone][farezone]
                            - prev_seg["@board_cost"]
                        )
                        prev_seg["@invehicle_cost"] = max(
                            invehicle_cost, prev_seg["@invehicle_cost"]
                        )
                    except KeyError:
                        self._log.append(
                            {
                                "type": "text3",
                                "content": farezone_warning5
                                % (matrix_id, prev_farezone, farezone, prev_seg, 0),
                            }
                        )
                if farezone in valid_farezones:
                    prev_farezone = farezone
                prev_seg = seg

    def station_to_station_approx(
        self, valid_farezones, fare_matrix, zone_nodes, valid_links, network
    ):
        network.create_attribute("LINK", "board_index", -1)
        network.create_attribute("LINK", "invehicle_index", -1)
        self._log.append(
            {
                "type": "text2",
                "content": "Using station-to-station least squares estimation",
            }
        )
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
                if (
                    link.j_node["@farezone"] != farezone
                    and link.j_node["@farezone"] in valid_farezones
                ):
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
                    path_links = find_path(
                        orig_node,
                        dest_node,
                        lambda l: l in valid_links,
                        lambda l: l.length,
                    )
                except NoPathFound:
                    continue
                b.append(cost)
                a_indices = [0] * index

                a_indices[path_links[0].board_index] = 1
                for link in path_links:
                    if link.invehicle_index == -1:
                        continue
                    a_indices[link.invehicle_index] = 1
                A.append(a_indices)

        # x, res, rank, s = _np.linalg.lstsq(A, b, rcond=None)
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
                    [
                        link.i_node.id,
                        link.j_node.id,
                        int(link.i_node["@farezone"]),
                        link.board_cost,
                        link.invehicle_cost,
                    ]
                )

        self._log.append(
            {"type": "text2", "content": "Table of boarding and in-vehicle costs"}
        )
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
                    path_links = find_path(
                        orig_node,
                        dest_node,
                        lambda l: l in valid_links,
                        lambda l: l.length,
                    )
                    path_cost = path_links[0].board_cost + sum(
                        l.invehicle_cost for l in path_links
                    )
                    row.append("%s, $%.2f" % (cost, path_cost))
                except NoPathFound:
                    row.append("%s, NO PATH" % (cost))
            prev_p = p
        table_content.append(row)

        self._log.append(
            {
                "type": "text2",
                "content": "Table of origin station p to destination station q input cost, estimated cost",
            }
        )
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
        max_xfer_dist = self.config.fare_max_transfer_distance_miles * 5280.0
        self._log.append({"type": "header", "content": "Faresystem distances"})
        self._log.append(
            {"type": "text2", "content": "Max transfer distance: %s" % max_xfer_dist}
        )

        def bounding_rect(shape):
            if shape.bounds:
                x_min, y_min, x_max, y_max = shape.bounds
                return _geom.Polygon(
                    [(x_min, y_max), (x_max, y_max), (x_max, y_min), (x_min, y_min)]
                )
            return _geom.Point()

        for fs_index, fs_data in enumerate(faresystems.values()):
            stops = set([])
            for line in fs_data["LINES"]:
                for stop in line.segments(True):
                    if stop.allow_alightings or stop.allow_boardings:
                        stops.add(stop.i_node)
            fs_data["shape"] = _geom.MultiPoint([(stop.x, stop.y) for stop in stops])
            # fs_data["bounding_rect"] = bounding_rect(fs_data["shape"])
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
                    # distance = fs_data["bounding_rect"].distance(fs_data2["bounding_rect"])
                    # if distance <= max_xfer_dist:
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
                            self._log.append(
                                {
                                    "type": "text3",
                                    "content": "Warning: non-zero transfer within 'FROMTO' faresystem not supported",
                                }
                            )
                    else:
                        xfer = "BOARD+%s" % fs_data2["FAREFROMFS"][fs_id]
                else:
                    xfer = fs_data2["FAREFROMFS"][fs_id]
                xfer_fares[fs_id2] = xfer

        distance_table = [["p/q"] + list(faresystems.keys())]
        for fs, fs_data in faresystems.items():
            distance_table.append(
                [fs]
                + [
                    ("%.0f" % d if isinstance(d, float) else d)
                    for d in fs_data["distance"]
                ]
            )
        self._log.append(
            {
                "type": "text2",
                "content": "Table of distance between stops in faresystems (feet)",
            }
        )
        self._log.append({"content": distance_table, "type": "table"})

    def group_faresystems(self, faresystems):
        self._log.append(
            {"type": "header", "content": "Faresystem groups for ALL MODES"}
        )

        def matching_xfer_fares(xfer_fares_list1, xfer_fares_list2):
            for xfer_fares1 in xfer_fares_list1:
                for xfer_fares2 in xfer_fares_list2:
                    for fs_id, fare1 in xfer_fares1.items():
                        fare2 = xfer_fares2[fs_id]
                        if fare1 != fare2 and (
                            fare1 != "TOO_FAR" and fare2 != "TOO_FAR"
                        ):
                            # if the difference between two fares are less that a number, 
                            # then treat them as the same fare
                            if isinstance(fare1, float) and isinstance(fare2, float) and (
                                abs(fare1 - fare2)<=1.5
                                ):
                                continue
                            else:
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
            {
                "type": "header",
                "content": "Faresystems grouped by compatible transfer fares",
            }
        )
        xfer_fares_table = [["p/q"] + list(faresystems.keys())]
        faresystem_groups = []
        i = 0
        for xfer_fares_list, group, modes in group_xfer_fares:
            xfer_fares = {}
            for fs_id in faresystems.keys():
                to_fares = [f[fs_id] for f in xfer_fares_list if f[fs_id] != "TOO_FAR"]
                # fare = to_fares[0] if len(to_fares) > 0 else 0.0
                if len(to_fares) == 0:
                    fare = 0.0
                elif all(isinstance(item, float) for item in to_fares): 
                    # caculate the average here becasue of the edits in matching_xfer_fares function
                    fare = round(sum(to_fares)/len(to_fares),2)
                else:
                    fare = to_fares[0]
                xfer_fares[fs_id] = fare
            faresystem_groups.append((group, xfer_fares))
            for fs_id in group:
                xfer_fares_table.append(
                    [fs_id] + list(faresystems[fs_id]["xfer_fares"].values())
                )
            i += 1
            self._log.append(
                {
                    "type": "text2",
                    "content": "Level %s faresystems: %s modes: %s"
                    % (
                        i,
                        ", ".join([str(x) for x in group]),
                        ", ".join([str(m) for m in modes]),
                    ),
                }
            )

        self._log.append(
            {
                "type": "header",
                "content": "Transfer fares list by faresystem, sorted by group",
            }
        )
        self._log.append({"content": xfer_fares_table, "type": "table"})

        return faresystem_groups

    def generate_transfer_fares(self, faresystems, faresystem_groups, network):
        self.create_attribute("MODE", "#orig_mode", self.scenario, network, "STRING")
        self.create_attribute(
            "TRANSIT_LINE", "#src_mode", self.scenario, network, "STRING"
        )
        self.create_attribute(
            "TRANSIT_LINE", "#src_veh", self.scenario, network, "STRING"
        )

        transit_modes = set([m for m in network.modes() if m.type == "TRANSIT"])
        #remove PNR dummy route from transit modes
        transit_modes -= set([m for m in network.modes() if m.description == "pnrdummy"])
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
            if line.mode.id != "p": #remove PNR dummy mode
                lines[line.vehicle.id].append(line)
            line["#src_mode"] = line.mode.id
            line["#src_veh"] = line.vehicle.id
        for vehicle in network.transit_vehicles():
            if vehicle.mode.id != "p": #remove PNR dummy mode
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
            # {
            #     "description": "base",
            #     "destinations_reachable": True,
            #     "transition_rules": transition_rules,
            #     "waiting_time": None,
            #     "boarding_time": None,
            #     "boarding_cost": None,
            # }
        ]
        mode_map = _defaultdict(lambda: [])
        level = 1
        for fs_ids, xfer_fares in faresystem_groups:
            boarding_cost_id = "@from_level_%s" % level
            self.create_attribute(
                "TRANSIT_SEGMENT", boarding_cost_id, self.scenario, network
            )
            journey_levels.append(
                {
                    "description": "Level_%s fs: %s"
                    % (level, ",".join([str(x) for x in fs_ids])),
                    "destinations_reachable": True,
                    "transition_rules": transition_rules,
                    "waiting_time": None,
                    "boarding_time": None,
                    "boarding_cost": {
                        "global": None,
                        "at_nodes": None,
                        "on_lines": None,
                        "on_segments": {
                            "penalty": boarding_cost_id,
                            "perception_factor": 1,
                        },
                    },
                }
            )

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
                        transition_rules.append(
                            {"mode": level_mode.id, "next_journey_level": level}
                        )
                        level_modes[line["#src_mode"]] = level_mode
                        mode_map[line["#src_mode"]].append(level_mode.id)
                    for segment in line.segments():
                        segment.link.modes |= set([level_mode])
                    new_vehicle = level_vehicles.get(line.vehicle.id)
                    if new_vehicle is None:
                        new_vehicle = network.create_transit_vehicle(
                            get_vehicle_id(), level_mode
                        )
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
                elif isinstance(xferboard_cost, str) and xferboard_cost.startswith(
                    "BOARD+"
                ):
                    xferboard_cost = float(xferboard_cost[6:])
                    for segment in line.segments():
                        if segment.allow_boardings:
                            segment[boarding_cost_id] = max(
                                xferboard_cost + segment["@board_cost"], 0
                            )
                else:
                    for segment in line.segments():
                        if segment.allow_boardings:
                            segment[boarding_cost_id] = max(xferboard_cost, 0)
            level += 1

        # for vehicle in network.transit_vehicles():
        #     if vehicle.mode == meta_mode:
        #         network.delete_transit_vehicle(vehicle)
        # for link in network.links():
        #     link.modes -= set([meta_mode])
        # network.delete_mode(meta_mode)
        self._log.append(
            {
                "type": "header",
                "content": "Mapping from original modes to modes for transition table",
            }
        )
        for orig_mode, new_modes in mode_map.items():
            self._log.append(
                {
                    "type": "text2",
                    "content": "%s : %s" % (orig_mode, ", ".join(new_modes)),
                }
            )
        return journey_levels, mode_map

    def save_journey_levels(self, name, journey_levels):
        spec_dir = os.path.join(
            os.path.dirname(
                self.get_abs_path(self.controller.config.emme.project_path)
            ),
            "Specifications",
        )
        path = os.path.join(spec_dir, "%s_%s_journey_levels.ems" % (self.period, name))
        with open(path, "w") as jl_spec_file:
            spec = {
                "type": "EXTENDED_TRANSIT_ASSIGNMENT",
                "journey_levels": journey_levels,
            }
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
        # manager = self.controller.emme_manager
        # emme_project = manager.project
        # manager.modeller(emme_project)
        # PageBuilder = _m.PageBuilder
        report = PageBuilder(title="Fare calculation report")
        try:
            for item in self._log:
                if item["type"] == "header":
                    report.add_html(
                        "<h3 style='margin-left:10px'>%s</h3>" % item["content"]
                    )
                elif item["type"] == "text":
                    report.add_html(
                        "<div style='margin-left:20px'>%s</div>" % item["content"]
                    )
                elif item["type"] == "text2":
                    report.add_html(
                        "<div style='margin-left:30px'>%s</div>" % item["content"]
                    )
                elif item["type"] == "text3":
                    report.add_html(
                        "<div style='margin-left:40px'>%s</div>" % item["content"]
                    )
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
                    report.add_html(
                        """
                        <div style='margin-left:20px'>
                            %s
                            <table>%s</table>
                        </div>
                        <br>
                        """
                        % (title, "".join(table_msg))
                    )

        except Exception as error:
            # no raise during report to avoid masking real error
            report.add_html("Error generating report")
            report.add_html(str(error))
            report.add_html(_traceback.format_exc())

        self.controller.emme_manager.logbook_write(
            "Apply fares report %s" % self.period, report.render()
        )

    def log_text_report(self):
        bank_dir = os.path.dirname(
            self.get_abs_path(self.controller.config.emme.transit_database_path)
        )
        timestamp = _time.strftime("%Y%m%d-%H%M%S")
        path = os.path.join(
            bank_dir, "apply_fares_report_%s_%s.txt" % (self.period, timestamp)
        )
        with open(path, "w") as report:
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
