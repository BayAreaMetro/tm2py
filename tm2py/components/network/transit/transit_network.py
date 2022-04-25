"""Transit network preparation module"""

from __future__ import annotations
from collections import defaultdict as _defaultdict
from typing import Dict
from typing_extensions import Literal

from tm2py.components.component import Component
from tm2py.emme.manager import EmmeScenario, EmmeNetwork, EmmeLink
from tm2py.logger import LogStartEnd


class PrepareTransitNetwork(Component):
    """Transit assignment and skim-related network preparation"""

    @LogStartEnd(
        "Prepare transit network attributes and update times from auto network"
    )
    def run(self):
        """Prepare transit network for assignment by updating link travel times from auto
        network and (if using TAZ-connectors for assignment) update connector walk times.
        """
        emmebank_path = self.get_abs_path(self.config.emme.transit_database_path)
        emmebank = self.controller.emme_manager.emmebank(emmebank_path)
        for time in self.time_period_names():
            scenario = self.get_emme_scenario(emmebank.path, time)
            network = scenario.get_partial_network(
                ["TRANSIT_SEGMENT"], include_attributes=False
            )
            self._update_auto_times(scenario, network, time)
            if self.config.transit.get("override_connector_times", False):
                self._update_connector_times(scenario, network, time)

    def _update_auto_times(
        self, scenario: EmmeScenario, transit_network: EmmeNetwork, period: str
    ):
        """Update the auto travel times from the last auto assignment to the transit scenario.

        Args:
            scenario:
            transit_network:
            period:
        """
        emme_manager = self.controller.emme_manager
        attributes = {
            "LINK": ["#link_id", "@trantime"],
            "TRANSIT_SEGMENT": ["@schedule_time", "@trantime_seg", "data1"],
        }
        emme_manager.copy_attribute_values(scenario, transit_network, attributes)
        auto_emmebank = self.controller.emme_manager.emmebank(
            self.get_abs_path(self.config.emme.highway_database_path)
        )
        auto_scenario = self.get_emme_scenario(auto_emmebank, period)
        if auto_scenario.has_traffic_results:
            auto_network = auto_scenario.get_partial_network(
                ["LINK"], include_attributes=False
            )
            attributes = {"LINK": ["#link_id", "auto_time"]}
            emme_manager.copy_attribute_values(auto_scenario, auto_network, attributes)
            link_lookup = {}
            for auto_link in auto_network.links():
                link_lookup[auto_link["#link_id"]] = auto_link
            for tran_link in transit_network.links():
                auto_link = link_lookup.get(tran_link["#link_id"])
                if not auto_link:
                    continue
                # NOTE: may need to remove "reliability" factor in future versions of VDF definition
                auto_time = auto_link.auto_time
                if auto_time >= 0:
                    tran_link["@trantime"] = auto_time

        # set us1 (segment data1), used in ttf expressions, from @trantime
        for segment in transit_network.transit_segments():
            if segment["@schedule_time"] <= 0 and segment.link is not None:
                segment.data1 = segment["@trantime_seg"] = segment.link["@trantime"]
        attributes = {
            "TRANSIT_SEGMENT": ["@trantime_seg", "data1"],
            "LINK": ["@trantime"],
        }
        emme_manager.copy_attribute_values(transit_network, scenario, attributes)

    def _update_connector_times(
        self, scenario: EmmeScenario, network: EmmeNetwork, period: str
    ):
        """Set the connector times from the source connector times files.

        See also _process_connector_file

        Args:
            scenario:
            network:
            period:
        """
        # walk time attributes per skim set
        emme_manager = self.controller.emme_manager
        attributes = {"NODE": ["@taz_id", "#node_id"]}
        emme_manager.copy_attribute_values(scenario, network, attributes)
        class_attr_map = {
            klass.skim_set_id: f"@walk_time_{klass.name.lower()}"
            for klass in self.config.transit.classes
        }
        for attr_name in class_attr_map.values():
            if scenario.extra_attribute(attr_name) is None:
                scenario.create_extra_attribute("LINK", attr_name)
            # delete attribute in network object to reinitialize to default values
            if attr_name in network.attributes("LINK"):
                network.delete_attribute("LINK", attr_name)
            network.create_attribute("LINK", attr_name, 9999)
        connectors = _defaultdict(lambda: {})
        # lookup adjacent stop ID (also accounts for connector splitting)
        for zone in network.centroids():
            taz_id = int(zone["@taz_id"])
            for link in zone.outgoing_links():
                connectors[taz_id][int(link.j_node["#node_id"])] = link
            for link in zone.incoming_links():
                connectors[int(link.i_node["#node_id"])][taz_id] = link
        self._process_connector_file("access", connectors, class_attr_map, period)
        self._process_connector_file("egress", connectors, class_attr_map, period)
        emme_manager.copy_attribute_values(
            network, scenario, {"LINK": class_attr_map.values()}
        )

    def _process_connector_file(
        self,
        direction: Literal["access", "egress"],
        connectors: Dict[int, Dict[int, EmmeLink]],
        class_attr_map,
        period,
    ):
        """Process the input connector times files and set the times on the connector links.

        Args:
            direction:
            connectors:
            class_attr_map:
            period:
        """
        period_name = period.lower()
        if direction == "access":
            from_name = "from_taz"
            to_name = "to_stop"
            file_path = self.config.transit.input_connector_access_times_path
        else:
            from_name = "from_stop"
            to_name = "to_taz"
            file_path = self.config.transit.input_connector_egress_times_path

        with open(
            self.get_abs_path(file_path), "r", encoding="utf8"
        ) as connector_times:
            header = [x.strip() for x in next(connector_times).split(",")]
            for line in connector_times:
                data = dict(zip(header, line.split(",")))
                if data["time_period"].lower() == period_name:
                    connector = connectors[int(data[from_name])][int(data[to_name])]
                    attr_name = class_attr_map[int(data["skim_set"])]
                    connector[attr_name] = float(data["est_walk_min"])
