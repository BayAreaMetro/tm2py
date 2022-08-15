"""Transit network preparation module."""

from __future__ import annotations

from collections import defaultdict as _defaultdict
from msilib import init_database
from typing import Dict

import pandas as pd
from typing_extensions import TYPE_CHECKING, Literal

from tm2py.components.component import Component
from tm2py.emme.manager import EmmeLink, EmmeNetwork, EmmeScenario
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
        self.emme_manager = self.controller.emme_manager
        self._transit_emmebank = None
        self._transit_networks = None
        self._transit_scenarios = None
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
        for time_period in self.time_period_names:
            self._update_auto_times(time_period)
            if self.config.override_connector_times:
                self._update_connector_times(time_period)

    @property
    def transit_emmebank(self):
        if not self._transit_emmebank:
            self._transit_emmebank = self.controller.emme_manager.emmebank(
                self.get_abs_path(self.controller.config.emme.transit_database_path)
            )
        return self._transit_emmebank

    @property
    def highway_emmebank(self):
        if not self._highway_emmebank:
            self._highway_emmebank = self.controller.emme_manager.emmebank(
                self.get_abs_path(self.controller.config.emme.highway_database_path)
            )
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
        return self.highway_scenarios

    @property
    def transit_networks(self):
        if self._transit_networks is None:
            self._transit_networks = {
                tp: self.transit_scenario[tp].get_partial_network(
                    ["TRANSIT_SEGMENT"], include_attributes=False
                )
                for tp in self.time_period_names
            }
        return self._transit_networks

    @property
    def access_connector_df(self):
        if not self._access_connector_df:
            self._access_connector_df = pd.read_csv(
                self.config.input_connector_access_times_path
            )
        return self._access_connector_df

    @property
    def egress_connector_df(self):
        if not self._egress_connector_df:
            self._egress_connector_df = pd.read_csv(
                self.config.input_connector_egress_times_path
            )
        return self._egress_connector_df

    def _update_auto_times(self, time_period: str):
        """Update the auto travel times from the last auto assignment to the transit scenario.

        TODO Document steps more when understand them.

        Note: may need to remove "reliability" factor in future versions of VDF def

        Args:
            time_period: time period name abbreviation
        """

        _highway_ttime_dict = self._get_highway_travel_times(time_period)
        _transit_link_dict = self._get_transit_links(time_period)

        for _link_id in _highway_ttime_dict.keys() & _transit_link_dict.keys():
            _transit_link_dict[_link_id]["@trantime"] = _highway_ttime_dict[_link_id]

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
            "LINK": ["@trantime"],
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
        _network = self.networks[time_period]
        _scenario = self.scenarios[time_period]
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
            for _tclass in self.config.transit.classes
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
            "LINK": ["#link_id", "@trantime"],
            "TRANSIT_SEGMENT": ["@schedule_time", "@trantime_seg", "data1"],
        }
        self.emme_manager.copy_attribute_values(
            _transit_scenario, _transit_net, transit_attributes
        )
        _transit_link_dict = {
            tran_link["#link_id"]: tran_link for tran_link in _transit_net.links()
        }
        return _transit_link_dict

    def _get_highway_travel_times(
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
        travel_time_attributes = {"LINK": ["#link_id", "auto_time"]}
        self.emme_manager.copy_attribute_values(
            _highway_scenario, _highway_net, travel_time_attributes
        )
        # TODO can we just get the link attributes as a DataFrame and merge them?
        auto_link_time_dict = {
            auto_link["#link_id"]: auto_link.auto_time
            for auto_link in _highway_net.links()
        }
        return auto_link_time_dict
