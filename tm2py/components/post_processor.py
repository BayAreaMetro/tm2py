"""Module for post processing.
"""

import heapq as _heapq
import os
from typing import TYPE_CHECKING, Dict, List, Set

import pandas as pd
from shapely.geometry import mapping, LineString
import json

from tm2py.components.component import Component, FileFormatError
from tm2py.emme.manager import EmmeNetwork, EmmeScenario
from tm2py.logger import LogStartEnd

if TYPE_CHECKING:
    from tm2py.controller import RunController


class PostProcessor(Component):
    """Post Processing."""

    def __init__(self, controller: "RunController"):
        """Constructor for PostProcessor.

        Args:
            controller (RunController): Reference to run controller object.
        """
        super().__init__(controller)
        self.config = self.controller.config.post_processor
        self._emme_manager = self.controller.emme_manager
        self._transit_emmebank = None
        self._transit_networks = None
        self._transit_scenarios = None
        self._highway_emmebank = None
        self._highway_scenarios = None

        self._tp_mapping = {
            tp.name.upper(): tp.emme_scenario_id
            for tp in self.controller.config.time_periods
        }

    @LogStartEnd("Exporting model networks")
    def run(self):
        """Export model networks."""
        for period in self.controller.time_period_names:
            with self.controller.emme_manager.logbook_trace(
                f"exporting networks for {period}"
            ):
                transit_scenario = self.transit_emmebank.scenario(period)
                highway_scenario = self.highway_emmebank.scenario(period)
                self._export_transit_network_as_shapefile(transit_scenario, period)
                self._export_highway_network_as_shapefile(highway_scenario, period)
                if period.upper() == "AM":
                    self._export_boardings_by_segment(transit_scenario, period)
                    self._export_boardings_by_segment_geofile(transit_scenario, period)

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
        self._transit_networks = {
            tp: self.transit_scenarios[tp].get_network()
            for tp in self.time_period_names
        }
        return self._transit_networks

    def _export_transit_network_as_shapefile(self, scenario: EmmeScenario, time_period: str):
        """Export transit segments and lines as shapefiles."""
        network_to_shapefile = self.controller.emme_manager.tool(
            "inro.emme.data.network.export_network_as_shapefile"
        )
        path_tmplt = self.get_abs_path(self.config.network_shapefile_path)
        period_scen_id = self._tp_mapping[time_period]
        output_path = path_tmplt.format(period=period_scen_id)
        network_to_shapefile(
            export_path = output_path,
            scenario = scenario,
            transit_shapes = "LINES_AND_SEGMENTS",
            selection={
                "link":'none',
                "node":'none',
                "turn": "none",
                "transit_line":'all'
            }
        )
        # emme_nodes and emme_links are empty
        # use links and nodes shapefiles from highway scenario
        for filename in os.listdir(output_path):
            if os.path.isfile(os.path.join(output_path, filename)):
                base_name, _ = os.path.splitext(filename)
                if base_name in ["emme_nodes","emme_links"]:
                    filepath = os.path.join(output_path, filename)
                    os.remove(filepath)

    def _export_highway_network_as_shapefile(self, scenario: EmmeScenario, time_period: str):
        """Export highway nodes and links as shapefiles."""
        network_to_shapefile = self.controller.emme_manager.tool(
            "inro.emme.data.network.export_network_as_shapefile"
        )
        path_tmplt = self.get_abs_path(self.config.network_shapefile_path)
        period_scen_id = self._tp_mapping[time_period]
        output_path = path_tmplt.format(period=period_scen_id)
        network_to_shapefile(
            export_path = output_path,
            scenario = scenario,
            selection={
                "link":'all',
                "node":'all',
                "turn": "all",
                "transit_line":'none'
            }
        )

    def _export_boardings_by_segment(self, scenario: EmmeScenario, time_period: str):
        """Export transit segment boardings to a CSV file.

        The output includes dwell time, travel time function, segment volumes,
        total and seated capacity of transit line per hour for each transit segment
        in the specified time period.
        """
        transit_network = scenario.get_network()
        path_tmplt = self.get_abs_path(self.config.boardings_by_segment_file_path)
        period_scen_id = self._tp_mapping[time_period]
        output_path = path_tmplt.format(period=period_scen_id)
        with open(output_path, "w") as f:
            f.write(
                ",".join(
                    [
                    "Line", 
                    "From",
                    "To",
                    "Length", 
                    "Dwt",
                    "capt",
                    "TTF",
                    "voltr",
                    "caps",
                    "Data1",
                    "Data2",
                    "Data3"
                    ]
                )
            )
            f.write("\n")

            for line in transit_network.transit_lines():
                total_capacity = line.vehicle.total_capacity
                seated_capacity = line.vehicle.seated_capacity
                hdw = line.headway
                line_hour_total_cap = 60 * total_capacity / hdw
                line_hour_seated_cap = 60 * seated_capacity / hdw
                for segment in line.segments(include_hidden=False):
                    f.write(
                        ",".join(
                            [
                                str(x) 
                                for x in [
                                    segment.line.id, 
                                    segment.i_node, 
                                    segment.j_node,
                                    segment.link.length,  
                                    segment.dwell_time,
                                    line_hour_total_cap,
                                    segment.transit_time_func,
                                    segment.transit_volume,
                                    line_hour_seated_cap,
                                    segment.data1,
                                    segment.data2,
                                    segment.data3
                                ]
                            ]
                        )
                    )
                    f.write("\n")

    def _export_boardings_by_segment_geofile(self, scenario: EmmeScenario, time_period: str):
        """Export transit segment boardings to a geojson file.

        The output includes segment volumes, total and seated capacity 
        of transit line per hour for each transit segment in the specified time period.
        """
        transit_network = scenario.get_network()
        path_tmplt = self.get_abs_path(self.config.boardings_by_segment_geofile_path)
        output_path = path_tmplt.format(period=time_period.lower())
        features = []

        for line in transit_network.transit_lines():
            total_capacity = line.vehicle.total_capacity
            seated_capacity = line.vehicle.seated_capacity
            hdw = line.headway
            line_hour_total_cap = 60 * total_capacity / hdw
            line_hour_seated_cap = 60 * seated_capacity / hdw

            for segment in line.segments(include_hidden=False):
                geometry = mapping(LineString(segment.link.shape))
                feature = {
                    "type": "Feature",
                    "geometry": geometry,
                    "properties": {
                        "LINE_ID": segment.line.id,
                        "INODE": int(segment.i_node.id),
                        "JNODE": int(segment.j_node.id),
                        "VOLTR": segment.transit_volume,
                        "caps": line_hour_seated_cap,
                        "capt": line_hour_total_cap
                    }
                }
                features.append(feature)

        geojson_data = {
            "type": "FeatureCollection",
            "crs": {
                "type": "name",
                "properties": {"name": "urn:ogc:def:crs:EPSG::2875"},
            },
            "features": features
        }

        with open(output_path, "w") as f:
            json.dump(geojson_data, f, indent=2)
