"""Module for post processing.
"""

import heapq as _heapq
import os
from typing import TYPE_CHECKING, Dict, List, Set

import pandas as pd
from shapely.geometry import mapping, LineString
import json
import numpy as np

from tm2py.components.component import Component, FileFormatError
from tm2py.emme.manager import EmmeNetwork, EmmeScenario, EmmeMatrix
from tm2py.logger import LogStartEnd, Logger
#from tm2py.config import LoggingConfig

if TYPE_CHECKING:
    from tm2py.controller import RunController


class PostProcessor(Component):
    """Post Processing."""

    def __init__(self, controller: "RunController"):
        """Constructor for PostProcessor.

        Args:
            controller (RunController): Reference to run controller object.
        """
        print("Starting initialization...")
        super().__init__(controller)
        self.config = self.controller.config.post_processor
        self._emme_manager = self.controller.emme_manager
        self._transit_emmebank = None
        self._transit_networks = None
        self._transit_scenarios = None
        self._highway_emmebank = None
        self._highway_scenarios = None
        self._individual_trips = None
        self._joint_trips = None

        self._tp_mapping = {
            tp.name.upper(): tp.emme_scenario_id
            for tp in self.controller.config.time_periods
        }

    @LogStartEnd("Exporting model networks")
    def run(self):
        """Export model networks."""

        indiv_trip_file = 'ctramp_output/indivTripData_3.csv'
        joint_trip_file = 'ctramp_output/jointTripData_3.csv'
        
        indiv_trip = pd.read_csv(self.get_abs_path(indiv_trip_file))
        #joint_trip = pd.read_csv(self.get_abs_path(joint_trip_file))
        

        # Prepare trip and tour dataframes by adding skim columns and time period (from start duration) """
        indiv_trip = self._add_skim_columns(indiv_trip)
        #joint_trip = self._add_skim_columns(joint_trip)

        for period in self.controller.time_period_names:
            with self.controller.emme_manager.logbook_trace(
                f"exporting networks for {period}"
            ):
                self.logger.log(f"Processing for {period}")
                transit_scenario = self.transit_emmebank.scenario(period)
                highway_scenario = self.highway_emmebank.scenario(period)
                
                indiv_trip = self._attach_highway_skims_to_trip(highway_scenario, period, indiv_trip)
                indiv_trip = self._attach_transit_skims_to_trip(transit_scenario, period, indiv_trip)

                #self._export_transit_network_as_shapefile(transit_scenario, period)
                #self._export_highway_network_as_shapefile(highway_scenario, period)
                # if period.upper() == "AM":
                #     self._export_boardings_by_segment(transit_scenario, period)
                #     self._export_boardings_by_segment_geofile(transit_scenario, period)
        #indiv_trip.to_csv(self.get_abs_path("updated_output/indivTripData_3.csv"))
        indiv_trip.to_parquet(self.get_abs_path("updated_output/indivTripData_3.parquet"))

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

    def _attach_highway_skims_to_trip(self, scenario: EmmeScenario, time_period: str, output: pd.DataFrame):
        """Attach skim (time, dist, cost, bridge toll, value toll) values to trips. 
        Args:
            scenario (EmmeScenario): Emme Scenario (i.e., TIme Period for Emme)
            time_period (str): Time period name.
            output (pd.DataFrame): DataFrame of CTRAMP trips (Indiv/Joint).
        """
        self.logger.log(f"Attaching highway skim to trips for time period {time_period}")
        emmebank = scenario.emmebank
        # Establishing matrix mode naming
        modes = {'da', 'datoll', 'sr2', 'sr2toll', 'sr3', 'sr3toll'}
        # Based on TripMode UEC, correlating the proper OMX modes to ctramp output modes
        modes_to_output = {'da': [1, 3, 6, 17], 
                           'datoll': [2],
                           'sr2': [4],
                           'sr2toll': [5, 15, 16],
                           'sr3': [7],
                           'sr3toll': [8]
                           }
   
        for mode in modes:
        # Read all matrices at once
            self.logger.log(f"Reading highway matrices from Emmebank for mode: {mode}")
            matrices = {
                'auto_time': emmebank.matrix(f"{time_period}_{mode}_time").get_numpy_data(),
                'auto_dist': emmebank.matrix(f"{time_period}_{mode}_dist").get_numpy_data(),
                'auto_cost': emmebank.matrix(f"{time_period}_{mode}_cost").get_numpy_data()
            }
            
            # Only create indices once
            if 'toll' in mode:
                matrices.update({
                    'auto_bridge_toll': emmebank.matrix(f"{time_period}_{mode}_bridgetoll_{mode[:-4]}").get_numpy_data(),
                    'auto_value_toll': emmebank.matrix(f"{time_period}_{mode}_valuetoll_{mode[:-4]}").get_numpy_data()
                })
            
            self.logger.log(f"Emmebank Highway Matrices: \n{matrices}", level = 'DEBUG')
            # Filter trips for this mode once
            self.logger.log(f"Filtering trips for mode: {mode} and timeperiod: {time_period}")
            mode_trips = output['trip_mode'].isin(modes_to_output[mode])
            period_trips = output['timeperiod'] == time_period
            mask = mode_trips & period_trips
            
            if not mask.any():
                self.logger.log(f"No trips for mode: {mode} in timeperiod: {time_period}")
                continue
                
            # Only create indices where needed - converting 1-based index to 0-based index for matrices lookup
            self.logger.log("Creating indices for trip origins and destinations", level = 'DETAIL')
            self.logger.log("Converting OD index from 1-based to 0-based for matrices lookup", level = 'DETAIL')
            trip_origins = output.loc[mask, 'origin_TAZ_SEQ'].values - 1
            trip_dests = output.loc[mask, 'destination_TAZ_SEQ'].values - 1
            
            # Extract values directly using advanced indexing
            for name, matrix in matrices.items():
                self.logger.log(f'Extracting values for {name}')
                output.loc[mask, name] = matrix[trip_origins, trip_dests]

        # Adjust time for school bus based on TripModeChoice UEC (Time at 20 mph = sov_dist * 3)
        self.logger.log("Adjust time for school bus mode")
        output.loc[output['trip_mode'] == 17, 'auto_time'] = output.loc[output['trip_mode'] == 17, 'auto_dist'] * 3
        

        return output


    def _attach_transit_skims_to_trip(self, scenario: EmmeScenario, time_period: str, output: pd.DataFrame):
        """Attach transit skim values to trips and tours.
        Args:
            scenario (EmmeScenario): Emme scenario for the time period.
            time_period (str): Time period name.
            trip_tour (pd.DataFrame): DataFrame of CTRAMP trips or tours outputs (Indiv/Joint).
        """
        # TODO
        # Filter dataframe from mode and time period
        # Mode not taken is null
        # Use EmmeMatrix to get skim values
        # establish columns for the trips 
        # Established time periods based on start and end periods (won't need to establish this as part of core summaries)

        # Transit:
        # {timeperiod}_transit_[in-vehicle time, wait time, walk time, cost, distance]
        # Need to filter by: time, mode, and return trip
        self.logger.log(f"Attaching transit skim to trips for time period: {time_period}")

        emmebank =scenario.emmebank

        # Transit skims mode to ctramp modes association
        transit_modes = {
            'WLK_TRN_WLK': [11],
            'WLK_TRN_KNR' : [13, 14],
            'WLK_TRN_PNR' : [12],
            'KNR_TRN_WLK' :[13, 14],
            'PNR_TRN_WLK' :[12]

        }
        for mode in transit_modes:
            self.logger.log(f"Attaching skims for mode: {mode}")
            matrices = {
                'transit_ivt': emmebank.matrix(f"{time_period}_{mode}_IVT").get_numpy_data(),
                'transit_iwait': emmebank.matrix(f"{time_period}_{mode}_IWAIT").get_numpy_data(),
                'transit_xwait': emmebank.matrix(f"{time_period}_{mode}_XWAIT").get_numpy_data(),
                'transit_transfer': emmebank.matrix(f"{time_period}_{mode}_XWAIT").get_numpy_data(),
                'transit_fare': emmebank.matrix(f"{time_period}_{mode}_FARE").get_numpy_data(),
                'transit_wacc': emmebank.matrix(f"{time_period}_{mode}_WACC").get_numpy_data(),
                'transit_waux': emmebank.matrix(f"{time_period}_{mode}_WAUX").get_numpy_data(),
                'transit_wegr': emmebank.matrix(f"{time_period}_{mode}_WEGR").get_numpy_data(),
                'transit_dtime': emmebank.matrix(f"{time_period}_{mode}_DTIME").get_numpy_data(),
            }

        # Total Transit Time = IVT + IWAIT + XTRANSFER + WAUX + [WACC/WEGR/DTIME] depending on path taken

            period_trips = output['timeperiod'] == time_period
            mode_trips = output['trip_mode'].isin(transit_modes[mode])
            
            # Determining whether trip was walk to or walk from transit based on if trip is inbound or outbound
            # If trip is inbound (inbound == 0), then they are driving to transit
            if mode in ['KNR_TRN_WLK', 'PNR_TRN_WLK']:
                inbound = output['inbound'] == 0
                mask = period_trips & mode_trips & inbound

            elif mode in ['WLK_TRN_KNR', 'WLK_TRN_PNR']:
                inbound = output['inbound'] == 1
                mask = period_trips & mode_trips & inbound
           
            else:
                mask = period_trips & mode_trips

            if not mask.any():
                self.logger.log(f"No trips for mode: {mode} in timeperiod: {time_period}")
                continue

            trip_origins = output.loc[mask, 'origin_TAZ_SEQ'].values - 1
            trip_dests = output.loc[mask, 'destination_TAZ_SEQ'].values - 1

            for name, matrix in matrices.items():
                #self.logger.log(f'Extracting values for {name}')
                output.loc[mask, name] = matrix[trip_origins, trip_dests]

        return output






    def _add_skim_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add skim columns (time, distance, cost), timeperiod, and TAZ_SEQ/TAZ_Node to the DataFrame. 

        Args:
            df (pd.DataFrame): Trip Dataframe to add skim columns to.

        Returns:
            pd.DataFrame: DataFrame with added skim columns.
        """
        # TODO
        # Add columns for skims
        self.logger.log("Adding time, distance, cost, and time period columns to trips")
        df[['auto_time', 'auto_dist', 'auto_cost', 'auto_bridge_toll', 'auto_value_toll', 
           'transit_ivt', 'transit_iwait', 'transit_xwait', 'transit_waux', 
           'transit_wacc', 'transit_wegr', 'transit_dtime','transit_fare',
           'walk_time', 'walk_dist', 'bike_time', 'bike_dist']] = None
        df['timeperiod'] = pd.cut(df['stop_period'], bins = [1, 4, 12, 22, 30, 40], labels = ['EA', 'AM', 'MD', 'PM', 'EV'], include_lowest= True)

        # Add TAZ Sequential/TAZ_NODE to dataframe based on landuse input file
        landuse_file = 'inputs/landuse/maz_data_withDensity.csv'
        self.logger.log(f"Reading landuse file from {self.get_abs_path(landuse_file)}")
        landuse_input = pd.read_csv(self.get_abs_path(landuse_file))
        
        # Won't need to do this once variable rename is complete
        landuse_input.rename(columns = {"MAZ": 'MAZ_SEQ', 'TAZ': 'TAZ_SEQ', 'MAZ_ORIGINAL': 'MAZ_NODE', 'TAZ_ORIGINAL': 'TAZ_NODE'}, inplace = True)
        self.logger.log(f"MAZ Input: \n{landuse_input.head()}", level = 'DEBUG')

        # Attach origin TAZ and destination TAZ
        self.logger.log("Adding origin TAZ")
        df = df.merge(landuse_input[['MAZ_SEQ', 'TAZ_SEQ', 'MAZ_NODE','TAZ_NODE']], left_on = 'orig_mgra', right_on = 'MAZ_SEQ', how = 'left', validate= 'm:1')
        df.rename(columns = {'MAZ_SEQ': 'origin_MAZ_SEQ', 'TAZ_SEQ': 'origin_TAZ_SEQ', 
                             'MAZ_NODE': 'origin_MAZ_NODE', 'TAZ_NODE': 'origin_TAZ_NODE'}, inplace = True)

        self.logger.log("Adding destination TAZ")
        df = df.merge(landuse_input[['MAZ_SEQ', 'TAZ_SEQ', 'MAZ_NODE','TAZ_NODE']], left_on = 'dest_mgra', right_on = 'MAZ_SEQ', how = 'left', validate= 'm:1')
        df.rename(columns = {'MAZ_SEQ': 'destination_MAZ_SEQ', 'TAZ_SEQ': 'destination_TAZ_SEQ', 
                             'MAZ_NODE': 'destination_MAZ_NODE', 'TAZ_NODE': 'destination_TAZ_NODE'}, inplace = True)
        self.logger.log(f"Proccessed Trip Data: \n: {df.head()}", level = 'DEBUG')

        return df
