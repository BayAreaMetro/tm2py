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
from tm2py.data_models import enriched_output_models
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
        self._iteration_num = None


        self._tp_mapping = {
            tp.name.upper(): tp.emme_scenario_id
            for tp in self.controller.config.time_periods
        }

        # TODO: Convert this to the enums inputs once that is pushed
        self._mode_label ={
            1: "Drive Alone Free",
            2: 'Drive ALone Pay',
            3: 'Shared 2 GP',
            4: 'Shared 2 HOV',
            5: 'Shared 2 Pay',
            6: 'Shared 3 GP',
            7: 'Shared 3 HOV',
            8: 'Shared 3 Pay',
            9: 'Walk',
            10: 'Bike',
            11: 'Walk to Transit',
            12: 'PNR to Transit',
            13: 'KNR to Transit',
            14: 'TNC to Transit',
            15: 'Taxi',
            16: 'TNC',
            17: 'School Bus'
        }

    def _find_highest_iteration(self, base_filename: str, directory: str = 'ctramp_output', max_iter: int = 4) -> str:
        """Find the highest available iteration of a file.
        
        Args:
            base_filename: Base name of the file (e.g., 'indivTripData')
            directory: Directory to search in (default: 'ctramp_output')
            max_iter: Maximum iteration number to check (default: 4)
            
        Returns:
            str: Path to the file with the highest available iteration
            
        Raises:
            FileNotFoundError: If no iteration of the file is found
        """
        for iteration in range(max_iter, 0, -1):
            file_path = f'{directory}/{base_filename}_{iteration}.csv'
            full_path = self.get_abs_path(file_path)
            if os.path.exists(full_path):
                self.logger.log(f"Found {base_filename} at iteration {iteration}")
                return file_path
        
        # If no file found, raise an error with helpful message
        raise FileNotFoundError(
            f"No {base_filename} file found in {directory}. "
            f"Checked iterations 1-{max_iter}. "
            f"Files expected: {base_filename}_1.csv through {base_filename}_{max_iter}.csv"
        )

    @LogStartEnd("Exporting model networks")
    def run(self):
        """Export model networks."""

        # print("Reading trip and tour data")
        # # Find the highest available iteration for each file
        indiv_trip_file = self._find_highest_iteration('indivTripData')
        joint_trip_file = self._find_highest_iteration('jointTripData')
        indiv_tour_file = self._find_highest_iteration('indivTourData')
        joint_tour_file = self._find_highest_iteration('jointTourData')
        
        # Extract iteration number from the file name (e.g., 'indivTripData_3.csv' -> '3')
        self._iteration_num = indiv_trip_file.split('_')[-1].split('.')[0]

        self.logger.log(f"Reading indiv and joint trip files from {indiv_trip_file} and {joint_trip_file}")

        indiv_trip = pd.read_csv(self.get_abs_path(indiv_trip_file))
        joint_trip = pd.read_csv(self.get_abs_path(joint_trip_file))
        
        self.logger.log(f"Reading indiv and joint trip files from {indiv_tour_file} and {joint_tour_file}")
        indiv_tour = pd.read_csv(self.get_abs_path(indiv_tour_file))
        joint_tour = pd.read_csv(self.get_abs_path(joint_tour_file))
        

        ## Prepare trip and tour dataframes by adding skim columns and time period (from start duration) """
        indiv_trip = self._add_trip_skim_columns(indiv_trip)
        joint_trip = self._add_trip_skim_columns(joint_trip)
        indiv_tour = self._add_tour_skim_columns(indiv_tour)
        joint_tour = self._add_tour_skim_columns(joint_tour)

        ## Attach nonmotorized skims
        indiv_trip = self._attach_nonmotorized_skims_to_trip_tour(indiv_trip, 'trip')
        joint_trip = self._attach_nonmotorized_skims_to_trip_tour(joint_trip, 'trip')

        indiv_tour = self._attach_nonmotorized_skims_to_trip_tour(indiv_tour, 'tour')
        joint_tour = self._attach_nonmotorized_skims_to_trip_tour(joint_tour, 'tour')

        for period in self.controller.time_period_names:
            with self.controller.emme_manager.logbook_trace(
                f"exporting networks for {period}"
                ):
                self.logger.log(f"Processing for {period}")
                transit_scenario = self.transit_emmebank.scenario(period)
                highway_scenario = self.highway_emmebank.scenario(period)
                
                indiv_trip = self._attach_highway_skims_to_trip(highway_scenario, period, indiv_trip)
                joint_trip = self._attach_highway_skims_to_trip(highway_scenario, period, joint_trip)

                indiv_tour = self._attach_highway_skims_to_tour(highway_scenario, period, indiv_tour)
                joint_tour = self._attach_highway_skims_to_tour(highway_scenario, period, joint_tour)

                indiv_trip = self._attach_transit_skims_to_trip(transit_scenario, period, indiv_trip)
                joint_trip = self._attach_transit_skims_to_trip(transit_scenario, period, joint_trip)
                
                indiv_tour = self._attach_transit_skims_to_tour(transit_scenario, period, indiv_tour)
                joint_tour = self._attach_transit_skims_to_tour(transit_scenario, period, joint_tour)


                if self.config.export_transit_network_shapefile:
                    self._export_transit_network_as_shapefile(transit_scenario, period)
                
                if self.config.export_highway_network_shapefile:
                    self._export_highway_network_as_shapefile(highway_scenario, period)
                
                if period.upper() == "AM":
                    if self.config.export_boardings_by_segment:
                        self._export_boardings_by_segment(transit_scenario, period)
                    if self.config.export_boardings_by_segment_geofile:
                        self._export_boardings_by_segment_geofile(transit_scenario, period)

        #indiv_trip.to_csv(self.get_abs_path(f"updated_output/indivTripData_{iteration_num}.csv"))
        indiv_trip = self._sum_time_dist_cost(indiv_trip, 'trip')
        joint_trip = self._sum_time_dist_cost(joint_trip, 'trip')
        indiv_tour = self._sum_time_dist_cost(indiv_tour, 'tour')
        joint_tour = self._sum_time_dist_cost(joint_tour, 'tour')

        # Create updated_output directory if it doesn't exist
        updated_output_dir = self.get_abs_path("updated_output")
        os.makedirs(updated_output_dir, exist_ok=True)

        indiv_trip.to_parquet(self.get_abs_path(f"updated_output/indivTripData_{self._iteration_num}.parquet"))
        joint_trip.to_parquet(self.get_abs_path(f"updated_output/jointTripData_{self._iteration_num}.parquet"))

        indiv_tour.to_parquet(self.get_abs_path(f"updated_output/indivTourData_{self._iteration_num}.parquet"))
        joint_tour.to_parquet(self.get_abs_path(f"updated_output/jointTourData_{self._iteration_num}.parquet"))

        self.prepare_output_data()

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
    
    def prepare_output_data(self):
        landuse = self._prepare_landuse_data()
        households = self._prepare_households_data(landuse)

        persons = self._prepare_persons_data(households)
        validated_persons = enriched_output_models.validate_dataframe(persons, enriched_output_models.PersonModel)

        households = self._add_kids_no_driver(persons, households)
        validated_households = enriched_output_models.validate_dataframe(households, enriched_output_models.HouseholdModel)

        tours = self._prepare_tours_data(households, landuse)
        validated_tours = enriched_output_models.validate_dataframe(tours, enriched_output_models.TourModel)

        trips = self._prepare_trips_data(persons, households)
        validated_trips = enriched_output_models.validate_dataframe(trips, enriched_output_models.TripModel)

        commute_tours = tours[tours['tour_purpose'] == 'Work']
        work_school_locations = self._prepare_work_school_locations_data(tours, landuse)
        validated_work_school_locations = enriched_output_models.validate_dataframe(work_school_locations, enriched_output_models.WorkSchoolLocation)

        self.logger.info("Saving prepared output data to updated_output folder")
        validated_households.to_parquet('updated_output/households.parquet')
        validated_persons.to_parquet('updated_output/persons.parquet')
        validated_trips.to_parquet('updated_output/trips.parquet')
        validated_tours.to_parquet('updated_output/tours.parquet')
        validated_work_school_locations.to_parquet('updated_output/work_school_locations.parquet')
        commute_tours.to_parquet('updated_output/commute_tours.parquet')

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
        """Attach skim (time, dist, cost, bridge toll, value toll) values to trips. Skims are attached at a taz level
        Args:
            scenario (EmmeScenario): Emme Scenario (i.e., TIme Period for Emme)
            time_period (str): Time period name.
            output (pd.DataFrame): DataFrame of CTRAMP trips/tours (Indiv/Joint).
            trip_tour (str): Specify if output dataframe is trip or tours
        """
        self.logger.log(f"Attaching highway skim to trip for time period {time_period}")
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
            self.logger.log(f"Creating indices for trip origins and destinations", level = 'DETAIL')
            self.logger.log("Converting OD index from 1-based to 0-based for matrices lookup", level = 'DETAIL')
            origins = output.loc[mask, 'origin_TAZ_SEQ'].values - 1
            dests = output.loc[mask, 'destination_TAZ_SEQ'].values - 1
            
            # Extract values directly using advanced indexing
            for name, matrix in matrices.items():
                self.logger.log(f'Extracting values for {name}')
                output.loc[mask, name] = matrix[origins, dests]

        # Adjust time for school bus based on TripModeChoice UEC (Time at 20 mph = sov_dist * 3)
        self.logger.log("Adjust time for school bus mode")
        output.loc[output['trip_mode'] == 17, 'auto_time'] = output.loc[output['trip_mode'] == 17, 'auto_dist'] * 3

        # Attaching distance to transit trips
        output = self._attach_dist_skim_to_transit_trip(scenario, time_period, output)
        

        return output
    
    def _attach_highway_skims_to_tour(self, scenario: EmmeScenario, time_period: str, output: pd.DataFrame):
        """Attach skim (time, dist, cost, bridge toll, value toll) values to tours. This will include inbound and outbound skims. Skims are attached at a taz level
        Args:
            scenario (EmmeScenario): Emme Scenario (i.e., TIme Period for Emme)
            time_period (str): Time period name.
            output (pd.DataFrame): DataFrame of CTRAMP trips/tours (Indiv/Joint).
            trip_tour (str): Specify if output dataframe is trip or tours
        """
        self.logger.log(f"Attaching highway skim to tour for time period {time_period}")
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
   
        for mode in modes_to_output:
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
            self.logger.log(f"Filtering tour for mode: {mode} and timeperiod: {time_period}")
            mode_trips = output['tour_mode'].isin(modes_to_output[mode])

            start_period_trips = output['timeperiod_start'] == time_period
            end_period_trips = output['timeperiod_end'] == time_period
            
            mask_out = mode_trips & start_period_trips
            mask_in = mode_trips & end_period_trips
            
            if mask_out.any():
                ## Getting values for outbound tour    
                # Only create indices where needed - converting 1-based index to 0-based index for matrices lookup
                self.logger.log(f"Creating indices for outbound tour origins and destinations", level = 'DETAIL')
                self.logger.log("Converting OD index from 1-based to 0-based for matrices lookup", level = 'DETAIL')
                origins = output.loc[mask_out, 'origin_TAZ_SEQ'].values - 1
                dests = output.loc[mask_out, 'destination_TAZ_SEQ'].values - 1
                
                # Extract values directly using advanced indexing
                for name, matrix in matrices.items():
                    self.logger.log(f'Extracting values for {name} for outbound tour')
                    output.loc[mask_out, f'{name}_out'] = matrix[origins, dests]
            

            ## Getting values for inbound tours
            if mask_in.any():
                
                self.logger.log(f"Creating indices for inbound tour origins and destinations", level = 'DETAIL')
                origins = output.loc[mask_in, 'destination_TAZ_SEQ'].values - 1
                dests = output.loc[mask_in, 'origin_TAZ_SEQ'].values - 1

                # Extract values directly using advanced indexing for inbound trips
                for name, matrix in matrices.items():
                    self.logger.log(f'Extracting values for {name} for inbound tour')
                    output.loc[mask_in, f'{name}_in'] = matrix[origins, dests]

            # Summing outbound and inbound skim variables together
            for name in matrices.keys():
                self.logger.log("Sum outbound and inbound highway skim variables together")
                output[name] = output[f'{name}_out'] + output[f'{name}_in']


        # Adjust time for school bus based on TripModeChoice UEC (Time at 20 mph = sov_dist * 3)
        self.logger.log("Adjust time for school bus mode")
        output.loc[output[f'tour_mode'] == 17, 'auto_time'] = output.loc[output['tour_mode'] == 17, 'auto_dist'] * 3

        # Attach highway distance skim to transit
        output = self._attach_dist_skim_to_transit_tour(scenario, time_period, output)
        
        return output

    def _attach_transit_skims_to_trip(self, scenario: EmmeScenario, time_period: str, output: pd.DataFrame):
        """Attach transit skim values to trips and tours. Skims are attached at a TAZ level
        Args:
            scenario (EmmeScenario): Emme scenario for the time period.
            time_period (str): Time period name.
            output (pd.DataFrame): DataFrame of CTRAMP trips or tours outputs (Indiv/Joint).
            trip_tour (str): Specify if output is trip or tours
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

        period_trips = output['timeperiod'] == time_period

        for mode in transit_modes:
            self.logger.log(f"Reading skims for mode: {mode}")
            matrices = {
                'transit_ivt': emmebank.matrix(f"{time_period}_{mode}_IVT").get_numpy_data(),
                'transit_iwait': emmebank.matrix(f"{time_period}_{mode}_IWAIT").get_numpy_data(),
                'transit_xwait': emmebank.matrix(f"{time_period}_{mode}_XWAIT").get_numpy_data(),
                'transit_fare': emmebank.matrix(f"{time_period}_{mode}_FARE").get_numpy_data(),
                'transit_wacc': emmebank.matrix(f"{time_period}_{mode}_WACC").get_numpy_data(),
                'transit_waux': emmebank.matrix(f"{time_period}_{mode}_WAUX").get_numpy_data(),
                'transit_wegr': emmebank.matrix(f"{time_period}_{mode}_WEGR").get_numpy_data(),
                'transit_dtime': emmebank.matrix(f"{time_period}_{mode}_DTIME").get_numpy_data(),
            }

        # Total Transit Time = IVT + IWAIT + XTRANSFER + WAUX + [WACC/WEGR/DTIME] depending on path taken

            mode_trips = output['trip_mode'].isin(transit_modes[mode])
            
            # Determining whether trip was walk to or walk from transit based on if trip is inbound or outbound
            # If trip is outbound (inbound == 0), then they are driving to transit
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
    
    def _attach_transit_skims_to_tour(self, scenario: EmmeScenario, time_period: str, output: pd.DataFrame):
        """Attach transit skim values to trips and tours. Skims are attached at a TAZ level
        Args:
            scenario (EmmeScenario): Emme scenario for the time period.
            time_period (str): Time period name.
            output (pd.DataFrame): DataFrame of CTRAMP trips outputs (Indiv/Joint).
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
            mask_out = None
            mask_in = None
            self.logger.log(f"Reading skims for mode: {mode}")
            print(f"Reading skims for {mode}")
            matrices = {
                'transit_ivt': emmebank.matrix(f"{time_period}_{mode}_IVT").get_numpy_data(),
                'transit_iwait': emmebank.matrix(f"{time_period}_{mode}_IWAIT").get_numpy_data(),
                'transit_xwait': emmebank.matrix(f"{time_period}_{mode}_XWAIT").get_numpy_data(),
                'transit_fare': emmebank.matrix(f"{time_period}_{mode}_FARE").get_numpy_data(),
                'transit_wacc': emmebank.matrix(f"{time_period}_{mode}_WACC").get_numpy_data(),
                'transit_waux': emmebank.matrix(f"{time_period}_{mode}_WAUX").get_numpy_data(),
                'transit_wegr': emmebank.matrix(f"{time_period}_{mode}_WEGR").get_numpy_data(),
                'transit_dtime': emmebank.matrix(f"{time_period}_{mode}_DTIME").get_numpy_data(),
            }

            # Total Transit Time = IVT + IWAIT + XTRANSFER + WAUX + [WACC/WEGR/DTIME] depending on path taken
            start_period_trips = output['timeperiod_start'] == time_period
            end_period_trips = output['timeperiod_end'] == time_period
            mode_trips = output['tour_mode'].isin(transit_modes[mode])

            # Determining whether trip was walk to or walk from transit based on if trip is inbound or outbound
            # If trip is outbound, then they are driving to transit
            if mode in ['KNR_TRN_WLK', 'PNR_TRN_WLK']:
                mask_out = start_period_trips & mode_trips

            # Trip is inbound (end period), they are walking to transit
            if mode in ['WLK_TRN_KNR', 'WLK_TRN_PNR']:
                mask_in = end_period_trips & mode_trips
           
            if mode in ['WLK_TRN_WLK']:
                mask_out = start_period_trips & mode_trips
                mask_in = end_period_trips & mode_trips

            if mask_out is not None:
                self.logger.log(f"{time_period}: Processing for outbound trips: {mode}")
                origins = output.loc[mask_out, 'origin_TAZ_SEQ'].values - 1
                dests = output.loc[mask_out, 'destination_TAZ_SEQ'].values - 1

                for name, matrix in matrices.items():
                    self.logger.log(f'Extracting values for outbound tour for {name} for mode: {mode} in timeperiod: {time_period}')
                    output.loc[mask_out, f'{name}_out'] = matrix[origins, dests]


            if mask_in is not None:
                self.logger.log(f"{time_period}: Processing for inbound trips: {mode}")
                origins = output.loc[mask_in, 'destination_TAZ_SEQ'].values - 1
                dests = output.loc[mask_in, 'origin_TAZ_SEQ'].values - 1

                for name, matrix in matrices.items():
                    self.logger.log(f'Extracting values for inbound tour for {name} for mode: {mode} in timeperiod: {time_period}')
                    output.loc[mask_in, f'{name}_in'] = matrix[origins, dests]
            
        
        self.logger.log("Sum outbound and inbound tour transit skim variables together")
        for name in matrices.keys():
            output[name] = output[f'{name}_in'] + output[f'{name}_out']

        return output


    def _attach_dist_skim_to_transit_trip(self, scenario: EmmeScenario, time_period: str, output:pd.DataFrame):
        """"
        Getting highway distance skims for transit trips since transit trips do not have distance. Distance is based on drive-alone no toll distance

        """
        # Transit modes do not have a distance so distance is the da distance between the OD
        transit_modes = output['trip_mode'].isin([11,12,13,14])
        period_trips = output['timeperiod'] == time_period
        mask = transit_modes & period_trips
        if not mask.any():
            self.logger.log(f"No transit trips for timeperiod: {time_period}")
            return output
        
        emmebank = scenario.emmebank

        self.logger.log(f"Attaching drive-alone distance for transit trips")
        matrix = emmebank.matrix(f"{time_period}_da_dist").get_numpy_data()
        origins = output.loc[mask, 'origin_TAZ_SEQ'].values - 1
        dests = output.loc[mask, 'destination_TAZ_SEQ'].values - 1 

        output.loc[mask, 'transit_dist'] = matrix[origins, dests]

        return output

    def _attach_dist_skim_to_transit_tour(self, scenario: EmmeScenario, time_period: str, output:pd.DataFrame):
        """
        Getting highway distance skims for transit tours since transit tours do not have distance. Distance is based on drive-alone no toll distance

        """
        # Transit modes do not have a distance so distance is the da distance between the OD
        transit_modes = output['tour_mode'].isin([11,12,13,14])
        start_period_trips = output['timeperiod_start'] == time_period
        end_period_trips = output['timeperiod_end'] == time_period
        mask_out = transit_modes & start_period_trips
        mask_in = transit_modes & end_period_trips

        emmebank = scenario.emmebank
        matrix = emmebank.matrix(f"{time_period}_da_dist").get_numpy_data()

        if mask_out.any():
            self.logger.log(f"Attaching drive-alone distance for outbound transit tours")
            origins = output.loc[mask_out, 'origin_TAZ_SEQ'].values - 1
            dests = output.loc[mask_out, 'destination_TAZ_SEQ'].values - 1 

            output.loc[mask_out, 'transit_dist_out'] = matrix[origins, dests]

        if mask_in.any():
            self.logger.log(f"Attaching drive-alone distance for inbound transit tours")
            origins = output.loc[mask_in, 'origin_TAZ_SEQ'].values - 1
            dests = output.loc[mask_in, 'destination_TAZ_SEQ'].values - 1 

            output.loc[mask_in, 'transit_dist_in'] = matrix[origins, dests]

        output['transit_dist'] = output['transit_dist_out'] + output['transit_dist_in']

        
        return output


    def _attach_nonmotorized_skims_to_trip_tour(self, output: pd.DataFrame, trip_tour: str):
        """Attach nomotorized skims (bike and walk) on MAZ to MAZ level to trips
        Nonmotorized time skims are calculated by dividing skim distance by average bike/ped walking time

        Args:
            output (pd.DataFrame): Trip/Tour Dataframe to join nonmotorized skims
            trip_tour (str): Str to specify if dataframe is trip or tour

        Returns:
            output: Updated Dataframe with nonmotorized skims attached
        """

        # Skims are not saved in the Emmebank and does not rely on time period
        # Read skims directly from skim_matrices and attach based on OD
        skim_files = {
            'walk': next((s['output'] for s in self.controller.config.active_modes.shortest_path_skims
                          if s['mode'] == 'walk'), None),
            'bike': next((s['output'] for s in self.controller.config.active_modes.shortest_path_skims
                          if s['mode'] == 'bike' and s.get('roots') == 'MAZ'), None)
        }

        # Pre-filter output to only relevant OD pairs for each mode
        walk_mask = output[f'{trip_tour}_mode'] == 9
        bike_mask = output[f'{trip_tour}_mode'] == 10

        # Get unique OD pairs needed
        od_cols = ['origin_MAZ_SEQ', 'destination_MAZ_SEQ']
        needed_od_pairs = output.loc[walk_mask | bike_mask, od_cols].drop_duplicates()
          
        # Formatting for the skims are: From MAZ, TO MAZ, To MAZ, Dist, Dist in Feet
        # This is based on: https://github.com/BayAreaMetro/tm2py/blob/master/tm2py/components/network/active/active_modes.py#L338
        
        # Currently MAZs are output as MAZ_SEQ
        dtypes = {0: 'int32', 1: 'int32', 3: 'float32', 4: 'float32'}
        self.logger.log(f"Reading in ped distance skim file: {skim_files['walk']}")
        walk_dist = pd.read_csv(self.get_abs_path(skim_files['walk']), header = None, 
                               names = ['origin_MAZ_SEQ', 'destination_MAZ_SEQ', 'dest1', 
                                        'walk_dist', 'walk_dist_ft'],
                               dtype = dtypes,
                               usecols = [0, 1, 3,4]) # Skip second destination columns
        

        self.logger.log(f"Reading in bike distance skim file: {skim_files['bike']}")
        bike_dist = pd.read_csv(self.get_abs_path(skim_files['bike']), header = None, 
                                names = ['origin_MAZ_SEQ', 'destination_MAZ_SEQ', 'dest1', 
                                         'bike_dist', 'bike_dist_ft'],
                                dtype = dtypes,
                                usecols = [0, 1, 3, 4])

        #Merging distance to needed OD Pair
        walk_dist = walk_dist.merge(needed_od_pairs, on = od_cols, how = 'inner', validate = '1:1')
        bike_dist = bike_dist.merge(needed_od_pairs, on = od_cols, how = 'inner', validate = '1:1')

        # Merge skims to output
        self.logger.log("Attaching skims to trips based on OD columns")
        output = output.merge(walk_dist, on = od_cols, how = 'left', validate = 'm:1')
        output = output.merge(bike_dist, on = od_cols, how = 'left', validate = 'm:1')

        # Set walk columns to NaN if trip is not walk
        output.loc[~walk_mask, ['walk_dist', 'walk_dist_ft']] = np.nan
        output.loc[~bike_mask, ['bike_dist', 'bike_dist_ft']] = np.nan

        if trip_tour == 'tour':
            # To account for inbound and outbound trip, distance is multipled by 2 for total distance traveled during the tour
            output[['walk_dist', 'walk_dist_ft', 'bike_dist', 'bike_dist_ft']] = output[['walk_dist', 'walk_dist_ft', 'bike_dist', 'bike_dist_ft']] * 2

        if output[output[f'{trip_tour}_mode']==9]['walk_dist'].isna().sum() > 0:
            self.logger.log(f"Could not find the OD skim pair for {output[output[f'{trip_tour}_mode']==9]['walk_dist'].isna().sum()} walk {trip_tour}s", level = "WARN")
        if output[output[f'{trip_tour}_mode']==10]['bike_dist'].isna().sum() > 0:
            self.logger.log(f"Could not find the OD skim pair for {output[output[f'{trip_tour}_mode']==10]['bike_dist'].isna().sum()} bike {trip_tour}s", level = "WARN")

        ## Walk and Bike Speed from CTRAMP: https://github.com/BayAreaMetro/travel-model-two/blob/3b765dd96f28c46dea92c77b8113b6fa6685cb57/src/java/com/pb/mtctm2/abm/ctramp/Constants.java#L32
        # In MPH
        self.logger.log("Calculating time based on distance and walk/bike speed")
        walk_speed = 3
        bike_speed = 12
        walk_speed_minpft = (1/walk_speed)*60/5280
        bike_speed_minpft = (1/bike_speed)*60/5280

        output['walk_time'] = output['walk_dist_ft'] * walk_speed_minpft
        output['bike_time'] = output['bike_dist_ft'] * bike_speed_minpft

        return output


    def _add_trip_skim_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add skim columns (time, distance, cost), timeperiod, and TAZ_SEQ/TAZ_Node to the DataFrame. 

        Args:
            df (pd.DataFrame): Dataframe to add skim columns to.
            
        Returns:
            pd.DataFrame: DataFrame with added skim columns.
        """
        self.logger.log("Adding time, distance, cost, and time period columns to trips")

        df = self._add_taz_identifiers(df)
        # Time period for trip
        df['timeperiod'] = pd.cut(df['stop_period'], bins = [1, 4, 12, 22, 30, 40], labels = ['EA', 'AM', 'MD', 'PM', 'EV'], include_lowest= True)

        df[['auto_time', 'auto_dist', 'auto_cost', 'auto_bridge_toll', 'auto_value_toll', 
        'transit_ivt', 'transit_iwait', 'transit_xwait', 'transit_waux', 
        'transit_wacc', 'transit_wegr', 'transit_dtime','transit_fare', 'transit_dist'
        #'walk_time', 'walk_dist', 'walk_dist_ft','bike_time', 'bike_dist'
        ]] = None
        
        return df
    
    def _add_tour_skim_columns(self, df: pd.DataFrame):
        """
        "Add Tour Skim Columns
        """
        df = self._add_taz_identifiers(df)

        df['timeperiod_start'] = pd.cut(df['start_period'], bins = [1, 4, 12, 22, 30, 40], labels = ['EA', 'AM', 'MD', 'PM', 'EV'], include_lowest= True)
        df['timeperiod_end'] = pd.cut(df['end_period'], bins = [1, 4, 12, 22, 30, 40], labels = ['EA', 'AM', 'MD', 'PM', 'EV'], include_lowest= True)

        df[['auto_time', 'auto_dist', 'auto_cost', 'auto_bridge_toll', 'auto_value_toll', 
                'auto_time_out', 'auto_dist_out', 'auto_cost_out', 'auto_bridge_toll_out', 'auto_value_toll_out',
                'auto_time_in', 'auto_dist_in', 'auto_cost_in', 'auto_bridge_toll_in', 'auto_value_toll_in',
                'transit_ivt', 'transit_iwait', 'transit_xwait', 'transit_waux', 
                'transit_wacc', 'transit_wegr', 'transit_dtime','transit_fare', 'transit_dist',
                'transit_ivt_out', 'transit_iwait_out', 'transit_xwait_out', 'transit_waux_out', 
                'transit_wacc_out', 'transit_wegr_out', 'transit_dtime_out','transit_fare_out', 'transit_dist_out',
                'transit_ivt_in', 'transit_iwait_in', 'transit_xwait_in', 'transit_waux_in', 
                'transit_wacc_in', 'transit_wegr_in', 'transit_dtime_in','transit_fare_in', 'transit_dist_in'
           ]] = None  

        return df

    def _add_taz_identifiers(self, df: pd.DataFrame) -> pd.DataFrame:
        """ Add TAZ Sequential/TAZ_NODE to dataframe based on landuse input file """
        landuse_file = 'inputs/landuse/maz_data_withDensity.csv'
        self.logger.log(f"Reading landuse file from {self.get_abs_path(landuse_file)}")
        landuse_input = pd.read_csv(self.get_abs_path(landuse_file))
        
        # Won't need to do this once variable rename is complete
        landuse_input.rename(columns = {"MAZ": 'MAZ_SEQ', 'TAZ': 'TAZ_SEQ', 'MAZ_ORIGINAL': 'MAZ_NODE', 'TAZ_ORIGINAL': 'TAZ_NODE'}, inplace = True)
        self.logger.log(f"MAZ Input: \n{landuse_input.head()}", level = 'DEBUG')

        # Attach origin TAZ and destination TAZ
        self.logger.log("Adding origin TAZ")
        df = df.merge(landuse_input[['MAZ_SEQ', 'TAZ_SEQ', 'MAZ_NODE','TAZ_NODE', 'DistID', 'CountyID']], left_on = 'orig_mgra', right_on = 'MAZ_SEQ', how = 'left', validate= 'm:1')
        df.rename(columns = {'MAZ_SEQ': 'origin_MAZ_SEQ', 'TAZ_SEQ': 'origin_TAZ_SEQ', 
                             'MAZ_NODE': 'origin_MAZ_NODE', 'TAZ_NODE': 'origin_TAZ_NODE',
                             'DistID': 'origin_DistID', 'CountyID': 'origin_CountyID'}, inplace = True)

        self.logger.log("Adding destination TAZ")
        df = df.merge(landuse_input[['MAZ_SEQ', 'TAZ_SEQ', 'MAZ_NODE','TAZ_NODE', 'DistID', 'CountyID']], left_on = 'dest_mgra', right_on = 'MAZ_SEQ', how = 'left', validate= 'm:1')
        df.rename(columns = {'MAZ_SEQ': 'destination_MAZ_SEQ', 'TAZ_SEQ': 'destination_TAZ_SEQ', 
                             'MAZ_NODE': 'destination_MAZ_NODE', 'TAZ_NODE': 'destination_TAZ_NODE',
                             'DistID': 'destination_DistID', 'CountyID': 'destination_CountyID'}, inplace = True)
        self.logger.log(f"Proccessed Trip Data: \n: {df.head()}", level = 'DEBUG')
        
        return df 


    def _sum_time_dist_cost(self, df: pd.DataFrame, trip_tour: str):
        """
        Calculate total time, dist, cost based on the skims variables for trips and tour

        Total Transit Time = IVT + IWAIT + XTRANSFER + WAUX + [WACC/WEGR/DTIME] depending on path taken

        """  

        # Since variables are null if trip/tour mode does not match, sum across every possible variable
        time_column = ['auto_time', 'transit_ivt', 'transit_iwait', 'transit_xwait', 'transit_waux',
                       'transit_wacc', 'transit_wegr', 'transit_dtime', 'walk_time', 'bike_time'
                       ]
        
        dist_column = ['auto_dist', 'walk_dist', 'bike_dist', 'transit_dist']

        cost_column = ['auto_cost', 'transit_fare']
        df[f'{trip_tour}_time'] = df[time_column].sum(axis = 1)
        self.logger.log(f"Calculating {trip_tour} distance")
        df[f'{trip_tour}_dist'] = df[dist_column].sum(axis = 1)
        self.logger.log(f"Calculating {trip_tour} cost - nonmotorized modes do not have cost")
        df[f'{trip_tour}_cost'] = df[cost_column].sum(axis = 1)

        return df
    

    def _prepare_landuse_data(self) -> pd.DataFrame:
        maz_file ='inputs/landuse/maz_data_withDensity.csv'
        landuse = pd.read_csv(self.get_abs_path(maz_file))
        self.logger.info(f"Reading land use data from {maz_file}")

        landuse = landuse.rename(columns = {'MAZ_ORIGINAL':'MAZ_NODE', 'TAZ_ORIGINAL':'TAZ_NODE', 'MAZ': 'MAZ_SEQ', 'TAZ': 'TAZ_SEQ'})
        
        # TODO: Verify the columns we need for the landuse data
        landuse = landuse[['MAZ_SEQ', 'TAZ_SEQ', 'MAZ_NODE', 'TAZ_NODE', 'CountyID', 'DistID', 'hparkcost']]
        self.logger.info(f"Read landuse data; have {len(landuse):,} rows")
        self.logger.debug(landuse.head())
        return landuse

    
    def _prepare_households_data(self, landuse) -> pd.DataFrame:
        popsyn_file = 'inputs/popsyn/households.csv'
        ctramp_hh_file = self._find_highest_iteration('householdData')

        self.logger.log(f"Reading input household data from {self.get_abs_path(popsyn_file)} and output CTRAMP household data from {self.get_abs_path(ctramp_hh_file)}")
        input_pop_hh = pd.read_csv(self.get_abs_path(popsyn_file))
        input_pop_hh.rename(columns={'HHID': 'hh_id', 'MAZ': 'MAZ_SEQ', 'TAZ': 'TAZ_SEQ', 'ORIG_MAZ': 'MAZ_NODE', 'ORIG_TAZ': 'TAZ_NODE', 
                                     'MAZ_ORIGINAL': 'MAZ_NODE', 'TAZ_ORIGINAL': 'TAZ_NODE'}, inplace=True)

        output_ctramp_hh = pd.read_csv(self.get_abs_path(ctramp_hh_file))
        output_ctramp_hh.rename(columns = {'home_mgra': 'HOME_MAZ_SEQ'}, inplace = True)
        self.logger.debug(f"Read {len(input_pop_hh):,} rows from popsyn households and {len(output_ctramp_hh):,} rows from ct households")

        households = input_pop_hh.merge(output_ctramp_hh, on = 'hh_id', how = 'inner', validate = '1:1')
        self.logger.info(f"Joined input and output household files; have {len(households):,} rows")

        # Add taz identifiers 
        # TODO: Need to update taz identifiers so we can determine which columns to merge the maz/tazs on (i.e., home, origin, destination, )
        households = households.merge(landuse, on = ['MAZ_NODE', 'TAZ_NODE', 'MAZ_SEQ', 'TAZ_SEQ'], how = 'left', validate = 'm:1')
        
        households = self._add_household_variables(households)

        return households

    def _add_household_variables(self, households: pd.DataFrame) -> pd.DataFrame:
        """
        Add derived variables to household data. 
        This includes income quartiles and auto sufficiency. Income quartile is based on the config income quartile bucket
        """

        self.logger.info("Adding income quartile to households data")
        self.logger.warn("There are {0} households with negative income".format((households['income'] < 0).sum()))

        income_segment_config = self.controller.config.household.income_segment
        
        households['incQ'] = pd.cut(households['income'],
                                    bins = income_segment_config["cutoffs"] + [float("inf")],
                                    labels = income_segment_config["segment_suffixes"],
                                    include_lowest = True) 

        households['autoSuff'] = np.where(households['autos'] == 0, 0,
                                        np.where(households['autos'] < households['NWRKRS_ESR'], 1, 2))

        households['autoSuff_label'] = households['autoSuff'].map({
            0: 'Zero automobiles',
            1: 'Automobiles < workers',
            2: 'Automobiles >= workers'
        })

        self.logger.info("Added household variables: income quartile, auto sufficiency")
        self.logger.debug(households.head())

        return households
    
    def _add_kids_no_driver(self, persons, households):
        self.logger.info("Adding kidsNoDr variable to households")
        #  No Driver as a binary (1 for kid, 0 for no kid)
        kidsNoDr_hhlds = persons[['hh_id', 'kidsNoDr']].groupby('hh_id', as_index= True ).agg({'kidsNoDr': 'max'})
        households = households.merge(kidsNoDr_hhlds, on = 'hh_id', how = 'left', validate = 'one_to_one')

        return households

    def _prepare_persons_data(self, households) -> pd.DataFrame:
        """Read and combine persons input and output files into a single DataFrame"""
        
        popsyn_persons_file = "inputs/popsyn/persons.csv"
        ctramp_persons_file = self._find_highest_iteration('personData')

        self.logger.info(f"Reading input persons data from {popsyn_persons_file} and output persons data {ctramp_persons_file}")
        input_pop_persons = pd.read_csv(self.get_abs_path(popsyn_persons_file))
        output_ctramp_persons = pd.read_csv(self.get_abs_path(ctramp_persons_file))

        input_pop_persons.rename(columns = {'HHID': 'hh_id', 'PERID': 'person_id'}, inplace = True)
        self.logger.info(f"Read {len(input_pop_persons):,} rows from input persons data and {len(output_ctramp_persons):,} rows from output persons data")

        persons = input_pop_persons.merge(output_ctramp_persons, on = ['hh_id', 'person_id'], how = 'inner', validate = 'one_to_one')
        self.logger.info(f"Joined input and output persons; have {len(persons):,} rows")
        
        self.logger.info("Adding household attributes to persons data")

        self.logger.debug(persons.head())

        # Add household attributes like income quartile, household size, auto ownership, home MAZ from households
        self.logger.info("Adding household attributes to persons data")
        persons = persons.merge(households[['hh_id', 'incQ', 'size', 'autos', 'MAZ_NODE', 'TAZ_NODE', 'MTCCountyID']], on = 'hh_id', how = 'left', validate = 'm:1')
        self.logger.debug(persons.head())

        # Add kids no driver indicator
        self.logger.info("Adding kidsNoDr dummy variable to persons data")
        persons['kidsNoDr'] = np.where(persons['type'].isin(['Child too young for school', 'Non-driving-age student']), 1, 0)

        self.logger.info(f"Finished reading persons files; final file has {len(persons):,} rows")
        self.logger.debug(persons.head())

        return persons


    def _prepare_tours_data(self, households: pd.DataFrame, landuse: pd.DataFrame) -> pd.DataFrame:
        """Combine joint and individual tours into a single DataFrame
        
        """

        self.logger.info("Reading and combining tours")
        joint_tours = self._read_tours("Joint")
        indiv_tours = self._read_tours("Indiv")

        joint_tours = joint_tours.drop(columns = ['tour_composition'])
        indiv_tours = indiv_tours.drop(columns = ['person_type', 'atWork_freq'])

        self.logger.info(f"Combining {len(indiv_tours):,} rows from Indiv Tours and {len(joint_tours):,} rows from Joint Tours")
        tours = pd.concat([indiv_tours, joint_tours], ignore_index= True)
        self.logger.debug(tours.head())

        # Add Household Info to tours
        self.logger.info("Adding household information to tours")
        tours = tours.merge(households[['hh_id', 'incQ']], on = 'hh_id', how = 'left', validate='m:1')
        self.logger.debug(tours.head())

        # TODO: This should be part of the enums function - that change has not been incorporate yet;
        # TODO: For now, this will be hard-coded until enums is incorporate and config file is updated
        
 
        tours['tour_mode_label'] = tours['tour_mode'].map(self._mode_label)

        self.logger.info(f"Combined tours; have {len(tours):,} rows")
        self.logger.debug(tours.head())

        return tours

    def _prepare_trips_data(self, persons: pd.DataFrame, households: pd.DataFrame) -> pd.DataFrame:
        """Prepares trips data by combining individual trips and joint trips together.
        
        Joint trips will be unwind so each person will have a trip. 
        Household attributes will also be merged with the trips data 
        """
        self.logger.info("Reading and combining tours")
        indiv_trip = self._read_trips('Indiv')
        joint_trip = self._read_trips('Joint')
        joint_person_trips = self._get_joint_persons_trips(joint_trip, persons)

        trips = pd.concat([indiv_trip, joint_person_trips], ignore_index=True)
        
        trips['trip_mode_label'] = trips['trip_mode'].map(self._mode_label)

        # Add household attributes
        trips = trips.merge(households[['hh_id', 'incQ', 'autoSuff', 'autoSuff_label']], 
                            on = 'hh_id', validate = 'm:1')
        
        trips.rename(columns = {'parking_mgra': 'parking_MAZ_SEQ'}, inplace = True)
        self.logger.info(f"Combined {len(indiv_trip):,} individual trips with {len(joint_person_trips):,} joint person trips to make {len(trips):,} rows")
        self.logger.debug(trips.head())

        return trips

    def _read_tours(self, IndivJoint: str) -> pd.DataFrame:
        """Read output tour data with skims attached
        
        Args:
            IndivJoint (str): String ('Indiv' or 'Joint') to specify tour type
        """
    
        tour_file = f"updated_output/{IndivJoint}TourData_{self._iteration_num}.parquet"
        tour = pd.read_parquet(self.get_abs_path(tour_file))
        
        tour.drop(list(tour.filter(regex='util|prob')), axis = 1, inplace = True)

        if IndivJoint == 'Indiv':
            tour['num_participants'] = 1
            tour['tour_participants'] = tour['person_num'].astype(str)
        else:
            tour['num_participants'] = (tour['tour_participants'].str.split(' '))
            tour['num_participants'] = tour['num_participants'].str.len()
            tour['person_id'] = 0
            tour['person_num'] = 0
        
        self.logger.info(f"Read {len(tour):,} rows from {tour_file}")
        self.logger.debug(tour.head())

        return tour

    
    def _read_trips(self, IndivJoint: str) -> pd.DataFrame:
        """Read output trip data with skims attached
        
        Args:
            IndivJoint (str): String ('Indiv' or 'Joint') to specify trip type
        """
        trip_file = f"updated_output/{IndivJoint}TripData_{self._iteration_num}.parquet"
        trip = pd.read_parquet(trip_file)

        if IndivJoint == 'Indiv':
            trip['num_participants'] = 1
            trip['tour_participants'] = trip['person_num'].astype(str)
        
        self.logger.info(f"Read {len(trip):,} rows from {trip_file}")
        self.logger.debug(trip.head())

        return trip

    def _get_joint_persons_trips(self, joint_trips: pd.DataFrame, persons: pd.DataFrame) -> pd.DataFrame:
        """Get persons associated with each tour and trip."""
        
        # Unwind the participants for joint tours and make each person their own row
        joint_tour_file = f"updated_output/JointTourData_{self._iteration_num}.parquet"
        participants = pd.read_parquet(self.get_abs_path(joint_tour_file))
        participants = participants[['hh_id', 'tour_id', 'tour_participants']]
        participants['person_num'] = participants['tour_participants'].str.split(' ')
        participants = participants.explode('person_num')
        participants['person_num'] = participants['person_num'].astype(int)

        ## Join on household and person num to get person_id
        joint_tour_persons = pd.merge(participants, persons[['hh_id', 'person_num', 'person_id']], on=['hh_id', 'person_num'], how='left', validate = 'many_to_one')

        self.logger.info(f"Combined joint tours and persons; have {len(joint_tour_persons):,} rows")
        self.logger.debug(joint_tour_persons.head())

        self.logger.info("Attaching person to joint trips")
        # This is a many to many inner join since we are unwinding joint trips by persons on the trip. Each joint trip becomes a row per participant
        joint_persons_trips = pd.merge(joint_trips, joint_tour_persons, on= ['hh_id', 'tour_id'], how = 'inner', indicator= True, validate = 'many_to_many')

        self.logger.debug(('Created joint_person_trips with {0} rows from {1} rows from joint trips {2} rows from joint_tour_persons')
              .format(len(joint_persons_trips), len(joint_trips), len(joint_tour_persons))
              )

        return joint_persons_trips 

    def _prepare_work_school_locations_data(self, tours: pd.DataFrame, landuse: pd.DataFrame) -> pd.DataFrame:
        """Read and prepare the work location

        Args:
            landuse: Prepared landuse dataframe
            tours: Prepared tours dataframe
        """
        wsLoc_file = f'ctramp_output/wsLocResults_{self._iteration_num}.csv'
        ws_location = pd.read_csv(self.get_abs_path(wsLoc_file))
        self.logger.info(f"Reading work-school location data from {wsLoc_file}; hoave {len(ws_location):,} rows")

        ws_location.rename(columns={'HHID': 'hh_id', 'HomeMGRA': 'HOME_MAZ_SEQ'}, inplace=True)
        # Add home county
        self.logger.info("Adding home county to work school location data")
        ws_location = ws_location.merge(landuse.rename(columns = {'MAZ_SEQ':'HOME_MAZ_SEQ', 'MAZ_NODE': 'HOME_MAZ_NODE',
                                                                    'TAZ_NODE': 'HOME_TAZ_NODE','DistID':'HOME_DistID','CountyID': 'HOME_CountyID'})
                                                                    [['HOME_MAZ_SEQ', 'HOME_MAZ_NODE', 'HOME_TAZ_NODE','HOME_DistID', 'HOME_CountyID']], 
                                                                    on = 'HOME_MAZ_SEQ', how = 'left', validate = 'm:1')
        self.logger.debug(ws_location.head())

        # Add work county
        ## WFH tours do not have a work county 
        self.logger.info("Adding work county to work school location data")
        ws_location = ws_location.merge(landuse.rename(columns = {'MAZ_SEQ':'WORK_MAZ_SEQ', 'MAZ_NODE': 'WORK_MAZ_NODE',
                                                                    'TAZ_NODE': 'WORK_TAZ_NODE','DistID':'WORK_DistID','CountyID': 'WORK_CountyID'})
                                                                    [['WORK_MAZ_SEQ', 'WORK_MAZ_NODE', 'WORK_TAZ_NODE', 'WORK_DistID', 'WORK_CountyID']],
                                                                      left_on = 'WorkLocation', right_on = 'WORK_MAZ_SEQ',  how = 'left', validate = 'm:1')
        self.logger.debug(ws_location.head())
        
        # Add school county
        self.logger.info("Adding school county to work school location data")
        ws_location = ws_location.merge(landuse.rename(columns = {'MAZ_SEQ':'SCH_MAZ_SEQ', 'MAZ_NODE': 'SCH_MAZ_NODE',
                                                                    'TAZ_NODE': 'SCH_TAZ_NODE','DistID':'SCH_DistID','CountyID': 'SCH_CountyID'})
                                                                    [['SCH_MAZ_SEQ', 'SCH_MAZ_NODE', 'SCH_TAZ_NODE', 'SCH_DistID', 'SCH_CountyID']],
                                                                      left_on = 'SchoolLocation', right_on = 'SCH_MAZ_SEQ',  how = 'left', validate = 'm:1')
        self.logger.debug(ws_location.head())
        
        # Adding WFH variable - these will not have a work county or taz
        self.logger.info("Adding WFH variable to work school location data")
        ws_location['WFH'] = np.where(ws_location['WorkSegment'] == 99999, 1, 0)

        # Filtering tours to only mandatory tours 
        self.logger.info("Filtering tours to mandatory tours (work and school)")
        commute_tours = tours[tours['tour_purpose'].isin(['Work', 'School'])]
        commute_tours = commute_tours[['hh_id', 'tour_participants', 'person_id','tour_purpose','tour_mode']]
        commute_tours['person_num'] = commute_tours['tour_participants'].astype(int)
        self.logger.debug(f"Commute tours: \n{commute_tours.head()}")

        #Merging tour mode from commute tour 
        self.logger.info("Adding tour mode to work school location data")
        ws_location = ws_location.merge(right = commute_tours[['person_num', 'person_id', 'tour_mode']], how = 'left',
                                            left_on = 'PersonID', right_on = 'person_id', validate= 'one_to_many')
        self.logger.debug(ws_location.head())
        
        self.logger.warn(f"There are {ws_location[(ws_location['WorkLocation'] != 0 )| (ws_location['SchoolLocation'] != 0)]['tour_mode'].isnull().sum()} persons with a work or school location but no associated tour mode")
        # Fill in missing values for all merges 
        ws_location.fillna(0, inplace = True)

        return ws_location
        
