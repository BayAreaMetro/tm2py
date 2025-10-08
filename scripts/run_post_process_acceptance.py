#!/usr/bin/env python3
"""
TM2PY Acceptance Criteria Post-Processing Script

This script extracts acceptance criteria data from EMME scenarios for use with
the tm2py-utils acceptance criteria validation framework.

Outputs:
    - simulated_roadway_assignment_results.csv: Highway volumes by time period
    - simulated_boardings.csv: Transit boardings by route and time period
    - transit_boardings_by_segment.csv: Detailed segment-level transit data
    - transit_boardings_by_segment.geojson: Spatial transit boarding data
    - network shapefiles: EMME network exports (if Modeller available)
    - network_analysis.txt: Network attribute analysis report

Usage:
    python run_post_process_acceptance.py

Requirements:
    - EMME model results at E:\2015-tm22-dev-sprint-04
    - tm2pyenv-acceptance Python environment
    - EMME API access via inro.emme modules
"""

import os
import sys
import logging
import json
from pathlib import Path
from typing import List, Dict, Optional, Tuple
import pandas as pd

# Add tm2py to path
sys.path.insert(0, r'c:\GitHub\tm2py')


class AcceptanceCriteriaProcessor:
    """Main processor for TM2PY acceptance criteria data extraction."""
    
    def __init__(self, model_run_dir: str, output_dir: Optional[str] = None):
        """
        Initialize the acceptance criteria processor.
        
        Args:
            model_run_dir: Path to the model run directory
            output_dir: Optional output directory (defaults to model_run_dir/acceptance_criteria)
        """
        self.model_run_dir = Path(model_run_dir)
        self.output_dir = Path(output_dir) if output_dir else self.model_run_dir / "acceptance_criteria"
        
        # Set up logging
        self._setup_logging()
        
        # Initialize database connections
        self.transit_bank = None
        self.highway_bank = None
        
    def _setup_logging(self) -> None:
        """Configure logging for the processor."""
        self.output_dir.mkdir(exist_ok=True)
        
        log_file = self.output_dir / "acceptance_processing.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        
        self.logger = logging.getLogger(__name__)
        
    def _connect_to_emme_databases(self) -> bool:
        """
        Connect to EMME transit and highway databases.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            from inro.emme.database.emmebank import Emmebank
            
            transit_db_path = self.model_run_dir / "emme_project" / "Database_transit" / "emmebank"
            highway_db_path = self.model_run_dir / "emme_project" / "Database_highway" / "emmebank"
            
            self.transit_bank = Emmebank(str(transit_db_path))
            self.highway_bank = Emmebank(str(highway_db_path))
            
            self.logger.info("Successfully connected to EMME databases")
            return True
            
        except ImportError as e:
            self.logger.error(f"EMME modules not available: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Failed to connect to EMME databases: {e}")
            return False
    
    def run(self) -> bool:
        """
        Execute the complete acceptance criteria processing workflow.
        
        Returns:
            True if successful, False otherwise
        """
        self.logger.info("Starting TM2PY acceptance criteria post-processing")
        self.logger.info(f"Model directory: {self.model_run_dir}")
        self.logger.info(f"Output directory: {self.output_dir}")
        
        if not self.model_run_dir.exists():
            self.logger.error(f"Model directory does not exist: {self.model_run_dir}")
            return False
        
        if not self._connect_to_emme_databases():
            return False
        
        try:
            # Generate network analysis report
            self._analyze_network_attributes()
            
            # Extract acceptance criteria data
            transit_data = self._extract_transit_data()
            highway_data = self._extract_highway_data()
            
            # Save main acceptance criteria files
            self._save_acceptance_data(transit_data, highway_data)
            
            # Generate additional outputs
            self._export_detailed_transit_csv()
            self._export_transit_geojson()
            self._export_highway_geojson()
            self._export_network_shapefiles()
            
            self.logger.info("Acceptance criteria post-processing completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Processing failed: {e}", exc_info=True)
            return False
    
    def _analyze_network_attributes(self) -> None:
        """Generate network attribute analysis report."""
        self.logger.info("Analyzing network attributes")
        
        try:
            analyzer = NetworkAnalyzer(self.transit_bank)
            analysis_file = self.output_dir / "network_analysis.txt"
            analyzer.generate_report(analysis_file)
            self.logger.info(f"Network analysis saved: {analysis_file}")
            
        except Exception as e:
            self.logger.warning(f"Network analysis failed: {e}")
    
    def _extract_transit_data(self) -> pd.DataFrame:
        """Extract transit boarding data for acceptance criteria."""
        self.logger.info("Extracting transit boarding data")
        
        extractor = TransitDataExtractor(self.transit_bank)
        return extractor.extract_acceptance_data()
    
    def _extract_highway_data(self) -> pd.DataFrame:
        """Extract highway assignment data for acceptance criteria."""
        self.logger.info("Extracting highway assignment data")
        
        extractor = HighwayDataExtractor(self.highway_bank)
        return extractor.extract_acceptance_data()
    
    def _save_acceptance_data(self, transit_data: pd.DataFrame, highway_data: pd.DataFrame) -> None:
        """Save main acceptance criteria CSV files."""
        transit_file = self.output_dir / "simulated_boardings.csv"
        highway_file = self.output_dir / "simulated_roadway_assignment_results.csv"
        
        transit_data.to_csv(transit_file, index=False)
        highway_data.to_csv(highway_file, index=False)
        
        self.logger.info(f"Transit data saved: {transit_file} ({len(transit_data)} records)")
        self.logger.info(f"Highway data saved: {highway_file} ({len(highway_data)} records)")
    
    def _export_detailed_transit_csv(self) -> None:
        """Export detailed transit segment CSV."""
        self.logger.info("Exporting detailed transit segment CSV")
        
        try:
            exporter = TransitSegmentExporter(self.transit_bank)
            output_file = self.output_dir / "transit_boardings_by_segment.csv"
            line_count, segment_count = exporter.export_csv(output_file)
            self.logger.info(f"Transit CSV exported: {line_count} lines, {segment_count} segments")
            
        except Exception as e:
            self.logger.warning(f"Transit CSV export failed: {e}")
    
    def _export_transit_geojson(self) -> None:
        """Export transit boarding GeoJSON with geometry."""
        self.logger.info("Exporting transit boarding GeoJSON")
        
        try:
            exporter = TransitGeoJSONExporter(self.transit_bank)
            output_file = self.output_dir / "transit_boardings_by_segment.geojson"
            feature_count = exporter.export_geojson(output_file)
            self.logger.info(f"Transit GeoJSON exported: {feature_count} features")
            
        except Exception as e:
            self.logger.warning(f"Transit GeoJSON export failed: {e}")
    
    def _export_highway_geojson(self) -> None:
        """Export highway assignment GeoJSON with geometry."""
        self.logger.info("Exporting highway assignment GeoJSON")
        
        try:
            exporter = HighwayGeoJSONExporter(self.highway_bank)
            output_file = self.output_dir / "highway_assignment_by_link.geojson"
            feature_count = exporter.export_geojson(output_file)
            self.logger.info(f"Highway GeoJSON exported: {feature_count} features")
            
        except Exception as e:
            self.logger.warning(f"Highway GeoJSON export failed: {e}")
    
    def _export_network_shapefiles(self) -> None:
        """Export network shapefiles using EMME Modeller."""
        self.logger.info("Exporting network shapefiles")
        
        try:
            exporter = NetworkShapefileExporter(self.transit_bank, self.highway_bank)
            exported_files = exporter.export_shapefiles(self.output_dir)
            self.logger.info(f"Network shapefiles exported: {len(exported_files)} files")
            
        except Exception as e:
            self.logger.warning(f"Network shapefile export failed: {e}")


class NetworkAnalyzer:
    """Analyzes EMME network attributes and generates reports."""
    
    def __init__(self, transit_bank):
        self.transit_bank = transit_bank
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
    
    def generate_report(self, output_file: Path) -> None:
        """Generate comprehensive network analysis report."""
        scenarios = list(self.transit_bank.scenarios())
        if not scenarios:
            raise ValueError("No transit scenarios found")
        
        # Use first non-placeholder scenario
        scenario_id = scenarios[0] if scenarios[0] != 1 else scenarios[1] if len(scenarios) > 1 else scenarios[0]
        scenario = self.transit_bank.scenario(scenario_id)
        network = scenario.get_network()
        
        with open(output_file, 'w') as f:
            self._write_report_header(f, scenario)
            self._write_scenario_analysis(f, scenario)
            self._write_network_statistics(f, network)
            self._write_sample_attributes(f, network)
    
    def _write_report_header(self, f, scenario) -> None:
        """Write report header."""
        f.write("TM2PY Network Analysis Report\n")
        f.write("=" * 50 + "\n\n")
        f.write(f"Generated: {pd.Timestamp.now()}\n")
        f.write(f"Scenario: {scenario.id} - {scenario.title}\n\n")
    
    def _write_scenario_analysis(self, f, scenario) -> None:
        """Write scenario-level analysis."""
        f.write("Scenario Analysis:\n")
        f.write("-" * 20 + "\n")
        f.write(f"Element totals: {scenario.element_totals}\n")
        
        if hasattr(scenario, 'has_traffic_results'):
            f.write(f"Has traffic results: {scenario.has_traffic_results}\n")
        
        f.write("\n")
    
    def _write_network_statistics(self, f, network) -> None:
        """Write network-level statistics."""
        f.write("Network Statistics:\n")
        f.write("-" * 20 + "\n")
        
        try:
            attributes = network.get_attribute_values()
            f.write(f"Available attributes: {attributes.names}\n")
        except Exception as e:
            f.write(f"Could not retrieve attributes: {e}\n")
        
        # Count network elements
        transit_lines = list(network.transit_lines())
        links = list(network.links())
        
        f.write(f"Transit lines: {len(transit_lines)}\n")
        f.write(f"Network links: {len(links)}\n\n")
    
    def _write_sample_attributes(self, f, network) -> None:
        """Write sample of network element attributes."""
        f.write("Sample Transit Line Attributes:\n")
        f.write("-" * 30 + "\n")
        
        for i, line in enumerate(network.transit_lines()):
            if i >= 2:  # Limit to first 2 lines
                break
                
            f.write(f"\nLine {line.id}:\n")
            f.write(f"  Headway: {line.headway}\n")
            f.write(f"  Vehicle capacity: {line.vehicle.total_capacity}\n")
            
            # Sample segments
            for j, segment in enumerate(line.segments(include_hidden=False)):
                if j >= 3:  # Limit to first 3 segments
                    break
                    
                f.write(f"  Segment {j+1}: {segment.i_node} -> {segment.j_node}\n")
                
                # Try different volume attributes
                volume = 0
                if hasattr(segment, 'transit_volume'):
                    volume = segment.transit_volume
                elif hasattr(segment, 'volume'):
                    volume = segment.volume
                elif hasattr(segment, 'volau'):
                    volume = segment.volau
                elif hasattr(segment, 'data1'):
                    volume = segment.data1
                elif hasattr(segment, 'data2'):
                    volume = segment.data2
                    
                f.write(f"    Volume: {volume}\n")
                f.write(f"    Length: {segment.link.length:.3f}\n")


class TransitDataExtractor:
    """Extracts transit data for acceptance criteria validation."""
    
    def __init__(self, transit_bank):
        self.transit_bank = transit_bank
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
    
    def _get_segment_volume(self, segment):
        """Get transit volume from segment, trying different attribute names."""
        volume_attrs = ['transit_volume', 'volume', 'volau', 'data1', 'data2', 'transit_boardings']
        
        for attr in volume_attrs:
            if hasattr(segment, attr):
                value = getattr(segment, attr)
                if value is not None and value != 0:
                    return value
        
        return 0
    
    def extract_acceptance_data(self) -> pd.DataFrame:
        """Extract transit data in tm2py-utils acceptance format."""
        all_data = []
        scenarios = list(self.transit_bank.scenarios())
        
        for scenario_id in scenarios:
            if scenario_id == 1:  # Skip placeholder scenario
                continue
                
            scenario = self.transit_bank.scenario(scenario_id)
            time_period = self._map_scenario_to_time_period(scenario)
            
            self.logger.info(f"Processing transit scenario {scenario_id}: {scenario.title} -> {time_period}")
            
            try:
                network = scenario.get_network()
                scenario_data = self._extract_scenario_data(network, time_period)
                all_data.extend(scenario_data)
                
            except Exception as e:
                self.logger.warning(f"Failed to process scenario {scenario_id}: {e}")
        
        return pd.DataFrame(all_data)
    
    def _map_scenario_to_time_period(self, scenario) -> str:
        """Map scenario title to standard time period."""
        title = scenario.title.lower() if scenario.title else ""
        
        if 'am' in title or 'morning' in title:
            return 'am'
        elif 'pm' in title or 'afternoon' in title:
            return 'pm'
        elif 'md' in title or 'midday' in title:
            return 'md'
        elif 'ea' in title or 'early' in title:
            return 'ea'
        elif 'ev' in title or 'night' in title:
            return 'ev'
        else:
            return 'unknown'
    
    def _extract_scenario_data(self, network, time_period: str) -> List[Dict]:
        """Extract data for a single scenario."""
        data = []
        
        for line in network.transit_lines():
            # Calculate capacity
            total_capacity = line.vehicle.total_capacity
            hdw = line.headway
            line_hour_total_cap = 60 * total_capacity / hdw if hdw > 0 else 0
            
            # Sum boarding volumes across segments
            total_boarding = sum(self._get_segment_volume(segment) for segment in line.segments(include_hidden=False))
            
            # Classify line
            line_mode, operator = self._classify_transit_line(line)
            
            data.append({
                'line_name': f"{line.id}_{time_period}",
                'daily_line_name': line.id,
                'tm2_mode': getattr(line.mode, 'id', 'b') if hasattr(line, 'mode') and line.mode else 'b',
                'line_mode': line_mode,
                'operator': operator,
                'technology': line_mode,
                'fare_system': operator,
                'time_period': time_period,
                'total_boarding': total_boarding,
                'total_hour_cap': line_hour_total_cap
            })
        
        return data
    
    def _classify_transit_line(self, line) -> Tuple[str, str]:
        """Classify transit line by mode and operator."""
        # Default values
        line_mode = 'Local Bus'
        operator = 'Unknown'
        
        # Mode classification
        if hasattr(line, 'mode') and line.mode:
            mode_id = line.mode.id.lower()
            mode_mapping = {
                'l': 'Light Rail',
                'h': 'Heavy Rail', 
                'r': 'Commuter Rail',
                'f': 'Ferry',
                'e': 'Express Bus'
            }
            line_mode = mode_mapping.get(mode_id, 'Local Bus')
        
        # Operator classification from line name
        line_name = line.id.upper()
        if 'MUNI' in line_name or 'SF' in line_name:
            operator = 'San Francisco Municipal Transportation Agency'
        elif 'AC' in line_name:
            operator = 'AC Transit'
        elif 'SAMTRANS' in line_name:
            operator = 'SamTrans'
        elif 'VTA' in line_name:
            operator = 'Santa Clara Valley Transportation Authority'
        elif 'BART' in line_name:
            operator = 'Bay Area Rapid Transit'
        elif 'CALTRAIN' in line_name:
            operator = 'Peninsula Corridor Joint Powers Board'
        
        return line_mode, operator


class HighwayDataExtractor:
    """Extracts highway data for acceptance criteria validation."""
    
    def __init__(self, highway_bank):
        self.highway_bank = highway_bank
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
    
    def extract_acceptance_data(self) -> pd.DataFrame:
        """Extract highway data in tm2py-utils acceptance format."""
        all_data = []
        scenarios = list(self.highway_bank.scenarios())
        
        for scenario_id in scenarios:
            scenario = self.highway_bank.scenario(scenario_id)
            time_period = self._map_scenario_to_time_period(scenario)
            
            self.logger.info(f"Processing highway scenario {scenario_id}: {scenario.title} -> {time_period}")
            
            try:
                network = scenario.get_network()
                scenario_data = self._extract_scenario_data(network, time_period)
                all_data.extend(scenario_data)
                
            except Exception as e:
                self.logger.warning(f"Failed to process scenario {scenario_id}: {e}")
        
        return pd.DataFrame(all_data)
    
    def _map_scenario_to_time_period(self, scenario) -> str:
        """Map scenario title to standard time period."""
        title = scenario.title.lower() if scenario.title else ""
        
        if 'am' in title:
            return 'am'
        elif 'pm' in title:
            return 'pm'
        elif 'md' in title:
            return 'md'
        elif 'ea' in title:
            return 'ea'
        elif 'ev' in title:
            return 'ev'
        else:
            return 'unknown'
    
    def _extract_scenario_data(self, network, time_period: str) -> List[Dict]:
        """Extract data for a single highway scenario."""
        data = []
        
        for link in network.links():
            # Get volume (simplified - could be enhanced with proper attribute mapping)
            volume = getattr(link, 'auto_volume', 0) or getattr(link, 'volume', 0)
            
            # Safely convert node IDs to integers
            try:
                i_node_id = int(link.i_node.id)
                j_node_id = int(link.j_node.id)
                standard_link_id = int(f"{i_node_id}{j_node_id:06d}")
            except (ValueError, TypeError):
                # Handle cases where node IDs are not numeric
                i_node_id = 0
                j_node_id = 0
                standard_link_id = 0
            
            data.append({
                'emme_a_node_id': i_node_id,
                'emme_b_node_id': j_node_id,
                'standard_link_id': standard_link_id,
                'time_period': time_period,
                'capacity': getattr(link, 'capacity', 0),
                'lanes': getattr(link, 'num_lanes', 0),
                'speed_mph': 0,  # Could be calculated if needed
                'ft': getattr(link, 'type', 0),
                'distance_in_miles': link.length,
                'flow_da': volume,  # Simplified - all volume in drive alone
                'flow_s2': 0,
                'flow_s3': 0,
                'flow_lrgt': 0,
                'flow_trk': 0,
                'flow_total': volume,
                'm_flow_da': 0,
                'm_flow_s2': 0,
                'm_flow_s3': 0,
                'm_flow_lrgt': 0,
                'm_flow_trk': 0
            })
        
        return data


class TransitSegmentExporter:
    """Exports detailed transit segment data to CSV."""
    
    def __init__(self, transit_bank):
        self.transit_bank = transit_bank
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
    
    def _get_segment_volume(self, segment):
        """Get transit volume from segment, trying different attribute names."""
        volume_attrs = ['transit_volume', 'volume', 'volau', 'data1', 'data2', 'transit_boardings']
        
        for attr in volume_attrs:
            if hasattr(segment, attr):
                value = getattr(segment, attr)
                if value is not None and value != 0:
                    return value
        
        return 0
    
    def export_csv(self, output_file: Path) -> Tuple[int, int]:
        """Export transit segment CSV and return counts."""
        scenarios = list(self.transit_bank.scenarios())
        if not scenarios:
            raise ValueError("No transit scenarios found")
        
        # Use first non-placeholder scenario
        scenario_id = scenarios[0] if scenarios[0] != 1 else scenarios[1] if len(scenarios) > 1 else scenarios[0]
        scenario = self.transit_bank.scenario(scenario_id)
        network = scenario.get_network()
        
        line_count = 0
        segment_count = 0
        
        with open(output_file, 'w') as f:
            # Write header
            f.write("Line,From,To,Length,Dwt,capt,TTF,voltr,caps,Data1,Data2,Data3\n")
            
            for line in network.transit_lines():
                line_count += 1
                
                # Calculate capacities
                total_capacity = line.vehicle.total_capacity
                seated_capacity = line.vehicle.seated_capacity
                hdw = line.headway
                line_hour_total_cap = 60 * total_capacity / hdw if hdw > 0 else 0
                line_hour_seated_cap = 60 * seated_capacity / hdw if hdw > 0 else 0
                
                for segment in line.segments(include_hidden=False):
                    segment_count += 1
                    
                    # Write segment data
                    row = [
                        str(segment.line.id),
                        str(segment.i_node),
                        str(segment.j_node),
                        str(segment.link.length),
                        str(segment.dwell_time),
                        str(line_hour_total_cap),
                        str(segment.transit_time_func),
                        str(self._get_segment_volume(segment)),
                        str(line_hour_seated_cap),
                        str(segment.data1),
                        str(segment.data2),
                        str(segment.data3)
                    ]
                    f.write(",".join(row) + "\n")
        
        return line_count, segment_count


class TransitGeoJSONExporter:
    """Exports transit boarding data as GeoJSON with geometry."""
    
    def __init__(self, transit_bank):
        self.transit_bank = transit_bank
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
    
    def _get_segment_volume(self, segment):
        """Get transit volume from segment, trying different attribute names."""
        volume_attrs = ['transit_volume', 'volume', 'volau', 'data1', 'data2', 'transit_boardings']
        
        for attr in volume_attrs:
            if hasattr(segment, attr):
                value = getattr(segment, attr)
                if value is not None and value != 0:
                    return value
        
        return 0
    
    def export_geojson(self, output_file: Path) -> int:
        """Export transit GeoJSON and return feature count."""
        try:
            from shapely.geometry import mapping, LineString
        except ImportError:
            raise ImportError("Shapely not available for GeoJSON export")
        
        scenarios = list(self.transit_bank.scenarios())
        if not scenarios:
            raise ValueError("No transit scenarios found")
        
        # Use first non-placeholder scenario
        scenario_id = scenarios[0] if scenarios[0] != 1 else scenarios[1] if len(scenarios) > 1 else scenarios[0]
        scenario = self.transit_bank.scenario(scenario_id)
        network = scenario.get_network()
        
        features = []
        
        for line in network.transit_lines():
            # Calculate capacities
            total_capacity = line.vehicle.total_capacity
            seated_capacity = line.vehicle.seated_capacity
            hdw = line.headway
            line_hour_total_cap = 60 * total_capacity / hdw if hdw > 0 else 0
            line_hour_seated_cap = 60 * seated_capacity / hdw if hdw > 0 else 0
            
            for segment in line.segments(include_hidden=False):
                try:
                    geometry = mapping(LineString(segment.link.shape))
                    feature = {
                        "type": "Feature",
                        "geometry": geometry,
                        "properties": {
                            "LINE_ID": segment.line.id,
                            "INODE": int(segment.i_node.id),
                            "JNODE": int(segment.j_node.id),
                            "VOLTR": self._get_segment_volume(segment),
                            "caps": line_hour_seated_cap,
                            "capt": line_hour_total_cap
                        }
                    }
                    features.append(feature)
                except Exception:
                    # Skip segments with geometry issues
                    continue
        
        geojson_data = {
            "type": "FeatureCollection",
            "crs": {
                "type": "name",
                "properties": {"name": "urn:ogc:def:crs:EPSG::2875"},
            },
            "features": features
        }
        
        with open(output_file, "w") as f:
            json.dump(geojson_data, f, indent=2)
        
        return len(features)


class HighwayGeoJSONExporter:
    """Exports highway assignment data as GeoJSON with geometry."""
    
    def __init__(self, highway_bank):
        self.highway_bank = highway_bank
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
    
    def _get_link_volume(self, link):
        """Get highway volume from link, trying different attribute names."""
        volume_attrs = ['auto_volume', 'volume', 'volau', 'data1', 'data2', 'flow_total']
        
        for attr in volume_attrs:
            if hasattr(link, attr):
                value = getattr(link, attr)
                if value is not None and value != 0:
                    return value
        
        return 0
    
    def export_geojson(self, output_file: Path) -> int:
        """Export highway GeoJSON and return feature count."""
        try:
            from shapely.geometry import mapping, LineString
        except ImportError:
            raise ImportError("Shapely not available for GeoJSON export")
        
        scenarios = list(self.highway_bank.scenarios())
        if not scenarios:
            raise ValueError("No highway scenarios found")
        
        # Use first non-placeholder scenario (typically AM peak for highway)
        scenario_id = scenarios[0] if scenarios[0] != 1 else scenarios[1] if len(scenarios) > 1 else scenarios[0]
        scenario = self.highway_bank.scenario(scenario_id)
        network = scenario.get_network()
        
        features = []
        
        for link in network.links():
            try:
                # Create geometry from link shape
                geometry = mapping(LineString(link.shape))
                
                # Safely convert node IDs to integers
                try:
                    i_node_id = int(link.i_node.id)
                    j_node_id = int(link.j_node.id)
                except (ValueError, TypeError):
                    i_node_id = 0
                    j_node_id = 0
                
                feature = {
                    "type": "Feature",
                    "geometry": geometry,
                    "properties": {
                        "LINK_ID": f"{i_node_id}_{j_node_id}",
                        "INODE": i_node_id,
                        "JNODE": j_node_id,
                        "VOLUME": self._get_link_volume(link),
                        "CAPACITY": getattr(link, 'capacity', 0),
                        "LANES": getattr(link, 'num_lanes', 0),
                        "LENGTH": link.length,
                        "LINK_TYPE": getattr(link, 'type', 0),
                        "VDF": getattr(link, 'volume_delay_func', 0)
                    }
                }
                features.append(feature)
            except Exception:
                # Skip links with geometry issues
                continue
        
        geojson_data = {
            "type": "FeatureCollection",
            "crs": {
                "type": "name",
                "properties": {"name": "urn:ogc:def:crs:EPSG::2875"},
            },
            "features": features
        }
        
        with open(output_file, "w") as f:
            json.dump(geojson_data, f, indent=2)
        
        return len(features)


class NetworkShapefileExporter:
    """Exports network shapefiles using EMME Modeller."""
    
    def __init__(self, transit_bank, highway_bank):
        self.transit_bank = transit_bank
        self.highway_bank = highway_bank
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
    
    def export_shapefiles(self, output_dir: Path) -> List[str]:
        """Export network shapefiles and return list of created files."""
        try:
            import inro.modeller as _m
            _MODELLER = _m.Modeller()
            network_to_shapefile = _MODELLER.tool("inro.emme.data.network.export_network_as_shapefile")
        except ImportError:
            raise ImportError("EMME Modeller not available")
        
        exported_files = []
        
        # Export transit shapefiles
        transit_scenarios = list(self.transit_bank.scenarios())[:2]  # Limit to first 2
        for scenario_id in transit_scenarios:
            if scenario_id == 1:
                continue
                
            scenario = self.transit_bank.scenario(scenario_id)
            output_path = str(output_dir / f"transit_scenario_{scenario_id}")
            
            network_to_shapefile(
                export_path=output_path,
                scenario=scenario,
                transit_shapes='LINES_AND_SEGMENTS',
                selection={
                    "link": 'none',
                    "node": 'none',
                    "turn": 'none',
                    'transit_line': 'all'
                }
            )
            exported_files.append(f"transit_scenario_{scenario_id}")
        
        # Export highway shapefiles
        highway_scenarios = list(self.highway_bank.scenarios())[:2]  # Limit to first 2
        for scenario_id in highway_scenarios:
            scenario = self.highway_bank.scenario(scenario_id)
            output_path = str(output_dir / f"highway_scenario_{scenario_id}")
            
            network_to_shapefile(
                export_path=output_path,
                scenario=scenario,
                selection={
                    "link": 'all',
                    "node": 'all',
                    "turn": 'all',
                    'transit_line': 'none'
                }
            )
            exported_files.append(f"highway_scenario_{scenario_id}")
        
        return exported_files


def main() -> int:
    """Main entry point for the acceptance criteria processor."""
    model_run_dir = r"E:\2015-tm22-dev-sprint-04"
    
    processor = AcceptanceCriteriaProcessor(model_run_dir)
    success = processor.run()
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())