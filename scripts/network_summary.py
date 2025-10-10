#!/usr/bin/env python3
"""
TM2PY Network Summary Script

This script generates comprehensive network performance summaries across all time periods
including VMT, VHT, and delay calculations by facility type and user class, plus transit
boarding analysis.

Outputs:
    Highway Network Analysis:
    - VMT/VHT/Delay by facility classification
    - Performance metrics by user class  
    - County-level summaries
    - Lane mile inventories
    
    Transit Network Analysis:
    - Boardings by line and time period
    - Boardings by segment and time period
    - All-day boarding totals by line
    - Service type summaries by mode

Usage:
    python network_summary.py <model_run_directory> [--output <output_dir>] [--verbose]

Examples:
    python network_summary.py E:\\2015-tm22-dev-sprint-04
    python network_summary.py E:\\2015-tm22-dev-sprint-04 --output C:\\results
    python network_summary.py /path/to/model/run --verbose

Requirements:
    - TM2PY model results with EMME highway database
    - TM2PY model results with EMME transit database (optional)
    - tm2pyenv-acceptance Python environment
    - EMME API access via inro.emme modules
"""

import os
import sys
import logging
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional
import numpy as np
import argparse

# Add tm2py to path - dynamically find it relative to this script's location
# This script is in tm2py/scripts/, so tm2py root is one level up
script_dir = Path(__file__).parent.resolve()
tm2py_root = script_dir.parent
if tm2py_root not in [Path(p) for p in sys.path]:
    sys.path.insert(0, str(tm2py_root))

# EMME imports (may not be available in all environments)
try:
    from inro.emme.database.emmebank import Emmebank
    EMME_AVAILABLE = True
except ImportError:
    EMME_AVAILABLE = False


class NetworkSummarizer:
    """Generates comprehensive network performance summaries from EMME results."""
    
    def __init__(self, model_run_dir: str, output_dir: Optional[str] = None):
        """
        Initialize the network summarizer.
        
        Args:
            model_run_dir: Path to the model run directory
            output_dir: Optional output directory (defaults to model_run_dir/network_summary)
        """
        self.model_run_dir = Path(model_run_dir)
        self.output_dir = Path(output_dir) if output_dir else self.model_run_dir / "network_summary"
        
        # Set up logging
        self._setup_logging()
        
        # Initialize database connection
        self.highway_bank = None
        
        # Time period mapping
        self.time_period_mapping = {
            'ea': '3to6',   # Early AM (3 AM to 6 AM)
            'am': '6to10',  # AM peak (6 AM to 10 AM)
            'md': '10to15', # Midday (10 AM to 3 PM)
            'pm': '15to19', # PM peak (3 PM to 7 PM)
            'ev': '19to3'   # Evening (7 PM to 3 AM)
        }
        
        # Facility type mapping based on @ft (functional class) attribute
        self.facility_type_mapping = {
            1: 'freeway',      # Interstate/freeway
            2: 'freeway',      # Principal arterial - freeway 
            3: 'arterial',     # Principal arterial
            4: 'arterial',     # Minor arterial
            5: 'collector',    # Major collector
            6: 'collector',    # Minor collector  
            7: 'local',        # Local street
            8: 'connector',    # Connector/ramp
            99: 'other'        # Special/other
        }
        
        # County mapping (simplified - can be expanded)
        self.county_mapping = {
            1: 'San Francisco',
            2: 'San Mateo', 
            3: 'Santa Clara',
            4: 'Alameda',
            5: 'Contra Costa',
            6: 'Solano',
            7: 'Napa',
            8: 'Sonoma',
            9: 'Marin'
        }
        
    def _setup_logging(self) -> None:
        """Configure logging for the summarizer."""
        self.output_dir.mkdir(exist_ok=True)
        
        log_file = self.output_dir / "network_summary.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        
        self.logger = logging.getLogger(__name__)
        
    def validate_inputs(self) -> Dict[str, any]:
        """
        Comprehensive input validation for TM2PY network analysis.
        
        Returns:
            Dict with validation results and detailed diagnostics
            
        Raises:
            ValueError: If critical validation checks fail
        """
        validation_results = {
            'status': 'unknown',
            'errors': [],
            'warnings': [],
            'checks': {}
        }
        
        self.logger.info("=== Starting Input Validation ===")
        
        # 1. Validate directory structure
        dir_validation = self._validate_directory_structure()
        validation_results['checks']['directory_structure'] = dir_validation
        validation_results['errors'].extend(dir_validation.get('errors', []))
        validation_results['warnings'].extend(dir_validation.get('warnings', []))
        
        # 2. Validate EMME environment
        emme_validation = self._validate_emme_environment()
        validation_results['checks']['emme_environment'] = emme_validation
        validation_results['errors'].extend(emme_validation.get('errors', []))
        validation_results['warnings'].extend(emme_validation.get('warnings', []))
        
        # 3. Validate database accessibility
        if emme_validation['status'] == 'pass':
            db_validation = self._validate_database_access()
            validation_results['checks']['database_access'] = db_validation
            validation_results['errors'].extend(db_validation.get('errors', []))
            validation_results['warnings'].extend(db_validation.get('warnings', []))
            
            # 4. Validate scenarios if database is accessible
            if db_validation['status'] == 'pass':
                scenario_validation = self._validate_scenarios()
                validation_results['checks']['scenarios'] = scenario_validation
                validation_results['errors'].extend(scenario_validation.get('errors', []))
                validation_results['warnings'].extend(scenario_validation.get('warnings', []))
                
                # 5. Validate network attributes
                attr_validation = self._validate_network_attributes()
                validation_results['checks']['network_attributes'] = attr_validation
                validation_results['errors'].extend(attr_validation.get('errors', []))
                validation_results['warnings'].extend(attr_validation.get('warnings', []))
        
        # Determine overall status
        if validation_results['errors']:
            validation_results['status'] = 'fail'
            self.logger.error(f"Validation failed with {len(validation_results['errors'])} errors")
        elif validation_results['warnings']:
            validation_results['status'] = 'pass_with_warnings'
            self.logger.warning(f"Validation passed with {len(validation_results['warnings'])} warnings")
        else:
            validation_results['status'] = 'pass'
            self.logger.info("All validation checks passed")
        
        return validation_results
    
    def _validate_directory_structure(self) -> Dict[str, any]:
        """Validate TM2PY model run directory structure."""
        result = {'status': 'unknown', 'errors': [], 'warnings': [], 'details': {}}
        
        self.logger.info("Checking directory structure...")
        
        # Check model run directory exists
        if not self.model_run_dir.exists():
            result['errors'].append(f"Model run directory does not exist: {self.model_run_dir}")
            result['status'] = 'fail'
            return result
        
        result['details']['model_run_dir'] = str(self.model_run_dir)
        
        # Check required subdirectories
        required_dirs = {
            'emme_project': 'EMME project directory',
            'emme_project/Database_highway': 'Highway database directory',
        }
        
        for rel_path, description in required_dirs.items():
            full_path = self.model_run_dir / rel_path
            if not full_path.exists():
                result['errors'].append(f"Missing {description}: {full_path}")
            else:
                result['details'][rel_path] = str(full_path)
        
        # Check for emmebank file
        emmebank_path = self.model_run_dir / "emme_project" / "Database_highway" / "emmebank"
        if not emmebank_path.exists():
            result['errors'].append(f"EMME database file not found: {emmebank_path}")
        else:
            result['details']['emmebank'] = str(emmebank_path)
        
        # Check optional directories
        optional_dirs = ['outputs', 'logs', 'ctramp_output']
        for dir_name in optional_dirs:
            dir_path = self.model_run_dir / dir_name
            if not dir_path.exists():
                result['warnings'].append(f"Optional directory not found: {dir_path}")
            else:
                result['details'][dir_name] = str(dir_path)
        
        result['status'] = 'fail' if result['errors'] else 'pass'
        return result
    
    def _validate_emme_environment(self) -> Dict[str, any]:
        """Validate EMME Python API environment."""
        result = {'status': 'unknown', 'errors': [], 'warnings': [], 'details': {}}
        
        self.logger.info("Checking EMME environment...")
        
        if not EMME_AVAILABLE:
            result['errors'].append("EMME modules not available in Python environment")
            result['errors'].append("Please ensure you're using tm2pyenv environment with EMME API access")
            result['status'] = 'fail'
        else:
            result['details']['emme_modules'] = 'Available'
            result['status'] = 'pass'
            
        return result
    
    def _validate_database_access(self) -> Dict[str, any]:
        """Validate EMME database connectivity."""
        result = {'status': 'unknown', 'errors': [], 'warnings': [], 'details': {}}
        
        self.logger.info("Checking database access...")
        
        try:
            import inro.emme.database.emmebank as _eb
            
            highway_db_path = self.model_run_dir / "emme_project" / "Database_highway" / "emmebank"
            
            # Attempt to open database
            self.highway_bank = _eb.Emmebank(str(highway_db_path))
            
            # Get basic database info
            result['details']['database_path'] = str(highway_db_path)
            result['details']['database_title'] = getattr(self.highway_bank, 'title', 'Unknown')
            result['details']['num_scenarios'] = len(list(self.highway_bank.scenarios()))
            
            result['status'] = 'pass'
            
        except Exception as e:
            result['errors'].append(f"Failed to access EMME database: {e}")
            result['status'] = 'fail'
            
        return result
    
    def _validate_scenarios(self) -> Dict[str, any]:
        """Validate required time period scenarios."""
        result = {'status': 'unknown', 'errors': [], 'warnings': [], 'details': {}}
        
        self.logger.info("Checking scenarios...")
        
        if not self.highway_bank:
            result['errors'].append("Database not connected")
            result['status'] = 'fail'
            return result
        
        # Get all scenarios
        scenarios = list(self.highway_bank.scenarios())
        scenario_info = []
        
        for scenario in scenarios:
            info = {
                'id': scenario.id,
                'title': scenario.title or '(no title)',
                'time_period': self._map_scenario_to_time_period(scenario)
            }
            scenario_info.append(info)
            
        result['details']['scenarios'] = scenario_info
        result['details']['total_scenarios'] = len(scenarios)
        
        # Check for expected time periods
        found_periods = {info['time_period'] for info in scenario_info if info['time_period'] != 'unknown'}
        expected_periods = {'ea', 'am', 'md', 'pm', 'ev'}
        missing_periods = expected_periods - found_periods
        
        if missing_periods:
            result['warnings'].append(f"Missing time periods: {sorted(missing_periods)}")
        
        unknown_scenarios = [info for info in scenario_info if info['time_period'] == 'unknown']
        if unknown_scenarios:
            result['warnings'].append(f"Scenarios with unknown time periods: {[s['id'] for s in unknown_scenarios]}")
        
        result['details']['found_time_periods'] = sorted(found_periods)
        result['details']['missing_time_periods'] = sorted(missing_periods)
        
        result['status'] = 'pass'
        return result
    
    def _validate_network_attributes(self) -> Dict[str, any]:
        """Validate network link attributes."""
        result = {'status': 'unknown', 'errors': [], 'warnings': [], 'details': {}}
        
        self.logger.info("Checking network attributes...")
        
        if not self.highway_bank:
            result['errors'].append("Database not connected")
            result['status'] = 'fail'
            return result
        
        try:
            # Get first available scenario
            scenarios = list(self.highway_bank.scenarios())
            if not scenarios:
                result['errors'].append("No scenarios found in database")
                result['status'] = 'fail'
                return result
            
            scenario = scenarios[0]
            network = scenario.get_network()
            
            # Get sample link
            links = list(network.links())
            if not links:
                result['errors'].append("No links found in network")
                result['status'] = 'fail'
                return result
            
            sample_link = links[0]
            result['details']['network_size'] = {
                'num_links': len(links),
                'num_nodes': len(list(network.nodes()))
            }
            
            # Check critical attributes
            critical_attrs = {
                '@ft': 'Facility type classification',
                'auto_volume': 'Auto volume (TM2PY main)',
                'length': 'Link length',
                '@capacity': 'Link capacity'
            }
            
            important_attrs = {
                'auto_time': 'Auto travel time',
                '@auto_time': 'Auto time (alternative)',
                '@free_flow_time': 'Free flow travel time',
                '@free_flow_speed': 'Free flow speed',
                '@lanes': 'Number of lanes',
                'num_lanes': 'Number of lanes (alternative)'
            }
            
            attr_status = {}
            
            # Check critical attributes
            for attr, desc in critical_attrs.items():
                try:
                    value = getattr(sample_link, attr)
                    attr_status[attr] = {'available': True, 'value': str(value)[:50], 'description': desc}
                except AttributeError:
                    attr_status[attr] = {'available': False, 'description': desc}
                    if attr in ['@ft', 'length']:  # Absolutely critical
                        result['errors'].append(f"Critical attribute missing: {attr} ({desc})")
                    else:
                        result['warnings'].append(f"Critical attribute missing: {attr} ({desc})")
            
            # Check important attributes  
            for attr, desc in important_attrs.items():
                try:
                    value = getattr(sample_link, attr)
                    attr_status[attr] = {'available': True, 'value': str(value)[:50], 'description': desc}
                except AttributeError:
                    attr_status[attr] = {'available': False, 'description': desc}
                    result['warnings'].append(f"Important attribute missing: {attr} ({desc})")
            
            result['details']['attribute_status'] = attr_status
            
            # Check volume attribute availability
            volume_attrs = ['auto_volume', '@flow_da', '@flow_sr2']
            available_volume_attrs = [attr for attr in volume_attrs if attr_status.get(attr, {}).get('available')]
            
            if not available_volume_attrs:
                result['errors'].append("No volume attributes found")
            else:
                result['details']['available_volume_attrs'] = available_volume_attrs
            
            result['status'] = 'fail' if result['errors'] else 'pass'
            
        except Exception as e:
            result['errors'].append(f"Failed to check network attributes: {e}")
            result['status'] = 'fail'
            
        return result
        
    def _connect_to_emme_database(self) -> bool:
        """Connect to EMME highway database."""
        try:
            if not EMME_AVAILABLE:
                raise ImportError("EMME modules not available")
            
            highway_db_path = self.model_run_dir / "emme_project" / "Database_highway" / "emmebank"
            self.logger.info(f"Attempting to connect to EMME database at: {highway_db_path}")
            
            if not highway_db_path.exists():
                self.logger.error(f"EMME database file does not exist: {highway_db_path}")
                return False
            
            self.highway_bank = Emmebank(str(highway_db_path))
            
            # Log database information
            self.logger.info("Successfully connected to EMME highway database")
            self.logger.info(f"  Database title: {getattr(self.highway_bank, 'title', 'Unknown')}")
            self.logger.info(f"  Database path: {self.highway_bank.path}")
            
            # Log available scenarios
            scenarios = list(self.highway_bank.scenarios())
            self.logger.info(f"  Found {len(scenarios)} scenarios: {scenarios}")
            
            return True
            
        except ImportError as e:
            self.logger.error(f"EMME modules not available: {e}")
            self.logger.error("Make sure you're running in the correct EMME environment")
            return False
        except Exception as e:
            self.logger.error(f"Failed to connect to EMME database: {e}")
            return False
    
    def run(self) -> bool:
        """Execute the complete network summarization workflow."""
        self.logger.info("Starting TM2PY network performance summarization")
        self.logger.info(f"Model directory: {self.model_run_dir}")
        self.logger.info(f"Output directory: {self.output_dir}")
        
        # Run comprehensive input validation
        validation_results = self.validate_inputs()
        
        if validation_results['status'] == 'fail':
            self.logger.error("Input validation failed. Cannot proceed.")
            self._log_validation_results(validation_results)
            return False
        elif validation_results['status'] == 'pass_with_warnings':
            self.logger.warning("Input validation passed with warnings. Proceeding with caution.")
            self._log_validation_results(validation_results)
        else:
            self.logger.info("Input validation passed successfully.")
        
        # Database should already be connected from validation
        if not self.highway_bank:
            self.logger.error("Database connection not established during validation")
            return False
        
        try:
            # Extract link-level data across all time periods
            link_data = self._extract_all_time_periods()
            
            if link_data.empty:
                self.logger.error("No link data extracted")
                return False
            
            # Validate extracted data quality
            if not self._validate_extracted_data(link_data):
                self.logger.error("Data validation failed")
                return False
            
            # Generate summary reports
            self._summarize_network_performance(link_data)
            
            self.logger.info("Network summarization completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Summarization failed: {e}", exc_info=True)
            return False
    
    def _log_validation_results(self, validation_results: Dict[str, any]) -> None:
        """Log detailed validation results."""
        self.logger.info("=== Validation Results Summary ===")
        
        # Log overall status
        status = validation_results['status']
        if status == 'fail':
            self.logger.error(f"Overall Status: FAILED")
        elif status == 'pass_with_warnings':
            self.logger.warning(f"Overall Status: PASSED WITH WARNINGS")
        else:
            self.logger.info(f"Overall Status: PASSED")
        
        # Log errors
        if validation_results['errors']:
            self.logger.error(f"Errors ({len(validation_results['errors'])}):")
            for i, error in enumerate(validation_results['errors'], 1):
                self.logger.error(f"  {i}. {error}")
        
        # Log warnings  
        if validation_results['warnings']:
            self.logger.warning(f"Warnings ({len(validation_results['warnings'])}):")
            for i, warning in enumerate(validation_results['warnings'], 1):
                self.logger.warning(f"  {i}. {warning}")
        
        # Log check details
        for check_name, check_result in validation_results['checks'].items():
            status_symbol = "PASS" if check_result['status'] == 'pass' else "FAIL"
            self.logger.info(f"{status_symbol} {check_name}: {check_result['status']}")
            
            # Log key details
            if 'details' in check_result and check_result['details']:
                for key, value in check_result['details'].items():
                    if isinstance(value, (str, int, float, bool)):
                        self.logger.debug(f"    {key}: {value}")
                    elif isinstance(value, dict) and key == 'attribute_status':
                        available_attrs = [attr for attr, info in value.items() if info.get('available')]
                        missing_attrs = [attr for attr, info in value.items() if not info.get('available')]
                        self.logger.info(f"    Available attributes: {len(available_attrs)}")
                        self.logger.info(f"    Missing attributes: {len(missing_attrs)}")
                        if missing_attrs:
                            self.logger.debug(f"    Missing: {missing_attrs}")
    
    def _extract_all_time_periods(self) -> pd.DataFrame:
        """Extract link-level data for all time periods."""
        self.logger.info("Extracting link data for all time periods")
        
        all_data = []
        scenarios = list(self.highway_bank.scenarios())
        
        for scenario_id in scenarios:
            scenario = self.highway_bank.scenario(scenario_id)
            time_period = self._map_scenario_to_time_period(scenario)
            
            if time_period == 'unknown':
                self.logger.warning(f"Skipping scenario {scenario_id}: unknown time period")
                continue
                
            self.logger.info(f"Processing scenario {scenario_id}: {scenario.title} -> {time_period}")
            
            try:
                network = scenario.get_network()
                scenario_data = self._extract_scenario_links(network, time_period)
                all_data.extend(scenario_data)
                
            except Exception as e:
                self.logger.warning(f"Failed to process scenario {scenario_id}: {e}")
        
        if not all_data:
            return pd.DataFrame()
            
        df = pd.DataFrame(all_data)
        self.logger.info(f"Extracted {len(df)} link records across {df['time_period'].nunique()} time periods")
        
        return df
    
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
    
    def _extract_scenario_links(self, network, time_period: str) -> List[Dict]:
        """Extract link data for a single scenario."""
        data = []
        links_processed = 0
        links_with_volume = 0
        links_missing_attributes = 0
        attribute_stats = {
            # Volume attributes (actual TM2PY)
            'auto_volume': 0, '@flow_da': 0, '@flow_sr2': 0,
            # Time/performance attributes  
            'auto_time': 0, '@auto_time': 0, '@free_flow_time': 0,
            # Facility type and classification
            '@ft': 0, 'type': 0, 
            # Capacity attributes
            '@lanes': 0, 'num_lanes': 0, '@capacity': 0
        }
        
        self.logger.info(f"Extracting link data for {time_period} period...")
        
        for link in network.links():
            links_processed += 1
            
            # Log progress every 10000 links
            if links_processed % 10000 == 0:
                self.logger.info(f"  Processed {links_processed:,} links...")
            
            # Get key link attributes using actual TM2PY attribute names
            volume = getattr(link, 'auto_volume', 0)  # Main volume attribute
            auto_time = getattr(link, 'auto_time', 0) or getattr(link, '@free_flow_time', 0)
            length = link.length
            num_lanes = getattr(link, '@lanes', 1) or getattr(link, 'num_lanes', 1)
            capacity = getattr(link, '@capacity', 0)
            
            # Get additional TM2PY performance metrics
            free_flow_time = getattr(link, '@free_flow_time', auto_time)
            free_flow_speed = getattr(link, '@free_flow_speed', 0)
            
            # Calculate derived performance metrics
            congested_speed = (length / (auto_time / 60)) if auto_time > 0 else free_flow_speed
            delay = max(0, auto_time - free_flow_time) if auto_time > 0 and free_flow_time > 0 else 0
            vol_over_cap = (volume / capacity) if capacity > 0 else 0
            
            # Track attribute availability
            for attr in attribute_stats:
                if hasattr(link, attr) and getattr(link, attr, None) is not None:
                    attribute_stats[attr] += 1
            
            # Count links with actual volume
            if volume > 0:
                links_with_volume += 1
            
            # Get facility type from @ft (functional class attribute)
            functional_class = getattr(link, '@ft', 0)
            facility_type = self.facility_type_mapping.get(functional_class, 'other')
            
            # Get county ID (assuming from data1 or similar)
            county_id = getattr(link, 'data1', 0)
            county_name = self.county_mapping.get(county_id, 'Outside Region')
            
            # Log detailed info for first 5 links
            if links_processed <= 5:
                self.logger.debug(f"  Link {links_processed}: {link.i_node.id}->{link.j_node.id}")
                self.logger.debug(f"    Volume: {volume}, Time: {auto_time}, Length: {length}")
                self.logger.debug(f"    FT: {functional_class} ({facility_type}), County: {county_id} ({county_name})")
            
            # Track missing critical attributes
            if functional_class == 0 or county_id == 0 or volume == 0:
                links_missing_attributes += 1
            
            # Create link identifier
            link_id = f"{link.i_node.id}_{link.j_node.id}"
            
            data.append({
                'link_id': link_id,
                'i_node': int(link.i_node.id),
                'j_node': int(link.j_node.id),
                'time_period': time_period,
                'volume': volume,
                'auto_time': auto_time,
                'length': length,
                'num_lanes': num_lanes,
                'capacity': capacity,
                'functional_class': functional_class,
                'facility_type': facility_type,
                'county_id': county_id,
                'county_name': county_name,
                # Additional TM2PY performance metrics
                'congested_speed': congested_speed,
                'delay': delay,
                'vol_over_cap': vol_over_cap
            })
        
        # Log summary statistics
        self.logger.info(f"  Completed {time_period}: {links_processed:,} links processed")
        self.logger.info(f"    Links with volume > 0: {links_with_volume:,} ({links_with_volume/links_processed*100:.1f}%)")
        self.logger.info(f"    Links missing key attributes: {links_missing_attributes:,} ({links_missing_attributes/links_processed*100:.1f}%)")
        
        # Log attribute availability
        self.logger.info("  Attribute availability:")
        for attr, count in attribute_stats.items():
            pct = count / links_processed * 100 if links_processed > 0 else 0
            self.logger.info(f"    {attr}: {count:,}/{links_processed:,} ({pct:.1f}%)")
        
        return data
    
    def _validate_extracted_data(self, df: pd.DataFrame) -> bool:
        """Validate extracted data for common issues."""
        self.logger.info("Validating extracted data quality...")
        
        if df.empty:
            self.logger.error("No data extracted from EMME database")
            return False
        
        # Check data completeness
        total_records = len(df)
        self.logger.info(f"Total records extracted: {total_records:,}")
        
        # Check for missing critical data
        zero_volume_pct = (df['volume'] == 0).sum() / total_records * 100
        zero_length_pct = (df['length'] == 0).sum() / total_records * 100
        zero_ft_pct = (df['functional_class'] == 0).sum() / total_records * 100
        
        self.logger.info(f"Data quality checks:")
        self.logger.info(f"  Zero volume links: {zero_volume_pct:.1f}%")
        self.logger.info(f"  Zero length links: {zero_length_pct:.1f}%")
        self.logger.info(f"  Unknown facility type (@ft=0): {zero_ft_pct:.1f}%")
        
        # Check value ranges
        volume_stats = df['volume'].describe()
        time_stats = df['auto_time'].describe()
        length_stats = df['length'].describe()
        
        self.logger.info(f"Value ranges:")
        self.logger.info(f"  Volume: {volume_stats['min']:.0f} to {volume_stats['max']:.0f} (mean: {volume_stats['mean']:.0f})")
        self.logger.info(f"  Auto time: {time_stats['min']:.1f} to {time_stats['max']:.1f} (mean: {time_stats['mean']:.1f})")
        self.logger.info(f"  Length: {length_stats['min']:.3f} to {length_stats['max']:.3f} (mean: {length_stats['mean']:.3f})")
        
        # Check facility type distribution
        ft_counts = df['facility_type'].value_counts()
        self.logger.info(f"Facility type distribution:")
        for ft, count in ft_counts.items():
            pct = count / total_records * 100
            self.logger.info(f"  {ft}: {count:,} ({pct:.1f}%)")
        
        # Check time period distribution
        tp_counts = df['time_period'].value_counts()
        self.logger.info(f"Time period distribution:")
        for tp, count in tp_counts.items():
            pct = count / total_records * 100
            self.logger.info(f"  {tp}: {count:,} ({pct:.1f}%)")
        
        # Warning thresholds
        warnings = []
        if zero_volume_pct > 50:
            warnings.append(f"High percentage of zero-volume links ({zero_volume_pct:.1f}%)")
        if zero_ft_pct > 20:
            warnings.append(f"High percentage of unknown facility types ({zero_ft_pct:.1f}%)")
        if volume_stats['max'] > 50000:
            warnings.append(f"Extremely high volume detected ({volume_stats['max']:.0f})")
        if time_stats['max'] > 3600:
            warnings.append(f"Extremely high travel time detected ({time_stats['max']:.1f} seconds)")
        
        if warnings:
            self.logger.warning("Data quality warnings:")
            for warning in warnings:
                self.logger.warning(f"  ⚠️  {warning}")
        
        return True
    
    def _summarize_network_performance(self, df: pd.DataFrame) -> None:
        """Generate comprehensive network performance summaries."""
        self.logger.info("Generating network performance summaries")
        
        # Exclude non-designated facilities (functional_class == 0)
        df = df[df['functional_class'] != 0].copy()
        
        # Calculate basic metrics
        df = self._calculate_performance_metrics(df)
        
        # Create Excel writer for multiple sheets
        excel_file = self.output_dir / "network_performance_summary.xlsx"
        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
            
            # Summary by facility type
            self._summarize_by_facility_type(df, writer)
            
            # Summary by time period
            self._summarize_by_time_period(df, writer)
            
            # County-level summaries
            self._summarize_by_county(df, writer)
            
            # Lane mile inventory
            self._generate_lane_mile_inventory(df, writer)
            
            # Overall summary
            self._generate_overall_summary(df, writer)
        
        self.logger.info(f"Network summary saved: {excel_file}")
    
    def _calculate_performance_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate VMT, VHT, and delay metrics."""
        self.logger.info("Calculating performance metrics")
        
        # Calculate VMT and VHT
        df['vmt'] = df['volume'] * df['length']
        df['vht'] = df['volume'] * df['auto_time'] / 60  # Convert to hours
        
        # Calculate lane miles
        df['lane_miles'] = df['length'] * df['num_lanes']
        
        # Calculate delay (using overnight as freeflow)
        freeflow_data = df[df['time_period'] == 'ea'][['link_id', 'auto_time']].copy()
        freeflow_data.rename(columns={'auto_time': 'freeflow_time'}, inplace=True)
        
        # Merge freeflow times
        df = df.merge(freeflow_data, on='link_id', how='left')
        
        # Calculate delay (handle missing freeflow times)
        df['freeflow_time'] = df['freeflow_time'].fillna(df['auto_time'])
        df['delay_per_vehicle'] = np.maximum(0, df['auto_time'] - df['freeflow_time'])
        df['total_delay'] = (df['delay_per_vehicle'] * df['volume']) / 60  # Hours
        
        # Calculate speeds
        df['speed_mph'] = np.where(df['auto_time'] > 0, 
                                  (df['length'] / df['auto_time']) * 3600 / 5280, 
                                  0)
        
        return df
    
    def _summarize_by_facility_type(self, df: pd.DataFrame, writer) -> None:
        """Generate summaries by facility type."""
        metrics = ['vmt', 'vht', 'total_delay']
        
        for metric in metrics:
            summary = pd.pivot_table(
                df,
                values=metric,
                index=['time_period'],
                columns='facility_type',
                aggfunc='sum'
            ).reset_index()
            
            # Sort by time period
            time_order = ['ea', 'am', 'md', 'pm', 'ev']
            summary['time_period'] = pd.Categorical(summary['time_period'], categories=time_order, ordered=True)
            summary = summary.sort_values('time_period').reset_index(drop=True)
            
            # Save to Excel and CSV
            sheet_name = f"{metric.upper()} by Facility"
            summary.to_excel(writer, sheet_name=sheet_name, index=False)
            
            csv_file = self.output_dir / f"{metric}_by_facility.csv"
            summary.to_csv(csv_file, index=False)
            
            self.logger.info(f"Generated {metric} by facility type summary")
    
    def _summarize_by_time_period(self, df: pd.DataFrame, writer) -> None:
        """Generate summaries by time period."""
        summary = df.groupby('time_period').agg({
            'vmt': 'sum',
            'vht': 'sum', 
            'total_delay': 'sum',
            'volume': 'sum',
            'length': lambda x: (x * df.loc[x.index, 'volume']).sum() / df.loc[x.index, 'volume'].sum()  # Weighted avg
        }).reset_index()
        
        # Add average speed
        summary['avg_speed_mph'] = (summary['vmt'] / summary['vht']).fillna(0)
        
        # Sort by time period
        time_order = ['ea', 'am', 'md', 'pm', 'ev']
        summary['time_period'] = pd.Categorical(summary['time_period'], categories=time_order, ordered=True)
        summary = summary.sort_values('time_period').reset_index(drop=True)
        
        summary.to_excel(writer, sheet_name="Summary by Time Period", index=False)
        
        csv_file = self.output_dir / "summary_by_time_period.csv"
        summary.to_csv(csv_file, index=False)
        
        self.logger.info("Generated time period summary")
    
    def _summarize_by_county(self, df: pd.DataFrame, writer) -> None:
        """Generate county-level summaries."""
        county_summary = df.groupby('county_name').agg({
            'vmt': 'sum',
            'vht': 'sum',
            'total_delay': 'sum',
            'volume': 'sum'
        }).reset_index()
        
        county_summary['avg_speed_mph'] = (county_summary['vmt'] / county_summary['vht']).fillna(0)
        
        county_summary.to_excel(writer, sheet_name="County Summary", index=False)
        
        csv_file = self.output_dir / "county_summary.csv"
        county_summary.to_csv(csv_file, index=False)
        
        self.logger.info("Generated county summary")
    
    def _generate_lane_mile_inventory(self, df: pd.DataFrame, writer) -> None:
        """Generate lane mile inventory by county and facility type."""
        # Use one time period to avoid double counting
        inventory_df = df[df['time_period'] == 'am'].copy()
        
        lane_miles = pd.pivot_table(
            inventory_df,
            values='lane_miles',
            index='county_name',
            columns='facility_type',
            aggfunc='sum'
        ).reset_index()
        
        # Add VMT for context
        vmt_summary = df.groupby('county_name')['vmt'].sum().reset_index()
        vmt_summary.rename(columns={'vmt': 'total_daily_vmt'}, inplace=True)
        
        lane_miles = lane_miles.merge(vmt_summary, on='county_name', how='left')
        
        lane_miles.to_excel(writer, sheet_name="Lane Mile Inventory", index=False)
        
        csv_file = self.output_dir / "lane_mile_inventory.csv"
        lane_miles.to_csv(csv_file, index=False)
        
        self.logger.info("Generated lane mile inventory")
    
    def _generate_overall_summary(self, df: pd.DataFrame, writer) -> None:
        """Generate overall system summary."""
        total_summary = {
            'Total Daily VMT': df['vmt'].sum(),
            'Total Daily VHT': df['vht'].sum(),
            'Total Daily Delay (hours)': df['total_delay'].sum(),
            'Average System Speed (mph)': df['vmt'].sum() / df['vht'].sum() if df['vht'].sum() > 0 else 0,
            'Total Lane Miles': df[df['time_period'] == 'am']['lane_miles'].sum(),
            'Peak Hour VMT (AM)': df[df['time_period'] == 'am']['vmt'].sum(),
            'Peak Hour VMT (PM)': df[df['time_period'] == 'pm']['vmt'].sum(),
        }
        
        summary_df = pd.DataFrame(list(total_summary.items()), columns=['Metric', 'Value'])
        summary_df.to_excel(writer, sheet_name="Overall Summary", index=False)
        
        csv_file = self.output_dir / "overall_summary.csv"
        summary_df.to_csv(csv_file, index=False)
        
        self.logger.info("Generated overall summary")
    
    def _generate_transit_summaries(self) -> None:
        """Transit analysis removed - focusing on highway network analysis only."""
        self.logger.info("Transit analysis not implemented in this version")


def main() -> int:
    """Main entry point for the network summarizer."""
    parser = argparse.ArgumentParser(
        description="Generate comprehensive network performance summaries from TM2PY EMME results",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python network_summary.py E:\\2015-tm22-dev-sprint-04
    python network_summary.py E:\\2015-tm22-dev-sprint-04 --output C:\\results
    python network_summary.py --help
        """
    )
    
    parser.add_argument(
        'model_run_dir',
        type=str,
        help='Path to the TM2PY model run directory containing EMME results'
    )
    
    parser.add_argument(
        '--output', '-o',
        type=str,
        default=None,
        help='Output directory for summary files (default: model_run_dir/network_summary)'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    
    parser.add_argument(
        '--validate-only',
        action='store_true',
        help='Run input validation only without processing (useful for troubleshooting)'
    )
    
    args = parser.parse_args()
    
    # Validate model run directory
    model_run_path = Path(args.model_run_dir)
    if not model_run_path.exists():
        print(f"Error: Model run directory does not exist: {model_run_path}")
        return 1
    
    # Set up logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        summarizer = NetworkSummarizer(args.model_run_dir, args.output)
        
        if args.validate_only:
            print("Running input validation only...")
            validation_results = summarizer.validate_inputs()
            summarizer._log_validation_results(validation_results)
            
            if validation_results['status'] == 'fail':
                print("\n❌ Validation FAILED")
                return 1
            elif validation_results['status'] == 'pass_with_warnings':
                print("\n⚠️  Validation PASSED with warnings")
                return 0
            else:
                print("\n✅ Validation PASSED")
                return 0
        else:
            success = summarizer.run()
            
            if success:
                print(f"\nNetwork summary completed successfully!")
                print(f"Results saved to: {summarizer.output_dir}")
                print(f"\nGenerated files include:")
                print(f"  Highway Analysis:")
                print(f"    - facility_type_summary.csv (VMT/VHT by facility type)")
                print(f"    - overall_summary.csv (system-wide metrics)")
                print(f"    - lane_mile_inventory.csv (network inventory)")
                print(f"  Transit Analysis (if transit database available):")
                print(f"    - transit_boardings_by_line_{{period}}.csv (line boardings by time period)")
                print(f"    - transit_boardings_by_segment_{{period}}.csv (segment boardings by time period)")
                print(f"    - transit_boardings_by_line_daily.csv (all-day totals by line)")
                print(f"    - transit_boardings_by_service_type.csv (summary by mode)")
            else:
                print("\nNetwork summary failed. Check logs for details.")
                
            return 0 if success else 1
        
    except Exception as e:
        print(f"Error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())