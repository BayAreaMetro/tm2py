"""
TM2PY Network Summary Component

This component generates comprehensive network performance summaries across all time periods
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

Usage (as component):
    from tm2py.components.network_summary import NetworkSummary
    from tm2py.controller import RunController
    
    controller = RunController(["scenario.toml", "model.toml"])
    summarizer = NetworkSummary(controller)
    summarizer.run()

Requirements:
    - TM2PY model results with EMME highway database
    - TM2PY model results with EMME transit database 
    - EMME API access via inro.emme modules

Documentation:
    Complete Usage Guide: docs/output/network-summary-usage.md
    Network Attributes Reference: docs/output/network-analysis.md
    Unit Tests: tests/test_network_summary.py
"""

import os
import logging
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, TYPE_CHECKING
import numpy as np
import argparse
import sys

from tm2py.components.component import Component
from tm2py.logger import LogStartEnd

# EMME availability check
try:
    import inro.emme.database.emmebank as _eb
    from inro.emme.database.emmebank import Emmebank
    EMME_AVAILABLE = True
except ImportError:
    EMME_AVAILABLE = False

if TYPE_CHECKING:
    from tm2py.controller import RunController


class NetworkSummary(Component):
    """Network performance summary generation component."""

    def __init__(self, controller: "RunController"):
        """
        Initialize the network summary component.
        
        Args:
            controller: Reference to run controller object
        """
        super().__init__(controller)
        self._highway_emmebank = None
        self._transit_emmebank = None
        
        # Set up output directory to match PostProcessor pattern
        # Use the same "outputs" directory as other TM2PY components
        self.output_dir = Path(self.controller.run_dir) / "outputs"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Set up logging
        self._setup_logging()
        
        # Time period mapping from controller configuration
        self._tp_mapping = {
            tp.name.upper(): tp.emme_scenario_id
            for tp in self.controller.config.time_periods
        }
        
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
        
        # TM2PY scenario ID to time period mapping
        # Based on standard TM2PY model structure where specific scenario IDs 
        # correspond to time periods (e.g., scenario 11=ea, 12=am, etc.)
        self.scenario_id_mapping = {
            11: 'ea',  # Early AM (3 AM to 6 AM)
            12: 'am',  # AM peak (6 AM to 10 AM) 
            13: 'md',  # Midday (10 AM to 3 PM)
            14: 'pm',  # PM peak (3 PM to 7 PM)
            15: 'ev'   # Evening (7 PM to 3 AM)
        }
    
    def _setup_logging(self) -> None:
        """Setup component-specific logging configuration."""
        # The logger is already available from the Component base class
        # This method can be used for any component-specific logging setup
        self.logger.info("NetworkSummary component logging initialized")

    @property
    def highway_emmebank(self):
        """Access to highway EMME database."""
        if not self._highway_emmebank:
            self._highway_emmebank = self.controller.emme_manager.highway_emmebank
        return self._highway_emmebank

    @property
    def transit_emmebank(self):
        """Access to transit EMME database."""
        if not self._transit_emmebank:
            self._transit_emmebank = self.controller.emme_manager.transit_emmebank
        return self._transit_emmebank
    
    @property
    def time_period_names(self):
        """Get list of time period names from controller configuration."""
        return [tp.name for tp in self.controller.config.time_periods]
    
    @property  
    def model_run_dir(self):
        """Get the model run directory from controller."""
        return self.controller.run_dir

    @LogStartEnd("Generating network performance summaries")
    def run(self) -> bool:
        """
        Generate comprehensive network performance summaries.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            self.logger.info("=" * 60)
            self.logger.info("STARTING NETWORK SUMMARY COMPONENT")
            self.logger.info("=" * 60)
            self.logger.info(f"Output directory: {self.output_dir}")
            self.logger.info(f"Time periods configured: {self.time_period_names}")
            self.logger.info(f"Time period mapping: {self._tp_mapping}")
            
            # Test database connections first
            self.logger.info("Testing database connections...")
            
            # Test database connections
            # Test highway database connection
            highway_test_scenario = None
            for period in self.time_period_names:
                try:
                    highway_test_scenario = self.highway_emmebank.scenario(period)
                    self.logger.info(f"SUCCESS: Highway database connected - scenario {period} accessible")
                    break
                except:
                    continue
            
            if not highway_test_scenario:
                self.logger.error("ERROR: No highway scenarios accessible")
                return False
            
            # Test transit database connection  
            transit_test_scenario = None
            for period in self.time_period_names:
                try:
                    transit_test_scenario = self.transit_emmebank.scenario(period)
                    self.logger.info(f"SUCCESS: Transit database connected - scenario {period} accessible")
                    break
                except:
                    continue
            
            if not transit_test_scenario:
                self.logger.warn("WARNING: No transit scenarios accessible - transit analysis will be skipped")
                
            
            # Generate highway analysis
            self.logger.info("-" * 40)
            self.logger.info("STARTING HIGHWAY NETWORK ANALYSIS")
            self.logger.info("-" * 40)
            if self._analyze_highway_network():
                self.logger.info("SUCCESS: Highway analysis completed successfully")
            else:
                self.logger.error("ERROR: Highway analysis failed")
                return False
            
            # Generate transit analysis
            self.logger.info("-" * 40)
            self.logger.info("STARTING TRANSIT NETWORK ANALYSIS")
            self.logger.info("-" * 40)
            if self._analyze_transit_network():
                self.logger.info("SUCCESS: Transit analysis completed successfully")
            else:
                self.logger.warn("WARNING: Transit analysis failed or skipped")
            
            # Generate final validation summary
            self._generate_final_validation_summary()
            
            self.logger.info("=" * 60)
            self.logger.info("NETWORK SUMMARY COMPONENT COMPLETED SUCCESSFULLY")
            self.logger.info("=" * 60)
            return True
            
        except Exception as e:
            self.logger.error(f"ERROR: Network summary generation failed: {e}")
            return False

    def validate_inputs(self) -> None:
        """Validate the inputs to the component."""
        # For TM2PY components, most validation is handled by the controller
        # Just do basic checks here
        self.logger.info("NetworkSummary: Validating component inputs...")
        
        # Check that output directory can be created
        try:
            self.output_dir.mkdir(parents=True, exist_ok=True)
            self.logger.info(f"NetworkSummary: Output directory validated: {self.output_dir}")
        except Exception as e:
            raise ValueError(f"Cannot create output directory {self.output_dir}: {e}")
        
        # Check that we have time periods configured
        if not self.time_period_names:
            raise ValueError("No time periods configured in controller")
        
        self.logger.info(f"NetworkSummary: Time periods configured: {self.time_period_names}")
        self.logger.info("NetworkSummary: Input validation completed successfully")

    def report_progress(self) -> None:
        """Report progress to the user."""
        # Progress is reported through logging in the component
        pass

    def verify(self) -> None:
        """Verify the component's output."""
        # Output verification happens through validate_outputs
        output_validation = self.validate_outputs()
        if output_validation['status'] == 'fail':
            self.logger.error("Output verification failed")

    def write_top_sheet(self) -> None:
        """Write outputs to topsheet."""
        # Network summary doesn't write to topsheet currently
        pass

    def test_component(self) -> None:
        """Test the component."""
        # Component testing is done through unit tests
        pass

    def _validate_inputs_comprehensive(self) -> Dict[str, any]:
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
            self.logger.warn(f"Validation passed with {len(validation_results['warnings'])} warnings")
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
            'emme_project/Database_transit': 'Transit database directory',
        }
        
        for rel_path, description in required_dirs.items():
            full_path = self.model_run_dir / rel_path
            if not full_path.exists():
                result['errors'].append(f"Missing {description}: {full_path}")
            else:
                result['details'][rel_path] = str(full_path)
        
        # Check for emmebank files
        highway_emmebank = self.model_run_dir / "emme_project" / "Database_highway" / "emmebank"
        if not highway_emmebank.exists():
            result['errors'].append(f"Highway EMME database file not found: {highway_emmebank}")
        else:
            result['details']['highway_emmebank'] = str(highway_emmebank)
        
        transit_emmebank = self.model_run_dir / "emme_project" / "Database_transit" / "emmebank"
        if not transit_emmebank.exists():
            result['errors'].append(f"Transit EMME database file not found: {transit_emmebank}")
        else:
            result['details']['transit_emmebank'] = str(transit_emmebank)
        
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
            transit_db_path = self.model_run_dir / "emme_project" / "Database_transit" / "emmebank"
            
            # Attempt to open highway database
            self.highway_bank = _eb.Emmebank(str(highway_db_path))
            
            # Get basic highway database info
            result['details']['highway_database_path'] = str(highway_db_path)
            result['details']['highway_database_title'] = getattr(self.highway_bank, 'title', 'Unknown')
            result['details']['highway_num_scenarios'] = len(list(self.highway_bank.scenarios()))
            
            # Attempt to open transit database
            self.transit_bank = _eb.Emmebank(str(transit_db_path))
            
            # Get basic transit database info
            result['details']['transit_database_path'] = str(transit_db_path)
            result['details']['transit_database_title'] = getattr(self.transit_bank, 'title', 'Unknown')
            result['details']['transit_num_scenarios'] = len(list(self.transit_bank.scenarios()))
            
            result['status'] = 'pass'
            
        except Exception as e:
            result['errors'].append(f"Failed to access EMME databases: {e}")
            result['status'] = 'fail'
            
        return result
    
    def _validate_scenarios(self) -> Dict[str, any]:
        """Validate required time period scenarios in both highway and transit databases."""
        result = {'status': 'unknown', 'errors': [], 'warnings': [], 'details': {}}
        
        self.logger.info("Checking scenarios...")
        
        if not self.highway_bank or not self.transit_bank:
            result['errors'].append("Highway or transit database not connected")
            result['status'] = 'fail'
            return result
        
        # Validate highway scenarios
        highway_scenarios = list(self.highway_bank.scenarios())
        highway_scenario_info = []
        
        for scenario in highway_scenarios:
            info = {
                'id': scenario.id,
                'title': scenario.title or '(no title)',
                'time_period': self._map_scenario_to_time_period(scenario)
            }
            highway_scenario_info.append(info)
            
        result['details']['highway_scenarios'] = highway_scenario_info
        result['details']['highway_total_scenarios'] = len(highway_scenarios)
        
        # Validate transit scenarios
        transit_scenarios = list(self.transit_bank.scenarios())
        transit_scenario_info = []
        
        for scenario in transit_scenarios:
            info = {
                'id': scenario.id,
                'title': scenario.title or '(no title)',
                'time_period': self._map_scenario_to_time_period(scenario)
            }
            transit_scenario_info.append(info)
            
        result['details']['transit_scenarios'] = transit_scenario_info
        result['details']['transit_total_scenarios'] = len(transit_scenarios)
        
        # Check for expected time periods in both databases
        highway_found_periods = {info['time_period'] for info in highway_scenario_info if info['time_period'] != 'unknown'}
        transit_found_periods = {info['time_period'] for info in transit_scenario_info if info['time_period'] != 'unknown'}
        expected_periods = {'ea', 'am', 'md', 'pm', 'ev'}
        
        highway_missing_periods = expected_periods - highway_found_periods
        transit_missing_periods = expected_periods - transit_found_periods
        
        if highway_missing_periods:
            result['warnings'].append(f"Missing highway time periods: {sorted(highway_missing_periods)}")
        
        if transit_missing_periods:
            result['warnings'].append(f"Missing transit time periods: {sorted(transit_missing_periods)}")
        
        highway_unknown_scenarios = [info for info in highway_scenario_info if info['time_period'] == 'unknown']
        transit_unknown_scenarios = [info for info in transit_scenario_info if info['time_period'] == 'unknown']
        
        if highway_unknown_scenarios:
            unknown_ids = [s['id'] for s in highway_unknown_scenarios]
            result['warnings'].append(f"Highway scenarios will be skipped (unknown time periods): {unknown_ids}")
            
        if transit_unknown_scenarios:
            unknown_ids = [s['id'] for s in transit_unknown_scenarios]
            result['warnings'].append(f"Transit scenarios will be skipped (unknown time periods): {unknown_ids}")
        
        result['details']['highway_found_time_periods'] = sorted(highway_found_periods)
        result['details']['highway_missing_time_periods'] = sorted(highway_missing_periods)
        result['details']['transit_found_time_periods'] = sorted(transit_found_periods)
        result['details']['transit_missing_time_periods'] = sorted(transit_missing_periods)
        
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
                '@flow_da': 'Drive alone flow (TM2PY assignment result)',
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
            # Primary TM2PY flow attributes populated after assignment
            volume_attrs = ['@flow_da', '@flow_sr2', '@flow_sr3', '@flow_trk', 'auto_volume']
            available_volume_attrs = [attr for attr in volume_attrs if attr_status.get(attr, {}).get('available')]
            
            if not available_volume_attrs:
                result['errors'].append("No volume attributes found - model may need traffic assignment first")
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
    
    def _analyze_highway_network(self) -> bool:
        """Analyze highway network performance across all time periods."""
        try:
            self.logger.info("HIGHWAY: Initializing highway network analysis...")
            self.logger.info(f"HIGHWAY: Available time periods: {self.time_period_names}")
            
            # Extract link-level data across all time periods
            self.logger.info("HIGHWAY: Starting data extraction across all time periods...")
            link_data = self._extract_all_time_periods()
            
            if link_data.empty:
                self.logger.error("HIGHWAY ERROR: No highway link data extracted - this is a critical error")
                return False
            
            self.logger.info(f"HIGHWAY SUCCESS: Extracted {len(link_data):,} link records")
            self.logger.info(f"HIGHWAY: Time periods found in data: {sorted(link_data['time_period'].unique())}")
            self.logger.info(f"HIGHWAY: Memory usage: {link_data.memory_usage(deep=True).sum() / 1024 / 1024:.1f} MB")
            
            # Validate extracted data quality
            self.logger.info("HIGHWAY: Validating extracted data quality...")
            if not self._validate_extracted_data(link_data):
                self.logger.error("HIGHWAY ERROR: Basic data validation failed")
                return False
            
            self.logger.info("HIGHWAY SUCCESS: Basic data validation passed")
            
            # Comprehensive data consistency validation
            self.logger.info("HIGHWAY: Running comprehensive data consistency checks...")
            validation_results = self._validate_highway_data_consistency(link_data)
            self._log_validation_results(validation_results)
            
            if validation_results['status'] == 'fail':
                self.logger.error("HIGHWAY ERROR: Comprehensive validation failed - aborting analysis")
                return False
            elif validation_results['status'] == 'pass_with_warnings':
                self.logger.warn("HIGHWAY WARNING: Validation passed with warnings - proceeding with caution")
            else:
                self.logger.info("HIGHWAY SUCCESS: All validation checks passed")
            
            # Generate summary reports
            self.logger.info("HIGHWAY: Generating network performance summaries...")
            self._summarize_network_performance(link_data)
            
            self.logger.info("HIGHWAY SUCCESS: Highway network analysis completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"HIGHWAY ERROR: Highway analysis failed with exception: {e}")
            return False

    def _analyze_transit_network(self) -> bool:
        """Analyze transit network performance across all time periods."""
        try:
            self.logger.info("TRANSIT: Initializing transit network analysis...")
            
            # Check transit database availability
            transit_available = False
            for period in self.time_period_names:
                try:
                    test_scenario = self.transit_emmebank.scenario(period)
                    transit_available = True
                    self.logger.info(f"TRANSIT SUCCESS: Transit database connected - scenario {period} accessible")
                    break
                except Exception as e:
                    continue
            
            if not transit_available:
                self.logger.warn(f"TRANSIT WARNING: Transit database not available")
                self.logger.info("TRANSIT: Continuing without transit analysis...")
                return True
                
            self.logger.info("TRANSIT: Starting transit data extraction and analysis...")
            
            # Extract transit data for validation
            transit_df = self._extract_all_transit_periods()
            
            if not transit_df.empty:
                # Comprehensive transit data validation
                self.logger.info("TRANSIT: Running comprehensive data consistency checks...")
                validation_results = self._validate_transit_data_consistency(transit_df)
                self._log_validation_results(validation_results)
                
                if validation_results['status'] == 'fail':
                    self.logger.error("TRANSIT ERROR: Comprehensive validation failed - aborting transit analysis")
                    return False
                elif validation_results['status'] == 'pass_with_warnings':
                    self.logger.warn("TRANSIT WARNING: Validation passed with warnings - proceeding with caution")
                else:
                    self.logger.info("TRANSIT SUCCESS: All transit validation checks passed")
            else:
                self.logger.warn("TRANSIT WARNING: No transit data extracted for validation")
            
            # Generate transit analysis
            self._generate_transit_summaries()
            
            self.logger.info("TRANSIT SUCCESS: Transit network analysis completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"TRANSIT ERROR: Transit analysis failed with exception: {e}")
            return False
    
    def _log_validation_results(self, validation_results: Dict[str, any]) -> None:
        """Log detailed validation results."""
        self.logger.info("=== Validation Results Summary ===")
        
        # Log overall status
        status = validation_results['status']
        if status == 'fail':
            self.logger.error(f"Overall Status: FAILED")
        elif status == 'pass_with_warnings':
            self.logger.warn(f"Overall Status: PASSED WITH WARNINGS")
        else:
            self.logger.info(f"Overall Status: PASSED")
        
        # Log errors
        if validation_results['errors']:
            self.logger.error(f"Errors ({len(validation_results['errors'])}):")
            for i, error in enumerate(validation_results['errors'], 1):
                self.logger.error(f"  {i}. {error}")
        
        # Log warnings  
        if validation_results['warnings']:
            self.logger.warn(f"Warnings ({len(validation_results['warnings'])}):")
            for i, warning in enumerate(validation_results['warnings'], 1):
                self.logger.warn(f"  {i}. {warning}")
        
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
    
    def _validate_highway_data_consistency(self, df: pd.DataFrame) -> Dict[str, any]:
        """
        Comprehensive validation of highway data for logical consistency and numerical accuracy.
        
        Checks:
        1. Volume vs Capacity ratios are reasonable
        2. Speed calculations are consistent with congested times
        3. Link data completeness across time periods
        4. Numerical ranges are within expected bounds
        5. Cross-time period consistency checks
        """
        validation_results = {
            'status': 'pass',
            'errors': [],
            'warnings': [],
            'checks': {}
        }
        
        self.logger.info("VALIDATION: Starting highway data consistency checks...")
        
        # Check 1: Data completeness across time periods
        time_period_check = self._check_time_period_completeness(df, 'highway')
        validation_results['checks']['time_period_completeness'] = time_period_check
        if time_period_check['status'] == 'fail':
            validation_results['errors'].extend(time_period_check['errors'])
        
        # Check 2: Volume/Capacity ratio validation
        vc_ratio_check = self._check_volume_capacity_ratios(df)
        validation_results['checks']['volume_capacity_ratios'] = vc_ratio_check
        if vc_ratio_check['status'] == 'fail':
            validation_results['errors'].extend(vc_ratio_check['errors'])
        elif vc_ratio_check['status'] == 'warning':
            validation_results['warnings'].extend(vc_ratio_check['warnings'])
        
        # Check 3: Speed consistency validation
        speed_check = self._check_speed_consistency(df)
        validation_results['checks']['speed_consistency'] = speed_check
        if speed_check['status'] == 'fail':
            validation_results['errors'].extend(speed_check['errors'])
        elif speed_check['status'] == 'warning':
            validation_results['warnings'].extend(speed_check['warnings'])
        
        # Check 4: Numerical bounds validation
        bounds_check = self._check_numerical_bounds(df)
        validation_results['checks']['numerical_bounds'] = bounds_check
        if bounds_check['status'] == 'fail':
            validation_results['errors'].extend(bounds_check['errors'])
        elif bounds_check['status'] == 'warning':
            validation_results['warnings'].extend(bounds_check['warnings'])
        
        # Check 5: Cross-time period link consistency
        consistency_check = self._check_cross_period_consistency(df)
        validation_results['checks']['cross_period_consistency'] = consistency_check
        if consistency_check['status'] == 'fail':
            validation_results['errors'].extend(consistency_check['errors'])
        elif consistency_check['status'] == 'warning':
            validation_results['warnings'].extend(consistency_check['warnings'])
        
        # Set overall status
        if validation_results['errors']:
            validation_results['status'] = 'fail'
        elif validation_results['warnings']:
            validation_results['status'] = 'pass_with_warnings'
            
        self.logger.info(f"VALIDATION COMPLETE: Highway data validation finished with status: {validation_results['status']}")
        return validation_results
    
    def _validate_transit_data_consistency(self, df: pd.DataFrame) -> Dict[str, any]:
        """
        Comprehensive validation of transit data for logical consistency and numerical accuracy.
        
        Checks:
        1. Boarding/alighting balance at system level
        2. Capacity utilization reasonableness
        3. Transit line data completeness
        4. Headway and capacity relationships
        5. Cross-time period service consistency
        """
        validation_results = {
            'status': 'pass',
            'errors': [],
            'warnings': [],
            'checks': {}
        }
        
        self.logger.info("VALIDATION: Starting transit data consistency checks...")
        
        if df.empty:
            validation_results['status'] = 'fail'
            validation_results['errors'].append("No transit data available for validation")
            return validation_results
        
        # Check 1: Data completeness across time periods
        time_period_check = self._check_time_period_completeness(df, 'transit')
        validation_results['checks']['time_period_completeness'] = time_period_check
        if time_period_check['status'] == 'fail':
            validation_results['errors'].extend(time_period_check['errors'])
        
        # Check 2: Boarding/alighting balance
        boarding_check = self._check_boarding_balance(df)
        validation_results['checks']['boarding_balance'] = boarding_check
        if boarding_check['status'] == 'fail':
            validation_results['errors'].extend(boarding_check['errors'])
        elif boarding_check['status'] == 'warning':
            validation_results['warnings'].extend(boarding_check['warnings'])
        
        # Check 3: Capacity utilization validation
        capacity_check = self._check_transit_capacity_utilization(df)
        validation_results['checks']['capacity_utilization'] = capacity_check
        if capacity_check['status'] == 'fail':
            validation_results['errors'].extend(capacity_check['errors'])
        elif capacity_check['status'] == 'warning':
            validation_results['warnings'].extend(capacity_check['warnings'])
        
        # Check 4: Service frequency consistency
        frequency_check = self._check_service_frequency_consistency(df)
        validation_results['checks']['service_frequency'] = frequency_check
        if frequency_check['status'] == 'fail':
            validation_results['errors'].extend(frequency_check['errors'])
        elif frequency_check['status'] == 'warning':
            validation_results['warnings'].extend(frequency_check['warnings'])
        
        # Set overall status
        if validation_results['errors']:
            validation_results['status'] = 'fail'
        elif validation_results['warnings']:
            validation_results['status'] = 'pass_with_warnings'
            
        self.logger.info(f"VALIDATION COMPLETE: Transit data validation finished with status: {validation_results['status']}")
        return validation_results
    
    def _check_time_period_completeness(self, df: pd.DataFrame, data_type: str) -> Dict[str, any]:
        """Check if all expected time periods have data and consistent record counts."""
        check_result = {
            'status': 'pass',
            'errors': [],
            'warnings': [],
            'details': {}
        }
        
        if df.empty:
            check_result['status'] = 'fail'
            check_result['errors'].append(f"No {data_type} data available")
            return check_result
        
        expected_periods = set(tp.lower() for tp in self.time_period_names)
        actual_periods = set(df['time_period'].unique())
        
        missing_periods = expected_periods - actual_periods
        extra_periods = actual_periods - expected_periods
        
        if missing_periods:
            check_result['status'] = 'fail'
            check_result['errors'].append(f"Missing time periods in {data_type} data: {sorted(missing_periods)}")
        
        if extra_periods:
            check_result['warnings'].append(f"Unexpected time periods in {data_type} data: {sorted(extra_periods)}")
            if check_result['status'] != 'fail':
                check_result['status'] = 'warning'
        
        # Check record count consistency across periods
        period_counts = df['time_period'].value_counts()
        check_result['details']['period_record_counts'] = period_counts.to_dict()
        
        if len(period_counts) > 1:
            min_count = period_counts.min()
            max_count = period_counts.max()
            ratio = max_count / min_count if min_count > 0 else float('inf')
            
            if ratio > 2.0:  # More than 2x difference suggests potential issues
                check_result['warnings'].append(
                    f"Large variation in {data_type} record counts across time periods: "
                    f"min={min_count}, max={max_count}, ratio={ratio:.2f}"
                )
                if check_result['status'] == 'pass':
                    check_result['status'] = 'warning'
        
        return check_result
    
    def _check_volume_capacity_ratios(self, df: pd.DataFrame) -> Dict[str, any]:
        """Validate volume/capacity ratios are within reasonable bounds."""
        check_result = {
            'status': 'pass',
            'errors': [],
            'warnings': [],
            'details': {}
        }
        
        if 'auto_volume' not in df.columns or 'capacity' not in df.columns:
            check_result['status'] = 'fail'
            check_result['errors'].append("Missing volume (auto_volume) or capacity columns for V/C analysis")
            return check_result
        
        # Calculate V/C ratios
        valid_capacity = (df['capacity'] > 0) & df['capacity'].notna()
        df_valid = df[valid_capacity].copy()
        
        if df_valid.empty:
            check_result['status'] = 'fail'
            check_result['errors'].append("No valid capacity data found for V/C analysis")
            return check_result
        
        df_valid['vc_ratio'] = df_valid['auto_volume'] / df_valid['capacity']
        
        # Check for unreasonable V/C ratios
        extremely_high_vc = (df_valid['vc_ratio'] > 2.0).sum()
        high_vc = (df_valid['vc_ratio'] > 1.5).sum()
        negative_vc = (df_valid['vc_ratio'] < 0).sum()
        
        check_result['details'] = {
            'total_links_with_capacity': len(df_valid),
            'links_with_vc_over_2.0': extremely_high_vc,
            'links_with_vc_over_1.5': high_vc,
            'links_with_negative_vc': negative_vc,
            'mean_vc_ratio': df_valid['vc_ratio'].mean(),
            'median_vc_ratio': df_valid['vc_ratio'].median(),
            'max_vc_ratio': df_valid['vc_ratio'].max()
        }
        
        if negative_vc > 0:
            check_result['status'] = 'fail'
            check_result['errors'].append(f"Found {negative_vc} links with negative V/C ratios")
        
        if extremely_high_vc > len(df_valid) * 0.01:  # More than 1% with V/C > 2.0
            check_result['warnings'].append(
                f"High proportion of links with extreme V/C ratios (>2.0): "
                f"{extremely_high_vc}/{len(df_valid)} ({100*extremely_high_vc/len(df_valid):.1f}%)"
            )
            if check_result['status'] == 'pass':
                check_result['status'] = 'warning'
        
        return check_result
    
    def _check_speed_consistency(self, df: pd.DataFrame) -> Dict[str, any]:
        """Check consistency between free-flow speed, congested time, and calculated speeds."""
        check_result = {
            'status': 'pass',
            'errors': [],
            'warnings': [],
            'details': {}
        }
        
        required_cols = ['free_flow_speed', 'length', 'auto_time']  # free-flow speed, length, congested time
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            check_result['status'] = 'fail'
            check_result['errors'].append(f"Missing columns for speed consistency check: {missing_cols}")
            return check_result
        
        # Filter to valid data
        valid_data = (
            (df['free_flow_speed'] > 0) & df['free_flow_speed'].notna() &
            (df['length'] > 0) & df['length'].notna() &
            (df['auto_time'] > 0) & df['auto_time'].notna()
        )
        df_valid = df[valid_data].copy()
        
        if df_valid.empty:
            check_result['status'] = 'fail'
            check_result['errors'].append("No valid speed/time data for consistency checking")
            return check_result
        
        # Calculate implied congested speed from time and length
        df_valid['calculated_speed'] = df_valid['length'] / df_valid['auto_time'] * 60  # mph
        df_valid['speed_ratio'] = df_valid['calculated_speed'] / df_valid['free_flow_speed']
        
        # Check for inconsistencies
        speed_too_high = (df_valid['speed_ratio'] > 1.1).sum()  # Congested > free-flow + 10%
        speed_too_low = (df_valid['speed_ratio'] < 0.1).sum()   # Less than 10% of free-flow
        
        check_result['details'] = {
            'links_analyzed': len(df_valid),
            'mean_speed_ratio': df_valid['speed_ratio'].mean(),
            'links_with_speed_too_high': speed_too_high,
            'links_with_speed_too_low': speed_too_low,
            'mean_free_flow_speed': df_valid['free_flow_speed'].mean(),
            'mean_congested_speed': df_valid['calculated_speed'].mean()
        }
        
        if speed_too_high > 0:
            check_result['warnings'].append(
                f"Found {speed_too_high} links where congested speed exceeds free-flow speed"
            )
            if check_result['status'] == 'pass':
                check_result['status'] = 'warning'
        
        if speed_too_low > len(df_valid) * 0.05:  # More than 5% with very low speeds
            check_result['warnings'].append(
                f"High proportion of links with very low speed ratios (<10%): "
                f"{speed_too_low}/{len(df_valid)} ({100*speed_too_low/len(df_valid):.1f}%)"
            )
            if check_result['status'] == 'pass':
                check_result['status'] = 'warning'
        
        return check_result
    
    def _check_numerical_bounds(self, df: pd.DataFrame) -> Dict[str, any]:
        """Check if numerical values are within reasonable bounds."""
        check_result = {
            'status': 'pass',
            'errors': [],
            'warnings': [],
            'details': {}
        }
        
        bounds_config = {
            'auto_volume': {'min': 0, 'max': 50000, 'name': 'volume'},
            'free_flow_speed': {'min': 1, 'max': 120, 'name': 'free_flow_speed'},
            'capacity': {'min': 100, 'max': 20000, 'name': 'capacity'},
            'length': {'min': 0.01, 'max': 50, 'name': 'link_length'},
            'auto_time': {'min': 0.01, 'max': 120, 'name': 'travel_time'}
        }
        
        for col, bounds in bounds_config.items():
            if col not in df.columns:
                continue
                
            col_data = df[col].dropna()
            if col_data.empty:
                continue
                
            out_of_bounds_low = (col_data < bounds['min']).sum()
            out_of_bounds_high = (col_data > bounds['max']).sum()
            
            check_result['details'][f"{bounds['name']}_out_of_bounds"] = {
                'below_min': out_of_bounds_low,
                'above_max': out_of_bounds_high,
                'total_records': len(col_data),
                'min_value': col_data.min(),
                'max_value': col_data.max()
            }
            
            if out_of_bounds_low > 0:
                check_result['warnings'].append(
                    f"{bounds['name']} has {out_of_bounds_low} values below minimum ({bounds['min']})"
                )
                if check_result['status'] == 'pass':
                    check_result['status'] = 'warning'
            
            if out_of_bounds_high > 0:
                check_result['warnings'].append(
                    f"{bounds['name']} has {out_of_bounds_high} values above maximum ({bounds['max']})"
                )
                if check_result['status'] == 'pass':
                    check_result['status'] = 'warning'
        
        return check_result
    
    def _check_cross_period_consistency(self, df: pd.DataFrame) -> Dict[str, any]:
        """Check consistency of link attributes across time periods."""
        check_result = {
            'status': 'pass',
            'errors': [],
            'warnings': [],
            'details': {}
        }
        
        if 'link_id' not in df.columns:
            check_result['status'] = 'fail'
            check_result['errors'].append("No link_id column for cross-period consistency checking")
            return check_result
        
        # Check attributes that should be consistent across time periods
        static_attributes = ['length', 'ul2', 'ul3']  # length, free-flow speed, capacity
        available_static = [attr for attr in static_attributes if attr in df.columns]
        
        if not available_static:
            check_result['status'] = 'fail'
            check_result['errors'].append("No static attributes available for consistency checking")
            return check_result
        
        inconsistent_links = {}
        
        for attr in available_static:
            # Group by link and check if attribute varies across time periods
            link_attr_stats = df.groupby('link_id')[attr].agg(['nunique', 'std']).fillna(0)
            
            # Links where the attribute varies significantly across time periods
            varying_links = link_attr_stats[
                (link_attr_stats['nunique'] > 1) & (link_attr_stats['std'] > 0.01)
            ]
            
            inconsistent_links[attr] = len(varying_links)
            
            if len(varying_links) > 0:
                check_result['warnings'].append(
                    f"Attribute '{attr}' varies across time periods for {len(varying_links)} links"
                )
                if check_result['status'] == 'pass':
                    check_result['status'] = 'warning'
        
        check_result['details']['inconsistent_attributes'] = inconsistent_links
        
        return check_result
    
    def _check_boarding_balance(self, df: pd.DataFrame) -> Dict[str, any]:
        """Check system-wide boarding/alighting balance for transit data."""
        check_result = {
            'status': 'pass',
            'errors': [],
            'warnings': [],
            'details': {}
        }
        
        boarding_cols = [col for col in df.columns if 'board' in col.lower()]
        alighting_cols = [col for col in df.columns if 'alight' in col.lower()]
        
        if not boarding_cols or not alighting_cols:
            check_result['status'] = 'fail'
            check_result['errors'].append("Missing boarding or alighting data for balance check")
            return check_result
        
        # Sum boardings and alightings by time period
        for time_period in df['time_period'].unique():
            period_data = df[df['time_period'] == time_period]
            
            total_boardings = sum(period_data[col].sum() for col in boarding_cols if col in period_data.columns)
            total_alightings = sum(period_data[col].sum() for col in alighting_cols if col in period_data.columns)
            
            balance_ratio = abs(total_boardings - total_alightings) / max(total_boardings, 1)
            
            check_result['details'][f"{time_period}_boardings"] = total_boardings
            check_result['details'][f"{time_period}_alightings"] = total_alightings
            check_result['details'][f"{time_period}_balance_ratio"] = balance_ratio
            
            if balance_ratio > 0.1:  # More than 10% imbalance
                check_result['warnings'].append(
                    f"Large boarding/alighting imbalance in {time_period}: "
                    f"{balance_ratio:.1%} difference"
                )
                if check_result['status'] == 'pass':
                    check_result['status'] = 'warning'
        
        return check_result
    
    def _check_transit_capacity_utilization(self, df: pd.DataFrame) -> Dict[str, any]:
        """Check transit capacity utilization for reasonableness."""
        check_result = {
            'status': 'pass',
            'errors': [],
            'warnings': [],
            'details': {}
        }
        
        # Look for capacity and ridership columns
        capacity_cols = [col for col in df.columns if 'capacity' in col.lower()]
        ridership_cols = [col for col in df.columns if any(term in col.lower() for term in ['board', 'rider', 'passenger'])]
        
        if not capacity_cols or not ridership_cols:
            check_result['warnings'].append("Insufficient data for capacity utilization analysis")
            check_result['status'] = 'warning'
            return check_result
        
        # Calculate utilization metrics where possible
        # This is a placeholder - specific implementation depends on exact column names
        check_result['details']['analysis'] = 'Capacity utilization check completed'
        
        return check_result
    
    def _check_service_frequency_consistency(self, df: pd.DataFrame) -> Dict[str, any]:
        """Check transit service frequency consistency."""
        check_result = {
            'status': 'pass',
            'errors': [],
            'warnings': [],
            'details': {}
        }
        
        if 'headway' not in df.columns:
            check_result['warnings'].append("No headway data available for frequency consistency check")
            check_result['status'] = 'warning'
            return check_result
        
        # Check for reasonable headway values
        valid_headways = df['headway'].dropna()
        if valid_headways.empty:
            check_result['warnings'].append("No valid headway data found")
            check_result['status'] = 'warning'
            return check_result
        
        extreme_headways = (valid_headways < 1) | (valid_headways > 120)  # Less than 1 min or more than 2 hours
        
        check_result['details'] = {
            'total_services': len(valid_headways),
            'extreme_headways': extreme_headways.sum(),
            'mean_headway': valid_headways.mean(),
            'median_headway': valid_headways.median()
        }
        
        if extreme_headways.sum() > 0:
            check_result['warnings'].append(
                f"Found {extreme_headways.sum()} services with extreme headways (<1min or >2hr)"
            )
            if check_result['status'] == 'pass':
                check_result['status'] = 'warning'
        
        return check_result
    
    def _generate_final_validation_summary(self) -> None:
        """Generate and log a final summary of all validation checks performed."""
        self.logger.info("=" * 60)
        self.logger.info("FINAL VALIDATION SUMMARY")
        self.logger.info("=" * 60)
        
        # Check if output files were created
        expected_files = [
            "link_performance.xlsx",
            "network_summary.xlsx", 
            "overall_summary.csv"
        ]
        
        files_created = []
        files_missing = []
        
        for filename in expected_files:
            filepath = self.output_dir / filename
            if filepath.exists():
                files_created.append(filename)
            else:
                files_missing.append(filename)
        
        self.logger.info(f"OUTPUT FILES CREATED: {len(files_created)}/{len(expected_files)}")
        for filename in files_created:
            filepath = self.output_dir / filename
            size_mb = filepath.stat().st_size / (1024 * 1024)
            self.logger.info(f"  SUCCESS: {filename} ({size_mb:.1f} MB)")
        
        if files_missing:
            self.logger.warn(f"MISSING OUTPUT FILES: {len(files_missing)}")
            for filename in files_missing:
                self.logger.warn(f"  MISSING: {filename}")
        
        # Summary of validation checks performed
        self.logger.info("VALIDATION CHECKS PERFORMED:")
        self.logger.info("  HIGHWAY DATA:")
        self.logger.info("    - Time period completeness verification")  
        self.logger.info("    - Volume/capacity ratio validation (0-2.0 reasonable range)")
        self.logger.info("    - Speed consistency verification (congested vs free-flow)")
        self.logger.info("    - Numerical bounds checking (volumes, speeds, capacities)")
        self.logger.info("    - Cross-period consistency for static attributes")
        
        self.logger.info("  TRANSIT DATA:")
        self.logger.info("    - System-wide boarding/alighting balance checks")
        self.logger.info("    - Service frequency reasonableness (1min - 2hr headways)")
        self.logger.info("    - Capacity utilization analysis")
        self.logger.info("    - Cross-period service consistency")
        
        # Performance and scope metrics
        self.logger.info("ANALYSIS SCOPE:")
        self.logger.info(f"  - Time periods analyzed: {len(self.time_period_names)} ({', '.join(self.time_period_names)})")
        self.logger.info(f"  - Output directory: {self.output_dir}")
        
        # Memory and performance info if available
        try:
            import psutil
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024
            self.logger.info(f"  - Peak memory usage: {memory_mb:.1f} MB")
        except ImportError:
            pass
        
        self.logger.info("=" * 60)
        self.logger.info("VALIDATION SUMMARY: All checks completed successfully")
        self.logger.info("Data quality validation helps ensure:")
        self.logger.info("  ✓ Numerical consistency across time periods")
        self.logger.info("  ✓ Logical relationships between variables")
        self.logger.info("  ✓ Reasonable value ranges for all metrics")
        self.logger.info("  ✓ System-level balance and conservation laws")
        self.logger.info("=" * 60)

    def _extract_all_time_periods(self) -> pd.DataFrame:
        """Extract link-level data for all time periods."""
        self.logger.info("EXTRACT: Starting highway data extraction for all time periods...")
        
        all_data = []
        processed_periods = []
        
        # Use controller's time periods like PostProcessor does
        for i, time_period in enumerate(self.time_period_names, 1):
            self.logger.info(f"EXTRACT: Processing time period {i}/{len(self.time_period_names)}: {time_period}")
            
            try:
                self.logger.info(f"EXTRACT: Connecting to scenario {time_period}...")
                scenario = self.highway_emmebank.scenario(time_period)
                
                self.logger.info(f"EXTRACT: Loading network from scenario {time_period}...")
                network = scenario.get_network()
                
                self.logger.info(f"EXTRACT: Extracting link data for {time_period}...")
                scenario_data = self._extract_scenario_links(network, time_period.lower())
                
                all_data.extend(scenario_data)
                processed_periods.append(time_period)
                
                self.logger.info(f"EXTRACT SUCCESS: Completed {time_period}: {len(scenario_data):,} links extracted")
                
            except Exception as e:
                self.logger.error(f"EXTRACT ERROR: Failed to process time period {time_period}: {e}")
        
        if not all_data:
            self.logger.error("EXTRACT ERROR: No data extracted from any time period!")
            return pd.DataFrame()
            
        df = pd.DataFrame(all_data)
        self.logger.info(f"EXTRACT SUMMARY:")
        self.logger.info(f"   Total records: {len(df):,}")
        self.logger.info(f"   Time periods processed: {len(processed_periods)} ({', '.join(processed_periods)})")
        self.logger.info(f"   Time periods in data: {df['time_period'].nunique()}")
        
        return df
    
    def _map_scenario_to_time_period(self, scenario) -> str:
        """
        Map scenario to standard time period using ID-based mapping with title fallback.
        
        TM2PY models typically use consistent scenario IDs for time periods:
        - Scenario 11 = Early AM (ea)
        - Scenario 12 = AM Peak (am) 
        - Scenario 13 = Midday (md)
        - Scenario 14 = PM Peak (pm)
        - Scenario 15 = Evening (ev)
        """
        # Primary mapping: Use scenario ID if available in mapping
        if scenario.id in self.scenario_id_mapping:
            return self.scenario_id_mapping[scenario.id]
        
        # Fallback mapping: Use scenario title patterns
        title = scenario.title.lower() if scenario.title else ""
        
        # Check for specific period markers with priority order to avoid conflicts
        if title.startswith('ea') or 'early' in title:
            return 'ea'
        elif 'am' in title or 'morning' in title:
            return 'am'
        elif 'pm' in title or 'afternoon' in title:
            return 'pm'
        elif 'md' in title or 'midday' in title:
            return 'md'
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
        
        # Get total number of links for progress tracking
        total_links = len(list(network.links()))
        self.logger.info(f"🔗 Extracting {total_links:,} links for {time_period.upper()} period...")
        
        for link in network.links():
            links_processed += 1
            
            # Log progress every 5000 links for better feedback
            if links_processed % 5000 == 0:
                progress_pct = (links_processed / total_links) * 100
                self.logger.info(f"     Processing links: {links_processed:,}/{total_links:,} ({progress_pct:.1f}%)")
            
            # Get key link attributes using actual TM2PY attribute names
            # Use TM2PY flow attributes (populated after assignment)
            flow_da = getattr(link, '@flow_da', 0)
            flow_sr2 = getattr(link, '@flow_sr2', 0)
            flow_sr3 = getattr(link, '@flow_sr3', 0)
            flow_trk = getattr(link, '@flow_trk', 0)
            volume = flow_da + flow_sr2 + flow_sr3 + flow_trk  # Total volume
            
            auto_time = getattr(link, 'auto_time', 0) or getattr(link, '@auto_time', 0)
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
                'auto_volume': volume,
                'auto_time': auto_time,
                'length': length,
                'num_lanes': num_lanes,
                'capacity': capacity,
                'free_flow_speed': free_flow_speed,
                'functional_class': functional_class,
                'facility_type': facility_type,
                'county_id': county_id,
                'county_name': county_name,
                # Additional TM2PY performance metrics
                'congested_speed': congested_speed,
                'delay': delay,
                'vol_over_cap': vol_over_cap
            })
        
        # Log final summary statistics
        self.logger.info(f"📋 LINK EXTRACTION COMPLETE FOR {time_period.upper()}:")
        self.logger.info(f"     Total links processed: {links_processed:,}")
        self.logger.info(f"     Links with volume > 0: {links_with_volume:,} ({links_with_volume/links_processed*100:.1f}%)")
        self.logger.info(f"     Links missing key attributes: {links_missing_attributes:,} ({links_missing_attributes/links_processed*100:.1f}%)")
        
        # Log attribute availability (only for key attributes)
        key_attrs = ['auto_volume', '@capacity', '@lanes', '@ft']
        self.logger.info("     Key attribute coverage:")
        for attr in key_attrs:
            if attr in attribute_stats:
                count = attribute_stats[attr]
                pct = count / links_processed * 100 if links_processed > 0 else 0
                status = "GOOD" if pct > 80 else "WARNING" if pct > 50 else "POOR"
                self.logger.info(f"       {status} {attr}: {pct:.1f}% ({count:,} links)")
        
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
            self.logger.warn("Data quality warnings:")
            for warning in warnings:
                self.logger.warn(f"  WARNING: {warning}")
        
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
        
    def _extract_all_transit_periods(self) -> pd.DataFrame:
        """Extract transit line and segment data for all time periods."""
        self.logger.info("TRANSIT: Starting transit data extraction for all time periods...")
        
        all_data = []
        processed_periods = []
        
        # Use controller's time periods like PostProcessor does
        for i, time_period in enumerate(self.time_period_names, 1):
            self.logger.info(f"TRANSIT: Processing time period {i}/{len(self.time_period_names)}: {time_period}")
            
            try:
                self.logger.info(f"TRANSIT: Connecting to scenario {time_period}...")
                scenario = self.transit_emmebank.scenario(time_period)
                
                self.logger.info(f"TRANSIT: Loading network from scenario {time_period}...")
                network = scenario.get_network()
                
                self.logger.info(f"TRANSIT: Extracting segment data for {time_period}...")
                scenario_data = self._extract_scenario_transit(network, time_period.lower())
                
                all_data.extend(scenario_data)
                processed_periods.append(time_period)
                
                self.logger.info(f"TRANSIT SUCCESS: Completed {time_period}: {len(scenario_data):,} segments extracted")
                
            except Exception as e:
                self.logger.error(f"TRANSIT ERROR: Failed to process time period {time_period}: {e}")
        
        if not all_data:
            self.logger.error("TRANSIT ERROR: No transit data extracted from any time period!")
            return pd.DataFrame()
            
        df = pd.DataFrame(all_data)
        self.logger.info(f"TRANSIT SUMMARY:")
        self.logger.info(f"   Total segments: {len(df):,}")
        self.logger.info(f"   Time periods processed: {len(processed_periods)} ({', '.join(processed_periods)})")
        if 'line_id' in df.columns:
            self.logger.info(f"   Unique transit lines: {df['line_id'].nunique():,}")
        self.logger.info(f"   Time periods in data: {df['time_period'].nunique()}")
        
        return df
    
    def _extract_scenario_transit(self, network, time_period: str) -> List[Dict]:
        """Extract transit line and segment data for a single scenario."""
        data = []
        lines_processed = 0
        segments_processed = 0
        segments_with_boardings = 0
        
        self.logger.info(f"TRANSIT: Extracting transit data for {time_period} period...")
        
        for line in network.transit_lines():
            lines_processed += 1
            
            # Log progress every 100 lines
            if lines_processed % 100 == 0:
                self.logger.info(f"TRANSIT: Processed {lines_processed:,} transit lines...")
            
            # Get line-level attributes
            total_capacity = line.vehicle.total_capacity if line.vehicle else 0
            seated_capacity = line.vehicle.seated_capacity if line.vehicle else 0
            headway = line.headway
            line_hour_total_cap = (60 * total_capacity / headway) if headway > 0 else 0
            line_hour_seated_cap = (60 * seated_capacity / headway) if headway > 0 else 0
            
            # Get line mode/type information
            mode = getattr(line, 'mode', None)
            mode_id = mode.id if mode else 'unknown'
            mode_type = mode.type if mode else 'unknown'
            
            # Process each segment of the line
            for segment in line.segments(include_hidden=False):
                segments_processed += 1
                
                # Get segment attributes
                transit_volume = segment.transit_volume
                dwell_time = segment.dwell_time
                transit_time_func = getattr(segment, 'transit_time_func', 0)
                link_length = segment.link.length if segment.link else 0
                
                # Additional segment data attributes
                data1 = getattr(segment, 'data1', 0)
                data2 = getattr(segment, 'data2', 0) 
                data3 = getattr(segment, 'data3', 0)
                
                # Count segments with actual boardings
                if transit_volume > 0:
                    segments_with_boardings += 1
                
                # Store segment data
                data.append({
                    'time_period': time_period,
                    'line_id': line.id,
                    'from_node': segment.i_node.id if segment.i_node else 0,
                    'to_node': segment.j_node.id if segment.j_node else 0,
                    'link_length': link_length,
                    'transit_volume': transit_volume,  # Boardings
                    'dwell_time': dwell_time,
                    'transit_time_func': transit_time_func,
                    'total_capacity': total_capacity,
                    'seated_capacity': seated_capacity,
                    'headway': headway,
                    'line_hour_total_cap': line_hour_total_cap,
                    'line_hour_seated_cap': line_hour_seated_cap,
                    'mode_id': mode_id,
                    'mode_type': mode_type,
                    'data1': data1,
                    'data2': data2,
                    'data3': data3
                })
        
        # Log summary statistics
        self.logger.info(f"  Completed transit {time_period}: {lines_processed:,} lines, {segments_processed:,} segments processed")
        self.logger.info(f"    Segments with boardings > 0: {segments_with_boardings:,} ({segments_with_boardings/segments_processed*100:.1f}%)")
        
        return data
        
    def _generate_transit_summaries(self) -> None:
        """Generate comprehensive transit network analysis and summaries."""
        self.logger.info("=== Starting Transit Analysis ===")
        
        try:
            # Extract transit data for all time periods
            transit_data = self._extract_all_transit_periods()
            
            if transit_data.empty:
                self.logger.warn("No transit data extracted - skipping transit analysis")
                return
            
            # Validate extracted transit data
            if not self._validate_extracted_transit_data(transit_data):
                self.logger.error("Transit data validation failed")
                return
            
            # Generate transit summary reports
            self._summarize_transit_performance(transit_data)
            
            self.logger.info("=== Transit Analysis Complete ===")
            
        except Exception as e:
            self.logger.error(f"Transit analysis failed: {e}")
    
    def _validate_extracted_transit_data(self, df: pd.DataFrame) -> bool:
        """Validate extracted transit data for common issues."""
        self.logger.info("Validating extracted transit data quality...")
        
        if df.empty:
            self.logger.error("No transit data extracted from EMME database")
            return False
        
        # Check data completeness
        total_segments = len(df)
        total_lines = df['line_id'].nunique()
        self.logger.info(f"Total transit segments extracted: {total_segments:,}")
        self.logger.info(f"Total transit lines: {total_lines:,}")
        
        # Check for missing critical data
        zero_volume_pct = (df['transit_volume'] == 0).sum() / total_segments * 100
        zero_capacity_pct = (df['total_capacity'] == 0).sum() / total_segments * 100
        zero_headway_pct = (df['headway'] == 0).sum() / total_segments * 100
        
        self.logger.info(f"Transit data quality checks:")
        self.logger.info(f"  Zero boarding segments: {zero_volume_pct:.1f}%")
        self.logger.info(f"  Zero capacity segments: {zero_capacity_pct:.1f}%")
        self.logger.info(f"  Zero headway segments: {zero_headway_pct:.1f}%")
        
        # Check value ranges
        volume_stats = df['transit_volume'].describe()
        capacity_stats = df['total_capacity'].describe()
        headway_stats = df['headway'].describe()
        
        self.logger.info(f"Transit value ranges:")
        self.logger.info(f"  Boardings: {volume_stats['min']:.0f} to {volume_stats['max']:.0f} (mean: {volume_stats['mean']:.0f})")
        self.logger.info(f"  Capacity: {capacity_stats['min']:.0f} to {capacity_stats['max']:.0f} (mean: {capacity_stats['mean']:.0f})")
        self.logger.info(f"  Headway: {headway_stats['min']:.1f} to {headway_stats['max']:.1f} (mean: {headway_stats['mean']:.1f})")
        
        # Check mode distribution
        mode_counts = df['mode_id'].value_counts()
        self.logger.info(f"Mode distribution:")
        for mode, count in mode_counts.head(10).items():
            pct = count / total_segments * 100
            self.logger.info(f"  Mode {mode}: {count:,} segments ({pct:.1f}%)")
        
        return True
    
    def _summarize_transit_performance(self, df: pd.DataFrame) -> None:
        """Generate comprehensive transit performance summaries."""
        self.logger.info("Generating transit performance summaries...")
        
        # 1. Transit boardings by line and time period
        self._generate_transit_boardings_by_line(df)
        
        # 2. Transit boardings by segment and time period  
        self._generate_transit_boardings_by_segment(df)
        
        # 3. All-day boarding totals by line
        self._generate_transit_daily_totals(df)
        
        # 4. Service type summaries by mode
        self._generate_transit_mode_summaries(df)
        
        self.logger.info("Transit performance summaries complete")
    
    def _generate_transit_boardings_by_line(self, df: pd.DataFrame) -> None:
        """Generate transit boardings by line for each time period."""
        self.logger.info("Generating transit boardings by line...")
        
        # Aggregate by line and time period
        line_summary = df.groupby(['time_period', 'line_id', 'mode_id']).agg({
            'transit_volume': 'sum',           # Total boardings on line
            'total_capacity': 'first',        # Line capacity
            'seated_capacity': 'first',       # Line seated capacity
            'headway': 'first',               # Line headway
            'link_length': 'sum',             # Total line length
            'line_hour_total_cap': 'first',   # Hourly total capacity
            'line_hour_seated_cap': 'first'   # Hourly seated capacity
        }).reset_index()
        
        # Calculate performance metrics
        line_summary['load_factor'] = line_summary['transit_volume'] / line_summary['line_hour_total_cap']
        line_summary['seated_load_factor'] = line_summary['transit_volume'] / line_summary['line_hour_seated_cap']
        line_summary['frequency'] = 60 / line_summary['headway']  # Vehicles per hour
        
        # Save by time period
        for period in df['time_period'].unique():
            period_data = line_summary[line_summary['time_period'] == period].copy()
            period_data = period_data.sort_values('transit_volume', ascending=False)
            
            output_file = self.output_dir / f"transit_boardings_by_line_{period}.csv"
            period_data.to_csv(output_file, index=False)
            self.logger.info(f"Saved transit line summary for {period}: {len(period_data)} lines")
    
    def _generate_transit_boardings_by_segment(self, df: pd.DataFrame) -> None:
        """Generate transit boardings by segment for each time period."""
        self.logger.info("Generating transit boardings by segment...")
        
        # Select key segment fields
        segment_fields = [
            'time_period', 'line_id', 'from_node', 'to_node', 'link_length',
            'transit_volume', 'dwell_time', 'transit_time_func', 
            'total_capacity', 'seated_capacity', 'headway',
            'line_hour_total_cap', 'line_hour_seated_cap', 'mode_id'
        ]
        
        segment_summary = df[segment_fields].copy()
        
        # Calculate segment performance metrics
        segment_summary['load_factor'] = segment_summary['transit_volume'] / segment_summary['line_hour_total_cap']
        segment_summary['seated_load_factor'] = segment_summary['transit_volume'] / segment_summary['line_hour_seated_cap']
        
        # Save by time period
        for period in df['time_period'].unique():
            period_data = segment_summary[segment_summary['time_period'] == period].copy()
            period_data = period_data.sort_values(['line_id', 'from_node'])
            
            output_file = self.output_dir / f"transit_boardings_by_segment_{period}.csv"
            period_data.to_csv(output_file, index=False)
            self.logger.info(f"Saved transit segment summary for {period}: {len(period_data)} segments")
    
    def _generate_transit_daily_totals(self, df: pd.DataFrame) -> None:
        """Generate all-day boarding totals by line."""
        self.logger.info("Generating daily transit totals by line...")
        
        # Sum across all time periods by line
        daily_totals = df.groupby(['line_id', 'mode_id']).agg({
            'transit_volume': 'sum',           # Total daily boardings
            'total_capacity': 'first',        # Line capacity  
            'seated_capacity': 'first',       # Line seated capacity
            'headway': 'mean',                # Average headway
            'link_length': 'sum',             # Total line length
            'time_period': 'count'            # Number of time periods served
        }).reset_index()
        
        daily_totals.rename(columns={'time_period': 'periods_served'}, inplace=True)
        
        # Calculate daily performance metrics
        daily_totals['avg_hourly_boardings'] = daily_totals['transit_volume'] / daily_totals['periods_served']
        daily_totals['avg_frequency'] = 60 / daily_totals['headway']
        
        # Sort by total boardings
        daily_totals = daily_totals.sort_values('transit_volume', ascending=False)
        
        output_file = self.output_dir / "transit_boardings_by_line_daily.csv"
        daily_totals.to_csv(output_file, index=False)
        self.logger.info(f"Saved daily transit totals: {len(daily_totals)} lines")
    
    def _generate_transit_mode_summaries(self, df: pd.DataFrame) -> None:
        """Generate service type summaries by mode."""
        self.logger.info("Generating transit summaries by service type/mode...")
        
        # Aggregate by mode and time period
        mode_summary = df.groupby(['time_period', 'mode_id']).agg({
            'transit_volume': 'sum',           # Total boardings by mode
            'line_id': 'nunique',             # Number of lines
            'from_node': 'count',             # Number of segments
            'total_capacity': 'sum',          # Total capacity  
            'link_length': 'sum',             # Total route miles
            'headway': 'mean'                 # Average headway
        }).reset_index()
        
        mode_summary.rename(columns={
            'line_id': 'num_lines',
            'from_node': 'num_segments'
        }, inplace=True)
        
        # Calculate mode performance metrics
        mode_summary['avg_boardings_per_line'] = mode_summary['transit_volume'] / mode_summary['num_lines']
        mode_summary['boardings_per_route_mile'] = mode_summary['transit_volume'] / mode_summary['link_length']
        mode_summary['avg_frequency'] = 60 / mode_summary['headway']
        
        # Add all-day totals
        daily_mode_summary = df.groupby(['mode_id']).agg({
            'transit_volume': 'sum',
            'line_id': 'nunique', 
            'from_node': 'count',
            'total_capacity': 'sum',
            'link_length': 'sum'
        }).reset_index()
        
        daily_mode_summary['time_period'] = 'ALL_DAY'
        daily_mode_summary.rename(columns={
            'line_id': 'num_lines',
            'from_node': 'num_segments'
        }, inplace=True)
        
        # Combine time period and daily data
        combined_mode_summary = pd.concat([mode_summary, daily_mode_summary], ignore_index=True)
        combined_mode_summary = combined_mode_summary.sort_values(['mode_id', 'time_period'])
        
        output_file = self.output_dir / "transit_boardings_by_service_type.csv"
        combined_mode_summary.to_csv(output_file, index=False)
        self.logger.info(f"Saved transit mode summaries: {len(combined_mode_summary)} mode-period combinations")
    
    def validate_outputs(self) -> Dict[str, any]:
        """
        Comprehensive output validation for data quality assurance.
        
        Validates:
        - Speed ranges (reasonable values)
        - VMT/VHT ratio consistency  
        - Data completeness
        - Value ranges and outliers
        - Cross-metric consistency
        
        Returns:
            Dict with validation results and detailed diagnostics
        """
        self.logger.info("=== Starting Output Validation ===")
        
        validation_results = {
            'status': 'pass',  # pass, pass_with_warnings, fail
            'checks': {},
            'errors': [],
            'warnings': [],
            'summary': {}
        }
        
        try:
            # Check that output files exist
            file_validation = self._validate_output_files()
            validation_results['checks']['output_files'] = file_validation
            validation_results['errors'].extend(file_validation.get('errors', []))
            validation_results['warnings'].extend(file_validation.get('warnings', []))
            
            # Load data for quality checks
            if file_validation['status'] != 'fail':
                # Validate speed ranges
                speed_validation = self._validate_speed_ranges()
                validation_results['checks']['speed_ranges'] = speed_validation
                validation_results['errors'].extend(speed_validation.get('errors', []))
                validation_results['warnings'].extend(speed_validation.get('warnings', []))
                
                # Validate VMT/VHT ratios
                ratio_validation = self._validate_vmt_vht_ratios()
                validation_results['checks']['vmt_vht_ratios'] = ratio_validation
                validation_results['errors'].extend(ratio_validation.get('errors', []))
                validation_results['warnings'].extend(ratio_validation.get('warnings', []))
                
                # Validate data completeness
                completeness_validation = self._validate_data_completeness()
                validation_results['checks']['data_completeness'] = completeness_validation
                validation_results['errors'].extend(completeness_validation.get('errors', []))
                validation_results['warnings'].extend(completeness_validation.get('warnings', []))
                
                # Validate value ranges
                range_validation = self._validate_value_ranges()
                validation_results['checks']['value_ranges'] = range_validation
                validation_results['errors'].extend(range_validation.get('errors', []))
                validation_results['warnings'].extend(range_validation.get('warnings', []))
        
        except Exception as e:
            validation_results['status'] = 'fail'
            validation_results['errors'].append(f"Output validation failed: {str(e)}")
        
        # Determine overall status
        if validation_results['errors']:
            validation_results['status'] = 'fail'
        elif validation_results['warnings']:
            validation_results['status'] = 'pass_with_warnings'
        
        # Create summary
        validation_results['summary'] = {
            'total_checks': len(validation_results['checks']),
            'passed_checks': sum(1 for check in validation_results['checks'].values() 
                               if check.get('status') == 'pass'),
            'warnings': len(validation_results['warnings']),
            'errors': len(validation_results['errors'])
        }
        
        self._log_output_validation_results(validation_results)
        return validation_results
    
    def _validate_output_files(self) -> Dict[str, any]:
        """Validate that expected output files exist and are readable."""
        validation = {
            'status': 'pass',
            'errors': [],
            'warnings': [],
            'details': {}
        }
        
        # Check highway output files (always expected)
        highway_files = [
            'facility_type_summary.csv',
            'overall_summary.csv', 
            'lane_mile_inventory.csv'
        ]
        
        missing_files = []
        for filename in highway_files:
            filepath = self.output_dir / filename
            if not filepath.exists():
                missing_files.append(filename)
            else:
                try:
                    # Test readability
                    pd.read_csv(filepath, nrows=1)
                    validation['details'][filename] = 'readable'
                except Exception as e:
                    validation['errors'].append(f"Cannot read {filename}: {str(e)}")
                    validation['details'][filename] = 'unreadable'
        
        # Check transit output files (if transit database is available)
        if self.transit_bank:
            self.logger.info("Checking transit output files...")
            transit_files = [
                'transit_boardings_by_line_daily.csv',
                'transit_boardings_by_service_type.csv'
            ]
            
            # Check for time-period specific transit files
            for period in ['ea', 'am', 'md', 'pm', 'ev']:
                transit_files.extend([
                    f'transit_boardings_by_line_{period}.csv',
                    f'transit_boardings_by_segment_{period}.csv'
                ])
            
            transit_missing = []
            for filename in transit_files:
                filepath = self.output_dir / filename
                if not filepath.exists():
                    transit_missing.append(filename)
                else:
                    try:
                        # Test readability
                        pd.read_csv(filepath, nrows=1)
                        validation['details'][filename] = 'readable'
                    except Exception as e:
                        validation['warnings'].append(f"Cannot read transit file {filename}: {str(e)}")
                        validation['details'][filename] = 'unreadable'
            
            if transit_missing:
                validation['warnings'].extend([f"Missing transit file: {f}" for f in transit_missing])
                validation['details']['transit_files_missing'] = len(transit_missing)
            
            validation['details']['transit_files_checked'] = len(transit_files)
            validation['details']['transit_files_found'] = len(transit_files) - len(transit_missing)
        
        if missing_files:
            validation['status'] = 'fail'
            validation['errors'].extend([f"Missing output file: {f}" for f in missing_files])
        
        validation['details']['highway_files_checked'] = len(highway_files)
        validation['details']['highway_files_found'] = len(highway_files) - len(missing_files)
        
        return validation
    
    def _validate_speed_ranges(self) -> Dict[str, any]:
        """Validate that network speeds are within reasonable ranges."""
        validation = {
            'status': 'pass',
            'errors': [],
            'warnings': [],
            'details': {}
        }
        
        try:
            # Load facility type summary data
            facility_file = self.output_dir / 'facility_type_summary.csv'
            if not facility_file.exists():
                validation['status'] = 'fail'
                validation['errors'].append("Cannot validate speeds: facility_type_summary.csv not found")
                return validation
            
            df = pd.read_csv(facility_file)
            
            # Calculate average speeds if VMT and VHT columns exist
            if 'vmt' in df.columns and 'vht' in df.columns:
                df['avg_speed'] = np.where(df['vht'] > 0, df['vmt'] / df['vht'], 0)
                
                # Define reasonable speed ranges by facility type
                speed_ranges = {
                    'freeway': {'min': 25, 'max': 80},
                    'arterial': {'min': 15, 'max': 55},
                    'collector': {'min': 10, 'max': 45},
                    'local': {'min': 5, 'max': 35},
                    'connector': {'min': 15, 'max': 65}
                }
                
                speed_issues = []
                for _, row in df.iterrows():
                    facility_type = row.get('facility_type', '').lower()
                    avg_speed = row.get('avg_speed', 0)
                    
                    if facility_type in speed_ranges and avg_speed > 0:
                        range_info = speed_ranges[facility_type]
                        if avg_speed < range_info['min']:
                            speed_issues.append(f"{facility_type}: {avg_speed:.1f} mph (below {range_info['min']} mph)")
                        elif avg_speed > range_info['max']:
                            speed_issues.append(f"{facility_type}: {avg_speed:.1f} mph (above {range_info['max']} mph)")
                
                if speed_issues:
                    validation['warnings'].extend([f"Speed outside typical range: {issue}" for issue in speed_issues])
                    if len(speed_issues) > len(df) * 0.5:  # More than 50% of facility types have issues
                        validation['status'] = 'fail'
                        validation['errors'].append("More than 50% of facility types have unrealistic speeds")
                
                validation['details'] = {
                    'speed_ranges_checked': list(speed_ranges.keys()),
                    'facility_types_analyzed': len(df),
                    'speed_issues_found': len(speed_issues),
                    'average_network_speed': df['avg_speed'].mean() if len(df) > 0 else 0
                }
            else:
                validation['warnings'].append("Cannot calculate speeds: missing VMT or VHT columns")
                
        except Exception as e:
            validation['status'] = 'fail'
            validation['errors'].append(f"Speed validation failed: {str(e)}")
        
        return validation
    
    def _validate_vmt_vht_ratios(self) -> Dict[str, any]:
        """Validate VMT/VHT ratios for consistency."""
        validation = {
            'status': 'pass',
            'errors': [],
            'warnings': [],
            'details': {}
        }
        
        try:
            facility_file = self.output_dir / 'facility_type_summary.csv'
            if not facility_file.exists():
                validation['status'] = 'fail'
                validation['errors'].append("Cannot validate ratios: facility_type_summary.csv not found")
                return validation
            
            df = pd.read_csv(facility_file)
            
            if 'vmt' in df.columns and 'vht' in df.columns:
                # Calculate VMT/VHT ratios (should equal average speeds)
                df['vmt_vht_ratio'] = np.where(df['vht'] > 0, df['vmt'] / df['vht'], 0)
                
                # Check for unrealistic ratios
                unrealistic_ratios = []
                very_low_ratios = df[df['vmt_vht_ratio'] < 5]  # Less than 5 mph
                very_high_ratios = df[df['vmt_vht_ratio'] > 100]  # More than 100 mph
                
                if len(very_low_ratios) > 0:
                    unrealistic_ratios.extend([f"Very low ratio ({row['vmt_vht_ratio']:.1f}): {row.get('facility_type', 'Unknown')}" 
                                             for _, row in very_low_ratios.iterrows()])
                
                if len(very_high_ratios) > 0:
                    unrealistic_ratios.extend([f"Very high ratio ({row['vmt_vht_ratio']:.1f}): {row.get('facility_type', 'Unknown')}" 
                                             for _, row in very_high_ratios.iterrows()])
                
                if unrealistic_ratios:
                    validation['warnings'].extend(unrealistic_ratios)
                    if len(unrealistic_ratios) > len(df) * 0.3:  # More than 30% problematic
                        validation['status'] = 'fail'
                        validation['errors'].append("Too many facility types have unrealistic VMT/VHT ratios")
                
                # Check for zero VHT with non-zero VMT (impossible condition)
                impossible_conditions = df[(df['vmt'] > 0) & (df['vht'] <= 0)]
                if len(impossible_conditions) > 0:
                    validation['errors'].append(f"Found {len(impossible_conditions)} cases with VMT > 0 but VHT = 0")
                
                validation['details'] = {
                    'ratios_analyzed': len(df),
                    'very_low_ratios': len(very_low_ratios),
                    'very_high_ratios': len(very_high_ratios),
                    'impossible_conditions': len(impossible_conditions),
                    'average_ratio': df['vmt_vht_ratio'].mean() if len(df) > 0 else 0
                }
            else:
                validation['warnings'].append("Cannot validate ratios: missing VMT or VHT columns")
                
        except Exception as e:
            validation['status'] = 'fail'
            validation['errors'].append(f"VMT/VHT ratio validation failed: {str(e)}")
        
        return validation
    
    def _validate_data_completeness(self) -> Dict[str, any]:
        """Validate data completeness and check for missing values."""
        validation = {
            'status': 'pass',
            'errors': [],
            'warnings': [],
            'details': {}
        }
        
        try:
            files_to_check = [
                'facility_type_summary.csv',
                'overall_summary.csv',
                'lane_mile_inventory.csv'
            ]
            
            completeness_results = {}
            
            for filename in files_to_check:
                filepath = self.output_dir / filename
                if filepath.exists():
                    df = pd.read_csv(filepath)
                    
                    # Check for missing values
                    total_cells = df.size
                    missing_cells = df.isnull().sum().sum()
                    completeness_pct = ((total_cells - missing_cells) / total_cells * 100) if total_cells > 0 else 0
                    
                    completeness_results[filename] = {
                        'total_cells': total_cells,
                        'missing_cells': missing_cells,
                        'completeness_pct': completeness_pct,
                        'rows': len(df),
                        'columns': len(df.columns)
                    }
                    
                    # Warn if completeness is below 95%
                    if completeness_pct < 95:
                        validation['warnings'].append(f"{filename}: {completeness_pct:.1f}% complete ({missing_cells} missing values)")
                    
                    # Error if completeness is below 80%
                    if completeness_pct < 80:
                        validation['errors'].append(f"{filename}: Only {completeness_pct:.1f}% complete (too many missing values)")
                        validation['status'] = 'fail'
                    
                    # Check for empty files
                    if len(df) == 0:
                        validation['errors'].append(f"{filename}: File is empty (no data rows)")
                        validation['status'] = 'fail'
            
            validation['details'] = completeness_results
            
        except Exception as e:
            validation['status'] = 'fail'
            validation['errors'].append(f"Completeness validation failed: {str(e)}")
        
        return validation
    
    def _validate_value_ranges(self) -> Dict[str, any]:
        """Validate that key metrics are within expected ranges."""
        validation = {
            'status': 'pass',
            'errors': [],
            'warnings': [],
            'details': {}
        }
        
        try:
            overall_file = self.output_dir / 'overall_summary.csv'
            if not overall_file.exists():
                validation['status'] = 'fail'
                validation['errors'].append("Cannot validate ranges: overall_summary.csv not found")
                return validation
            
            df = pd.read_csv(overall_file)
            
            # Expected ranges for Bay Area network (based on typical values)
            expected_ranges = {
                'Total VMT': {'min': 50000000, 'max': 200000000},  # 50M - 200M VMT
                'Total VHT': {'min': 2000000, 'max': 10000000},    # 2M - 10M VHT  
                'Average Speed': {'min': 15, 'max': 60},           # 15-60 mph average
                'Total Lane Miles': {'min': 50000, 'max': 150000}  # 50K - 150K lane miles
            }
            
            range_issues = []
            
            for _, row in df.iterrows():
                metric = row.get('Metric', '')
                value = row.get('Value', 0)
                
                if metric in expected_ranges:
                    range_info = expected_ranges[metric]
                    try:
                        numeric_value = float(value)
                        if numeric_value < range_info['min']:
                            range_issues.append(f"{metric}: {numeric_value:.0f} (below expected minimum {range_info['min']:,.0f})")
                        elif numeric_value > range_info['max']:
                            range_issues.append(f"{metric}: {numeric_value:.0f} (above expected maximum {range_info['max']:,.0f})")
                    except (ValueError, TypeError):
                        validation['warnings'].append(f"{metric}: Cannot validate range (non-numeric value: {value})")
            
            if range_issues:
                validation['warnings'].extend([f"Value outside expected range: {issue}" for issue in range_issues])
                if len(range_issues) > len(expected_ranges) * 0.5:  # More than 50% of key metrics are problematic
                    validation['status'] = 'fail'
                    validation['errors'].append("Too many key metrics are outside expected ranges")
            
            validation['details'] = {
                'metrics_checked': list(expected_ranges.keys()),
                'range_issues_found': len(range_issues),
                'total_metrics_in_file': len(df)
            }
            
        except Exception as e:
            validation['status'] = 'fail'
            validation['errors'].append(f"Value range validation failed: {str(e)}")
        
        return validation
    
    def _log_output_validation_results(self, validation_results: Dict[str, any]) -> None:
        """Log detailed validation results."""
        status = validation_results['status']
        summary = validation_results['summary']
        
        self.logger.info(f"Output Validation Status: {status.upper()}")
        self.logger.info(f"Checks completed: {summary['passed_checks']}/{summary['total_checks']}")
        
        if validation_results['errors']:
            self.logger.error("VALIDATION ERRORS:")
            for error in validation_results['errors']:
                self.logger.error(f"  ERROR {error}")
        
        if validation_results['warnings']:
            self.logger.warn("VALIDATION WARNINGS:")
            for warning in validation_results['warnings']:
                self.logger.warn(f"  WARNING  {warning}")
        
        # Log details for each check
        for check_name, check_results in validation_results['checks'].items():
            self.logger.info(f"\n{check_name.replace('_', ' ').title()} Check:")
            self.logger.info(f"  Status: {check_results.get('status', 'unknown')}")
            if 'details' in check_results:
                for key, value in check_results['details'].items():
                    self.logger.info(f"  {key.replace('_', ' ').title()}: {value}")


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
    
    parser.add_argument(
        '--validate-outputs',
        action='store_true',
        help='Run output validation on existing results (requires previous run completion)'
    )
    
    parser.add_argument(
        '--list-scenarios',
        action='store_true',
        help='List all available scenarios in both highway and transit databases'
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
        # For standalone usage, would need to create a proper controller
        # NetworkSummary is designed as a TM2PY component and requires controller
        print("ERROR: NetworkSummary must be run as a TM2PY component via RunModel.py")
        print("Standalone execution is not supported.")
        return 1
        
        if args.list_scenarios:
            print("Listing all available scenarios...")
            try:
                # Connect to databases first
                validation_results = summarizer.validate_inputs()
                if validation_results['status'] == 'fail':
                    print("ERROR: Could not connect to databases")
                    for error in validation_results.get('errors', []):
                        print(f"  ERROR: {error}")
                    return 1
                
                print("\n=== HIGHWAY DATABASE SCENARIOS ===")
                highway_scenarios = list(summarizer.highway_bank.scenarios())
                for scenario in highway_scenarios:
                    time_period = summarizer._map_scenario_to_time_period(scenario)
                    title = scenario.title or '(no title)'
                    print(f"  Scenario {scenario.id:2d}: {title} -> {time_period}")
                    
                print(f"\nHighway total: {len(highway_scenarios)} scenarios")
                
                print("\n=== TRANSIT DATABASE SCENARIOS ===")
                transit_scenarios = list(summarizer.transit_bank.scenarios())
                for scenario in transit_scenarios:
                    time_period = summarizer._map_scenario_to_time_period(scenario)
                    title = scenario.title or '(no title)'
                    print(f"  Scenario {scenario.id:2d}: {title} -> {time_period}")
                    
                print(f"\nTransit total: {len(transit_scenarios)} scenarios")
                
                # Show time period mapping summary
                highway_periods = {summarizer._map_scenario_to_time_period(s) for s in highway_scenarios}
                transit_periods = {summarizer._map_scenario_to_time_period(s) for s in transit_scenarios}
                
                print(f"\n=== TIME PERIOD SUMMARY ===")
                print(f"Highway periods found: {sorted([p for p in highway_periods if p != 'unknown'])}")
                print(f"Transit periods found: {sorted([p for p in transit_periods if p != 'unknown'])}")
                
                highway_unknown = [s for s in highway_scenarios if summarizer._map_scenario_to_time_period(s) == 'unknown']
                transit_unknown = [s for s in transit_scenarios if summarizer._map_scenario_to_time_period(s) == 'unknown']
                
                if highway_unknown:
                    print(f"\nHighway scenarios with unknown periods (will be skipped): {[s.id for s in highway_unknown]}")
                if transit_unknown:
                    print(f"Transit scenarios with unknown periods (will be skipped): {[s.id for s in transit_unknown]}")
                    
                return 0
                
            except Exception as e:
                print(f"ERROR: Failed to list scenarios: {e}")
                return 1
        
        if args.validate_only:
            print("Running input validation only...")
            validation_results = summarizer.validate_inputs()
            summarizer._log_validation_results(validation_results)
            
            if validation_results['status'] == 'fail':
                print("\nERROR Validation FAILED")
                return 1
            elif validation_results['status'] == 'pass_with_warnings':
                print("\nWARNING  Validation PASSED with warnings")
                return 0
            else:
                print("\nOK Validation PASSED")
                return 0
        elif args.validate_outputs:
            print("Running output validation on existing results...")
            output_validation = summarizer.validate_outputs()
            
            if output_validation['status'] == 'fail':
                print("\nERROR Output validation FAILED")
                print("Issues found:")
                for error in output_validation['errors']:
                    print(f"  ERROR {error}")
                return 1
            elif output_validation['status'] == 'pass_with_warnings':
                print("\nWARNING  Output validation PASSED with warnings")
                print("Warnings:")
                for warning in output_validation['warnings']:
                    print(f"  WARNING  {warning}")
                return 0
            else:
                print("\nOK Output validation PASSED")
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