#!/usr/bin/env python3
"""
Test script for the NetworkSummary TM2PY component.

This script creates a minimal TM2PY controller configuration to test
the NetworkSummary component with real model data.

Usage:
    python test_network_summary_component.py E:\2015-tm22-dev-sprint-04
"""

import sys
import logging
from pathlib import Path
from typing import Dict, Any

# Add tm2py to path
sys.path.insert(0, r'c:\GitHub\tm2py')

from tm2py.config import Configuration
from tm2py.controller import RunController
from tm2py.components.network_summary import NetworkSummary


def create_test_config(model_run_dir: str) -> Dict[str, Any]:
    """Create a minimal configuration for testing the NetworkSummary component."""
    
    model_path = Path(model_run_dir)
    
    config = {
        # Basic run configuration
        "run": {
            "start_iteration": 1,
            "end_iteration": 1,
            "run_dir": str(model_path.parent),
            "emme_project_dir": str(model_path / "emme_project"),
            "scenario": "test_network_summary"
        },
        
        # Time periods configuration (typical TM2PY structure)
        "time_periods": [
            {
                "name": "EA",
                "emme_scenario_id": 11,
                "length_hours": 3.0,
                "start_time": "03:00",
                "end_time": "06:00"
            },
            {
                "name": "AM", 
                "emme_scenario_id": 12,
                "length_hours": 4.0,
                "start_time": "06:00", 
                "end_time": "10:00"
            },
            {
                "name": "MD",
                "emme_scenario_id": 13, 
                "length_hours": 5.0,
                "start_time": "10:00",
                "end_time": "15:00"
            },
            {
                "name": "PM",
                "emme_scenario_id": 14,
                "length_hours": 4.0, 
                "start_time": "15:00",
                "end_time": "19:00"
            },
            {
                "name": "EV",
                "emme_scenario_id": 15,
                "length_hours": 8.0,
                "start_time": "19:00",
                "end_time": "03:00"
            }
        ],
        
        # EMME configuration
        "emme": {
            "highway_database_path": str(model_path / "emme_project" / "Database_highway"),
            "transit_database_path": str(model_path / "emme_project" / "Database_transit"),
            "num_processors": 1,
            "delete_matrix_files": False,
            "delete_result_matrices": False
        }
    }
    
    return config


def main():
    """Test the NetworkSummary component."""
    if len(sys.argv) < 2:
        print("Usage: python test_network_summary_component.py <model_run_dir>")
        print("Example: python test_network_summary_component.py E:\\2015-tm22-dev-sprint-04")
        return 1
    
    model_run_dir = sys.argv[1]
    model_path = Path(model_run_dir)
    
    if not model_path.exists():
        print(f"ERROR: Model run directory does not exist: {model_path}")
        return 1
    
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger(__name__)
    logger.info("Starting NetworkSummary component test")
    logger.info(f"Model run directory: {model_path}")
    
    try:
        # Create test configuration
        config_dict = create_test_config(model_run_dir)
        
        # Create TM2PY configuration object  
        config = Configuration.from_dict(config_dict)
        
        # Create controller (this will handle EMME connections)
        controller = RunController(config)
        
        # Initialize the NetworkSummary component
        logger.info("Initializing NetworkSummary component...")
        network_summary = NetworkSummary(controller)
        
        # Test component validation
        logger.info("Running component input validation...")
        validation_results = network_summary.validate_inputs()
        
        if validation_results['status'] == 'fail':
            logger.error("Component validation failed")
            for error in validation_results.get('errors', []):
                logger.error(f"  ERROR: {error}")
            return 1
        elif validation_results['status'] == 'pass_with_warnings':
            logger.warning("Component validation passed with warnings")
            for warning in validation_results.get('warnings', []):
                logger.warning(f"  WARNING: {warning}")
        else:
            logger.info("Component validation passed successfully")
        
        # Test time period mapping
        logger.info("Testing time period configuration...")
        logger.info(f"Time periods: {network_summary.time_period_names}")
        logger.info(f"Time period mapping: {network_summary._tp_mapping}")
        
        # Test database connections
        logger.info("Testing database connections...")
        try:
            highway_scenarios = list(network_summary.highway_emmebank.scenarios())
            logger.info(f"Highway database: {len(highway_scenarios)} scenarios found")
            
            transit_scenarios = list(network_summary.transit_emmebank.scenarios())
            logger.info(f"Transit database: {len(transit_scenarios)} scenarios found")
            
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return 1
        
        # Run the component
        logger.info("Running NetworkSummary component...")
        success = network_summary.run()
        
        if success:
            logger.info("✅ NetworkSummary component completed successfully!")
            logger.info(f"Results saved to: {network_summary.output_dir}")
        else:
            logger.error("❌ NetworkSummary component failed")
            return 1
        
        return 0
        
    except Exception as e:
        logger.error(f"Test failed with exception: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())