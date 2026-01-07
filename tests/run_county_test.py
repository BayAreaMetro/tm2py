"""Quick Test Script for County Highway Framework

This script helps you test the county highway assignment framework by:
1. Checking prerequisites
2. Setting up a minimal test directory
3. Running a basic highway test

Usage:
    From EMME Python environment:
    
    # Edit tests/county_test_config.toml with your paths and settings, then run:
    python tests/run_county_test.py
    
    # Or specify a different config file:
    python tests/run_county_test.py --config tests/my_custom_config.toml
"""

import argparse
import shutil
import sys
import toml
import logging
from pathlib import Path
from datetime import datetime
import io

# Force UTF-8 encoding for console output on Windows
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))


def setup_console_logging(log_level='INFO'):
    """Setup console-only logging."""
    logger = logging.getLogger('county_test')
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()  # Clear any existing handlers
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, log_level.upper()))
    console_formatter = logging.Formatter('%(levelname)s: %(message)s')
    console_handler.setFormatter(console_formatter)
    
    logger.addHandler(console_handler)
    return logger


def add_file_logging(logger, output_dir):
    """Add file handler to existing logger after test directory is set up."""
    log_dir = Path(output_dir) / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"county_test_{timestamp}.log"
    
    # File handler (detailed)
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    
    logger.addHandler(file_handler)
    logger.info(f"File logging started: {log_file}")
    
    return log_file

def check_prerequisites(config, logger):
    """Check if all required files and directories exist."""
    logger.info("="*70)
    logger.info("CHECKING PREREQUISITES")
    logger.info("="*70)
    
    issues = []
    warnings = []
    
    county_name = config['test']['county_name']
    logger.info(f"County: {county_name}")
    
    # Check source dataset
    source_dir = Path(config['paths']['source_dataset'])
    logger.debug(f"Checking source dataset: {source_dir}")
    if not source_dir.exists():
        issues.append(f"Source dataset not found: {source_dir}")
        logger.error(f"Source dataset not found: {source_dir}")
    else:
        logger.info(f"✓ Source dataset found: {source_dir}")
    
    # Check EMME project
    emme_project = source_dir / "emme_project"
    logger.debug(f"Checking EMME project: {emme_project}")
    if not emme_project.exists():
        issues.append(f"EMME project not found: {emme_project}")
        logger.error(f"EMME project not found: {emme_project}")
    else:
        logger.info(f"✓ EMME project found: {emme_project}")
    
    # Check EMME database
    emme_db = emme_project / "Database_highway" / "emmebank"
    logger.debug(f"Checking EMME database: {emme_db}")
    if not emme_db.exists():
        issues.append(f"EMME database not found: {emme_db}")
        logger.error(f"EMME database not found: {emme_db}")
    else:
        logger.info(f"✓ EMME database found: {emme_db}")
    
    # Check for required input files
    required_files = {
        "MAZ data": source_dir / "inputs" / "landuse" / "maz_data.csv",
        "Tolls": source_dir / "inputs" / "hwy" / "tolls.csv",
        "AM Demand": source_dir / "demand_matrices" / "highway" / "household" / "TAZ_Demand_AM.omx",
    }
    
    for name, path in required_files.items():
        logger.debug(f"Checking {name}: {path}")
        if not path.exists():
            warnings.append(f"{name} file not found: {path}")
            logger.warning(f"{name} file not found: {path}")
        else:
            logger.info(f"✓ {name} found: {path}")
    
    # Check config templates
    config_dir = Path(__file__).parent / "config_templates"
    logger.debug(f"Checking config templates: {config_dir}")
    if not config_dir.exists():
        issues.append(f"Config templates not found: {config_dir}")
        logger.error(f"Config templates not found: {config_dir}")
    else:
        logger.info(f"✓ Config templates found: {config_dir}")
    
    if issues:
        logger.error("CRITICAL ISSUES FOUND:")
        for issue in issues:
            logger.error(f"  - {issue}")
        return False
    
    if warnings:
        logger.warning("WARNINGS:")
        for warning in warnings:
            logger.warning(f"  - {warning}")
    
    logger.info("✓ All prerequisites met!")
    return True


def setup_test_directory(config, logger):
    """Create test directory structure."""
    logger.info("="*70)
    logger.info("SETTING UP TEST DIRECTORY")
    logger.info("="*70)
    
    county_name = config['test']['county_name']
    output_dir = Path(config['paths']['output_dir'])
    skip_emme_copy = config['test']['skip_emme_copy']
    thin_network = config['test'].get('thin_network')
    source_dir = Path(config['paths']['source_dataset'])
    auto_confirm = config['test'].get('auto_confirm', True)
    
    test_dir = Path(output_dir)
    logger.info(f"Test directory: {test_dir.absolute()}")
    
    if test_dir.exists():
        logger.warning(f"Directory already exists: {test_dir}")
        response = input("Do you want to overwrite it? (y/n): ")
        if response.lower() != 'y':
            logger.info("Operation cancelled by user")
            print("Cancelled.")
            sys.exit(0)
        
        logger.info(f"Removing existing directory: {test_dir}")
        # Try to remove the directory, with better error handling
        try:
            shutil.rmtree(test_dir)
            logger.info("Existing directory removed successfully")
        except (OSError, PermissionError) as e:
            logger.error(f"Cannot delete directory: {e}")
            print(f"\n❌ Cannot delete directory: {e}")
            print(f"\nPossible causes:")
            print(f"  - EMME Desktop has files open from this directory")
            print(f"  - Another process is using files in this directory")
            print(f"\nSolutions:")
            print(f"  1. Close EMME Desktop and try again")
            print(f"  2. Use skip_setup=true to reuse existing directory")
            print(f"  3. Choose a different output_dir")
            print(f"  4. Manually delete the directory and try again")
            sys.exit(1)
    
    logger.info("Creating directory structure...")
    # Create directory structure
    directories = [
        test_dir / "config",
        test_dir / "inputs" / "hwy",
        test_dir / "inputs" / "landuse",
        test_dir / "inputs" / "demand",
        test_dir / "logs",
    ]
    
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)
        logger.debug(f"Created directory: {directory}")
    
    logger.info("✓ Directory structure created")
    
    logger.info("Copying configuration templates...")
    # Copy config templates (using fixed complete config files)
    config_templates = Path(__file__).parent / "config_templates"
    shutil.copy(
        config_templates / "fixed_san_mateo_scenario.toml",
        test_dir / "config" / "scenario.toml"
    )
    shutil.copy(
        config_templates / "fixed_san_mateo_model.toml",
        test_dir / "config" / "model.toml"
    )
    
    # Update model config - the household section is not used for highway-only tests
    # Demand is loaded from time-period-specific files in inputs/demand/
    model_config = toml.load(test_dir / "config" / "model.toml")
    if 'household' in model_config:
        # Point ALL household demand files to the AM demand file as placeholder (actual loading is per time period)
        model_config['household']['highway_demand_file'] = "inputs/demand/TAZ_Demand_AM.omx"
        model_config['household']['transit_demand_file'] = "inputs/demand/TAZ_Demand_AM.omx"
        model_config['household']['active_demand_file'] = "inputs/demand/TAZ_Demand_AM.omx"
    
    # Update air_passenger and internal_external paths to use stub files (not used in county test)
    if 'air_passenger' in model_config:
        model_config['air_passenger']['highway_demand_file'] = "inputs/demand/TAZ_Demand_AM.omx"
    if 'internal_external' in model_config:
        model_config['internal_external']['highway_demand_file'] = "inputs/demand/TAZ_Demand_AM.omx"
    
    # Point truck demand to the copied truck file
    if 'truck' in model_config:
        model_config['truck']['highway_demand_file'] = "inputs/demand/tripstrkAM.omx"
    
    logger.debug("Updated demand paths in model config")
    
    # Write back the updated config
    with open(test_dir / "config" / "model.toml", "wb") as f:
        content = toml.dumps(model_config).encode('utf-8')
        if content.startswith(b'\xef\xbb\xbf'):
            content = content[3:]
        f.write(content)
    
    logger.info("✓ Configuration files copied")
    
    # Apply thin_network setting if provided
    if thin_network is not None:
        scenario_config = toml.load(test_dir / "config" / "scenario.toml")
        if "emme" not in scenario_config:
            scenario_config["emme"] = {}
        scenario_config["emme"]["thin_network_ft_threshold"] = thin_network
        with open(test_dir / "config" / "scenario.toml", "wb") as f:
            content = toml.dumps(scenario_config).encode('utf-8')
            if content.startswith(b'\xef\xbb\xbf'):
                content = content[3:]
            f.write(content)
        logger.info(f"Network thinning enabled: @ft <= {thin_network}")
    
    # Copy EMME project
    source_emme = source_dir / "emme_project"
    dest_emme = test_dir / "emme_project"
    
    if skip_emme_copy:
        logger.info("Skipping EMME project copy (skip_emme_copy=true)")
        if not dest_emme.exists():
            logger.warning(f"EMME project not found at {dest_emme}")
            logger.warning(f"You must manually copy it from {source_emme}")
    elif dest_emme.exists():
        logger.warning(f"EMME project already exists at {dest_emme}")
        if auto_confirm:
            logger.info("Auto-confirm enabled, using existing EMME project")
        else:
            response = input("  Do you want to skip copying (reuse existing)? (y/n): ")
            if response.lower() == 'y':
                logger.info("Using existing EMME project")
            else:
                logger.info("Replacing EMME project (this may take a few minutes)...")
                shutil.rmtree(dest_emme)
                shutil.copytree(source_emme, dest_emme)
                logger.info(f"Copied EMME project to {dest_emme}")
    else:
        logger.info("Copying EMME project (this may take a few minutes)...")
        logger.debug(f"Source: {source_emme}")
        logger.debug(f"Dest: {dest_emme}")
        shutil.copytree(source_emme, dest_emme)
        logger.info(f"Copied EMME project to {dest_emme}")
    
    # Copy essential input files
    
    # Copy tolls
    logger.debug("Copying tolls.csv...")
    shutil.copy(
        source_dir / "inputs" / "hwy" / "tolls.csv",
        test_dir / "inputs" / "hwy" / "tolls.csv"
    )
    logger.debug("Copied tolls.csv")
    
    # Copy interchange_nodes
    logger.debug("Copying interchange_nodes.csv...")
    shutil.copy(
        source_dir / "inputs" / "hwy" / "interchange_nodes.csv",
        test_dir / "inputs" / "hwy" / "interchange_nodes.csv"
    )
    logger.debug("Copied interchange_nodes.csv")
    
    # Copy MAZ data
    logger.debug("Copying MAZ data...")
    shutil.copy(
        source_dir / "inputs" / "landuse" / "maz_data.csv",
        test_dir / "inputs" / "landuse" / "maz_data.csv"
    )
    logger.debug("Copied maz_data.csv")
    
    # Check if demand filtering is enabled
    scenario_config = toml.load(test_dir / "config" / "scenario.toml")
    filter_demand = config['test'].get('filter_demand', False)
    
    if filter_demand:
        logger.info("="*70)
        logger.info("FILTERING DEMAND TO INTRA-COUNTY TRIPS")
        logger.info("="*70)
        
        from tests.test_highway_assign_skim import CountyDataFilter, get_county_zones
        
        # Get zone ranges for the county
        logger.info(f"Detecting zone ranges for {county_name} County...")
        zone_info = get_county_zones(county_name)
        taz_range = zone_info['taz_range']
        maz_range = zone_info['maz_range']
        
        logger.info(f"County zone ranges:")
        logger.info(f"  TAZ: {taz_range[0]} - {taz_range[1]}")
        logger.info(f"  MAZ: {maz_range[0]} - {maz_range[1]}")
        
        logger.debug("Creating CountyDataFilter helper...")
        filter_helper = CountyDataFilter(
            taz_range=taz_range,
            maz_range=maz_range,
            county_name=county_name
        )
        
        # Get time periods from scenario config to filter only those demand files
        time_periods = []
        if 'time_periods' in scenario_config:
            for tp in scenario_config['time_periods']:
                time_periods.append(tp['name'])
        
        logger.info(f"Time periods to filter: {', '.join(time_periods)}")
        logger.debug(f"Found {len(time_periods)} time periods in config")
        
        # Filter demand files for configured time periods only
        demand_dir = source_dir / "demand_matrices" / "highway" / "household"
        logger.debug(f"Source demand directory: {demand_dir}")
        
        logger.info("Filtering demand files...")
        for period in time_periods:
            demand_file = demand_dir / f"TAZ_Demand_{period}.omx"
            if demand_file.exists():
                output_file = test_dir / "inputs" / "demand" / demand_file.name
                logger.info(f"  Processing {demand_file.name}...")
                logger.debug(f"    Source: {demand_file}")
                logger.debug(f"    Output: {output_file}")
                filter_helper.filter_trip_table(demand_file, output_file)
                
                # Verify the output file was created and list its matrices
                if output_file.exists():
                    logger.debug(f"    Output file created successfully")
                    try:
                        import openmatrix as omx
                        with omx.open_file(str(output_file), 'r') as omx_file:
                            matrices = omx_file.list_matrices()
                            logger.info(f"    Created {len(matrices)} matrices: {', '.join(matrices[:5])}{'...' if len(matrices) > 5 else ''}")
                    except Exception as e:
                        logger.warning(f"    Could not verify matrices: {e}")
                else:
                    logger.error(f"    Output file was not created!")
            else:
                logger.warning(f"  {demand_file.name} not found, skipping")
        
        logger.info("Demand filtering complete!")
    else:
        logger.info("Copying demand files (filtering disabled)...")
        
        # Get time periods from scenario config to copy only those demand files
        time_periods = []
        if 'time_periods' in scenario_config:
            for tp in scenario_config['time_periods']:
                time_periods.append(tp['name'])
        
        logger.info(f"Time periods to copy: {', '.join(time_periods)}")
        logger.debug(f"Found {len(time_periods)} time periods in config")
        
        # Copy demand files for configured time periods only
        demand_dir = source_dir / "demand_matrices" / "highway" / "household"
        logger.debug(f"Source demand directory: {demand_dir}")
        
        for period in time_periods:
            demand_file = demand_dir / f"TAZ_Demand_{period}.omx"
            if demand_file.exists():
                output_file = test_dir / "inputs" / "demand" / demand_file.name
                shutil.copy(demand_file, output_file)
                logger.info(f"  Copied {demand_file.name}")
                logger.debug(f"    Size: {demand_file.stat().st_size / 1024 / 1024:.1f} MB")
                
                # Verify the output file and list its matrices
                try:
                    import openmatrix as omx
                    with omx.open_file(str(output_file), 'r') as omx_file:
                        matrices = omx_file.list_matrices()
                        logger.debug(f"    Contains {len(matrices)} matrices: {', '.join(matrices[:5])}{'...' if len(matrices) > 5 else ''}")
                except Exception as e:
                    logger.warning(f"    Could not verify matrices: {e}")
            else:
                logger.warning(f"  {demand_file.name} not found, skipping")
    
    # Copy truck demand files
    logger.info("Copying truck demand files...")
    truck_dir = source_dir / "demand_matrices" / "highway" / "commercial"
    logger.debug(f"Truck demand directory: {truck_dir}")
    
    for period in time_periods:
        truck_file = truck_dir / f"tripstrk{period}.omx"
        if truck_file.exists():
            output_file = test_dir / "inputs" / "demand" / truck_file.name
            shutil.copy(truck_file, output_file)
            logger.info(f"  Copied {truck_file.name}")
            logger.debug(f"    Size: {truck_file.stat().st_size / 1024 / 1024:.1f} MB")
        else:
            logger.warning(f"  {truck_file.name} not found, skipping")
    
    logger.info("Test directory setup complete!")
    return test_dir


def run_test(config, logger):
    """Run the highway test."""
    logger.info("="*70)
    logger.info("RUNNING HIGHWAY TEST")
    logger.info("="*70)
    
    county_name = config['test']['county_name']
    test_dir = Path(config['paths']['output_dir'])
    
    try:
        logger.info("Importing CountyHighwayController...")
        from tests.highway_assign_skim_controller import CountyHighwayController
        
        logger.info(f"Initializing controller for {county_name} County...")
        logger.debug(f"Scenario config: {test_dir / 'config' / 'scenario.toml'}")
        logger.debug(f"Model config: {test_dir / 'config' / 'model.toml'}")
        logger.debug(f"Run directory: {test_dir}")
        
        controller = CountyHighwayController(
            scenario_config=str(test_dir / "config" / "scenario.toml"),
            model_config=str(test_dir / "config" / "model.toml"),
            run_dir=str(test_dir),
            county_name=county_name,
            include_maz_components=False,  # Skip MAZ for initial test
            include_network_summary=False  # Skip network summary for speed
        )
        
        logger.info("Starting highway components...")
        logger.info("Components to run:")
        logger.info("  1. prepare_network_highway - Prepare network attributes")
        logger.info("  2. highway - Assignment and skimming")
        
        logger.info("Executing controller.run_highway_only()...")
        logger.info("This may take 5-15 minutes depending on network size...")
        logger.info("The following steps will occur:")
        logger.info("  - Loading network from EMME")
        logger.info("  - Setting network attributes (@useclass, tolls, etc.)")
        logger.info("  - Loading demand matrices")
        logger.info("  - Running highway assignment (iterative convergence)")
        logger.info("  - Computing skims")
        logger.info("  - Exporting loaded network")
        logger.info("")
        logger.info("Watch the EMME Modeller window for detailed progress...")
        logger.info("")
        
        controller.run_highway_only()
        
        logger.info("")
        logger.info("Controller execution completed!")
        logger.info("="*70)
        logger.info("TEST COMPLETED SUCCESSFULLY!")
        logger.info("="*70)
        
        logger.info("Validating results...")
        success = controller.validate_results()
        if success:
            logger.info("Results validation passed!")
        else:
            logger.warning("Results validation had warnings - check logs")
        
        return True
        
    except Exception as e:
        logger.error(f"Test failed with error: {str(e)}")
        logger.debug("Full traceback:", exc_info=True)
        return False
        print("\n" + "="*70)
        print("❌ TEST FAILED")
        print("="*70)
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
        # Also write to file
        with open(test_dir / "logs" / "error_traceback.txt", "w") as f:
            f.write(f"Error: {e}\n\n")
            traceback.print_exc(file=f)
        print(f"\nFull traceback saved to: {test_dir / 'logs' / 'error_traceback.txt'}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Run county highway test using configuration file"
    )
    parser.add_argument(
        "--config",
        default="tests/county_test_config.toml",
        help="Path to configuration file (default: tests/county_test_config.toml)"
    )
    
    args = parser.parse_args()
    
    # Load configuration
    config_path = Path(args.config)
    if not config_path.exists():
        print(f"❌ Configuration file not found: {config_path}")
        print(f"\nPlease create a configuration file or specify an existing one with --config")
        print(f"Example: python tests/run_county_test.py --config my_config.toml")
        sys.exit(1)
    
    print(f"Loading configuration from: {config_path}")
    config = toml.load(config_path)
    
    # Setup console logging only initially (file logging added after directory setup)
    log_level = config.get('logging', {}).get('console_log_level', 'INFO')
    logger = setup_console_logging(log_level)
    
    logger.info("County Test Framework - Starting")
    logger.info(f"Configuration file: {config_path}")
    
    # Display configuration summary
    logger.info("="*70)
    logger.info("CONFIGURATION SUMMARY")
    logger.info("="*70)
    logger.info(f"County: {config['test']['county_name']}")
    logger.info(f"Source dataset: {config['paths']['source_dataset']}")
    logger.info(f"Output directory: {config['paths']['output_dir']}")
    logger.info(f"Filter demand: {config['test'].get('filter_demand', False)}")
    logger.info(f"Skip EMME copy: {config['test'].get('skip_emme_copy', False)}")
    logger.info(f"Skip setup: {config['test'].get('skip_setup', False)}")
    if config['test'].get('thin_network'):
        logger.info(f"Network thinning: @ft <= {config['test']['thin_network']}")
    logger.info("="*70)
    
    # Check prerequisites
    if not check_prerequisites(config, logger):
        logger.error("Prerequisites not met. Please resolve issues and try again.")
        return 1
    
    # Confirm before running
    if not config['test'].get('auto_confirm', True):
        print("\nReady to run test? This will take several minutes. (y/n): ", end='')
        response = input()
        if response.lower() != 'y':
            logger.info("Test cancelled by user")
            print("\nTest cancelled.")
            return 0
    else:
        logger.info("Auto-confirm enabled, proceeding with test")
    
    # Setup test directory
    if not config['test'].get('skip_setup', False):
        test_dir = setup_test_directory(config, logger)
    else:
        test_dir = Path(config['paths']['output_dir'])
        logger.info(f"Skipping setup, using existing directory: {test_dir}")
        if not test_dir.exists():
            logger.error(f"Test directory does not exist: {test_dir}")
            return 1
        
        # Verify required files exist
        required_files = [
            test_dir / "config" / "scenario.toml",
            test_dir / "config" / "model.toml",
        ]
        missing_files = [f for f in required_files if not f.exists()]
        if missing_files:
            logger.error("Required files missing from test directory:")
            for f in missing_files:
                logger.error(f"  - {f}")
            print(f"\nTo fix:")
            print(f"  1. Close EMME Desktop if it's open")
            print(f"  2. Delete the directory manually")
            print(f"  3. Run without skip_setup=true to create fresh directory")
            return 1
    
    # Now add file logging (after directory is set up)
    output_dir = config['paths']['output_dir']
    log_file = add_file_logging(logger, output_dir)
    
    # Run test
    logger.info("Starting test execution...")
    success = run_test(config, logger)
    
    if success:
        test_dir = Path(config['paths']['output_dir'])
        logger.info("="*70)
        logger.info("TEST COMPLETED SUCCESSFULLY")
        logger.info("="*70)
        logger.info(f"Test artifacts location: {test_dir.absolute()}")
        logger.info(f"  - Logs: {test_dir / 'logs'}")
        logger.info(f"  - Loaded network: {test_dir / 'loaded_highway'}")
        logger.info(f"  - Skims: {test_dir / 'skim_matrices' / 'highway'}")
        logger.info(f"  - Full log: {log_file}")
        return 0
    else:
        test_dir = Path(config['paths']['output_dir'])
        logger.error("TEST FAILED")
        logger.error(f"Check logs in: {test_dir / 'logs'}")
        logger.error(f"Full log: {log_file}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

