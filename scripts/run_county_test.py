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
from pathlib import Path
from datetime import datetime
import io

from tests.test_highway_assign_skim import CountyDataFilter, get_county_zones


# Force UTF-8 encoding for console output on Windows
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Import tm2py logger after path is set up
from tm2py.controller import RunController


def generate_setupmodel_config(test_dir, inputs_source, emme_project_source, logger):
    """Generate setupmodel_config.toml for the setup component.
    
    Args:
        test_dir: Test output directory path
        inputs_source: Source directory for input files
        emme_project_source: Source directory for EMME project
        logger: Logger instance
    """
    logger.info("Generating setupmodel_config.toml for setup component...")
    
    # Auto-detect folder structure: some datasets have hwy/ at root, others have inputs/hwy/
    inputs_source = Path(inputs_source)
    if (inputs_source / "hwy").exists():
        # Flat structure - files at root level
        input_dir = str(inputs_source).replace("\\", "/")
        logger.debug("Detected flat structure (hwy/ at root)")
    else:
        # Nested structure - files in inputs/ subdirectory  
        input_dir = str(inputs_source / "inputs").replace("\\", "/")
        logger.debug("Detected nested structure (inputs/hwy/)")
    
    # Create setupmodel config content
    setupmodel_config = {
        # Point to the inputs directory (contains hwy, trn, landuse, etc.)
        "INPUT_NETWORK_DIR": input_dir,
        "INPUT_POPLU_DIR": input_dir,
        "INPUT_NONRES_DIR": input_dir,
        
        # Point directly to the EMME network directory (not its parent)
        # SetupModel expects INPUT_EMME_NETWORK_DIR to contain the emme_network folder or zipped databases
        "INPUT_EMME_NETWORK_DIR": str(emme_project_source).replace("\\", "/"),
        
        # Required fields - SetupConfig validation requires non-empty values
        # Point to demand_matrices for warmstart files
        "WARMSTART_FILES_DIR": str(inputs_source / "demand_matrices").replace("\\", "/"),
        # Use "none" for release tag - SetupModel won't download if this isn't a valid GitHub tag
        "TRAVEL_MODEL_TWO_RELEASE_TAG": "none",
        
        # EMME template project - use the EMME 25 bare template (matches EMME_25.00.01.zip databases)
        "EMME_TEMPLATE_PROJECT_DIR": "E:/Box/Modeling and Surveys/Development/Travel Model Two Conversion/Model Inputs/2015-tm22-dev-sprint-04/emme_25_project_template",
        "CONFIGS_GITHUB_PATH": "",
    }
    
    # Write to test directory
    config_path = test_dir / "setupmodel_config.toml"
    with open(config_path, "w") as f:
        # Write manually formatted TOML (toml.dump doesn't handle Path objects well)
        f.write("#######################\n")
        f.write("# Setup Model Configs #\n")
        f.write("#######################\n")
        f.write("# Auto-generated for county test\n\n")
        f.write(f'INPUT_NETWORK_DIR = "{setupmodel_config["INPUT_NETWORK_DIR"]}"\n')
        f.write(f'INPUT_POPLU_DIR = "{setupmodel_config["INPUT_POPLU_DIR"]}"\n')
        f.write(f'INPUT_NONRES_DIR = "{setupmodel_config["INPUT_NONRES_DIR"]}"\n')
        f.write(f'INPUT_EMME_NETWORK_DIR = "{setupmodel_config["INPUT_EMME_NETWORK_DIR"]}"\n')
        f.write(f'WARMSTART_FILES_DIR = "{setupmodel_config["WARMSTART_FILES_DIR"]}"\n')
        f.write(f'TRAVEL_MODEL_TWO_RELEASE_TAG = "{setupmodel_config["TRAVEL_MODEL_TWO_RELEASE_TAG"]}"\n')
        f.write(f'EMME_TEMPLATE_PROJECT_DIR = "{setupmodel_config["EMME_TEMPLATE_PROJECT_DIR"]}"\n')
        f.write(f'CONFIGS_GITHUB_PATH = ""\n')
    
    logger.info(f"✓ Created setupmodel_config.toml")
    
    return config_path


def check_prerequisites(config, logger):
    """Check if all required files and directories exist."""
    logger.info("="*70)
    logger.info("CHECKING PREREQUISITES")
    logger.info("="*70)
    
    issues = []
    warnings = []
    
    county_name = config['test']['county_name']
    logger.info(f"County: {county_name}")
    
    # Check EMME project source
    emme_project = Path(config['paths']['emme_project_source'])
    if not emme_project.exists():
        issues.append(f"EMME project not found: {emme_project}")
        logger.error(f"EMME project not found: {emme_project}")
    else:
        logger.info(f"✓ EMME project found: {emme_project}")
    
    # Check EMME database (could be folder or zip file)
    # Check if setup component will handle unzipping
    run_components = config.get('components', {}).get('run_components', [])
    use_setup_component = 'setup' in run_components
    
    emme_db = emme_project / "Database_highway" / "emmebank"
    emme_db_zip = list(emme_project.glob("Database_highway*.zip"))
    
    if not emme_db.exists() and not emme_db_zip:
        issues.append(f"EMME database not found (neither folder nor zip): {emme_db}")
        logger.error(f"EMME database not found: {emme_db}")
    elif emme_db_zip and use_setup_component:
        logger.info(f"✓ EMME database zip found: {emme_db_zip[0].name} (setup component will unzip)")
    elif emme_db.exists():
        logger.info(f"✓ EMME database found: {emme_db}")
    else:
        logger.warn(f"EMME database is zipped but setup component is not enabled")
        logger.warn(f"  Found: {emme_db_zip[0].name if emme_db_zip else 'none'}")
        logger.warn(f"  Add 'setup' to run_components to handle zipped databases")
    
    # Check inputs source directory
    inputs_dir = Path(config['paths']['inputs_source'])
    if not inputs_dir.exists():
        issues.append(f"Inputs source not found: {inputs_dir}")
        logger.error(f"Inputs source not found: {inputs_dir}")
    else:
        logger.info(f"✓ Inputs source found: {inputs_dir}")
    
    # Check demand source directory (separate from inputs)
    demand_dir = Path(config['paths'].get('demand_source', config['paths']['inputs_source']))
    if demand_dir != inputs_dir:
        logger.debug(f"Checking demand source: {demand_dir}")
        if not demand_dir.exists():
            issues.append(f"Demand source not found: {demand_dir}")
            logger.error(f"Demand source not found: {demand_dir}")
        else:
            logger.info(f"✓ Demand source found: {demand_dir}")
    
    # Check for required input files - detect folder structure
    # Some datasets have inputs/hwy/, others have hwy/ at root
    if (inputs_dir / "hwy").exists():
        hwy_subdir = inputs_dir / "hwy"
        landuse_subdir = inputs_dir / "landuse"
    else:
        hwy_subdir = inputs_dir / "inputs" / "hwy"
        landuse_subdir = inputs_dir / "inputs" / "landuse"
    
    required_files = {
        "MAZ data": landuse_subdir / "maz_data_new.csv",
        "Tolls": hwy_subdir / "tolls.csv",
        "AM Demand": demand_dir / "demand_matrices" / "highway" / "household" / "TAZ_Demand_am.omx",
    }
    
    for name, path in required_files.items():
        logger.debug(f"Checking {name}: {path}")
        if not path.exists():
            warnings.append(f"{name} file not found: {path}")
            logger.warn(f"{name} file not found: {path}")
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
        logger.warn("WARNINGS:")
        for warning in warnings:
            logger.warn(f"  - {warning}")
    
    logger.info("✓ All prerequisites met!")
    return True


def setup_test_directory(config, logger):
    """Complete test directory setup (EMME copy, demand filtering, etc).
    
    Note: Basic directory structure and config files should already be created.
    """
    logger.info("="*70)
    logger.info("COMPLETING TEST DIRECTORY SETUP")
    logger.info("="*70)
    
    county_name = config['test']['county_name']
    output_dir = Path(config['paths']['output_dir'])
    skip_emme_copy = config['test']['skip_emme_copy']
    thin_network = config['test'].get('thin_network')
    auto_confirm = config['test'].get('auto_confirm', True)
    emme_project_source = Path(config['paths']['emme_project_source'])
    inputs_source = Path(config['paths']['inputs_source'])
    demand_source = Path(config['paths'].get('demand_source', config['paths']['inputs_source']))
    test_dir = Path(output_dir)
    
    logger.info(f"Test directory: {test_dir.absolute()}")
    logger.info(f"Inputs source: {inputs_source}")
    if demand_source != inputs_source:
        logger.info(f"Demand source: {demand_source}")
    if thin_network:
        logger.info(f"Network thinning enabled: @ft <= {thin_network}")
    
    # Always do manual setup - we're not using the setup component
    # (RunController is initialized with run_components=[] to avoid auto-running setup)
    logger.info("Performing manual EMME and input file setup...")
    
    # Copy EMME project
    source_emme = emme_project_source
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
    
    # Copy essential input files from inputs_source
    # Detect folder structure: some datasets have hwy/ at root, others have inputs/hwy/
    if (inputs_source / "hwy").exists():
        hwy_subdir = inputs_source / "hwy"
        landuse_subdir = inputs_source / "landuse"
        logger.debug("Detected flat structure (hwy/ at root)")
    else:
        hwy_subdir = inputs_source / "inputs" / "hwy"
        landuse_subdir = inputs_source / "inputs" / "landuse"
        logger.debug("Detected nested structure (inputs/hwy/)")
    
    # Copy tolls
    logger.debug("Copying tolls.csv...")
    shutil.copy(
        hwy_subdir / "tolls.csv",
        test_dir / "inputs" / "hwy" / "tolls.csv"
    )
    logger.debug("Copied tolls.csv")
    
    # Copy interchange_nodes (if exists)
    logger.debug("Copying interchange_nodes.csv...")
    interchange_file = hwy_subdir / "interchange_nodes.csv"
    if interchange_file.exists():
        shutil.copy(
            interchange_file,
            test_dir / "inputs" / "hwy" / "interchange_nodes.csv"
        )
        logger.debug("Copied interchange_nodes.csv")
    else:
        logger.warning("interchange_nodes.csv not found, skipping")
    
    # Copy MAZ data
    logger.debug("Copying MAZ data...")
    shutil.copy(
        landuse_subdir / "maz_data_new.csv",
        test_dir / "inputs" / "landuse" / "maz_data_new.csv"
    )
    logger.debug("Copied maz_data_new.csv")
    
    # Check if demand filtering is enabled (runs regardless of setup component)
    scenario_config = toml.load(test_dir / "config" / "scenario.toml")
    filter_demand = config['test'].get('filter_demand', False)
    
    if filter_demand:
        logger.info("="*70)
        logger.info("FILTERING DEMAND TO INTRA-COUNTY TRIPS")
        logger.info("="*70)
                
        # Get zone ranges for the county
        logger.info(f"Detecting zone ranges for {county_name} County...")
        logger.debug(f"config['paths']:{config['paths']}")
        zone_info = get_county_zones(county_name, crosswalk_file=Path(config['paths'].get('crosswalk_file')))
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
        demand_dir = demand_source / "demand_matrices" / "highway" / "household"
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
        demand_dir = demand_source / "demand_matrices" / "highway" / "household"
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
    truck_dir = demand_source / "demand_matrices" / "highway" / "commercial"
    logger.info(f"  Truck demand directory: {truck_dir}")
    logger.info(f"  Truck directory exists: {truck_dir.exists()}")
    logger.info(f"  Time periods to process: {time_periods}")
    
    truck_files_copied = 0
    for period in time_periods:
        truck_file = truck_dir / f"tripstrk{period}.omx"
        logger.info(f"  Checking for {truck_file.name}...")
        if truck_file.exists():
            output_file = test_dir / "inputs" / "demand" / truck_file.name
            logger.info(f"    Copying {truck_file.name} to {output_file}...")
            shutil.copy(truck_file, output_file)
            truck_files_copied += 1
            logger.info(f"    ✓ Copied {truck_file.name} (Size: {truck_file.stat().st_size / 1024 / 1024:.1f} MB)")
        else:
            logger.warning(f"    {truck_file.name} not found, skipping")
    
    logger.info(f"Truck demand files copied: {truck_files_copied}")
    logger.info("="*70)
    logger.info("TEST DIRECTORY SETUP COMPLETE!")
    logger.info("="*70)
    return test_dir


def run_test(config, controller):
    """Run the highway test using the provided controller.
    
    Args:
        config: Test configuration dictionary
        controller: RunController instance with initialized logger
    """
    logger = controller.logger
    
    logger.info("="*70)
    logger.info("ENTERING run_test() FUNCTION")
    logger.info("="*70)
    
    county_name = config['test']['county_name']
    test_dir = Path(config['paths']['output_dir'])
    
    logger.info(f"Test directory: {test_dir}")
    logger.info(f"County: {county_name}")
    
    try:
        logger.info("Importing CountyHighwayController...")
        from tests.highway_assign_skim_controller import CountyHighwayController
        logger.info("  ✓ Import successful")
        
        logger.info(f"Initializing county controller for {county_name} County...")
        
        # Create CountyHighwayController, passing the existing RunController
        county_controller = CountyHighwayController(
            scenario_config=str(test_dir / "config" / "scenario.toml"),
            model_config=str(test_dir / "config" / "model.toml"),
            run_dir=str(test_dir),
            county_name=county_name,
            include_maz_components=False,  # Skip MAZ for initial test
            include_network_summary=False  # Skip network summary for speed
        )
        
        logger.info("✓ County controller initialized successfully")
        logger.info("="*70)
        logger.info("RUNNING HIGHWAY TEST")
        logger.info("="*70)
        logger.info(f"Test directory: {test_dir}")
        logger.info(f"County: {county_name}")
        logger.info(f"  Scenario config: {test_dir / 'config' / 'scenario.toml'}")
        logger.info(f"  Model config: {test_dir / 'config' / 'model.toml'}")
        logger.info(f"  Run directory: {test_dir}")
        logger.info("Starting highway components...")
        logger.info("Components to run:")
        logger.info("  1. prepare_network_highway - Prepare network attributes")
        logger.info("  2. highway - Assignment and skimming")
        
        logger.info("Executing county_controller.run_highway_only()...")
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
        
        county_controller.run_highway_only()
        
        logger.info("")
        logger.info("Controller execution completed!")
        
        # Print network statistics
        logger.info("")
        county_controller.print_network_statistics(logger)
        
        logger.info("")
        logger.info("="*70)
        logger.info("TEST COMPLETED SUCCESSFULLY!")
        logger.info("="*70)
        
        logger.info("Validating results...")
        success = county_controller.validate_results()
        if success:
            logger.info("Results validation passed!")
        else:
            logger.warning("Results validation had warnings - check logs")
        
        return True
        
    except Exception as e:
        logger.error("="*70)
        logger.error("TEST FAILED WITH EXCEPTION")
        logger.error("="*70)
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Error message: {str(e)}")
        logger.error("Full traceback:")
        
        import traceback
        error_traceback = traceback.format_exc()
        logger.error(error_traceback)
        
        # Also write to file
        error_file = test_dir / "logs" / "error_traceback.txt"
        error_file.parent.mkdir(parents=True, exist_ok=True)
        with open(error_file, "w") as f:
            f.write(f"Error Type: {type(e).__name__}\n")
            f.write(f"Error: {e}\n\n")
            f.write("Full Traceback:\n")
            f.write(error_traceback)
        
        logger.error(f"Full traceback saved to: {error_file}")
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
    
    # Print immediately to console so we know the script started
    print("="*70, flush=True)
    print("COUNTY TEST STARTING", flush=True)
    print(f"Time: {datetime.now()}", flush=True)
    print(f"Config file: {args.config}", flush=True)
    print("="*70, flush=True)
    
    # Load configuration
    config_path = Path(args.config)
    if not config_path.exists():
        print(f"❌ Configuration file not found: {config_path}")
        print(f"\nPlease create a configuration file or specify an existing one with --config")
        print(f"Example: python tests/run_county_test.py --config my_config.toml")
        sys.exit(1)
    
    print(f"Loading configuration from: {config_path}", flush=True)
    config = toml.load(config_path)
    print(f"✓ Configuration loaded successfully", flush=True)
    
    # Display configuration summary
    print("="*70, flush=True)
    print("CONFIGURATION SUMMARY", flush=True)
    print("="*70, flush=True)
    print(f"County: {config['test']['county_name']}")
    print(f"EMME project source: {config['paths']['emme_project_source']}")
    print(f"Inputs source: {config['paths']['inputs_source']}")
    print(f"Output directory: {config['paths']['output_dir']}")
    print(f"Filter demand: {config['test'].get('filter_demand', False)}")
    print(f"Skip EMME copy: {config['test'].get('skip_emme_copy', False)}")
    print(f"Skip setup: {config['test'].get('skip_setup', False)}")
    if config['test'].get('thin_network'):
        print(f"Network thinning: @ft <= {config['test']['thin_network']}")
    print("="*70)
    
    # Determine test directory path
    test_dir = Path(config['paths']['output_dir'])
    
    # Setup phase 1: Create directory structure and config files
    # This must be done before RunController initialization
    if not config['test'].get('skip_setup', False):
        print("Creating test directory structure...", flush=True)
        
        # Create directory structure
        test_dir.mkdir(parents=True, exist_ok=True)
        (test_dir / "config").mkdir(exist_ok=True)
        (test_dir / "logs").mkdir(exist_ok=True)
        
        # Create input directories
        for subdir in ["hwy", "landuse", "demand"]:
            (test_dir / "inputs" / subdir).mkdir(parents=True, exist_ok=True)
        
        print(f"✓ Directory structure created: {test_dir}", flush=True)
        
        # Copy configuration templates
        print("Copying configuration templates...", flush=True)
        config_templates_dir = Path("tests/config_templates")
        
        shutil.copy(
            config_templates_dir / "fixed_san_mateo_scenario.toml",
            test_dir / "config" / "scenario.toml"
        )
        shutil.copy(
            config_templates_dir / "fixed_san_mateo_model.toml",
            test_dir / "config" / "model.toml"
        )
        
        print("✓ Configuration templates copied", flush=True)
        
        # Generate setupmodel_config.toml (needed by setup component)
        print("Generating setupmodel_config.toml...", flush=True)
        # Use a simple logger replacement for this pre-RunController phase
        class SimpleLogger:
            def info(self, msg): print(f"  {msg}")
            def debug(self, msg): pass
            def warn(self, msg): print(f"  WARNING: {msg}")
            def warning(self, msg): print(f"  WARNING: {msg}")
            def error(self, msg): print(f"  ERROR: {msg}")
        
        generate_setupmodel_config(
            test_dir=test_dir,
            inputs_source=Path(config['paths']['inputs_source']),
            emme_project_source=Path(config['paths']['emme_project_source']),
            logger=SimpleLogger()
        )
        
        print("✓ setupmodel_config.toml generated", flush=True)
    else:
        print("Skipping setup (skip_setup=True)", flush=True)
        
        # Verify directory and required files exist
        if not test_dir.exists():
            print(f"ERROR: Test directory does not exist: {test_dir}")
            return 1
        
        required_files = [
            test_dir / "config" / "scenario.toml",
            test_dir / "config" / "model.toml",
        ]
        missing_files = [f for f in required_files if not f.exists()]
        if missing_files:
            print("ERROR: Required files missing from test directory:")
            for f in missing_files:
                print(f"  - {f}")
            print(f"\nTo fix:")
            print(f"  1. Close EMME Desktop if it's open")
            print(f"  2. Delete the directory manually")
            print(f"  3. Run without skip_setup=true to create fresh directory")
            return 1
    
    # Use simple logger before RunController can be created
    class SimpleLogger:
        def info(self, msg): print(f"INFO: {msg}", flush=True)
        def debug(self, msg): pass  # Skip debug for simplicity
        def warn(self, msg): print(f"WARNING: {msg}", flush=True)
        def warning(self, msg): print(f"WARNING: {msg}", flush=True)
        def error(self, msg): print(f"ERROR: {msg}", flush=True)
    
    simple_logger = SimpleLogger()
    
    # Check prerequisites
    simple_logger.info("Checking prerequisites...")
    if not check_prerequisites(config, simple_logger):
        simple_logger.error("Prerequisites not met. Please resolve issues and try again.")
        return 1
    simple_logger.info("✓ Prerequisites check passed")
    
    # Confirm before running
    if not config['test'].get('auto_confirm', True):
        print("\nReady to run test? This will take several minutes. (y/n): ", end='', flush=True)
        response = input()
        if response.lower() != 'y':
            simple_logger.info("Test cancelled by user.")
            return 0
    else:
        simple_logger.info("Auto-confirm enabled, proceeding with test")
    
    # Complete setup (EMME copy, demand filtering, etc.)
    if not config['test'].get('skip_setup', False):
        simple_logger.info("Completing test directory setup...")
        setup_test_directory(config, simple_logger)
        simple_logger.info(f"✓ Test directory setup complete: {test_dir}")
    
    # Now that EMME project exists, initialize RunController to get real tm2py logger
    print("Initializing RunController...", flush=True)
    controller = RunController(
        config_file=[
            test_dir / "config" / "scenario.toml",
            test_dir / "config" / "model.toml"
        ],
        run_dir=test_dir,
        run_components=[]  # Don't run any components yet
    )
    logger = controller.logger
    logger.info("="*70)
    logger.info("COUNTY TEST - RunController initialized")
    logger.info("="*70)
    
    # Run test using existing controller
    logger.info("="*70)
    logger.info("STARTING TEST EXECUTION")
    logger.info("="*70)
    success = run_test(config, controller)
    
    if success:
        logger.info("="*70)
        logger.info("TEST COMPLETED SUCCESSFULLY")
        logger.info("="*70)
        logger.info(f"Test artifacts location: {test_dir.absolute()}")
        logger.info(f"  - Logs: {test_dir / 'logs'}")
        logger.info(f"  - Loaded network: {test_dir / 'loaded_highway'}")
        logger.info(f"  - Skims: {test_dir / 'skim_matrices' / 'highway'}")
        return 0
    else:
        logger.error("="*70)
        logger.error("TEST FAILED")
        logger.error("="*70)
        logger.error(f"Check logs in: {test_dir / 'logs'}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

