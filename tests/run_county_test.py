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


def generate_setupmodel_config(test_dir, inputs_source, landuse_source, emme_project_source, logger):
    """Generate setupmodel_config.toml for the setup component.
    
    Args:
        test_dir: Test output directory path
        inputs_source: Source directory for network input files (hwy, trn)
        landuse_source: Source directory for population/landuse files
        emme_project_source: Source directory for EMME project
        logger: Logger instance
    """
    logger.info("Generating setupmodel_config.toml for setup component...")
    
    # Auto-detect folder structure: some datasets have hwy/ at root, others have inputs/hwy/
    inputs_source = Path(inputs_source)
    landuse_source = Path(landuse_source)
    
    if (inputs_source / "hwy").exists():
        # Flat structure - files at root level
        input_dir = str(inputs_source).replace("\\", "/")
        logger.debug("Detected flat structure for network inputs (hwy/ at root)")
    else:
        # Nested structure - files in inputs/ subdirectory  
        input_dir = str(inputs_source / "inputs").replace("\\", "/")
        logger.debug("Detected nested structure for network inputs (inputs/hwy/)")
    
    # Landuse may come from a different source
    if (landuse_source / "landuse").exists():
        poplu_dir = str(landuse_source).replace("\\", "/")
        logger.debug(f"Detected flat structure for landuse (landuse/ at root)")
    else:
        poplu_dir = str(landuse_source / "inputs").replace("\\", "/")
        logger.debug(f"Detected nested structure for landuse (inputs/landuse/)")
    
    # Create setupmodel config content
    setupmodel_config = {
        # Point to the inputs directory for network files (hwy, trn)
        "INPUT_NETWORK_DIR": input_dir,
        # Point to landuse source for population/landuse
        "INPUT_POPLU_DIR": poplu_dir,
        "INPUT_NONRES_DIR": poplu_dir,
        
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
    
    logger.info(f"✓ Created setupmodel_config.toml at {config_path}")
    logger.debug(f"  INPUT_NETWORK_DIR: {setupmodel_config['INPUT_NETWORK_DIR']}")
    logger.debug(f"  INPUT_EMME_NETWORK_DIR: {setupmodel_config['INPUT_EMME_NETWORK_DIR']}")
    
    return config_path


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
    
    # Check EMME project source
    emme_project = Path(config['paths']['emme_project_source'])
    logger.debug(f"Checking EMME project: {emme_project}")
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
    
    logger.debug(f"Checking EMME database: {emme_db}")
    if not emme_db.exists() and not emme_db_zip:
        issues.append(f"EMME database not found (neither folder nor zip): {emme_db}")
        logger.error(f"EMME database not found: {emme_db}")
    elif emme_db_zip and use_setup_component:
        logger.info(f"✓ EMME database zip found: {emme_db_zip[0].name} (setup component will unzip)")
    elif emme_db.exists():
        logger.info(f"✓ EMME database found: {emme_db}")
    else:
        logger.warning(f"EMME database is zipped but setup component is not enabled")
        logger.warning(f"  Found: {emme_db_zip[0].name if emme_db_zip else 'none'}")
        logger.warning(f"  Add 'setup' to run_components to handle zipped databases")
    
    # Check inputs source directory
    inputs_dir = Path(config['paths']['inputs_source'])
    logger.debug(f"Checking inputs source: {inputs_dir}")
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
    else:
        hwy_subdir = inputs_dir / "inputs" / "hwy"
    
    # Landuse can come from a separate source
    landuse_dir = Path(config['paths'].get('landuse_source', config['paths']['inputs_source']))
    if (landuse_dir / "landuse").exists():
        landuse_subdir = landuse_dir / "landuse"
    else:
        landuse_subdir = landuse_dir / "inputs" / "landuse"
    
    required_files = {
        "Tolls": hwy_subdir / "tolls.csv",
        "AM Demand": demand_dir / "demand_matrices" / "highway" / "household" / "TAZ_Demand_am.omx",
    }
    
    # MAZ data can have different names depending on dataset version
    maz_file_candidates = [
        landuse_subdir / "maz_data_withDensity.csv",  # 2023 format
        landuse_subdir / "maz_data_new.csv",           # 2015 format
        landuse_subdir / "maz_data.csv",               # fallback
    ]
    maz_file = None
    for candidate in maz_file_candidates:
        if candidate.exists():
            maz_file = candidate
            logger.info(f"✓ MAZ data found: {candidate}")
            break
    if maz_file is None:
        warnings.append(f"MAZ data file not found in {landuse_subdir}")
        logger.warning(f"MAZ data file not found. Tried: {[str(c) for c in maz_file_candidates]}")
    
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
    emme_project_source = Path(config['paths']['emme_project_source'])
    inputs_source = Path(config['paths']['inputs_source'])
    demand_source = Path(config['paths'].get('demand_source', config['paths']['inputs_source']))
    # landuse_source: if not specified, fall back to inputs_source for backward compatibility
    landuse_source = Path(config['paths'].get('landuse_source', config['paths']['inputs_source']))
    auto_confirm = config['test'].get('auto_confirm', True)
    
    logger.info(f"Test directory: {output_dir}")
    logger.info(f"Inputs source: {inputs_source}")
    if landuse_source != inputs_source:
        logger.info(f"Landuse source: {landuse_source}")
    if demand_source != inputs_source:
        logger.info(f"Demand source: {demand_source}")
    
    test_dir = Path(output_dir)
    logger.info(f"Test directory: {test_dir.absolute()}")
    
    if test_dir.exists():
        logger.warning(f"Directory already exists: {test_dir}")
        if not auto_confirm:
            response = input("Do you want to overwrite it? (y/n): ")
            if response.lower() != 'y':
                logger.info("Operation cancelled by user")
                print("Cancelled.")
                sys.exit(0)
        else:
            logger.info("Auto-confirm enabled, proceeding with overwrite")
        
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
    
    # Generate setupmodel_config.toml for setup component (after directory creation)
    logger.info("Generating setupmodel_config.toml for setup component...")
    setupmodel_config_path = generate_setupmodel_config(
        test_dir=test_dir,
        inputs_source=inputs_source,
        landuse_source=landuse_source,
        emme_project_source=emme_project_source,
        logger=logger
    )
    logger.info(f"✓ Created setupmodel_config.toml at {setupmodel_config_path}")
    
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
    
    # Check if setup component will handle file copying
    run_components = config.get('components', {}).get('run_components', [])
    use_setup_component = 'setup' in run_components
    
    if use_setup_component:
        logger.info("="*70)
        logger.info("SETUP COMPONENT ENABLED")
        logger.info("="*70)
        logger.info("The 'setup' component will handle:")
        logger.info("  - Copying EMME project and unzipping databases")
        logger.info("  - Copying input files (hwy, trn, landuse)")
        logger.info("  - Copying demand matrices")
        logger.info("Skipping old setup file copying logic...")
        logger.info("="*70)
        # Setup component will handle everything, so skip to demand filtering
    else:
        # Old setup logic: Copy EMME project manually
        logger.info("Setup component NOT enabled, using legacy file copying...")
        
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
            logger.debug("Detected flat structure for inputs (hwy/ at root)")
        else:
            hwy_subdir = inputs_source / "inputs" / "hwy"
            logger.debug("Detected nested structure for inputs (inputs/hwy/)")
        
        # Landuse can come from a separate source
        if (landuse_source / "landuse").exists():
            landuse_subdir = landuse_source / "landuse"
            logger.debug(f"Using landuse from: {landuse_subdir}")
        else:
            landuse_subdir = landuse_source / "inputs" / "landuse"
            logger.debug(f"Using landuse from nested path: {landuse_subdir}")
        
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
        
        # Copy MAZ data (check multiple possible filenames)
        logger.debug("Copying MAZ data...")
        maz_file_candidates = [
            landuse_subdir / "maz_data_withDensity.csv",  # 2023 format
            landuse_subdir / "maz_data_new.csv",           # 2015 format
            landuse_subdir / "maz_data.csv",               # fallback
        ]
        maz_source = None
        for candidate in maz_file_candidates:
            if candidate.exists():
                maz_source = candidate
                break
        if maz_source:
            # Always copy to maz_data.csv (standardized name expected by model)
            shutil.copy(
                maz_source,
                test_dir / "inputs" / "landuse" / "maz_data.csv"
            )
            logger.debug(f"Copied {maz_source.name} -> maz_data.csv")
        else:
            logger.error(f"No MAZ data file found in {landuse_subdir}")
            raise FileNotFoundError(f"MAZ data file not found. Tried: {[str(c) for c in maz_file_candidates]}")
    
    # Check if demand filtering is enabled (runs regardless of setup component)
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


def run_test(config, logger):
    """Run the highway test."""
    print("\n" + "="*70)
    print("ENTERING run_test() FUNCTION")
    print("="*70)
    
    logger.info("="*70)
    logger.info("RUNNING HIGHWAY TEST")
    logger.info("="*70)
    
    county_name = config['test']['county_name']
    test_dir = Path(config['paths']['output_dir'])
    
    print(f"Test directory: {test_dir}")
    print(f"County: {county_name}")
    
    logger.info(f"Test directory: {test_dir}")
    logger.info(f"County: {county_name}")
    
    try:
        print("Importing CountyHighwayController...")
        logger.info("Importing CountyHighwayController...")
        from tests.highway_assign_skim_controller import CountyHighwayController
        print("  ✓ Import successful")
        logger.info("  ✓ Import successful")
        
        print(f"Initializing controller for {county_name} County...")
        logger.info(f"Initializing controller for {county_name} County...")
        logger.info(f"  Scenario config: {test_dir / 'config' / 'scenario.toml'}")
        logger.info(f"  Model config: {test_dir / 'config' / 'model.toml'}")
        logger.info(f"  Run directory: {test_dir}")
        
        controller = CountyHighwayController(
            scenario_config=str(test_dir / "config" / "scenario.toml"),
            model_config=str(test_dir / "config" / "model.toml"),
            run_dir=str(test_dir),
            county_name=county_name,
            include_maz_components=False,  # Skip MAZ for initial test
            include_network_summary=False  # Skip network summary for speed
        )
        
        print("✓ Controller initialized successfully")
        logger.info("Starting highway components...")
        logger.info("Components to run:")
        logger.info("  1. prepare_network_highway - Prepare network attributes")
        logger.info("  2. highway - Assignment and skimming")
        
        print("Executing controller.run_highway_only()...")
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
        
        # Print network statistics
        logger.info("")
        controller.print_network_statistics(logger)
        
        logger.info("")
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
    print("="*70)
    print("COUNTY TEST STARTING")
    print(f"Time: {datetime.now()}")
    print(f"Config file: {args.config}")
    print("="*70)
    
    # Load configuration
    config_path = Path(args.config)
    if not config_path.exists():
        print(f"❌ Configuration file not found: {config_path}")
        print(f"\nPlease create a configuration file or specify an existing one with --config")
        print(f"Example: python tests/run_county_test.py --config my_config.toml")
        sys.exit(1)
    
    print(f"Loading configuration from: {config_path}")
    config = toml.load(config_path)
    print(f"✓ Configuration loaded successfully")
    
    # Setup console logging only initially (file logging added after directory setup)
    log_level = config.get('logging', {}).get('console_log_level', 'INFO')
    print(f"Setting up logging with level: {log_level}")
    logger = setup_console_logging(log_level)
    
    logger.info("County Test Framework - Starting")
    logger.info(f"Configuration file: {config_path}")
    
    # Display configuration summary
    logger.info("="*70)
    logger.info("CONFIGURATION SUMMARY")
    logger.info("="*70)
    logger.info(f"County: {config['test']['county_name']}")
    logger.info(f"EMME project source: {config['paths']['emme_project_source']}")
    logger.info(f"Inputs source: {config['paths']['inputs_source']}")
    logger.info(f"Output directory: {config['paths']['output_dir']}")
    logger.info(f"Filter demand: {config['test'].get('filter_demand', False)}")
    logger.info(f"Skip EMME copy: {config['test'].get('skip_emme_copy', False)}")
    logger.info(f"Skip setup: {config['test'].get('skip_setup', False)}")
    if config['test'].get('thin_network'):
        logger.info(f"Network thinning: @ft <= {config['test']['thin_network']}")
    logger.info("="*70)
    
    # Check prerequisites
    logger.info("Checking prerequisites...")
    if not check_prerequisites(config, logger):
        logger.error("Prerequisites not met. Please resolve issues and try again.")
        return 1
    logger.info("✓ Prerequisites check passed")
    
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
        logger.info("Setting up test directory...")
        test_dir = setup_test_directory(config, logger)
        logger.info(f"✓ Test directory setup complete: {test_dir}")
    else:
        logger.info("Skipping setup (skip_setup=True)")
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
    logger.info(f"Adding file logging to: {output_dir}")
    log_file = add_file_logging(logger, output_dir)
    logger.info(f"✓ File logging enabled: {log_file}")
    
    # Run test
    logger.info("="*70)
    logger.info("STARTING TEST EXECUTION")
    logger.info("="*70)
    success = run_test(config, logger)
    logger.info("="*70)
    logger.info("TEST EXECUTION COMPLETED")
    logger.info("="*70)
    
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

