USAGE = """Quick Test Script for County Highway Framework

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
import logging
import os
import shutil
import sys
import toml
import tomlkit # preserves comments
from pathlib import Path
from datetime import datetime
import io
import pprint

import tm2py
from tm2py.controller import RunController
from tm2py.county_tools import CountyDataFilter, get_county_zones


# Force UTF-8 encoding for console output on Windows
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')



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

"""
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

"""

def update_config_for_test(model_dir: Path, test_county: str, logger):
    """Updates the model configuration (scenario and model) for the test
    from the default for the full model run.

    Args:
        model_dir (Path): Directory for model run test
        test_county (str): County for testing
    """
    # The model run is setup, but it's setup for a complete run
    # Adjust for county-specific assignment test

    # replace the scenario_config.toml one for the county test
    scenario_config_file = model_dir.resolve() / "scenario_config.toml"
    with open(scenario_config_file, "r", encoding="utf-8") as f:
        scenario_config_content = f.read()
    scenario_config = tomlkit.parse(scenario_config_content)
    
    logger.debug(f"scenario_config:\n{scenario_config}")

    # rename scenario
    scenario_config['scenario']['name'] = f"{test_county} County Highway Test"

    # Run a subset of components
    scenario_config['run']['initial_components'] = [
        "create_tod_scenarios",
        "prepare_network_highway",
        "highway",
        "highway_maz_skim",
    ]
    scenario_config['run']['global_iteration_components'] = [
        "highway_maz_assign",
        "highway"
    ] 
    scenario_config['run']['final_components'] = ["network_summary"]

    # Only run for 1 interation
    scenario_config['run']['end_iteration'] = 1

    # Disable slack
    scenario_config['slack_notifications']['enabled'] = False

    # TODO: What are warmstart skims used for?
    # ohhh.... I'm guessing it's to make the initial skims rather than generating them
    scenario_config['warmstart']['use_warmstart_skim'] = False
    scenario_config['warmstart']['use_warmstart_demand'] = True

    logger.debug(f"scenario_config:\n{scenario_config}")    

    # write it
    with open(scenario_config_file, "w", encoding="utf-8") as f:
        tomlkit.dump(scenario_config, f)
    logger.info(f"Wrote updated scenario config for county test to {scenario_config_file}")

    # model config - reduce max iterations for highway
    model_config_file = model_dir.resolve() / "model_config.toml"
    with open(model_config_file, "r", encoding="utf-8") as f:
        model_config_content = f.read()
    model_config = tomlkit.parse(model_config_content)

    # adjust down to 3
    model_config['highway']['max_iterations'] = 3
    
    # write it
    with open(model_config_file, "w", encoding="utf-8") as f:
        tomlkit.dump(model_config, f)
    logger.info(f"Wrote updated model config for county test to {model_config_file}")

def main():
    parser = argparse.ArgumentParser(description=USAGE, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("setup_config_toml", type=Path, help="The setup_config.toml to use. Can be absolute or relative.")
    parser.add_argument("model_dir", type=Path, help="The model directory. Can be absolute or relative.")
    parser.add_argument("test_county", type=str, help="The county to test")
    parser.add_argument("--overwrite", action='store_true', help="Overwrite directory if it exists")

    args = parser.parse_args()
    
    print("Running tm2py.setup_model.setup.SetupModel with")
    print(f"setup_config_toml: {args.setup_config_toml.resolve()}")
    print(f"        model_dir: {args.model_dir.resolve()}")
    print(f"      test_county: {args.test_county}")
    print(f"        overwrite: {args.overwrite}")
    print("")
    print(f"See log file: {args.model_dir.resolve() / 'setup.log'}", flush=True)
    
    if args.overwrite and args.model_dir.resolve().exists():
        print(f"overwrite={args.overwrite} and {args.model_dir.resolve()} exists: DELETING", flush=True)
        shutil.rmtree(args.model_dir.resolve())
    
    setup_model = tm2py.setup_model.setup.SetupModel(config_file=args.setup_config_toml, model_dir=args.model_dir)
    # since this is a highway assignment/skim test, we don't need all the inputs
    setup_model.setup_config.COPY_POPLU_INPUTS = True
    # I don't think this should be required but for now, it is
    # because mazdata_withDensity.csv is required for CreateTODScenarios.run()
    setup_model.setup_config.COPY_NONRES_INPUTS = True # demand is needed for assignment
    setup_model.setup_config.COPY_WARMSTART_DEMAND = True # hmm we might as well use this tho...
    setup_model.setup_config.COPY_WARMSTART_SKIMS = False
    
    # run the setup
    #TODO: This creates more than is needed; we could instrument to suppress more
    setup_model.run_setup()

    # The model run is setup, but it's setup for a complete run
    # Adjust for county-specific assignment test
    update_config_for_test(args.model_dir, args.test_county, setup_model.logger)

    # We're done setting up. Shut down logging as RunModel will do its own logging
    setup_model.logger.info(f"Setup complete; switching to RunModel.py in {args.model_dir}")
    # do something with logging here?
    # logging.shutdown()

    initial_cwd = Path.cwd()
    # Let's try to run it
    os.chdir(args.model_dir)
    print(f"Switched to {Path.cwd()}")
    # add that path to sysdir for import
    sys.path.append(str(args.model_dir.resolve()))

    # Fingers crossed
    retcode = 0
    try:
        import RunModel
        RunModel.main()
    except Exception as e:
        print(f"Exception occurred: {e}")
        retcode = 1
    
    # go back to initial cwd and return the return code
    os.chdir(initial_cwd)
    return retcode

if __name__ == "__main__":
    sys.exit(main())

