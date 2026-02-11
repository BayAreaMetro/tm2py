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
import os
import shutil
import sys
import tomlkit # preserves comments
from pathlib import Path
import io

import tm2py

# Force UTF-8 encoding for console output on Windows
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

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
    # configure inline test filter so it stays within the [scenario] table
    test_filter = tomlkit.inline_table()
    test_filter['county'] = test_county
    test_filter.trailing_comma = False
    test_filter.trivia.indent = "    "
    scenario_section = scenario_config['scenario']
    if 'test_filter' in scenario_section:
        del scenario_section['test_filter']
    scenario_section.add('test_filter', test_filter)

    # Run a subset of components
    scenario_config['run']['initial_components'] = [
        "create_tod_scenarios",
        "prepare_network_highway",
        "highway",
        "highway_maz_skim",
    ]
    scenario_config['run']['global_iteration_components'] = [
        "highway_maz_assign",
    ] 
    scenario_config['run']['final_components'] = ["post_processor","network_summary"]

    # Only run for 1 interation
    scenario_config['run']['end_iteration'] = 1

    # Disable slack
    scenario_config['slack_notifications']['enabled'] = False

    # TODO: What are warmstart skims used for?
    # ohhh.... I'm guessing it's to make the initial skims rather than generating them
    scenario_config['warmstart']['use_warmstart_skim'] = False
    scenario_config['warmstart']['use_warmstart_demand'] = True

    # disable transit network post processing
    scenario_config['post_processor']['export_transit_network_shapefile'] = False
    scenario_config['post_processor']['export_boardings_by_segment'] = False
    scenario_config['post_processor']['export_boardings_by_segment_geofile'] = False

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

