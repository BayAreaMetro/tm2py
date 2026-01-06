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
import io

# Force UTF-8 encoding for console output on Windows
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

def check_prerequisites(config):
    """Check if all required files and directories exist."""
    print("="*70)
    print("CHECKING PREREQUISITES")
    print("="*70)
    
    issues = []
    warnings = []
    
    county_name = config['test']['county_name']
    
    # Check source dataset
    source_dir = Path(config['paths']['source_dataset'])
    if not source_dir.exists():
        issues.append(f"Source dataset not found: {source_dir}")
    else:
        print(f"[OK] Source dataset found: {source_dir}")
    
    # Check EMME project
    emme_project = source_dir / "emme_project"
    if not emme_project.exists():
        issues.append(f"EMME project not found: {emme_project}")
    else:
        print(f"[OK] EMME project found: {emme_project}")
    
    # Check EMME database
    emme_db = emme_project / "Database_highway" / "emmebank"
    if not emme_db.exists():
        issues.append(f"EMME database not found: {emme_db}")
    else:
        print(f"[OK] EMME database found: {emme_db}")
    
    # Check for required input files
    required_files = {
        "MAZ data": source_dir / "inputs" / "landuse" / "maz_data.csv",
        "Tolls": source_dir / "inputs" / "hwy" / "tolls.csv",
        "AM Demand": source_dir / "demand_matrices" / "highway" / "household" / "TAZ_Demand_AM.omx",
    }
    
    for name, path in required_files.items():
        if not path.exists():
            warnings.append(f"{name} file not found: {path}")
        else:
            print(f"[OK] {name} found: {path}")
    
    # Check config templates
    config_dir = Path(__file__).parent / "config_templates"
    if not config_dir.exists():
        issues.append(f"Config templates not found: {config_dir}")
    else:
        print(f"[OK] Config templates found: {config_dir}")
    
    print()
    if issues:
        print("❌ CRITICAL ISSUES FOUND:")
        for issue in issues:
            print(f"   - {issue}")
        return False
    
    if warnings:
        print("⚠ WARNINGS:")
        for warning in warnings:
            print(f"   - {warning}")
        print()
    
    print("[OK] All prerequisites met!")
    return True
nfig):
    """Create test directory structure."""
    print("\n" + "="*70)
    print("SETTING UP TEST DIRECTORY")
    print("="*70)
    
    county_name = config['test']['county_name']
    output_dir = Path(config['paths']['output_dir'])
    skip_emme_copy = config['test']['skip_emme_copy']
    thin_network = config['test'].get('thin_network')
    source_dir = Path(config['paths']['source_dataset'] "="*70)
    print("SETTING UP TEST DIRECTORY")
    print("="*70)
    
    test_dir = Path(output_dir)
    print(f"\nTest directory: {test_dir.absolute()}")
    
    if test_dir.exists():
        print(f"⚠ Warning: Directory already exists")
        response = input("Do you want to overwrite it? (y/n): ")
        if response.lower() != 'y':
            print("Cancelled.")
            sys.exit(0)
        
        # Try to remove the directory, with better error handling
        try:
            shutil.rmtree(test_dir)
        except (OSError, PermissionError) as e:
            print(f"\n❌ Cannot delete directory: {e}")
            print(f"\nPossible causes:")
            print(f"  - EMME Desktop has files open from this directory")
            print(f"  - Another process is using files in this directory")
            print(f"\nSolutions:")
            print(f"  1. Close EMME Desktop and try again")
            print(f"  2. Use --skip-setup to reuse existing directory")
            print(f"  3. Choose a different --output-dir")
            print(f"  4. Manually delete the directory and try again")
            sys.exit(1)
    
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
        print(f"  [OK] Created {directory}")
    
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
    print(f"  [OK] Copied config files")
    
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
        print(f"  [OK] Network thinning enabled: @ft <= {thin_network}")
    
    # Copy EMME project
    source_emme = source_dir / "emme_project"
    dest_emme = test_dir / "emme_project"
    
    if skip_emme_copy:
        print(f"  ⊘ Skipping EMME project copy (--skip-emme-copy flag set)")
        if not dest_emme.exists():
            print(f"  ⚠ WARNING: EMME project not found at {dest_emme}")
            print(f"  You must manually copy it from {source_emme}")
    elif dest_emme.exists():
        print(f"  ⚠ EMME project already exists at {dest_emme}")
        response = input("  Do you want to skip copying (reuse existing)? (y/n): ")
        if response.lower() == 'y':
            print(f"  [OK] Using existing EMME project")
        else:
            print(f"  Replacing EMME project (this may take a few minutes)...")
            shutil.rmtree(dest_emme)
            shutil.copytree(source_emme, dest_emme)
            print(f"  [OK] Copied EMME project to {dest_emme}")
    else:
        print(f"  Copying EMME project (this may take a few minutes)...")
        shutil.copytree(source_emme, dest_emme)
        print(f"  ✓ Copied EMME project to {dest_emme}")
    
    # Copy essential input files
    
    # Copy tolls
    shutil.copy(
        source_dir / "inputs" / "hwy" / "tolls.csv",
        test_dir / "inputs" / "hwy" / "tolls.csv"
    )
    print(f"  [OK] Copied tolls.csv")
    
    # Copy MAZ data
    shutil.copy(
        source_dir / "inputs" / "landuse" / "maz_data.csv",
        test_dir / "inputs" / "landuse" / "maz_data.csv"
    )
    print(f"  [OK] Copied maz_data.csv")
    
    # Check if demand filtering is enabled
    scenario_config = toml.load(test_dir / "config" / "scenario.toml")
    filter_demand = config['test'].get('filter_demand', False)
    
    if filter_demand:
        print(f"\n{'='*70}")
        print("FILTERING DEMAND TO INTRA-COUNTY TRIPS")
        print(f"{'='*70}")
        
        from tests.test_highway_assign_skim import CountyDataFilter, get_county_zones
        
        # Get zone ranges for the county
        zone_info = get_county_zones(county_name)
        taz_range = zone_info['taz_range']
        maz_range = zone_info['maz_range']
        
        print(f"\nCounty zone ranges:")
        print(f"  TAZ: {taz_range[0]} - {taz_range[1]}")
        print(f"  MAZ: {maz_range[0]} - {maz_range[1]}")
        
        filter_helper = CountyDataFilter(
            taz_range=taz_range,
            maz_range=maz_range,
            county_name=county_name
        )
        
        # Get time periods from scenario config to filter only those demand files
        time_periods = []
        if 'emme' in scenario_config and 'time_period' in scenario_config['emme']:
            for tp in scenario_config['emme']['time_period']:
                time_periods.append(tp['name'])
        
        print(f"\nTime periods to filter: {', '.join(time_periods)}")
        
        # Filter demand files for configured time periods only
        demand_dir = source_dir / "demand_matrices" / "highway" / "household"
        
        print(f"\nFiltering demand files...")
        for period in time_periods:
            demand_file = demand_dir / f"TAZ_Demand_{period}.omx"
            if demand_file.exists():
                output_file = test_dir / "inputs" / "demand" / demand_file.name
                print(f"\n  {demand_file.name}:")
                filter_helper.filter_trip_table(demand_file, output_file)
            else:
                print(f"\n  ⚠ Warning: {demand_file.name} not found, skipping")
        
        print(f"\n[OK] Demand filtering complete!")
    else:
        print(f"\nCopying demand files (filtering disabled)...")
        
        # Get time periods from scenario config to copy only those demand files
        time_periods = []
        if 'emme' in scenario_config and 'time_period' in scenario_config['emme']:
            for tp in scenario_config['emme']['time_period']:
                time_periods.append(tp['name'])
        
        print(f"Time periods to copy: {', '.join(time_periods)}")
        
        # Copy demand files for configured time periods only
        demand_dir = source_dir / "demand_matrices" / "highway" / "household"
        
        for period in time_periods:
            demand_file = demand_dir / f"TAZ_Demand_{period}.omx"
            if demand_file.exists():
                output_file = test_dir / "inputs" / "demand" / demand_file.name
                shutil.copy(demand_file, output_file)
                print(f"  [OK] Copied {demand_file.name}")
            else:
                print(f"  ⚠ Warning: {demand_file.name} not found, skipping")
    
    print(f"\n[OK] Test directory setup complete!")
    return test_dir


def run_test(config):
    """Run the highway test."""
    print("\n" + "="*70)
    print("RUNNING HIGHWAY TEST")
    print("="*70)
    
    county_name = config['test']['county_name']
    test_dir = Path(config['paths']['output_dir'])
    
    try:
        from tests.highway_assign_skim_controller import CountyHighwayController
        
        print(f"\nInitializing controller for {county_name} County...")
        
        controller = CountyHighwayController(
            scenario_config=str(test_dir / "config" / "scenario.toml"),
            model_config=str(test_dir / "config" / "model.toml"),
            run_dir=str(test_dir),
            county_name=county_name,
            include_maz_components=False,  # Skip MAZ for initial test
            include_network_summary=False  # Skip network summary for speed
        )
        
        print("\nStarting highway components...")
        print("This will run:")
        print("  1. prepare_network_highway - Prepare network attributes")
        print("  2. highway - Assignment and skimming")
        print()
        
        controller.run_highway_only()
        
        print("\n" + "="*70)
        print("[OK] TEST COMPLETED SUCCESSFULLY!")
        print("="*70)
        
        # Validate results
        success = controller.validate_results()
        if success:
            print("\n[OK] Results validation passed!")
        else:
            print("\n⚠ Results validation had warnings - check logs")
        
        return True
        
    except Exception as e:
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
    
    # Display configuration summary
    print("\n" + "="*70)
    print("CONFIGURATION SUMMARY")
    print("="*70)
    print(f"County: {config['test']['county_name']}")
    print(f"Source dataset: {config['paths']['source_dataset']}")
    print(f"Output directory: {config['paths']['output_dir']}")
    print(f"Filter demand: {config['test'].get('filter_demand', False)}")
    print(f"Skip EMME copy: {config['test'].get('skip_emme_copy', False)}")
    print(f"Skip setup: {config['test'].get('skip_setup', False)}")
    if config['test'].get('thin_network'):
        print(f"Network thinning: @ft <= {config['test']['thin_network']}")
    print("="*70)
    
    # Check prerequisites
    if not check_prerequisites(config):
        print("\n❌ Prerequisites not met. Please resolve issues and try again.")
        return 1
    
    # Setup test directory
    if not config['test'].get('skip_setup', False):
        test_dir = setup_test_directory(config)
    else:
        test_dir = Path(config['paths']['output_dir'])
        print(f"\nUsing existing test directory: {test_dir}")
        if not test_dir.exists():
            print(f"❌ Test directory does not exist: {test_dir}")
            return 1
        
        # Verify required files exist
        required_files = [
            test_dir / "config" / "scenario.toml",
            test_dir / "config" / "model.toml",
        ]
        missing_files = [f for f in required_files if not f.exists()]
        if missing_files:
            print(f"\n❌ Required files missing from test directory:")
            for f in missing_files:
                print(f"   - {f}")
            print(f"\nTo fix:")
            print(f"  1. Close EMME Desktop if it's open")
            print(f"  2. Delete the directory manually")
            print(f"  3. Run without skip_setup=true to create fresh directory")
            return 1
    
    # Prompt before running
    if not config['test'].get('auto_confirm', False):
        print("\n" + "="*70)
        response = input("\nReady to run test? This will take several minutes. (y/n): ")
        if response.lower() != 'y':
            print("\nTest cancelled.")
            return 0
    else:
        print("\n" + "="*70)
        print("\nStarting test (auto_confirm enabled)...")
    
    # Run test
    success = run_test(config)
    
    if success:
        test_dir = Path(config['paths']['output_dir'])
        print(f"\nTest artifacts are in: {test_dir.absolute()}")
        print(f"  - Logs: {test_dir / 'logs'}")
        print(f"  - Loaded network: {test_dir / 'loaded_highway'}")
        print(f"  - Skims: {test_dir / 'skim_matrices' / 'highway'}")
        return 0
    else:
        test_dir = Path(config['paths']['output_dir'])
        print(f"\nCheck logs in: {test_dir / 'logs'}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
