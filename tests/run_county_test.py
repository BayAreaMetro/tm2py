"""Quick Test Script for County Highway Framework

This script helps you test the county highway assignment framework by:
1. Checking prerequisites
2. Setting up a minimal test directory
3. Running a basic highway test

Usage:
    From EMME Python environment:
    python tests/run_county_test.py --county "San Mateo" --output-dir "C:/MyTests/san_mateo_test"
"""

import argparse
import shutil
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

def check_prerequisites(county_name):
    """Check if all required files and directories exist."""
    print("="*70)
    print("CHECKING PREREQUISITES")
    print("="*70)
    
    issues = []
    warnings = []
    
    # Check source dataset
    source_dir = Path(r"E:\2015_TM2_20250619")
    if not source_dir.exists():
        issues.append(f"Source dataset not found: {source_dir}")
    else:
        print(f"✓ Source dataset found: {source_dir}")
    
    # Check EMME project
    emme_project = source_dir / "emme_project"
    if not emme_project.exists():
        issues.append(f"EMME project not found: {emme_project}")
    else:
        print(f"✓ EMME project found: {emme_project}")
    
    # Check EMME database
    emme_db = emme_project / "Database_highway" / "emmebank"
    if not emme_db.exists():
        issues.append(f"EMME database not found: {emme_db}")
    else:
        print(f"✓ EMME database found: {emme_db}")
    
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
            print(f"✓ {name} found: {path}")
    
    # Check config templates
    config_dir = Path(__file__).parent / "config_templates"
    if not config_dir.exists():
        issues.append(f"Config templates not found: {config_dir}")
    else:
        print(f"✓ Config templates found: {config_dir}")
    
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
    
    print("✓ All prerequisites met!")
    return True


def setup_test_directory(county_name, output_dir):
    """Create test directory structure."""
    print("\n" + "="*70)
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
        shutil.rmtree(test_dir)
    
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
        print(f"  ✓ Created {directory}")
    
    # Copy config templates
    config_templates = Path(__file__).parent / "config_templates"
    shutil.copy(
        config_templates / "san_mateo_scenario.toml",
        test_dir / "config" / "scenario.toml"
    )
    shutil.copy(
        config_templates / "san_mateo_model.toml",
        test_dir / "config" / "model.toml"
    )
    print(f"  ✓ Copied config files")
    
    # Copy EMME project
    source_emme = Path(r"E:\2015_TM2_20250619\emme_project")
    dest_emme = test_dir / "emme_project"
    
    print(f"  Copying EMME project (this may take a few minutes)...")
    shutil.copytree(source_emme, dest_emme)
    print(f"  ✓ Copied EMME project to {dest_emme}")
    
    # Copy essential input files
    source_dir = Path(r"E:\2015_TM2_20250619")
    
    # Copy tolls
    shutil.copy(
        source_dir / "inputs" / "hwy" / "tolls.csv",
        test_dir / "inputs" / "hwy" / "tolls.csv"
    )
    print(f"  ✓ Copied tolls.csv")
    
    # Copy MAZ data
    shutil.copy(
        source_dir / "inputs" / "landuse" / "maz_data.csv",
        test_dir / "inputs" / "landuse" / "maz_data.csv"
    )
    print(f"  ✓ Copied maz_data.csv")
    
    print(f"\n✓ Test directory setup complete!")
    return test_dir


def run_test(test_dir, county_name):
    """Run the highway test."""
    print("\n" + "="*70)
    print("RUNNING HIGHWAY TEST")
    print("="*70)
    
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
        print("✓ TEST COMPLETED SUCCESSFULLY!")
        print("="*70)
        
        # Validate results
        success = controller.validate_results()
        if success:
            print("\n✓ Results validation passed!")
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
        return False


def main():
    parser = argparse.ArgumentParser(description="Run county highway test")
    parser.add_argument(
        "--county",
        default="San Mateo",
        help="County name (default: San Mateo)"
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Output directory for test results (e.g., C:/MyTests/san_mateo_test)"
    )
    parser.add_argument(
        "--skip-setup",
        action="store_true",
        help="Skip test directory setup (use existing)"
    )
    
    args = parser.parse_args()
    
    print("="*70)
    print(f"COUNTY HIGHWAY TEST - {args.county}")
    print("="*70)
    
    # Check prerequisites
    if not check_prerequisites(args.county):
        print("\n❌ Prerequisites not met. Please resolve issues and try again.")
        return 1
    
    # Setup test directory
    if not args.skip_setup:
        test_dir = setup_test_directory(args.county, args.output_dir)
    else:
        test_dir = Path(args.output_dir)
        print(f"\nUsing existing test directory: {test_dir}")
        if not test_dir.exists():
            print(f"❌ Test directory does not exist: {test_dir}")
            return 1
    
    # Prompt before running
    print("\n" + "="*70)
    response = input("\nReady to run test? This will take several minutes. (y/n): ")
    if response.lower() != 'y':
        print("\nTest cancelled.")
        return 0
    
    # Run test
    success = run_test(test_dir, args.county)
    
    if success:
        print(f"\nTest artifacts are in: {test_dir.absolute()}")
        print(f"  - Logs: {test_dir / 'logs'}")
        print(f"  - Loaded network: {test_dir / 'loaded_highway'}")
        print(f"  - Skims: {test_dir / 'skim_matrices' / 'highway'}")
        return 0
    else:
        print(f"\nCheck logs in: {test_dir / 'logs'}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
    """Check if all required files and directories exist."""
    print("="*70)
    print("CHECKING PREREQUISITES")
    print("="*70)
    
    issues = []
    warnings = []
    
    # Check source dataset
    source_dir = Path(r"E:\2015_TM2_20250619")
    if not source_dir.exists():
        issues.append(f"Source dataset not found: {source_dir}")
    else:
        print(f"✓ Source dataset found: {source_dir}")
    
    # Check EMME project
    emme_project = source_dir / "emme_project"
    if not emme_project.exists():
        issues.append(f"EMME project not found: {emme_project}")
    else:
        print(f"✓ EMME project found: {emme_project}")
    
    # Check EMME database
    emme_db = emme_project / "Database_highway" / "emmebank"
    if not emme_db.exists():
        issues.append(f"EMME database not found: {emme_db}")
    else:
        print(f"✓ EMME database found: {emme_db}")
    
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
            print(f"✓ {name} found: {path}")
    
    # Check config templates
    config_dir = Path(__file__).parent / "config_templates"
    if not config_dir.exists():
        issues.append(f"Config templates not found: {config_dir}")
    else:
        print(f"✓ Config templates found: {config_dir}")
    
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
    
    print("✓ All prerequisites met!")
    return True


def setup_test_directory(county_name):
    """Create test directory structure."""
    print("\n" + "="*70)
    print("SETTING UP TEST DIRECTORY")
    print("="*70)
    
    test_dir = Path(f"test_{county_name.lower().replace(' ', '_')}")
    print(f"\nTest directory: {test_dir.absolute()}")
    
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
        print(f"  ✓ Created {directory}")
    
    # Copy config templates
    config_templates = Path(__file__).parent / "config_templates"
    shutil.copy(
        config_templates / "san_mateo_scenario.toml",
        test_dir / "config" / "scenario.toml"
    )
    shutil.copy(
        config_templates / "san_mateo_model.toml",
        test_dir / "config" / "model.toml"
    )
    print(f"  ✓ Copied config files")
    
    # Copy EMME project
    source_emme = Path(r"E:\2015_TM2_20250619\emme_project")
    dest_emme = test_dir / "emme_project"
    
    if dest_emme.exists():
        print(f"  ⚠ EMME project already exists at {dest_emme}")
        print(f"    Skipping copy to avoid overwriting")
    else:
        print(f"  Copying EMME project (this may take a few minutes)...")
        shutil.copytree(source_emme, dest_emme)
        print(f"  ✓ Copied EMME project to {dest_emme}")
    
    # Copy essential input files
    source_dir = Path(r"E:\2015_TM2_20250619")
    
    # Copy tolls
    shutil.copy(
        source_dir / "inputs" / "hwy" / "tolls.csv",
        test_dir / "inputs" / "hwy" / "tolls.csv"
    )
    print(f"  ✓ Copied tolls.csv")
    
    # Copy MAZ data
    shutil.copy(
        source_dir / "inputs" / "landuse" / "maz_data.csv",
        test_dir / "inputs" / "landuse" / "maz_data.csv"
    )
    print(f"  ✓ Copied maz_data.csv")
    
    print(f"\n✓ Test directory setup complete!")
    return test_dir


def run_test(test_dir, county_name):
    """Run the highway test."""
    print("\n" + "="*70)
    print("RUNNING HIGHWAY TEST")
    print("="*70)
    
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
        print("✓ TEST COMPLETED SUCCESSFULLY!")
        print("="*70)
        
        # Validate results
        success = controller.validate_results()
        if success:
            print("\n✓ Results validation passed!")
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
        return False


def main():
    parser = argparse.ArgumentParser(description="Run county highway test")
    parser.add_argument(
        "--county",
        default="San Mateo",
        help="County name (default: San Mateo)"
    )
    parser.add_argument(
        "--skip-setup",
        action="store_true",
        help="Skip test directory setup (use existing)"
    )
    
    args = parser.parse_args()
    
    print("="*70)
    print(f"COUNTY HIGHWAY TEST - {args.county}")
    print("="*70)
    
    # Check prerequisites
    if not check_prerequisites(args.county):
        print("\n❌ Prerequisites not met. Please resolve issues and try again.")
        return 1
    
    # Setup test directory
    if not args.skip_setup:
        test_dir = setup_test_directory(args.county)
    else:
        test_dir = Path(f"test_{args.county.lower().replace(' ', '_')}")
        print(f"\nUsing existing test directory: {test_dir}")
        if not test_dir.exists():
            print(f"❌ Test directory does not exist: {test_dir}")
            return 1
    
    # Prompt before running
    print("\n" + "="*70)
    response = input("\nReady to run test? This will take several minutes. (y/n): ")
    if response.lower() != 'y':
        print("\nTest cancelled.")
        return 0
    
    # Run test
    success = run_test(test_dir, args.county)
    
    if success:
        print(f"\nTest artifacts are in: {test_dir.absolute()}")
        print(f"  - Logs: {test_dir / 'logs'}")
        print(f"  - Loaded network: {test_dir / 'loaded_highway'}")
        print(f"  - Skims: {test_dir / 'skim_matrices' / 'highway'}")
        return 0
    else:
        print(f"\nCheck logs in: {test_dir / 'logs'}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
