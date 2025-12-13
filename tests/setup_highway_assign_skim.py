"""Setup utility for County Highway Assignment and Skimming Test Framework

This script helps initialize the test framework by:
1. Creating the necessary directory structure
2. Copying template configuration files
3. Auto-detecting county zones from crosswalk file
4. Optionally filtering input data to county subset

Usage:
    # Interactive setup
    python tests/setup_highway_assign_skim.py
    
    # Automated setup with parameters
    python tests/setup_highway_assign_skim.py --county "San Mateo" --test-dir test_san_mateo
"""

import argparse
import shutil
from pathlib import Path
from typing import Optional, Tuple
import sys

# Add parent directory to import tm2py modules
sys.path.insert(0, str(Path(__file__).parent.parent))


def create_directory_structure(test_dir: Path) -> None:
    """Create the directory structure for the test.
    
    Args:
        test_dir: Root directory for the test
    """
    print(f"\nCreating directory structure in {test_dir}...")
    
    directories = [
        test_dir / "config",
        test_dir / "inputs" / "hwy",
        test_dir / "inputs" / "landuse",
        test_dir / "inputs" / "demand",
        test_dir / "emme_project" / "Database_highway",
        test_dir / "skim_matrices" / "highway",
        test_dir / "loaded_highway",
        test_dir / "logs",
    ]
    
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)
        print(f"  ✓ Created {directory}")


def copy_config_templates(test_dir: Path, template_dir: Optional[Path] = None) -> None:
    """Copy configuration template files to the test directory.
    
    Args:
        test_dir: Root directory for the test
        template_dir: Directory containing template files (auto-detected if None)
    """
    if template_dir is None:
        template_dir = Path(__file__).parent / "config_templates"
    
    print(f"\nCopying configuration templates...")
    
    config_files = [
        "san_mateo_scenario.toml",
        "san_mateo_model.toml"
    ]
    
    for config_file in config_files:
        src = template_dir / config_file
        dst = test_dir / "config" / config_file
        
        if src.exists():
            shutil.copy(src, dst)
            print(f"  ✓ Copied {config_file}")
        else:
            print(f"  ✗ Template not found: {src}")


def identify_county_zones_from_crosswalk(county_name: str) -> Tuple[Tuple[int, int], Tuple[int, int]]:
    """Identify TAZ and MAZ ranges for a county from crosswalk file.
    
    Args:
        county_name: Name of the county
        
    Returns:
        Tuple of ((taz_min, taz_max), (maz_min, maz_max))
    """
    try:
        zones = get_county_zones(county_name)
        return zones['taz_range'], zones['maz_range']
    except Exception as e:
        print(f"  ✗ Error auto-detecting zones: {e}")
        return None
    
    Args:
        maz_data_file: Path to maz_data.csv file
        
    Returns:
        Tuple of ((taz_min, taz_max), (maz_min, maz_max))
    """
    try:
        import pandas as pd
        
        print(f"\nAnalyzing {maz_data_file} to identify San Mateo County zones...")
        
        df = pd.read_csv(maz_data_file)
        
        # Look for county column (common names)
        county_col = None
        for col in ['county', 'COUNTY', 'County', 'county_name', 'COUNTY_NAME']:
            if col in df.columns:
                county_col = col
                break
        
        if county_col is None:
            print("  ✗ Could not find county column in maz_data.csv")
            print(f"  Available columns: {', '.join(df.columns)}")
            return None
        
        # Filter to San Mateo County
        san_mateo_mask = df[county_col].str.contains('San Mateo|San_Mateo|SAN_MATEO', 
                                                       case=False, na=False)
        san_mateo_df = df[san_mateo_mask]
        
        if len(san_mateo_df) == 0:
            print(f"  ✗ No San Mateo County zones found")
            print(f"  Unique county values: {df[county_col].unique()[:10]}")
            return None
        
        # Get TAZ range (if TAZ column exists)
        taz_col = None
        for col in ['TAZ', 'taz', 'TAZ_ID', 'taz_id']:
            if col in df.columns:
                taz_col = col
                break
        
        if taz_col:
            taz_min = int(san_mateo_df[taz_col].min())
            taz_max = int(san_mateo_df[taz_col].max())
        else:
            taz_min, taz_max = 0, 0
            print("  ⚠ TAZ column not found")
        
        # Get MAZ range
        maz_col = None
        for col in ['MAZ', 'maz', 'MAZ_ID', 'maz_id', 'MAZ_ORIGINAL', 'ZONE']:
            if col in df.columns:
                maz_col = col
                break
        
        if maz_col:
            maz_min = int(san_mateo_df[maz_col].min())
            maz_max = int(san_mateo_df[maz_col].max())
        else:
            print("  ✗ MAZ column not found")
            return None
        
        print(f"\n  ✓ Found {len(san_mateo_df)} San Mateo County zones")
        if taz_col:
            print(f"  TAZ range: {taz_min} - {taz_max}")
        print(f"  MAZ range: {maz_min} - {maz_max}")
        
        return ((taz_min, taz_max), (maz_min, maz_max))
        
    except ImportError:
        print("  ✗ pandas not available. Cannot auto-detect zones.")
        return None
    except Exception as e:
        print(f"  ✗ Error analyzing maz_data.csv: {e}")
        return None


def update_config_with_zones(
    config_file: Path, 
    taz_range: Tuple[int, int], 
    maz_range: Tuple[int, int]
) -> None:
    """Update configuration file with detected zone ranges.
    
    Args:
        config_file: Path to configuration file to update
        taz_range: Tuple of (taz_min, taz_max)
        maz_range: Tuple of (maz_min, maz_max)
    """
    # For now, just print the ranges - user can update manually
    # In the future, could use TOML library to update programmatically
    pass


def filter_county_data(
    source_dir: Path,
    test_dir: Path,
    county_name: str,
    taz_range: Tuple[int, int],
    maz_range: Tuple[int, int]
) -> None:
    """Filter model input data to specified county.
    
    Args:
        source_dir: Source directory with full model data
        test_dir: Test directory where filtered data will be written
        county_name: Name of the county
        taz_range: Tuple of (taz_min, taz_max)
        maz_range: Tuple of (maz_min, maz_max)
    """
    try:
        from test_highway_assign_skim import CountyDataFilter, setup_county_test_data, get_county_zones
        
        print(f"\nFiltering data from {source_dir} to {test_dir}...")
        
        filter_helper = CountyDataFilter(county_name, taz_range, maz_range)
        setup_county_test_data(source_dir, test_dir, county_name, taz_range, maz_range)
        
        print("  ✓ Data filtering complete")
        
    except ImportError as e:
        print(f"  ✗ Could not import filtering utilities: {e}")
    except Exception as e:
        print(f"  ✗ Error during data filtering: {e}")


def interactive_setup() -> dict:
    """Interactive setup mode - prompts user for configuration.
    
    Returns:
        Dictionary of setup parameters
    """
    print("\n" + "="*70)
    print("County Highway Test Framework - Interactive Setup")
    print("="*70)
    
    params = {}
    
    # Test directory
    test_dir_default = "test_san_mateo"
    test_dir_input = input(f"\nTest directory [{test_dir_default}]: ").strip()
    params['test_dir'] = Path(test_dir_input if test_dir_input else test_dir_default)
    
    # Get county name
    county_input = input(f"\nCounty name [San Mateo]: ").strip()
    params['county_name'] = county_input if county_input else "San Mateo"
    
    # Ask about auto-detection from crosswalk
    print(f"\nDo you want to auto-detect {params['county_name']} County zones from crosswalk?")
    print("(Uses TAZ/MAZ/County crosswalk file)")
    auto_detect = input("Auto-detect zones? [Y/n]: ").strip().lower()
    
    if auto_detect not in ['n', 'no']:
        # Try auto-detection
        try:
            detected = identify_county_zones_from_crosswalk(params['county_name'])
            if detected:
                params['taz_range'], params['maz_range'] = detected
            else:
                params['taz_range'] = None
                params['maz_range'] = None
        except:
            params['taz_range'] = None
            params['maz_range'] = None
    else:
        params['taz_range'] = None
        params['maz_range'] = None
        
    # Manual zone entry if auto-detection failed or skipped
    if not params.get('taz_range'):
        print(f"\nEnter {params['county_name']} County zone ranges manually:")
        try:
            taz_min = int(input("  TAZ minimum (or 0 if unknown): "))
            taz_max = int(input("  TAZ maximum (or 0 if unknown): "))
            maz_min = int(input("  MAZ minimum: "))
            maz_max = int(input("  MAZ maximum: "))
            
            params['taz_range'] = (taz_min, taz_max)
            params['maz_range'] = (maz_min, maz_max)
        except ValueError:
            print("  ✗ Invalid zone range input")
            params['taz_range'] = None
            params['maz_range'] = None
    
    # Ask about data filtering
    print("\nData Filtering Options:")
    print("  1. Filter trip files from CTRAMP output (recommended)")
    print("  2. Filter full model input data")
    print("  3. Skip filtering (I'll provide trip files manually)")
    filter_choice = input("\nChoose option [1/2/3]: ").strip()
    
    if filter_choice == '1':
        ctramp_dir_input = input("Path to CTRAMP output directory (e.g., E:/2023-tm22-dev-version-05/ctramp_output): ").strip()
        params['ctramp_output_dir'] = Path(ctramp_dir_input) if ctramp_dir_input else None
        params['source_dir'] = None
    elif filter_choice == '2':
        source_dir_input = input("Path to full model inputs directory: ").strip()
        params['source_dir'] = Path(source_dir_input) if source_dir_input else None
        params['ctramp_output_dir'] = None
    else:
        params['source_dir'] = None
        params['ctramp_output_dir'] = None
    
    return params


def main():
    """Main setup function."""
    parser = argparse.ArgumentParser(
        description="Setup County Highway Assignment and Skimming Test Framework",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    
    parser.add_argument(
        '--county',
        type=str,
        default="San Mateo",
        help='Name of the county (default: San Mateo)'
    )
    
    parser.add_argument(
        '--test-dir',
        type=Path,
        help='Directory for test setup (default: test_{county_name})'
    )
    
    parser.add_argument(
        '--source-dir',
        type=Path,
        help='Source directory with full model data to filter'
    )
    
    parser.add_argument(
        '--taz-min',
        type=int,
        help='Minimum TAZ number (auto-detected if not provided)'
    )
    
    parser.add_argument(
        '--taz-max',
        type=int,
        help='Maximum TAZ number (auto-detected if not provided)'
    )
    
    parser.add_argument(
        '--maz-min',
        type=int,
        help='Minimum MAZ number (auto-detected if not provided)'
    )
    
    parser.add_argument(
        '--maz-max',
        type=int,
        help='Maximum MAZ number (auto-detected if not provided)'
    )
    
    parser.add_argument(
        '--interactive',
        action='store_true',
        help='Interactive setup mode'
    )
    
    args = parser.parse_args()
    
    # Interactive mode
    if args.interactive or len(sys.argv) == 1:
        params = interactive_setup()
        test_dir = params['test_dir']
        source_dir = params.get('source_dir')
        ctramp_output_dir = params.get('ctramp_output_dir')
        taz_range = params.get('taz_range')
        maz_range = params.get('maz_range')
        county_name = params.get('county_name', 'San Mateo')
    else:
        # Command-line mode
        test_dir = args.test_dir
        source_dir = args.source_dir
        ctramp_output_dir = args.ctramp_output
        county_name = args.county or "San Mateo"
        
        if args.taz_min and args.taz_max:
            taz_range = (args.taz_min, args.taz_max)
        else:
            taz_range = None
        
        if args.maz_min and args.maz_max:
            maz_range = (args.maz_min, args.maz_max)
        else:
            maz_range = None
    
    # Execute setup steps
    print("\n" + "="*70)
    print("Starting Setup")
    print("="*70)
    
    # Step 1: Create directories
    create_directory_structure(test_dir)
    
    # Step 2: Copy config templates
    copy_config_templates(test_dir)
    
    # Step 3: Auto-detect zones if county specified
    county_name = params.get('county_name', 'San Mateo')
    if taz_range is None or maz_range is None:
        try:
            print(f"\nAttempting auto-detection for {county_name} County...")
            zones = identify_county_zones_from_crosswalk(county_name)
            taz_range = (zones['taz_min'], zones['taz_max'])
            maz_range = (zones['maz_min'], zones['maz_max'])
            print(f"✓ Auto-detected: TAZ {taz_range[0]}-{taz_range[1]}, MAZ {maz_range[0]}-{maz_range[1]}")
        except Exception as e:
            print(f"✗ Auto-detection failed: {e}")
            if not (taz_range and maz_range):
                print("Cannot proceed without zone ranges")
                return
    
    # Step 4: Filter data if requested
    if ctramp_output_dir and ctramp_output_dir.exists() and taz_range and maz_range:
        print("\nFiltering CTRAMP output trip files...")
        from test_highway_assign_skim import CountyDataFilter, filter_ctramp_highway_trips
        
        filter_helper = CountyDataFilter(county_name, taz_range, maz_range)
        filter_ctramp_highway_trips(
            ctramp_output_dir,
            test_dir / "inputs" / "demand",
            filter_helper
        )
    elif source_dir and source_dir.exists() and taz_range and maz_range:
        print("\nFiltering full model input data...")
        filter_county_data(source_dir, test_dir, county_name, taz_range, maz_range)
    
    # Summary
    print("\n" + "="*70)
    print("Setup Complete!")
    print("="*70)
    print(f"\nTest directory: {test_dir.absolute()}")
    
    if taz_range and maz_range:
        print(f"\nDetected/configured zone ranges:")
        if taz_range[0] > 0:
            print(f"  TAZ: {taz_range[0]} - {taz_range[1]}")
        print(f"  MAZ: {maz_range[0]} - {maz_range[1]}")
        
        print(f"\n⚠ Update these ranges in:")
        print(f"  - tests/test_highway_assign_skim.py")
    
    print(f"\nNext steps:")
    if ctramp_output_dir:
        print(f"  1. Verify filtered trip files in {test_dir}/inputs/demand/")
    else:
        print(f"  1. Add your trip data to {test_dir}/inputs/demand/")
    print(f"  2. Review and update config files in {test_dir}/config/")
    print(f"  3. Ensure Emme project is set up in {test_dir}/emme_project/")
    print(f"  4. Run the test:")
    print(f"     python tests/highway_assign_skim_controller.py \\")
    print(f"       --county \"{county_name}\" \\")
    print(f"       --scenario {test_dir}/config/scenario.toml \\")
    print(f"       --model-config {test_dir}/config/model.toml")
    
    print(f"\nFor detailed instructions, see:")
    print(f"  tests/SAN_MATEO_HIGHWAY_TEST_README.md")


if __name__ == "__main__":
    main()
