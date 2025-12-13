"""Example: Running County Highway Test Programmatically

This example shows how to use the County Highway Test Framework
programmatically in your own scripts.
"""

from pathlib import Path
import sys

# Add the tests directory to the path
sys.path.insert(0, str(Path(__file__).parent))

from highway_assign_skim_controller import CountyHighwayController
from test_highway_assign_skim import CountyDataFilter, setup_county_test_data, get_county_zones


def example_1_basic_run():
    """Example 1: Basic run with existing config files."""
    print("="*70)
    print("Example 1: Basic Highway Test Run")
    print("="*70)
    
    # Create controller
    controller = CountyHighwayController(
        scenario_config="test_county/config/county_scenario.toml",
        model_config="test_county/config/county_model.toml",
        run_dir="test_county",
        county_name="San Mateo"  # Specify county name
    )
    
    # Run highway components
    controller.run_highway_only()
    
    # Validate results
    success = controller.validate_results()
    
    if success:
        print("\n✓ Test completed successfully!")
    else:
        print("\n✗ Test completed with validation errors")
    
    return success


def example_2_with_data_filtering():
    """Example 2: Set up test with data filtering using auto-detected zones."""
    print("="*70)
    print("Example 2: Setup with Auto-Detected Zones")
    print("="*70)
    
    COUNTY_NAME = "San Mateo"  # Change to your county
    
    # Auto-detect zone ranges from crosswalk file
    try:
        zones = get_county_zones(COUNTY_NAME)
        COUNTY_TAZ_RANGE = zones['taz_range']
        COUNTY_MAZ_RANGE = zones['maz_range']
    except Exception as e:
        print(f"✗ Could not auto-detect zones: {e}")
        print("  Falling back to manual specification")
        COUNTY_TAZ_RANGE = (200, 400)
        COUNTY_MAZ_RANGE = (2000, 4000)
    
    # Create data filter
    filter_helper = CountyDataFilter(
        taz_range=COUNTY_TAZ_RANGE,
        maz_range=COUNTY_MAZ_RANGE,
        county_name=COUNTY_NAME
    )
    
    # Set up paths
    source_dir = Path("path/to/full/model/inputs")  # UPDATE THIS
    test_dir = Path(f"test_{COUNTY_NAME.lower().replace(' ', '_')}")
    
    # Filter data to county
    if source_dir.exists():
        print(f"\nFiltering data from {source_dir}...")
        setup_county_test_data(source_dir, test_dir, filter_helper)
        print("✓ Data filtering complete")
    else:
        print(f"✗ Source directory not found: {source_dir}")
        print("  Update the source_dir path in this example")
        return False
    
    # Now run the test
    controller = CountyHighwayController(
        scenario_config=str(test_dir / "config" / "county_scenario.toml"),
        model_config=str(test_dir / "config" / "county_model.toml"),
        run_dir=str(test_dir),
        county_name=COUNTY_NAME
    )
    
    controller.run_highway_only()
    success = controller.validate_results()
    
    return success


def example_3_custom_configuration():
    """Example 3: Run with custom configuration (no MAZ components, no network summary)."""
    print("="*70)
    print("Example 3: Custom Configuration")
    print("="*70)
    
    # Create controller without MAZ components and network summary for faster testing
    controller = CountyHighwayController(
        scenario_config="test_county/config/county_scenario.toml",
        model_config="test_county/config/county_model.toml",
        run_dir="test_county",
        include_maz_components=False,  # Skip MAZ components
        include_network_summary=False,  # Skip network summary
        county_name="San Mateo"
    )
    
    print("\nRunning highway test WITHOUT MAZ components or network summary...")
    print("This is faster and good for initial testing\n")
    
    controller.run_highway_only()
    success = controller.validate_results()
    
    return success


def example_4_filter_specific_files():
    """Example 4: Filter specific input files."""
    print("="*70)
    print("Example 4: Filter Specific Input Files")
    print("="*70)
    
    # Define zone ranges
    COUNTY_TAZ_RANGE = (200, 400)
    COUNTY_MAZ_RANGE = (2000, 4000)
    COUNTY_NAME = "San Mateo"
    
    filter_helper = CountyDataFilter(
        taz_range=COUNTY_TAZ_RANGE,
        maz_range=COUNTY_MAZ_RANGE,
        county_name=COUNTY_NAME
    )
    
    # Filter a specific trip table (OMX format)
    input_trips = Path("full_model/demand/trips_AM.omx")
    output_trips = Path(f"test_{COUNTY_NAME.lower().replace(' ', '_')}/inputs/demand/trips_AM_county.omx")
    
    if input_trips.exists():
        print(f"Filtering {input_trips}...")
        filter_helper.filter_trip_table(input_trips, output_trips)
        print(f"✓ Filtered trips saved to {output_trips}")
    else:
        print(f"✗ Input file not found: {input_trips}")
    
    # Filter MAZ data
    input_maz = Path("full_model/landuse/maz_data.csv")
    output_maz = Path(f"test_{COUNTY_NAME.lower().replace(' ', '_')}/inputs/landuse/maz_data.csv")
    
    if input_maz.exists():
        print(f"\nFiltering {input_maz}...")
        filter_helper.filter_maz_data(input_maz, output_maz)
        print(f"✓ Filtered MAZ data saved to {output_maz}")
    else:
        print(f"✗ Input file not found: {input_maz}")


def example_5_compare_scenarios():
    """Example 5: Run and compare multiple scenarios."""
    print("="*70)
    print("Example 5: Compare Multiple Scenarios")
    print("="*70)
    
    scenarios = [
        {
            'name': 'baseline',
            'scenario_config': 'test_county/config_baseline/county_scenario.toml',
            'model_config': 'test_county/config_baseline/county_model.toml',
            'run_dir': 'test_county/baseline'
        },
        {
            'name': 'alternative',
            'scenario_config': 'test_county/config_alt/county_scenario.toml',
            'model_config': 'test_county/config_alt/county_model.toml',
            'run_dir': 'test_county/alternative'
        }
    ]
    
    results = {}
    
    for scenario in scenarios:
        print(f"\n--- Running {scenario['name']} scenario ---")
        
        controller = CountyHighwayController(
            scenario_config=scenario['scenario_config'],
            model_config=scenario['model_config'],
            run_dir=scenario['run_dir'],
            county_name="San Mateo"
        )
        
        try:
            controller.run_highway_only()
            success = controller.validate_results()
            results[scenario['name']] = {
                'success': success,
                'controller': controller
            }
        except Exception as e:
            print(f"✗ Error running {scenario['name']}: {e}")
            results[scenario['name']] = {
                'success': False,
                'error': str(e)
            }
    
    # Print summary
    print("\n" + "="*70)
    print("Scenario Comparison Summary")
    print("="*70)
    
    for name, result in results.items():
        status = "✓ PASS" if result.get('success') else "✗ FAIL"
        print(f"{name:20s}: {status}")
    
    # Compare outputs (example with openmatrix)
    try:
        import openmatrix as omx
        import numpy as np
        
        print("\n--- Comparing Highway Skims (AM Period) ---")
        
        base_skim = Path(results['baseline']['controller'].run_dir) / "skim_matrices/highway/HWYSKIM_AM.omx"
        alt_skim = Path(results['alternative']['controller'].run_dir) / "skim_matrices/highway/HWYSKIM_AM.omx"
        
        if base_skim.exists() and alt_skim.exists():
            with omx.open_file(str(base_skim), 'r') as base_f:
                with omx.open_file(str(alt_skim), 'r') as alt_f:
                    # Compare a specific matrix (e.g., SOV travel time)
                    if "SOV_TIME" in base_f.list_matrices():
                        base_time = base_f["SOV_TIME"][:]
                        alt_time = alt_f["SOV_TIME"][:]
                        
                        diff = alt_time - base_time
                        diff_nonzero = diff[diff != 0]
                        
                        if len(diff_nonzero) > 0:
                            print(f"  Average time difference: {np.mean(diff_nonzero):.2f} minutes")
                            print(f"  Max time increase: {np.max(diff_nonzero):.2f} minutes")
                            print(f"  Max time decrease: {np.min(diff_nonzero):.2f} minutes")
                        else:
                            print("  No differences found between scenarios")
        
    except ImportError:
        print("  (openmatrix not available for comparison)")
    except Exception as e:
        print(f"  Error comparing scenarios: {e}")


def example_6_validation_only():
    """Example 6: Validate existing results without re-running."""
    print("="*70)
    print("Example 6: Validation Only")
    print("="*70)
    
    controller = CountyHighwayController(
        scenario_config="test_county/config/county_scenario.toml",
        model_config="test_county/config/county_model.toml",
        run_dir="test_county",
        county_name="San Mateo"
    )
    
    print("\nValidating existing results (not re-running model)...")
    success = controller.validate_results()
    
    if success:
        print("\n✓ All validation checks passed")
    else:
        print("\n✗ Some validation checks failed")
    
    # Can also check specific outputs
    skim_dir = Path(controller.run_dir) / "skim_matrices" / "highway"
    if skim_dir.exists():
        omx_files = list(skim_dir.glob("*.omx"))
        print(f"\nFound {len(omx_files)} OMX skim files:")
        for f in omx_files:
            print(f"  - {f.name} ({f.stat().st_size / 1024 / 1024:.2f} MB)")
    
    return success


def example_7_multi_county():
    """Example 7: Demonstrate multi-county auto-detection."""
    print("="*70)
    print("Example 7: Multi-County Auto-Detection Demo")
    print("="*70)
    
    # List of counties to check
    counties = ["San Mateo", "Alameda", "Santa Clara", "San Francisco"]
    
    print("\nAuto-detecting zone ranges for multiple counties:\n")
    
    for county in counties:
        try:
            zones = get_county_zones(county)
            print(f"{county:15s}: TAZ {zones['taz_range'][0]:4d}-{zones['taz_range'][1]:4d}, "
                  f"MAZ {zones['maz_range'][0]:5d}-{zones['maz_range'][1]:5d}")
        except Exception as e:
            print(f"{county:15s}: Error - {e}")
    
    print("\n✓ Auto-detection makes it easy to work with any county!")


def example_8_filter_ctramp_output():
    """Example 8: Filter highway trip files from CTRAMP output directory."""
    print("="*70)
    print("Example 8: Filter CTRAMP Output Trip Files (Recommended)")
    print("="*70)
    
    from test_highway_assign_skim import filter_ctramp_highway_trips
    
    # Define county
    COUNTY_NAME = "San Mateo"
    
    # Auto-detect zones
    print(f"\nAuto-detecting zones for {COUNTY_NAME}...")
    zones = get_county_zones(COUNTY_NAME)
    
    # Create filter
    filter_helper = CountyDataFilter(
        county_name=COUNTY_NAME,
        taz_range=zones['taz_range'],
        maz_range=zones['maz_range']
    )
    
    # Paths
    ctramp_output_dir = Path(r"E:\2023-tm22-dev-version-05\ctramp_output")  # UPDATE THIS
    test_dir = Path(f"test_{COUNTY_NAME.lower().replace(' ', '_')}")
    
    # Filter trip files
    if ctramp_output_dir.exists():
        print(f"\nFiltering highway trip files from CTRAMP output...")
        print(f"  Source: {ctramp_output_dir}")
        print(f"  Destination: {test_dir / 'inputs' / 'demand'}")
        
        filter_ctramp_highway_trips(
            ctramp_output_dir=ctramp_output_dir,
            test_demand_dir=test_dir / "inputs" / "demand",
            filter_helper=filter_helper,
            time_periods=['EA', 'AM', 'MD', 'PM', 'EV']  # Optional: filter specific time periods
        )
        
        print("\n✓ Trip files filtered successfully!")
        print(f"\nYou can now run the test with these filtered trips:")
        print(f"  python tests/highway_assign_skim_controller.py \\")
        print(f"    --county \"{COUNTY_NAME}\" \\")
        print(f"    --scenario {test_dir}/config/scenario.toml \\")
        print(f"    --model-config {test_dir}/config/model.toml")
    else:
        print(f"\n✗ CTRAMP output directory not found: {ctramp_output_dir}")
        print(f"\nUpdate the path in this example to point to your CTRAMP output directory.")
        print(f"It should contain OMX files with trip matrices by time period and mode.")


if __name__ == "__main__":
    """
    Run examples based on command line argument
    """
    import argparse
    
    parser = argparse.ArgumentParser(description="Highway Assignment Test Examples")
    parser.add_argument(
        'example',
        type=int,
        nargs='?',
        default=1,
        help='Example number to run (1-8), default is 1'
    )
    
    args = parser.parse_args()
    
    examples = {
        1: example_1_basic_run,
        2: example_2_with_data_filtering,
        3: example_3_custom_configuration,
        4: example_4_filter_specific_files,
        5: example_5_compare_scenarios,
        6: example_6_validation_only,
        7: example_7_multi_county,
        8: example_8_filter_ctramp_output,
    }
    
    if args.example in examples:
        print(f"\nRunning Example {args.example}\n")
        examples[args.example]()
    else:
        print(f"Error: Example {args.example} not found")
        print(f"Available examples: {', '.join(map(str, examples.keys()))}")
        print("\nExample descriptions:")
        print("  1: Basic run with existing config files")
        print("  2: Setup with data filtering from full model")
        print("  3: Custom configuration (no MAZ components)")
        print("  4: Filter specific input files")
        print("  5: Run and compare multiple scenarios")
        print("  6: Validate existing results without re-running")
        print("  7: Multi-county auto-detection demo")
        print("  8: Filter trip files from CTRAMP output (recommended)")
