"""County-Specific Highway Test Controller

This script provides a simplified controller for running highway network creation,
skimming, and assignment for a specific county.

This is useful for:
- Testing highway assignment with a subset of zones
- Validating network changes for a specific county
- Faster iteration during development
- Debugging highway components

Usage:
    python tests/highway_assign_skim_controller.py --config config/scenario.toml --model config/model.toml
    
Or programmatically:
    from tests.highway_assign_skim_controller import CountyHighwayController
    
    controller = CountyHighwayController(
        scenario_config="config/scenario.toml",
        model_config="config/model.toml",
        run_dir="test_county"
    )
    controller.run_highway_only()
"""

import argparse
import sys
from pathlib import Path
from typing import List, Optional

# Add parent directory to path to import tm2py
sys.path.insert(0, str(Path(__file__).parent.parent))

from tm2py.controller import RunController
from tm2py.logger import Logger


class CountyHighwayController:
    """Controller for running highway-only tests for a specific county.
    
    This controller extends the standard RunController to:
    - Run only highway network creation, skimming, and assignment
    - Work with a filtered subset of zones (specific county)
    - Provide simplified configuration and execution
    
    Attributes:
        controller: The underlying RunController instance
        run_dir: Directory where the model run will execute
        config_files: List of configuration file paths
    """
    
    # Components needed for highway network creation, skimming, and assignment
    # create_tod_scenarios: Creates time-of-day scenarios from all-day base scenario
    # and copies period-specific attributes (@useclass_am → @useclass)
    HIGHWAY_COMPONENTS = [
        "create_tod_scenarios",      # Create TOD scenarios from all-day base
        "prepare_network_highway",   # Network creation
        "highway",                   # Assignment and skimming
        "highway_maz_skim",          # MAZ-level skims (optional)
        "highway_maz_assign",        # MAZ-level assignment (optional)
        "network_summary",           # Network summary reports (optional)
    ]
    
    def __init__(
        self,
        scenario_config: str,
        model_config: str,
        run_dir: Optional[str] = None,
        include_maz_components: bool = True,
        include_network_summary: bool = True,
        county_name: Optional[str] = None
    ):
        """Initialize the County Highway Controller.
        
        Args:
            scenario_config: Path to scenario configuration TOML file
            model_config: Path to model configuration TOML file
            run_dir: Optional directory for model run. If not provided, uses
                the directory of the scenario_config file
            include_maz_components: If True, include MAZ-level skimming and assignment
            include_network_summary: If True, include network summary component
            county_name: Optional name of county (used for display in log messages)
        """
        self.config_files = [scenario_config, model_config]
        self.run_dir = Path(run_dir) if run_dir else Path(scenario_config).parent
        self.include_maz = include_maz_components
        self.include_network_summary = include_network_summary
        self.county_name = county_name or "County"
        
        # Determine which components to run
        components_to_run = ["create_tod_scenarios", "prepare_network_highway", "highway"]
        if include_maz_components:
            components_to_run.extend(["highway_maz_skim", "highway_maz_assign"])
        if include_network_summary:
            components_to_run.append("network_summary")
        
        # Initialize the controller
        print(f"Initializing {self.county_name} Highway Controller")
        print(f"  Config files: {self.config_files}")
        print(f"  Run directory: {self.run_dir}")
        print(f"  Components: {components_to_run}")
        
        self.controller = RunController(
            config_file=self.config_files,
            run_dir=self.run_dir,
            run_components=components_to_run
        )
        
    def run_highway_only(self):
        """Run only the highway network creation, skimming, and assignment components.
        
        This method executes the highway workflow:
        1. Prepare network (load network, apply tolls, etc.)
        2. Highway assignment and skimming
        3. (Optional) MAZ-level skimming and assignment
        4. (Optional) Network summary reports
        
        Returns:
            The completed controller object with results
        """
        print("\n" + "="*70)
        print(f"{self.county_name} Highway-Only Test")
        print("="*70)
        print(f"Running components: {self.HIGHWAY_COMPONENTS}")
        print(f"Iteration: {self.controller.config.run.start_iteration} to "
              f"{self.controller.config.run.end_iteration}")
        print("="*70 + "\n")
        
        # Run the controller
        self.controller.run()
        
        print("\n" + "="*70)
        print(f"{self.county_name} Highway Test Completed")
        print("="*70)
        self._print_results()
        
        return self.controller
    
    def _print_results(self):
        """Print summary of results."""
        print("\nCompleted components:")
        for iteration, component_name, component in self.controller.completed_components:
            print(f"  Iteration {iteration}: {component_name}")
        
        # Check for output files
        skim_dir = self.run_dir / "skim_matrices" / "highway"
        if skim_dir.exists():
            print(f"\nHighway skims written to: {skim_dir}")
            omx_files = list(skim_dir.glob("*.omx"))
            if omx_files:
                print(f"  Found {len(omx_files)} OMX files:")
                for f in omx_files[:5]:  # Show first 5
                    print(f"    - {f.name}")
                if len(omx_files) > 5:
                    print(f"    ... and {len(omx_files) - 5} more")
        
        # Check for loaded highway network
        loaded_dir = self.run_dir / "loaded_highway"
        if loaded_dir.exists():
            print(f"\nLoaded highway network written to: {loaded_dir}")
    
    def validate_results(self) -> bool:
        """Validate that expected outputs were created.
        
        Returns:
            True if all expected outputs exist, False otherwise
        """
        print("\nValidating results...")
        
        validation_passed = True
        
        # Check for highway skims
        skim_dir = self.run_dir / "skim_matrices" / "highway"
        if not skim_dir.exists():
            print(f"  ✗ Highway skim directory not found: {skim_dir}")
            validation_passed = False
        else:
            omx_files = list(skim_dir.glob("*.omx"))
            if omx_files:
                print(f"  ✓ Highway skims found: {len(omx_files)} files")
            else:
                print(f"  ✗ No OMX files found in {skim_dir}")
                validation_passed = False
        
        # Check for loaded network
        loaded_dir = self.run_dir / "loaded_highway"
        if not loaded_dir.exists():
            print(f"  ✗ Loaded highway directory not found: {loaded_dir}")
            validation_passed = False
        else:
            print(f"  ✓ Loaded highway network found")
        
        if validation_passed:
            print("\n✓ All validations passed!")
        else:
            print("\n✗ Some validations failed")
        
        return validation_passed


def main():
    """Command-line interface for County Highway Controller."""
    parser = argparse.ArgumentParser(
        description="Run highway network creation, skimming, and assignment for a specific county",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with config files
  python san_mateo_controller.py -s config/scenario.toml -m config/model.toml
  
  # Run with custom output directory
  python san_mateo_controller.py -s config/scenario.toml -m config/model.toml -d test_output
  
  # Run without MAZ components
  python san_mateo_controller.py -s config/scenario.toml -m config/model.toml --no-maz
"""
    )
    
    parser.add_argument(
        '-s', '--scenario',
        required=True,
        help='Path to scenario configuration TOML file'
    )
    
    parser.add_argument(
        '-m', '--model',
        required=True,
        help='Path to model configuration TOML file'
    )
    
    parser.add_argument(
        '-d', '--dir',
        dest='run_dir',
        help='Run directory (default: directory of scenario config)'
    )
    
    parser.add_argument(
        '--no-maz',
        action='store_true',
        help='Exclude MAZ-level skimming and assignment components'
    )
    
    parser.add_argument(
        '--no-network-summary',
        action='store_true',
        help='Exclude network summary component'
    )
    
    parser.add_argument(
        '--county',
        dest='county_name',
        help='Name of the county (for display purposes)'
    )
    
    parser.add_argument(
        '--validate-only',
        action='store_true',
        help='Only validate results, do not run model'
    )
    
    args = parser.parse_args()
    
    # Create controller
    controller = CountyHighwayController(
        scenario_config=args.scenario,
        model_config=args.model,
        run_dir=args.run_dir,
        include_maz_components=not args.no_maz,
        include_network_summary=not args.no_network_summary,
        county_name=args.county_name
    )
    
    if args.validate_only:
        # Just validate existing results
        success = controller.validate_results()
        sys.exit(0 if success else 1)
    else:
        # Run the model
        try:
            controller.run_highway_only()
            
            # Validate results
            success = controller.validate_results()
            sys.exit(0 if success else 1)
            
        except Exception as e:
            print(f"\n✗ Error during execution: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)


if __name__ == "__main__":
    main()
