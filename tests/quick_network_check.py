"""Quick EMME Network Check

Fast inspection of key network properties without loading full network.

Usage:
    python tests/quick_network_check.py "M:/path/to/Database_highway"
"""

import argparse
import sys
from pathlib import Path

def quick_check(emmebank_path):
    """Quick check of EMME database."""
    
    try:
        import inro.emme.database.emmebank as _eb
    except ImportError:
        print("ERROR: EMME modules not available!")
        sys.exit(1)
    
    emmebank_path = Path(emmebank_path)
    if not emmebank_path.exists():
        print(f"ERROR: Database not found: {emmebank_path}")
        sys.exit(1)
    
    print("="*80)
    print("QUICK EMME NETWORK CHECK")
    print("="*80)
    print(f"Database: {emmebank_path}")
    print()
    
    try:
        bank = _eb.Emmebank(str(emmebank_path))
    except Exception as e:
        print(f"ERROR: Could not open database: {e}")
        sys.exit(1)
    
    # List scenarios (fast - doesn't load networks)
    print("SCENARIOS:")
    print("-"*80)
    scenarios = list(bank.scenarios())
    
    for scen in scenarios:
        print(f"  ID: {scen.id:3}  Title: {scen.title}")
    
    print()
    print(f"Total scenarios: {len(scenarios)}")
    
    if scenarios:
        print()
        print("RECOMMENDATIONS:")
        print("-"*80)
        print(f"✓ Available scenario IDs: {', '.join(str(s.id) for s in scenarios)}")
        print(f"✓ Suggested scenario to use: {scenarios[0].id} ('{scenarios[0].title}')")
        print()
        print("Next steps:")
        print("  1. Update county_test_config.toml:")
        print(f"     all_day_scenario_id = {scenarios[0].id}")
        print()
        print("  2. To inspect full network details (slow for large networks):")
        print(f"     python tests/inspect_emme_network.py \"{emmebank_path}\"")
    
    print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Quick EMME network check")
    parser.add_argument('emmebank_path', type=str, help='Path to EMME database')
    args = parser.parse_args()
    
    path = Path(args.emmebank_path)
    if path.is_dir():
        path = path / "emmebank"
    
    quick_check(path)
