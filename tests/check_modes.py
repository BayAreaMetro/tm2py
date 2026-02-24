"""Check Modes in EMME Network

Lists all modes defined in an EMME network scenario.

Usage:
    python tests/check_modes.py <path_to_emmebank> [scenario_id]
"""
import argparse
import sys
from pathlib import Path

try:
    import inro.emme.database.emmebank as _eb
except ImportError:
    print("ERROR: EMME modules not available!")
    sys.exit(1)


def check_modes(emmebank_path: str, scenario_id: int = 1):
    """Check modes in an EMME scenario."""
    
    emmebank_path = Path(emmebank_path)
    if emmebank_path.is_dir():
        emmebank_path = emmebank_path / "emmebank"
    
    if not emmebank_path.exists():
        print(f"ERROR: Database not found: {emmebank_path}")
        sys.exit(1)
    
    emmebank = _eb.Emmebank(str(emmebank_path))
    scenario = emmebank.scenario(scenario_id)
    
    if scenario:
        print(f'Scenario {scenario_id}: {scenario.title}')
        modes = list(scenario.modes())
        print(f'Number of modes: {len(modes)}')
        print('Modes:')
        for mode in modes:
            print(f'  {mode.id}: {mode.type} - {mode.description}')
    else:
        print(f'Scenario {scenario_id} not found')
        print(f'Available: {[s.id for s in emmebank.scenarios()]}')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check modes in EMME network")
    parser.add_argument('emmebank_path', type=str, help='Path to EMME database')
    parser.add_argument('scenario_id', type=int, nargs='?', default=1, help='Scenario ID')
    args = parser.parse_args()
    
    check_modes(args.emmebank_path, args.scenario_id)
