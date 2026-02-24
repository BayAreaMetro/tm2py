"""List Network Attributes

Lists all user-defined @ attributes in an EMME network scenario.
Useful for discovering what attributes are available in a network.

Usage:
    python tests/list_network_attributes.py <path_to_emmebank> [scenario_id]
    
Example:
    python tests/list_network_attributes.py "E:/Tests/san_mateo_test/emme/emme_project/Database_highway/emmebank" 1
"""
import argparse
import sys
from pathlib import Path

try:
    import inro.emme.database.emmebank as _eb
except ImportError:
    print("ERROR: EMME modules not available!")
    print("Run from EMME Python environment or with EMME on PATH")
    sys.exit(1)


def list_attributes(emmebank_path: str, scenario_id: int = None):
    """List all attributes in an EMME network."""
    
    emmebank_path = Path(emmebank_path)
    if emmebank_path.is_dir():
        emmebank_path = emmebank_path / "emmebank"
    
    if not emmebank_path.exists():
        print(f"ERROR: Database not found: {emmebank_path}")
        sys.exit(1)

    print("="*80)
    print("EMME NETWORK ATTRIBUTES")
    print("="*80)
    print(f"Database: {emmebank_path}")
    
    bank = _eb.Emmebank(str(emmebank_path))
    
    # Get scenario
    scenarios = list(bank.scenarios())
    if not scenarios:
        print("ERROR: No scenarios in database")
        sys.exit(1)
    
    if scenario_id:
        scenario = bank.scenario(scenario_id)
        if not scenario:
            print(f"ERROR: Scenario {scenario_id} not found")
            print(f"Available: {[s.id for s in scenarios]}")
            sys.exit(1)
    else:
        scenario = scenarios[0]
    
    print(f"Scenario: {scenario.id} - {scenario.title}")
    print()
    
    network = scenario.get_network()

    print("="*80)
    print("LINK ATTRIBUTES (user-defined @ attributes)")
    print("="*80)

    sample_link = next(iter(network.links()))
    user_attrs = sorted([attr for attr in dir(sample_link) if attr.startswith('@')])

    print(f"\nFound {len(user_attrs)} user-defined attributes on links:")
    for attr in user_attrs:
        try:
            val = sample_link[attr]
            print(f"  {attr:30} = {val!r:20} (type: {type(val).__name__})")
        except Exception:
            print(f"  {attr:30} (error reading)")

    print("\n" + "="*80)
    print("STANDARD LINK ATTRIBUTES")
    print("="*80)

    standard = ['id', 'i_node', 'j_node', 'length', 'type', 
                'data1', 'data2', 'data3', 'volume_delay_func', 'num_lanes']
    for attr in standard:
        try:
            val = getattr(sample_link, attr)
            print(f"  {attr:30} = {val!r:20}")
        except Exception as e:
            print(f"  {attr:30} (not available: {e})")

    print("\n" + "="*80)
    print("NODE ATTRIBUTES")  
    print("="*80)

    sample_node = next(iter(network.nodes()))
    user_node_attrs = sorted([attr for attr in dir(sample_node) if attr.startswith('@')])

    print(f"\nFound {len(user_node_attrs)} user-defined attributes on nodes:")
    for attr in user_node_attrs:
        try:
            val = sample_node[attr]
            print(f"  {attr:30} = {val!r:20} (type: {type(val).__name__})")
        except Exception:
            print(f"  {attr:30} (error reading)")

    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print(f"Total links: {len(list(network.links())):,}")
    print(f"Total nodes: {len(list(network.nodes())):,}")
    print(f"Total zones: {len(list(network.centroids())):,}")
    print(f"Link @ attributes: {len(user_attrs)}")
    print(f"Node @ attributes: {len(user_node_attrs)}")
    print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="List all attributes in an EMME network"
    )
    parser.add_argument('emmebank_path', type=str, 
                        help='Path to EMME database (emmebank file or directory)')
    parser.add_argument('scenario_id', type=int, nargs='?', default=None,
                        help='Scenario ID to analyze (default: first scenario)')
    args = parser.parse_args()
    
    list_attributes(args.emmebank_path, args.scenario_id)
