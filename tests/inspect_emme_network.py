"""Inspect EMME Network Database

This script examines an EMME database to discover:
- Available scenarios and their IDs
- Network size (nodes, links, zones)
- Available link attributes
- Functional type distribution
- Mode definitions

Usage:
    From EMME Python environment:
    python tests/inspect_emme_network.py "M:/Development/Travel Model Two/Supply/Network Creation 2025/from_OSM/SanMateo/7_scenario/emme/emme_project/Database_highway/emmebank"
"""

import argparse
import sys
from pathlib import Path
from collections import Counter, defaultdict

def inspect_network(emmebank_path):
    """Inspect EMME database and report findings."""
    
    try:
        import inro.emme.database.emmebank as _eb
    except ImportError:
        print("ERROR: EMME modules not available!")
        print("You must run this script from an EMME Python environment.")
        sys.exit(1)
    
    emmebank_path = Path(emmebank_path)
    if not emmebank_path.exists():
        print(f"ERROR: Database not found: {emmebank_path}")
        sys.exit(1)
    
    print("="*80)
    print("EMME NETWORK INSPECTION")
    print("="*80)
    print(f"Database: {emmebank_path}")
    print()
    
    # Open the database
    try:
        bank = _eb.Emmebank(str(emmebank_path))
    except Exception as e:
        print(f"ERROR: Could not open database: {e}")
        sys.exit(1)
    
    print("-"*80)
    print("DATABASE INFO")
    print("-"*80)
    print(f"Title: {getattr(bank, 'title', 'Unknown')}")
    print(f"Path: {bank.path}")
    print(f"Dimensions: {bank.dimensions}")
    print()
    
    # List scenarios
    print("-"*80)
    print("SCENARIOS")
    print("-"*80)
    scenarios = list(bank.scenarios())
    
    if not scenarios:
        print("WARNING: No scenarios found in database!")
        print()
        return
    
    print(f"Found {len(scenarios)} scenario(s):\n")
    
    for scen in scenarios:
        print(f"  ID: {scen.id}")
        print(f"  Title: {scen.title}")
        
        # Check if network exists by trying to get it
        try:
            net = scen.get_network()
            has_net = True
        except:
            has_net = False
            print(f"  Has network: False (empty scenario)")
            print()
            continue
        
        print(f"  Has network: True")
        print(f"  Network size:")
        print(f"    - Nodes: {len(list(net.nodes()))}")
        print(f"    - Links: {len(list(net.links()))}")
        print(f"    - Zones: {len(list(net.centroids()))}")
        
        # Check for modes
        modes = list(net.modes())
        print(f"    - Modes: {len(modes)} ({', '.join([m.id for m in modes[:10]])}{'...' if len(modes) > 10 else ''})")
        print()
    
    # Detailed analysis of first scenario with network
    scenario = None
    for scen in scenarios:
        try:
            net = scen.get_network()
            scenario = scen
            break
        except:
            continue
    
    if not scenario:
        print("WARNING: No scenarios with networks found!")
        return
    
    print("-"*80)
    print(f"DETAILED ANALYSIS: Scenario {scenario.id}")
    print("-"*80)
    
    net = scenario.get_network()
    
    # Analyze link attributes
    print("\nLINK ATTRIBUTES:")
    print("-"*80)
    
    # Get all link attribute names
    sample_link = next(iter(net.links()), None)
    if sample_link:
        # Get all attributes that start with @ (user-defined)
        user_attrs = [attr for attr in dir(sample_link) if attr.startswith('@')]
        
        print(f"Found {len(user_attrs)} user-defined attributes:")
        for attr in sorted(user_attrs):
            try:
                val = getattr(sample_link, attr)
                print(f"  {attr:20} = {val} (type: {type(val).__name__})")
            except:
                print(f"  {attr:20} (could not read value)")
        
        # Standard attributes
        print("\nStandard attributes:")
        standard = ['length', 'type', 'lanes', 'vdf', 'data1', 'data2', 'data3']
        for attr in standard:
            try:
                val = getattr(sample_link, attr)
                print(f"  {attr:20} = {val}")
            except:
                pass
    
    # Analyze functional types
    print("\nFUNCTIONAL TYPE DISTRIBUTION:")
    print("-"*80)
    
    ft_counter = Counter()
    ft_by_attr = defaultdict(Counter)
    
    # Try common functional type attribute names
    ft_attrs = ['@ft', '@ftype', '@functype', '@func_type', 'type', '@type']
    
    for attr_name in ft_attrs:
        ft_counter.clear()
        found = False
        
        for link in net.links():
            try:
                ft_value = getattr(link, attr_name)
                if ft_value is not None:
                    ft_counter[ft_value] += 1
                    found = True
            except:
                pass
        
        if found:
            print(f"\nAttribute: {attr_name}")
            print(f"  Unique values: {len(ft_counter)}")
            print(f"  Distribution (top 15):")
            for ft, count in sorted(ft_counter.items())[:15]:
                pct = 100 * count / len(list(net.links()))
                print(f"    {ft:10} : {count:6} links ({pct:5.1f}%)")
            
            ft_by_attr[attr_name] = ft_counter.copy()
    
    if not ft_by_attr:
        print("  WARNING: Could not find functional type attribute!")
        print("  Common names to check: @ft, @ftype, @functype, type")
    
    # Analyze zones
    print("\nZONE ANALYSIS:")
    print("-"*80)
    
    centroids = list(net.centroids())
    if centroids:
        zone_ids = [c.id for c in centroids]
        print(f"  Total zones: {len(zone_ids)}")
        print(f"  Zone range: {min(zone_ids)} to {max(zone_ids)}")
        
        # Check if these look like TAZs or MAZs
        if max(zone_ids) > 10000:
            print(f"  → Appears to be MAZ-level network (zones > 10000)")
        else:
            print(f"  → Appears to be TAZ-level network (zones < 10000)")
    else:
        print("  No centroids found!")
    
    # Analyze modes
    print("\nMODE ANALYSIS:")
    print("-"*80)
    
    modes = list(net.modes())
    print(f"  Total modes: {len(modes)}")
    
    for mode in modes[:20]:  # Show first 20
        print(f"    {mode.id:10} : {mode.description or '(no description)'}")
    
    if len(modes) > 20:
        print(f"    ... and {len(modes) - 20} more")
    
    print("\n" + "="*80)
    print("INSPECTION COMPLETE")
    print("="*80)
    
    # Summary recommendations
    print("\nRECOMMENDATIONS:")
    print("-"*80)
    
    if scenarios:
        print(f"✓ Use scenario ID: {scenarios[0].id}")
    
    if ft_by_attr:
        # Find the attribute with most diversity
        best_attr = max(ft_by_attr.keys(), key=lambda k: len(ft_by_attr[k]))
        print(f"✓ Functional type attribute appears to be: {best_attr}")
        print(f"  (Has {len(ft_by_attr[best_attr])} unique values)")
    else:
        print("⚠ Need to identify functional type attribute manually")
    
    if centroids:
        if max([c.id for c in centroids]) > 10000:
            print("⚠ MAZ-level network detected - you'll need MAZ data")
        else:
            print("✓ TAZ-level network - can proceed with TAZ-only setup")
    
    print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Inspect EMME network database",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        'emmebank_path',
        type=str,
        help='Path to the EMME database (emmebank file or its parent directory)'
    )
    
    args = parser.parse_args()
    
    # If directory provided, append 'emmebank'
    path = Path(args.emmebank_path)
    if path.is_dir():
        path = path / "emmebank"
    
    inspect_network(path)
