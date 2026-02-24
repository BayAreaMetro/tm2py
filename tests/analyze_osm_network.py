"""Analyze EMME Network for tm2py Compatibility

This script checks an EMME network for the required attributes needed by tm2py
highway assignment and network processing components.

REQUIRED ATTRIBUTES (will cause errors if missing):
  - @capclass: Capacity class index (used for capacity lookup)
  - @lanes: Number of lanes (used for capacity calculation)  
  - @ft: Functional type (used for VDF assignment)
  - @free_flow_speed: Free flow speed in mph
  - @drive_link: 1 for driveable links, 0 otherwise

OPTIONAL ATTRIBUTES (gracefully skipped if missing):
  - @tollbooth: Toll booth ID for bridge tolls
  - @tollseg: Toll segment for value toll lookup
  - @useclass: Vehicle use class for toll differentiation

Usage:
    python tests/analyze_osm_network.py <path_to_emmebank> [scenario_id]
    
Example:
    python tests/analyze_osm_network.py "E:/Tests/san_mateo_test/emme/emme_project/Database_highway/emmebank" 1
"""
import argparse
import sys
from pathlib import Path
from collections import Counter

try:
    import inro.emme.database.emmebank as _eb
except ImportError:
    print("ERROR: EMME modules not available!")
    print("Run from EMME Python environment or with EMME on PATH")
    sys.exit(1)


def analyze_network(emmebank_path: str, scenario_id: int = None):
    """Analyze an EMME network for tm2py compatibility."""
    
    emmebank_path = Path(emmebank_path)
    if emmebank_path.is_dir():
        emmebank_path = emmebank_path / "emmebank"
    
    if not emmebank_path.exists():
        print(f"ERROR: Database not found: {emmebank_path}")
        sys.exit(1)

    print("="*80)
    print("TM2PY NETWORK COMPATIBILITY CHECK")
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
        print(f"Using first scenario: {scenario.id}")
    
    print(f"Scenario: {scenario.id} - {scenario.title}")
    print()
    
    network = scenario.get_network()
    links = list(network.links())
    
    if not links:
        print("ERROR: No links in network")
        sys.exit(1)
    
    print(f"Network size: {len(links):,} links, {len(list(network.nodes())):,} nodes")
    print()
    
    # =========================================================================
    # CHECK REQUIRED ATTRIBUTES
    # =========================================================================
    print("="*80)
    print("REQUIRED ATTRIBUTES CHECK")
    print("="*80)
    
    required_attrs = ['@capclass', '@lanes', '@ft', '@free_flow_speed', '@drive_link']
    sample_link = links[0]
    
    missing_required = []
    present_required = []
    
    for attr in required_attrs:
        try:
            val = sample_link[attr]
            present_required.append(attr)
            
            # Get stats for this attribute
            values = [link[attr] for link in links]
            zeros = sum(1 for v in values if v == 0)
            non_zeros = len(values) - zeros
            
            if attr == '@lanes':
                min_val = min(v for v in values if v > 0) if non_zeros > 0 else 0
                max_val = max(values)
                print(f"  ✓ {attr:20} Found - range: {min_val:.1f} to {max_val:.1f}, zeros: {zeros:,}")
            elif attr == '@drive_link':
                ones = sum(1 for v in values if v == 1)
                print(f"  ✓ {attr:20} Found - drive_link=1: {ones:,}, drive_link=0: {zeros:,}")
            else:
                unique = len(set(values))
                print(f"  ✓ {attr:20} Found - {unique} unique values, zeros: {zeros:,}")
                
        except KeyError:
            missing_required.append(attr)
            print(f"  ✗ {attr:20} MISSING")
    
    print()
    
    # =========================================================================
    # CHECK OPTIONAL TOLL ATTRIBUTES
    # =========================================================================
    print("="*80)
    print("OPTIONAL TOLL ATTRIBUTES CHECK")
    print("="*80)
    
    toll_attrs = ['@tollbooth', '@tollseg', '@useclass']
    missing_toll = []
    
    for attr in toll_attrs:
        try:
            val = sample_link[attr]
            values = [link[attr] for link in links]
            non_zeros = sum(1 for v in values if v != 0)
            print(f"  ✓ {attr:20} Found - {non_zeros:,} non-zero values")
        except KeyError:
            missing_toll.append(attr)
            print(f"  ○ {attr:20} Not present (tolls will be skipped)")
    
    print()
    
    # =========================================================================
    # ANALYZE VDF SETUP
    # =========================================================================
    print("="*80)
    print("VDF (VOLUME DELAY FUNCTION) CHECK")
    print("="*80)
    
    vdf_counter = Counter(link.volume_delay_func for link in links)
    print(f"\nVDF distribution across {len(links):,} links:")
    for vdf, count in sorted(vdf_counter.items()):
        pct = 100 * count / len(links)
        print(f"  VDF {vdf:2}: {count:8,} links ({pct:5.1f}%)")
    
    # Check if VDFs exist in emmebank
    print("\nDefined VDFs in emmebank:")
    vdfs = [f for f in bank.functions() if f.type == "VOLUME_DELAY"]
    for vdf in vdfs[:10]:
        print(f"  fd{vdf.id}: {vdf.expression[:60]}...")
    if len(vdfs) > 10:
        print(f"  ... and {len(vdfs) - 10} more")
    
    print()
    
    # =========================================================================
    # ANALYZE LANES
    # =========================================================================
    print("="*80)
    print("LANES ANALYSIS")
    print("="*80)
    
    lanes_counter = Counter(link.num_lanes for link in links)
    print(f"\nLanes distribution:")
    for lanes, count in sorted(lanes_counter.items()):
        pct = 100 * count / len(links)
        print(f"  {lanes:4.1f} lanes: {count:8,} links ({pct:5.1f}%)")
    
    # Check for zero lanes (common problem!)
    zero_lanes = sum(1 for link in links if link.num_lanes == 0)
    if zero_lanes > 0:
        print(f"\n  ⚠️  WARNING: {zero_lanes:,} links have num_lanes=0!")
        print(f"      This will cause @capacity=0 and SOLA assignment failures")
    
    print()
    
    # =========================================================================
    # SUMMARY
    # =========================================================================
    print("="*80)
    print("SUMMARY")
    print("="*80)
    
    if missing_required:
        print(f"\n❌ NETWORK NOT COMPATIBLE WITH TM2PY")
        print(f"   Missing required attributes: {', '.join(missing_required)}")
        print(f"\n   These attributes must exist in the base network.")
        print(f"   They are typically created during network preparation.")
    else:
        print(f"\n✅ NETWORK HAS ALL REQUIRED ATTRIBUTES")
    
    if missing_toll:
        print(f"\n⚠️  Missing toll attributes: {', '.join(missing_toll)}")
        print(f"   Toll processing will be skipped (this is OK for testing)")
    
    if zero_lanes > 0:
        print(f"\n⚠️  {zero_lanes:,} links have zero lanes - may cause issues")
    
    print()
    return len(missing_required) == 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Check EMME network for tm2py compatibility"
    )
    parser.add_argument('emmebank_path', type=str, 
                        help='Path to EMME database (emmebank file or directory)')
    parser.add_argument('scenario_id', type=int, nargs='?', default=None,
                        help='Scenario ID to analyze (default: first scenario)')
    args = parser.parse_args()
    
    success = analyze_network(args.emmebank_path, args.scenario_id)
    sys.exit(0 if success else 1)
