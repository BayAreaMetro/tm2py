"""Validate Highway Assignment Results

Check traffic volumes on links after highway assignment completes.
Useful for verifying that assignment ran correctly.

Usage:
    python tests/check_network_volumes.py <path_to_emmebank> [scenario_id]
    
Example:
    python tests/check_network_volumes.py "E:/Tests/san_mateo_test/emme/emme_project/Database_highway" 11
"""
import argparse
import sys
from pathlib import Path

try:
    import inro.emme.database.emmebank as _eb
except ImportError:
    print("ERROR: EMME modules not available!")
    sys.exit(1)


def check_volumes(emmebank_path: str, scenario_id: int = 11):
    """Check network volumes after assignment."""
    
    emmebank_path = Path(emmebank_path)
    if emmebank_path.is_dir():
        emmebank_path = emmebank_path / "emmebank"
    
    if not emmebank_path.exists():
        print(f"ERROR: Database not found: {emmebank_path}")
        sys.exit(1)

    print("Opening EMME database...")
    eb = _eb.Emmebank(str(emmebank_path))
    scen = eb.scenario(scenario_id)
    
    if not scen:
        print(f"ERROR: Scenario {scenario_id} not found")
        print(f"Available: {[s.id for s in eb.scenarios()]}")
        sys.exit(1)

    print(f"Loading network for scenario {scenario_id}...")
    net = scen.get_network()

    # Get all links
    links = list(net.links())
    print(f"\n{'='*60}")
    print(f"NETWORK STATISTICS - Scenario {scenario_id}")
    print(f"{'='*60}")
    print(f"Total links: {len(links):,}")

    # Check volumes
    volumes = [l.auto_volume for l in links]
    links_with_volume = [v for v in volumes if v > 0]

    print(f"Links with traffic (volume > 0): {len(links_with_volume):,}")
    print(f"Links with no traffic: {len(volumes) - len(links_with_volume):,}")

    if links_with_volume:
        print(f"\nVolume Statistics:")
        print(f"  Min volume: {min(links_with_volume):,.1f}")
        print(f"  Max volume: {max(links_with_volume):,.1f}")
        print(f"  Average volume: {sum(links_with_volume)/len(links_with_volume):,.1f}")
        print(f"  Total volume: {sum(links_with_volume):,.0f}")

    # Check specific class volumes
    print(f"\nClass-specific volumes:")
    for attr_name in ['@flow_da', '@flow_datoll', '@flow_sr2', '@flow_sr2toll', 
                      '@flow_sr3', '@flow_sr3toll', '@flow_vsm', '@flow_sml', 
                      '@flow_med', '@flow_lrg']:
        try:
            class_vols = [l[attr_name] for l in links if l[attr_name] > 0]
            if class_vols:
                print(f"  {attr_name:15}: {len(class_vols):6,} links, total = {sum(class_vols):12,.0f}")
        except (KeyError, AttributeError):
            pass

    # Sample some high-volume links
    print(f"\nTop 10 highest volume links:")
    sorted_links = sorted(links, key=lambda l: l.auto_volume, reverse=True)[:10]
    for i, link in enumerate(sorted_links, 1):
        print(f"  {i}. Link {link.i_node.id}-{link.j_node.id}: {link.auto_volume:,.0f} vehicles")

    eb.dispose()
    print(f"\n{'='*60}")
    print("✅ Network validation complete!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check network volumes after assignment")
    parser.add_argument('emmebank_path', type=str, help='Path to EMME database')
    parser.add_argument('scenario_id', type=int, nargs='?', default=11, 
                        help='Scenario ID (default: 11 for AM)')
    args = parser.parse_args()
    
    check_volumes(args.emmebank_path, args.scenario_id)
