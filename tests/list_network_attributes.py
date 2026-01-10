"""List Network Attributes

Quick script to see what attributes exist in an EMME network scenario.
"""
import sys
from pathlib import Path

try:
    import inro.emme.database.emmebank as _eb
except ImportError:
    print("ERROR: EMME modules not available!")
    sys.exit(1)

emmebank_path = Path("M:/Development/Travel Model Two/Supply/Network Creation 2025/from_OSM/SanMateo/7_scenario/emme/emme_project/Database_highway/emmebank")

if not emmebank_path.exists():
    print(f"ERROR: Database not found: {emmebank_path}")
    sys.exit(1)

print("Opening database...")
bank = _eb.Emmebank(str(emmebank_path))

scenario = bank.scenario(1)
print(f"Getting network from scenario {scenario.id}: {scenario.title}")

network = scenario.get_network()

print("\n" + "="*80)
print("LINK ATTRIBUTES (user-defined @ attributes)")
print("="*80)

sample_link = next(iter(network.links()))
user_attrs = sorted([attr for attr in dir(sample_link) if attr.startswith('@')])

print(f"\nFound {len(user_attrs)} user-defined attributes on links:")
for attr in user_attrs:
    try:
        val = getattr(sample_link, attr)
        print(f"  {attr:30} = {val!r:20} (type: {type(val).__name__})")
    except:
        print(f"  {attr:30} (error reading)")

print("\n" + "="*80)
print("STANDARD LINK ATTRIBUTES")
print("="*80)

standard = ['id', 'i_node', 'j_node', 'length', 'type', 'lanes', 'vdf', 
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
        val = getattr(sample_node, attr)
        print(f"  {attr:30} = {val!r:20} (type: {type(val).__name__})")
    except:
        print(f"  {attr:30} (error reading)")

print("\n" + "="*80)
print("SUMMARY")
print("="*80)
print(f"Total links: {len(list(network.links()))}")
print(f"Total nodes: {len(list(network.nodes()))}")
print(f"Total zones: {len(list(network.centroids()))}")
print(f"Link attributes: {len(user_attrs)}")
print(f"Node attributes: {len(user_node_attrs)}")
