"""Analyze OSM Network Standard Attributes

This script analyzes what values exist in the standard EMME fields
that could be used to initialize @ attributes.
"""
import sys
from pathlib import Path
from collections import Counter

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
print(f"Analyzing scenario {scenario.id}: {scenario.title}")
print("Loading network...")

network = scenario.get_network()

print("\n" + "="*80)
print("ANALYZING LINK ATTRIBUTES")
print("="*80)

# Collect all values
type_counter = Counter()
lanes_counter = Counter()
vdf_counter = Counter()
length_values = []
data1_values = set()
data2_values = set()
data3_values = set()

print("\nScanning links...")
for i, link in enumerate(network.links()):
    if i % 10000 == 0:
        print(f"  Processed {i:,} links...")
    
    type_counter[link.type] += 1
    lanes_counter[link.num_lanes] += 1
    vdf_counter[link.volume_delay_func] += 1
    length_values.append(link.length)
    data1_values.add(link.data1)
    data2_values.add(link.data2)
    data3_values.add(link.data3)

print(f"\n✓ Analyzed {len(list(network.links())):,} links")

print("\n" + "="*80)
print("TYPE FIELD ANALYSIS")
print("="*80)
print(f"\nUnique type values: {len(type_counter)}")
print("\nDistribution:")
for type_val, count in sorted(type_counter.items()):
    pct = 100 * count / len(list(network.links()))
    print(f"  type={type_val:2} : {count:6,} links ({pct:5.1f}%)")

print("\n" + "="*80)
print("NUM_LANES FIELD ANALYSIS")
print("="*80)
print(f"\nUnique num_lanes values: {len(lanes_counter)}")
print("\nDistribution:")
for lanes, count in sorted(lanes_counter.items()):
    pct = 100 * count / len(list(network.links()))
    print(f"  num_lanes={lanes:4.1f} : {count:6,} links ({pct:5.1f}%)")

print("\n" + "="*80)
print("VOLUME_DELAY_FUNC ANALYSIS")
print("="*80)
print(f"\nUnique VDF values: {len(vdf_counter)}")
print("\nDistribution:")
for vdf, count in sorted(vdf_counter.items()):
    pct = 100 * count / len(list(network.links()))
    print(f"  vdf={vdf:2} : {count:6,} links ({pct:5.1f}%)")

print("\n" + "="*80)
print("LENGTH ANALYSIS")
print("="*80)
print(f"\nMin length: {min(length_values):.4f} miles")
print(f"Max length: {max(length_values):.4f} miles")
print(f"Avg length: {sum(length_values)/len(length_values):.4f} miles")

print("\n" + "="*80)
print("DATA FIELDS ANALYSIS")
print("="*80)
print(f"\ndata1 unique values: {sorted(data1_values)}")
print(f"data2 unique values: {sorted(data2_values)}")
print(f"data3 unique values: {sorted(data3_values)}")

print("\n" + "="*80)
print("MODE ANALYSIS")
print("="*80)
modes = list(network.modes())
print(f"\nTotal modes in network: {len(modes)}")
for mode in modes[:20]:
    print(f"  {mode.id:10} : {mode.type:10} - {mode.description or '(no description)'}")
if len(modes) > 20:
    print(f"  ... and {len(modes) - 20} more modes")

# Sample a few links to see mode assignments
print("\nSample link mode assignments (first 5 links):")
for i, link in enumerate(network.links()):
    if i >= 5:
        break
    mode_ids = [m.id for m in link.modes]
    print(f"  Link {link.id}: modes = {mode_ids}")

print("\n" + "="*80)
print("RECOMMENDATIONS")
print("="*80)
print("\nBased on analysis above:")
print("  1. Use 'type' field to map to @ft (functional type)")
print("  2. Copy 'num_lanes' directly to @lanes")
print("  3. Calculate @capclass from type")
print("  4. Calculate @free_flow_speed from type")
print("  5. Set @drive_link=1 for all links (if all are driveable)")
print("  6. Set toll attributes to 0 (no toll coding)")
print()
