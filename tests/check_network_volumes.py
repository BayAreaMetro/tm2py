"""Quick script to validate highway assignment results."""
import inro.emme.database.emmebank as _eb

emmebank_path = 'E:/Tests/san_mateo_test/emme_project/Database_highway/emmebank'
scenario_id = 11  # AM period

print("Opening EMME database...")
eb = _eb.Emmebank(emmebank_path)
scen = eb.scenario(scenario_id)

print(f"Loading network for scenario {scenario_id}...")
net = scen.get_network()

# Get all links
links = list(net.links())
print(f"\n{'='*60}")
print(f"NETWORK STATISTICS")
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
for attr_name in ['@flow_da', '@flow_datoll', '@flow_sr2']:
    try:
        class_vols = [getattr(l, attr_name) for l in links if hasattr(l, attr_name) and getattr(l, attr_name) > 0]
        if class_vols:
            print(f"  {attr_name}: {len(class_vols):,} links, total = {sum(class_vols):,.0f}")
    except:
        pass

# Sample some high-volume links
print(f"\nTop 10 highest volume links:")
sorted_links = sorted(links, key=lambda l: l.auto_volume, reverse=True)[:10]
for i, link in enumerate(sorted_links, 1):
    print(f"  {i}. Link {link.i_node}-{link.j_node}: {link.auto_volume:,.0f} vehicles")

eb.dispose()
print(f"\n{'='*60}")
print("✅ Network validation complete!")
