# Network Thinning for Faster Testing

## Overview
Network thinning removes lower functional class links (local streets, collectors) to create a "skeleton network" of major roads only. This further reduces test runtime.

## Functional Class Hierarchy
Links are classified by `@ft` (functional type) attribute:

| @ft | Facility Type | Keep for Thinning? |
|-----|---------------|-------------------|
| 1 | Freeway | ✓ Always keep |
| 2 | Freeway/Principal Arterial | ✓ Always keep |
| 3 | Principal Arterial | ✓ Recommended |
| 4 | Minor Arterial | ✓ Recommended |
| 5 | Major Collector | ? Optional |
| 6 | Minor Collector | ✗ Consider removing |
| 7 | Local Streets | ✗ Usually remove |
| 8 | Ramps/Connectors | ✓ Always keep |
| 99 | Special/Other | ✓ Always keep |

## Implementation Approach

### Option 1: Filter in create_tod_scenarios (Recommended)
Add filtering after scenario copy but before publishing:

```python
# In create_tod_scenarios._prepare_scenarios_and_attributes()
# After copying scenario, before scenario.publish_network(network)

if self.controller.config.emme.get("thin_network_ft_threshold"):
    ft_threshold = self.controller.config.emme.thin_network_ft_threshold
    links_removed = 0
    
    for link in list(network.links()):
        # Keep connectors, special facilities, and links above threshold
        if link["@ft"] not in [8, 99] and link["@ft"] > ft_threshold:
            # Don't delete centroid connectors
            if not (link.i_node.is_centroid or link.j_node.is_centroid):
                network.delete_link(link.i_node, link.j_node)
                links_removed += 1
    
    self.logger.info(f"Network thinning: removed {links_removed} links with @ft > {ft_threshold}")
```

### Option 2: Filter in EMME Database Directly
Use EMME Desktop to manually delete links before copying to test directory:

1. Open base scenario in EMME Desktop
2. Delete links: `@ft > 4 and type != 99` (keeps freeways/arterials only)
3. Save as new base scenario
4. Use that scenario as your test base

### Option 3: Config-Based Filtering
Add to scenario config:

```toml
[emme]
# Network thinning - remove links below this functional class threshold
# Values: 2 (arterials+), 3 (major arterials+), 4 (arterials+), 5 (collectors+), 6 (all except local)
thin_network_ft_threshold = 4  # Keep freeways (1-2), arterials (3-4), remove collectors/local (5-7)

# Alternative: explicit list of FT values to keep
thin_network_keep_ft = [1, 2, 3, 4, 8, 99]  # Freeways, arterials, connectors, special
```

## Example Implementation

Add to `highway_assign_skim_controller.py`:

```python
def thin_network_by_ft(self, scenario_id: int, ft_threshold: int = 4):
    """Remove links below functional class threshold.
    
    Args:
        scenario_id: EMME scenario ID to thin
        ft_threshold: Remove links where @ft > this value (except connectors/special)
    
    Returns:
        Number of links removed
    """
    from inro.emme.database.emmebank import Emmebank
    
    emmebank = self.controller.emme_manager.highway_emmebank.emmebank
    scenario = emmebank.scenario(scenario_id)
    network = scenario.get_network()
    
    links_removed = 0
    links_to_remove = []
    
    for link in network.links():
        # Always keep: connectors (8), special (99), centroids
        if link.i_node.is_centroid or link.j_node.is_centroid:
            continue
        if link["@ft"] in [8, 99]:
            continue
            
        # Remove if functional class is below threshold
        if link["@ft"] > ft_threshold:
            links_to_remove.append((link.i_node, link.j_node))
    
    for i_node, j_node in links_to_remove:
        network.delete_link(i_node, j_node)
        links_removed += 1
    
    scenario.publish_network(network)
    print(f"Network thinning: removed {links_removed:,} links with @ft > {ft_threshold}")
    print(f"Remaining links: {len(list(network.links())):,}")
    
    return links_removed
```

## Expected Impact

### San Mateo County Base Network
Approximate link counts by functional class:

| @ft Range | Description | Approximate Links | % of Total |
|-----------|-------------|-------------------|------------|
| 1-2 | Freeways | ~2,000 | 2% |
| 3-4 | Arterials | ~8,000 | 9% |
| 5-6 | Collectors | ~15,000 | 16% |
| 7 | Local | ~60,000 | 65% |
| 8,99 | Connectors/Special | ~7,000 | 8% |

### Runtime Impact by Threshold

| Threshold | Links Kept | Est. Runtime | Use Case |
|-----------|------------|-------------|----------|
| None | 100% (~92K) | 2-5 min | Full accuracy |
| @ft ≤ 6 | 35% (~32K) | 1-2 min | Without local streets |
| @ft ≤ 5 | 19% (~17K) | 45-90 sec | Major collectors+ |
| @ft ≤ 4 | 11% (~10K) | 30-60 sec | Arterials+ only |
| @ft ≤ 3 | 6% (~6K) | 20-40 sec | Major arterials+ |
| @ft ≤ 2 | 2% (~2K) | 10-20 sec | Freeways only |

## Considerations

### What You Lose
1. **Local access patterns** - Can't capture neighborhood circulation
2. **Fine-grained congestion** - Local street spillback not modeled
3. **Complete trip chains** - Some origins/destinations become unreachable

### What You Keep
1. **Regional flow patterns** - Major movements preserved
2. **Bottleneck identification** - Freeway/arterial congestion captured
3. **Fast iteration** - Test algorithm changes quickly

## Recommended Thresholds

### Development/Algorithm Testing
- **Threshold: @ft ≤ 3** (major arterials+)
- Runtime: ~30 seconds
- Good for: Testing assignment algorithms, VDF parameters, convergence

### Configuration Testing
- **Threshold: @ft ≤ 4** (all arterials+)
- Runtime: ~1 minute  
- Good for: Testing toll logic, mode choice, skim generation

### Pre-Production Validation
- **Threshold: @ft ≤ 6** (no local streets)
- Runtime: ~2 minutes
- Good for: Final validation before full run

### Full Network (No Thinning)
- Runtime: ~3-5 minutes
- Good for: Final results, publication, detailed analysis

## Usage Example

```powershell
# Test with freeways and arterials only
python tests\run_county_test.py `
  --output-dir "E:\Tests\san_mateo_arterials" `
  --county "San Mateo" `
  --thin-network 4 `
  --yes

# Test with major roads (no collectors/local)
python tests\run_county_test.py `
  --output-dir "E:\Tests\san_mateo_major" `
  --county "San Mateo" `
  --thin-network 5 `
  --yes
```

## Implementation Checklist

- [ ] Add `thin_network_ft_threshold` to EmmeConfig dataclass
- [ ] Add filtering logic to `create_tod_scenarios._prepare_scenarios_and_attributes()`
- [ ] Add `--thin-network` flag to `run_county_test.py`
- [ ] Update config templates with commented example
- [ ] Test with various thresholds
- [ ] Document in COUNTY_TEST_FRAMEWORK_GUIDE.md
- [ ] Measure actual runtime improvements

## Notes

- Always test with full network before trusting results from thinned network
- Thinning affects demand loading - some TAZ/MAZ may become unreachable
- Consider keeping all links within county boundary even if low @ft
- Network thinning + demand filtering = dramatic runtime reduction (90%+)
