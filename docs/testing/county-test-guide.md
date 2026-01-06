# County Highway Test Guide

## Overview

The county highway test framework enables rapid testing of highway assignment and skimming on a subset of zones (single county) instead of the full region. This dramatically reduces runtime while maintaining realistic network behavior.

## Purpose

This test framework is designed for:
- **Rapid testing** of highway assignment changes without running the full model (minutes vs hours)
- **Debugging** highway network or assignment issues for a specific county
- **Validation** of network modifications in a subset of the region
- **Performance testing** with a smaller geographic scope
- **Development** of new highway-related features without full model overhead
- **Multi-county comparison** testing (run same test for different counties)

## Quick Start

### Run a Test

```powershell
C:\GitHub\tm2pyenv\Scripts\python.exe tests\run_county_test.py --output-dir "E:\Tests\san_mateo_test" --county "San Mateo"
```

### Skip Setup (Reuse Existing)

```powershell
# Skip EMME copy and directory setup
python tests\run_county_test.py --output-dir "E:\Tests\san_mateo_test" --county "San Mateo" --skip-emme-copy --skip-setup --yes
```

## What Gets Filtered vs. What Doesn't

| Component | Filtered? | Why? |
|-----------|-----------|------|
| **Demand (trip tables)** | ✓ Yes (configurable) | Extract intra-county trips for focused testing |
| **Network** | ✗ No | Full network needed for path-finding (county trips may use regional links) |
| **Skims** | ✗ No | Generated for all network zones (only county O-D pairs used in assignment) |
| **MAZ land use** | ✗ Not yet | Could be filtered for memory savings (TODO) |
| **Tolls** | ✗ No | Full toll file used (could be filtered for speed) |

**Key Insight**: Even for intra-county trips, the assignment may use links outside the county, so you need the full regional network.

## Architecture

### Components Executed

1. **create_tod_scenarios** - Creates time-of-day scenarios and copies period-specific attributes
2. **prepare_network_highway** - Prepares network (tolls, VDFs, modes, costs)
3. **highway** - Runs assignment and generates skims
4. **highway_maz_skim** (optional) - MAZ-to-MAZ skims
5. **highway_maz_assign** (optional) - MAZ-to-MAZ assignment
6. **network_summary** (optional) - Network performance reports

### Component Dependencies

```
create_tod_scenarios (REQUIRED FIRST)
    ↓
    Creates @useclass from @useclass_{period}
    ↓
prepare_network_highway (uses @useclass)
    ↓
highway (assignment + skims)
```

**Critical**: `create_tod_scenarios` must run before `prepare_network_highway` or you'll get `KeyError: '@useclass'`

### Design Principles

1. **Use existing EMME projects** - Don't create networks from scratch; copy projects with loaded base networks
2. **Auto-detect zones** - Read crosswalk files to identify TAZ/MAZ ranges programmatically
3. **Configurable filtering** - Allow testing with intra-county-only trips OR full regional trips
4. **Single time period default** - Run AM only for speed; easily expand to all periods
5. **User-specified outputs** - Keep all test artifacts outside source code directory

## Zone Detection Algorithm

Zones are automatically detected from the crosswalk file:
`C:\GitHub\tm2py-utils\tm2py_utils\inputs\maz_taz\mazs_tazs_county_tract_PUMA_2.5.csv`

```python
from tests.test_highway_assign_skim import get_county_zones

# Auto-detect zones for any county
zones = get_county_zones("San Mateo")
# Returns: {'taz_min': X, 'taz_max': Y, 'maz_min': A, 'maz_max': B}
```

**Example Output:**
- San Mateo County: TAZ 190-418 (229 zones), MAZ 2000-4999 (~3000 zones)
- Alameda County: TAZ 1-190 (~190 zones), MAZ 1-2000 (~2000 zones)

## Demand Filtering

### How It Works

The framework filters trip tables to include only trips with BOTH origin AND destination in the target county:

1. Reads MAZ data to identify county MAZs
2. Builds TAZ-to-MAZ mapping
3. Filters each time period's demand file
4. Saves filtered OMX files to test directory

### Configuration

```toml
[scenario]
filter_demand = true  # Enable/disable filtering
county = "San Mateo"  # Auto-set by --county flag
```

### Data Sources

- **MAZ Data**: `E:\2015_TM2_20250619\inputs\landuse\maz_data.csv`
- **Demand**: `E:\2015_TM2_20250619\demand_matrices\highway\household\TAZ_Demand_{period}.omx`

## Configuration

### File Locations

```
tests/
├── run_county_test.py              # Test runner script
├── highway_assign_skim_controller.py  # Controller class
├── config_templates/
│   ├── fixed_san_mateo_scenario.toml  # Complete scenario config
│   └── fixed_san_mateo_model.toml     # Complete model config
```

### Key Config Settings

#### Scenario Config (scenario.toml)

```toml
[scenario]
name = "San Mateo County Test"
year = 2015
filter_demand = true  # Enable demand filtering

[emme.time_period]
[[emme.time_period]]
name = "AM"
emme_scenario_id = 11
highway_capacity_factor = 1.00

[emme]
project_path = "E:/2015_TM2_20250619/emme_project/mtc_emme.emp"  # Use absolute paths
highway_database_path = "E:/2015_TM2_20250619/emme_project/Database_highway/emmebank"
```

#### Model Config (model.toml)

```toml
[[highway.classes]]
name = "DA"
description = "drive alone"
mode_code = "d"
value_of_time = 18.93
operating_cost_per_mile = 17.23
veh_group_name = "da"  # Must match tolls.csv columns (da, s2, s3, etc.)
excluded_links = ["is_toll_da"]
toll = ["@bridgetoll_da"]
skims = ["time", "dist", "freeflowtime", "bridgetoll"]

[[highway.classes]]
name = "SR2"
veh_group_name = "s2"  # NOT "sr2" - must match CSV
excluded_links = ["is_toll_s2"]  # NOT "is_toll_sr2"
toll = ["@bridgetoll_s2"]  # NOT "@bridgetoll_sr2"
```

## Common Issues

### 1. KeyError: '@useclass'

**Problem**: `create_tod_scenarios` not in component list

**Solution**: Verify `highway_assign_skim_controller.py` includes:
```python
HIGHWAY_COMPONENTS = [
    "create_tod_scenarios",      # Must be first!
    "prepare_network_highway",
    "highway",
    ...
]
```

### 2. Field Name Mismatches

**Problem**: Config uses `sr2`/`sr3` but tolls.csv has `s2`/`s3`

**Solution**: Use `s2`/`s3` everywhere:
```toml
veh_group_name = "s2"  # NOT "sr2"
excluded_links = ["is_toll_s2"]
toll = ["@bridgetoll_s2"]
```

See [Field Name Mapping](../assignment/field-name-mapping.md) for full details.

### 3. Relative vs Absolute Paths

**Problem**: Config has relative paths like `"emme_project/Database_highway"`

**Solution**: Use absolute paths in configs:
```toml
project_path = "E:/2015_TM2_20250619/emme_project/mtc_emme.emp"
highway_database_path = "E:/2015_TM2_20250619/emme_project/Database_highway/emmebank"
```

## Performance Comparison

| Configuration | Zones | Trips | Runtime | Use Case |
|---------------|-------|-------|---------|----------|
| Full region, 5 periods | 1454×1454 | 10M×5 | ~4 hours | Production runs |
| San Mateo filtered, 1 period | 229×229 | 500K×1 | ~10 min | Rapid testing |
| San Mateo unfiltered, 1 period | 1454×1454 | 10M×1 | ~1 hour | Connectivity testing |
| Alameda filtered, 1 period | 190×190 | 400K×1 | ~8 min | Different county |

## File Outputs

### Directory Structure

```
E:\Tests\san_mateo_test\
├── config\
│   ├── scenario.toml
│   └── model.toml
├── inputs\
│   ├── hwy\
│   │   └── tolls.csv
│   ├── landuse\
│   │   └── maz_data.csv
│   └── demand\
│       └── TAZ_Demand_AM.omx  (filtered)
├── emme_project\
│   └── [full EMME project copy]
├── logs\
│   ├── tm2py_detail.log
│   └── tm2py_summary.log
└── outputs\
    └── [skims and assignment results]
```

## Command Reference

### Basic Usage

```powershell
python tests\run_county_test.py --output-dir DIR --county COUNTY
```

### Flags

- `--skip-setup` - Skip directory setup (reuse existing)
- `--skip-emme-copy` - Skip EMME project copy (use existing)
- `--yes` / `-y` - Skip interactive prompts
- `--county` - County name for filtering

### Examples

```powershell
# Full setup (first run)
python tests\run_county_test.py --output-dir "E:\Tests\test1" --county "San Mateo"

# Rerun without setup
python tests\run_county_test.py --output-dir "E:\Tests\test1" --county "San Mateo" --skip-setup --skip-emme-copy --yes

# Different county
python tests\run_county_test.py --output-dir "E:\Tests\alameda_test" --county "Alameda"
```

## Troubleshooting

### Check EMME Scenarios

```python
from inro.emme.database.emmebank import Emmebank
eb = Emmebank('E:/Tests/san_mateo_test/emme_project/Database_highway/emmebank')
print([s.id for s in eb.scenarios()])  # Should show: ['1', '11', '12', '13', '14', '15']
```

### Check Attributes

```python
scenario = eb.scenario(11)  # AM scenario
attrs = [attr.name for attr in scenario.extra_attributes()]
print([a for a in attrs if 'useclass' in a.lower()])
# Before create_tod_scenarios: []
# After create_tod_scenarios: ['@useclass']
```

### View Logs

```powershell
# Detail log
Get-Content E:\Tests\san_mateo_test\logs\tm2py_detail.log -Tail 50

# Summary log
Get-Content E:\Tests\san_mateo_test\logs\tm2py_summary.log
```

## Related Documentation

- **[Setup & Configuration](setup.md)** - Detailed setup checklist
- **[Quick Reference](quick-reference.md)** - Commands and examples
- **[Trip Filtering](filtering-trips.md)** - Filter CTRAMP output files
- **[Network Thinning](network-thinning.md)** - Optimize network for speed
- **[EMME Manager Flow](emme-manager-flow.md)** - EMME initialization details
- **[Field Name Mapping](../assignment/field-name-mapping.md)** - Vehicle naming conventions
