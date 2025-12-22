# County Highway Test Framework Guide

## Overview
The county highway test framework enables rapid testing of highway assignment and skimming on a subset of zones (single county) instead of the full region. This dramatically reduces runtime while maintaining realistic network behavior.

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

## Configuration

### File Locations
```
tests/
├── run_county_test.py              # Test runner script
├── highway_assign_skim_controller.py  # Controller class
├── config_templates/
│   ├── fixed_san_mateo_scenario.toml  # Complete scenario config
│   └── fixed_san_mateo_model.toml     # Complete model config
└── COUNTY_TEST_FRAMEWORK_UPDATE.md    # Original design doc
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

### 2. KeyError: 'am' (scenario lookup)
**Problem**: Case mismatch in scenario dictionary

**Solution**: Already fixed in tm2py/emme/manager.py line 146 (removed `.lower()`)

### 3. Field Name Mismatches
**Problem**: Config uses `sr2`/`sr3` but tolls.csv has `s2`/`s3`

**Solution**: Use `s2`/`s3` everywhere:
```toml
veh_group_name = "s2"  # NOT "sr2"
excluded_links = ["is_toll_s2"]
toll = ["@bridgetoll_s2"]
```

See `tests/COMPLETE_FIELD_NAME_MAPPING.md` for full mapping.

### 4. Config Validation Errors
**Problem**: tm2py requires complete configs even for partial tests

**Solution**: Use the `fixed_*` templates which include all required sections:
- `fixed_san_mateo_scenario.toml`
- `fixed_san_mateo_model.toml`

### 5. UTF-8 BOM Corruption
**Problem**: PowerShell `Set-Content` adds UTF-8 BOM (0xEF 0xBB 0xBF) breaking TOML parser

**Solution**: Use Python for file writes:
```python
with open(file_path, 'wb') as f:
    content_bytes = content.encode('utf-8')
    if content_bytes.startswith(b'\xef\xbb\xbf'):
        content_bytes = content_bytes[3:]
    f.write(content_bytes)
```

### 6. Relative vs Absolute Paths
**Problem**: Config has relative paths like `"emme_project/Database_highway"`

**Solution**: Use absolute paths in configs:
```toml
project_path = "E:/2015_TM2_20250619/emme_project/mtc_emme.emp"
highway_database_path = "E:/2015_TM2_20250619/emme_project/Database_highway/emmebank"
```

## Performance Optimization

### Startup Time
EMME Desktop opens all databases listed in the .emp file (highway, transit, active_north, active_south), even if you only need highway. This is controlled by the .emp file, not Python code.

**Current**: ~1 minute startup (opens 4 databases)
**Potential optimization**: Create highway-only .emp file

### Test Execution
With demand filtering (San Mateo County):
- ~5,000 TAZs instead of ~30,000 (83% reduction)
- Assignment time: ~2-5 minutes (vs 15-30 minutes full region)

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
- `tests/COMPLETE_FIELD_NAME_MAPPING.md` - Vehicle naming conventions
- `tests/EMME_MANAGER_FLOW.md` - EMME initialization details
- `tests/COUNTY_TEST_FRAMEWORK_UPDATE.md` - Original design doc
- `tests/FILTERING_CTRAMP_TRIPS.md` - Demand filtering details
