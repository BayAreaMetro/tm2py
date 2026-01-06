# Configuration Reference

## Quick Start: Paths to Update

When your input data location changes, update these paths in `tests/county_test_config.toml`:

### Essential Paths (Always Required)

```toml
[paths]
# 1. Source dataset location - Contains emme_project, inputs, demand_matrices
source_dataset = "E:/2015_TM2_20250619"

# 2. Output directory - Where test results will be written
output_dir = "E:/Tests/san_mateo_test"

# 3. Crosswalk file - Maps zones to counties
crosswalk_file = "C:/GitHub/tm2py-utils/tm2py_utils/inputs/maz_taz/mazs_tazs_county_tract_PUMA_2.5.csv"
```

### What Must Be Inside source_dataset

Your `source_dataset` directory **must** contain:

```
source_dataset/
├── emme_project/
│   ├── mtc_emme.emp
│   └── Database_highway/
│       └── emmebank
├── inputs/
│   ├── hwy/
│   │   ├── tolls.csv
│   │   └── freeflow.csv
│   ├── landuse/
│   │   └── maz_data.csv
│   └── validation/
│       └── interchange_nodes.csv
└── demand_matrices/
    └── highway/
        └── household/
            ├── TAZ_Demand_EA.omx
            ├── TAZ_Demand_AM.omx
            ├── TAZ_Demand_MD.omx
            ├── TAZ_Demand_PM.omx
            └── TAZ_Demand_EV.omx
```

### County-Specific Settings

```toml
[test]
# Change this to test a different county
county_name = "San Mateo"  # Must match county_name in crosswalk_file
```

## Complete Configuration File Reference

The complete configuration file is located at `tests/county_test_config.toml`.

### [paths] Section

Controls where input data is read from and where output is written.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `source_dataset` | string | Yes | Root directory containing emme_project, inputs, and demand_matrices subdirectories |
| `output_dir` | string | Yes | Directory where test results will be written (created if doesn't exist) |
| `crosswalk_file` | string | Yes | CSV file mapping TAZ/MAZ to counties (used for zone detection) |

**Example:**
```toml
[paths]
source_dataset = "E:/2015_TM2_20250619"
output_dir = "E:/Tests/san_mateo_test"
crosswalk_file = "C:/GitHub/tm2py-utils/tm2py_utils/inputs/maz_taz/mazs_tazs_county_tract_PUMA_2.5.csv"
```

### [test] Section

Controls test behavior and filtering options.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `county_name` | string | Yes | - | County to test (must match crosswalk file) |
| `filter_demand` | boolean | No | true | Filter to intra-county trips only |
| `skip_emme_copy` | boolean | No | false | Skip copying EMME project if already exists |
| `skip_setup` | boolean | No | false | Skip test directory setup if already exists |
| `thin_network` | integer | No | (none) | Remove links with @ft > this value |
| `auto_confirm` | boolean | No | false | Skip confirmation prompt |

**Filter Demand:**
- `true` = Only trips with both origin AND destination in the county (faster, fewer trips)
- `false` = All regional trips touching the county (tests connectivity)

⚠️ **IMPORTANT:** County tests use **HOUSEHOLD demand ONLY**. Truck demand is NOT included!

**Network Thinning:**
- Not set = Use full network
- `2` = Keep only freeways/expressways
- `4` = Keep freeways + arterials (recommended)
- `6` = Keep all except local streets

**Example:**
```toml
[test]
county_name = "San Mateo"
filter_demand = true
skip_emme_copy = false
skip_setup = false
thin_network = 4  # Optional: comment out to use full network
auto_confirm = false
```

### [emme] Section

Controls EMME scenario settings.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `all_day_scenario_id` | integer | No | 100 | Source scenario with base network |
| `time_periods` | array | No | ["AM"] | Time periods to test |

**Time Periods:**
Valid values: `"EA"`, `"AM"`, `"MD"`, `"PM"`, `"EV"`

**Example:**
```toml
[emme]
all_day_scenario_id = 100
time_periods = ["AM"]  # Quick test with AM only
# time_periods = ["EA", "AM", "MD", "PM", "EV"]  # Full day
```

### [components] Section

Controls which components to run.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `run_components` | array | Yes | - | List of components to execute |

**Available Components:**

| Component | Required? | Description |
|-----------|-----------|-------------|
| `create_tod_scenarios` | Yes | Creates time-of-day scenarios |
| `prepare_network_highway` | Yes | Sets up network attributes |
| `highway` | Yes | Highway assignment and skimming |
| `highway_maz_skim` | No | MAZ-to-MAZ shortest paths |
| `highway_maz_assign` | No | MAZ assignment |
| `network_summary` | No | Network performance reports |

**Example:**
```toml
[components]
run_components = [
    "create_tod_scenarios",
    "prepare_network_highway",
    "highway",
]
```

### [logging] Section

Controls log verbosity.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `console_log_level` | string | No | "INFO" | Console output verbosity |
| `file_log_level` | string | No | "DEBUG" | Log file verbosity |

**Log Levels:** (least to most verbose)
- `ERROR` = Only errors
- `WARNING` = Errors + warnings
- `INFO` = Errors + warnings + progress messages
- `DEBUG` = Everything including detailed diagnostics

**Example:**
```toml
[logging]
console_log_level = "INFO"   # Clean console output
file_log_level = "DEBUG"      # Detailed file logs
```

## Common Scenarios

### Scenario 1: New Dataset Location

When moving to a new dataset:

```toml
[paths]
source_dataset = "E:/2015_TM2_NEW_DATE"  # ← Change this
output_dir = "E:/Tests/san_mateo_test"   # ← And this if needed
```

### Scenario 2: Testing Different County

```toml
[test]
county_name = "Alameda"  # ← Change county
output_dir = "E:/Tests/alameda_test"  # ← Change output location
```

### Scenario 3: Quick vs. Full Testing

**Quick Test (recommended for development):**
```toml
[test]
filter_demand = true
thin_network = 4

[emme]
time_periods = ["AM"]

[components]
run_components = [
    "create_tod_scenarios",
    "prepare_network_highway",
    "highway",
]
```

**Full Test (for validation):**
```toml
[test]
filter_demand = false
# thin_network not set (full network)

[emme]
time_periods = ["EA", "AM", "MD", "PM", "EV"]

[components]
run_components = [
    "create_tod_scenarios",
    "prepare_network_highway",
    "highway",
    "highway_maz_skim",
    "highway_maz_assign",
    "network_summary",
]
```

### Scenario 4: Repeated Runs (Skip Setup)

After first successful run, speed up subsequent runs:

```toml
[test]
skip_emme_copy = true  # Don't re-copy EMME project
skip_setup = true      # Don't recreate directory structure
auto_confirm = true    # Don't prompt for confirmation
```

## File Location

The configuration file is located at:
```
tests/county_test_config.toml
```

## Creating Multiple Configurations

To maintain multiple test configurations:

```powershell
# Create county-specific configs
copy tests\county_test_config.toml tests\san_mateo_test.toml
copy tests\county_test_config.toml tests\alameda_test.toml

# Run with specific config
cd tests
C:\GitHub\tm2pyenv\Scripts\python.exe run_county_test.py --config alameda_test.toml
```

## Validation

The test script validates configuration on startup:

1. **Path checks:** Verifies all required files exist
2. **County detection:** Validates county_name exists in crosswalk
3. **Zone ranges:** Detects TAZ/MAZ ranges automatically
4. **EMME project:** Confirms emmebank is accessible

If validation fails, check:
- Are all paths spelled correctly?
- Do files exist at specified locations?
- Is county_name spelled exactly as in crosswalk file?
- Is the EMME project intact (has Database_highway/emmebank)?

## See Also

- [Quick Reference](quick-reference.md) - Command syntax and examples
- [Data Flow](data-flow.md) - Input-to-output data flow
- [Setup Guide](setup.md) - Prerequisites and environment setup
