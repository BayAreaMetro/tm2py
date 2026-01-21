# Configuration Reference

## Quick Start: Paths to Update

When your input data location changes, update these paths in `tests/county_test_config.toml`:

### Essential Paths (Always Required)

```toml
[paths]
# 1. EMME network project location
emme_project_source = "E:/Box/.../Model Inputs/2015-tm22-dev-sprint-04/emme_network"

# 2. Input files (tolls, land use, etc.)
inputs_source = "E:/Box/.../Model Inputs/2015-tm22-dev-sprint-04"

# 3. Demand matrices (from a model run)
demand_source = "E:/Box/.../Model Outputs/2015-tm22-dev-sprint-04"

# 4. Output directory - Where test results will be written
output_dir = "E:/Tests/san_mateo_test"

# 5. Crosswalk file - Maps zones to counties
crosswalk_file = "C:/GitHub/tm2py-utils/tm2py_utils/inputs/maz_taz/mazs_tazs_county_tract_PUMA_2.5.csv"
```

### What Files Are Needed From Each Source

**From `emme_project_source`:**
```
emme_project_source/
├── emme_project/
│   ├── mtc_emme.emp
│   └── Database_highway/
│       └── emmebank (or .zip file)
```

**From `inputs_source`:**

The script auto-detects two folder structures:
```
# Structure A (Model Inputs - files at root)
inputs_source/
├── hwy/
│   ├── tolls.csv
│   └── interchange_nodes.csv
└── landuse/
    └── maz_data.csv

# Structure B (Model Outputs - files in inputs/ subfolder)
inputs_source/
└── inputs/
    ├── hwy/
    │   ├── tolls.csv
    │   └── interchange_nodes.csv
    └── landuse/
        └── maz_data.csv
```

**From `demand_source`:**
```
demand_source/
└── demand_matrices/
    └── highway/
        ├── household/
        │   ├── TAZ_Demand_ea.omx
        │   ├── TAZ_Demand_am.omx
        │   ├── TAZ_Demand_md.omx
        │   ├── TAZ_Demand_pm.omx
        │   └── TAZ_Demand_ev.omx
        └── commercial/  (optional - truck demand)
            └── tripstrk{period}.omx
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
| `emme_project_source` | string | Yes | Directory containing EMME project with highway network |
| `inputs_source` | string | Yes | Directory containing input files (tolls, land use) |
| `demand_source` | string | No | Directory containing demand matrices (defaults to `inputs_source` if not set) |
| `output_dir` | string | Yes | Directory where test results will be written (created if doesn't exist) |
| `crosswalk_file` | string | Yes | CSV file mapping TAZ/MAZ to counties (used for zone detection) |

**Example:**
```toml
[paths]
emme_project_source = "E:/Box/.../Model Inputs/2015-tm22-dev-sprint-04/emme_network"
inputs_source = "E:/Box/.../Model Inputs/2015-tm22-dev-sprint-04"
demand_source = "E:/Box/.../Model Outputs/2015-tm22-dev-sprint-04"
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
| `auto_confirm` | boolean | No | false | Skip all confirmation prompts for automated runs |

**Filter Demand:**
- `true` = Only trips with both origin AND destination in the county (faster, fewer trips)
- `false` = All regional trips touching the county (tests connectivity)

⚠️ **IMPORTANT:** County tests use **HOUSEHOLD demand ONLY**. Truck demand is NOT included!

**Auto-Confirm:**
- `true` = Skip all y/n confirmation prompts (recommended for automated/unattended runs)
- `false` = Prompt user for confirmation when overwriting existing directories

When `auto_confirm = false`, the script will:
- Ask before overwriting existing test directories
- Ask before replacing existing EMME projects

When `auto_confirm = true`, the script will:
- Automatically proceed with all operations
- Useful for batch testing or CI/CD pipelines
- Still logs warnings about overwriting existing files

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

Controls log verbosity for both console and file output.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `console_log_level` | string | No | "INFO" | Console output verbosity |
| `file_log_level` | string | No | "DEBUG" | Log file verbosity |

**Log Levels:** (least to most verbose)
- `ERROR` = Only errors
- `WARNING` = Errors + warnings
- `INFO` = Errors + warnings + progress messages
- `DEBUG` = Everything including detailed diagnostics

**Dual Logging System:**

The test framework uses a dual logging system:

1. **Console Output** - Shows progress during test execution
   - Controlled by `console_log_level`
   - Defaults to `INFO` for clean, readable output
   - Simple format: `LEVEL: message`
   - Use `WARNING` or `ERROR` for minimal output

2. **File Output** - Complete detailed log saved to disk
   - Controlled by `file_log_level`
   - Defaults to `DEBUG` for comprehensive diagnostics
   - Full format: `timestamp - LEVEL - message`
   - Saved to: `{output_dir}/logs/county_test_YYYYMMDD_HHMMSS.log`
   - Includes all debug information, configuration values, and detailed progress

**Log File Location:**

Log files are automatically created in the test output directory:
```
{output_dir}/
  └── logs/
      └── county_test_20260108_125547.log
```

The filename includes a timestamp for tracking multiple test runs.

**Example Configurations:**

**Standard Testing (recommended):**
```toml
[logging]
console_log_level = "INFO"   # Progress messages to console
file_log_level = "DEBUG"      # Full details in log file
```

**Quiet Console:**
```toml
[logging]
console_log_level = "WARNING"  # Only warnings/errors to console
file_log_level = "DEBUG"        # Full details in log file
```

**Verbose Console (debugging):**
```toml
[logging]
console_log_level = "DEBUG"   # Everything to console
file_log_level = "DEBUG"       # Everything to log file
```

**What Gets Logged:**

- **DEBUG**: Configuration values, file paths, zone ranges, attribute checks, EMME operations
- **INFO**: Test progress, component execution, completion messages, results summary
- **WARNING**: Missing optional files, existing directory prompts, validation warnings
- **ERROR**: Missing required files, configuration errors, component failures

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
