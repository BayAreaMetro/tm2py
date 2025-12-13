# Highway Assignment & Skimming Test Framework

This framework provides tools for testing highway network creation, skimming, and assignment using county-specific subsets with fixed trips.

## Purpose

This test framework is designed for:
- **Rapid testing** of highway assignment changes without running the full model
- **Debugging** highway network or assignment issues for a specific county
- **Validation** of network modifications in a subset of the region
- **Performance testing** with a smaller geographic scope
- **Development** of new highway-related features
- **Multi-county comparison** testing

## Components

The framework consists of four main files:

1. **`test_highway_assign_skim.py`** - Pytest-based test suite with data filtering and auto-detection utilities
2. **`highway_assign_skim_controller.py`** - Standalone controller script for running highway components
3. **`config_templates/san_mateo_scenario.toml`** - Template scenario configuration
4. **`config_templates/san_mateo_model.toml`** - Template model configuration

## Test Network Location

A test network for San Mateo County is available at:
```
M:\Development\Travel Model Two\Supply\Network Creation 2025\from_OSM\SanMateo\7_scenario\emme
```

This network can be used for testing highway assignment and skimming with the county-specific test framework.

## Quick Start

### Step 1: Auto-Detect County Zones

The framework automatically detects zone ranges for any county using the crosswalk file:
`C:\GitHub\tm2py-utils\tm2py_utils\inputs\maz_taz\mazs_tazs_county_tract_PUMA_2.5.csv`

You can use auto-detection by simply providing the county name:

```python
from tests.test_highway_assign_skim import get_county_zones

# Automatically detect zones for any county
zones = get_county_zones("San Mateo")
print(f"TAZ: {zones['taz_min']}-{zones['taz_max']}")
print(f"MAZ: {zones['maz_min']}-{zones['maz_max']}")

# Works for any county
zones_alameda = get_county_zones("Alameda")
zones_scl = get_county_zones("Santa Clara")
```

No manual zone identification needed!

### Step 2: Prepare Test Data

You have two options:

#### Option A: Use the Data Filter with Auto-Detection (Recommended)

The framework includes utilities to automatically detect zones and filter your full model data:

```python
from tests.test_highway_assign_skim import get_county_zones, setup_county_test_data
from pathlib import Path

# Auto-detect zones and filter data for any county
county_name = "San Mateo"  # Or "Alameda", "Santa Clara", etc.
zones = get_county_zones(county_name)

source_dir = Path("path/to/full/model/inputs")
test_dir = Path(f"test_{county_name.lower().replace(' ', '_')}")

setup_county_test_data(
    source_dir, 
    test_dir, 
    county_name,
    taz_range=(zones['taz_min'], zones['taz_max']),
    maz_range=(zones['maz_min'], zones['maz_max'])
)
```

#### Option B: Manually Prepare Subset Data

Create a test directory with this structure:

```
test_{county_name}/
├── config/
│   ├── scenario.toml
│   └── model.toml
├── inputs/
│   ├── hwy/
│   │   ├── highway.net (or .csv)
│   │   ├── tolls.csv
│   │   └── turn_restrictions.csv
│   ├── landuse/
│   │   └── maz_data.csv (filtered to county)
│   └── demand/
│       └── trips_{county}.omx (your fixed trips)
├── emme_project/
│   └── Database_highway/
└── logs/
```

### Step 3: Configure Your Test

1. Use the setup script (recommended):

```bash
python tests/setup_highway_assign_skim.py --county "San Mateo" --source-dir path/to/full/model
```

Or copy template files manually:

```bash
cp tests/config_templates/san_mateo_scenario.toml test_san_mateo/config/scenario.toml
cp tests/config_templates/san_mateo_model.toml test_san_mateo/config/model.toml
```

2. Edit `scenario.toml`:
   - Update `project_path` and `highway_database_path` to point to your Emme project
   - Adjust time periods as needed
   - Update input/output paths

3. Edit `model.toml`:
   - Verify highway class definitions match your demand data
   - Adjust assignment parameters if needed

### Step 4: Prepare Trip Data

Create fixed trip tables for San Mateo County. These should be in OMX format with matrices for each time period and vehicle class.

**Example structure:**
- `trips_san_mateo_EA_DA.omx` - Early AM drive alone trips
- `trips_san_mateo_AM_DA.omx` - AM peak drive alone trips
- etc.

Or a single OMX file with multiple matrices:
- `trips_san_mateo.omx`
  - Matrix: "EA_DA", "EA_SR2", etc.

Make sure the zone system matches your filtered TAZ range.

## Running the Test

### Method 1: Using the Standalone Controller (Recommended)

```bash
# Basic run with auto-detection
python tests/highway_assign_skim_controller.py \
    --county "San Mateo" \
    --scenario test_san_mateo/config/scenario.toml \
    --model-config test_san_mateo/config/model.toml

# Run any county
python tests/highway_assign_skim_controller.py \
    --county "Alameda" \
    --scenario test_alameda/config/scenario.toml \
    --model-config test_alameda/config/model.toml

# Run without MAZ components (faster)
python tests/highway_assign_skim_controller.py \
    --county "San Mateo" \
    --scenario test_san_mateo/config/scenario.toml \
    --model-config test_san_mateo/config/model.toml \
    --no-maz
```

### Method 2: Using Pytest

```bash
# Run the test
pytest tests/test_highway_assign_skim.py::test_county_highway_subset -v

# Run with output
pytest tests/test_highway_assign_skim.py::test_county_highway_subset -v -s
```

### Method 3: Programmatically

```python
from tests.highway_assign_skim_controller import CountyHighwayController

# Create controller for any county
controller = CountyHighwayController(
    scenario_config="test_san_mateo/config/scenario.toml",
    model_config="test_san_mateo/config/model.toml",
    run_dir="test_san_mateo",
    county_name="San Mateo",
    include_maz_components=True
)

# Run
controller.run_highway_only()

# Validate
success = controller.validate_results()
```

## What Gets Run

The framework runs these components in order:

1. **`prepare_network_highway`** - Loads highway network, applies tolls, creates scenarios
2. **`highway`** - Performs highway assignment and skimming for all time periods
3. **`highway_maz_skim`** (optional) - MAZ-to-MAZ shortest path skimming
4. **`highway_maz_assign`** (optional) - MAZ shortest path assignment

## Output Files

After a successful run, you'll find:

```
test_san_mateo/
├── skim_matrices/
│   └── highway/
│       ├── HWYSKIM_EA.omx
│       ├── HWYSKIM_AM.omx
│       ├── HWYSKIM_MD.omx
│       ├── HWYSKIM_PM.omx
│       ├── HWYSKIM_EV.omx
│       └── HWYSKIM_MAZMAZ_*.csv (if MAZ components run)
├── loaded_highway/
│   ├── loaded_highway_EA.csv
│   ├── loaded_highway_AM.csv
│   └── ... (link volumes by time period)
└── logs/
    ├── tm2py_run.log
    └── tm2py_detail.log
```

## Customization

### Running Only Specific Time Periods

Edit the `time_periods` section in your scenario config:

```toml
# Only run AM and PM
[[time_periods]]
name = "AM"
emme_scenario_id = 2
length_hours = 3.5

[[time_periods]]
name = "PM"
emme_scenario_id = 4
length_hours = 4.0
```

### Changing Assignment Parameters

Edit the `[highway]` section in your model config:

```toml
[highway]
relative_gap = 0.001  # Looser convergence for faster testing
max_iterations = 50   # Fewer iterations
```

### Adding More Vehicle Classes

Add additional class definitions to the model config:

```toml
[[highway.classes]]
name = "MY_NEW_CLASS"
description = "My custom vehicle class"
mode = "n"  # Use an unused mode letter
value_of_time = 25.00
operating_cost_per_mile = 0.30
pce = 1.0
```

## Troubleshooting

### Common Issues

1. **"Zone X not found in network"**
   - Make sure your network includes all zones in your trip tables
   - Verify your zone range definitions are correct

2. **"No trips found for time period Y"**
   - Check that your trip files are named correctly
   - Verify the demand file paths in your config

3. **"Emmebank not found"**
   - Ensure the Emme project path is correct
   - Check that the database has been initialized
   - You may need to create the database first using Emme

4. **Assignment doesn't converge**
   - Try loosening the `relative_gap` parameter
   - Reduce `max_iterations` for testing
   - Check for network errors in the logs

### Getting More Information

Enable detailed logging by editing the scenario config:

```toml
[logging]
display_level = "DEBUG"  # More verbose console output
log_file_level = "DEBUG"  # More detailed log file
```

## Advanced Usage

### Filtering Existing Trip Tables

Use the `CountyDataFilter` class with auto-detected zones:

```python
from tests.test_highway_assign_skim import CountyDataFilter, get_county_zones

# Auto-detect zones for any county
county_name = "San Mateo"
zones = get_county_zones(county_name)

filter_helper = CountyDataFilter(
    county_name=county_name,
    taz_range=(zones['taz_min'], zones['taz_max']),
    maz_range=(zones['maz_min'], zones['maz_max'])
)

# Filter OMX file
filter_helper.filter_trip_table(
    input_file=Path("full_model/demand/trips.omx"),
    output_file=Path("test_san_mateo/inputs/demand/trips_san_mateo.omx")
)

# Filter CSV trip table
filter_helper.filter_trip_table(
    input_file=Path("full_model/demand/trips.csv"),
    output_file=Path("test_san_mateo/inputs/demand/trips_san_mateo.csv"),
    origin_col="otaz",
    dest_col="dtaz"
)
```

### Running Multiple Test Scenarios

You can create multiple configuration sets for different tests:

```
test_san_mateo/
├── config_baseline/
│   ├── scenario.toml
│   └── model.toml
├── config_alt1/
│   ├── scenario.toml
│   └── model.toml
└── config_alt2/
    ├── scenario.toml
    └── model.toml
```

Then run each:

```bash
python tests/highway_assign_skim_controller.py --county "San Mateo" --scenario config_baseline/scenario.toml --model-config config_baseline/model.toml
python tests/highway_assign_skim_controller.py --county "San Mateo" --scenario config_alt1/scenario.toml --model-config config_alt1/model.toml
```

### Comparing Results

After running multiple scenarios, compare the outputs:

```python
import openmatrix as omx
import numpy as np

# Load skims
base = omx.open_file("test_san_mateo/baseline/skim_matrices/highway/HWYSKIM_AM.omx")
alt = omx.open_file("test_san_mateo/alt1/skim_matrices/highway/HWYSKIM_AM.omx")

# Compare travel times
base_time = base["SOV_TIME"][:]
alt_time = alt["SOV_TIME"][:]

diff = alt_time - base_time
print(f"Average time difference: {np.mean(diff[diff > 0]):.2f} minutes")

base.close()
alt.close()
```

## Next Steps

Once you've validated your test framework works:

1. **Automate testing** - Add to CI/CD pipeline for regression testing
2. **Expand coverage** - Create similar frameworks for other counties
3. **Add validation** - Compare against known results or previous runs
4. **Performance profiling** - Use this framework to benchmark performance improvements

## Support

For issues or questions:
- Check the main tm2py documentation
- Review the test logs in `test_san_mateo/logs/`
- Examine the Emme logbook for assignment-specific issues
