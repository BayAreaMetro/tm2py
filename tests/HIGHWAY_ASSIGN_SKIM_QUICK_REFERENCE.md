# Highway Assignment & Skimming Test Framework - Quick Reference

## What This Framework Does

A comprehensive test framework for running highway network creation, skimming, and assignment for **any county** using fixed trips and automatic zone detection.

### Core Files

1. **`tests/test_highway_assign_skim.py`** (400+ lines)
   - Pytest test suite with data filtering and auto-detection
   - `CountyDataFilter` class for filtering trip tables, MAZ data, and network links
   - `get_county_zones()` function for automatic zone detection from crosswalk
   - `setup_county_test_data()` function to prepare filtered input data
   - Pytest fixtures and test functions

2. **`tests/highway_assign_skim_controller.py`** (300+ lines)
   - Standalone controller script for running highway components
   - `CountyHighwayController` class - simplified interface to RunController
   - Command-line interface with county parameter and validation
   - Runs only: prepare_network_highway, highway, highway_maz_skim, highway_maz_assign

3. **`tests/config_templates/san_mateo_scenario.toml`** (100 lines)
   - Template scenario configuration
   - Configured for single iteration test
   - 5 time periods (EA, AM, MD, PM, EV)
   - Paths and logging setup

4. **`tests/config_templates/san_mateo_model.toml`** (170 lines)
   - Template model configuration  
   - Highway assignment parameters
   - 10 vehicle classes (DA, SR2, SR3, trucks, toll variants)
   - Link types and demand aggregation

5. **`tests/setup_highway_assign_skim.py`** (400+ lines)
   - Interactive setup script
   - Creates directory structure
   - Copies config templates
   - Auto-detects county zones from crosswalk file
   - Optionally filters full model data

6. **`tests/HIGHWAY_ASSIGN_SKIM_README.md`** (450+ lines)
   - Comprehensive documentation
   - Quick start guide
   - Configuration instructions
   - Troubleshooting tips
   - Advanced usage examples

## How to Get Started

### Step 1: Run Setup

```bash
# Interactive setup (recommended for first time)
cd c:\GitHub\tm2py
python tests/setup_highway_assign_skim.py --interactive

# Or automated if you know your parameters
python tests/setup_highway_assign_skim.py \
    --county "San Mateo" \
    --source-dir path/to/full/model/inputs

# Works for any county
python tests/setup_highway_assign_skim.py \
    --county "Alameda" \
    --source-dir path/to/full/model/inputs
```

### Step 2: Zones Auto-Detected!

Zone ranges are automatically detected from the crosswalk file:
`C:\GitHub\tm2py-utils\tm2py_utils\inputs\maz_taz\mazs_tazs_county_tract_PUMA_2.5.csv`

No manual zone identification needed! The framework reads the crosswalk and automatically finds:
- TAZ min/max for the county
- MAZ min/max for the county

You can also detect zones programmatically:

```python
from tests.test_highway_assign_skim import get_county_zones

zones = get_county_zones("San Mateo")
print(f"TAZ: {zones['taz_min']}-{zones['taz_max']}")
print(f"MAZ: {zones['maz_min']}-{zones['maz_max']}")
```

### Step 3: Prepare Trip Data

Create fixed trip tables for your target county in OMX format:
- Filter your existing trip tables to only county origin-destination pairs
- Place in `test_{county}/inputs/demand/`
- Update the demand file paths in your config

### Step 4: Configure

Edit the configuration files in `test_{county}/config/`:
- Update Emme project paths
- Verify highway class definitions match your trip data
- Adjust time periods as needed

### Step 5: Run

```bash
# Using the controller script
python tests/highway_assign_skim_controller.py \
    --county "San Mateo" \
    --scenario test_san_mateo/config/scenario.toml \
    --model-config test_san_mateo/config/model.toml

# Or using pytest
pytest tests/test_highway_assign_skim.py::test_county_highway_subset -v
```

## What Components Run

The framework runs these components in sequence:

1. **prepare_network_highway**: Loads network, applies tolls, creates Emme scenarios
2. **highway**: Highway assignment and skimming for all time periods  
3. **highway_maz_skim** (optional): MAZ-to-MAZ shortest paths
4. **highway_maz_assign** (optional): MAZ shortest path assignment

## Expected Outputs

After running, you'll have:

```
test_san_mateo/
├── skim_matrices/highway/
│   ├── HWYSKIM_EA.omx
│   ├── HWYSKIM_AM.omx
│   ├── HWYSKIM_MD.omx
│   ├── HWYSKIM_PM.omx
│   └── HWYSKIM_EV.omx
├── loaded_highway/
│   ├── loaded_highway_EA.csv
│   ├── loaded_highway_AM.csv
│   └── ... (link volumes)
└── logs/
    ├── tm2py_run.log
    └── tm2py_detail.log
```

## Key Features

### Data Filtering

The `CountyDataFilter` class can filter:
- Trip tables (CSV or OMX format)
- MAZ land use data
- Network links (with some customization)

```python
from tests.test_highway_assign_skim import CountyDataFilter, get_county_zones

county = "San Mateo"
zones = get_county_zones(county)
filter = CountyDataFilter(
    county_name=county,
    taz_range=(zones['taz_min'], zones['taz_max']),
    maz_range=(zones['maz_min'], zones['maz_max'])
)
filter.filter_trip_table(input_omx, output_omx)
filter.filter_maz_data(input_csv, output_csv)
```

### Flexible Execution

Three ways to run:

1. **Standalone controller** (easiest):
   ```bash
   python tests/highway_assign_skim_controller.py \
       --county "San Mateo" \
       --scenario scenario.toml \
       --model-config model.toml
   ```

2. **Pytest** (for automated testing):
   ```bash
   pytest tests/test_highway_assign_skim.py::test_county_highway_subset
   ```

3. **Programmatically** (for integration):
   ```python
   from tests.highway_assign_skim_controller import CountyHighwayController
   controller = CountyHighwayController(
       county_name="San Mateo",
       scenario_config="scenario.toml",
       model_config="model.toml"
   )
   controller.run_highway_only()
   ```

### Customization Options

- Run with or without MAZ components (`--no-maz` flag)
- Single or multiple time periods (edit config)
- Different convergence criteria (adjust relative_gap)
- Custom highway classes (add to model config)

## Common Use Cases

### Quick Network Testing
Test highway network changes quickly with subset of data:
```bash
python tests/highway_assign_skim_controller.py \
    --county "San Mateo" \
    --scenario config/scenario.toml \
    --model-config config/model.toml \
    --no-maz
```

### Validating Assignment Parameters
Run with different relative gap values to test convergence:
```toml
# In model config
[highway]
relative_gap = 0.001  # Looser for testing
max_iterations = 50
```

### Debugging Network Issues
Enable detailed logging to diagnose problems:
```toml
# In scenario config
[logging]
display_level = "DEBUG"
log_file_level = "DEBUG"
```

## Architecture

The framework follows this structure:

```
┌─────────────────────────────────────────┐
│  User Interface                         │
│  - setup_highway_assign_skim.py        │
│  - highway_assign_skim_controller.py   │
│  - test_highway_assign_skim.py         │
└────────────────┬────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────┐
│  CountyHighwayController                │
│  - Auto-detects zones from crosswalk   │
│  - Configures RunController             │
│  - Limits to highway components         │
│  - Validates results                    │
└────────────────┬────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────┐
│  tm2py.controller.RunController         │
│  - Runs configured components           │
│  - Manages Emme interaction             │
└────────────────┬────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────┐
│  Highway Components                     │
│  - PrepareNetwork                       │
│  - HighwayAssignment                    │
│  - AssignMAZSPDemand                    │
│  - SkimMAZCosts                         │
└─────────────────────────────────────────┘
```

## Next Steps

Once you have the basic test working:

1. **Add validation**: Compare results against known good runs
2. **Automate**: Integrate into CI/CD for regression testing  
3. **Expand**: Create similar frameworks for other counties
4. **Benchmark**: Use for performance testing and optimization

## Tips

- Start with a single time period (e.g., just MD) for fastest testing
- Use `--no-maz` flag to skip MAZ components if not needed
- Keep a "known good" output set for comparison
- Use the validation functions to check outputs automatically

## Troubleshooting

See the full README at `tests/HIGHWAY_ASSIGN_SKIM_README.md` for:
- Common error messages and solutions
- How to enable detailed logging
- Network debugging tips
- Zone system validation

## File Locations

All files created in your workspace:

- `tests/test_highway_assign_skim.py` - Main test file with auto-detection
- `tests/highway_assign_skim_controller.py` - Controller script
- `tests/setup_highway_assign_skim.py` - Setup utility
- `tests/config_templates/san_mateo_scenario.toml` - Scenario config template
- `tests/config_templates/san_mateo_model.toml` - Model config template
- `tests/HIGHWAY_ASSIGN_SKIM_README.md` - Full documentation
- `tests/HIGHWAY_ASSIGN_SKIM_QUICK_REFERENCE.md` - This file
