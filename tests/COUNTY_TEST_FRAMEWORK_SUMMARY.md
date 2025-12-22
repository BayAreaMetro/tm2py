# County Highway Test Framework - Technical Summary

## Overview

The County Highway Test Framework enables rapid testing and debugging of tm2py highway assignment components by isolating tests to a single county. This reduces runtime from hours to minutes while maintaining full model fidelity.

## Architecture

### Design Principles

1. **Use existing EMME projects** - Don't create networks from scratch; copy projects with loaded base networks
2. **Auto-detect zones** - Read crosswalk files to identify TAZ/MAZ ranges programmatically
3. **Configurable filtering** - Allow testing with intra-county-only trips OR full regional trips
4. **Single time period default** - Run AM only for speed; easily expand to all periods
5. **User-specified outputs** - Keep all test artifacts outside source code directory

### Data Flow

```
Source Dataset (E:\2015_TM2_20250619)
├── emme_project/              → Copied to test directory (contains base network)
├── demand_matrices/           → Filtered by zone ranges (optional)
├── inputs/hwy/tolls.csv       → Copied as-is
└── inputs/landuse/maz_data.csv → Copied as-is (could be filtered)

Crosswalk File (C:\GitHub\tm2py-utils\...\mazs_tazs_county_tract_PUMA_2.5.csv)
└── county_name column         → Used to detect TAZ_SEQ/MAZ_SEQ ranges

Test Directory (User-specified, e.g., C:\MyTests\san_mateo_test)
├── config/
│   ├── scenario.toml          → Controls filter_demand, time periods, EMME settings
│   └── model.toml             → Highway assignment parameters
├── emme_project/              → Copy of source EMME project
│   └── Database_highway/
│       └── emmebank           → Must contain all-day scenario (default: ID 100)
├── inputs/
│   ├── hwy/tolls.csv
│   ├── landuse/maz_data.csv
│   └── demand/
│       └── TAZ_Demand_*.omx   → Filtered or full trips
└── logs/                      → Component execution logs
```

## Components

### Core Files

| File | Purpose | Key Functions |
|------|---------|---------------|
| `run_county_test.py` | Automated test runner | `check_prerequisites()`, `setup_test_directory()`, `run_test()` |
| `test_highway_assign_skim.py` | Zone detection & filtering | `get_county_zones()`, `CountyDataFilter.filter_trip_table()` |
| `highway_assign_skim_controller.py` | Component execution | `CountyHighwayController.run_highway_only()` |
| `examples_highway_assign_skim.py` | Usage examples | Example filtering workflows |

### Configuration Templates

| File | Settings | Customizable |
|------|----------|--------------|
| `san_mateo_scenario.toml` | `filter_demand=true`, single AM period, relative EMME paths, `all_day_scenario_id=100` | Time periods, filtering, scenario IDs |
| `san_mateo_model.toml` | `skim_period="AM"`, highway assignment parameters | Skim period, convergence criteria, classes |

## Zone Detection Algorithm

```python
def get_county_zones(county_name):
    """Auto-detect TAZ/MAZ ranges from crosswalk file."""
    
    # 1. Read crosswalk CSV
    df = pd.read_csv(CROSSWALK_FILE)
    
    # 2. Filter to specified county
    county_df = df[df['county_name'].str.contains(county_name, case=False)]
    
    # 3. Extract zone ranges
    taz_min = county_df['TAZ_SEQ'].min()
    taz_max = county_df['TAZ_SEQ'].max()
    maz_min = county_df['MAZ_SEQ'].min()
    maz_max = county_df['MAZ_SEQ'].max()
    
    return (taz_min, taz_max), (maz_min, maz_max)
```

**Example Output:**
- San Mateo County: TAZ 190-418 (229 zones), MAZ 2000-4999 (~3000 zones)
- Alameda County: TAZ 1-190 (~190 zones), MAZ 1-2000 (~2000 zones)

## Demand Filtering Algorithm

```python
def filter_trip_table(input_omx, output_omx, taz_min, taz_max):
    """Extract intra-county trips from full trip table."""
    
    with omx.open_file(input_omx) as omx_in:
        # 1. Get zone mapping
        zone_mapping = omx_in.mapping('taz').keys()
        
        # 2. Find indices for county zones
        county_indices = [i for i, z in enumerate(zone_mapping) 
                          if taz_min <= z <= taz_max]
        county_zones = [z for z in zone_mapping 
                        if taz_min <= z <= taz_max]
        
        # 3. Extract submatrix (county origins × county destinations)
        with omx.open_file(output_omx, 'w') as omx_out:
            omx_out.create_mapping('taz', county_zones)
            
            for matrix_name in omx_in.list_matrices():
                matrix_data = omx_in[matrix_name][:]
                
                # Extract only county O-D pairs
                filtered = matrix_data[np.ix_(county_indices, county_indices)]
                
                omx_out[matrix_name] = filtered
```

**Example:**
- Full regional matrix: 1454×1454 zones, 10M trips
- San Mateo only: 229×229 zones, 500K trips (95% reduction)

## Execution Workflow

### Automated Script Flow

```
run_county_test.py
│
├─1─► check_prerequisites()
│     ├─ Verify source dataset exists
│     ├─ Check EMME project/emmebank
│     ├─ Validate crosswalk file
│     └─ Check demand matrices
│
├─2─► setup_test_directory()
│     ├─ Create directory structure
│     ├─ Copy config templates
│     ├─ Copy EMME project (full)
│     ├─ Copy input files (tolls, MAZ data)
│     │
│     └─► Check filter_demand setting
│         ├─ If true:
│         │   ├─ Call get_county_zones(county_name)
│         │   ├─ Initialize CountyDataFilter
│         │   └─ Filter all TAZ_Demand_*.omx files
│         │
│         └─ If false:
│             └─ Copy TAZ_Demand_*.omx as-is
│
└─3─► run_test()
      ├─ Initialize CountyHighwayController
      │   ├─ scenario_config: test_dir/config/scenario.toml
      │   ├─ model_config: test_dir/config/model.toml
      │   ├─ run_dir: test_dir
      │   └─ county_name: "San Mateo"
      │
      └─ Call controller.run_highway_only()
          ├─► create_tod_scenarios (from base scenario 100)
          ├─► prepare_network_highway (tolls, attributes)
          └─► highway (assignment + skimming for AM)
```

### Component Sequence

```
create_tod_scenarios
├─ Input: EMME scenario 100 (all-day base network)
├─ Creates: Scenario 1 (AM time-of-day)
└─ Output: Time-specific network with period attributes

prepare_network_highway
├─ Input: Scenario 1 (AM network)
├─ Applies: Tolls, capacities, speeds
└─ Output: Network ready for assignment

highway (AM period)
├─ Input: Filtered TAZ_Demand_AM.omx (229×229 zones)
├─ Runs: Traffic assignment (iterative equilibrium)
└─ Outputs:
    ├─ Loaded network (volumes on links)
    └─ Skim matrices (travel times, costs)
```

## Configuration Options

### Filter Control (scenario.toml)

```toml
[scenario]
filter_demand = true  # or false

# true  → Only intra-county trips (fastest)
# false → All regional trips (tests connectivity)
```

### Time Period Control (scenario.toml)

```toml
# Single period (default)
[[emme.time_period]]
name = "AM"
emme_scenario_id = 1

# Multiple periods (uncomment to enable)
# [[emme.time_period]]
# name = "MD"
# emme_scenario_id = 2
```

### Skim Period (model.toml)

```toml
[highway.skim]
skim_period = "AM"  # Must match one time period name
```

## Performance Comparison

| Configuration | Zones | Trips | Runtime | Use Case |
|---------------|-------|-------|---------|----------|
| Full region, 5 periods | 1454×1454 | 10M×5 | ~4 hours | Production runs |
| San Mateo filtered, 1 period | 229×229 | 500K×1 | ~10 min | Rapid testing |
| San Mateo unfiltered, 1 period | 1454×1454 | 10M×1 | ~1 hour | Connectivity testing |
| Alameda filtered, 1 period | 190×190 | 400K×1 | ~8 min | Different county |

## Key Technical Details

### Why Keep Full Network?

Even for intra-county trips, vehicles may route through adjacent counties. Example:
- Trip from San Mateo TAZ 200 → TAZ 300
- Optimal path uses I-280 through Santa Clara County
- If network filtered to San Mateo only, path would be invalid

### Why Filter Demand?

1. **Speed**: 95% fewer O-D pairs = much faster assignment
2. **Focus**: Test intra-county patterns without regional noise
3. **Debugging**: Easier to trace specific problematic trips
4. **Memory**: Smaller matrices load faster, use less RAM

### EMME Requirements

- **Base scenario must exist** with loaded network
- **Can't import network from CSV** in this framework (uses existing EMME project)
- **Scenario IDs are configurable** (default: base=100, AM=1)
- **All-day network serves as template** for time-of-day scenarios

### Crosswalk File Version

Current: `mazs_tazs_county_tract_PUMA_2.5.csv` (newer version)
- May not match 2015 dataset exactly
- Zones likely the same, but verify if detection fails
- Old crosswalk may be in 2015 dataset somewhere

## Limitations & Future Enhancements

### Current Limitations

1. **MAZ data not filtered** - Uses full regional maz_data.csv
2. **Network not filtered** - Full regional network copied
3. **Single source dataset** - Hardcoded to E:\2015_TM2_20250619
4. **No transit testing** - Highway only
5. **Manual zone verification** - No automated check of detected ranges

### Potential Enhancements

1. **Filter MAZ data** - Reduce memory for MAZ components
2. **Parameterize source dataset** - Command-line or config file
3. **Add transit components** - Extend framework to transit testing
4. **Zone range validation** - Cross-check detected ranges with actual data
5. **Multi-county testing** - Test multiple counties in sequence
6. **Result comparison** - Diff tool for comparing county runs
7. **Automated reporting** - Generate summary statistics/plots

## Usage Examples

### Test Single County, Intra-County Trips Only

```powershell
python tests/run_county_test.py `
    --county "San Mateo" `
    --output-dir "C:/Tests/san_mateo_intra"
```

### Test Single County, All Regional Trips

1. Run setup:
```powershell
python tests/run_county_test.py `
    --county "San Mateo" `
    --output-dir "C:/Tests/san_mateo_regional"
```

2. Edit `C:/Tests/san_mateo_regional/config/scenario.toml`:
```toml
filter_demand = false
```

3. Re-run setup to copy unfiltered demand:
```powershell
python tests/run_county_test.py `
    --county "San Mateo" `
    --output-dir "C:/Tests/san_mateo_regional"
```

### Compare Multiple Counties

```powershell
# Test San Mateo
python tests/run_county_test.py --county "San Mateo" --output-dir "C:/Tests/san_mateo"

# Test Alameda
python tests/run_county_test.py --county "Alameda" --output-dir "C:/Tests/alameda"

# Test Santa Clara
python tests/run_county_test.py --county "Santa Clara" --output-dir "C:/Tests/santa_clara"

# Compare results (manual)
explorer C:/Tests/san_mateo/loaded_highway
explorer C:/Tests/alameda/loaded_highway
explorer C:/Tests/santa_clara/loaded_highway
```

### Programmatic Usage

```python
from pathlib import Path
from tests.test_highway_assign_skim import get_county_zones, CountyDataFilter

# Auto-detect zones
taz_range, maz_range = get_county_zones("San Mateo")

# Filter demand
filter_helper = CountyDataFilter(
    taz_range=taz_range,
    maz_range=maz_range,
    county_name="San Mateo"
)

input_omx = Path("E:/2015_TM2_20250619/demand_matrices/highway/household/TAZ_Demand_AM.omx")
output_omx = Path("C:/Tests/san_mateo/inputs/demand/TAZ_Demand_AM.omx")

filter_helper.filter_trip_table(input_omx, output_omx)

# Run highway components
from tests.highway_assign_skim_controller import CountyHighwayController

controller = CountyHighwayController(
    scenario_config="C:/Tests/san_mateo/config/scenario.toml",
    model_config="C:/Tests/san_mateo/config/model.toml",
    run_dir="C:/Tests/san_mateo",
    county_name="San Mateo",
    include_maz_components=False,
    include_network_summary=False
)

controller.run_highway_only()
```

## Troubleshooting Reference

| Error | Cause | Solution |
|-------|-------|----------|
| "EMME modules not available" | Not in EMME Python environment | Activate tm2pyenv, ensure EMME installed |
| "Scenario 100 not found" | all_day_scenario_id wrong | Check emmebank, update scenario.toml |
| "No zones found for county" | County name typo/wrong | Check crosswalk file for exact county names |
| "Crosswalk file not found" | Wrong path | Update CROSSWALK_FILE constant |
| "demand_matrices not found" | Wrong source dataset | Verify E:\2015_TM2_20250619 exists |
| Assignment doesn't converge | Bad network/demand | Check logs, verify network attributes |
| "Permission denied" on EMME copy | File locks | Close EMME Desktop, retry |
| Filtered trips = 0 | Zone ranges wrong | Verify detected ranges match demand matrices |

## Related Documentation

- **[TESTING_INSTRUCTIONS.md](TESTING_INSTRUCTIONS.md)** - Quick start guide for new users
- **[HIGHWAY_ASSIGN_SKIM_README.md](HIGHWAY_ASSIGN_SKIM_README.md)** - Detailed framework documentation
- **[COUNTY_TEST_SETUP_CHECKLIST.md](COUNTY_TEST_SETUP_CHECKLIST.md)** - Pre-flight checklist
- **[examples_highway_assign_skim.py](examples_highway_assign_skim.py)** - Runnable code examples
- **[HIGHWAY_ASSIGN_SKIM_QUICK_REFERENCE.md](HIGHWAY_ASSIGN_SKIM_QUICK_REFERENCE.md)** - Command reference

## Version History

- **v1.0 (Dec 2025)** - Initial framework with auto-detection and filtering
  - Automated test script
  - Zone auto-detection from crosswalk
  - Configurable demand filtering
  - Single time period testing
  - User-specified output directories
