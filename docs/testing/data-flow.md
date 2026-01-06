# County Test Data Flow

## Overview

This document details the complete input-to-output data flow for the county highway test framework.

## Input Files

### Configuration File
```
tests/county_test_config.toml
```

**All paths and settings are now configured in this file.** Edit this file before running tests.

**Key sections:**
- `[paths]` - Source dataset, output directory, crosswalk file locations
- `[test]` - County name, filtering options, test behavior
- `[emme]` - EMME scenario settings and time periods
- `[components]` - Which components to run
- `[logging]` - Log levels

See the config file for detailed documentation of each setting.

### Source Dataset Location
Configured in `county_test_config.toml`:
```toml
[paths]
source_dataset = "E:/2015_TM2_20250619"
```

### Output/Test Directory Location
Configured in `county_test_config.toml`:
```toml
[paths]
output_dir = "E:/Tests/san_mateo_test"
```

### Crosswalk File Location
Configured in `county_test_config.toml`:
```toml
[paths]
crosswalk_file = "C:/GitHub/tm2py-utils/tm2py_utils/inputs/maz_taz/mazs_tazs_county_tract_PUMA_2.5.csv"
```

### Required Input Files

#### 1. EMME Project (Network)
**Location**: `E:\2015_TM2_20250619\emme_project\`

**Files**:
- `mtc_emme.emp` - EMME project file
- `Database_highway/emmebank` - Highway network database
- Base scenario (default ID: 100) with loaded all-day network

**Purpose**: Contains the highway network with nodes, links, and attributes

**Used By**: 
- `create_tod_scenarios` - Copies base network to time-period scenarios
- All highway components

#### 2. MAZ Land Use Data
**Location**: `E:\2015_TM2_20250619\inputs\landuse\maz_data.csv`

**Key Fields**:
- `MAZ` or `MAZ_ORIGINAL` - MAZ zone ID
- `COUNTY` - County name (for filtering)
- `TAZ` - Parent TAZ zone
- Other land use attributes (population, employment, etc.)

**Purpose**: 
- Identifies which MAZs belong to which county
- Used for demand filtering
- Referenced by MAZ components

**Used By**:
- `run_county_test.py` - For zone detection and filtering
- MAZ assignment/skimming components

#### 3. Highway Tolls
**Location**: `E:\2015_TM2_20250619\inputs\hwy\tolls.csv`

**Key Fields**:
- `a`, `b` - Link nodes
- `toll{period}_{vehicle}` - Toll by time period and vehicle type
  - Example: `tollea_da`, `tollam_s2`, `tollpm_lrg`
  - Periods: `ea`, `am`, `md`, `pm`, `ev`
  - Vehicles: `da`, `s2`, `s3`, `vsm`, `sml`, `med`, `lrg`

**Purpose**: Link-specific toll costs by vehicle class and time period

**Used By**: `prepare_network_highway` component

#### 4. Demand Matrices (OMX files)
**Location**: `E:\2015_TM2_20250619\demand_matrices\highway\household\`

**Files** (one per time period):
- `TAZ_Demand_EA.omx`
- `TAZ_Demand_AM.omx`
- `TAZ_Demand_MD.omx`
- `TAZ_Demand_PM.omx`
- `TAZ_Demand_EV.omx`

**Structure**:
- Matrix dimensions: 1454×1454 TAZs (full region)
- Multiple matrices per file (one per vehicle class)
- Zone mapping stored as `taz` mapping

**Purpose**: Origin-destination trip tables by time period

**Used By**: `highway` component for traffic assignment

#### 5. Crosswalk File (for zone detection)
**Location**: `C:\GitHub\tm2py-utils\tm2py_utils\inputs\maz_taz\mazs_tazs_county_tract_PUMA_2.5.csv`

**Key Fields**:
- `MAZ_SEQ` - MAZ zone ID
- `TAZ_SEQ` - TAZ zone ID
- `county_name` - County name

**Purpose**: Maps MAZs/TAZs to counties for automatic zone range detection

## Path Configuration Reference

### How to Configure Paths

**All paths are now centralized in `tests/county_test_config.toml`**

1. **Edit the config file:**
   ```powershell
   notepad tests\county_test_config.toml
   ```

2. **Update the paths section:**
   ```toml
   [paths]
   source_dataset = "E:/2015_TM2_20250619"  # Your source data location
   output_dir = "E:/Tests/san_mateo_test"    # Where test results go
   crosswalk_file = "C:/GitHub/tm2py-utils/..."  # Zone mapping file
   ```

3. **Update test settings:**
   ```toml
   [test]
   county_name = "San Mateo"     # County to test
   filter_demand = true           # Filter to intra-county trips
   skip_emme_copy = false         # Copy EMME project
   skip_setup = false             # Create test directory
   ```

4. **Run the test:**
   ```powershell
   python tests\run_county_test.py
   ```

### Using Multiple Configurations

Create different config files for different scenarios:

```powershell
# Copy the template
copy tests\county_test_config.toml tests\san_mateo_config.toml
copy tests\county_test_config.toml tests\alameda_config.toml

# Edit each for different counties
notepad tests\san_mateo_config.toml
notepad tests\alameda_config.toml

# Run with specific config
python tests\run_county_test.py --config tests\san_mateo_config.toml
python tests\run_county_test.py --config tests\alameda_config.toml
```

### Config File Reference

| Section | Setting | Description |
|---------|--python tests\run_county_test.py`
**Configuration**: `tests/county_test_config.toml
| **[paths]** | `source_dataset` | Location of full model dataset |
| | `output_dir` | Where to create test directory |
| | `crosswalk_file` | TAZ/MAZ to county mapping file |
| **[test]** | `county_name` | County to test |
| | `filter_demand` | Filter to intra-county trips only |
| | `skip_emme_copy` | Don't copy EMME project (use existing) |
| | `skip_setup` | Don't create directory structure (use existing) |
| | `thin_network` | Remove low functional class links (optional) |
| | `auto_confirm` | Skip confirmation prompt |
| **[emme]** | `all_day_scenario_id` | Base scenario ID with network |
| | `time_periods` | List of time periods to test |
| **[components]** | `run_components` | List of components to execute |
| **[logging]** | `console_log_level` | Console logging verbosity |
| | `file_log_level` | File logging verbosity |

See `tests/county_test_config.toml` for complete documentation and examples.

## Data Transformation Flow

### Step 1: Test Directory Setup
**Script**: `run_county_test.py --output-dir DIR --county COUNTY`

**Actions**:
1. Detects county zone ranges from crosswalk file
2. Creates test directory structure
3. Copies configuration templates
4. Copies EMME project (full network)
5. Copies tolls.csv (unfiltered)
6. Copies maz_data.csv (unfiltered)
7. Optionally filters demand matrices

**Input → Output Mapping**:
```
Source Dataset                          Test Directory
├── emme_project/                   →   ├── emme_project/
│   └── Database_highway/emmebank       │   └── Database_highway/emmebank
├── inputs/hwy/tolls.csv            →   ├── inputs/hwy/tolls.csv (copied as-is)
├── inputs/landuse/maz_data.csv     →   ├── inputs/landuse/maz_data.csv (copied as-is)
└── demand_matrices/.../                └── inputs/demand/
    TAZ_Demand_AM.omx (1454×1454)  →        TAZ_Demand_AM.omx (filtered to 229×229 if filter_demand=true)
```

### Step 2: Demand Filtering (Optional)
**Controlled by**: `filter_demand = true` in scenario config

**Process**:
1. Read maz_data.csv to identify county MAZs
2. Build TAZ-to-MAZ mapping
3. For each time period's OMX file:
   - Read full regional matrix (1454×1454)
   - Identify TAZ indices within county range
   - Extract submatrix (county origins × county destinations)
   - Write filtered OMX to test directory

**Example - San Mateo County**:
```
Input:  TAZ_Demand_AM.omx (1454×1454 zones, ~10M trips)
Output: TAZ_Demand_AM.omx (229×229 zones, ~500K trips)
Reduction: 95% fewer O-D pairs
```

### Step 3: Component Execution

#### Component 1: create_tod_scenarios
**Input**:
- EMME base scenario (ID 100) with all-day network
- Time period definitions from scenario config

**Process**:
1. For each time period (EA, AM, MD, PM, EV):
   - Create new EMME scenario
   - Copy network from base scenario
   - Copy time-specific attributes to generic names
     - Example: `@useclass_am` → `@useclass`
   - Apply time-of-day capacity factors

**Output**:
- New EMME scenarios (IDs: 1, 11, 12, 13, 14, 15)
- Each scenario has time-specific network ready for assignment

**Files Created**: None (modifies EMME database)

#### Component 2: prepare_network_highway
**Input**:
- Time-of-day EMME scenarios from step 1
- `tolls.csv`
- Highway configuration from model config

**Process**:
1. For each time period scenario:
   - Apply tolls to links (create `@bridgetoll_*`, `@valuetoll_*` attributes)
   - Create toll flags (`is_toll_*` attributes)
   - Set volume-delay functions (VDFs)
   - Configure modes and vehicle classes
   - Calculate generalized costs

**Output**:
- EMME scenarios with network attributes set for assignment
- Link attributes: `@bridgetoll_da`, `@valuetoll_s2`, `is_toll_da`, etc.

**Files Created**: None (modifies EMME database)

#### Component 3: highway
**Input**:
- Prepared EMME scenarios from step 2
- Filtered demand matrices from `inputs/demand/`
- Highway assignment parameters from model config

**Process**:
For each time period (default: AM only):
1. **Traffic Assignment**:
   - Load trip matrices for each vehicle class
   - Run multi-class equilibrium assignment
   - Iterate until convergence (relative gap < 0.0001)
   - Update link volumes, speeds, and travel times

2. **Skimming**:
   - Generate level-of-service matrices
   - Calculate shortest paths for each class
   - Create skim matrices (time, distance, toll, etc.)

**Output Files**:

##### A. Highway Skims
**Location**: `{test_dir}/outputs/skims/`

**Files**:
```
HWYSKIM_AM.omx (or per time period)
```

**Structure** (OMX format):
- Dimensions: 1454×1454 (full network zones)
- Matrices (per vehicle class):
  - `time_da` - Travel time for drive alone
  - `dist_da` - Distance for drive alone
  - `bridgetoll_da` - Bridge toll costs
  - `freeflowtime_da` - Free-flow travel time
  - `time_s2` - Travel time for shared ride 2
  - (repeated for each class: da, s2, s3, vsm, sml, med, lrg)

##### B. Loaded Network
**Location**: `{test_dir}/outputs/loaded_highway/`

**Files**:
```
loaded_highway_AM.csv (or per time period)
```

**Structure** (CSV format):
- One row per link
- Columns:
  - `i_node`, `j_node` - Link endpoints
  - `@ft` - Facility type
  - `length` - Link length
  - `lanes` - Number of lanes
  - `auto_volume` - Total vehicle volume
  - `auto_time` - Link travel time
  - `volau` - Auxiliary volumes per class
  - `@bridgetoll_da`, `@valuetoll_s2`, etc. - Toll attributes
  - Plus 80+ other EMME link attributes

##### C. Assignment Logs
**Location**: `{test_dir}/logs/`

**Files**:
- `tm2py_detail.log` - Detailed component execution log
- `tm2py_summary.log` - High-level progress and timing

**Content**:
- Component start/end times
- Assignment convergence statistics (gap, iterations)
- File paths and operations
- Error messages and warnings

## Complete File Tree

### Before Test Run
```
E:\2015_TM2_20250619\                    (Source Dataset)
├── emme_project\
│   ├── mtc_emme.emp
│   └── Database_highway\
│       └── emmebank                      [Base scenario ID 100]
├── inputs\
│   ├── hwy\
│   │   └── tolls.csv                     [Full regional tolls]
│   └── landuse\
│       └── maz_data.csv                  [Full regional MAZ data]
└── demand_matrices\
    └── highway\
        └── household\
            ├── TAZ_Demand_EA.omx         [1454×1454 zones]
            ├── TAZ_Demand_AM.omx         [1454×1454 zones]
            ├── TAZ_Demand_MD.omx         [1454×1454 zones]
            ├── TAZ_Demand_PM.omx         [1454×1454 zones]
            └── TAZ_Demand_EV.omx         [1454×1454 zones]

C:\GitHub\tm2py-utils\                    (Zone Crosswalk)
└── tm2py_utils\
    └── inputs\
        └── maz_taz\
            └── mazs_tazs_county_tract_PUMA_2.5.csv
```

### After Test Run (filter_demand = true)
```
{test_dir}\                               (e.g., E:\Tests\san_mateo_test\)
├── config\
│   ├── scenario.toml                     [Generated from template]
│   └── model.toml                        [Generated from template]
├── emme_project\
│   ├── mtc_emme.emp                      [Copied from source]
│   └── Database_highway\
│       └── emmebank                      [Scenarios: 100, 1, 11, 12, 13, 14, 15]
├── inputs\
│   ├── hwy\
│   │   └── tolls.csv                     [Copied - full regional]
│   ├── landuse\
│   │   └── maz_data.csv                  [Copied - full regional]
│   └── demand\
│       └── TAZ_Demand_AM.omx             [Filtered - 229×229 zones for San Mateo]
├── outputs\
│   ├── skims\
│   │   └── HWYSKIM_AM.omx                [Generated - 1454×1454 zones]
│   └── loaded_highway\
│       └── loaded_highway_AM.csv         [Generated - link volumes & attributes]
└── logs\
    ├── tm2py_detail.log                  [Generated - detailed execution log]
    └── tm2py_summary.log                 [Generated - summary log]
```

## Data Size Reference (San Mateo County)

| File | Source Size | Filtered Size | Notes |
|------|-------------|---------------|-------|
| EMME emmebank | ~500 MB | ~500 MB | Full network (not filtered) |
| TAZ_Demand_AM.omx | ~20 MB | ~1 MB | 95% reduction |
| tolls.csv | ~5 MB | ~5 MB | Not filtered |
| maz_data.csv | ~50 MB | ~50 MB | Not filtered (could be) |
| HWYSKIM_AM.omx | N/A | ~100 MB | Generated for full network |
| loaded_highway_AM.csv | N/A | ~50 MB | All links with volumes |

## Key Insights

### What Gets Filtered
✓ **Demand matrices** - Reduced to intra-county trips (configurable)

### What Doesn't Get Filtered
✗ **Network** - Full regional network preserved (needed for routing)
✗ **Skims** - Generated for all zones (even if zero demand)
✗ **Tolls** - All regional toll data included
✗ **MAZ data** - All regional MAZ records included (could be filtered)

### Why Keep Full Network?
Even intra-county trips may use routes through other counties. Example:
- Trip: San Mateo TAZ 250 → San Mateo TAZ 350
- Route: May use I-280 through Santa Clara County
- If network filtered to San Mateo only: Invalid routing

## Validation Checks

### Pre-Run Checks
1. Source EMME project exists
2. Base scenario (ID 100) has loaded network
3. All input files exist and are readable
4. Crosswalk file can detect county zones
5. Demand matrices contain county TAZs

### Post-Run Checks
1. Time-period scenarios created (1, 11-15)
2. Assignment converged (relative gap < threshold)
3. Skim files generated with expected matrices
4. Link volumes non-negative
5. No missing/infinite values in outputs

## Troubleshooting

### Missing Input Files
**Symptom**: FileNotFoundError during setup
**Check**: Verify paths in source dataset
**Fix**: Update `SOURCE_DATASET` constant or use `--source-dir` flag

### Zero Filtered Trips
**Symptom**: Filtered OMX has all zeros
**Check**: Zone ranges match demand matrix zones
**Fix**: Verify crosswalk file county names and TAZ numbering

### Assignment Doesn't Converge
**Symptom**: Max iterations reached, high relative gap
**Check**: Network attributes, demand reasonableness
**Fix**: Review logs, adjust convergence criteria, check for network errors
