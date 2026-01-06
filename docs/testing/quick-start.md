# Quick Start: County Test Setup

This guide lists **all paths you need to change** when setting up or updating county tests with new input data.

## TL;DR - Files to Edit

When input data changes, you must update paths in **TWO configuration files**:

1. **`tests/county_test_config.toml`** - Main test configuration (paths, county, options)
2. **`tests/config_templates/fixed_san_mateo_model.toml`** - Model configuration (automatically copied to test directory)

⚠️ **The scenario config is automatically generated** - you don't need to edit `fixed_san_mateo_scenario.toml` manually.

---

## File 1: tests/county_test_config.toml

### Paths You Must Change

```toml
[paths]
# 1. Source dataset - MUST contain emme_project, inputs, demand_matrices
source_dataset = "E:/2015_TM2_20250619"

# 2. Output directory - Where test results go
output_dir = "E:/Tests/san_mateo_test"

# 3. Crosswalk file - Zone-to-county mapping
crosswalk_file = "C:/GitHub/tm2py-utils/tm2py_utils/inputs/maz_taz/mazs_tazs_county_tract_PUMA_2.5.csv"
```

### County Selection

```toml
[test]
county_name = "San Mateo"  # Change to test different county
```

### Optional Settings

```toml
[test]
filter_demand = true       # true = faster (intra-county only), false = full regional
thin_network = 4          # Optional: Remove links with @ft > 4 (comment out for full network)

[emme]
time_periods = ["AM"]     # Quick test, or ["EA", "AM", "MD", "PM", "EV"] for full day
```

---

## File 2: tests/config_templates/fixed_san_mateo_model.toml

This file contains relative paths that point to files **inside the test directory**. Most paths are correct by default, but check these sections:

### Section A: Highway Network Files

**These paths are relative to the test output directory and should work as-is:**

```toml
[highway]
interchange_nodes_file = "inputs/hwy/interchange_nodes.csv"  # ✓ Auto-copied
model_to_emme_node_id_xwalk = "inputs/hwy/node_xwalk.csv"   # ✓ Auto-generated
output_node_sequential_id_xwalk = "inputs/hwy/node_seq_xwalk.csv"  # ✓ Auto-generated

[highway.tolls]
file_path = "inputs/hwy/tolls.csv"  # ✓ Auto-copied

[highway.maz_to_maz]
demand_file = "inputs/demand/maz_demand.omx"  # ✓ Placeholder (not used in basic test)
```

### Section B: Demand Files (Most Important!)

**These are automatically updated by the setup script** to point to filtered demand files:

```toml
[household]
# *** UPDATED AUTOMATICALLY by run_county_test.py setup ***
# After setup, these will point to inputs/demand/TAZ_Demand_{period}.omx
highway_demand_file = "output/household_highway.omx"  # Gets updated to filtered demand
transit_demand_file = "output/household_transit.omx"  # Gets updated to filtered demand
active_demand_file = "output/household_active.omx"    # Gets updated to filtered demand
```

**Setup script updates these to:**
```toml
highway_demand_file = "inputs/demand/TAZ_Demand_AM.omx"  # Filtered intra-county trips
# (Same for EA, MD, PM, EV if testing multiple periods)
```

### Section C: Truck Demand (DISABLED)

```toml
[truck]
# *** IMPORTANT: TRUCK DEMAND IS DISABLED FOR COUNTY TESTS! ***
# County tests focus on household demand only for faster testing
highway_demand_file = ""  # Empty = disabled
```

### Section D: Air Passenger Demand

```toml
[air_passenger]
# *** UPDATED AUTOMATICALLY by run_county_test.py setup ***
highway_demand_file = "output/air_passenger_highway.omx"  # Gets updated
```

### Section E: Internal-External Demand

```toml
[internal_external]
# *** UPDATED AUTOMATICALLY by run_county_test.py setup ***
highway_demand_file = "output/internal_external_highway.omx"  # Gets updated
```

---

## What Gets Updated Automatically?

The `run_county_test.py` script automatically updates these paths in the model config:

| Original Path | Updated To | Reason |
|---------------|------------|--------|
| `household.highway_demand_file` | `inputs/demand/TAZ_Demand_{period}.omx` | Points to filtered county demand |
| `household.transit_demand_file` | `inputs/demand/TAZ_Demand_{period}.omx` | Same file (multi-matrix OMX) |
| `household.active_demand_file` | `inputs/demand/TAZ_Demand_{period}.omx` | Same file (multi-matrix OMX) |
| `air_passenger.highway_demand_file` | `inputs/demand/TAZ_Demand_{period}.omx` | Placeholder for county test |
| `internal_external.highway_demand_file` | `inputs/demand/TAZ_Demand_{period}.omx` | Placeholder for county test |

---

## File 3: tests/config_templates/fixed_san_mateo_scenario.toml

**This file is automatically updated** by the setup script. You don't need to edit it manually.

The script updates these paths:
```toml
[emme]
project_path = "{output_dir}/emme_project/mtc_emme.emp"
highway_database_path = "{output_dir}/emme_project/Database_highway/emmebank"
# etc.
```

---

## Complete Workflow

### 1. Update Source Data Location

Edit `tests/county_test_config.toml`:

```toml
[paths]
source_dataset = "E:/NEW_DATA_LOCATION/2015_TM2_20250619"
output_dir = "E:/Tests/my_test"
```

### 2. Verify Source Dataset Structure

Your source dataset **must** have this structure:

```
source_dataset/
├── emme_project/
│   ├── mtc_emme.emp
│   └── Database_highway/emmebank
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

### 3. Check Model Config (Optional)

Open `tests/config_templates/fixed_san_mateo_model.toml` and verify:

- All relative paths use forward slashes: `inputs/hwy/tolls.csv`
- Demand files will be updated automatically by setup script
- Truck demand is disabled: `highway_demand_file = ""`

### 4. Run the Test

```powershell
cd tests
C:\GitHub\tm2pyenv\Scripts\python.exe run_county_test.py
```

The script will:
1. ✓ Validate all source files exist
2. ✓ Create test directory structure
3. ✓ Copy EMME project and input files
4. ✓ Filter demand to intra-county trips
5. ✓ Update model config with correct demand paths
6. ✓ Update scenario config with test directory paths
7. ✓ Run highway assignment and skimming

---

## Testing Different Counties

To test a different county, create a new config:

```powershell
# Create county-specific config
copy tests\county_test_config.toml tests\alameda_config.toml

# Edit the new file
notepad tests\alameda_config.toml
```

Change these values:
```toml
[paths]
output_dir = "E:/Tests/alameda_test"  # Different output location

[test]
county_name = "Alameda"  # Different county
```

Run with the new config:
```powershell
cd tests
C:\GitHub\tm2pyenv\Scripts\python.exe run_county_test.py --config alameda_config.toml
```

---

## Common Issues

### Issue: "Source file not found"

**Problem:** Paths in `county_test_config.toml` are incorrect

**Solution:** 
1. Check `source_dataset` path exists
2. Verify it contains `emme_project/`, `inputs/`, `demand_matrices/`
3. Check file structure matches requirements above

### Issue: "County not found in crosswalk"

**Problem:** `county_name` doesn't match crosswalk file

**Solution:**
1. Open crosswalk CSV file
2. Check exact spelling in `county_name` column
3. Update `county_name` in config (case-sensitive)

### Issue: "Demand matrix not found"

**Problem:** Matrix names in demand files don't match config

**Solution:**
1. Check OMX files contain these matrices:
   - `SOV_GP_{period}`, `SOV_PAY_{period}`
   - `SR2_GP_{period}`, `SR2_PAY_{period}`
   - `SR3_GP_{period}`, `SR3_PAY_{period}`
2. Matrix names must match exactly (case-sensitive)
3. CTRAMP consolidation process uses `_PAY` suffix (not `_TOLL`)

### Issue: Test runs but results seem wrong

**Problem:** Using full regional demand instead of filtered

**Solution:**
1. Check `filter_demand = true` in `county_test_config.toml`
2. Delete test directory and rerun to ensure fresh filtering
3. Check log file for "Filtering demand" messages

---

## Path Summary Checklist

Before running a test with new data, verify:

- [ ] `tests/county_test_config.toml`:
  - [ ] `source_dataset` points to your data location
  - [ ] `output_dir` is where you want results
  - [ ] `crosswalk_file` path is correct
  - [ ] `county_name` matches crosswalk file
  
- [ ] Source dataset structure:
  - [ ] `emme_project/` exists with Database_highway
  - [ ] `inputs/hwy/` has tolls.csv and other required files
  - [ ] `demand_matrices/highway/household/` has TAZ_Demand_*.omx files
  - [ ] `inputs/validation/` has interchange_nodes.csv

- [ ] Model config (usually OK as-is):
  - [ ] `tests/config_templates/fixed_san_mateo_model.toml` uses relative paths
  - [ ] Truck demand is disabled
  - [ ] Demand file paths will be updated automatically

- [ ] Scenario config:
  - [ ] Don't edit manually - automatically generated

---

## See Also

- [Configuration Reference](configuration.md) - Complete documentation of all settings
- [County Test Guide](county-test-guide.md) - Step-by-step test execution
- [Data Flow](data-flow.md) - How data flows through the test
- [Quick Reference](quick-reference.md) - Command syntax and examples
