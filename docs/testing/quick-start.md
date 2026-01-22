# Quick Start: County Test Setup

This guide explains the complete process for setting up county tests with new input data.

## Prerequisites

Before running county tests, you need:

1. **Python environment**: Follow the [tm2py installation guide](../install.md) to set up your Python virtual environment with EMME support.

2. **EMME license**: Ensure you have access to EMME 24.01.00 or later.

3. **Test data**: Access to model inputs (EMME network, land use, demand matrices).

!!! tip "Environment Variable"
    Set the `EMMEPATH` environment variable before running tests:
    ```powershell
    $env:EMMEPATH = "C:\Program Files\Bentley\OpenPaths\EMME 24.01.00"
    ```

## Understanding the Configuration Files

### Where They Live (Before Test Runs)

```
tests/
├── county_test_config.toml              ← YOU EDIT THIS (main test settings)
└── config_templates/
    ├── fixed_san_mateo_model.toml       ← OPTIONAL EDIT (model settings)
    └── fixed_san_mateo_scenario.toml    ← DON'T EDIT (auto-generated)
```

### Where They Go (During Test Execution)

When you run the test, the script:

1. **Creates test directory** at your `output_dir` location
2. **Copies templates** to the test directory:
   ```
   E:/Tests/san_mateo_test/
   └── config/
       ├── model.toml        ← Copy of fixed_san_mateo_model.toml (auto-updated)
       └── scenario.toml     ← Copy of fixed_san_mateo_scenario.toml (fully rewritten)
   ```
3. **Automatically updates paths** in the copies to point to correct locations
4. **Runs the test** using the updated configs

## What You Need To Edit

### For New Input Data Location: ONLY Edit One File

**When you have a new dataset at a different path:**

1. **Edit:** `tests/county_test_config.toml`
   ```toml
   [paths]
   # EMME network project
   emme_project_source = "E:/NEW_LOCATION/emme_network"
   
   # Input files (tolls, land use) - script auto-detects folder structure
   inputs_source = "E:/NEW_LOCATION/Model_Inputs"
   
   # Demand matrices (from a model run) - optional, defaults to inputs_source
   demand_source = "E:/NEW_LOCATION/Model_Outputs"
   
   # Output directory
   output_dir = "E:/Tests/my_test"
   ```

2. **Run the test:**
   ```powershell
   C:\GitHub\tm2pyenv\Scripts\python.exe tests\run_county_test.py
   ```

3. **Done!** The script handles everything else.

### When To Edit Template TOMLs

**Only edit `tests/config_templates/fixed_san_mateo_model.toml` if:**
- Your dataset has different matrix names (not SOV_GP_AM, SR2_GP_AM, etc.)
- Your file structure differs (not demand_matrices/highway/household/)
- You want to change model settings (VOT, capacity factors, etc.)

**Never edit `fixed_san_mateo_scenario.toml`** - it's completely rewritten by the script.

---

## File 1: tests/county_test_config.toml (Main Test Settings)

### Paths You Must Change

```toml
[paths]
# 1. EMME network project
emme_project_source = "E:/Box/.../Model Inputs/2015-tm22-dev-sprint-04/emme_network"

# 2. Input files (tolls, land use) - script auto-detects folder structure
inputs_source = "E:/Box/.../Model Inputs/2015-tm22-dev-sprint-04"

# 3. Demand matrices (from a model run) - optional, defaults to inputs_source
demand_source = "E:/Box/.../Model Outputs/2015-tm22-dev-sprint-04"

# 4. Output directory - Where test results go
output_dir = "E:/Tests/san_mateo_test"

# 5. Crosswalk file - Zone-to-county mapping
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
auto_confirm = true       # Skip confirmation prompts (for automated runs)

[emme]
time_periods = ["AM"]     # Quick test, or ["EA", "AM", "MD", "PM", "EV"] for full day

[logging]
console_log_level = "INFO"   # Console output level (DEBUG, INFO, WARNING, ERROR)
file_log_level = "DEBUG"     # Log file detail level (DEBUG recommended)
```

---

## File 2: tests/config_templates/fixed_san_mateo_model.toml (Model Settings)

**Location:** Stays in `tests/config_templates/` as a template

**Copied to:** `{output_dir}/config/model.toml` during test setup

**Auto-updated:** Yes - demand file paths are rewritten by script

This file contains **relative paths** that point to files inside the test directory. Most paths work as-is because they're relative.

### Paths That Work As-Is (Relative Paths)

```toml
[highway]
interchange_nodes_file = "inputs/hwy/interchange_nodes.csv"  # ✓ Relative path
model_to_emme_node_id_xwalk = "inputs/hwy/node_xwalk.csv"   # ✓ Created during network build
output_node_sequential_id_xwalk = "inputs/hwy/node_seq_xwalk.csv"  # ✓ Created at runtime

[highway.tolls]
file_path = "inputs/hwy/tolls.csv"  # ✓ Relative path

[highway.maz_to_maz]
demand_file = "inputs/demand/maz_demand.omx"  # ✓ Placeholder (not used in basic test)
```

### Paths Auto-Updated By Script

**In the template (`fixed_san_mateo_model.toml`):**
```toml
[household]
highway_demand_file = "output/household_highway.omx"  # Placeholder
transit_demand_file = "output/household_transit.omx"  # Placeholder
active_demand_file = "output/household_active.omx"    # Placeholder
```

**After script copies to test directory (`{output_dir}/config/model.toml`):**
```toml
[household]
highway_demand_file = "inputs/demand/TAZ_Demand_AM.omx"  # ← Script updates
transit_demand_file = "inputs/demand/TAZ_Demand_AM.omx"  # ← Script updates
active_demand_file = "inputs/demand/TAZ_Demand_AM.omx"   # ← Script updates
```

**You don't need to edit these** - the script automatically:
1. Copies the template to `{output_dir}/config/model.toml`
2. Updates all household demand paths to point to filtered demand files
3. Updates air_passenger and internal_external paths too

### Truck Demand (Always Disabled)

```toml
[truck]
# *** IMPORTANT: TRUCK DEMAND IS DISABLED FOR COUNTY TESTS! ***
highway_demand_file = ""  # Empty = disabled
```

⚠️ **County tests use HOUSEHOLD demand ONLY**. Truck demand is not included.

---

## File 3: tests/config_templates/fixed_san_mateo_scenario.toml

**Location:** Stays in `tests/config_templates/` as a template

**Copied to:** `{output_dir}/config/scenario.toml` during test setup  

**Auto-updated:** Yes - COMPLETELY REWRITTEN by script

**You NEVER need to edit this file.** The script:
1. Copies it to `{output_dir}/config/scenario.toml`
2. Rewrites ALL EMME paths to absolute paths pointing to your test directory:
   ```toml
   [emme]
   project_path = "E:/Tests/san_mateo_test/emme_project/mtc_emme.emp"
   highway_database_path = "E:/Tests/san_mateo_test/emme_project/Database_highway/emmebank"
   # etc.
   ```

---

## Complete Workflow Example

### Step 1: Edit Main Config

```powershell
notepad tests\county_test_config.toml
```

Update these lines:
```toml
[paths]
# EMME network project
emme_project_source = "E:/NEW_DATA/emme_network"

# Input files (tolls, land use) - script auto-detects folder structure
inputs_source = "E:/NEW_DATA/Model_Inputs"

# Demand matrices (from a model run)
demand_source = "E:/NEW_DATA/Model_Outputs"

# Where test will run
output_dir = "E:/Tests/my_test"
```

### Step 2: Verify Source Data Structure

Your source directories must contain:

**emme_project_source:**
```
emme_project_source/
├── emme_project/
│   ├── mtc_emme.emp
│   └── Database_highway/emmebank (or .zip)
```

**inputs_source** (script auto-detects either structure):
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
    └── landuse/
```

**demand_source:**
```
demand_source/
└── demand_matrices/
    └── highway/
        └── household/
            ├── TAZ_Demand_ea.omx
            ├── TAZ_Demand_am.omx
            └── ...
```

### Step 3: Run the Test

```powershell
C:\GitHub\tm2pyenv\Scripts\python.exe tests\run_county_test.py
```

**What the script does automatically:**

1. ✓ **Validates** source files exist (auto-detects folder structure)
2. ✓ **Creates** `E:/Tests/my_test/` directory structure:
   ```
   E:/Tests/my_test/
   ├── config/
   │   ├── model.toml      ← Copy of fixed_san_mateo_model.toml (paths updated)
   │   └── scenario.toml   ← Copy of fixed_san_mateo_scenario.toml (fully rewritten)
   ├── inputs/
   │   ├── hwy/            ← Copied from inputs_source
   │   ├── landuse/        ← Copied from inputs_source
   │   └── demand/         ← Filtered demand files from demand_source
   ├── emme_project/       ← Full copy from emme_project_source
   └── logs/               ← Test logs written here
   ```
3. ✓ **Copies** EMME project and input files
4. ✓ **Filters** demand to intra-county trips
5. ✓ **Updates** `config/model.toml` with correct demand file paths
6. ✓ **Updates** `config/scenario.toml` with absolute paths to test directory
7. ✓ **Runs** highway assignment and skimming

### Step 4: View Results

**Test completion message shows output locations:**
```
======================================================================
TEST COMPLETED SUCCESSFULLY
======================================================================
Test artifacts location: E:\Tests\my_test
  - Logs: E:\Tests\my_test\logs
  - Loaded network: E:\Tests\my_test\loaded_highway
  - Skims: E:\Tests\my_test\skim_matrices\highway
  - Full log: E:\Tests\my_test\logs\county_test_20260108_125547.log
```

**View the detailed log:**
```powershell
# View last 50 lines
Get-Content E:\Tests\my_test\logs\county_test_*.log -Tail 50

# Open in notepad
notepad E:\Tests\my_test\logs\county_test_*.log
```

**Log file contains:**
- Configuration values used
- Zone detection results (TAZ/MAZ ranges)
- Component execution progress with timestamps
- EMME operations and timing
- Validation results and warnings

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
1. Check `inputs_source` and `demand_source` paths exist
2. Verify `inputs_source` contains `hwy/` and `landuse/` (or `inputs/hwy/` structure)
3. Verify `demand_source` contains `demand_matrices/highway/household/`
4. Check `emme_project_source` contains `emme_project/` folder

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
  - [ ] `emme_project_source` points to EMME network folder
  - [ ] `inputs_source` points to input files (tolls, land use)
  - [ ] `demand_source` points to demand matrices (can be same as inputs_source)
  - [ ] `output_dir` is where you want results
  - [ ] `crosswalk_file` path is correct
  - [ ] `county_name` matches crosswalk file
  
- [ ] Source data structure:
  - [ ] `emme_project_source/emme_project/` exists with Database_highway
  - [ ] `inputs_source` has `hwy/tolls.csv` (or `inputs/hwy/tolls.csv`)
  - [ ] `demand_source/demand_matrices/highway/household/` has TAZ_Demand_*.omx files

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
