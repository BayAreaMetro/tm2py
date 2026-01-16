# Setup Component Integration

## Overview

The setup component has been integrated into the tm2py component workflow, allowing model initialization (copying input files and initializing EMME networks) to be part of the standard model run process.

## What Changed

### 1. New Setup Component
- Added `"setup"` to the ComponentNames literal in `config.py`
- Created `Setup` component class in `components/setup.py`
- Registered setup in `component_cls_map` in `controller.py`

### 2. Automatic Validation
When setup is **not** included in `initial_components`, the controller automatically validates that required input files exist before starting the run. If files are missing, it provides a clear error message with instructions.

### 3. Configuration Integration
Setup is now treated like any other component - add it to `initial_components` in your scenario_config.toml.

## Usage

### Option 1: With Setup Component (Recommended for fresh runs)

Add `"setup"` as the **first** component in `initial_components`:

```toml
# scenario_config.toml
[run]
    initial_components = [
        "setup",                      # ← Add this first!
        "create_tod_scenarios",
        "active_modes",
        # ... rest of your components
    ]
    start_iteration = 0
    end_iteration = 3
```

**Requirements:**
- Must have `setupmodel_config.toml` in the run directory
- setupmodel_config.toml must specify paths to input files (see below)

**What it does:**
1. Reads setupmodel_config.toml from run directory
2. Copies input files from source to run directory (hwy, trn, landuse, popsyn, nonres)
3. Unzips EMME network databases
4. Copies warmstart files if configured
5. Downloads config files from GitHub if specified

### Option 2: Without Setup Component (For existing runs)

If you've already run setup manually or have all input files in place, simply omit "setup" from initial_components:

```toml
[run]
    initial_components = [
        "create_tod_scenarios",
        "active_modes",
        # ... your components
    ]
```

**What happens:**
- Controller automatically validates that required files exist
- If files are missing, raises FileNotFoundError with helpful message
- If files exist, run proceeds normally

## setupmodel_config.toml Structure

The setup component reads `setupmodel_config.toml` from your run directory. Example:

```toml
#######################
# Setup Model Configs #
#######################

# Location of network support files (tolls, transit fares, etc.)
INPUT_NETWORK_DIR = "E:/Box/Model Inputs/2015-tm22-dev-sprint-03"

# Location of EMME Networks (built from Lasso)
INPUT_EMME_NETWORK_DIR = "E:/Box/Model Inputs/2015-tm22-dev-sprint-03/emme_network"

# Location of population sim and land use inputs
INPUT_POPLU_DIR = "E:/Box/Model Inputs/2015-tm22-dev-sprint-03"

# Location of non-residential inputs
INPUT_NONRES_DIR = "E:/Box/Model Inputs/2015-tm22-dev-sprint-03"

# Location of warmstart demand/skims (if using warmstart)
WARMSTART_FILES_DIR = "E:/Box/Model Inputs/2015-tm22-dev-sprint-03/warmstart"

# Location of EMME project template
EMME_TEMPLATE_PROJECT_DIR = "E:/Box/Model Inputs/2015-tm22-dev-sprint-03/emme_23_project_template"

# GitHub path for config files (optional)
CONFIGS_GITHUB_PATH = "https://raw.githubusercontent.com/BayAreaMetro/tm2py-utils/refs/heads/main/tm2py_utils/config/develop"

# Travel Model Two release tag
TRAVEL_MODEL_TWO_RELEASE_TAG = "TM2.2.4"
```

## EMME Network Setup Flow

Understanding how EMME networks are initialized is critical for debugging network-related issues.

### Components of EMME Setup

#### 1. Project Template (`EMME_TEMPLATE_PROJECT_DIR`)
**What it is:** Empty EMME project structure with folders and configuration.

**Contents:**
- `.emp` project file
- `Database/` folder (empty or with minimal empty emmebank)
- Configuration for dimensions (scenarios, zones, extra attributes capacity)

**Purpose:** Provides the skeleton structure for an EMME project.

**Key Point:** Contains **NO network data, NO extra attributes** - just the structure.

**Version Matching:** The template EMME version (e.g., `emme_23_project_template` or `emme_25_project_template`) should ideally match the EMME version that created the database zips, though EMME often maintains backward compatibility.

#### 2. Database Zips (`INPUT_EMME_NETWORK_DIR`)
**What it is:** Compressed EMME network databases with actual network data built from Lasso.

**Contents:**
- Nodes (TAZ, MAZ, network nodes with coordinates)
- Links (roadway segments with standard EMME attributes)
- **Standard attributes only**: 
  - `length` - link length
  - `lanes` - number of lanes (lowercase, not `@lanes`)
  - `modes` - allowed modes
  - `type` - link type
  - `vdf` - volume delay function
  - Other base EMME attributes

**Does NOT contain:**
- Extra attributes (`@lanes`, `@capclass`, `@useclass`, `@free_flow_time`, etc.)
- Time-of-day specific scenarios
- Populated volume fields

**Location:** Typically in `emme_network/` subdirectory:
- `Database_highway_EMME_25.00.01.zip`
- `Database_transit_EMME_25.00.01.zip`
- `Database_active_north_EMME_25.00.01.zip`
- `Database_active_south_EMME_25.00.01.zip`

#### 3. Setup Component Execution Flow

```
Step 1: Copy Project Template
  Source: EMME_TEMPLATE_PROJECT_DIR
  Destination: <run_dir>/emme_project/
  Result: Empty project structure created

Step 2: Unzip Database Zips
  Source: INPUT_EMME_NETWORK_DIR/*.zip
  Destination: <run_dir>/emme_project/Database_*/
  Result: Network data now in project (nodes, links with standard attributes only)

Step 3: Initial Components Run (CRITICAL!)
  Components like create_tod_scenarios create extra attributes
```

#### 4. The `create_tod_scenarios` Component (CRITICAL!)

**Why it's needed:** Database zips intentionally do not contain extra attributes. These must be created by tm2py components during model setup.

**What `create_tod_scenarios` does:**

1. Reads base scenario from `Database_highway`
2. **Creates extra attributes** on links:
   - `@lanes` - copies from standard `lanes` attribute
   - `@capclass` - capacity class for highway assignment
   - `@useclass` - facility type classification
   - `@free_flow_time` - calculated free-flow travel time
   - `@free_flow_speed` - free-flow speed
   - `@area_type` - area type classification
   - Others as needed
3. Populates these attributes with calculated values
4. Creates time-of-day scenarios (EA, AM, MD, PM, EV)
5. Writes enhanced scenarios back to emmebank

**Configuration requirement:**

```toml
[run]
    initial_components = [
        "setup",                      # Copies template, unzips databases
        "create_tod_scenarios",       # ← MUST be in initial_components!
        # ... other components
    ]
```

**Common error if missing:**

```
ERROR: Missing required network attributes: @lanes
Traceback: prepare_network_highway → Missing @lanes attribute
```

#### 5. Complete Flow Diagram

```
Template (structure)
    ↓
+ Database Zip (network with standard attributes)
    ↓
+ create_tod_scenarios (creates extra attributes)
    ↓
= Complete EMME Project Ready for Assignment
```

**Without `create_tod_scenarios` in `initial_components`:**
```
❌ Missing: @lanes, @capclass, @useclass, etc.
❌ Components fail: prepare_network, highway_assignment, etc.
```

**With `create_tod_scenarios` in `initial_components`:**
```
✅ Has: all required extra attributes
✅ Components succeed: prepare_network, highway_assignment, etc.
```

### Troubleshooting EMME Network Issues

#### Issue: "Missing required network attributes: @lanes"

**Diagnosis:** Extra attributes were not created during setup.

**Solution:** 
1. Verify `create_tod_scenarios` is in `initial_components`
2. Verify it runs **before** components that need the attributes (like `prepare_network_highway`)
3. Check the log to confirm create_tod_scenarios executed successfully

**Common cause:** Running a partial model with custom `initial_components` that omits `create_tod_scenarios`.

#### Issue: "EMME version mismatch errors"

**Diagnosis:** Project template EMME version doesn't match database EMME version.

**Solution:** 
- Check database zip filenames (e.g., `EMME_25.00.01.zip` means EMME version 25.00.01)
- Use matching template (e.g., `emme_25_project_template` for EMME 25 databases)
- Note: EMME often maintains backward compatibility (EMME 25 databases may work with EMME 23 templates)

#### Issue: "Database appears empty after setup"

**Diagnosis:** Database zips weren't unzipped or unzip failed.

**Solution:**
1. Check logs for unzip errors
2. Verify `INPUT_EMME_NETWORK_DIR` path is correct in setupmodel_config.toml
3. Verify database zip files exist and aren't corrupted
4. Check disk space in destination directory

### Key Insights

1. **Database zips are intentionally incomplete** - they contain only base EMME network data (nodes, links, standard attributes), not extra attributes needed by tm2py.

2. **Extra attributes are created by tm2py components** - specifically `create_tod_scenarios` and related network preparation components.

3. **Order matters** - `create_tod_scenarios` must run before any components that reference extra attributes.

4. **Templates are just structure** - the project template provides the EMME project structure but no actual network data. The data comes from unzipping the database zips.

5. **Version matching is important but not always strict** - ideally use a template that matches your database EMME version, but EMME maintains reasonable backward compatibility.

## Files Validated When Setup is Skipped

If setup is not in initial_components, the controller checks for:

### Required Directories:
- `inputs/` - Main inputs directory
- `inputs/hwy/` - Highway network inputs
- `inputs/trn/` - Transit network inputs  
- `inputs/landuse/` - Land use data
- `emme_project/` - EMME project folder
- `emme_project/Database_highway/` - Highway EMME database
- `emme_project/Database_transit/` - Transit EMME database

### Required Files:
- `inputs/landuse/maz_data.csv` (from scenario.maz_landuse_file)
- `inputs/landuse/mtc_final_network_zone_seq.csv` (from scenario.zone_seq_file)

## Migration Guide

### For Existing Test Systems

If you have an existing test system that doesn't use setup:

**Before:**
```bash
# Manual process
python scripts/setup_model.py setupmodel_config.toml /path/to/run/dir
python scripts/run_model.py scenario_config.toml model_config.toml
```

**After (Option A - Integrate setup):**
```toml
# scenario_config.toml
[run]
    initial_components = ["setup", "create_tod_scenarios", ...]
```
```bash
# Single command - setup runs automatically
python scripts/run_model.py scenario_config.toml model_config.toml
```

**After (Option B - Keep manual setup):**
```bash
# Keep your existing workflow - setup is optional
python scripts/setup_model.py setupmodel_config.toml /path/to/run/dir
python scripts/run_model.py scenario_config.toml model_config.toml
```

### For New Test Systems

For new test systems, use Option A (integrated setup) for a streamlined single-command workflow.

## Benefits

1. **Single Command Workflow**: Setup + run in one command
2. **Reproducibility**: Setup configuration is version controlled alongside scenario config
3. **Validation**: Automatic checking of required files when setup is skipped
4. **Flexibility**: Setup can be included or excluded based on your needs
5. **Logging**: Setup activities are logged in the main model run log

## Implementation Files

- [tm2py/config.py](c:\GitHub\tm2py\tm2py\config.py) - Added "setup" to ComponentNames
- [tm2py/components/setup.py](c:\GitHub\tm2py\tm2py\components\setup.py) - Setup component class
- [tm2py/controller.py](c:\GitHub\tm2py\tm2py\controller.py) - Integration and validation
- [Example: scenario_config_with_setup.toml](e:\2015_TM2_20250619\scenario_config_with_setup.toml) - Example configuration

## Testing

To test the setup component integration:

```bash
# 1. Create a new test run directory
mkdir e:\test_setup_component

# 2. Copy config files
copy model_config.toml e:\test_setup_component\
copy setupmodel_config.toml e:\test_setup_component\

# 3. Create scenario_config.toml with setup in initial_components
# (see example above)

# 4. Run the model - setup will execute first
cd c:\GitHub\tm2py
python -m tm2py.controller -s e:\test_setup_component\scenario_config.toml -m e:\test_setup_component\model_config.toml
```

The setup component will:
- Log its progress
- Validate setupmodel_config.toml
- Copy all input files
- Initialize EMME networks
- Then proceed with other components

## Troubleshooting

### Error: "Setup component requires setupmodel_config.toml"
**Solution**: Create setupmodel_config.toml in your run directory with appropriate paths

### Error: "Required input files/directories are missing"
**Solution**: Either:
- Add "setup" to initial_components to have files copied automatically, OR
- Manually copy required files to run directory

### Error: "Invalid setup configuration"
**Solution**: Check that all required fields in setupmodel_config.toml are present and paths are valid

## Future Enhancements

Potential future improvements:
- Support for setup_config_file path in scenario config (instead of requiring it in run_dir)
- Skip setup validation for specific components that don't need inputs
- Setup component dry-run mode to preview what will be copied
- Progress bars for large file copies
