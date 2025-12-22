# County Highway Test - Quick Start Instructions

## What This Test System Does

This framework allows you to test highway assignment and skimming for a **single county** using a subset of the full regional model. It's designed for:

- **Fast iteration** - Test changes in minutes instead of hours
- **Debugging** - Isolate issues to a specific county
- **Development** - Validate new features on a manageable subset
- **Quality control** - Check network changes county-by-county

### Key Features

1. **Automatic Zone Detection** - Reads crosswalk file to find TAZ/MAZ ranges for any county
2. **Demand Filtering** - Extracts only intra-county trips (origin AND destination in county) from full trip tables
3. **Network Isolation** - Uses EMME project with all-day base network, creates time-of-day scenarios
4. **Single Time Period** - Runs only AM period for speed (configurable)
5. **No Code Changes** - Uses same tm2py components as full model
6. **User-Specified Output** - All test outputs go to your chosen directory (not in tm2py source)

### What Gets Filtered

- **Demand (OMX files)**: Filtered to intra-county trips only (configurable via `filter_demand = true` in scenario.toml)
- **MAZ land use data**: Could be filtered to county MAZs (not currently implemented)
- **Network**: Uses full regional network (not filtered - necessary for path-finding)
- **Skims**: Generated for all zones in network (filtered demand means only county O-D pairs used)

## Prerequisites

1. **EMME Environment**: You must run this from an EMME Python environment
2. **Source Data**: Verify dataset exists at `E:\2015_TM2_20250619` with EMME project containing loaded base network
3. **Disk Space**: Ensure ~10GB free space for test directory (includes EMME project copy)
4. **Crosswalk File**: Zone mapping file at `C:\GitHub\tm2py-utils\tm2py_utils\inputs\maz_taz\mazs_tazs_county_tract_PUMA_2.5.csv`

## Quick Test (Automated)

### Step 1: Open EMME Python Environment

**In PowerShell, run these commands:**

```powershell
# Navigate to tm2py directory
cd C:\GitHub\tm2py

# Activate the tm2pyenv environment
C:\GitHub\tm2pyenv\Scripts\Activate.ps1

# You should now see (tm2pyenv) in your prompt
```

**Note**: If you get an execution policy error, run:
```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

### Step 2: Run the Test Script

**Important**: You must specify an output directory (outside of the tm2py directory).

```powershell
# Run the test with your desired output location
python tests/run_county_test.py --county "San Mateo" --output-dir "C:/MyTests/san_mateo_test"

# Or use another drive/location
python tests/run_county_test.py --county "San Mateo" --output-dir "E:/Tests/highway_test"
```

The script will:
1. ✓ Check all prerequisites
2. ✓ Create test directory structure  
3. ✓ Copy EMME project from source dataset (must have loaded base network)
4. ✓ Copy required input files (tolls, land use)
5. ✓ **Filter demand to intra-county trips** (if `filter_demand = true` in config)
6. ✓ Run highway components:
   - `create_tod_scenarios` - Create time-of-day scenarios from base
   - `prepare_network_highway` - Set network attributes
   - `highway` - Assignment and skimming for AM period
7. ✓ Validate results

### Script Options

```powershell
# Specify output directory (required)
python tests/run_county_test.py --county "San Mateo" --output-dir "C:/MyTests/san_mateo_test"

# Test a different county
python tests/run_county_test.py --county "Alameda" --output-dir "C:/MyTests/alameda_test"

# Skip setup if test directory already exists
python tests/run_county_test.py --county "San Mateo" --output-dir "C:/MyTests/san_mateo_test" --skip-setup
```
, **create your test directory outside of the tm2py folder**:

### Step 1: Create Test Directory

```powershell
# Choose your output location (not in tm2py directory!)
$testDir = "C:\MyTests\san_mateo_test"

# Create directory structure
mkdir $testDir\config
mkdir $testDir\inputs\hwy
mkdir $testDir\inputs\landuse
mkdir $testDir\inputs\demand
mkdir $testDir\logs
```

### Step 2: Copy Config Files

```powershell
copy C:\GitHub\tm2py\tests\config_templates\san_mateo_scenario.toml $testDir\config\scenario.toml
copy C:\GitHub\tm2py\tests\config_templates\san_mateo_model.toml $testDir\config\model.toml
```

### Step 3: Copy EMME Project (Critical!)

```powershell
# This copies the EMME database with the base network
# Takes a few minutes - be patient!
xcopy /E /I E:\2015_TM2_20250619\emme_project $testDir\emme_project
```

### Step 4: Copy Input Files

```powershell
copy E:\2015_TM2_20250619\inputs\hwy\tolls.csv $testDir\inputs\hwy\
copy E:\2015_TM2_20250619\inputs\landuse\maz_data.csv $testDir\inputs\landuse\
```

### Step 5: Verify EMME Database

```powershell
# Check that the EMME database exists
Test-Path $testDir\emme_project\Database_highway\emmebank
# Should return: True
```

### Step 6: Run Test

```python
# From Python in EMME environment
from tests.highway_assign_skim_controller import CountyHighwayController

controller = CountyHighwayController(
    scenario_config="C:/MyTests/san_mateo_test/config/scenario.toml",
    model_config="C:/MyTests/san_mateo_test/config/model.toml",
    run_dir="C:/MyTests/san_mateo_testontroller(
    scenario_config="test_san_mateo/config/scenario.toml",
    model_config="test_san_mateo/config/model.toml",
    run_dir="test_san_mateo",
    county_name="San Mateo"
)

controller.run_highway_only()
```

## What Gets Tested

The test runs these components:
1. **prepare_network_highway**: Loads network, applies tolls, sets up attributes
2. **highway**: Highway assignment and skimming for AM period

**Note**: MAZ components and network summary are skipped by default for faster testing.

## Expected Output

If successful, you s in your specified output directory:
- `logs/` - Detailed logs
- `loaded_highway/` - Loaded network results
- `eation
- ✓ Final validation report

Output files created:
- `test_san_mateo/logs/` - Detailed logs
- `test_san_mateo/loaded_highway/` - Loaded network results
- `test_san_mateo/skim_matrices/highway/` - Skim matrices

## Troubleshooting to your output directory
- Check: `Test-Path <your-output-dir>\emme_project\Database_highway\emmebank`
- If missing, re-copy from source

### "Scenario 100 not found"
- Check what scenarios exist in your EMME database
- Update `all_day_scenario_id` in `<your-output-dir>
### "Scenario 100 not found"
- Check what scenarios exist in your EMME database
- Update `all_day_scenario_id` in `test_san_mateo/config/scenario.toml`

### "EMME modules not available"
- You must run from EMME Python environment
- Open EMME shell before running

### "Zone ranges don't match"
- You may need to find the correct crosswalk file from the 2015 dataset
- See note in [test_highway_assign_skim.py](test_highway_assign_skim.py#L33-L34)

### Test runs but assignment doesn't converge
- Check demand matrices are properly formatted
- Review logs in `<your-output-dir>/logs/`
- Verify network has proper capacity and speed attributes

### Want to see ALL trips (not just intra-county)?
- Edit `<your-output-dir>/config/scenario.toml`
- Change `filter_demand = true` to `filter_demand = false`
- Re-run setup: `python tests/run_county_test.py --county "San Mateo" --output-dir "<your-output-dir>"`

## Configuration Options

### Demand Filtering (scenario.toml)

```toml
[scenario]
# Set to true to filter demand to intra-county trips only
# Set to false to use all trips (slower but tests inter-county flows)
filter_demand = true
```

**With filtering enabled:**
- Only trips where BOTH origin AND destination are in the county
- Smaller trip tables = faster assignment
- Tests true intra-county travel patterns

**With filtering disabled:**
- All trips involving any zone in the network
- Slower assignment but tests full connectivity
- Useful for debugging regional routing issues

### Time Periods (scenario.toml)

Currently configured for AM period only. To test multiple periods, edit:
```toml
[[emme.time_period]]
name = "AM"
emme_scenario_id = 1

# Uncomment to add more periods
# [[emme.time_period]]
# name = "MD"
# emme_scenario_id = 2
```

## Next Steps

Once basic test works:
1. **Try different counties**: Change `--county` parameter
2. **Test filtering off**: Set `filter_demand = false` in config
3. **Enable MAZ components**: Set `include_maz_components=True` in controller
4. **Enable network summary**: Set `include_network_summary=True`
5. **Test multiple time periods**: Add more periods in scenario.toml

## Need Help?

Check these files for more details:
- [COUNTY_TEST_SETUP_CHECKLIST.md](COUNTY_TEST_SETUP_CHECKLIST.md) - Detailed setup checklist
- [HIGHWAY_ASSIGN_SKIM_README.md](HIGHWAY_ASSIGN_SKIM_README.md) - Full framework documentation
- [examples_highway_assign_skim.py](examples_highway_assign_skim.py) - Example code
