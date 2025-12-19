# County Highway Test - Quick Start Instructions

## Prerequisites

1. **EMME Environment**: You must run this from an EMME Python environment
2. **Source Data**: Verify dataset exists at `E:\2015_TM2_20250619`
3. **Disk Space**: Ensure ~10GB free space for test directory (includes EMME project copy)

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
3. ✓ Copy EMME project from source dataset
4. ✓ Copy required input files
5. ✓ Run highway network preparation and assignment
6. ✓ Validate results

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
- Check demand mat<your-output-dir>d are properly formatted
- Review logs in `test_san_mateo/logs/`
- Verify network has proper capacity and speed attributes

## Next Steps

Once basic test works:
1. Enable MAZ components: Set `include_maz_components=True`
2. Enable network summary: Set `include_network_summary=True`
3. Test with filtered data for your specific county
4. Run multiple time periods (update scenario.toml)

## Need Help?

Check these files for more details:
- [COUNTY_TEST_SETUP_CHECKLIST.md](COUNTY_TEST_SETUP_CHECKLIST.md) - Detailed setup checklist
- [HIGHWAY_ASSIGN_SKIM_README.md](HIGHWAY_ASSIGN_SKIM_README.md) - Full framework documentation
- [examples_highway_assign_skim.py](examples_highway_assign_skim.py) - Example code
