# County Test Setup Checklist

## Before Running County Tests

### ✅ Configuration Updates Complete
- [x] Updated dataset path to `E:\2015_TM2_20250619` in [examples_highway_assign_skim.py](examples_highway_assign_skim.py)
- [x] Updated EMME paths to relative paths in [config_templates/san_mateo_scenario.toml](config_templates/san_mateo_scenario.toml)
- [x] Changed to single AM time period in [config_templates/san_mateo_scenario.toml](config_templates/san_mateo_scenario.toml)
- [x] Updated skim_period to "AM" in [config_templates/san_mateo_model.toml](config_templates/san_mateo_model.toml)

### 📋 Still Need To Configure

#### 1. **Crosswalk File** (for zone detection)
   - Current: Using newer crosswalk at `C:\GitHub\tm2py-utils\tm2py_utils\inputs\maz_taz\mazs_tazs_county_tract_PUMA_2.5.csv`
   - **TODO**: May need to find old crosswalk file from 2015 dataset
   - See note in [test_highway_assign_skim.py](test_highway_assign_skim.py#L33)

#### 2. **Zone Ranges** (TAZ and MAZ)
   - Once crosswalk is correct, run:
     ```python
     from tests.test_highway_assign_skim import get_county_zones
     zones = get_county_zones("San Mateo")
     print(f"TAZ: {zones['taz_min']}-{zones['taz_max']}")
     print(f"MAZ: {zones['maz_min']}-{zones['maz_max']}")
     ```
   - Update `DEFAULT_TAZ_RANGE` and `DEFAULT_MAZ_RANGE` in [test_highway_assign_skim.py](test_highway_assign_skim.py#L38-L39)

#### 3. **Source Data Files**
   Verify these exist in `E:\2015_TM2_20250619`:
   - [x] `inputs/landuse/maz_data.csv` ✓ Confirmed
   - [x] `inputs/hwy/tolls.csv` ✓ Confirmed
   - [x] `demand_matrices/highway/household/TAZ_Demand_AM.omx` ✓ Confirmed
   - [ ] Highway network is in EMME project (will be used from there)
   
   **Note**: Highway network files (`.net`) are stored in the EMME project at `E:\2015_TM2_20250619\emme_project\`, not in `inputs/hwy/`

#### 4. **EMME Project with Base Network** ✅ ASSUMPTION
   **Assuming the EMME project is already set up** with an all-day base network scenario.
   
   For your test, you should have:
   - [ ] EMME project copied from `E:\2015_TM2_20250619\emme_project\` to test directory
   - [ ] Contains `Database_highway/emmebank` with loaded network
   - [ ] All-day reference scenario exists (check scenario ID in the database)
   - [ ] Verify `emme.all_day_scenario_id` in config matches the scenario ID in the database
   
   **Process starts here:** We're beginning with `create_tod_scenarios` which reads the existing base network and creates time-of-day specific scenarios.

### 🚀 Running the Test

Once setup is complete, run:

```python
# Option 1: Run example script
python tests/examples_highway_assign_skim.py

# Option 2: Use setup script to create filtered test data
python tests/setup_highway_assign_skim.py --county "San Mateo" --test-dir test_san_mateo

# Option 3: Run controller directly
from tests.highway_assign_skim_controller import CountyHighwayController

controller = CountyHighwayController(
    scenario_config="test_san_mateo/config/county_scenario.toml",
    model_config="test_san_mateo/config/county_model.toml",
    run_dir="test_san_mateo",
    county_name="San Mateo"
)
controller.run_highway_only()
```

### 📝 Notes

- **Crosswalk Issue**: Using newer crosswalk may cause zone range mismatches with 2015 data
- **Single Time Period**: Only running AM period for faster testing
- **EMME Requirement**: Must be run from EMME Python environment
