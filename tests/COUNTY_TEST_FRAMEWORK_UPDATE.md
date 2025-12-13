# County Test Framework - Now with Automatic Zone Detection!

The test framework has been updated to support **any county** with **automatic zone detection** from the crosswalk file.

## Key Changes

### 1. Automatic Zone Detection from Crosswalk

Instead of manual zone range specification, zones are automatically detected from:
`C:\GitHub\tm2py-utils\tm2py_utils\inputs\maz_taz\mazs_tazs_county_tract_PUMA_2.5.csv`

**Simply provide the county name:**

```python
from tests.test_highway_assign_skim import get_county_zones

# Auto-detect zones for any county
zones = get_county_zones("San Mateo")
# Returns: {'taz_min': X, 'taz_max': Y, 'maz_min': A, 'maz_max': B}

zones_alameda = get_county_zones("Alameda")
zones_scl = get_county_zones("Santa Clara")
```

### 2. Renamed Classes and Files

**Classes:**
- `SanMateoDataFilter` → `CountyDataFilter`
- `SanMateoHighwayController` → `CountyHighwayController`

**Files:**
- `test_san_mateo_highway.py` → `test_highway_assign_skim.py`
- `san_mateo_controller.py` → `highway_assign_skim_controller.py`
- `examples_san_mateo.py` → `examples_highway_assign_skim.py`
- `setup_san_mateo_test.py` → `setup_highway_assign_skim.py`

All now accept a `county_name` parameter and use auto-detection.

### 3. Usage with Auto-Detection

All main classes now support automatic zone detection:

```python
from tests.test_highway_assign_skim import get_county_zones, CountyDataFilter
from tests.highway_assign_skim_controller import CountyHighwayController

# Auto-detect and filter data
county = "San Mateo"
zones = get_county_zones(county)
filter = CountyDataFilter(
    county_name=county,
    taz_range=(zones['taz_min'], zones['taz_max']),
    maz_range=(zones['maz_min'], zones['maz_max'])
)

# Controller with auto-detection
controller = CountyHighwayController(
    scenario_config="config/scenario.toml",
    model_config="config/model.toml",
    run_dir="test_output",
    county_name=county  # Auto-detects zones internally
)
```

### 4. Setup Script with Auto-Detection

The setup script automatically detects zones:

```bash
# Auto-detect zones for any county
python tests/setup_highway_assign_skim.py --county "San Mateo"
python tests/setup_highway_assign_skim.py --county "Alameda"
python tests/setup_highway_assign_skim.py --county "Santa Clara"

# Interactive mode
python tests/setup_highway_assign_skim.py --interactive
```

### 5. CLI with Auto-Detection

The controller script uses crosswalk for auto-detection:

```bash
python tests/highway_assign_skim_controller.py \
    --county "San Mateo" \
    --scenario config/scenario.toml \
    --model-config config/model.toml

python tests/highway_assign_skim_controller.py \
    --county "Alameda" \
    --scenario config/scenario.toml \
    --model-config config/model.toml
```

## How to Use for Different Counties

### Example: Testing Alameda County

1. **Auto-detect zones**:
```python
from tests.test_highway_assign_skim import get_county_zones

zones = get_county_zones("Alameda")
print(f"TAZ: {zones['taz_min']}-{zones['taz_max']}")
print(f"MAZ: {zones['maz_min']}-{zones['maz_max']}")
```

2. **Generate config files**:
```bash
python tests/setup_highway_assign_skim.py --county "Alameda"
```

This creates: `test_alameda/config/scenario.toml` and `model.toml`

3. **Filter data** (programmatically):
```python
from tests.test_highway_assign_skim import CountyDataFilter, setup_county_test_data, get_county_zones

county = "Alameda"
zones = get_county_zones(county)

setup_county_test_data(
    source_dir=Path("full_model/inputs"),
    test_dir=Path("test_alameda"),
    county_name=county,
    taz_range=(zones['taz_min'], zones['taz_max']),
    maz_range=(zones['maz_min'], zones['maz_max'])
)
```
    filter_helper=filter
)
```

4. **Run the test**:
```bash
python tests/highway_assign_skim_controller.py \
    --county "Alameda" \
    --scenario test_alameda/config/scenario.toml \
    --model-config test_alameda/config/model.toml
```

### Example: Testing Multiple Counties with Auto-Detection

```python
from tests.test_highway_assign_skim import get_county_zones, CountyDataFilter
from tests.highway_assign_skim_controller import CountyHighwayController

counties = ["San Mateo", "Alameda", "Santa Clara"]

for county_name in counties:
    # Auto-detect zones
    zones = get_county_zones(county_name)
    
    # Create filter
    filter = CountyDataFilter(
        county_name=county_name,
        taz_range=(zones['taz_min'], zones['taz_max']),
        maz_range=(zones['maz_min'], zones['maz_max'])
    )
    
    # Run controller
    test_dir = f"test_{county_name.lower().replace(' ', '_')}"
    controller = CountyHighwayController(
        scenario_config=f"{test_dir}/config/scenario.toml",
        model_config=f"{test_dir}/config/model.toml",
        run_dir=test_dir,
        county_name=county_name
    )
    
    controller.run_highway_only()
```

## Backward Compatibility

The framework still works for San Mateo County - just specify "San Mateo" as the county name:

```python
controller = CountyHighwayController(
    scenario_config="config/scenario.toml",
    model_config="config/model.toml",
    county_name="San Mateo"
)
```

## What Changed in Each File

| Old File | New File | Key Changes |
|----------|----------|-------------|
| `test_san_mateo_highway.py` | `test_highway_assign_skim.py` | - Added `get_county_zones()` for auto-detection<br>- `SanMateoDataFilter` → `CountyDataFilter` with `county_name` param<br>- Uses crosswalk file for zone detection |
| `san_mateo_controller.py` | `highway_assign_skim_controller.py` | - `SanMateoHighwayController` → `CountyHighwayController`<br>- Added `county_name` parameter<br>- Added `--county` CLI flag<br>- Log messages use county name |
| `examples_san_mateo.py` | `examples_highway_assign_skim.py` | - Updated all examples to use new class names<br>- Added auto-detection examples<br>- Shows multi-county usage |
| `setup_san_mateo_test.py` | `setup_highway_assign_skim.py` | - Integrated crosswalk auto-detection<br>- Accepts `--county` parameter<br>- Interactive mode prompts for county |
| Config templates | (unchanged names) | - Generic headers (no hardcoded county)<br>- Work with any county |

## Crosswalk File

The framework uses this crosswalk file for auto-detection:
`C:\GitHub\tm2py-utils\tm2py_utils\inputs\maz_taz\mazs_tazs_county_tract_PUMA_2.5.csv`

**Required columns:**
- `MAZ_SEQ` - MAZ sequence number
- `TAZ_SEQ` - TAZ sequence number
- `county_name` - County name (e.g., "San Mateo", "Alameda")

The `get_county_zones()` function reads this file and returns the min/max TAZ and MAZ for any county.
