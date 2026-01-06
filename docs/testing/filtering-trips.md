# Filtering CTRAMP Output Trip Files for County Tests

This guide explains how to filter highway trip files from a full model CTRAMP output directory to create county-specific test data.

## Overview

When you have a completed model run, the CTRAMP output directory contains trip matrices in OMX format. To test highway assignment for a specific county, you need to filter these matrices to only include origin-destination pairs where both the origin and destination TAZs are within the county.

## Quick Start

### Method 1: Using Setup Script (Easiest)

```bash
python tests/setup_highway_assign_skim.py --interactive
```

When prompted:
1. Choose option 1: "Filter trip files from CTRAMP output"
2. Provide path to CTRAMP output directory (e.g., `E:\2023-tm22-dev-version-05\ctramp_output`)
3. The script will auto-detect zones and filter all trip files

### Method 2: Command Line

```bash
python tests/setup_highway_assign_skim.py \
    --county "San Mateo" \
    --ctramp-output "E:\2023-tm22-dev-version-05\ctramp_output" \
    --test-dir test_san_mateo
```

### Method 3: Programmatically

```python
from pathlib import Path
from test_highway_assign_skim import (
    get_county_zones, 
    CountyDataFilter, 
    filter_ctramp_highway_trips
)

# Auto-detect zones for your county
county_name = "San Mateo"
zones = get_county_zones(county_name)

# Create filter
filter_helper = CountyDataFilter(
    county_name=county_name,
    taz_range=zones['taz_range'],
    maz_range=zones['maz_range']
)

# Filter trip files
filter_ctramp_highway_trips(
    ctramp_output_dir=Path(r"E:\2023-tm22-dev-version-05\ctramp_output"),
    test_demand_dir=Path("test_san_mateo/inputs/demand"),
    filter_helper=filter_helper,
    time_periods=['EA', 'AM', 'MD', 'PM', 'EV']  # Optional
)
```

## What Gets Filtered

The filtering process:

1. **Finds all OMX files** in the CTRAMP output directory
2. **Reads the zone mapping** from each file
3. **Identifies county zones** based on TAZ range (auto-detected from crosswalk)
4. **Extracts submatrices** containing only O-D pairs within the county
5. **Writes filtered OMX files** with the same names to the test demand directory

### Example

If San Mateo County has TAZs 200-400:

**Original OMX file (1454x1454 zones):**
- Contains all regional O-D pairs
- Total trips: 5,000,000

**Filtered OMX file (201x201 zones):**
- Contains only San Mateo → San Mateo trips
- Trips where origin OR destination is outside county are excluded
- Total trips: 250,000 (trips within county only)

## File Naming

Filtered files keep their original names:
- `autoTrips_EA.omx` → `autoTrips_EA.omx`
- `autoTrips_AM.omx` → `autoTrips_AM.omx`
- etc.

They are written to: `{test_dir}/inputs/demand/`

## Filtering Specific Time Periods

To filter only certain time periods (faster):

```python
filter_ctramp_highway_trips(
    ctramp_output_dir=ctramp_dir,
    test_demand_dir=test_demand_dir,
    filter_helper=filter_helper,
    time_periods=['AM', 'PM']  # Only morning and evening peaks
)
```

## Typical CTRAMP Output Structure

```
E:\2023-tm22-dev-version-05\ctramp_output\
├── autoTrips_EA.omx
├── autoTrips_AM.omx
├── autoTrips_MD.omx
├── autoTrips_PM.omx
├── autoTrips_EV.omx
├── tranTrips_EA.omx
├── tranTrips_AM.omx
└── ... (other trip types)
```

The filter will process all `.omx` files found.

## After Filtering

1. **Verify the filtered files:**
   ```bash
   ls test_san_mateo/inputs/demand/
   ```

2. **Update your config** to point to these files:
   ```toml
   # In scenario config
   [highway.demand]
   EA = "inputs/demand/autoTrips_EA.omx"
   AM = "inputs/demand/autoTrips_AM.omx"
   # etc.
   ```

3. **Run the test:**
   ```bash
   python tests/highway_assign_skim_controller.py \
       --county "San Mateo" \
       --scenario test_san_mateo/config/scenario.toml \
       --model-config test_san_mateo/config/model.toml
   ```

## Troubleshooting

### "No zones found in range X-Y"

The OMX file doesn't contain zones in your county range. Check:
- Is the TAZ range correct for your county?
- Does the OMX file use the same zone numbering system?

### "No OMX files found"

Check the CTRAMP output path. It should contain `.omx` files.

### Memory Issues

If filtering very large matrices, the process may use significant memory. Filter one time period at a time if needed.

## See Also

- [HIGHWAY_ASSIGN_SKIM_README.md](HIGHWAY_ASSIGN_SKIM_README.md) - Full framework documentation
- [examples_highway_assign_skim.py](examples_highway_assign_skim.py) - Example 8 shows CTRAMP filtering
- [test_highway_assign_skim.py](test_highway_assign_skim.py) - Source code for filtering functions
