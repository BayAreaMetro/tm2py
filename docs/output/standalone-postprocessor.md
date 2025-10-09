# TM2PY Standalone PostProcessor Guide

This guide explains how to run only the TM2PY PostProcessor component to generate network shapefiles and transit data exports from an existing model run. These are files needed to run the acceptance criteria.

## Overview

The PostProcessor component exports EMME network data to various formats including:
- Network shapefiles (highway links/nodes, transit lines/segments)
- Transit boarding data (CSV and GeoJSON formats)

## Prerequisites

### Required Software
- **TM2PY environment** with EMME API access
- **Elevated PowerShell** (Run as Administrator)
- **Existing EMME model run** with completed assignments

### Required Files
- **EMME project** with populated scenarios
- **TM2PY configuration files**:
  - `scenario_config.toml`
  - `model_config.toml`

## Configuration Setup

### 1. Configure scenario_config.toml

Set the configuration to run only the PostProcessor:

```toml
[run]
    start_component = ""
    initial_components = [
        # Comment out all initial components
        #"create_tod_scenarios", 
        #"active_modes",
        #"prepare_network_highway",
        # ... etc
    ]
    global_iteration_components = [
        # Comment out all global iteration components
        #"household",
        #"highway",
        # ... etc
    ]
    final_components = ["post_processor"]
    start_iteration = 0
    end_iteration = 0  # Use 0 for fastest execution
```

### 2. Verify model_config.toml PostProcessor Settings

Check the output paths in your `model_config.toml`:

```toml
[post_processor]
    network_shapefile_path = "output_summaries/Scenario_{period}"
    boardings_by_segment_file_path = "output_summaries/boardings_by_segment_{period}.csv"
    boardings_by_segment_geofile_path = "output_summaries/boardings_by_segment_{period}.geojson"
```

## Execution Steps

### 1. Open OpenPaths EMME Shell

**Critical**: You must use the OpenPaths EMME Shell (not regular PowerShell or Command Prompt) to ensure proper EMME API access.

1. Open **OpenPaths EMME Shell** from your Start menu
2. The shell should display something like:
   ```
   OpenPaths EMME Environment is set to:
   OpenPaths EMME 24.01.00 64-bit, Copyright 2024 Bentley Systems, Incorporated
   
   Python Path is set to:
   C:\Program Files\Bentley\OpenPaths\EMME 24.01.00\Python311\
   ```

### 2. Activate TM2PY Virtual Environment

Activate your tm2py virtual environment in the EMME shell:

```batch
C:\GitHub\tm2pyenv\Scripts\activate
```

You should see the environment name in parentheses:
```batch
(tm2pyenv) C:\Users\username>
```

### 3. Navigate to Model Directory

```batch
cd E:\your-model-directory
```

Example:
```batch
(tm2pyenv) C:\Users\username>cd E:\2015-tm22-dev-sprint-04
(tm2pyenv) E:\2015-tm22-dev-sprint-04>
```

### 4. Clear Any Locked Log Files (if needed)

If you encounter permission errors, delete the locked runtime log:

```batch
del logs\runtime_log.txt
```

### 5. Run TM2PY

```batch
python runModel.py
```

**Note**: Use `python` directly (not the full path) since the EMME shell environment handles the Python path correctly.

## Expected Runtime

**Total Duration**: Approximately **1.5-2 hours** for a full Bay Area model

**Breakdown by Component**:
- **Network Shapefiles**: ~25-30 minutes per time period (5 periods total)
- **Transit CSV Export**: ~15 minutes 
- **Transit GeoJSON Export**: ~5 minutes

**Time Period Processing Order**:
1. EA (Early AM) - Scenario 11
2. AM (AM Peak) - Scenario 12  
3. MD (Midday) - Scenario 13
4. PM (PM Peak) - Scenario 14
5. EV (Evening) - Scenario 15

## Output Files

### Network Shapefiles

**Location**: `output_summaries/Scenario_{period}/`

**Files per time period**:
- `emme_links.*` - Highway network links with assignment results
- `emme_nodes.*` - Highway network nodes  
- `emme_tlines.*` - Transit lines
- `emme_tsegs.*` - Transit segments with boarding data

**File Sizes** (typical):
- Links DBF: ~2.8 GB (contains detailed link attributes)
- Nodes DBF: ~444 MB
- Transit segments DBF: ~352 MB

### Transit Boarding Data

**CSV File**: `output_summaries/boardings_by_segment_{period}.csv`
- **Size**: ~28 MB for AM period
- **Content**: Line-by-line segment data with volumes, capacity, dwell times

**GeoJSON File**: `output_summaries/boardings_by_segment_{period}.geojson`  
- **Size**: ~149 MB for AM period
- **Content**: Geographic transit segments with boarding volumes
- **CRS**: EPSG:2875 (California State Plane Zone 3)

### Output Directory Structure

```
output_summaries/
├── Scenario_11/          # EA period shapefiles
├── Scenario_12/          # AM period shapefiles  
├── Scenario_13/          # MD period shapefiles
├── Scenario_14/          # PM period shapefiles
├── Scenario_15/          # EV period shapefiles
├── boardings_by_segment_12.csv     # AM transit CSV
├── boardings_by_segment_am.geojson # AM transit GeoJSON
└── [other existing files...]
```

## Troubleshooting

### EMME Environment Issues

**Error**: `ModuleNotFoundError: No module named 'inro'` or EMME import errors

**Solution**: 
1. Ensure you're using the **OpenPaths EMME Shell** (not regular PowerShell)
2. Verify `emme.pth` file is in your virtual environment's `site-packages` folder
3. Check that your virtual environment was created from within the EMME shell

### Permission Errors

**Error**: `PermissionError: [Errno 13] Permission denied: 'logs\\runtime_log.txt'`

**Solution**: 
1. Delete locked log file from EMME shell: `del logs\runtime_log.txt`
2. If that fails, you may need to run EMME shell as Administrator
3. Retry execution

### ReadOnly Database Errors  

**Error**: `ReadOnlyError: attempt to write a readonly database`

**Solution**:
1. Close EMME Desktop application completely
2. Kill any Python processes: `taskkill /F /PID <pid>`
3. Delete SQLite WAL files:
   ```batch
   del emme_project\Logbook\project.mlbk-shm
   del emme_project\Logbook\project.mlbk-wal
   ```

### Missing EMME Scenarios

**Error**: Scenario not found errors

**Solution**: Verify that your EMME project contains populated scenarios 11-15 with completed network assignments.

## Performance Notes

- **Large file sizes**: Highway links DBF files can exceed 2GB
- **Memory usage**: Process requires significant RAM for large networks  
- **Disk I/O intensive**: Multiple GB of shapefile data written to disk
- **Network complexity**: Runtime scales with network size and detail level

## Integration with Analysis Workflows

The exported files can be used for:

- **GIS Analysis**: Import shapefiles into ArcGIS, QGIS
- **Web Mapping**: Use GeoJSON for web-based visualizations  
- **Performance Monitoring**: Compare transit boarding levels across scenarios
- **Network Validation**: Verify assignment results and network connectivity

## Configuration Variants

### Export Only Specific Time Periods

Modify the time periods in your configuration to export only desired periods:

```toml
[[time_periods]]
name = "AM"
emme_scenario_id = 12
# Remove other time period definitions
```

### Change Output Locations

Customize output paths in `model_config.toml`:

```toml
[post_processor]
    network_shapefile_path = "custom_output/networks/period_{period}"
    boardings_by_segment_file_path = "custom_output/transit_{period}.csv"
    boardings_by_segment_geofile_path = "custom_output/transit_{period}.geojson"
```