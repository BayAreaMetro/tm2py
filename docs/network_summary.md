# Network Summary Component

The NetworkSummary component provides comprehensive analysis and reporting of transportation network performance, including highway, transit, and landuse summaries.

## Overview

The NetworkSummary component generates detailed CSV reports analyzing:
- Highway network performance (speeds, volumes, congestion)
- Transit ridership and operator analysis
- Landuse totals (households, population, jobs)
- Network inventory and capacity analysis

The component can be run as part of the TM2PY model workflow or as a standalone analysis tool.

## Running the Network Summary

### As a TM2PY Component

#### Basic Component Usage

```python
import tm2py

# Initialize controller with your model configuration
controller = tm2py.RunController.from_config_file("config.toml")

# Run network summary component
controller.run_component("network_summary")
```

#### Programmatic Usage

```python
from tm2py.components.network_summary import NetworkSummary
from tm2py.config import Configuration

# Load configuration
config = Configuration.from_config_file("config.toml")

# Initialize and run component
summary = NetworkSummary(controller=controller)
summary.run()
```

### As a Standalone Script

#### Command Line Usage

The legacy `network_summary.py` script provides standalone functionality:

```bash
# Basic usage
python scripts\network_summary.py <model_run_directory>

# Example
python scripts\network_summary.py E:\2015-tm22-dev-sprint-04

# Advanced usage with options
python scripts\network_summary.py E:\2015-tm22-dev-sprint-04 --output C:\results --verbose
```

#### Command Line Arguments

| Argument | Type | Required | Description |
|----------|------|----------|-------------|
| `model_run_directory` | Positional | Yes | Path to TM2PY model run directory |
| `--output` | Optional | No | Custom output directory (default: `model_run_directory/network_summary`) |
| `--verbose` | Flag | No | Enable detailed logging output |
| `--validate-only` | Flag | No | Run input validation only without processing |
| `--validate-outputs` | Flag | No | Run output validation on existing results |

### Environment Setup

Both component and script usage require the tm2py Python environment with EMME API access:

1. **Activate tm2pyenv environment**:
   ```batch
   # Open OpenPaths EMME Shell first, then activate your tm2py virtual environment
   C:\GitHub\tm2pyenv\Scripts\activate
   ```

2. **Verify EMME modules are available**:
   ```python
   import inro.emme.database.emmebank
   import inro.emme.network
   ```

## Requirements and Inputs

### Required Model Outputs

The NetworkSummary component requires the following model outputs to be available:

#### EMME Databases
- **Highway Database**: Must contain modeled highway network with:
  - Link volumes (`auto_volume` attribute)
  - Link speeds (`auto_time` attribute) 
  - Link capacities (`capacity` attribute)
  - Free-flow travel times (`length` and `data2` attributes)
  - Truck volumes (`@flow_trk` attribute, optional)

- **Transit Database** (optional): Must contain modeled transit network with:
  - Transit lines with segments
  - Boardings data (`transit_boardings`, `boardings`, `@transit_boardings`, or `@boardings`)
  - Line attributes (mode, operator, capacity, headway)
  - Segment attributes (dwell times, transit time functions)

#### Landuse Data
- **MAZ File**: CSV file with zone-level landuse data containing:
  - `TOTHH` or `hh` - Total households
  - `TOTPOP` or `pop` - Total population  
  - `TOTEMP` or `emp` - Total employment/jobs

### Configuration Requirements

The component requires the following configuration sections:

```toml
[scenario]
# Path to landuse MAZ file
landuse_file = "input/maz_data.csv"

[emme]
# EMME project and scenario settings
project_path = "path/to/emme/project"
highway_database_path = "path/to/highway.emmebank"
transit_database_path = "path/to/transit.emmebank"

[time_periods]
# Time period definitions for analysis
am = "6:00-10:00"
pm = "15:00-19:00" 
# ... additional periods

[network_summary]
# Optional: Override default output settings
output_directory = "output_summaries"  # Default
output_filename = "topsheet.csv"       # Default
```

### System Requirements

- **EMME**: Compatible EMME installation with Python API
- **Python**: Python 3.8+ with required dependencies:
  - pandas
  - numpy
  - pathlib
  - logging

## Processing Workflow

The Network Summary component executes in 6 comprehensive phases:

### Phase 1: Input Validation
- ✅ Model run directory exists
- ✅ Highway and transit database path validation
- ✅ EMME environment verification
- ✅ Database file accessibility check

### Phase 2: Database Connection  
- 🔗 Connect to EMME highway and transit databases
- 📊 Enumerate available scenarios in both databases
- 🕐 Map scenarios to time periods
- 📈 Validate scenario structure

### Phase 3: Highway Data Extraction
- 📝 Extract link data for all time periods
- 🔍 Process highway attributes per link
- 📊 Validate data completeness
- 💾 Generate extraction statistics

### Phase 4: Highway Performance Calculations
- 🧮 Calculate VMT, VHT, delay metrics
- 🏗️ Aggregate by facility type
- 🌍 Summarize by county
- 📐 Compute lane mile inventories

### Phase 5: Transit Data Extraction & Analysis
- 🚌 Extract transit line and segment data
- 🚊 Process boarding volumes and service attributes
- 📈 Calculate ridership by mode and time period
- 🔄 Generate service performance metrics

### Phase 6: Output Generation & Validation
- 📂 Create CSV summary files  
- 📋 Generate processing reports
- 📝 Save detailed logs
- ✅ **Validate output quality and data ranges**

## Output Files and Fields

The NetworkSummary component generates the following output files in the configured output directory:

### Core Summary File

#### `topsheet.csv`
Main summary file with key regional metrics:

| Field | Description |
|-------|-------------|
| `Metric` | Name of the performance metric |
| `Value` | Numeric value of the metric |
| `Category` | Category (Highway, Transit, Landuse, etc.) |

**Metrics included:**
- Highway: Total VMT, VHT, average speeds by facility type
- Transit: Daily boardings by time period and operator
- Landuse: Total households, population, jobs
- Network: Lane miles by facility type

### Highway Analysis Files

#### `highway_performance_{period}.csv`
Highway performance by time period and facility type:

| Field | Description |
|-------|-------------|
| `time_period` | Time period (am, pm, etc.) |
| `facility_type` | Highway facility type |
| `total_volume` | Total vehicle volume |
| `lane_miles` | Lane miles for facility type |
| `vmt` | Vehicle miles traveled |
| `vht` | Vehicle hours traveled |
| `avg_speed` | Average speed (mph) |
| `avg_density` | Average density (veh/mile/lane) |
| `congested_speed` | Speed during congested conditions |
| `speed_ratio` | Ratio of actual to free-flow speed |

#### `highway_performance_daily.csv`
All-day highway performance totals:

| Field | Description |
|-------|-------------|
| `facility_type` | Highway facility type |
| `daily_volume` | Total daily volume |
| `lane_miles` | Lane miles |
| `daily_vmt` | Daily vehicle miles traveled |
| `daily_vht` | Daily vehicle hours traveled |
| `avg_speed` | Average daily speed |
| `periods_served` | Number of time periods with data |

#### `lane_mile_inventory.csv`
Network infrastructure inventory:

| Field | Description |
|-------|-------------|
| `facility_type` | Highway facility type |
| `lane_miles` | Total lane miles |
| `centerline_miles` | Centerline miles |
| `avg_lanes` | Average lanes per link |
| `total_capacity` | Total hourly capacity |

### Transit Analysis Files

#### `transit_boardings_by_line_{period}.csv`
Transit boardings by line and time period:

| Field | Description |
|-------|-------------|
| `time_period` | Time period |
| `line_id` | Transit line identifier |
| `mode_id` | Transit mode |
| `boardings` | Total boardings on line |
| `total_capacity` | Line capacity |
| `seated_capacity` | Seated capacity |
| `headway` | Service headway (minutes) |
| `link_length` | Total line length |
| `line_hour_total_cap` | Hourly total capacity |
| `line_hour_seated_cap` | Hourly seated capacity |
| `load_factor` | Boardings/total capacity ratio |
| `seated_load_factor` | Boardings/seated capacity ratio |
| `frequency` | Vehicles per hour |

#### `transit_boardings_by_line_daily.csv`
All-day transit totals by line:

| Field | Description |
|-------|-------------|
| `line_id` | Transit line identifier |
| `mode_id` | Transit mode |
| `boardings` | Total daily boardings |
| `total_capacity` | Line capacity |
| `seated_capacity` | Seated capacity |
| `headway` | Average headway |
| `link_length` | Total line length |
| `periods_served` | Number of periods served |
| `avg_hourly_boardings` | Average hourly boardings |
| `avg_frequency` | Average frequency |

#### `transit_boardings_by_operator.csv`
Transit boardings by operator and mode:

| Field | Description |
|-------|-------------|
| `operator` | Transit operator name |
| `mode_type` | Transit mode type |
| `modelled_boardings` | Total modeled boardings |
| `line_count` | Number of lines operated |
| `operator_group` | Operator category |

**Operators detected:**
- BART, SF Muni, AC Transit, VTA, Caltrain
- Golden Gate Transit, SamTrans
- Smaller Operators (for unrecognized operators)

#### `transit_boardings_by_service_type.csv`
Transit summary by service mode:

| Field | Description |
|-------|-------------|
| `mode_id` | Transit mode identifier |
| `mode_type` | Mode type description |
| `total_boardings` | Total boardings for mode |
| `line_count` | Number of lines |
| `avg_boardings_per_line` | Average boardings per line |

## Configuration Options

### NetworkSummaryConfig Section

```toml
[network_summary]
# Output directory for summary files (relative to project root)
output_directory = "output_summaries"

# Main summary filename
output_filename = "topsheet.csv"
```

### Default Values
- `output_directory`: "output_summaries"
- `output_filename`: "topsheet.csv"

## Error Handling and Validation

The component includes comprehensive validation:

### Data Quality Checks
- **Highway**: Validates speed ranges, volume reasonableness, capacity consistency
- **Transit**: Validates boarding data availability, operator detection
- **Landuse**: Validates required columns and positive values

### Missing Data Handling
- **Graceful degradation**: Component continues if optional data missing
- **Clear logging**: Detailed progress and error messages
- **Fallback values**: Uses reasonable defaults for missing attributes

### Common Issues and Solutions

#### "No highway database found"
- Verify EMME project path in configuration
- Ensure highway database exists and is accessible
- Check EMME API connectivity

#### "No transit data available"
- Transit analysis is optional - highway analysis will continue
- Verify transit database path if transit analysis desired
- Check that transit assignment has been run

#### "Missing boardings column"
- Verify transit assignment results include boarding data
- Component looks for: `transit_boardings`, `boardings`, `@transit_boardings`, `@boardings`
- Falls back to `transit_volume` if no boarding attributes found

#### "Landuse file not found"
- Verify `landuse_file` path in configuration
- Ensure MAZ file exists and contains required columns
- Check file format (CSV) and column naming

## Performance Considerations

### Runtime
- **Small networks** (< 5,000 links): 1-2 minutes
- **Large networks** (> 20,000 links): 5-10 minutes
- **Transit processing**: Adds 2-5 minutes depending on line count

### Memory Usage
- **Highway analysis**: ~100MB for large networks
- **Transit analysis**: ~200MB for complex transit networks
- **Peak usage**: During data aggregation and file writing

### Optimization Tips
- Run during off-peak hours for large networks
- Ensure sufficient disk space for output files
- Consider running highway and transit analysis separately if memory constrained

## Understanding the Outputs

### Performance Metrics

**VMT (Vehicle Miles Traveled)**:
- Formula: `Volume × Link Length`
- Units: Vehicle-miles
- Interpretation: Total distance traveled by all vehicles

**VHT (Vehicle Hours Traveled)**:
- Formula: `Volume × Travel Time ÷ 60`
- Units: Vehicle-hours  
- Interpretation: Total time spent traveling by all vehicles

**Delay**:
- Formula: `Volume × (Congested Time - Free Flow Time) ÷ 60`
- Units: Vehicle-hours
- Interpretation: Additional time due to congestion

**Speed**:
- Formula: `VMT ÷ VHT`
- Units: Miles per hour
- Interpretation: Network average speed

### Facility Type Categories

| Facility Type | VMT Contribution | Typical Speed Range | Usage Pattern |
|---------------|------------------|---------------------|---------------|
| **Freeway** | 40-50% of total | 45-70 mph | High-volume, long-distance |
| **Arterial** | 30-40% of total | 25-45 mph | Medium-volume, medium-distance |
| **Collector** | 10-20% of total | 20-35 mph | Local distribution |
| **Local** | 5-10% of total | 15-25 mph | Neighborhood access |

### Time Period Analysis

| Time Period | Peak Hours | Typical Delay | Analysis Focus |
|-------------|------------|---------------|----------------|
| **EA** (Early AM) | 3 AM - 6 AM | Minimal | Free-flow baseline |
| **AM** (AM Peak) | 6 AM - 10 AM | High | Morning commute |
| **MD** (Midday) | 10 AM - 3 PM | Low-Medium | Off-peak operations |
| **PM** (PM Peak) | 3 PM - 7 PM | High | Evening commute |
| **EV** (Evening) | 7 PM - 3 AM | Low | Off-peak/recreational |

### Transit Analysis

#### Transit Performance Metrics

**Boardings**: Number of passengers boarding transit vehicles at each segment
- Used to measure ridership demand and route popularity
- Available by line, segment, and time period

**Load Factor**: Ratio of boardings to vehicle capacity
- Indicates capacity utilization and crowding levels
- Calculated for both total and seated capacity

**Service Frequency**: Vehicles per hour (60 / headway)
- Measures service intensity and convenience
- Higher frequency typically indicates better service

**Route Productivity**: Boardings per route mile
- Efficiency metric for service planning
- Helps identify high-performing vs. underutilized routes

## Integration with Other Components

The NetworkSummary component can be integrated with other TM2PY components:

### Data Dependencies
- **Requires**: Completed highway assignment
- **Optional**: Completed transit assignment
- **Landuse**: Static input data

### Output Usage
- Summary files can be consumed by reporting tools
- CSV format enables easy integration with dashboards
- Topsheet provides key metrics for model validation

### Workflow Integration
```python
# Example full model run with network summary
controller.run_component("highway_assignment")
controller.run_component("transit_assignment")  # Optional
controller.run_component("network_summary")
```

## Troubleshooting

### Debug Logging
Enable verbose logging for detailed component diagnostics:

```python
import logging
logging.getLogger('tm2py.components.network_summary').setLevel(logging.DEBUG)
```

### Common Log Messages
- `"HIGHWAY SUCCESS: Completed analysis"` - Highway processing completed
- `"TRANSIT SUCCESS: Completed analysis"` - Transit processing completed  
- `"NetworkSummary completed successfully"` - Full component success
- `"WARNING: No transit database found"` - Transit analysis skipped

### Support Resources
- GitHub Issues: Report bugs and feature requests
- Documentation: Additional examples and use cases
- Community: User forums and discussions

## Output Validation

The component includes comprehensive **output validation** to ensure data quality and catch potential issues:

### Validation Checks

| Check Type | Purpose | Action on Failure |
|------------|---------|-------------------|
| **File Validation** | Verify all expected output files exist and are readable | Error - process fails |
| **Speed Range Validation** | Check network speeds are within realistic ranges (5-80 mph) | Warning - flag unusual speeds |
| **VMT/VHT Ratio Validation** | Ensure VMT/VHT ratios are consistent and realistic | Warning/Error - flag impossible conditions |
| **Data Completeness** | Check for missing values and empty datasets | Warning/Error - based on severity |
| **Value Range Validation** | Verify key metrics are within expected ranges for Bay Area | Warning - flag unusual values |

### Expected Ranges (Bay Area Network)

| Metric | Typical Range | Validation Action |
|--------|---------------|-------------------|
| **Network Speed** | 15-60 mph | Warn if outside range |
| **Facility Speed** | Freeway: 25-80 mph, Arterial: 15-55 mph | Warn if outside range |
| **Total VMT** | 50M - 200M | Warn if outside range |
| **Total VHT** | 2M - 10M | Warn if outside range |
| **Data Completeness** | >95% complete | Warn if <95%, Error if <80% |

## Troubleshooting

### Common Issues

#### ❌ "Database not found" Error
**Problem**: Cannot locate EMME database
```
FileNotFoundError: Highway database not found at: <path>
```

**Solutions**:
1. Verify model run directory path is correct
2. Check that both `emme_project/Database_highway/emmebank` and `emme_project/Database_transit/emmebank` exist
3. Ensure the model run completed successfully (both highway and transit assignment)
4. Ensure EMME database files are not corrupted
5. Verify file permissions

#### ❌ "EMME modules not available" Error  
**Problem**: Python environment lacks EMME API
```
ModuleNotFoundError: No module named 'inro.emme'
```

**Solutions**:
1. **Open OpenPaths EMME Shell** and activate tm2pyenv: `C:\GitHub\tm2pyenv\Scripts\activate`
2. Verify EMME installation on system
3. Check EMME license availability
4. Contact system administrator for EMME access

#### ❌ "Insufficient scenarios" Error
**Problem**: Missing time period scenarios
```
ValidationError: Expected 6 scenarios, found X
```

**Solutions**:
1. Verify model run completed successfully
2. Check all time periods (EA, AM, MD, PM, EV, EA2) are present
3. Re-run model if scenarios are missing
4. Verify scenario naming conventions

#### ❌ "Memory error" or "Performance issues"
**Problem**: Large network processing
```
MemoryError: Unable to allocate arrays
```

**Solutions**:
1. Close other applications to free memory
2. Use 64-bit Python environment
3. Process smaller geographic areas if possible
4. Increase virtual memory if needed

### Data Quality Issues

#### ⚠️ Unrealistic Speed Values
**Symptoms**: Speeds > 80 mph or < 5 mph
**Investigation**:
1. Check validation results for quality flags
2. Review link-level data for outliers
3. Verify network coding accuracy
4. Check time period scenario assignments

#### ⚠️ Missing Attribute Values
**Symptoms**: High percentages of missing data
**Investigation**:
1. Review EMME database integrity
2. Check scenario assignment results
3. Verify network build process
4. Examine attribute calculation procedures

#### ⚠️ Inconsistent VMT/VHT Ratios
**Symptoms**: Very high or low VHT relative to VMT
**Investigation**:
1. Review congested vs. free-flow travel times
2. Check link capacity assignments
3. Verify volume loading accuracy
4. Examine delay calculation methodology

### Getting Help

#### Log File Analysis
1. **Check the log file**: Contains detailed processing information
2. **Search for ERROR/WARNING**: Use text search to find issues
3. **Review validation results**: Check data quality summaries
4. **Examine processing statistics**: Verify completion rates

#### Contacting Support
When reporting issues, please provide:
- Complete error message
- Model run directory path  
- Log file contents
- Environment details (TM2PY environment setup and Python version)
- Processing summary output