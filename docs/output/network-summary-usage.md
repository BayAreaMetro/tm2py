````markdown
# Network Summary Script Usage Guide

!!! warning "Documentation Consolidated"
    This usage guide has been integrated into the comprehensive [Network Summary Component Documentation](../network_summary.md). 
    
    **Please use the new consolidated documentation which includes:**
    
    - Component and script usage examples
    - Complete configuration guide
    - Detailed output specifications
    - Troubleshooting and validation
    - Integration examples
    
    [→ Go to Network Summary Component Documentation](../network_summary.md)

---

*The content below is maintained for legacy reference but may be outdated. Please refer to the consolidated documentation above.*

---

# Legacy Network Summary Script Usage Guide

This guide provides comprehensive instructions for using the enhanced TM2PY network summary script to analyze both highway and transit network performance.

## Overview

The `network_summary.py` script generates comprehensive network performance summaries from TM2PY model results, including:

**Highway Analysis**: VMT, VHT, and delay calculations by facility type, time period, and geographic area
**Transit Analysis**: Boarding volumes, ridership patterns, and service performance by line, segment, and mode

**Location**: `scripts/network_summary.py`

## Prerequisites

### Environment Setup

The script requires the tm2py Python environment with EMME API access:

1. **Activate tm2pyenv environment**:
   ```batch
   # Open OpenPaths EMME Shell first, then activate your tm2py virtual environment
   C:\GitHub\tm2pyenv\Scripts\activate
   ```
   
   You should see the environment name in parentheses:
   ```batch
   (tm2pyenv) C:\Users\username>
   ```

2. **Verify EMME modules are available**:
   ```python
   import inro.emme.database.emmebank
   import inro.emme.network
   ```

3. **Required Python packages**:
   - pandas
   - numpy  
   - pathlib
   - EMME API (inro.emme modules)

### Model Run Requirements

Your TM2PY model run directory must contain:

- **Highway database**: `emme_project/Database_highway/emmebank`
- **Transit database**: `emme_project/Database_transit/emmebank` 
- **EMME scenarios**: 6 time periods (EA, AM, MD, PM, EV, EA2) in both databases
- **Complete network attributes**: All 85 highway attributes documented
- **Transit assignment results**: Transit line boarding data and service attributes

## Installation

No installation required - the script is included with TM2PY.

```bash
# Navigate to tm2py directory
cd c:\GitHub\tm2py

# Script location
scripts\network_summary.py
```

## Command Line Usage

### Basic Usage

```bash
python scripts\network_summary.py <model_run_directory>
```

**Example**:
```bash
python scripts\network_summary.py E:\2015-tm22-dev-sprint-04
```

### Advanced Usage

```bash
python scripts\network_summary.py <model_run_directory> [--output <output_dir>] [--verbose]
```

**Examples**:
```bash
# Custom output directory
python scripts\network_summary.py E:\2015-tm22-dev-sprint-04 --output C:\results

# Verbose logging
python scripts\network_summary.py E:\2015-tm22-dev-sprint-04 --verbose

# Verbose logging
python scripts\network_summary.py E:\2015-tm22-dev-sprint-04 --verbose

# Input validation only (for troubleshooting)
python scripts\network_summary.py E:\2015-tm22-dev-sprint-04 --validate-only

# Output validation only (check existing results)
python scripts\network_summary.py E:\2015-tm22-dev-sprint-04 --validate-outputs

# Both options
python scripts\network_summary.py E:\2015-tm22-dev-sprint-04 --output C:\results --verbose
```

### Command Line Arguments

| Argument | Type | Required | Description |
|----------|------|----------|-------------|
| `model_run_directory` | Positional | Yes | Path to TM2PY model run directory |
| `--output` | Optional | No | Custom output directory (default: `model_run_directory/network_summary`) |
| `--verbose` | Flag | No | Enable detailed logging output |
| `--validate-only` | Flag | No | Run input validation only without processing |
| `--validate-outputs` | Flag | No | Run output validation on existing results |

## Script Processing Phases

The script executes in 6 comprehensive phases with detailed validation:

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
- 🔍 Process 85 highway attributes per link
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

## Output Validation

The script includes comprehensive **output validation** to ensure data quality and catch potential issues:

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

### Running Validation

```bash
# Validation is automatic during normal processing
python network_summary.py E:\model-run

# Run validation only on existing results
python network_summary.py E:\model-run --validate-outputs

# Input validation only (before processing)
python network_summary.py E:\model-run --validate-only
```

## Output Files

The script generates multiple CSV files in the output directory:

### Highway Output Files

| File | Description | Contents |
|------|-------------|----------|
| `highway_summary_by_facility.csv` | Performance by facility type | VMT, VHT, delay by freeway/arterial/etc. |
| `highway_summary_by_county.csv` | Performance by county | County-level aggregated metrics |
| `highway_summary_by_time_period.csv` | Performance by time period | AM/PM/etc. performance comparison |
| `lane_mile_inventory.csv` | Infrastructure inventory | Lane miles by facility type and county |

### Transit Output Files

| File | Description | Contents |
|------|-------------|----------|
| `transit_boardings_by_line_{period}.csv` | Line boardings by time period | EA, AM, MD, PM, EV boarding volumes by line |
| `transit_boardings_by_segment_{period}.csv` | Segment boardings by time period | Detailed segment-level boarding data |
| `transit_boardings_by_line_daily.csv` | All-day totals by line | Daily boarding summaries and service metrics |
| `transit_boardings_by_service_type.csv` | Summary by mode | Ridership aggregated by bus, rail, BRT, etc. |

### Processing Reports

| File | Description | Contents |
|------|-------------|----------|
| `processing_summary.csv` | Processing statistics | Links and segments processed, completion rates, data quality |
| `attribute_coverage.csv` | Attribute availability | Coverage rates for all 85 highway attributes |
| `validation_results.csv` | Data validation results | Quality checks and validation outcomes |

### Log Files

| File | Description | Contents |
|------|-------------|----------|
| `network_summary.log` | Detailed processing log | Complete processing history with timestamps |
| `error_log.csv` | Error tracking | Any processing errors or warnings |

## Understanding the Output

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

#### Transit Output Structure

**Line-Level Analysis** (`transit_boardings_by_line_{period}.csv`):
- Total boardings per line per time period
- Service attributes (headway, capacity, frequency)
- Load factors and performance metrics
- Route length and coverage statistics

**Segment-Level Analysis** (`transit_boardings_by_segment_{period}.csv`):
- Boarding volumes at each link segment
- Dwell times and travel time functions
- Capacity utilization by segment
- Geographic boarding patterns

**Mode-Level Analysis** (`transit_boardings_by_service_type.csv`):
- Aggregated ridership by mode (bus, rail, BRT, etc.)
- Service coverage (lines, route miles) by mode
- Average frequencies and performance by mode
- Mode share and productivity comparisons

**Daily Analysis** (`transit_boardings_by_line_daily.csv`):
- All-day boarding totals by line
- Average hourly ridership patterns
- Service span (number of time periods served)
- Daily productivity metrics

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
3. Ensure EMME database files are not corrupted
4. Verify file permissions

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
1. Check `validation_results.csv` for quality flags
2. Review link-level data for outliers
3. Verify network coding accuracy
4. Check time period scenario assignments

#### ⚠️ Missing Attribute Values
**Symptoms**: High percentages in `attribute_coverage.csv`
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
1. **Check the log file**: `network_summary.log` contains detailed processing information
2. **Search for ERROR/WARNING**: Use text search to find issues
3. **Review validation results**: Check data quality summaries
4. **Examine processing statistics**: Verify completion rates

#### Contacting Support
When reporting issues, please provide:
- Complete error message
- Model run directory path  
- Log file (`network_summary.log`)
- Environment details (TM2PY environment setup and Python version)
- Processing summary output

## Advanced Usage

### Custom Analysis

The script provides a foundation for custom network analysis. Key extension points:

#### Additional Metrics
Modify `_calculate_performance_metrics()` to add custom calculations:
- Person delay (multiply by vehicle occupancy)
- Environmental metrics (emissions per VMT)
- Economic metrics (value of time calculations)

#### Geographic Aggregation
Extend county-level analysis with:
- Corridor-specific summaries
- Zone-to-zone flow analysis  
- Subarea performance metrics

#### Time Period Customization
Modify `_map_scenario_to_time_period()` for:
- Custom time period definitions
- Peak/off-peak classification
- Seasonal analysis scenarios

### Integration with Other Tools

#### Post-Processing Workflows
1. **Load CSV outputs** into your preferred analysis tool
2. **Join with spatial data** for mapping applications
3. **Combine with other TM2PY outputs** for comprehensive analysis
4. **Automate reporting** using output CSV files

#### Quality Assurance
1. **Compare with historical results** to identify anomalies
2. **Cross-validate with independent data sources**
3. **Review processing logs** for consistency checks
4. **Document analysis methodology** for reproducibility

## Best Practices

### Routine Analysis
1. **Run after every model update** to track changes
2. **Archive results** with model version information
3. **Document any data quality issues** found
4. **Maintain processing logs** for audit trail

### Performance Optimization
1. **Use SSD storage** for faster database access
2. **Close unnecessary applications** during processing
3. **Monitor system resources** during large network analysis
4. **Process during off-peak hours** to avoid conflicts

### Quality Control
1. **Review processing logs** before using results
2. **Validate against expected ranges** for your region
3. **Cross-check with previous model runs** for consistency
4. **Document assumptions** in analysis methodology