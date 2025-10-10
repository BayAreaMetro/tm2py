# TM2PY Network Summary Script - Testing Plan

This document outlines a comprehensive step-wise testing plan for the enhanced TM2PY network summary script with multimodal (highway + transit) analysis capabilities.

## Testing Overview

### Objectives
- Validate script functionality across all processing phases
- Ensure robust error handling and input validation
- Verify output quality and data accuracy
- Test multimodal integration (highway + transit)
- Confirm production readiness

### Test Environment Setup

#### Prerequisites
```bash
# 1. Activate TM2PY environment
C:\GitHub\tm2pyenv\Scripts\activate

# 2. Verify EMME modules available
python -c "import inro.emme.database.emmebank; print('EMME OK')"

# 3. Navigate to TM2PY directory
cd c:\GitHub\tm2py

# 4. Locate test model run directory
# Example: E:\2015-tm22-dev-sprint-04
```

#### Test Data Requirements
- **Model Run Directory**: Complete TM2PY model run with both databases
- **Highway Database**: `emme_project/Database_highway/emmebank`
- **Transit Database**: `emme_project/Database_transit/emmebank`
- **Scenarios**: 6 time periods (EA, AM, MD, PM, EV, EA2) in both databases

---

## Phase 1: Basic Script Validation

**Objective**: Test basic script execution and command-line interface

### Test 1.1: Help and Usage Display
```bash
# Test help text display
python scripts\network_summary.py --help

# Expected: Complete help text with all options displayed
# Verify: Transit analysis mentioned in output files
```

### Test 1.2: Argument Validation
```bash
# Test missing required argument
python scripts\network_summary.py

# Expected: Error message about missing model_run_directory
# Verify: Clear error message and usage hint

# Test invalid directory
python scripts\network_summary.py C:\NonExistent\Path

# Expected: Error message about directory not existing
# Verify: Clean error handling without stack trace
```

### Test 1.3: Optional Arguments
```bash
# Test custom output directory
python scripts\network_summary.py E:\2015-tm22-dev-sprint-04 --output C:\temp\test_output

# Test verbose flag
python scripts\network_summary.py E:\2015-tm22-dev-sprint-04 --verbose

# Expected: Verbose logging enabled, detailed output
# Verify: Debug-level messages appear
```

**Success Criteria**:
- [ ] Help text displays correctly with transit analysis mentioned
- [ ] Proper error messages for invalid inputs
- [ ] Optional arguments work as expected
- [ ] No unhandled exceptions during argument parsing

---

## Phase 2: Input Validation Testing

**Objective**: Test comprehensive input validation for both databases

### Test 2.1: Validation-Only Mode
```bash
# Test input validation only
python scripts\network_summary.py E:\2015-tm22-dev-sprint-04 --validate-only

# Expected: Complete validation report without processing
# Verify: Both highway and transit database validation
```

### Test 2.2: Directory Structure Validation
```bash
# Test with missing transit database
# (Temporarily rename Database_transit folder)
python scripts\network_summary.py E:\2015-tm22-dev-sprint-04 --validate-only

# Expected: Error about missing transit database
# Verify: Clear error message identifying missing components
```

### Test 2.3: Database Connectivity
```bash
# Test with valid databases
python scripts\network_summary.py E:\2015-tm22-dev-sprint-04 --validate-only --verbose

# Expected: Successful database connections to both highway and transit
# Verify: Database titles and scenario counts reported
```

### Test 2.4: Scenario Validation
```bash
# Check scenario validation output
python scripts\network_summary.py E:\2015-tm22-dev-sprint-04 --validate-only --verbose

# Expected: Time period mapping for both databases
# Verify: All 6 time periods identified correctly
# Verify: Warnings for any missing or unknown scenarios
```

**Success Criteria**:
- [ ] Directory structure validation works for both databases
- [ ] Database connectivity test succeeds
- [ ] Scenario enumeration and time period mapping correct
- [ ] Appropriate warnings for missing components
- [ ] Validation completes without crashes

---

## Phase 3: Highway Analysis Testing

**Objective**: Test highway data extraction and performance analysis

### Test 3.1: Highway Data Extraction
```bash
# Run with verbose logging to see extraction progress
python scripts\network_summary.py E:\2015-tm22-dev-sprint-04 --verbose

# Monitor output for:
# - Link processing progress (every 10,000 links)
# - Attribute availability statistics
# - Data quality checks
```

### Test 3.2: Highway Output Generation
```bash
# Check highway output files are created
python scripts\network_summary.py E:\2015-tm22-dev-sprint-04

# Expected highway files:
ls network_summary/facility_type_summary.csv
ls network_summary/overall_summary.csv
ls network_summary/lane_mile_inventory.csv

# Verify files contain data and reasonable values
```

### Test 3.3: Highway Data Quality
```python
# Validate highway output data quality
import pandas as pd

# Load and check facility type summary
df = pd.read_csv('network_summary/facility_type_summary.csv')
print(df.head())
print(df['facility_type'].value_counts())

# Check VMT/VHT ratios are reasonable (15-60 mph range)
df['speed_mph'] = df['vmt'] / df['vht']
print(f"Speed range: {df['speed_mph'].min():.1f} - {df['speed_mph'].max():.1f} mph")
```

**Success Criteria**:
- [ ] Highway data extraction completes without errors
- [ ] All expected highway output files created
- [ ] Data values within reasonable ranges for Bay Area
- [ ] Speed calculations between 15-60 mph for most facility types
- [ ] Processing statistics logged correctly

---

## Phase 4: Transit Analysis Testing

**Objective**: Test transit data extraction and ridership analysis

### Test 4.1: Transit Data Extraction
```bash
# Run full analysis and monitor transit processing
python scripts\network_summary.py E:\2015-tm22-dev-sprint-04 --verbose

# Monitor output for:
# - Transit line processing progress (every 100 lines)
# - Segment processing statistics
# - Mode distribution reporting
# - Boarding data quality checks
```

### Test 4.2: Transit Output Generation
```bash
# Check all transit output files are created
python scripts\network_summary.py E:\2015-tm22-dev-sprint-04

# Expected transit files by time period:
ls network_summary/transit_boardings_by_line_ea.csv
ls network_summary/transit_boardings_by_line_am.csv
ls network_summary/transit_boardings_by_segment_am.csv
ls network_summary/transit_boardings_by_line_daily.csv
ls network_summary/transit_boardings_by_service_type.csv

# Verify all time periods (ea, am, md, pm, ev) have files
```

### Test 4.3: Transit Data Quality
```python
# Validate transit output data quality
import pandas as pd

# Check line-level data
lines_am = pd.read_csv('network_summary/transit_boardings_by_line_am.csv')
print(f"AM period lines: {len(lines_am)}")
print(f"Total AM boardings: {lines_am['transit_volume'].sum():,.0f}")

# Check mode distribution
modes = pd.read_csv('network_summary/transit_boardings_by_service_type.csv')
print("Mode summary:")
print(modes[modes['time_period'] == 'ALL_DAY'][['mode_id', 'transit_volume', 'num_lines']])

# Check daily totals consistency
daily = pd.read_csv('network_summary/transit_boardings_by_line_daily.csv')
print(f"Daily total boardings: {daily['transit_volume'].sum():,.0f}")
```

**Success Criteria**:
- [ ] Transit data extraction completes without errors
- [ ] All expected transit output files created for all time periods
- [ ] Boarding volumes within reasonable ranges (>0, <10,000 per segment)
- [ ] Mode distribution makes sense for Bay Area
- [ ] Load factors between 0-3 (reasonable capacity utilization)

---

## Phase 5: Output Validation Testing

**Objective**: Test output validation and quality assurance

### Test 5.1: Output Validation Mode
```bash
# Test output validation on completed results
python scripts\network_summary.py E:\2015-tm22-dev-sprint-04 --validate-outputs

# Expected: Comprehensive validation report
# Verify: Both highway and transit files validated
```

### Test 5.2: Speed Range Validation
```bash
# Run full analysis and check speed validation
python scripts\network_summary.py E:\2015-tm22-dev-sprint-04 --verbose

# Monitor validation output for:
# - Speed range warnings (outside 5-80 mph)
# - VMT/VHT ratio consistency checks
# - Data completeness percentages
```

### Test 5.3: Cross-Modal Validation
```python
# Compare highway and transit results for consistency
import pandas as pd

# Load highway summary
highway = pd.read_csv('network_summary/overall_summary.csv')
print("Highway system totals:")
print(highway[['time_period', 'total_vmt', 'total_vht']].head())

# Load transit summary  
transit = pd.read_csv('network_summary/transit_boardings_by_service_type.csv')
am_transit = transit[transit['time_period'] == 'AM']
print(f"AM transit boardings: {am_transit['transit_volume'].sum():,.0f}")

# Check for reasonable mode split (transit should be meaningful but not dominant)
```

**Success Criteria**:
- [ ] Output validation completes successfully
- [ ] Speed ranges flagged appropriately
- [ ] Data completeness >95% for both modes
- [ ] Cross-modal results are reasonable for Bay Area
- [ ] No critical validation errors

---

## Phase 6: Error Handling Testing

**Objective**: Test error handling and recovery scenarios

### Test 6.1: Missing Transit Database
```bash
# Temporarily rename transit database folder
# Run analysis to test graceful degradation
python scripts\network_summary.py E:\2015-tm22-dev-sprint-04 --verbose

# Expected: Highway analysis continues, transit skipped with warning
# Verify: No crashes, clear warning messages
```

### Test 6.2: Corrupted Database
```bash
# Test with empty/corrupted database files
# (Create empty emmebank file for testing)
python scripts\network_summary.py E:\test-corrupted-db --verbose

# Expected: Clear error messages about database corruption
# Verify: Graceful failure without stack traces
```

### Test 6.3: Permission Errors
```bash
# Test with read-only output directory
# (Set output directory to read-only)
python scripts\network_summary.py E:\2015-tm22-dev-sprint-04 --output C:\Windows\System32

# Expected: Clear error about write permissions
# Verify: Helpful suggestion for resolution
```

### Test 6.4: Memory/Performance Testing
```bash
# Test with very verbose logging on large dataset
python scripts\network_summary.py E:\2015-tm22-dev-sprint-04 --verbose

# Monitor:
# - Memory usage during processing
# - Processing time for each phase
# - Log file size and rotation
```

**Success Criteria**:
- [ ] Graceful handling of missing transit database
- [ ] Clear error messages for corrupted databases
- [ ] Appropriate permission error handling
- [ ] No memory leaks or excessive resource usage
- [ ] All errors logged appropriately

---

## Phase 7: Integration Testing

**Objective**: Comprehensive end-to-end testing with production data

### Test 7.1: Full Production Run
```bash
# Complete production run with all features
python scripts\network_summary.py E:\2015-tm22-dev-sprint-04 --output .\test_results --verbose

# Monitor complete workflow:
# - All 6 processing phases
# - Highway and transit analysis
# - Output generation and validation
# - Performance metrics
```

### Test 7.2: Results Validation
```python
# Comprehensive results validation script
import pandas as pd
import os

results_dir = "test_results"

# Validate all expected files exist
expected_files = [
    'facility_type_summary.csv',
    'overall_summary.csv', 
    'lane_mile_inventory.csv',
    'transit_boardings_by_line_daily.csv',
    'transit_boardings_by_service_type.csv'
]

for period in ['ea', 'am', 'md', 'pm', 'ev']:
    expected_files.extend([
        f'transit_boardings_by_line_{period}.csv',
        f'transit_boardings_by_segment_{period}.csv'
    ])

print("File validation:")
for file in expected_files:
    path = os.path.join(results_dir, file)
    exists = os.path.exists(path)
    size = os.path.getsize(path) if exists else 0
    print(f"  {file}: {'✓' if exists else '✗'} ({size:,} bytes)")

# Validate data ranges for Bay Area
highway = pd.read_csv(os.path.join(results_dir, 'overall_summary.csv'))
print(f"\nHighway validation:")
print(f"  Daily VMT range: {highway['total_vmt'].min():.0f} - {highway['total_vmt'].max():.0f}")
print(f"  Daily VHT range: {highway['total_vht'].min():.0f} - {highway['total_vht'].max():.0f}")

transit = pd.read_csv(os.path.join(results_dir, 'transit_boardings_by_service_type.csv'))
all_day = transit[transit['time_period'] == 'ALL_DAY']
print(f"\nTransit validation:")
print(f"  Daily boardings: {all_day['transit_volume'].sum():,.0f}")
print(f"  Number of modes: {len(all_day)}")
```

### Test 7.3: Performance Benchmarking
```bash
# Time the full analysis
time python scripts\network_summary.py E:\2015-tm22-dev-sprint-04 --output .\benchmark_results

# Expected timeframes:
# - Highway analysis: 5-15 minutes
# - Transit analysis: 10-20 minutes  
# - Total runtime: 15-35 minutes
```

### Test 7.4: Regression Testing
```python
# Compare results with baseline (if available)
# Check for significant changes in key metrics
import pandas as pd

# Load current results
current_highway = pd.read_csv('test_results/overall_summary.csv')
current_transit = pd.read_csv('test_results/transit_boardings_by_service_type.csv')

# Compare with baseline (if exists)
try:
    baseline_highway = pd.read_csv('baseline/overall_summary.csv')
    
    # Check for significant changes (>10% difference)
    for period in current_highway['time_period'].unique():
        current_vmt = current_highway[current_highway['time_period']==period]['total_vmt'].iloc[0]
        baseline_vmt = baseline_highway[baseline_highway['time_period']==period]['total_vmt'].iloc[0]
        pct_change = (current_vmt - baseline_vmt) / baseline_vmt * 100
        
        if abs(pct_change) > 10:
            print(f"WARNING: {period} VMT changed by {pct_change:.1f}%")
        else:
            print(f"OK: {period} VMT change: {pct_change:.1f}%")
            
except FileNotFoundError:
    print("No baseline data available for comparison")
```

**Success Criteria**:
- [ ] Complete end-to-end run succeeds
- [ ] All output files generated with reasonable sizes
- [ ] Highway metrics within expected Bay Area ranges
- [ ] Transit ridership totals reasonable for Bay Area
- [ ] Performance within acceptable time limits
- [ ] Results consistent with baseline (if available)

---

## Testing Checklist

### Pre-Testing Setup
- [ ] TM2PY environment activated
- [ ] EMME modules available
- [ ] Test model run directory identified
- [ ] Both highway and transit databases present
- [ ] Output directory permissions verified

### Phase 1: Basic Validation
- [ ] Help text displays correctly
- [ ] Error handling for invalid arguments
- [ ] Optional arguments function properly
- [ ] No unhandled exceptions

### Phase 2: Input Validation
- [ ] Directory structure validation
- [ ] Database connectivity testing
- [ ] Scenario enumeration and mapping
- [ ] Appropriate error/warning messages

### Phase 3: Highway Analysis
- [ ] Data extraction completes
- [ ] Output files generated
- [ ] Data quality within ranges
- [ ] Processing statistics accurate

### Phase 4: Transit Analysis
- [ ] Transit data extraction succeeds
- [ ] All time period files created
- [ ] Boarding data quality reasonable
- [ ] Mode distribution makes sense

### Phase 5: Output Validation
- [ ] Validation mode functions
- [ ] Speed/quality checks work
- [ ] Cross-modal consistency
- [ ] No critical validation errors

### Phase 6: Error Handling
- [ ] Missing database handling
- [ ] Corrupted data handling
- [ ] Permission error handling
- [ ] Memory/performance acceptable

### Phase 7: Integration
- [ ] Full production run succeeds
- [ ] All files generated correctly
- [ ] Results within expected ranges
- [ ] Performance benchmarks met

---

## Troubleshooting Common Issues

### EMME Connection Issues
```bash
# Verify EMME shell environment
echo $EMMEHOME
python -c "import inro.emme; print('EMME OK')"

# Check database file permissions
ls -la emme_project/Database_*/emmebank
```

### Memory Issues
```bash
# Monitor memory usage during processing
# Close other applications
# Use Task Manager to monitor Python process
```

### Performance Issues
```bash
# Use SSD storage for better I/O performance
# Run during off-peak hours
# Consider processing subsets for testing
```

### Validation Failures
```bash
# Check log files for detailed error information
# Verify database integrity with EMME desktop
# Validate input data ranges and completeness
```

---

## Success Criteria Summary

✅ **Script executes without crashes across all test scenarios**  
✅ **Both highway and transit analysis complete successfully**  
✅ **All expected output files generated with reasonable data**  
✅ **Data quality validation passes without critical errors**  
✅ **Performance within acceptable time limits (< 1 hour)**  
✅ **Robust error handling and informative messages**  
✅ **Results consistent with Bay Area transportation patterns**  

The script is ready for production use when all phases pass successfully!