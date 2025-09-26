# CT-RAMP Model Execution Workflow Guide

## Overview

This guide provides step-by-step instructions for executing the complete CT-RAMP model system, from initial data preparation through final output generation. It serves as the operational manual for practitioners implementing CT-RAMP for regional travel demand forecasting.

## Model Execution Philosophy

### Sequential Processing Framework

CT-RAMP follows a hierarchical execution sequence where each model component builds upon results from previous components:

```text
Execution Logic:
Individual/Household Characteristics → Activity Patterns → Travel Choices → Network Loading

Key Principles:
1. **Hierarchical Decision Structure**: Long-term choices constrain short-term decisions
2. **Iterative Convergence**: Feedback between travel choices and network conditions
3. **Data Flow Management**: Systematic passing of results between model components
4. **Quality Assurance**: Validation checkpoints throughout execution process
```

## Pre-Execution Requirements Checklist

### Essential Data Inputs

**Population Synthesis Data**:
- [ ] **Households File**: HH ID, size, income, vehicles, location (TAZ/MAZ)
- [ ] **Persons File**: Person ID, age, sex, employment, student status
- [ ] **Data Validation**: Population controls match regional totals
- [ ] **Geographic Coverage**: All TAZs have population assignments
- [ ] **File Format**: CSV with required field names and data types

**Land Use Data**:
- [ ] **Employment by Category**: Basic, retail, service employment by TAZ
- [ ] **Households by Income**: Household distribution by income category
- [ ] **School Enrollment**: Students by grade level and school locations
- [ ] **Area Types**: Urban form classification (CBD, urban, suburban, rural)
- [ ] **Accessibility Measures**: Distance to airports, regional attractions

**Transportation Network Data**:
- [ ] **Highway Skims**: Travel time, distance, cost by time period and vehicle class
- [ ] **Transit Skims**: In-vehicle time, walk access, wait time, fares, transfers
- [ ] **Non-Motorized Skims**: Walk and bike time/distance with infrastructure quality
- [ ] **Parking Data**: Cost and availability by destination type and time period
- [ ] **Network Validation**: Skim matrices reasonable and complete

### Configuration Files Setup

**Model Parameters**:
- [ ] **Utility Coefficients**: Calibrated parameters for each choice model
- [ ] **Market Segmentation**: Demographic categories and interaction terms
- [ ] **Alternative Availability**: Rules for mode and destination availability
- [ ] **Convergence Criteria**: Iteration limits and tolerance thresholds

**Geographic Settings**:
- [ ] **Zone System**: TAZ and MAZ definitions with area types
- [ ] **District Definitions**: County, jurisdiction, and analysis district boundaries
- [ ] **External Stations**: Gateway zones for external travel
- [ ] **Special Generators**: Airports, major attractions, and activity centers

## Core Model Execution Sequence

### Phase 1: Individual and Household Characteristics

#### Step 1.1: Auto Ownership Model
```bash
# Execute auto ownership choice
run_model.py --component auto_ownership --scenario baseline

# Validation checkpoints:
validate_results.py --check auto_ownership_rates --by_income --by_density
validate_results.py --check vehicle_availability --by_household_size
```

**Expected Results**:
- Household vehicle ownership (0, 1, 2, 3+ vehicles)
- Ownership rates by income: Low (0.8 autos/HH), Mid (1.6), High (2.1)
- Urban vs. Suburban variations in ownership patterns

**Quality Assurance**:
- [ ] Total vehicles match regional vehicle registration data
- [ ] Income-based ownership patterns realistic
- [ ] Zero-vehicle households concentrated in transit-accessible areas
- [ ] No households with negative vehicles or missing assignments

#### Step 1.2: Coordinated Daily Activity Pattern (CDAP)
```bash
# Execute household activity coordination
run_model.py --component cdap --scenario baseline

# Validation checkpoints:
validate_results.py --check activity_patterns --by_household_type
validate_results.py --check mandatory_activity_rates --compare_survey
```

**Expected Results**:
- Person daily activity patterns (Mandatory, Non-mandatory, Home)
- Household coordination patterns and joint activity propensity
- Work/school participation rates by demographics

**Quality Assurance**:
- [ ] Employment rates match observed labor force participation
- [ ] School attendance rates appropriate for student age groups
- [ ] Household activity coordination patterns reasonable
- [ ] Non-mandatory activity rates consistent with time-use surveys

### Phase 2: Tour Generation and Coordination

#### Step 2.1: Mandatory Tour Frequency
```bash
# Execute work and school tour generation
run_model.py --component mandatory_tour_frequency --scenario baseline

# Validation checkpoints:
validate_results.py --check work_tour_rates --by_employment_status
validate_results.py --check school_tour_rates --by_student_type
```

**Expected Results**:
- Work tours: 0.85-0.95 tours per full-time worker
- School tours: 0.90-0.98 tours per student
- Part-time work and flexible schedule variations

**Quality Assurance**:
- [ ] Tour generation rates consistent with employment/school patterns
- [ ] Multiple tour rates reasonable for complex work arrangements
- [ ] Weekend and holiday tour adjustments appropriate

#### Step 2.2: Joint Tour Generation
```bash
# Execute household joint activity generation
run_model.py --component joint_tour_generation --scenario baseline

# Validation checkpoints:
validate_results.py --check joint_tour_rates --by_household_composition
validate_results.py --check joint_participation --by_age_role
```

**Expected Results**:
- Joint shopping tours: 1.2-1.8 per week for families
- Joint social/recreation tours: 0.8-1.4 per week
- Higher rates for households with children

**Quality Assurance**:
- [ ] Joint tour rates higher for families with children
- [ ] Adult participation rates in joint activities appropriate
- [ ] Joint tour purposes match survey observations

#### Step 2.3: Individual Non-Mandatory Tour Frequency
```bash
# Execute individual discretionary tour generation
run_model.py --component individual_tour_frequency --scenario baseline

# Validation checkpoints:
validate_results.py --check individual_tour_rates --by_age_employment
validate_results.py --check activity_purpose_split --compare_survey
```

**Expected Results**:
- Individual shopping: 1.5-2.5 tours per person per week
- Individual social/recreation: 1.0-2.0 tours per person per week
- Age and employment status variations

**Quality Assurance**:
- [ ] Tour rates decrease with mandatory activity commitments
- [ ] Age-based activity patterns match survey data
- [ ] Purpose distribution realistic for discretionary activities

### Phase 3: Spatial and Temporal Choices

#### Step 3.1: Tour Destination Choice
```bash
# Execute destination choice for all tour purposes
run_model.py --component tour_destination --scenario baseline

# Validation checkpoints:
validate_results.py --check destination_patterns --by_purpose_distance
validate_results.py --check market_penetration --by_activity_center
```

**Expected Results**:
- Work destinations: Average 12-15 mile commute distances
- Shopping destinations: 80% within 10 miles, neighborhood vs. regional patterns
- School destinations: Enrollment boundary compliance and choice patterns

**Quality Assurance**:
- [ ] Distance distributions match survey observations
- [ ] Activity center market penetration rates reasonable
- [ ] Destination choice responds to accessibility differences
- [ ] Special generators (airports, universities) show appropriate draw

#### Step 3.2: Tour Time of Day Choice
```bash
# Execute departure time choice for all tours
run_model.py --component tour_time_of_day --scenario baseline

# Validation checkpoints:
validate_results.py --check departure_time_patterns --by_purpose
validate_results.py --check peak_period_loading --compare_counts
```

**Expected Results**:
- Work departure times: 7-9 AM peak (60% of departures)
- Shopping departures: Mid-day and afternoon concentration
- Social/recreation: Evening and weekend patterns

**Quality Assurance**:
- [ ] Peak period concentration appropriate for work activities
- [ ] Off-peak activity timing matches observed patterns
- [ ] Time-of-day choice responds to congestion levels
- [ ] Weekend vs. weekday patterns realistic

#### Step 3.3: Tour Mode Choice
```bash
# Execute primary mode choice for all tours
run_model.py --component tour_mode_choice --scenario baseline

# Validation checkpoints:
validate_results.py --check mode_shares --by_purpose_distance
validate_results.py --check transit_ridership --compare_observed
```

**Expected Results**:
- Auto mode share: 80-90% for most purposes
- Transit mode share: 5-15% depending on regional transit service
- Walk/bike shares higher for short distances and dense areas

**Quality Assurance**:
- [ ] Mode shares match regional survey data
- [ ] Transit ridership consistent with observed boardings
- [ ] Distance and density effects on mode choice appropriate
- [ ] Policy sensitivity (parking costs, gas prices) reasonable

### Phase 4: Tour Complexity and Intermediate Stops

#### Step 4.1: At-Work Subtour Generation
```bash
# Execute work-based subtour generation
run_model.py --component at_work_subtours --scenario baseline

# Validation checkpoints:
validate_results.py --check subtour_rates --by_occupation_industry
validate_results.py --check work_based_travel --compare_survey
```

**Expected Results**:
- Business subtours: 0.3-0.8 per work day by occupation
- Lunch subtours: 0.6-0.9 per work day
- Professional workers higher subtour rates

**Quality Assurance**:
- [ ] Subtour rates vary appropriately by occupation type
- [ ] Work-based travel patterns match employment surveys
- [ ] Subtour destinations accessible from work locations

#### Step 4.2: Stop Frequency Choice
```bash
# Execute intermediate stop generation
run_model.py --component stop_frequency --scenario baseline

# Validation checkpoints:
validate_results.py --check stop_patterns --by_tour_purpose_mode
validate_results.py --check trip_chaining --compare_survey
```

**Expected Results**:
- Work tours: 65% no stops, 25% one stop, 10% multiple stops
- Shopping tours: 35% no stops, 40% one stop, 25% multiple stops
- Mode effects: Auto tours higher stop frequency

**Quality Assurance**:
- [ ] Stop frequency varies by tour purpose appropriately
- [ ] Mode effects on stop-making realistic
- [ ] Trip chaining patterns match survey observations

#### Step 4.3: Stop Location Choice
```bash
# Execute stop destination choice
run_model.py --component stop_location --scenario baseline

# Validation checkpoints:
validate_results.py --check stop_location_patterns --by_tour_destination
validate_results.py --check route_deviation --efficiency_measures
```

**Expected Results**:
- En-route stops: Minimal route deviation (< 15% distance increase)
- Destination clustering: Multiple stops near primary destination
- Activity complementarity in stop purpose combinations

**Quality Assurance**:
- [ ] Stop locations show spatial efficiency patterns
- [ ] Activity clustering around major destinations
- [ ] Route deviation patterns reasonable for time budgets

### Phase 5: Individual Trip Segments

#### Step 5.1: Trip Mode Choice
```bash
# Execute trip-level mode choice within tours
run_model.py --component trip_mode_choice --scenario baseline

# Validation checkpoints:
validate_results.py --check trip_mode_consistency --with_tour_mode
validate_results.py --check mode_switching --at_activity_locations
```

**Expected Results**:
- High tour-trip mode consistency (90-95%)
- Mode switching at transit stops and activity clusters
- Walking trips for short segments within auto tours

**Quality Assurance**:
- [ ] Trip modes generally consistent with tour modes
- [ ] Mode switching occurs at appropriate locations
- [ ] Local accessibility affects trip mode choice

#### Step 5.2: Trip Scheduling and Final Assembly
```bash
# Execute trip timing and create final trip tables
run_model.py --component trip_scheduling --scenario baseline
run_model.py --component trip_tables --scenario baseline

# Final validation:
validate_results.py --comprehensive_check --scenario baseline
```

**Expected Results**:
- Complete trip tables by purpose, mode, and time period
- Balanced trip productions and attractions by zone
- Realistic temporal and spatial travel patterns

**Quality Assurance**:
- [ ] Trip tables balance to zone-level activity generation
- [ ] Peak period concentration matches observed patterns
- [ ] Mode-specific trip patterns consistent with infrastructure

## Iterative Feedback and Convergence

### Network Feedback Loop

```bash
# Execute feedback iteration cycle
for iteration in {1..5}; do
    echo "Starting feedback iteration $iteration"
    
    # 1. Load trips onto networks
    run_assignment.py --highway --trips highway_trips_$iteration.csv
    run_assignment.py --transit --trips transit_trips_$iteration.csv
    
    # 2. Calculate new level-of-service
    calculate_skims.py --highway --time_period all
    calculate_skims.py --transit --time_period all
    
    # 3. Re-run mode and destination choice with updated LOS
    run_model.py --component tour_mode_choice --iteration $iteration
    run_model.py --component tour_destination --iteration $iteration
    
    # 4. Check convergence
    check_convergence.py --iteration $iteration --tolerance 0.05
    
    if [ $? -eq 0 ]; then
        echo "Converged at iteration $iteration"
        break
    fi
done
```

### Convergence Criteria

**Mode Choice Convergence**:
- Mode shares stable within 2% between iterations
- Transit ridership stable within 5% between iterations
- Vehicle miles traveled stable within 3% between iterations

**Destination Choice Convergence**:
- Trip length distribution stable within 3% between iterations
- Major destination market shares stable within 5%
- Activity center attraction rates stable within 5%

**Network Performance Convergence**:
- Highway congestion levels stable within 10% between iterations
- Transit load factors stable within 15% between iterations
- Regional accessibility measures stable within 5%

## Performance Monitoring and Diagnostics

### Execution Time Monitoring

```bash
# Track model execution performance
time_model.py --component all --log execution_times.csv

# Expected execution times (10M population):
# Auto Ownership: 5-10 minutes
# CDAP: 15-30 minutes  
# Tour Generation: 20-40 minutes
# Destination Choice: 60-120 minutes
# Mode Choice: 30-60 minutes
# Stop Models: 40-80 minutes
# Trip Models: 20-40 minutes
```

### Memory and Resource Usage

```bash
# Monitor system resources during execution
monitor_resources.py --log system_performance.csv

# Resource requirements:
# RAM: 16-32 GB for large regions (>2M population)
# CPU: Multi-core processing recommended (8+ cores)
# Storage: 100-500 GB for model runs and outputs
# Network: Fast I/O for large data file processing
```

### Error Detection and Recovery

**Common Error Categories**:

**Data Issues**:
- Missing or invalid input data values
- Inconsistent geographic coding between datasets
- Population synthesis errors or missing households

**Model Issues**:
- Non-convergence in choice model estimation
- Extreme parameter values causing numerical instability
- Memory overflow with large population samples

**Configuration Issues**:
- Incorrect file paths or missing configuration parameters
- Inconsistent model component versions or parameters
- Invalid geographic zone definitions or network coding

**Error Recovery Procedures**:

```bash
# Automated error detection and reporting
check_model_inputs.py --comprehensive --report input_validation.html
diagnose_model_run.py --log model_execution.log --report diagnostic_report.html

# Common fixes:
fix_missing_data.py --impute --method hot_deck
recalibrate_parameters.py --component problematic_model --data survey_data.csv
restart_from_checkpoint.py --checkpoint last_successful_component
```

## Output Generation and Quality Assurance

### Standard Model Outputs

**Trip Tables by Purpose and Mode**:
```bash
# Generate standard trip table outputs
export_trip_tables.py --format omx --time_periods 5 --modes all
export_trip_tables.py --format csv --summary --by_purpose_mode_period
```

**Activity and Tour Summaries**:
```bash
# Generate activity pattern summaries
summarize_activities.py --by_demographics --by_geography --by_time
summarize_tours.py --by_purpose --by_mode --by_distance_time
```

**Model Performance Diagnostics**:
```bash
# Generate model validation reports
generate_validation_report.py --compare_survey --compare_counts --scenario baseline
calculate_performance_metrics.py --efficiency --equity --environmental
```

### Quality Assurance Procedures

**Automated Validation Checks**:

```bash
# Run comprehensive validation suite
validate_model_run.py --scenario baseline --report validation_report.html

# Key validation categories:
# - Trip generation rates vs. survey data
# - Mode shares vs. observed ridership/traffic counts  
# - Trip length distributions vs. survey patterns
# - Peak period travel patterns vs. count data
# - Activity participation rates vs. time-use surveys
```

**Manual Review Procedures**:

1. **Reasonableness Checks**:
   - [ ] Activity rates consistent with regional demographics
   - [ ] Travel patterns reflect known regional characteristics
   - [ ] Mode shares appropriate for transit service levels
   - [ ] Peak period concentration matches observed traffic

2. **Sensitivity Testing**:
   - [ ] Model responds appropriately to policy changes
   - [ ] Parameter changes produce expected behavioral shifts
   - [ ] Convergence stable across different scenarios

3. **Comparative Analysis**:
   - [ ] Results comparable to previous model versions
   - [ ] Regional patterns consistent with peer regions
   - [ ] Trend analysis shows reasonable evolution

## Troubleshooting Common Issues

### Performance Optimization

**Slow Execution Issues**:
```bash
# Profile model execution to identify bottlenecks
profile_model.py --component all --detailed --optimize

# Common optimizations:
# - Increase memory allocation for large choice sets
# - Use parallel processing for independent calculations
# - Optimize file I/O with faster storage systems
# - Reduce sample sizes for testing and debugging
```

**Memory Management**:
```bash
# Monitor and optimize memory usage
optimize_memory.py --component memory_intensive --strategy chunking

# Memory optimization strategies:
# - Process population in smaller chunks
# - Clear intermediate results between components
# - Use efficient data structures and file formats
# - Implement garbage collection between major steps
```

### Model Calibration Issues

**Parameter Instability**:
```bash
# Diagnose and fix parameter estimation issues
diagnose_calibration.py --component problematic_model --sensitivity_analysis
recalibrate_model.py --robust_estimation --cross_validation
```

**Convergence Problems**:
```bash
# Address feedback loop convergence issues
adjust_convergence_criteria.py --relax_tolerance --increase_iterations
dampen_feedback.py --factor 0.5 --component mode_destination_feedback
```

This comprehensive execution workflow guide provides the operational foundation for successful CT-RAMP implementation, ensuring reliable, validated model results for transportation planning applications.