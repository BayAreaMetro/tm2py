# CT-RAMP Model Calibration Procedures

## Overview

This guide provides detailed procedures for calibrating CT-RAMP models to match regional travel behavior patterns and observed data. Calibration transforms generic model parameters into region-specific coefficients that accurately represent local travel behavior.

## Calibration Philosophy and Approach

### Hierarchical Calibration Strategy

**Sequential Model Calibration**: Calibrate models in execution order to maintain parameter stability and avoid feedback complications:

```text
Calibration Sequence:
1. Auto Ownership → Establishes vehicle availability foundations
2. CDAP → Sets activity pattern framework  
3. Mandatory Tour Frequency → Anchors work/school travel
4. Joint Tour Generation → Defines household coordination patterns
5. Individual Tour Frequency → Completes activity generation
6. Destination Choice → Establishes spatial patterns
7. Mode Choice → Calibrates transportation behavior
8. Time-of-Day Choice → Sets temporal patterns
9. Stop Models → Refines trip chaining behavior
```

### Calibration Data Hierarchy

**Primary Calibration Sources** (highest priority):
- Regional household travel surveys (HHTS)
- Census and American Community Survey (ACS) data
- Transit operator ridership data
- Traffic count data from regional agencies

**Secondary Sources** (supporting validation):
- National Household Travel Survey (NHTS) patterns
- Peer region model parameters and results
- Academic research on travel behavior
- Census Transportation Planning Products (CTPP)

## Auto Ownership Model Calibration

### Target Data Sources

**Primary Targets**:
```text
Data Source: Regional Household Travel Survey, ACS
Key Metrics:
- Household vehicle ownership by income quartile
- Zero-vehicle household rates by density/area type
- Vehicles per household by household size
- Vehicle ownership by life cycle category
```

### Calibration Procedure

**Step 1: Baseline Model Run**
```bash
# Run auto ownership with default parameters
run_model.py --component auto_ownership --scenario baseline --log calibration.log

# Generate initial results summary
summarize_auto_ownership.py --scenario baseline --compare_targets targets_auto_ownership.csv
```

**Step 2: Parameter Adjustment Strategy**
```text
Key Parameters for Adjustment:

Alternative Specific Constants (ASCs):
- ASC_0_vehicles: Adjust to match zero-vehicle household rate
- ASC_1_vehicle: Typically set to zero as base alternative
- ASC_2_vehicles: Adjust to match two-vehicle household rate  
- ASC_3plus_vehicles: Adjust to match high vehicle ownership rate

Income Coefficients:
- beta_income_0_veh: Increase magnitude to reduce ownership for low income
- beta_income_2plus_veh: Adjust to match high-income vehicle ownership patterns

Density/Urban Form Coefficients:
- beta_density_0_veh: Increase to match urban zero-vehicle rates
- beta_density_2plus_veh: Adjust suburban high-ownership patterns
```

**Step 3: Iterative Parameter Adjustment**
```bash
# Systematic calibration procedure
calibrate_auto_ownership.py --method gradient_descent --max_iterations 20

# Manual adjustment approach:
# 1. If zero-vehicle rate too low: Increase ASC_0_vehicles by 0.1-0.2
# 2. If high-vehicle rate too low: Increase ASC_3plus_vehicles by 0.1-0.2  
# 3. If income effects weak: Increase magnitude of income coefficients
# 4. Re-run and check convergence to targets
```

**Calibration Targets**:
```text
Vehicle Ownership Targets (Regional Variations):
- Overall HH auto ownership rate: 1.4-2.1 vehicles per household
- Zero-vehicle households: 5-25% (varies by region/urban form)
- Low income (<$35K): 0.8-1.2 vehicles per household
- High income (>$100K): 2.0-2.8 vehicles per household
- Urban core areas: 15-30% zero-vehicle households
- Suburban areas: 2-8% zero-vehicle households
```

## Coordinated Daily Activity Pattern (CDAP) Calibration

### Target Pattern Data

**Activity Pattern Targets**:
```text
Data Source: Regional HHTS, Time Use Surveys
Key Patterns:
- Mandatory activity rates by employment/student status
- Non-mandatory activity rates by demographics
- Home-based activity rates by age and employment
- Household coordination patterns and joint activity propensity
```

### CDAP Calibration Process

**Step 1: Employment and School Activity Calibration**
```bash
# Focus on mandatory activity pattern matching
calibrate_cdap.py --focus mandatory_patterns --targets mandatory_activity_rates.csv

# Key adjustments:
# - Worker mandatory activity rates: Should match employment participation
# - Student mandatory activity rates: Should match school enrollment patterns
# - Part-time worker patterns: Intermediate between full-time and non-worker
```

**Step 2: Household Coordination Calibration**
```text
Household Coordination Parameters:
- Joint activity propensity by household composition
- Adult coordination in two-adult households
- Parent-child coordination in family households
- Independence patterns for teenagers and young adults

Adjustment Strategy:
1. Compare household-level coordination rates to survey
2. Adjust coordination utility parameters for household types
3. Validate individual vs. joint activity balance
```

**Calibration Targets**:
```text
Mandatory Activity Participation:
- Full-time workers: 85-95% mandatory activity days
- Part-time workers: 40-70% mandatory activity days  
- Students: 80-95% mandatory activity during school year
- Non-workers: 5-15% mandatory activity (appointments, obligations)

Non-Mandatory Activity Participation:
- Working adults: 60-80% non-mandatory activity participation
- Non-working adults: 75-90% non-mandatory activity participation
- Children/teens: 70-85% non-mandatory activity participation
```

## Tour Generation Models Calibration

### Mandatory Tour Frequency Calibration

**Calibration Targets**:
```bash
# Extract survey-based tour generation rates
extract_survey_rates.py --purpose work --purpose school --by_demographics

# Expected patterns:
# - Full-time workers: 0.9-1.1 work tours per work day
# - Part-time workers: 0.8-1.0 work tours per work day
# - Students: 0.95-1.0 school tours per school day
# - Multiple tour rates: 5-15% depending on job complexity
```

**Parameter Adjustment Process**:
```text
Key Parameters:
- Alternative specific constants for tour frequency alternatives (0, 1, 2+ tours)
- Employment type interactions (professional, service, retail variations)
- Schedule flexibility coefficients
- Multiple job/school location coefficients

Calibration Strategy:
1. Match overall tour generation rates by employment status
2. Adjust multiple tour rates for complex employment arrangements  
3. Validate temporal patterns (weekday vs. weekend differences)
4. Check tour generation by occupation and industry categories
```

### Non-Mandatory Tour Frequency Calibration

**Individual Tour Generation Targets**:
```text
Data Sources: HHTS, Time-use surveys
Purpose Categories:
- Shopping tours: 1.5-2.5 per person per week
- Personal business: 0.8-1.5 per person per week
- Social/recreation: 1.0-2.5 per person per week
- Other discretionary: 0.3-0.8 per person per week
```

**Joint Tour Generation Targets**:
```text
Household Joint Activity Rates:
- Shopping: 1.0-2.0 joint tours per household per week
- Social/recreation: 0.8-1.5 joint tours per household per week  
- Personal business: 0.3-0.8 joint tours per household per week
- Higher rates for families with children
```

## Destination Choice Model Calibration

### Spatial Pattern Calibration

**Trip Length Distribution Targets**:
```bash
# Extract observed trip length patterns by purpose
analyze_trip_lengths.py --survey_data regional_hhts.csv --by_purpose --by_mode

# Generate calibration targets:
# - Average trip distances by purpose
# - Distance decay patterns and distribution shapes
# - Mode-specific distance variations
# - Urban vs. suburban trip length differences
```

**Destination Choice Parameters**:
```text
Core Parameters for Calibration:

Size Variables:
- Employment attraction for work destinations
- Retail employment attraction for shopping
- Population and household attraction for social activities

Distance Decay:
- Travel time coefficients by purpose and mode
- Distance coefficients and impedance functions
- Mode-specific accessibility sensitivities

Quality/Attractiveness:
- Activity center attraction premiums
- Regional destination draw factors  
- Specialized destination attraction (airports, universities)
```

**Calibration Process**:
```bash
# Systematic destination choice calibration
calibrate_destination_choice.py --purpose work --method maximum_likelihood
calibrate_destination_choice.py --purpose shopping --method market_share_matching
calibrate_destination_choice.py --purpose social --method gravity_model_fitting

# Iterative adjustment process:
# 1. Run destination choice with current parameters
# 2. Compare trip length distributions to survey targets
# 3. Adjust distance decay parameters to match patterns
# 4. Validate major destination market penetration rates
# 5. Check for spatial reasonableness and accessibility effects
```

## Mode Choice Model Calibration

### Mode Share Calibration Targets

**Regional Mode Share Patterns**:
```text
Data Sources: HHTS, Transit Ridership Data, Traffic Counts
Target Mode Shares by Purpose:

Work Trips:
- Drive Alone: 60-80% (varies by region/density)
- Carpool: 8-15%  
- Transit: 5-25% (higher in transit-rich regions)
- Walk/Bike: 2-8%

Non-Work Trips:
- Drive Alone: 65-85%
- Carpool: 10-20%
- Transit: 2-10%  
- Walk/Bike: 3-12%
```

### Mode Choice Calibration Procedure

**Step 1: Level-of-Service Validation**
```bash
# Validate transportation network inputs
validate_highway_skims.py --compare_observed_speeds --peak_periods
validate_transit_skims.py --compare_operator_data --routes_frequencies

# Ensure realistic travel times and costs before calibrating behavioral parameters
```

**Step 2: Alternative Specific Constant Adjustment**
```text
ASC Calibration Strategy:
1. Set Drive Alone ASC to zero (base alternative)
2. Adjust other mode ASCs to match observed mode shares:
   - Increase Transit ASC to boost transit share
   - Adjust Walk/Bike ASCs for active transportation shares
   - Modify Carpool ASCs for shared ride patterns

Typical ASC Adjustments:
- Transit ASC: +1.0 to +3.0 (depending on service quality)
- Walk ASC: -0.5 to +1.5 (depending on walkability)
- Bike ASC: -1.0 to +1.0 (depending on cycling infrastructure)
```

**Step 3: Cost and Time Sensitivity Calibration**
```bash
# Calibrate value of time and cost sensitivity
calibrate_mode_choice.py --focus cost_time_coefficients --method elasticity_matching

# Target elasticities:
# - Gas price elasticity: -0.1 to -0.3 for auto modes
# - Transit fare elasticity: -0.2 to -0.4 for transit modes  
# - Travel time elasticity: -0.5 to -1.2 across modes
```

## Time-of-Day Choice Calibration

### Temporal Distribution Targets

**Peak Period Travel Patterns**:
```text
Data Sources: Traffic Counts, Transit Ridership, Survey Data
Target Temporal Distributions:

Morning Peak (6-9 AM):
- Work tours: 60-75% of departures
- School tours: 70-85% of departures
- Other purposes: 15-25% of departures

Evening Peak (4-7 PM):  
- Work return: 50-70% of arrivals
- Shopping: 20-35% of departures
- Social: 25-40% of departures
```

### Time-of-Day Calibration Process

**Step 1: Work Schedule Calibration**
```bash
# Calibrate work departure time patterns
calibrate_time_of_day.py --purpose work --focus departure_times

# Match observed commute timing from regional data
# Adjust work schedule flexibility parameters
# Validate peak period concentration rates
```

**Step 2: Non-Work Activity Timing**
```text
Key Parameters:
- Activity timing preferences by purpose
- Peak period avoidance for discretionary activities  
- Evening activity timing for social/recreation
- Shopping activity timing preferences

Calibration Approach:
1. Match observed activity timing patterns from survey
2. Adjust activity-specific timing preferences
3. Validate congestion avoidance behavior
4. Check weekend vs. weekday timing differences
```

## Stop Frequency and Location Calibration

### Trip Chaining Pattern Targets

**Stop-Making Behavior Data**:
```text
Survey-Based Trip Chaining Patterns:
- Stop frequency by tour purpose and mode
- Average stops per tour by demographics
- Route deviation patterns and efficiency measures
- Activity clustering and spatial optimization behavior
```

### Calibration Methodology

**Stop Frequency Calibration**:
```bash
# Calibrate intermediate stop generation
calibrate_stop_frequency.py --by_tour_purpose --by_mode --match_survey_patterns

# Key targets:
# - Work tours: 65% no stops, 25% one stop, 10% multiple stops
# - Shopping tours: 35% no stops, 40% one stop, 25% multiple stops
# - Mode effects: Auto tours have higher stop frequencies
```

**Stop Location Calibration**:
```bash
# Calibrate stop destination choice  
calibrate_stop_location.py --focus spatial_efficiency --route_optimization

# Validation criteria:
# - Route deviation patterns reasonable (< 25% distance increase)
# - Activity clustering around major destinations
# - Stop location accessibility and infrastructure quality effects
```

## Model System Integration and Convergence

### Feedback Loop Calibration

**Network Assignment Integration**:
```bash
# Calibrate feedback between travel choices and network performance
calibrate_feedback_loops.py --max_iterations 10 --convergence_tolerance 0.05

# Monitor convergence indicators:
# - Mode share stability between iterations
# - Trip length distribution stability  
# - Network performance measure stability
# - Regional VMT and transit ridership stability
```

### System-Wide Validation

**Comprehensive Model Validation**:
```bash
# Execute comprehensive model validation suite
validate_model_system.py --comprehensive --report validation_summary.html

# Key validation categories:
# - Trip generation rates vs. survey benchmarks
# - Mode shares vs. observed ridership and traffic data
# - Spatial patterns vs. journey-to-work and activity surveys
# - Temporal patterns vs. count data and survey timing
# - Demographic variations vs. detailed survey analysis
```

## Advanced Calibration Techniques

### Automated Parameter Estimation

**Optimization-Based Calibration**:
```python
# Example automated calibration script
from calibration_tools import ModelCalibrator

calibrator = ModelCalibrator(
    model_components=['mode_choice', 'destination_choice'],
    target_data='regional_survey_targets.csv',
    optimization_method='gradient_descent',
    convergence_criteria={'tolerance': 0.02, 'max_iterations': 50}
)

# Execute automated calibration
calibration_results = calibrator.run_calibration()
calibrator.generate_report('calibration_results.html')
```

### Cross-Validation and Robustness Testing

**Model Validation Procedures**:
```bash
# Split sample validation
split_sample_validation.py --train_pct 70 --test_pct 30 --cross_validate

# Temporal validation (if multiple survey years available)
temporal_validation.py --base_year 2015 --validation_year 2019

# Geographic validation (holdout zones/districts)
geographic_validation.py --holdout_districts downtown,airport --validate_spatial_patterns
```

## Documentation and Parameter Management

### Calibration Documentation Standards

**Parameter Documentation Requirements**:
```text
For Each Calibrated Parameter:
1. Initial default value and source
2. Final calibrated value and justification
3. Target data source and validation criteria  
4. Calibration method and adjustment history
5. Sensitivity analysis and robustness testing results
6. Regional context and comparison to peer regions
```

**Version Control and Change Management**:
```bash
# Maintain parameter version control
track_parameter_changes.py --component all --version v2.1 --document_changes
backup_calibrated_parameters.py --scenario production --archive parameter_archive/

# Generate parameter summary documentation
document_calibration.py --comprehensive --output calibration_documentation.pdf
```

This comprehensive calibration guide ensures that CT-RAMP models accurately represent regional travel behavior while maintaining behavioral realism and parameter stability across different application scenarios.