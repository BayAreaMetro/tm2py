# CT-RAMP Calibration and Validation Data Specifications

## Overview

This document provides comprehensive specifications for calibration target data and validation datasets required for CT-RAMP model development, calibration, and performance validation. It establishes data requirements, collection procedures, and quality standards supporting systematic model calibration and validation procedures outlined in Step 6 implementation guidance.

## Calibration Target Data Framework

### Model Component Calibration Requirements

**Auto Ownership Model Calibration Targets**:

```bash
# Auto ownership calibration target data collection and validation
collect_ao_targets.py --survey_data household_survey.csv --census_data acs_vehicles.csv --validate_consistency

# Auto ownership target categories:
# 1. Household vehicle ownership distribution by income quartile
# 2. Zero-vehicle household rates by geographic area and density
# 3. Vehicle ownership by household size and composition
# 4. Vehicle ownership by employment status and accessibility level
```

**Auto Ownership Target Data Structure**:

```text
Required Auto Ownership Calibration Targets:
1. Household Vehicle Distribution Targets
   - 0-vehicle households: Target percentage by income and geography
   - 1-vehicle households: Target percentage by household characteristics
   - 2-vehicle households: Target percentage by suburban/urban location
   - 3+ vehicle households: Target percentage by income and household size

2. Demographic Segmentation Targets
   - Vehicle ownership by age of head of household
   - Vehicle ownership by presence of children and workers
   - Vehicle ownership by housing type and tenure
   - Vehicle ownership by parking availability and cost

3. Geographic and Accessibility Targets
   - Vehicle ownership by transit service level and accessibility
   - Vehicle ownership by urban form and built environment
   - Vehicle ownership by employment accessibility and job density
   - Vehicle ownership by income and cost of transportation alternatives
```

**Mandatory Tour Generation Calibration Targets**:

```text
Work Tour Generation Targets:
1. Work Tour Participation Rates
   - Full-time worker participation rate by income and occupation
   - Part-time worker participation rate by demographics
   - Multiple job holder tour generation patterns
   - Telecommuting and flexible work arrangement impacts

2. Work Tour Frequency Distribution
   - Single daily work tour frequency by employment type
   - Multiple work tour patterns and characteristics
   - Work-related business tour generation rates
   - Lunch and mid-day work tour frequency patterns

School Tour Generation Targets:
1. Student Participation Rates by Age and Grade Level
   - Pre-school (ages 3-4): Participation rates and trip patterns
   - Elementary school (grades K-5): Mandatory attendance patterns
   - Middle school (grades 6-8): Independent travel emergence
   - High school (grades 9-12): Driving and independence patterns
   - University students: Campus-based and commuter patterns

2. School Choice and Location Patterns
   - Public school assignment and choice program participation
   - Private school enrollment and travel pattern differences
   - Charter and magnet school selection and accessibility
   - Higher education institution choice and commute patterns
```

### Non-Mandatory Activity Calibration Targets

**Individual Non-Mandatory Tour Targets**:

```bash
# Individual non-mandatory tour calibration data processing
process_nonmandatory_targets.py --activity_survey activity_data.csv --time_use_survey time_use.csv --validate_patterns

# Non-mandatory activity categories and target data:
# 1. Shopping tour generation and frequency by household characteristics
# 2. Social/recreational tour participation by age and lifestyle
# 3. Personal business and medical appointment tour patterns
# 4. Maintenance and service activity tour generation rates
```

**Joint Tour Generation Calibration Targets**:

```text
Joint Tour Target Specifications:
1. Household Joint Activity Participation
   - Joint shopping tour frequency by household type and income
   - Joint social/recreational activity participation patterns
   - Joint maintenance and service activity coordination
   - Joint eating out and entertainment activity patterns

2. Household Composition and Joint Activity Relationships
   - Adult-only household joint activity patterns
   - Households with children joint activity coordination
   - Multi-generational household activity patterns
   - Single-parent household constraint and coordination patterns

3. Geographic and Accessibility Influences
   - Joint activity participation by neighborhood characteristics
   - Joint tour generation by vehicle availability and parking
   - Joint activity patterns by transit accessibility and service
   - Joint tour destination choice and household coordination
```

**Stop Frequency and Location Calibration Targets**:

```text
Tour Complexity and Stop Pattern Targets:
1. Stop Frequency Distribution by Tour Type
   - Work tour stop frequency (no stops, outbound, inbound, both)
   - School tour stop frequency and intermediate activity patterns
   - Shopping tour complexity and multi-purpose trip patterns
   - Social/recreational tour stop frequency and coordination

2. Stop Purpose and Activity Type Distribution
   - Personal business stops on work tours by employment type
   - Shopping stops on non-shopping tours by household needs
   - Social stops and activity chaining by age and lifestyle
   - Service and maintenance stops by household characteristics

3. Stop Location Choice and Accessibility Patterns
   - Stop location relative to home and primary destination
   - Stop clustering and activity center utilization patterns
   - Stop location choice by mode and accessibility constraints
   - Stop timing and scheduling coordination with primary activities
```

## Mode Choice Calibration Target Data

### Tour-Level Mode Choice Targets

**Work Tour Mode Choice Calibration Data**:

```bash
# Work tour mode choice target data preparation and validation
prepare_work_mode_targets.py --commute_survey commute_data.csv --employer_survey employer_data.csv --validate_consistency

# Work mode choice target categories:
# 1. Drive alone mode share by income, occupation, and accessibility
# 2. Carpool mode share by household vehicle availability and coordination
# 3. Transit mode share by service level and employment density
# 4. Walk/bike mode share by distance and built environment
```

**Mode Choice Target Data Structure by Purpose**:

```text
Comprehensive Mode Choice Target Framework:
1. Work Tour Mode Choice Targets
   - Drive alone: Target share by income quartile and accessibility level
   - Carpool (2-person): Target share by household vehicle constraints
   - Carpool (3+ person): Target share by employment location and parking cost
   - Transit: Target share by service frequency and employment density
   - Walk: Target share by distance and pedestrian infrastructure
   - Bike: Target share by infrastructure and topographic conditions

2. School Tour Mode Choice Targets  
   - School bus: Target share by grade level and service availability
   - Drive alone (high school): Target share by license holding and vehicle access
   - Passenger (dropped off): Target share by age and household coordination
   - Transit: Target share by service availability and distance
   - Walk: Target share by distance and safety considerations
   - Bike: Target share by age, distance, and infrastructure

3. Non-Mandatory Tour Mode Choice Targets
   - Shopping tour mode choice by household vehicle availability and destination
   - Social/recreational mode choice by activity type and time of day
   - Personal business mode choice by trip urgency and accessibility
   - Maintenance/service mode choice by activity requirements and constraints
```

### Trip-Level Mode Choice Validation

**Stop-Level Mode Choice Consistency**:

```text
Trip Mode Choice Validation Framework:
1. Mode Consistency with Tour Mode
   - Drive alone tour with consistent trip modes
   - Transit tour with walk access/egress and transfer patterns
   - Carpool coordination and passenger pickup/drop-off patterns
   - Walk/bike tour consistency with intermediate stop accessibility

2. Mode Switching and Transfer Patterns
   - Park-and-ride usage and transfer behavior validation
   - Kiss-and-ride coordination and household scheduling
   - Multi-modal trip patterns and transfer time validation
   - Mode choice adaptation for intermediate stops and activities

3. Time-of-Day and Mode Choice Interactions
   - Peak period mode choice and capacity constraints
   - Off-peak mode choice and service level impacts
   - Evening and weekend mode choice pattern differences
   - Special event and non-routine activity mode choice patterns
```

## Destination Choice Calibration Targets

### Workplace and School Location Calibration

**Employment Location Choice Targets**:

```bash
# Employment location choice calibration target preparation
prepare_workplace_targets.py --lodes_data lodes_flows.csv --commute_survey commute_data.csv --validate_flows

# Workplace location target categories:
# 1. Commute flow matrices by industry and occupation type
# 2. Average commute distance and time by worker characteristics
# 3. Employment center market share by size and accessibility
# 4. Cross-county and jurisdiction commute flow patterns
```

**Workplace Location Target Specifications**:

```text
Employment Destination Choice Validation Data:
1. Origin-Destination Flow Matrices
   - Home-to-work flows by TAZ or county-level geography
   - Industry-specific commute patterns and employment clustering
   - Occupation-based commute distance and destination patterns
   - Income-based workplace location choice and accessibility patterns

2. Commute Distance and Time Distributions
   - Average commute distance by mode and residential location
   - Commute time distribution validation by employment sector
   - Long-distance commuting patterns and housing-job relationships
   - Reverse commuting and suburb-to-city employment flows

3. Employment Center Performance Validation
   - Major employment center capture rates and market share
   - Downtown and central city employment attraction validation
   - Suburban employment center performance and accessibility
   - Mixed-use development employment attraction and mode split
```

**School Location Choice Targets**:

```text
Educational Destination Choice Validation:
1. School Enrollment and Choice Patterns
   - Public school attendance area compliance and choice program utilization
   - Private school market share and geographic draw patterns
   - Charter and magnet school selection and transportation implications
   - Higher education institution enrollment and commute patterns

2. School Travel Distance and Mode Patterns
   - Elementary school travel distance distribution and school bus usage
   - Middle and high school independence and travel mode evolution
   - University student housing choice and campus accessibility patterns
   - Adult education and continuing education travel patterns

3. School Location Accessibility and Performance
   - Neighborhood school accessibility and enrollment validation
   - Regional school program draw patterns and transportation requirements
   - University and college regional draw and housing market impacts
   - Educational facility location and transportation system coordination
```

### Non-Mandatory Destination Choice Targets

**Shopping Destination Choice Calibration**:

```bash
# Shopping destination choice target data development
develop_shopping_targets.py --retail_survey retail_data.csv --credit_card_data spending_patterns.csv --validate_market_share

# Shopping destination target categories:
# 1. Retail center market share by size and accessibility level
# 2. Shopping trip distance distribution by goods type and frequency
# 3. Multi-purpose shopping trip patterns and activity coordination
# 4. Online shopping impact and physical retail destination choice
```

**Other Non-Mandatory Destination Choice Targets**:

```text
Comprehensive Non-Mandatory Destination Validation:
1. Personal Business and Service Destinations
   - Medical facility choice and healthcare accessibility patterns
   - Financial service location choice and convenience factors
   - Government service facility usage and accessibility validation
   - Personal service destination choice and frequency patterns

2. Social and Recreational Destination Choice
   - Social visit destination patterns and friendship network geography
   - Recreation facility choice by activity type and accessibility
   - Entertainment destination choice and evening activity patterns
   - Cultural and arts facility patronage and regional draw patterns

3. Maintenance and Service Activity Destinations
   - Vehicle maintenance and service location choice patterns
   - Household maintenance service provider choice and accessibility
   - Childcare and eldercare service location choice and coordination
   - Pet care and veterinary service destination choice patterns
```

## Time-of-Day Choice Calibration Targets

### Tour Scheduling and Time-of-Day Patterns

**Work Tour Time-of-Day Targets**:

```text
Work Tour Scheduling Validation Data:
1. Departure Time Distribution by Employment Type
   - Standard work schedule departure time distribution (7-9 AM peak)
   - Flexible work schedule departure time spreading and variability
   - Shift work departure time patterns and transportation implications
   - Compressed work week schedule impacts and peak period spreading

2. Duration and Return Time Patterns
   - Standard 8-hour work day duration and return time validation
   - Part-time work schedule duration and return time flexibility
   - Extended work day patterns and overtime transportation impacts
   - Lunch break return and mid-day activity coordination patterns

3. Work Tour Scheduling Constraints and Coordination
   - Household coordination constraints and joint scheduling decisions
   - Childcare schedule coordination and work tour timing impacts
   - Commute mode and schedule coordination (carpool timing, transit schedules)
   - Parking and facility access timing constraints and optimization
```

**School Tour Time-of-Day Calibration**:

```bash
# School tour time-of-day calibration target development
develop_school_tod_targets.py --school_schedules school_times.csv --student_survey student_data.csv --validate_timing

# School time-of-day target categories:
# 1. School start time distribution by grade level and district
# 2. Before-school and after-school activity participation and timing
# 3. School transportation coordination and household scheduling impacts
# 4. University class scheduling flexibility and transportation implications
```

### Non-Mandatory Activity Time-of-Day Targets

**Shopping and Personal Business Time-of-Day Patterns**:

```text
Non-Mandatory Activity Timing Validation:
1. Shopping Activity Time-of-Day Distribution
   - Grocery shopping timing patterns by household work schedules
   - Comparison shopping timing and weekend concentration patterns
   - Convenience shopping timing and daily routine integration
   - Seasonal and holiday shopping timing pattern variations

2. Personal Business and Service Timing
   - Medical appointment scheduling and weekday concentration
   - Financial service timing and business hour constraints
   - Government service facility usage timing and capacity constraints
   - Personal service appointment timing and coordination requirements

3. Social and Recreational Activity Timing
   - Social visit timing patterns and evening/weekend concentration
   - Recreation facility usage timing and peak capacity management
   - Entertainment activity timing and evening activity patterns
   - Cultural event timing and transportation coordination requirements
```

## Traffic Count and Transit Ridership Validation Data

### Highway Traffic Count Validation

**Traffic Count Data Collection and Processing**:

```bash
# Traffic count validation data preparation and quality control
prepare_traffic_counts.py --count_stations permanent_stations.csv --temp_counts temporary_counts.csv --validate_coverage

# Traffic count validation components:
# 1. Permanent count station annual average daily traffic (AADT)
# 2. Temporary count stations with seasonal and weekly adjustment factors
# 3. Screenline and cordon count summaries for area-wide validation
# 4. Vehicle classification counts for mode and vehicle type validation
```

**Traffic Count Validation Framework**:

```text
Comprehensive Traffic Count Validation:
1. Network Coverage and Representation
   - Interstate and freeway count coverage and validation
   - Arterial and collector road count coverage by functional class
   - Local road count sample representation and expansion procedures
   - Bridge and tunnel count validation and special facility representation

2. Temporal Pattern Validation
   - Peak hour and daily profile validation by facility type
   - Seasonal variation patterns and adjustment factor validation
   - Day-of-week patterns and weekend traffic validation
   - Special event and irregular pattern identification and treatment

3. Geographic Pattern and Flow Validation
   - Screenline crossing validation and flow balance checking
   - Cordon count validation and area-wide traffic generation/attraction
   - Inter-county and jurisdiction boundary flow validation
   - Major facility and bottleneck capacity utilization validation
```

### Transit Ridership and Performance Validation

**Transit System Performance Data**:

```bash
# Transit ridership validation data collection and processing
collect_transit_validation.py --gtfs_data gtfs_feeds/ --ridership_data operator_ridership.csv --performance_data service_metrics.csv

# Transit validation data categories:
# 1. Route-level ridership and passenger-mile validation
# 2. Stop-level boarding and alighting count validation
# 3. System-wide mode share and market penetration validation
# 4. Service performance and reliability validation metrics
```

**Transit Ridership Validation Specifications**:

```text
Transit System Validation Framework:
1. Ridership and Usage Pattern Validation
   - Route-level annual ridership and growth trend validation
   - Peak and off-peak ridership patterns and capacity utilization
   - Weekend and special event ridership pattern validation
   - Transfer patterns and system connectivity validation

2. Geographic Coverage and Market Share Validation
   - Service area population and employment coverage validation
   - Mode share validation by corridor and geographic area
   - Transit-dependent population service validation
   - Regional transit system coordination and transfer validation

3. Service Performance and Quality Validation
   - On-time performance and schedule adherence validation
   - Vehicle capacity utilization and crowding validation
   - System reliability and passenger experience validation
   - Accessibility compliance and special needs population service
```

## Validation Data Quality and Standards

### Data Collection Quality Assurance

**Survey Data Quality Standards**:

```text
Survey Data Quality Framework:
1. Sample Representativeness and Coverage
   - Demographic representativeness validation against population benchmarks
   - Geographic coverage validation and sample adequacy assessment
   - Response rate validation and non-response bias assessment
   - Weighting methodology validation and expansion factor accuracy

2. Data Collection Quality Control
   - Interviewer training and quality assurance procedures
   - Real-time data validation and error correction procedures
   - Response consistency checking and logical validation
   - GPS and location validation for travel survey data

3. Processing and Imputation Quality Assurance
   - Missing data imputation methodology validation
   - Trip purpose and mode classification validation procedures
   - Geographic coding accuracy and validation procedures
   - Outlier detection and treatment validation procedures
```

**Administrative Data Quality Validation**:

```bash
# Administrative data quality assurance and validation
validate_admin_data.py --traffic_counts --transit_data --employment_data --comprehensive_qa

# Administrative data quality components:
# 1. Data completeness and coverage validation procedures
# 2. Temporal consistency and trend validation procedures
# 3. Cross-source validation and reconciliation procedures
# 4. Accuracy assessment and uncertainty quantification procedures
```

This comprehensive calibration and validation data specification provides the foundation for systematic CT-RAMP model calibration and performance validation, ensuring model reliability and defensible transportation planning applications.