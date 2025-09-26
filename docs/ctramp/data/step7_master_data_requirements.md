# CT-RAMP Master Data Requirements and Specifications

## Overview

This document provides comprehensive specifications for all data inputs required for CT-RAMP model implementation, calibration, and operation. It serves as the authoritative reference for data collection, preparation, and validation procedures supporting the complete CT-RAMP modeling framework.

## Data Architecture and Framework

### Core Data Categories

**Population and Demographic Data**:

- Household and person synthetic population databases
- Demographic control totals and marginal distributions
- Economic and employment characteristics by geography
- Special population groups (students, military, group quarters)

**Land Use and Built Environment Data**:

- Transportation Analysis Zone (TAZ) and Microzone (MAZ) definitions
- Employment by industry category and geographic location
- Residential units by type, tenure, and accessibility characteristics
- Activity location data (schools, shopping, services, recreation)

**Transportation Network Data**:

- Highway network topology, capacity, and operating characteristics
- Transit system routes, schedules, fares, and accessibility
- Active transportation infrastructure (walking, cycling networks)
- Transportation facility characteristics and level-of-service data

**Behavioral and Calibration Data**:

- Travel survey data for model estimation and validation
- Traffic counts and transit ridership for calibration targets
- Economic data for cost and time value specifications
- Policy and regulatory framework parameters

## Detailed Data Specifications

### Population Synthesis Data Requirements

**Household Data Structure**:

```text
Required Household Attributes:
- Household ID (unique identifier linking to persons)
- Geographic location (TAZ, MAZ coordinates)
- Household size (number of persons)
- Household composition (adults, children by age groups)
- Income category (detailed income ranges for market segmentation)
- Dwelling unit type (single family, multifamily, other)
- Tenure (own/rent) and housing costs
- Vehicle availability and parking characteristics

Data Format Specifications:
- File format: CSV with UTF-8 encoding
- Numeric precision: Income in dollars, coordinates in decimal degrees
- Missing value coding: Use -999 for missing numeric, blank for missing text
- Validation rules: All households must have valid TAZ/MAZ assignments
```

**Person Data Structure**:

```text
Required Person Attributes:
- Person ID (unique identifier within household)
- Household ID (foreign key linking to household record)
- Age (years, used for lifecycle and activity constraints)
- Gender (male/female/other for demographic segmentation)
- Employment status (full-time, part-time, unemployed, retired, student)
- Occupation category (management, service, production, etc.)
- Industry classification (NAICS codes for employment location modeling)
- Student status (grade level for mandatory activity generation)
- Driver license status and transportation mobility characteristics

Cross-Reference Validation:
- All persons must belong to valid households
- Household size must match person count
- Age-grade enrollment relationships must be consistent
- Employment persons must have valid occupation/industry codes
```

**Control Data and Marginal Totals**:

```bash
# Population control validation and consistency
validate_population_controls.py --households household_controls.csv --persons person_controls.csv --geography TAZ

# Control total categories:
# 1. Household controls by size, income, and dwelling type
# 2. Person controls by age, gender, and employment status  
# 3. Geographic controls ensuring spatial consistency
# 4. Cross-tabulation controls for joint distributions
```

### Land Use Data Specifications

**Transportation Analysis Zone (TAZ) Data**:

```text
TAZ Attribute Requirements:
- TAZ identifier (unique integer ID for geographic reference)
- Area measurements (total area, developable area in acres)
- Population (households and persons by demographic category)
- Employment (jobs by industry category and wage level)
- Land use mix (residential, commercial, industrial percentages)
- Built environment characteristics (density, design, diversity measures)
- Accessibility indicators (transit service level, highway accessibility)

Employment Industry Categories:
- Agriculture, Natural Resources, Mining (NAICS 11, 21)
- Construction (NAICS 23)
- Manufacturing (NAICS 31-33)
- Wholesale Trade (NAICS 42)
- Retail Trade (NAICS 44-45)
- Transportation, Warehousing, Utilities (NAICS 22, 48-49)
- Information (NAICS 51)
- Financial Activities (NAICS 52-53)
- Professional, Business Services (NAICS 54-56)
- Educational Services (NAICS 61)
- Health Care, Social Assistance (NAICS 62)
- Arts, Entertainment, Recreation, Accommodation, Food Services (NAICS 71-72)
- Other Services (NAICS 81)
- Public Administration (NAICS 92)
```

**Microzone (MAZ) Data Structure**:

```text
MAZ Detailed Specifications:
- MAZ identifier (unique ID within TAZ hierarchy)
- Parent TAZ assignment (hierarchical geographic relationship)
- Land use type (residential, employment, mixed-use, special)
- Walk accessibility measures (pedestrian infrastructure quality)
- Transit accessibility (distance to transit stops, service frequency)
- Parking characteristics (supply, cost, restrictions)
- Activity opportunities (retail, services, recreation within walking distance)

Built Environment Indicators:
- Intersection density (4-way intersections per square mile)
- Retail employment within 0.5 miles of MAZ centroid
- Household density (housing units per residential acre)
- Employment density (jobs per commercial acre)
- Land use diversity (entropy measure of mixed-use development)
```

### Transportation Network Data

**Highway Network Specifications**:

```bash
# Highway network data validation and processing
validate_highway_network.py --network highway_links.csv --nodes highway_nodes.csv --validate_topology

# Required link attributes:
# - Link ID (unique identifier for network topology)
# - From/To node IDs (network connectivity definition)
# - Facility type (freeway, arterial, collector, local)
# - Number of lanes and capacity (vehicles per hour)
# - Free flow speed and speed-capacity relationships
# - Length (miles) and geometric characteristics
# - Time-of-day capacity variations and restrictions
```

**Highway Link Data Structure**:

```text
Highway Link Attribute Requirements:
- Link identifier and directional coding
- Functional classification (Interstate, arterial, collector, local)
- Capacity and operational characteristics by time period
- Speed limits and congested speed-flow relationships
- Facility restrictions (HOV lanes, truck restrictions, tolls)
- Geometric characteristics (number of lanes, median type)

Time-of-Day Variations:
- AM Peak (6:00-10:00): Capacity and speed adjustments
- Midday (10:00-15:00): Base capacity and free-flow conditions  
- PM Peak (15:00-19:00): Capacity and directional flow adjustments
- Evening/Night (19:00-6:00): Capacity and speed characteristics
```

**Transit Network Data**:

```text
Transit System Components:
1. Route Definitions
   - Route identifier and service type (bus, rail, BRT)
   - Route alignment and stop sequence
   - Service frequency by time period and day type
   - Fare structure and payment systems

2. Stop and Station Data
   - Stop identifier and geographic coordinates
   - Stop characteristics (shelter, accessibility, parking)
   - Transfer opportunities and connection times
   - Catchment area and access characteristics

3. Service Patterns
   - Headways and schedule reliability by route and time period
   - Route coverage (span of service, days of operation)
   - Vehicle characteristics (capacity, speed, accessibility features)
   - Fare integration and transfer policies
```

### Behavioral Data and Calibration Targets

**Travel Survey Data Requirements**:

```bash
# Travel survey data processing and validation
process_travel_survey.py --households survey_hh.csv --persons survey_per.csv --trips survey_trips.csv

# Survey data components:
# 1. Household characteristics matching synthetic population structure
# 2. Person demographics and employment/student status
# 3. Trip records with origin, destination, mode, purpose, time
# 4. Activity participation and time use patterns
# 5. Stated preference data for mode choice model estimation
```

**Trip and Activity Data Structure**:

```text
Trip Record Specifications:
- Trip identifier (unique within person-day)
- Person and household identifiers (linking to demographics)
- Origin and destination locations (TAZ, coordinates, address)
- Trip purpose (work, school, shopping, social, recreation, other)
- Mode of transportation (drive alone, carpool, transit, walk, bike, other)
- Departure and arrival times (24-hour format)
- Trip distance and duration
- Activity duration at destination
- Trip cost and parking information

Activity Pattern Requirements:
- Daily activity schedules and time allocation
- Joint activities and household coordination
- Activity location choices and accessibility factors
- Mode availability and choice constraints
- Time-of-day preferences and scheduling constraints
```

**Calibration Target Data Sources**:

```text
Traffic Count Data:
- Permanent and temporary count stations
- Average daily traffic (ADT) and peak hour volumes
- Seasonal and day-of-week variation factors
- Vehicle classification and occupancy data
- Screenline and cordon count summaries

Transit Ridership Data:
- Route-level boardings and passenger miles
- Stop-level boarding and alighting counts
- Transfer patterns and system connectivity measures
- Fare revenue and passenger trip characteristics
- System performance indicators (on-time performance, capacity utilization)

Travel Time and Level-of-Service Data:
- Floating car studies and GPS probe data
- Travel time reliability and congestion patterns
- Intersection delay and signal coordination effects
- Transit schedule adherence and passenger experience measures
```

## Data Quality Standards and Validation

### Quality Assurance Framework

**Data Completeness Requirements**:

```bash
# Comprehensive data completeness validation
validate_data_completeness.py --check_coverage --check_consistency --generate_report

# Completeness criteria:
# 1. Geographic Coverage: All TAZs have required data elements
# 2. Temporal Coverage: Data represents target analysis year
# 3. Demographic Coverage: All population segments represented
# 4. Network Coverage: Complete connectivity and attribute specification
```

**Data Accuracy and Consistency Validation**:

```text
Validation Procedure Categories:
1. Internal Consistency Checks
   - Cross-tabulation totals match marginal distributions
   - Geographic aggregations consistent across data sources
   - Temporal patterns consistent with known behavior
   - Network topology and routing validation

2. External Benchmark Validation
   - Population totals match official demographic projections
   - Employment totals consistent with economic forecasts
   - Network characteristics match observed conditions
   - Travel patterns consistent with regional surveys

3. Logical Relationship Validation
   - Age-school enrollment relationships
   - Employment-commute pattern consistency
   - Housing unit-household matching
   - Network capacity-demand relationship reasonableness
```

### Data Integration and Cross-Validation

**Multi-Source Data Integration**:

```bash
# Integrate multiple data sources with validation
integrate_data_sources.py --population census_data.csv --employment bls_data.csv --network osm_data.xml --validate_integration

# Integration validation steps:
# 1. Geographic coordinate system consistency
# 2. Time period and reference year alignment
# 3. Classification system harmonization
# 4. Scale and unit measurement consistency
```

**Temporal Consistency and Updates**:

```text
Data Currency Management:
1. Reference Year Standardization
   - Establish base year for all data sources
   - Document vintage and collection methodology
   - Implement interpolation/extrapolation procedures for missing years

2. Forecast Year Projections
   - Population and employment growth assumptions
   - Land use development and policy implementation timelines
   - Transportation infrastructure improvement schedules
   - Economic and demographic trend projections

3. Update and Maintenance Schedules
   - Annual updates for demographic and employment data
   - Transportation network updates following major projects
   - Calibration data refresh cycle and validation procedures
   - Version control and change documentation requirements
```

## Technical Implementation Standards

### Data Format and Storage Specifications

**File Format Standards**:

```text
Standardized Data Formats:
1. Tabular Data: CSV files with UTF-8 encoding
   - Header row with descriptive variable names
   - Consistent missing value coding (-999 for numeric, blank for text)
   - Date formats in ISO 8601 standard (YYYY-MM-DD)
   - Coordinate reference system documentation

2. Spatial Data: GIS shapefiles or GeoJSON format
   - Consistent coordinate reference system (State Plane or UTM)
   - Required attribute fields documented and validated
   - Topology validation for polygon and network data
   - Metadata documentation for projection and datum

3. Network Data: XML or standardized network format
   - Node-link topology with validation rules
   - Attribute specifications with units and ranges
   - Time-dependent attribute handling procedures
   - Quality assurance and error checking protocols
```

**Database Organization and Naming Conventions**:

```bash
# Standardized data organization and file naming
organize_model_data.py --base_dir model_inputs --year 2045 --scenario baseline

# Directory structure standards:
# /model_inputs/
#   /demographics/households_2045_baseline.csv
#   /demographics/persons_2045_baseline.csv
#   /land_use/taz_data_2045_baseline.csv
#   /land_use/maz_data_2045_baseline.csv
#   /networks/highway_network_2045_baseline.xml
#   /networks/transit_routes_2045_baseline.csv
#   /calibration/travel_survey_processed.csv
#   /validation/traffic_counts_target.csv
```

### Version Control and Documentation

**Data Lineage and Provenance**:

```text
Documentation Requirements:
1. Source Data Documentation
   - Original data source and collection methodology
   - Processing steps and transformation procedures
   - Quality control and validation results
   - Known limitations and uncertainty estimates

2. Processing History
   - Software tools and versions used for processing
   - Parameter settings and configuration files
   - Intermediate processing steps and validation checks
   - Final output validation and acceptance criteria

3. Change Management
   - Version numbering system and change logs
   - Approval process for data updates and modifications
   - Impact assessment for downstream model components
   - Rollback procedures and backup maintenance
```

This comprehensive data requirements specification provides the foundation for reliable CT-RAMP model implementation, ensuring data quality, consistency, and traceability throughout the modeling process.