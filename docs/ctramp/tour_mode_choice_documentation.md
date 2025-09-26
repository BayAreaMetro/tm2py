# Tour Mode Choice Model - Comprehensive Documentation

## Overview

The Tour Mode Choice Model is one of the most critical components in CT-RAMP, determining the primary transportation mode for each tour. This model directly influences network loading, environmental impacts, and policy sensitivity of the travel demand system.

## Model Purpose

**Primary Function**: Select the transportation mode for each generated tour based on level-of-service, demographic characteristics, built environment, and policy variables.

**Key Decisions**:

- Primary transportation mode for entire tour
- Access/egress modes for transit tours  
- Vehicle occupancy for shared ride modes
- Parking strategy for auto modes
- Cost and time trade-offs between alternatives

## Behavioral Foundation

### Mode Choice Theory

**Utility Maximization**: Travelers choose the mode that provides the highest utility (satisfaction) given:

- **Level-of-Service**: Travel time, cost, reliability, comfort
- **Personal Preferences**: Past experience, cultural factors, lifestyle
- **Constraints**: Vehicle availability, physical ability, time budget
- **Built Environment**: Density, walkability, transit service quality

**Hierarchical Decision Structure**: Mode choice involves nested decisions:

1. **Motorized vs. Non-Motorized**: Basic mobility choice
2. **Auto vs. Transit**: Within motorized modes
3. **Specific Mode**: Detailed alternative selection

## Mode Alternatives Structure

CT-RAMP typically includes 17 mode alternatives organized hierarchically:

### Auto Modes (Alternatives 1-9)

**Drive Alone Modes**:

- **1**: Drive Alone - Free parking
- **2**: Drive Alone - Pay parking  
- **3**: Drive Alone - Park at transit for return trip

**Shared Ride 2 Person (HOV2)**:

- **4**: Shared Ride 2 - Free parking
- **5**: Shared Ride 2 - Pay parking
- **6**: Shared Ride 2 - Park at transit for return trip

**Shared Ride 3+ Person (HOV3+)**:

- **7**: Shared Ride 3+ - Free parking
- **8**: Shared Ride 3+ - Pay parking
- **9**: Shared Ride 3+ - Park at transit for return trip

### Non-Motorized Modes (Alternatives 10-11)

**Active Transportation**:

- **10**: Walk - All access by walking
- **11**: Bike - All access by bicycle

### Transit Modes (Alternatives 12-14)

**Public Transit**:

- **12**: Walk-Transit-Walk - Access/egress by walking
- **13**: Park & Ride - Drive to transit, transit to destination
- **14**: Kiss & Ride - Dropped off at transit by household member

### New Mobility Modes (Alternatives 15-17)

**On-Demand Transportation**:

- **15**: Taxi - Traditional taxi service
- **16**: Transportation Network Company (TNC) - Uber, Lyft, etc.
- **17**: School Bus - Specialized service for students

## Utility Specification Framework

### Level-of-Service Variables

**Time Components**:

```text
In_Vehicle_Time = Auto: driving time
                 Transit: time on transit vehicles
                 Walk/Bike: total travel time

Access_Time = Transit: walk/drive time to first boarding
             Auto: walk time from parking to destination

Wait_Time = Transit: initial wait + transfer wait times

Transfer_Time = Transit: walk time between transit vehicles
```

**Cost Components**:

```text
Auto_Operating_Cost = distance * cost_per_mile + tolls

Parking_Cost = hourly_rate * duration (for short trips)
              daily_rate (for long trips)
              
Transit_Fare = base_fare + distance_fare + transfer_penalties

TNC_Cost = base_fare + time_rate * total_time + 
           distance_rate * distance + surge_pricing
```

**Reliability and Comfort**:

```text
Reliability = Travel time standard deviation
             Service frequency (transit)
             Weather exposure (walk/bike)

Comfort = Crowding levels (transit)
         Bike infrastructure quality
         Walking environment safety
```

### Demographic and Household Variables

**Personal Characteristics**:

```text
Age_Effects = Different mode preferences by age group
             Senior discounts and accessibility needs
             Youth transit pass availability

Gender_Effects = Safety perceptions (especially transit, walk)
                Mode-specific preferences and constraints

Income_Effects = Cost sensitivity variations
                Vehicle quality and comfort preferences
                Transit subsidy eligibility
```

**Household Context**:

```text
Vehicle_Availability = Autos per licensed driver ratio
                      Vehicle type and condition effects
                      Parking availability at residence

Household_Structure = Joint travel coordination effects
                     Escort responsibilities
                     Activity participation patterns

Employment_Status = Parking subsidies and transit passes
                   Flexible scheduling options
                   Expense reimbursement policies
```

### Built Environment Variables

**Density and Mix**:

```text
Population_Density = Pedestrian activity levels
                    Transit service justification
                    Parking scarcity effects

Employment_Density = Activity clustering benefits
                    Competition for parking
                    Transit ridership support

Mixed_Use = Reduced travel needs
           Pedestrian-friendly environment
           Shorter trip distances
```

**Transportation Infrastructure**:

```text
Transit_Service = Route frequency and coverage
                 Stop/station accessibility and amenities
                 Service reliability and speed

Walking_Environment = Sidewalk coverage and condition
                     Intersection density and safety
                     Weather protection and lighting

Cycling_Infrastructure = Protected bike lane availability
                        Bike parking facilities
                        Network connectivity and safety
```

## Model Structure and Parameters

### Nested Logit Hierarchy

**Upper Level: Motorized vs. Non-Motorized**

```text
U_Motorized = ASC_Motorized + 
              lambda_1 * Logsum(Auto_Modes + Transit_Modes + TNC_Modes) +
              demographics_motorized +
              built_environment_motorized

U_NonMotorized = ASC_NonMotorized +
                lambda_2 * Logsum(Walk + Bike) +
                demographics_non_motorized +
                built_environment_non_motorized
```

**Lower Level: Specific Mode Utilities**

Auto Mode Utilities include drive alone, shared ride 2-person, and shared ride 3+ person alternatives with parking and route choice variations.

Transit Mode Utilities incorporate walk access, park-and-ride, and kiss-and-ride with full level-of-service representation.

Non-Motorized Utilities reflect infrastructure quality, safety, and environmental factors affecting walk and bike mode choice.

### Market Segmentation

**Purpose-Based Segmentation**:

- **Work Tours**: Emphasis on reliability, parking costs, employer incentives
- **School Tours**: Safety, specialized services (school bus), parent preferences  
- **Shopping Tours**: Convenience, cargo capacity, trip chaining efficiency
- **Social/Recreation Tours**: Flexibility, group travel, cost sharing
- **Personal Business**: Time efficiency, parking availability, accessibility

**Demographic Segmentation**:

- **By Income**: Low (high cost sensitivity), Middle (balanced trade-offs), High (time emphasis)
- **By Age**: Youth (transit familiarity), Adult (peak auto ownership), Senior (mobility limitations)
- **By Life Stage**: Single (individual preferences), Family (safety priorities), Empty Nester (comfort preferences)

## Data Requirements and Sources

### Level-of-Service Data

**Highway Skims**: Time, distance, cost, tolls by vehicle class and time period

**Transit Skims**: In-vehicle time, access/egress, wait times, transfers, fares, frequency

**Non-Motorized Skims**: Walk/bike time and distance, safety indices, infrastructure quality

### Demographic Data

**Person Attributes**: Age, gender, income, employment status, disabilities

**Household Context**: Vehicle ownership, residential location, household structure

**Revealed Preferences**: Past travel behavior, mode choice patterns

### Built Environment

**Zone Characteristics**: Density, land use mix, walkability, parking supply

**Transportation Infrastructure**: Transit service, pedestrian facilities, bike infrastructure

## Model Estimation and Calibration

### Estimation Data Sources

**Travel Surveys**:

- California Household Travel Survey (CHTS)
- Regional travel behavior surveys
- Transit on-board surveys
- Workplace travel surveys

**Revealed Preference Data**:

- Transit ridership counts by route and time period
- Traffic counts by vehicle occupancy
- Parking occupancy and pricing data
- Emerging mobility usage patterns

**Stated Preference Surveys**:

- Mode choice trade-off experiments
- Technology adoption preferences
- Policy response scenarios

### Calibration Targets by Purpose

**Work Tours**: Auto ~85%, Transit ~12%, Walk/Bike ~3%

**School Tours**: Auto ~65%, Transit ~15%, Walk/Bike ~15%, School Bus ~5%

**Shopping Tours**: Auto ~90%, Transit ~5%, Walk/Bike ~5%

**Social/Recreation**: Auto ~80%, Transit ~10%, Walk/Bike ~10%

### Geographic Variation Targets

**Urban Core**: Higher transit and non-motorized shares

**Suburban Areas**: Auto dominance with moderate transit usage

**Rural Areas**: Nearly complete auto dependence

## Model Outputs and Integration

### Direct Model Outputs

**Tour Mode Assignments**: Primary mode for each generated tour by person and purpose

**Mode Share Statistics**: Aggregate usage patterns by market segment and geography

**Accessibility Measures**: Mode-specific accessibility logsums for land use integration

**Policy Sensitivity Indicators**: Elasticity measures for infrastructure and pricing policies

### Network Model Integration

**Auto Assignment**: Vehicle trips by occupancy class for traffic assignment

**Transit Assignment**: Passenger loads by route, access mode, and time period

**Active Transportation**: Pedestrian and bicycle flows for infrastructure planning

### Policy Analysis Applications

**Transit Investment Evaluation**: Ridership response to service improvements

**Pricing Policy Assessment**: Behavioral response to tolls, parking fees, fare changes

**Land Use Policy Integration**: Mode choice sensitivity to development patterns

**Technology Impact Analysis**: Adoption and usage of new mobility services

## Model Validation and Performance

### Statistical Performance Measures

**Goodness of Fit**: Rho-squared statistics indicating model explanatory power

**Parameter Significance**: t-statistics and confidence intervals for coefficient estimates

**Market Share Accuracy**: Comparison between predicted and observed mode shares

**Elasticity Validation**: Realistic response to level-of-service and policy changes

### Behavioral Validation Criteria

**Demographic Reasonableness**: Realistic variations across population segments

**Geographic Consistency**: Appropriate urban/suburban/rural differences

**Temporal Stability**: Consistent behavior patterns across time periods

**Policy Sensitivity**: Reasonable response to transportation policies

### Implementation Considerations

**Computational Efficiency**: Execution time for large regional populations

**Numerical Stability**: Robust handling of extreme values and edge cases

**Convergence Properties**: Stability in iterative model feedback loops

**Model Maintainability**: Clear structure for parameter updates and enhancements

## Integration with Other CT-RAMP Components

### Upstream Dependencies

**Mandatory Tour Frequency**: Determines work and school tours requiring mode choice

**Auto Ownership Model**: Vehicle availability constraints on auto mode alternatives

**Coordinated Daily Activity Pattern**: Household coordination effects on mode choice

### Downstream Integration

**Tour Time-of-Day Model**: Mode choice logsums influence departure time preferences

**Stop Frequency Model**: Mode characteristics affect intermediate stop generation

**Trip Mode Choice Model**: Tour mode provides constraints and preferences for trip-level decisions

### Feedback Mechanisms

**Network Congestion**: Highway and transit level-of-service updates based on usage

**Parking Availability**: Dynamic parking costs and availability based on demand

**Mode-Specific Accessibility**: Land use model integration through accessibility measures

This comprehensive Tour Mode Choice Model serves as the central behavioral engine for transportation policy analysis in CT-RAMP, providing detailed representation of how travelers choose among available alternatives based on their circumstances and the transportation system characteristics.