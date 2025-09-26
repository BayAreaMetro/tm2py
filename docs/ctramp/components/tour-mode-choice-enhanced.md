# Tour Mode Choice Model

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
```
In_Vehicle_Time = Auto: driving time
                 Transit: time on transit vehicles
                 Walk/Bike: total travel time

Access_Time = Transit: walk/drive time to first boarding
             Auto: walk time from parking to destination

Wait_Time = Transit: initial wait + transfer wait times

Transfer_Time = Transit: walk time between transit vehicles
```

**Cost Components**:
```
Auto_Operating_Cost = distance * cost_per_mile + tolls

Parking_Cost = hourly_rate * duration (for short trips)
              daily_rate (for long trips)
              
Transit_Fare = base_fare + distance_fare + transfer_penalties

TNC_Cost = base_fare + time_rate * total_time + 
           distance_rate * distance + surge_pricing
```

**Reliability and Comfort**:
```
Reliability = Travel time standard deviation
             Service frequency (transit)
             Weather exposure (walk/bike)

Comfort = Crowding levels (transit)
         Bike infrastructure quality
         Walking environment safety
```

### Demographic and Household Variables

**Personal Characteristics**:
```
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
```
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
```
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
```
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

## Nested Logit Structure

### Upper Level: Motorized vs. Non-Motorized

**Motorized Branch Utility**:
```
U_Motorized = ASC_Motorized + 
              lambda_1 * Logsum(Auto_Modes + Transit_Modes + TNC_Modes) +
              demographics_motorized +
              built_environment_motorized
```

**Non-Motorized Branch Utility**:
```
U_NonMotorized = ASC_NonMotorized +
                lambda_2 * Logsum(Walk + Bike) +
                demographics_non_motorized +
                built_environment_non_motorized
```

### Lower Level: Specific Mode Choice

**Auto Mode Utilities**:
```
U_DriveAlone = ASC_DA +
               beta_time * (ivt + access_time) +
               beta_cost * (operating_cost + parking_cost) * income_factor +
               beta_reliability * reliability_index +
               demographic_interactions

U_SharedRide2 = ASC_SR2 +
               beta_time * (ivt + access_time) +
               beta_cost * (operating_cost + parking_cost) / 2 * income_factor +
               beta_social * household_size_factor +
               demographic_interactions

U_SharedRide3 = ASC_SR3 +
               beta_time * (ivt + access_time) +
               beta_cost * (operating_cost + parking_cost) / 3.5 * income_factor +
               beta_social * household_size_factor +
               demographic_interactions
```

**Transit Mode Utilities**:
```
U_WalkTransit = ASC_WT +
               beta_ivt * in_vehicle_time +
               beta_access * (initial_walk + final_walk) +
               beta_wait * (initial_wait + transfer_wait) +
               beta_transfer * number_of_transfers +
               beta_cost * total_fare * income_factor +
               beta_frequency * service_frequency +
               demographic_interactions +
               built_environment_effects

U_ParkRide = ASC_PNR +
            beta_ivt * (drive_time + in_vehicle_time) +
            beta_access * walk_to_station +
            beta_wait * (parking_search + initial_wait + transfer_wait) +
            beta_cost * (auto_cost + parking_cost + transit_fare) * income_factor +
            demographic_interactions +
            pnr_capacity_constraints
```

**Non-Motorized Utilities**:
```
U_Walk = ASC_Walk +
         beta_time * walk_time +
         beta_safety * pedestrian_safety_index +
         beta_weather * weather_exposure +
         demographic_interactions +
         built_environment_walkability

U_Bike = ASC_Bike +
         beta_time * bike_time +
         beta_safety * bike_safety_index +
         beta_infrastructure * bike_lane_quality +
         beta_topography * hilliness_index +
         demographic_interactions +
         built_environment_bike_friendly
```

## Market Segmentation

### Purpose-Based Segmentation

**Work Tours**: Emphasis on reliability, parking costs, employer incentives
**School Tours**: Safety, specialized services (school bus), parent preferences  
**Shopping Tours**: Convenience, cargo capacity, trip chaining efficiency
**Social/Recreation Tours**: Flexibility, group travel, cost sharing
**Personal Business**: Time efficiency, parking availability, accessibility

### Demographic Segmentation

**By Income**:
- **Low Income**: High cost sensitivity, transit subsidy eligibility
- **Middle Income**: Balanced time/cost trade-offs, parking concerns  
- **High Income**: Time emphasis, comfort preferences, TNC adoption

**By Age**:
- **Youth** (16-24): Transit familiarity, cost constraints, technology adoption
- **Adult** (25-64): Peak auto ownership, employment parking benefits
- **Senior** (65+): Fixed incomes, mobility limitations, transit discounts

**By Life Stage**:
- **Single**: Individual preferences, cost sensitivity
- **Family**: Safety priorities, escort responsibilities, vehicle sharing
- **Empty Nester**: Reduced constraints, comfort preferences

## Alternative Availability

### Vehicle Availability Constraints
```java
// Drive Alone available only if:
available[DRIVE_ALONE] = (household.getAutos() > 0) && 
                        (person.canDrive()) &&
                        (person.getAutoAvailability() > 0);

// Shared Ride available if:
available[SHARED_RIDE2] = (household.getAutos() > 0) || 
                         (person.hasRideShareAccess());
```

### Distance and Time Constraints
```java
// Walk available for reasonable distances:
available[WALK] = (walkDistance <= maxWalkDistance) &&
                 (walkTime <= maxWalkTime) &&
                 (person.canWalk());

// Bike available with infrastructure and ability:
available[BIKE] = (bikeDistance <= maxBikeDistance) &&
                 (bikeInfrastructure >= minBikeInfra) &&
                 (person.canBike()) &&
                 (person.getAge() >= minBikeAge);
```

### Service Availability
```java
// Transit available with reasonable access:
available[WALK_TRANSIT] = (walkToTransit <= maxWalkToTransit) &&
                         (transitService.exists()) &&
                         (serviceFrequency >= minFrequency);

// TNC available in service area:
available[TNC] = tncServiceArea.contains(origin) &&
                tncServiceArea.contains(destination) &&
                (person.hasSmartphone());
```

## Data Requirements

### Level-of-Service Matrices

**Highway Skims**:
- Time by vehicle class and time period
- Distance and operating costs  
- Tolls and parking costs
- Reliability measures

**Transit Skims**:
- In-vehicle time by mode
- Access and egress times
- Wait times and number of transfers
- Fares and service frequency
- Crowding and reliability measures

**Non-Motorized Skims**:
- Walk time and distance
- Bike time and distance
- Safety and comfort indices
- Infrastructure quality measures

### Person and Household Attributes

**Demographics**: Age, gender, income, employment status
**Vehicle Information**: Number, type, availability
**Location**: Residential zone, workplace, school locations
**Preferences**: Stated preferences, revealed behavior patterns

### Built Environment Data

**Zone Characteristics**: Density, land use mix, walkability scores
**Transportation Supply**: Transit service levels, parking supply/cost
**Infrastructure Quality**: Sidewalk coverage, bike facilities, safety measures

## Model Estimation and Calibration

### Data Sources for Estimation

**Travel Surveys**: 
- California Household Travel Survey (CHTS)
- Bay Area Travel Survey (BATS)  
- Transit on-board surveys
- Workplace travel surveys

**Revealed Preference Data**:
- Transit ridership counts
- Traffic counts by vehicle class
- Parking occupancy data
- TNC usage patterns

**Stated Preference Surveys**:
- Mode choice trade-offs
- New technology adoption
- Policy response preferences

### Calibration Targets

**Mode Shares by Purpose**:
- Work: Auto ~85%, Transit ~12%, Walk/Bike ~3%
- School: Auto ~65%, Transit ~15%, Walk/Bike ~15%, School Bus ~5%
- Shopping: Auto ~90%, Transit ~5%, Walk/Bike ~5%
- Social: Auto ~80%, Transit ~10%, Walk/Bike ~10%

**Demographic Variations**:
- Income-based mode choice patterns
- Age-related preferences and constraints
- Household structure effects on sharing

**Geographic Patterns**:
- Urban core: Higher transit and walk shares
- Suburban: Auto dominance with some transit
- Rural: Nearly complete auto dependence

## Model Outputs and Applications

### Direct Outputs

**Tour Mode Assignments**: Primary mode for each generated tour
**Mode Share Statistics**: Aggregate mode usage by market segment
**Accessibility Measures**: Mode-specific accessibility logsums
**Policy Impact Indicators**: Sensitivity to infrastructure and pricing changes

### Integration with Network Models

**Auto Assignment**: Vehicle trips by occupancy level and route choice
**Transit Assignment**: Passenger loads by route, time period, and access mode
**Active Transportation**: Pedestrian and bicycle flows for infrastructure planning

### Policy Applications

**Transit Investment Analysis**: Ridership impacts of service improvements
**Pricing Policy Evaluation**: Response to tolls, parking fees, transit fares
**Land Use Policy Integration**: Mode choice response to density and mixed-use development
**New Technology Assessment**: Adoption rates for autonomous vehicles, shared mobility

## Model Validation and Performance

### Statistical Validation

**Goodness of Fit**: Rho-squared, log-likelihood statistics
**Parameter Significance**: t-statistics, confidence intervals  
**Market Share Predictions**: Comparison to observed mode shares
**Elasticity Validation**: Response to level-of-service changes

### Behavioral Validation

**Segmentation Effects**: Realistic demographic and geographic variations
**Policy Sensitivity**: Reasonable response to policy changes
**Temporal Stability**: Consistent behavior across time periods
**Cross-Validation**: Performance on holdout datasets

### Implementation Considerations

**Computational Performance**: Execution time for large populations
**Numerical Stability**: Robust handling of extreme values
**Convergence Properties**: Stability in iterative feedback processes
**Maintainability**: Clear structure for updates and modifications

This comprehensive Tour Mode Choice Model forms the backbone of transportation policy analysis in CT-RAMP, providing detailed behavioral representation of how individuals choose among available transportation alternatives based on their personal circumstances, trip characteristics, and the built environment context.