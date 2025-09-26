# Tour Time-of-Day Choice Model

## Overview

The Tour Time-of-Day Choice Model determines departure and arrival times for each generated tour, balancing individual preferences, activity constraints, and transportation system conditions. This model is critical for understanding peak period travel demand, congestion patterns, and the temporal distribution of activities throughout the day.

## Model Purpose

**Primary Function**: Select optimal departure and arrival times for tours based on activity requirements, personal preferences, household coordination, and transportation level-of-service.

**Key Decisions**:

- Tour departure time from home
- Activity start time at destination  
- Activity duration and end time
- Return departure time from activity location
- Arrival time back at home
- Coordination with other household member schedules

## Behavioral Foundation

### Time-of-Day Preference Theory

**Utility-Based Timing Decisions**: Individuals choose departure times to maximize utility considering:

- **Activity Timing Preferences**: Preferred times for specific activities
- **Transportation Level-of-Service**: Travel time, congestion, transit frequency  
- **Schedule Coordination**: Work hours, household obligations, appointment times
- **Personal Chronotype**: Individual circadian rhythm and energy patterns
- **Social and Cultural Norms**: Standard business hours, meal times, social conventions

**Time Budget Optimization**:

```text
Total_Daily_Time = Sleep + Work/School + Household_Obligations + 
                  Personal_Care + Travel + Discretionary_Activities

Temporal_Constraints = Fixed appointment times +
                      Coordination requirements +
                      Service/facility operating hours +
                      Transportation service availability
```

### Activity-Specific Timing Requirements

**Work Tours**:

- **Fixed Start Times**: Most employees have required arrival times
- **Flexible Margins**: Some flexibility around core hours
- **Commute Duration Consideration**: Departure time accounts for expected travel time
- **Schedule Reliability**: Buffer time for congestion and delays

**School Tours**:

- **Rigid Schedule Requirements**: Fixed class start times
- **Age-Based Variations**: Elementary vs. high school timing differences
- **Transportation Dependencies**: School bus schedules, parent escort timing
- **Activity Coordination**: Before/after school programs and activities

**Shopping Tours**:

- **Store Operating Hours**: Retail and service availability windows
- **Crowd Avoidance**: Preference for less busy periods
- **Activity Chaining**: Coordination with other stops and activities
- **Parking Availability**: Time-of-day variations in parking access

**Social/Recreation Tours**:

- **Event-Specific Timing**: Scheduled entertainment, sports, social events
- **Social Coordination**: Meeting times with friends and family
- **Optimal Activity Conditions**: Weather, lighting, facility availability
- **Personal Energy Patterns**: Individual preferences for recreation timing

## Time Period Structure

### Temporal Discretization

CT-RAMP typically uses 30 time periods representing different departure time windows:

**Early Morning Periods** (5:00-7:00 AM):
- **5:00-5:30**: Very early shift workers, airport trips
- **5:30-6:00**: Early shift workers, commuter transit
- **6:00-6:30**: Standard early commuters
- **6:30-7:00**: Main early commute period

**Morning Peak** (7:00-9:00 AM):
- **7:00-7:30**: Peak commute period beginning  
- **7:30-8:00**: Maximum morning congestion
- **8:00-8:30**: Late commuters, school trips
- **8:30-9:00**: Flexible schedule workers, business trips

**Mid-Day Off-Peak** (9:00 AM-3:00 PM):
- **9:00-12:00**: Business meetings, personal business, shopping
- **12:00-1:00**: Lunch-time activities
- **1:00-3:00**: Continued off-peak activities, early school pickup

**Afternoon/Evening Peak** (3:00-7:00 PM):
- **3:00-4:00**: School pickup, early evening activities
- **4:00-5:00**: Beginning of evening commute
- **5:00-6:00**: Peak evening commute period
- **6:00-7:00**: Late evening commute, dinner activities

**Evening Off-Peak** (7:00 PM-11:00 PM):
- **7:00-9:00**: Dinner, evening shopping, social activities
- **9:00-11:00**: Entertainment, late social activities

**Late Night** (11:00 PM-5:00 AM):
- **11:00 PM-5:00 AM**: Limited activities, shift workers, special events

## Utility Specification Framework

### Core Time-of-Day Utility Structure

**Departure Time Utility**:

```text
U_Departure_t = ASC_period_t +
               beta_early * early_departure_penalty +
               beta_late * late_departure_penalty +  
               beta_travel_time * expected_travel_time_t +
               beta_schedule_delay * |preferred_arrival - actual_arrival| +
               beta_congestion * congestion_level_t +
               beta_parking_cost * parking_cost_t +
               beta_transit_frequency * transit_frequency_t +
               demographic_interactions +
               activity_type_interactions
```

### Level-of-Service Variables by Time Period

**Highway Congestion Effects**:

```text
Travel_Time_t = Free_Flow_Time * Congestion_Factor_t

Congestion_Factor_Peak = 1.8-2.5 (80-150% longer than free-flow)
Congestion_Factor_Off_Peak = 1.1-1.3 (10-30% longer than free-flow)

Reliability_t = Standard_Deviation(Travel_Time_t)
              Higher during peak periods due to incident risk
              Weather and construction impacts
```

**Transit Service Variations**:

```text
Frequency_t = Vehicles per hour by time period
             Peak: 15-minute headways or better  
             Off-Peak: 30-60 minute headways
             Evening: Reduced service levels

Crowding_t = Passenger load factor by time period
            Peak periods: Standing room, passenger discomfort
            Off-peak: Comfortable seating availability
```

**Parking Cost and Availability**:

```text
Parking_Cost_t = Hourly rates varying by time and location
                Peak periods: Maximum rates, limited availability
                Off-peak: Lower rates, abundant availability

Parking_Search_t = Expected search time for parking space
                  Peak: Extended search in congested areas
                  Off-peak: Immediate availability
```

### Activity-Specific Timing Preferences

**Work Activity Timing**:

```text
U_Work_Arrival = beta_work_start * preferred_work_start_time +
                beta_flexibility * work_schedule_flexibility +
                beta_commute_stress * commute_difficulty_factor +
                beta_coordination * household_departure_coordination

Preferred times often cluster around:
- 8:00 AM: Standard business hours
- 9:00 AM: Professional and office jobs
- 7:00 AM: Manufacturing and service jobs
- Variable: Shift workers and flexible schedules
```

**Shopping Activity Timing**:

```text
U_Shopping_Start = beta_store_hours * store_availability +
                  beta_crowds * crowd_avoidance_preference +
                  beta_coordination * household_schedule_fit +
                  beta_chaining * multi_purpose_efficiency

Preferred patterns:
- Weekend mornings: Family shopping, fresh products
- Weekday mid-day: Convenience, shorter lines  
- Evenings: After work, extended store hours
- Avoid: Lunch rush, commute periods
```

**Social/Recreation Timing**:

```text
U_Social_Start = beta_event_time * scheduled_event_timing +
               beta_social_norms * conventional_activity_times +
               beta_energy * personal_energy_patterns +
               beta_coordination * social_group_preferences

Common patterns:
- Fitness: Early morning or evening after work
- Dining: Standard meal times with social conventions
- Entertainment: Evening and weekend time slots  
- Sports: Seasonal and daylight considerations
```

## Demographic and Personal Variations

### Age-Based Timing Preferences

**Young Adults (18-34)**:

```text
Characteristics:
- Later preferred activity times (night owl tendency)
- Flexible work schedules more common
- Active social lives requiring evening coordination
- Technology use enabling flexible scheduling

Timing Patterns:
- Work: Flexible arrival times when possible
- Social: Late afternoon and evening concentration
- Shopping: Evening and weekend convenience focus
- Recreation: After work and weekend emphasis
```

**Middle-Aged Adults (35-54)**:

```text
Characteristics:  
- Family coordination responsibilities
- Peak career demands with fixed schedules
- Efficiency focus due to time constraints
- Health and wellness activity integration

Timing Patterns:
- Work: Standard business hours, reliable schedules
- Family: Coordination around children's schedules
- Personal: Early morning or late evening for individual activities
- Shopping: Weekend efficiency, online alternatives
```

**Older Adults (55+)**:

```text
Characteristics:
- Flexibility with retirement or reduced work
- Health considerations affecting activity timing
- Established routines and preferences  
- Congestion and crowd avoidance preferences

Timing Patterns:
- Activities: Mid-day off-peak preference
- Medical: Business hours, appointment scheduling
- Social: Traditional timing patterns, early evening
- Shopping: Off-peak hours, avoiding crowds and traffic
```

### Employment Status and Schedule Flexibility

**Full-Time Standard Hours**:

- **Constraints**: Fixed departure and arrival times for work
- **Activity Windows**: Early morning, lunch break, after work, weekends
- **Travel Timing**: Peak period commuting, off-peak discretionary travel
- **Coordination**: Limited flexibility requiring advance planning

**Flexible Work Arrangements**:

- **Opportunities**: Core hours with flexible arrival/departure
- **Activity Integration**: Mid-day activities possible with schedule adjustment
- **Commute Timing**: Potential for off-peak travel to avoid congestion
- **Work-Life Balance**: Better integration of personal activities

**Shift Workers**:

- **Non-Standard Patterns**: Travel during off-peak periods
- **Activity Constraints**: Limited by business and service hours
- **Family Coordination**: Challenges with standard household schedules
- **Transportation Service**: Potential gaps in transit service availability

## Household Coordination Effects

### Joint Activity Timing

**Household Scheduling Coordination**:

```text
Joint_Departure_Utility = beta_coordination * number_joint_participants +
                         beta_vehicle_sharing * shared_vehicle_efficiency +
                         beta_activity_overlap * synchronized_activity_benefits +
                         beta_conflict_avoidance * schedule_conflict_penalties

Coordination Benefits:
- Shared transportation costs and vehicle use
- Family time and social interaction during travel
- Coordinated activity participation and timing
- Reduced household scheduling conflicts
```

**Sequential Activity Dependencies**:

```text
Household_Sequence = Escort responsibilities for children +
                    Vehicle sharing requiring coordination +
                    Meal preparation and household maintenance +
                    Caregiver responsibilities for elderly/disabled members

Time Dependencies:
- School drop-off/pickup constraining parent schedules
- Vehicle handoff requiring temporal coordination
- Meal times affecting individual activity scheduling
- Elder care affecting caregiver activity timing
```

## Model Implementation and Structure

### Discrete Choice Framework

**Multinomial Logit Structure**:

```text
P(departure_time_t) = exp(U_t) / Σ[exp(U_s)] for all time periods s

Where U_t includes:
- Time period-specific constants
- Level-of-service variables by time period
- Activity-specific timing preferences
- Demographic interaction effects
- Schedule coordination requirements
```

**Alternative Availability**:

```text
Available time periods depend on:
- Activity operating hours and constraints
- Transportation service availability  
- Household coordination requirements
- Personal schedule conflicts and commitments
- Minimum activity duration requirements
```

### Integration with Mode Choice

**Joint Mode-Time Choice**:

Time-of-day preferences interact with mode choice through level-of-service variables:

- **Auto Travel Times**: Congestion varies significantly by departure time
- **Transit Service**: Frequency and reliability peak during commute periods
- **Non-Motorized**: Safety and comfort considerations vary by time of day
- **Parking**: Cost and availability strongly time-dependent

**Logsums from Mode Choice**:

```text
Mode_Accessibility_t = log(Σ[exp(U_mode_m,t)]) for modes m in time period t

This accessibility measure captures:
- Transportation option quality by time period
- Mode-specific congestion and service level effects
- Cost variations by time of day
- Integration with land use accessibility
```

## Data Requirements and Calibration

### Data Sources for Model Estimation

**Activity-Based Travel Surveys**:

- Departure and arrival times for all trips and tours
- Activity start times, durations, and scheduling flexibility
- Stated preferences for timing trade-offs and constraints
- Household coordination patterns and dependencies

**GPS and Smartphone Data**:

- Revealed timing patterns at high temporal resolution
- Route choice and congestion experience by departure time  
- Activity duration and timing variations
- Real-world scheduling behavior and adaptation

### Calibration Targets

**Departure Time Distribution by Purpose**:

```text
Work Tours:
- 6:00-7:00 AM: 15% of departures
- 7:00-8:00 AM: 35% of departures  
- 8:00-9:00 AM: 25% of departures
- 9:00+ AM: 25% of departures (flexible schedules)

Shopping Tours:
- Morning (9-12): 30% of departures
- Afternoon (12-5): 35% of departures
- Evening (5-8): 25% of departures  
- Weekend concentration: 60% of total shopping tours

Social/Recreation:
- Weekday Evening (5-9): 40% of departures
- Weekend Afternoon (12-6): 35% of departures  
- Weekend Evening (6-10): 25% of departures
```

**Peak Period Loading**:

- Morning Peak (7-9 AM): 40% of work tours, 15% of total tours
- Evening Peak (5-7 PM): 35% of work tours, 20% of total tours  
- Off-Peak Periods: 85% of non-work tours distributed across day

### Model Validation

**Temporal Distribution Accuracy**:

- Departure time distributions matching observed survey data
- Peak period concentration appropriate for activity purposes
- Realistic response to congestion and level-of-service changes
- Seasonal and day-of-week variations consistent with behavior

**Policy Sensitivity**:

- Reasonable response to congestion pricing and time-based tolls
- Realistic shift patterns with flexible work hour policies
- Appropriate sensitivity to transit service frequency changes
- Behavioral response to parking pricing variations by time period

## Policy Applications

### Congestion Management

**Time-Based Pricing Strategies**:

- Peak period tolls encouraging departure time shifts
- Variable parking pricing reducing peak period demand
- Transit fare incentives for off-peak travel
- Employer-based flexible schedule incentives

**Infrastructure Capacity Planning**:

- Peak period demand forecasting for capacity expansion
- Off-peak capacity utilization and efficiency analysis  
- Transit service planning based on temporal demand patterns
- Parking facility sizing and pricing optimization

### Activity and Land Use Planning

**Activity Center Design**:

- Operating hour policies affecting peak period concentration
- Mixed-use development spreading activity timing
- Destination clustering effects on travel time distributions
- Service provision matching temporal activity patterns

### Transportation Demand Management

**Schedule-Based TDM Strategies**:

- Flexible work hour programs and congestion relief
- Staggered school start times affecting peak periods
- Activity timing incentives and behavioral change programs
- Real-time information systems enabling dynamic schedule adjustments

This Tour Time-of-Day Choice Model provides essential insights into temporal travel patterns, enabling transportation system planning that addresses peak period congestion, service provision timing, and the coordination of individual activities within household and community contexts.