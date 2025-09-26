# Joint Tour Generation and Participation Model

## Overview

The Joint Tour Generation and Participation Model determines which household members participate in joint travel activities together. This model captures the coordination aspects of household travel behavior, where family members synchronize their activities and travel arrangements for efficiency, companionship, or necessity.

## Model Purpose

**Primary Function**: Determine which household members participate in joint tours and the characteristics of these coordinated travel patterns.

**Key Decisions**:

- Whether household generates joint tours
- Which household members participate in each joint tour
- Tour purpose and priority among competing activities
- Leadership and decision-making roles within household
- Resource allocation for joint activities

## Behavioral Foundation

### Household Coordination Theory

**Joint Activity Benefits**:

- **Efficiency**: Shared transportation costs and parking
- **Companionship**: Social interaction during travel and activities
- **Childcare**: Supervision requirements for dependent members
- **Resource Sharing**: Single vehicle serving multiple household members
- **Activity Complementarity**: Activities that benefit from joint participation

**Coordination Constraints**:

- **Scheduling**: Alignment of individual schedules and preferences
- **Mobility**: Vehicle availability and driving capability requirements
- **Activity Compatibility**: Activities that can be reasonably shared
- **Spatial Feasibility**: Reasonable travel distances for all participants
- **Time Budget**: Available time for coordinated activities

### Decision Hierarchy

1. **Joint Activity Generation**: Household propensity for coordinated activities
2. **Participation Composition**: Which members participate in joint activities
3. **Activity Priority**: Ordering when multiple joint opportunities exist
4. **Leadership Assignment**: Primary decision-maker and trip organizer

## Model Structure

### Joint Tour Types

**Shopping Tours**:

- **Participants**: Adults, teenagers, children requiring supervision
- **Characteristics**: Flexible timing, shared purchasing decisions
- **Benefits**: Bulk purchasing, shared carrying capacity, family preferences

**Social/Recreation Tours**:

- **Participants**: All household members, often including children
- **Characteristics**: Leisure timing, entertainment and social activities  
- **Benefits**: Family bonding, shared experiences, cost sharing

**Personal Business Tours**:

- **Participants**: Adults, accompanied children
- **Characteristics**: Scheduled appointments, errands
- **Benefits**: Childcare during appointments, efficiency of combined trips

**Escort Tours** (Special Case):

- **Participants**: Adult + child/dependent
- **Characteristics**: School, activity drop-off/pick-up
- **Benefits**: Supervision, safety, transportation access

### Participation Rules and Constraints

**Age-Based Participation**:

```text
Children (0-15): Can participate in any joint tour type
                Must be accompanied by adult for supervision

Teenagers (16-17): Independent participation in some activities
                  May serve as drivers if licensed
                  Subject to parental coordination preferences

Adults (18+): Full participation capability
             Can serve as tour leaders and drivers
             Make primary household coordination decisions

Seniors (65+): May have mobility limitations affecting participation
              Preferences for familiar activities and destinations
```

**Vehicle and Mobility Constraints**:

```text
Driver Requirements: At least one licensed driver must participate
                    Driver capability affects tour feasibility

Vehicle Capacity: Physical capacity limits on participation
                 Comfort considerations for long tours

Mobility Needs: Wheelchair accessibility requirements
               Walking distance limitations for some members
```

### Utility Framework for Joint Tour Generation

**Household Propensity Model**:

```text
U_JointTour = ASC_Joint + 
              beta_hhsize * household_size +
              beta_children * number_children +
              beta_workers * number_workers +
              beta_autos * autos_per_adult +
              beta_income * income_category +
              beta_lifecycle * household_lifecycle +
              beta_density * residential_density +
              beta_weekend * weekend_indicator
```

**Key Variables**:

- **Household Size**: More members increase coordination opportunities and needs
- **Presence of Children**: Strong driver for joint activities (supervision, family time)
- **Worker Status**: Employment schedules affect coordination feasibility
- **Auto Availability**: Vehicle access enables joint travel
- **Income Level**: Resources for joint activities and transportation
- **Life Cycle Stage**: Young families vs. empty nesters have different patterns
- **Residential Location**: Urban vs. suburban affects joint activity opportunities

### Participation Composition Model

**Person-Level Participation Utility**:

```text
U_Participate = ASC_person_type +
                beta_age * age_category +
                beta_gender * gender +
                beta_student * student_status +
                beta_worker * employment_status +
                beta_driver * driver_status +
                beta_mandatory * mandatory_commitments +
                beta_previous * previous_joint_participation +
                person_type_interactions
```

**Participation Patterns by Household Structure**:

**Two-Adult Household**:

- High joint participation rates for shopping and social activities
- Coordination around work schedules and individual responsibilities
- Shared decision-making and tour leadership

**Family with Children**:

- Adult-child combinations most common
- Parent serves as driver and supervisor
- Children's activity schedules drive timing decisions

**Single Parent Household**:

- Limited joint tour generation due to single adult responsibilities
- Children accompany parent on necessary activities
- Efficiency-focused rather than recreation-focused

**Multi-Generational Household**:

- Complex coordination involving seniors and adult children
- Mobility limitations may affect participation patterns
- Shared caregiving responsibilities influence joint activities

## Activity Purpose and Timing

### Purpose-Specific Participation Patterns

**Shopping Tours**:

```text
Typical Participants: Adult + Adult (couples)
                     Adult + Children (family shopping)
                     Adult + Senior (assistance/companionship)

Timing Preferences: Weekend mornings (family time)
                   Weekday evenings (after work)
                   Avoid peak commute periods

Activity Characteristics: Multiple stops common
                         Bulk purchasing opportunities
                         Entertainment/social aspects
```

**Social/Recreation Tours**:

```text
Typical Participants: Entire household (family outings)
                     Adult + Children (parent-child activities)
                     Adult couples (evening/weekend entertainment)

Timing Preferences: Weekends and holidays
                   Evening hours (after work/school)
                   School holiday periods

Activity Characteristics: Longer duration activities
                         Destination-focused (parks, events, restaurants)
                         Seasonal and weather-dependent
```

**Personal Business Tours**:

```text
Typical Participants: Adult + Children (appointments, errands)
                     Adult couples (major purchases, services)

Timing Preferences: Weekday business hours
                   Saturday mornings
                   Scheduled appointment times

Activity Characteristics: Time-constrained activities
                         Often combined with other purposes
                         Location-specific requirements
```

## Spatial and Temporal Constraints

### Geographic Feasibility

**Distance Considerations**:

- Joint tours typically involve longer distances than individual tours
- Multiple household members must find destinations acceptable
- Return travel coordination affects destination choice

**Activity Location Clusters**:

- Shopping centers and malls facilitate joint activities
- Recreation destinations serve multiple age groups
- Medical/service clusters serve multiple household needs

### Scheduling Coordination

**Temporal Alignment**:

```text
Work Schedule Constraints: Available windows for joint activities
                          Weekend vs. weekday opportunities
                          Shift work affecting coordination

School Schedule Integration: Coordination around school hours
                           Holiday and summer schedule changes
                           After-school activity participation

Individual Commitment Conflicts: Mandatory activities limit availability
                                Personal appointments reduce flexibility
                                Social commitments compete for time
```

## Model Implementation Framework

### Sequential Decision Process

1. **Joint Tour Generation Decision** (Household Level)
   - Probability of generating joint tours by purpose
   - Number of joint tours by day type and purpose
   - Household propensity based on demographics and constraints

2. **Participation Composition** (Person Level)  
   - Which household members participate in each joint tour
   - Leadership and decision-making role assignment
   - Vehicle allocation and driving responsibilities

3. **Activity Scheduling** (Tour Level)
   - Integration with individual mandatory activities
   - Time-of-day coordination among participants
   - Duration and activity sequence planning

### Integration with Individual Models

**Coordination with Individual Tour Generation**:

- Joint tours reduce need for individual tours of same purpose
- Remaining individual activity needs after joint tour participation
- Resource allocation between joint and individual activities

**Mode Choice Implications**:

- Joint tours typically use household vehicles
- Higher vehicle occupancy affects mode choice utilities
- Parking and access requirements for multiple passengers

## Data Requirements and Calibration

### Survey Data Sources

**Activity-Based Travel Surveys**:

- Household activity diaries showing joint participation patterns
- Activity purpose, timing, and participant composition
- Stated coordination preferences and constraints

**Time Use Surveys**:

- Within-household activity coordination patterns
- Joint activity participation by household type
- Time allocation for family vs. individual activities

### Calibration Targets

**Joint Tour Generation Rates by Household Type**:

```text
Two-Adult Households: 
- Shopping: 2.1 joint tours per week
- Social/Recreation: 1.4 joint tours per week
- Personal Business: 0.8 joint tours per week

Families with Children:
- Shopping: 1.8 joint tours per week  
- Social/Recreation: 2.2 joint tours per week
- Personal Business: 1.1 joint tours per week

Single Parent Households:
- All Purposes: 0.7 joint tours per week (mostly with children)
```

**Participation Composition Patterns**:

- Adult couples: 45% of joint shopping tours
- Adult + children: 35% of joint social tours  
- Whole household: 25% of joint recreation tours

### Model Validation

**Behavioral Reasonableness**:

- Higher joint activity rates for households with children
- Weekend and evening peaks for joint social activities
- Reduced individual tour generation with joint participation

**Demographic Sensitivity**:

- Income effects on joint recreation activities
- Vehicle availability impacts on joint tour feasibility
- Life cycle stage differences in coordination patterns

## Policy Applications and Insights

### Transportation Planning Applications

**Vehicle Occupancy Impacts**:

- Joint tours increase average vehicle occupancy
- HOV lane usage patterns and effectiveness
- Parking demand estimation for family destinations

**Transit Service Planning**:

- Family-friendly transit service design
- Off-peak service for joint activities
- Multi-generational accessibility requirements

### Land Use Planning Integration

**Activity Center Design**:

- Mixed-use developments facilitating joint activities
- Family-oriented retail and recreation clustering
- Parking design for family vehicles and mobility needs

**Neighborhood Planning**:

- Walkable environments supporting joint activities
- Safe routes for family walking and cycling
- Community facility programming for joint activities

### Policy Sensitivity Analysis

**Work Schedule Flexibility**:

- Telecommuting effects on household coordination opportunities
- Flexible work hours enabling joint activities
- Compressed work weeks and family time allocation

**Transportation Pricing**:

- Congestion pricing impacts on joint tour timing
- Parking pricing effects on joint activity participation
- Transit family fare structures and usage

This Joint Tour Generation and Participation Model provides essential insights into household coordination behavior, supporting transportation planning that recognizes the family context of travel decisions and the efficiency benefits of coordinated household activities.