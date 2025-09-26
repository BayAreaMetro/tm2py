# At-Work Subtour Model

## Overview

The At-Work Subtour Model determines secondary trips that occur during the work day, departing from and returning to the primary workplace. These subtours represent business meetings, client visits, lunch activities, and other work-related travel that occurs as part of the work tour but involves intermediate destinations.

## Model Purpose

**Primary Function**: Generate and characterize subtours that occur during work hours, capturing the complexity of work-based travel patterns beyond simple home-to-work commuting.

**Key Decisions**:

- Whether to generate at-work subtours during work period
- Number of subtours within the work day
- Purpose and destination of each subtour  
- Timing within the work schedule
- Mode choice for work-based travel
- Coordination with work responsibilities and schedule flexibility

## Behavioral Foundation

### Work-Based Travel Theory

**Employment-Related Mobility Requirements**:

- **Client/Customer Visits**: Face-to-face business meetings and service delivery
- **Multi-Site Operations**: Employees working across multiple business locations  
- **Supply Chain Activities**: Procurement, delivery, and logistics coordination
- **Professional Networking**: Industry meetings, conferences, and business development
- **Work Support Activities**: Banking, equipment procurement, professional services

**Personal Activities During Work Hours**:

- **Meal Activities**: Lunch at restaurants, food procurement, social dining
- **Personal Business**: Banking, appointments, errands during work breaks
- **Healthcare**: Medical appointments scheduled during work hours
- **Household Coordination**: School events, childcare coordination, family emergencies

### Work Schedule and Flexibility Constraints

**Job-Specific Flexibility**:

```text
High Flexibility Occupations:
- Sales representatives: Client visit requirements
- Consultants: Multiple client sites and meetings
- Healthcare workers: Multiple facility coverage
- Real estate agents: Property showing and client meetings
- Service technicians: Field service and repair activities

Medium Flexibility Occupations:  
- Managers: Business meetings and site visits
- Professional services: Client meetings and business development
- Government workers: Inter-agency coordination and field work
- Education: Professional development and administrative meetings

Low Flexibility Occupations:
- Manufacturing workers: Fixed production schedules
- Retail workers: Customer service requirements during shifts
- Administrative support: Office coverage and phone responsibilities  
- Security personnel: Continuous coverage requirements
```

**Employer Policy Variations**:

- **Company Vehicle Provision**: Employer-provided vehicles for business use
- **Expense Reimbursement**: Mileage, parking, and meal allowances
- **Flexible Lunch Policies**: Extended lunch breaks vs. fixed timing
- **Business Travel Expectations**: Job requirements for client interaction
- **Work Schedule Autonomy**: Self-directed vs. supervised work timing

## At-Work Subtour Categories

### Business-Related Subtours

**Client and Customer Visits**:

- **Purpose**: Face-to-face meetings, service delivery, relationship management
- **Typical Occupations**: Sales, consulting, healthcare, real estate, legal services
- **Timing**: Scheduled appointments, often morning or afternoon blocks
- **Distance**: Varies from local (5-10 miles) to regional (50+ miles)
- **Duration**: 1-4 hours including travel time
- **Mode Choice**: Company vehicle preferred, personal auto with reimbursement

**Inter-Office Travel**:

- **Purpose**: Meetings at other company locations, document delivery, coordination
- **Typical Workers**: Managers, administrators, technical specialists
- **Timing**: Mid-morning or early afternoon to avoid commute conflicts
- **Distance**: Usually within metropolitan area (10-30 miles)
- **Duration**: 2-6 hours including meeting time
- **Mode Choice**: Personal auto, company shuttle, or transit between major employment centers

**Supply Chain and Logistics**:

- **Purpose**: Procurement, delivery coordination, vendor meetings, site inspections
- **Typical Occupations**: Procurement specialists, project managers, quality inspectors
- **Timing**: Business hours coordinated with supplier/vendor schedules
- **Distance**: Local to regional, depending on supply chain geography
- **Duration**: Variable, often half-day commitments
- **Mode Choice**: Company vehicle for deliveries, personal auto for meetings

### Personal Activities During Work Hours

**Meal and Dining Activities**:

- **Purpose**: Lunch dining, food procurement, business meals with colleagues/clients
- **Participants**: All workers with lunch break flexibility
- **Timing**: Traditional lunch hours (11:30 AM - 1:30 PM) with some variation
- **Distance**: Usually local (within 2-5 miles of workplace)
- **Duration**: 30 minutes to 1.5 hours depending on employer policy
- **Mode Choice**: Walking for nearby options, driving for distant or group dining

**Personal Business During Work**:

- **Purpose**: Banking, medical appointments, government services, family coordination
- **Participants**: Workers with flexible schedules or extended breaks
- **Timing**: Mid-morning or early afternoon to avoid peak service periods
- **Distance**: Local area, often combining multiple errands efficiently
- **Duration**: 1-2 hours for appointment-based activities
- **Mode Choice**: Personal vehicle for efficiency and multiple stops

## Model Structure and Specification

### Subtour Generation Model

**At-Work Activity Propensity**:

```text
U_AtWork_Subtour = ASC_occupation_type +
                  beta_flexibility * work_schedule_flexibility +
                  beta_vehicle_access * company_vehicle_availability +
                  beta_reimbursement * employer_travel_policy +
                  beta_job_requirements * client_interaction_needs +
                  beta_lunch_policy * lunch_break_flexibility +
                  beta_workplace_density * workplace_activity_opportunities +
                  beta_day_type * weekday_business_activity_levels
```

**Key Explanatory Variables**:

**Occupation-Specific Factors**:

```text
Sales_Professional = High business travel requirements
                    Client relationship management needs
                    Flexible schedule with quota-based performance
                    Company vehicle or mileage reimbursement

Manager_Executive = Inter-office coordination requirements  
                   Business meeting and networking needs
                   Moderate schedule flexibility
                   Expense account and travel support

Service_Technical = Field service and repair requirements
                   Customer site visits and installations  
                   Variable daily schedules and locations
                   Company vehicle and equipment needs

Administrative = Limited travel requirements
                Fixed office coverage responsibilities
                Scheduled personal activities during breaks
                Personal vehicle for lunch and errands
```

**Workplace Characteristics**:

```text
Employment_Density = Activity opportunities near workplace
                    Restaurant and service availability
                    Multi-company business districts
                    Transit accessibility for work-based trips

Workplace_Location = Central business district: High activity density
                    Suburban office park: Limited nearby services
                    Industrial area: Specialized business services
                    Mixed-use area: Integrated work-activity environment

Employer_Size = Large employers: On-site services, company transportation
               Small employers: Limited amenities, personal vehicle dependence
               Corporate campus: Integrated facilities, reduced external travel
               Multi-location: Inter-site coordination travel requirements
```

### Purpose-Specific Subtour Models

**Business Meeting/Client Visit Generation**:

```text
U_Business_Subtour = ASC_business +
                    beta_client_base * number_client_accounts +
                    beta_sales_role * sales_occupation_indicator +
                    beta_company_vehicle * company_car_availability +
                    beta_expense_policy * business_expense_reimbursement +
                    beta_territory_size * geographic_coverage_area
```

**Meal Activity Subtour Generation**:

```text
U_Meal_Subtour = ASC_meal +
                beta_lunch_flexibility * lunch_break_duration +
                beta_workplace_food * on_site_food_availability +
                beta_restaurant_density * nearby_restaurant_options +
                beta_social_dining * coworker_group_dining_patterns +
                beta_income * discretionary_dining_budget
```

**Personal Business Subtour Generation**:

```text
U_Personal_Business = ASC_personal +
                     beta_schedule_flexibility * work_autonomy_level +
                     beta_service_access * nearby_service_availability +
                     beta_appointment_needs * scheduled_personal_appointments +
                     beta_family_responsibilities * dependent_care_coordination
```

## Temporal Patterns and Scheduling

### Work Day Timing Windows

**Morning Business Activities** (8:00-11:00 AM):

```text
Characteristics:
- Early client meetings before main business day
- Inter-office coordination and planning meetings
- Supply chain and vendor coordination activities
- Professional networking and industry meetings

Advantages:
- Reduced traffic congestion for business travel
- Fresh energy levels for important client interactions
- Coordination with East Coast business partners
- Availability of professional services and business facilities
```

**Mid-Day Activities** (11:00 AM-2:00 PM):

```text
Peak Activity Period:
- Traditional lunch break timing and meal activities
- Business lunches and client entertainment
- Personal business appointments during standard hours
- Banking, government services, and professional appointments

Scheduling Considerations:
- Restaurant availability and reservation requirements
- Service provider business hours and appointment slots
- Coordination with coworkers for group activities
- Return timing to maintain afternoon work productivity
```

**Afternoon Business Activities** (2:00-5:00 PM):

```text
Characteristics:
- Follow-up meetings and client service calls
- Site visits and field service activities  
- Late-day coordination and project meetings
- West Coast business communication and travel

Constraints:
- Return timing to coordinate with evening commute
- Family pickup responsibilities and childcare coordination
- Energy levels and meeting productivity considerations
- Traffic congestion affecting return travel times
```

### Day-of-Week Variations

**Monday-Thursday Patterns**:

- **Higher Business Activity**: Client meetings, professional appointments
- **Standard Lunch Timing**: Established patterns and group coordination
- **Personal Business Integration**: Efficient scheduling with regular work week
- **Consistent Workplace Services**: Full staffing and business hour operations

**Friday Patterns**:

- **Reduced Business Meetings**: End-of-week timing and vacation considerations  
- **Social Dining Increase**: Team lunches and informal gatherings
- **Extended Lunch Breaks**: Relaxed end-of-week atmosphere
- **Personal Activity Shift**: Weekend preparation and personal appointments

## Mode Choice and Transportation

### Business Trip Mode Considerations

**Company Vehicle Usage**:

```text
Advantages:
- Employer-provided transportation and fuel
- Professional appearance and client impression
- Storage for business materials and equipment
- Parking expense coverage and reserved spaces

Usage Pattern:
- Client visits and service calls
- Inter-office business travel
- Supply chain and vendor coordination
- Professional networking and industry events
```

**Personal Vehicle with Reimbursement**:

```text
Characteristics:
- Mileage reimbursement for business use
- Flexibility for personal stops and routing
- Familiar vehicle and driving preferences
- Personal responsibility for maintenance and insurance

Common Applications:
- Occasional client meetings and business travel
- Mixed business and personal activity tours
- Professional appointments and networking
- Local business errands and coordination
```

**Transit and Shared Mobility**:

```text
Usage Scenarios:
- Downtown business districts with parking constraints
- Major employment centers with transit connectivity
- Environmental employer policies and incentives
- Cost-conscious business travel for routine meetings

Considerations:
- Schedule coordination with meeting times
- Professional appearance and client interaction requirements
- Materials and equipment transportation needs
- Weather protection and comfort considerations
```

### Personal Activity Mode Choice

**Walking for Lunch Activities**:

- **Distance Range**: Within 0.5 miles of workplace
- **Time Efficiency**: Quick access for short lunch breaks
- **Health Benefits**: Exercise integration with work day
- **Weather Dependency**: Climate and seasonal variations affect feasibility

**Driving for Personal Business**:

- **Multiple Stop Efficiency**: Errands and appointment combinations
- **Time Constraint Management**: Maximizing activity within break periods
- **Privacy and Convenience**: Personal vehicle for sensitive appointments
- **Flexibility**: Route and timing adjustments based on real-time needs

## Integration with Other Model Components

### Coordination with Main Work Tour

**Work Tour Duration Extension**:

```text
Impact on Work Tour:
- Extended workplace presence for subtour generation
- Modified departure and arrival times for home-based work tour
- Coordination with household member schedules and expectations
- Vehicle availability implications for household transportation
```

**Mode Choice Interactions**:

- **Auto Mode Tours**: Enable flexible subtour generation and routing
- **Transit Work Tours**: Limit subtour opportunities to walking/transit accessible activities
- **Mixed Mode Patterns**: Park-and-ride enabling some auto-based subtours

### Employment and Land Use Integration

**Workplace Location Effects**:

```text
Central Business District:
- High subtour generation potential
- Walking access to restaurants and services
- Transit connectivity for business meetings
- Parking constraints affecting mode choice

Suburban Office Parks:
- Limited walking access to activities
- Auto dependency for most subtours  
- On-site amenities reducing external trip needs
- Free parking supporting auto-based subtours

Industrial Areas:
- Specialized business services and suppliers
- Limited personal service availability
- Company vehicle usage for business coordination
- Lunch options may require longer travel distances
```

## Data Requirements and Model Calibration

### Data Sources for Model Development

**Employee Travel Surveys**:

- Work-based travel patterns by occupation and industry
- Subtour frequency, purpose, timing, and destination patterns
- Employer policies affecting work-based travel opportunities
- Mode choice for different types of work-based activities

**Employer Transportation Surveys**:

- Company vehicle policies and provision patterns
- Business travel reimbursement and expense policies
- Workplace amenities and on-site service availability
- Flexible work schedule and break time policies

### Calibration Targets by Occupation

**Professional/Technical Workers**:

```text
Business Subtours: 1.2 subtours per work day
Meal Subtours: 0.8 subtours per work day  
Personal Business: 0.3 subtours per work day
Average Subtour Distance: 8.5 miles
Mode Split: 85% personal auto, 10% company vehicle, 5% walk/transit
```

**Sales Representatives**:

```text
Business Subtours: 2.1 subtours per work day
Meal Subtours: 0.6 subtours per work day (often business meals)
Personal Business: 0.2 subtours per work day
Average Subtour Distance: 15.2 miles  
Mode Split: 60% personal auto, 35% company vehicle, 5% other
```

**Administrative/Clerical Workers**:

```text
Business Subtours: 0.2 subtours per work day
Meal Subtours: 0.9 subtours per work day
Personal Business: 0.4 subtours per work day
Average Subtour Distance: 3.8 miles
Mode Split: 70% personal auto, 25% walk, 5% transit
```

## Policy Applications and Planning Insights

### Transportation System Planning

**Peak Period Analysis**:

- At-work subtours contribute to reverse-commute and mid-day traffic
- Business travel affects congestion on arterial and highway networks
- Parking demand at business districts and client locations
- Transit service opportunities for inter-employment center travel

**Economic Development Support**:

- Transportation infrastructure supporting business travel and client access
- Parking provision and pricing policies affecting business district competitiveness  
- Transit connectivity between major employment centers
- Mixed-use development facilitating work-based activity integration

### Employer Transportation Policies

**Transportation Demand Management**:

- Flexible work schedules reducing peak period business travel
- Company shuttle services between business locations
- Teleconferencing and virtual meeting technologies reducing business travel
- Employer-sponsored transit passes for work-based travel

**Sustainability Initiatives**:

- Company vehicle fleet electrification and sustainability programs
- Business travel carbon footprint reduction strategies
- Employer incentives for sustainable work-based transportation choices
- Workplace amenities reducing external trip generation needs

This At-Work Subtour Model provides crucial insights into the complexity of work-based travel patterns, enabling transportation planning that serves business community needs while recognizing the economic and personal activities that occur as part of modern work life.