# Stop Frequency Model

## Overview

The Stop Frequency Model determines how many intermediate stops are made during tours, capturing the trip chaining behavior that characterizes modern travel patterns. This model is essential for understanding the complexity of multi-purpose trips and the efficiency gains people achieve by combining activities into single tours.

## Model Purpose

**Primary Function**: Determine the number of intermediate stops that occur on the outbound and inbound portions of tours, representing the trip chaining and activity bundling behavior of travelers.

**Key Decisions**:

- Number of stops on outbound leg (home to primary destination)
- Number of stops on inbound leg (primary destination to home)  
- Total tour complexity and multi-purpose character
- Activity efficiency and time budget optimization
- Mode-specific constraints on stop-making behavior

## Behavioral Foundation

### Trip Chaining Theory

**Activity Bundling Behavior**: Travelers combine multiple activities into single tours to achieve:

- **Transportation Efficiency**: Reduced total travel time and cost
- **Time Budget Optimization**: Maximum activity participation within time constraints  
- **Spatial Efficiency**: Logical geographic sequencing of activities
- **Mode Synergy**: Leveraging mode characteristics for multiple purposes
- **Household Coordination**: Serving multiple family members' needs efficiently

**Stop-Making Trade-offs**:

```text
Benefits of Additional Stops:
+ Activity consolidation and efficiency
+ Reduced number of separate tours needed
+ Shared transportation costs across activities  
+ Geographic clustering advantages
+ Time savings from combined travel

Costs of Additional Stops:
- Increased tour complexity and planning requirements
- Extended total travel time and vehicle use
- Parking and access challenges at multiple locations
- Coordination difficulties with other household members
- Physical and mental fatigue from extended tours
```

### Activity Compatibility and Sequencing

**Compatible Activity Combinations**:

- **Shopping Activities**: Grocery, retail, personal services naturally cluster
- **Personal Business**: Banking, medical, government services often combined
- **Social Activities**: Dining, entertainment, visiting friends can be sequenced
- **Work-Related**: Business meetings, client visits, professional services

**Incompatible Activity Constraints**:

- **Time-Specific Activities**: Appointments limiting flexibility for additional stops
- **Cargo/Package Issues**: Bulky purchases affecting subsequent activity participation
- **Dress Code Requirements**: Professional vs. casual activity combinations
- **Duration Conflicts**: Long activities leaving insufficient time for additional stops

## Tour Type and Mode Variations

### Stop Frequency by Tour Purpose

**Work Tours**:

```text
Characteristics:
- Limited stop opportunities due to schedule constraints
- Outbound stops: Coffee, breakfast, work-related errands
- Inbound stops: Shopping, personal business, social activities
- Mode dependency: Auto enables more stops than transit

Typical Patterns:
- 0 stops: 65% of work tours (direct home-work-home)
- 1 stop: 25% of work tours (usually inbound shopping/personal)
- 2+ stops: 10% of work tours (multiple errands on return)
```

**Shopping Tours**:

```text
Characteristics:
- High stop frequency due to activity complementarity
- Multiple retail locations and service combinations
- Grocery combined with other shopping and services
- Efficiency focus on geographic clustering

Typical Patterns:
- 0 stops: 35% of shopping tours (single-purpose)
- 1 stop: 40% of shopping tours (two-destination shopping)
- 2+ stops: 25% of shopping tours (multi-purpose efficiency)
```

**Social/Recreation Tours**:

```text
Characteristics:
- Moderate stop frequency with activity enhancement
- Dining combined with entertainment or social visits
- Shopping integrated with recreation activities
- Event-based timing affecting stop flexibility

Typical Patterns:
- 0 stops: 50% of social tours (event-focused)
- 1 stop: 35% of social tours (dining + entertainment)
- 2+ stops: 15% of social tours (complex social/shopping combinations)
```

### Mode-Specific Stop Constraints

**Auto Mode Tours**:

```text
Advantages for Stop-Making:
+ Flexible routing and scheduling
+ Storage capacity for packages and purchases
+ Weather protection for multiple activities
+ Direct access to destinations with parking

Limitations:
- Parking costs accumulating across multiple stops
- Traffic congestion affecting time efficiency
- Downtown parking constraints limiting accessibility
- Fuel costs increasing with additional stops
```

**Transit Mode Tours**:

```text
Constraints on Stop-Making:
- Fixed route networks limiting routing flexibility
- Package carrying capacity and convenience
- Weather exposure during access/egress
- Transfer requirements complicating stop integration

Opportunities:
+ Access to dense activity centers
+ Reduced parking cost concerns
+ Walkable environments facilitating stop combinations
+ Service frequency enabling flexible timing
```

**Non-Motorized Tours**:

```text
Walk Mode Limitations:
- Distance constraints severely limiting stop opportunities
- Package carrying capacity restrictions
- Weather dependency for extended tours
- Time requirements for multiple walking segments

Bike Mode Characteristics:
- Moderate distance capability enabling some stops
- Storage limitations affecting shopping activities
- Infrastructure requirements for safe multi-destination travel
- Security concerns for bike parking at multiple locations
```

## Stop Frequency Model Structure

### Discrete Choice Framework

**Stop Frequency Alternatives**:

CT-RAMP typically models stop frequency with discrete alternatives:

- **0 Stops**: Direct travel between home and primary destination
- **1 Stop**: Single intermediate activity on outbound or inbound
- **2 Stops**: Two intermediate activities, various combinations
- **3+ Stops**: Complex multi-purpose tours (capped for computational efficiency)

**Separate Outbound and Inbound Models**:

```text
P(Outbound_Stops = n) = exp(U_out_n) / Σ[exp(U_out_k)] for k = 0,1,2,3+

P(Inbound_Stops = m) = exp(U_in_m) / Σ[exp(U_in_j)] for j = 0,1,2,3+

Where utilities include:
- Tour and person characteristics
- Mode-specific stop-making capabilities
- Time and distance constraints
- Activity purpose compatibility factors
```

### Utility Specification

**Base Stop Frequency Utility**:

```text
U_n_stops = ASC_n_stops +
           beta_time_pressure * available_tour_time +
           beta_mode * mode_stop_capability +
           beta_activities * potential_activity_opportunities +
           beta_efficiency * activity_bundling_benefits +
           beta_complexity * tour_coordination_costs +
           beta_demographics * person_household_characteristics +
           beta_spatial * geographic_activity_clustering
```

**Key Variable Categories**:

**Tour Characteristics**:

```text
Tour_Duration_Available = Total time budget minus primary activity time
                         Remaining time for intermediate activities
                         
Primary_Activity_Flexibility = Fixed appointment vs. flexible timing
                              Duration certainty vs. variable length
                              
Tour_Distance = Geographic scope affecting stop opportunities
               Distance between home and primary destination
               Activity clustering potential along route
```

**Person and Household Factors**:

```text
Employment_Status = Workers: Limited weekday tour time
                   Non-workers: More flexible time for multiple stops
                   
Household_Responsibilities = Escort duties limiting tour complexity
                           Shopping responsibilities encouraging efficiency
                           
Age_Effects = Young adults: High social stop-making
             Middle-aged: Efficiency-focused multiple stops
             Seniors: Reduced complexity preferences
```

**Spatial and Transportation Factors**:

```text
Activity_Density = High density: More stop opportunities
                  Low density: Limited activity clustering
                  
Mode_Capability = Auto: High stop-making capability
                 Transit: Moderate, route-dependent capability
                 Walk/Bike: Limited stop capability
                 
Parking_Costs = High costs: Discouraging multiple auto stops
               Free/cheap: Enabling flexible stop patterns
```

## Stop Location and Sequencing

### Geographic Stop Patterns

**Route-Based Stops**:

```text
Characteristics:
- Activities located along or near direct travel route
- Minimal route deviation and time penalty
- Geographic clustering of compatible activities
- "Path accessibility" maximizing stop efficiency

Examples:
- Gas stations and coffee shops along commute routes
- Shopping centers positioned between residential and employment areas
- Service businesses in mixed-use corridors
- Transit-oriented activity clusters
```

**Destination-Based Stops**:

```text
Characteristics:
- Activities clustered around primary destination
- Activity concentration in downtown, shopping malls, town centers
- Walking connections between activities after arrival
- Parking once and accessing multiple services

Examples:
- Downtown business districts with diverse services
- Regional shopping malls with multiple retail and dining
- Medical centers with multiple healthcare providers
- University campuses with integrated services
```

**Origin-Based Stops**:

```text
Characteristics:  
- Activities near residential location before departure
- Convenience and routine service access
- Return trip activities after main tour purpose
- Local neighborhood activity participation

Examples:
- Neighborhood shopping and services before work
- School drop-off combined with work commute
- Local errands and services on return from work
- Community center activities combined with other tours
```

### Temporal Sequencing Constraints

**Operating Hours Coordination**:

```text
Business Hours Alignment:
- Personal business requiring weekday business hours
- Shopping activities with retail operating schedules  
- Service appointments with professional availability
- Banking and government services with limited hours

Timing Sequence Requirements:
- Time-specific appointments constraining other activities
- Service duration uncertainty affecting subsequent stops
- Rush hour timing affecting travel time budgets
- Meal timing affecting social and dining activities
```

**Activity Duration Variability**:

- **Short Activities** (15-30 minutes): Gas, banking, quick shopping
- **Medium Activities** (30-90 minutes): Grocery shopping, personal services, dining
- **Long Activities** (1+ hours): Medical appointments, social visits, entertainment
- **Variable Duration**: Shopping activities with uncertain time requirements

## Demographic and Market Segmentation

### Life Stage Effects on Stop Frequency

**Young Adults (18-34)**:

```text
Stop-Making Characteristics:
- High social activity stop combinations
- Flexible schedule enabling complex tours
- Technology-assisted activity planning and coordination
- Cost-conscious activity bundling for efficiency

Typical Patterns:
- Work tours: Limited stops due to time constraints
- Social tours: High stop frequency for entertainment combinations
- Shopping tours: Efficiency focus with multiple retail stops
- Personal business: Bundled with other activities for time efficiency
```

**Families with Children**:

```text
Stop-Making Characteristics:
- Escort responsibilities creating complex tour patterns
- Efficiency imperative due to time constraints
- Family-oriented activity combinations
- Safety and convenience prioritized in stop selection

Typical Patterns:
- School escort tours: Combined with shopping and personal business
- Family activity tours: Multiple child-focused destinations
- Efficiency shopping: Bulk purchasing and household service combinations
- Work tours: Limited stops due to family time priorities
```

**Empty Nesters/Seniors**:

```text
Stop-Making Characteristics:
- Flexible schedules enabling leisurely complex tours
- Health and social service activity combinations  
- Preference for familiar destinations and routine patterns
- Comfort and accessibility priorities in stop selection

Typical Patterns:
- Health tours: Medical appointments combined with pharmacy, shopping
- Social tours: Extended social visits with dining and activities
- Shopping tours: Leisure shopping with social and dining combinations
- Personal business: Comprehensive service and appointment coordination
```

### Income and Employment Effects

**High-Income Households**:

- **Time Value**: Preference for efficiency and time-saving stop combinations
- **Service Substitution**: Personal services reducing stop-making needs
- **Quality Focus**: Destination quality prioritized over convenience
- **Technology Integration**: Online services reducing physical stop needs

**Low-Income Households**:

- **Cost Efficiency**: Price comparison shopping requiring multiple stops
- **Service Access**: Public services and discount retailers in different locations
- **Transportation Constraints**: Limited vehicle access affecting stop capability
- **Time Flexibility**: More time available for complex bargain-hunting tours

## Model Implementation and Estimation

### Data Requirements

**Travel Survey Data**:

- Detailed stop patterns by tour purpose, mode, and demographics
- Activity duration and timing for multi-stop tours
- Stated preferences for activity bundling and tour complexity
- Geographic clustering of activities and spatial accessibility

**Activity Location Data**:

- Retail and service location density and clustering patterns
- Operating hours and service availability by location type
- Parking availability and cost structure affecting stop decisions
- Transit accessibility to activity cluster locations

### Calibration and Validation Targets

**Stop Frequency Distribution by Tour Purpose**:

```text
Work Tours:
- 0 stops: 65%
- 1 stop: 25%  
- 2+ stops: 10%

Shopping Tours:
- 0 stops: 35%
- 1 stop: 40%
- 2+ stops: 25%

Social/Recreation Tours:
- 0 stops: 50%
- 1 stop: 35%
- 2+ stops: 15%

Personal Business Tours:
- 0 stops: 45%
- 1 stop: 35%
- 2+ stops: 20%
```

**Mode-Specific Variations**:

- Auto tours: 30% higher stop frequency than transit tours
- Transit tours: Higher stop frequency in high-density areas with good connectivity
- Walk/bike tours: Limited to short-distance, single-stop patterns

## Policy Applications and Planning Insights

### Transportation System Planning

**Network Design Implications**:

- Activity clustering strategies encouraging efficient multi-purpose travel
- Transit route design serving major activity cluster destinations
- Parking policies affecting multi-stop tour feasibility and mode choice
- Mixed-use development facilitating trip chaining and stop efficiency

**Congestion and Environmental Benefits**:

- Trip chaining reducing total vehicle miles traveled through tour efficiency
- Peak spreading effects of flexible stop timing reducing congestion  
- Mode shift potential for well-designed activity clusters with transit access
- Environmental benefits of reduced tour generation through efficient stop patterns

### Land Use and Economic Development

**Activity Center Planning**:

- Mixed-use development design facilitating stop combinations and trip chaining
- Retail and service clustering strategies maximizing customer convenience
- Parking provision and management supporting multi-purpose visits
- Pedestrian infrastructure enabling walking connections between activities

**Economic Impact Analysis**:

- Business location strategies leveraging trip chaining and stop-making behavior
- Customer convenience and accessibility improving business performance
- Transportation cost reduction benefits for households through efficient tours
- Regional economic development through activity clustering and accessibility

This Stop Frequency Model provides essential insights into trip chaining behavior, enabling transportation and land use planning that supports efficient multi-purpose travel patterns while recognizing the complex activity coordination requirements of modern households.