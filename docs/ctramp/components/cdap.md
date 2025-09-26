# Coordinated Daily Activity Pattern (CDAP)

!!! info "Model Purpose"
    The Coordinated Daily Activity Pattern (CDAP) model coordinates daily activity patterns among household members, determining who participates in mandatory activities (work/school) and how household resources and responsibilities are allocated.

## Model Overview

### Purpose and Role
CDAP serves as the **central coordination mechanism** in CT-RAMP, addressing the fundamental challenge that household members' activity patterns are not independent. The model:

- **Coordinates activity patterns** across all household members simultaneously
- **Allocates household resources** including vehicles and childcare responsibilities
- **Resolves scheduling conflicts** between competing household needs
- **Sets constraints** for downstream tour generation and choice models

### Key Behavioral Assumptions

**Joint Utility Maximization**
: Households coordinate to maximize overall household utility, not just individual preferences

**Resource Constraints**
: Vehicle availability, childcare needs, and time constraints affect all household members

**Activity Priorities**
: Mandatory activities (work/school) take precedence over discretionary activities

**Coordination Benefits**
: Joint activities and shared travel provide utility benefits that influence patterns

## Model Structure

### Activity Pattern Alternatives
CDAP assigns each person one of these daily activity patterns:

| Pattern | Code | Description | Typical Persons |
|---------|------|-------------|-----------------|
| **Mandatory** | M | Work or school activity | Workers, students |
| **Non-mandatory** | N | Discretionary activities only | Non-workers, weekends |  
| **Home** | H | Stay home all day | Preschoolers, retirees, disabled |

### Household Pattern Combinations
For a 2-adult household, possible combinations include:

| Household Pattern | Person 1 | Person 2 | Description |
|-------------------|----------|----------|-------------|
| **MM** | Mandatory | Mandatory | Both adults work/school |
| **MN** | Mandatory | Non-mandatory | One works, one discretionary |
| **MH** | Mandatory | Home | One works, one stays home |
| **NM** | Non-mandatory | Mandatory | One discretionary, one works |
| **NN** | Non-mandatory | Non-mandatory | Both discretionary |
| **NH** | Non-mandatory | Home | One discretionary, one home |
| **HM** | Home | Mandatory | One home, one works |
| **HN** | Home | Non-mandatory | One home, one discretionary |
| **HH** | Home | Home | Both stay home |

### Utility Function Structure

CDAP uses a **multinomial logit model** at the household level with utilities for each possible household pattern combination.

**Individual Components**
```
V_individual = β₁ × Person_Type + β₂ × Age + β₃ × Employment + β₄ × Income_Effect
```

**Interaction Components**  
```
V_interaction = β₅ × Joint_Activities + β₆ × Vehicle_Sharing + β₇ × Childcare_Coordination
```

**Constraint Components**
```
V_constraints = β₈ × Vehicle_Availability + β₉ × Schedule_Conflicts + β₁₀ × Travel_Time
```

## Technical Implementation

### Model Segmentation
CDAP typically segments households by:

**Household Size and Composition**
- 1-person households (no coordination needed)
- 2-person households (adult couples)  
- 3+ person households (families with children)

**Lifecycle Stage**
- Young adults without children
- Families with preschool children
- Families with school-age children
- Empty nesters and retirees

**Income and Auto Ownership**
- Low-income, transit-dependent households
- Middle-income households with limited auto access
- High-income households with multiple vehicles

### Coordination Mechanisms

**Vehicle Allocation**
The model considers vehicle availability constraints:
- Number of household vehicles vs. number of workers
- Shared vehicle scenarios and coordination requirements
- Transit accessibility for vehicle-constrained scenarios

**Childcare Coordination**
For households with children:
- Escort responsibilities for school trips
- Supervision requirements for young children
- Joint activity opportunities (shopping with children)

**Schedule Coordination**
- Synchronized departure times for shared rides
- Coordination of return times for vehicle sharing
- Joint activity timing and duration

### Sample Utility Specifications

**2-Adult Household MM Pattern (Both Mandatory)**
```
V_MM = β₁ + β₂×(Worker1) + β₃×(Worker2) + β₄×(HH_Income) + 
       β₅×(Auto_Sufficiency) + β₆×(Transit_Access) + 
       β₇×(Childcare_Needs) + β₈×(Commute_Distance_Interaction)
```

**2-Adult Household MN Pattern (Mixed)**
```  
V_MN = β₉ + β₁₀×(Part_Time_Worker) + β₁₁×(Non_Worker_Characteristics) +
       β₁₂×(Household_Maintenance_Needs) + β₁₃×(Vehicle_Availability) +
       β₁₄×(Age_Interaction) + β₁₅×(Income_Effect)
```

## Data Requirements

### Input Data Sources

**Household Characteristics**
- Household size and composition
- Number and ages of children
- Household income level
- Vehicle ownership (from auto ownership model)

**Person Characteristics**
- Age, gender, employment status
- Student enrollment and grade level
- Worker category (full-time, part-time, non-worker)
- Driver's license availability

**Accessibility Data**
- Work/school accessibility by mode
- Non-work accessibility measures  
- Transit service levels
- Auto travel times and costs

**Spatial Context**
- Home location characteristics
- Neighborhood walkability
- Transit stop accessibility
- Parking availability and costs

### Required Preprocessing
1. **Auto Ownership Results**: Input from auto ownership model
2. **Accessibility Calculation**: Mode-specific accessibility by person type
3. **Workplace/School Locations**: For mandatory activity accessibility
4. **Household Relationship Matrix**: Define coordination relationships

## Model Outputs

### Primary Outputs
**Person-Level Activity Pattern**
- Activity pattern (M, N, H) for each person
- Pattern choice probability and utility
- Coordination constraints and dependencies

**Household-Level Coordination**
- Household pattern combination (MM, MN, etc.)
- Vehicle allocation strategy
- Childcare and escort responsibilities

### Derived Outputs for Downstream Models
**Tour Generation Constraints**
- Mandatory tour requirements by person
- Available time windows for discretionary tours  
- Joint tour opportunities and constraints

**Mode Choice Context**
- Vehicle availability by person and time period
- Coordination requirements for shared vehicle use
- Transit dependency indicators

### Validation Metrics
**Activity Pattern Distributions**
- Person-type activity rates (employment rates, school attendance)
- Household pattern combinations by lifecycle stage
- Vehicle utilization and sharing patterns

**Coordination Patterns**
- Joint activity participation rates
- Vehicle sharing frequencies
- Escort trip generation rates

## Advanced Coordination Features

### Joint Activity Coordination

**Shopping and Personal Business**
- Coordinate shopping trips among household members
- Share childcare responsibilities during activities
- Optimize household maintenance tasks

**Social and Recreation**
- Joint recreational activities (dining out, entertainment)
- Coordination of children's activities and transportation
- Social visits and family obligations

### Vehicle Sharing Coordination

**Commute Coordination**
- Shared commute trips to work or transit
- Drop-off/pick-up coordination for vehicle sharing
- Temporal coordination of work schedules

**Activity Chain Coordination**
- Coordinate complex activity chains involving multiple people
- Optimize vehicle use across multiple household tours
- Balance individual preferences with household efficiency

### Childcare and Escort Coordination

**School Escort Coordination**
- Assign escort responsibilities for school trips
- Coordinate with work schedules and other activities
- Consider walk-to-school vs. drive options

**Activity Supervision**
- Determine supervision needs for children's activities
- Coordinate adult availability with children's schedules
- Balance supervision with adult activity preferences

## Implementation Considerations

### Computational Challenges
**Combinatorial Complexity**
- Number of pattern combinations grows exponentially with household size
- Need efficient enumeration and evaluation strategies
- Memory and computational time optimization

**Coordination Algorithms**
- Resolve conflicting preferences and constraints
- Handle infeasible pattern combinations gracefully
- Maintain consistency with individual utility maximization

### Configuration Parameters
**Segmentation Definitions**
- Household size and composition categories
- Person type definitions (worker categories, student types)
- Lifecycle stage classifications

**Utility Function Specification**
- Individual vs. interaction term importance
- Constraint penalty magnitudes
- Segmentation-specific parameters

**Alternative Set Definition**
- Include/exclude certain pattern combinations
- Handle households with special needs (disability, etc.)
- Define feasibility constraints

## Calibration and Validation

### Parameter Estimation
**Data Sources**
- Regional household travel surveys with multi-person households
- Time-use surveys showing household coordination
- Employment and school enrollment data
- Observed vehicle sharing patterns

**Estimation Challenges**
- Limited data on household coordination decisions
- Revealed vs. stated preference trade-offs
- Unobserved heterogeneity in household preferences

### Validation Approaches
**Activity Pattern Validation**
- Match observed employment and school rates
- Reproduce household activity pattern combinations
- Validate vehicle utilization patterns

**Coordination Validation**
- Joint activity participation rates
- Vehicle sharing and escort trip rates
- Consistency with time-use survey data

**Sensitivity Testing**
- Response to changes in vehicle availability
- Sensitivity to accessibility and cost changes
- Robustness across different household types

## Common Issues and Troubleshooting

### Pattern Distribution Problems
**Symptoms**: Unrealistic activity pattern distributions
**Causes**: Incorrect person type definitions, poor accessibility measures
**Solutions**: Refine person type segmentation, validate accessibility calculations

### Coordination Inconsistencies
**Symptoms**: Infeasible household patterns, coordination conflicts
**Causes**: Incorrect constraint specifications, utility function problems
**Solutions**: Review constraint logic, test pattern feasibility, adjust penalties

### Performance Issues
**Symptoms**: Long execution times, memory problems with large households
**Causes**: Combinatorial explosion, inefficient algorithms
**Solutions**: Limit household size, optimize pattern enumeration, parallel processing

## Usage Examples

### Basic CDAP Execution
```python
# Configure CDAP model
cdap_config = {
    "max_household_size": 5,
    "segmentation": "lifecycle_income",
    "coordination_level": "full_coordination",
    "vehicle_allocation": "optimal"
}

# Run CDAP model  
cdap_results = cdap_model.run(
    households=household_data,
    persons=person_data,
    auto_ownership=auto_ownership_results,
    accessibility=accessibility_data,
    config=cdap_config
)
```

### Analyze Coordination Patterns
```python
# Analyze household coordination results
coordination_analysis = {
    "household_patterns": cdap_results.household_patterns.value_counts(),
    "vehicle_sharing": cdap_results.vehicle_sharing_rate,
    "joint_activities": cdap_results.joint_activity_participation
}

print(f"Most common household pattern: {coordination_analysis['household_patterns'].index[0]}")
print(f"Vehicle sharing rate: {coordination_analysis['vehicle_sharing']:.1%}")
```

## Integration with Other Models

### Upstream Dependencies
**Auto Ownership Model**
- Vehicle availability determines coordination feasibility
- Auto ownership affects utility of different activity patterns

**Accessibility Calculation**
- Mode-specific accessibility affects pattern attractiveness
- Work/school accessibility crucial for mandatory patterns

### Downstream Dependencies  
**Tour Generation Models**
- Activity patterns determine mandatory tour requirements
- Set constraints and opportunities for discretionary tours

**Joint Tour Model**
- CDAP patterns influence joint tour participation
- Household coordination affects joint activity feasibility

**Mode Choice Models**
- Vehicle allocation affects individual mode choice sets
- Coordination requirements influence mode utilities

## Advanced Applications

### Policy Analysis
**Transit Investment Scenarios**
- How improved transit affects household activity coordination
- Changes in vehicle dependency and sharing patterns

**Telecommuting Policy**
- Impact of work-from-home policies on household patterns
- Changes in coordination needs and vehicle utilization

**Childcare Policy**
- Effects of childcare availability on activity patterns
- Impact on parent work participation and coordination

### Behavioral Research
**Household Decision-Making**
- Understanding coordination mechanisms in travel behavior
- Gender roles and activity allocation within households

**Life Course Analysis**
- How household coordination changes over the lifecycle
- Adaptation to changing household composition and needs

---

!!! tip "Coordination Foundation"
    CDAP is fundamental to household-based modeling. Ensure coordination mechanisms are well-understood and calibrated, as they affect all subsequent household and individual models.

!!! warning "Complexity Management"
    Large households create computational challenges. Consider simplification strategies for households with 5+ members while maintaining behavioral realism.

**Related Components:**
- [Auto Ownership](auto-ownership.md) - Provides vehicle availability constraints
- [Mandatory Tours](mandatory-tours.md) - Uses CDAP patterns for tour generation
- [Joint Tours](joint-tours.md) - Builds on CDAP coordination results

*Last updated: September 26, 2025*
