# Mandatory Tours Model

!!! info "Model Purpose"
    The Mandatory Tours Model generates work and school tours for persons whose CDAP pattern includes mandatory activities. It determines tour frequency (0, 1, or 2+ tours) based on employment status, school enrollment, and activity scheduling constraints.

## Model Overview

### Purpose and Role
The Mandatory Tours Model translates CDAP mandatory activity patterns into specific tour requirements:

- **Work Tours**: Generated for employed persons with mandatory (M) patterns
- **School Tours**: Generated for students with mandatory (M) patterns  
- **University Tours**: Generated for university students with special scheduling
- **Multiple Tours**: Handles cases where persons make multiple mandatory tours per day

### Key Behavioral Assumptions

**Activity-Tour Relationship**
: Each mandatory activity location generates at least one home-based tour

**Scheduling Flexibility**  
: Some workers/students have flexible schedules allowing multiple or no tours on specific days

**Tour Consolidation**
: Multiple activities at the same location are consolidated into single tours when possible

**Person-Type Differences**
: Tour generation varies by employment status, occupation, and student type

## Model Structure

### Choice Alternatives
The model generates tours by purpose and frequency:

**Work Tours**
| Alternative | Description | Typical Workers |
|-------------|-------------|-----------------|
| **0 work tours** | Work from home, day off | Telecommuters, flexible workers |
| **1 work tour** | Single work tour | Standard full-time workers |
| **2 work tours** | Multiple work locations | Multi-job workers, meetings |

**School Tours**
| Alternative | Description | Typical Students |
|-------------|-------------|------------------|
| **0 school tours** | No school today | Part-time students, sick days |
| **1 school tour** | Regular school attendance | Most K-12 and university students |
| **2 school tours** | Multiple school locations | Students with multiple campuses |

**University Tours**
| Alternative | Description | Typical Students |
|-------------|-------------|------------------|
| **0 university tours** | No classes today | Flexible schedule students |
| **1 university tour** | Regular class attendance | Most university students |
| **2+ university tours** | Multiple campus visits | Research students, complex schedules |

### Segmentation Structure
Tours are generated separately by:

**Person Type Segmentation**
- Full-time workers (FT)
- Part-time workers (PT)  
- University students (Univ)
- K-12 students (School)
- Non-working adults (Non-worker)

**Employment Category**
- Professional/managerial occupations
- Service and retail workers
- Manufacturing and construction
- Government and institutional

**Student Category**
- Elementary school (grades K-5)
- Middle school (grades 6-8)  
- High school (grades 9-12)
- University undergraduate
- University graduate/professional

### Utility Function Structure

**Work Tour Generation**
```
V(work_tours) = β₁ × Person_Type + β₂ × Employment_Category + 
                β₃ × Work_Flexibility + β₄ × Commute_Distance +
                β₅ × Household_Responsibilities + β₆ × Income_Effect
```

**School Tour Generation**
```
V(school_tours) = β₇ × Student_Type + β₈ × Grade_Level + 
                  β₉ × School_Distance + β₁₀ × Extracurricular +
                  β₁₁ × Parent_Coordination + β₁₂ × Transport_Mode
```

## Technical Implementation

### Model Type
**Ordered Logit Models** for tour frequency (0, 1, 2+ tours)
- Recognizes ordinal nature of tour frequency
- Handles threshold effects in tour generation
- Efficient for frequency-based choice modeling

### Person Type Models

**Full-Time Workers**
- High probability of exactly 1 work tour
- Some probability of 0 tours (work from home, vacation)
- Low probability of 2+ tours (multiple job sites, meetings)

**Part-Time Workers**  
- Higher probability of 0 tours on any given day
- Lower probability of multiple tours
- Influenced by schedule flexibility and job characteristics

**University Students**
- Variable tour patterns based on class schedules  
- Higher probability of 0 tours (no classes)
- Moderate probability of multiple tours (multiple campuses)

**K-12 Students**
- Very high probability of exactly 1 school tour
- Low probability of 0 tours (sick days, holidays)
- Some probability of multiple tours (after-school activities)

### Sample Utility Specifications

**Full-Time Worker Work Tours**
```
V(0 tours) = β₁ × Work_From_Home_Option + β₂ × Flexible_Schedule + 
             β₃ × Long_Commute + β₄ × Household_Responsibilities

V(1 tour) = β₅  # Base alternative (normalized to 0)

V(2+ tours) = β₆ × Multiple_Job_Sites + β₇ × Management_Level + 
              β₈ × Business_Travel + β₉ × Meeting_Requirements
```

**University Student School Tours**  
```
V(0 tours) = β₁₀ × Online_Classes + β₁₁ × Flexible_Schedule + 
             β₁₂ × Part_Time_Status + β₁₃ × Work_Obligations

V(1 tour) = β₁₄ + β₁₅ × Regular_Schedule + β₁₆ × Full_Time_Status

V(2+ tours) = β₁₇ × Multiple_Campuses + β₁₈ × Research_Activities + 
              β₁₉ × Extracurricular + β₂₀ × Graduate_Status
```

## Data Requirements

### Input Data Sources

**Person Characteristics**
- Employment status and occupation category
- Student enrollment and grade level  
- Work/school location (from CDAP or external data)
- Schedule flexibility indicators

**Household Context**
- CDAP results (mandatory activity pattern)
- Household responsibilities (childcare, etc.)
- Vehicle availability and transportation constraints

**Spatial Context**
- Commute distance and travel time
- Alternative transportation options
- Work/school site characteristics

**Policy Variables**
- Telecommuting policies and capabilities
- Flexible work arrangements  
- School schedule policies (early release, etc.)

### Required Preprocessing
1. **CDAP Integration**: Filter to persons with mandatory (M) patterns
2. **Work/School Locations**: Validate location assignments
3. **Commute Accessibility**: Calculate work/school accessibility measures  
4. **Person Type Classification**: Assign detailed worker/student categories

## Model Outputs

### Primary Outputs
**Tour Frequency by Purpose**
- Number of work tours per person per day
- Number of school/university tours per person per day
- Tour generation probabilities and utilities

**Person-Level Tour Requirements**
- Mandatory tour obligations for downstream models
- Tour purpose and basic characteristics
- Scheduling constraints and requirements

### Derived Outputs
**Household Tour Load**
- Total mandatory tours by household
- Vehicle demand and scheduling coordination needs
- Time window constraints for discretionary activities

**Validation Metrics**  
- Tour generation rates by person type
- Multiple tour frequencies
- Comparison with observed work/school trip rates

### Integration with Tour Characteristics
Mandatory tours feed into:
- **Tour Destination Choice**: Where to conduct mandatory activities
- **Tour Mode Choice**: How to travel to mandatory activities  
- **Tour Time-of-Day**: When to schedule mandatory tours

## Advanced Features

### Flexible Work Modeling

**Telecommuting Integration**
- Model work-from-home probabilities
- Consider technology availability and job suitability
- Account for employer policies and personal preferences

**Compressed Work Weeks**  
- Handle 4/10 and 9/80 work schedules
- Adjust tour generation for non-standard schedules
- Consider schedule coordination within households

**Flexible Hours**
- Model variable start/end times
- Consider core hour requirements vs. flexible time
- Account for coordination with childcare and commuting

### Education System Modeling

**School Calendar Integration**
- Handle school holidays, teacher workdays, early release
- Model seasonal variation in school attendance
- Consider different academic calendars (year-round, traditional)

**Extracurricular Activities**
- Model after-school activity participation
- Generate additional school-purpose tours
- Consider transportation and coordination requirements

**Higher Education Flexibility**
- Model variable university schedules
- Handle online/hybrid course participation
- Consider research and work obligations

### Multi-Location Modeling

**Multiple Work Sites**
- Handle workers with multiple job locations
- Model frequency of visits to each site
- Consider travel between work locations

**Multiple School Sites**  
- Handle students attending multiple campuses
- Model shared time between institutions
- Consider specialized programs and resources

## Implementation Considerations

### Calibration Challenges

**Limited Observed Data**
- Tour frequency data less available than trip frequency
- Need to infer tours from trip patterns
- Validation against employment/enrollment rates

**Temporal Variation**
- Model represents average day, not specific weekdays
- Handle seasonal variation in work/school patterns
- Consider holiday and vacation effects

**Policy Sensitivity**
- Ensure reasonable response to telecommuting policies
- Validate sensitivity to transportation improvements
- Test response to schedule flexibility changes

### Configuration Parameters

**Person Type Definitions**
- Employment category classifications  
- Student type and grade level groupings
- Worker subcategories (management, service, etc.)

**Tour Frequency Limits**
- Maximum number of tours per person per purpose
- Threshold probabilities for multiple tours
- Handling of special cases (multiple jobs, campuses)

**Utility Function Specification**
- Person-type specific parameters
- Distance/time sensitivity parameters
- Schedule flexibility coefficients

## Calibration and Validation

### Parameter Estimation
**Data Sources**
- Regional household travel surveys
- Employment surveys and workplace studies
- School district attendance and schedule data
- Time-use surveys showing work/school patterns

**Key Relationships to Calibrate**
- Work tour rates by employment type and schedule
- School tour rates by student type and grade
- Multiple tour frequencies for flexible workers/students
- Response to commute distance and accessibility

### Validation Metrics
**Tour Generation Validation**
- Work tour rates match employment survey data
- School tour rates consistent with enrollment data
- Multiple tour patterns reasonable for occupations

**Person Type Validation**
- Full-time vs. part-time worker differences
- Student tour patterns by education level  
- Consistency with commute mode choice data

**Sensitivity Validation**
- Reasonable response to telecommuting scenarios
- Appropriate sensitivity to commute distance changes
- Realistic effects of schedule flexibility policies

## Common Issues and Troubleshooting

### Tour Generation Problems
**Symptoms**: Unrealistic tour generation rates
**Causes**: Incorrect person type assignments, poor employment data
**Solutions**: Validate person type definitions, check employment/enrollment data

### Multiple Tour Issues
**Symptoms**: Too many or too few multiple tours
**Causes**: Incorrect utility specifications, occupation misclassification  
**Solutions**: Review occupation categories, adjust multiple tour utilities

### Integration Problems
**Symptoms**: Inconsistencies with CDAP or downstream models
**Causes**: Data format mismatches, missing coordination
**Solutions**: Validate data interfaces, check model sequencing

## Usage Examples

### Basic Tour Generation
```python
# Configure mandatory tours model
mandatory_config = {
    "person_types": ["FT_worker", "PT_worker", "university", "K12_student"],
    "max_tours": 3,
    "segmentation": "detailed_occupation",
    "telecommute_modeling": True
}

# Generate mandatory tours
tour_results = mandatory_tours_model.run(
    persons=person_data,
    cdap_results=cdap_results,
    accessibility=work_school_accessibility,
    config=mandatory_config
)
```

### Policy Scenario Analysis
```python
# Test telecommuting policy scenario
baseline_tours = run_mandatory_tours(baseline_telecommute_rate=0.1)
policy_tours = run_mandatory_tours(policy_telecommute_rate=0.3)

# Compare tour generation impacts  
tour_reduction = baseline_tours.mean() - policy_tours.mean()
print(f"Telecommuting policy reduces tours by {tour_reduction:.2f} per person")
```

## Integration with Other Models

### Upstream Dependencies
**CDAP Model**
- Determines which persons have mandatory activity patterns
- Provides household coordination context

**Auto Ownership**
- Vehicle availability affects tour generation feasibility
- Influences commute mode and tour scheduling

### Downstream Dependencies
**Tour Destination Choice**
- Uses mandatory tour generation results
- Determines specific work/school locations for tours

**Tour Mode Choice**
- Mandatory tours require mode choice decisions
- Tour characteristics affect mode choice utilities

**At-Work Subtours**
- Work tours enable potential at-work subtours
- Multiple work locations affect subtour patterns

## Advanced Applications

### Workforce Analysis
**Employment Center Impacts**
- How employment location affects tour generation
- Transportation access effects on work participation

**Flexible Work Policies**
- Impact of telecommuting on travel demand
- Compressed work week effects on peak period travel

### Education Planning
**School Transportation Policy**
- Effects of school choice on tour generation
- Transportation service impacts on attendance

**University Development**
- Campus accessibility effects on enrollment and tours
- Mixed-use development impacts on student travel

---

!!! tip "Foundation for Tour Models"
    Mandatory tours provide the foundation for all other tour models. Ensure these generation rates are well-calibrated before proceeding to tour characteristic models.

!!! note "Policy Sensitivity"
    This model is key for analyzing telecommuting, flexible work, and transportation demand management policies. Careful calibration of policy variables is essential.

**Related Components:**
- [CDAP](cdap.md) - Provides mandatory activity pattern inputs
- [Tour Destination](tour-destination.md) - Uses mandatory tour generation results
- [At-Work Subtours](at-work-subtours.md) - Depends on work tour generation

*Last updated: September 26, 2025*
