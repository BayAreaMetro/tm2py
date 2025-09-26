# Auto Ownership Model

!!! info "Model Purpose"
    The Auto Ownership Model determines the number of vehicles available to each household, establishing a fundamental constraint that influences all subsequent travel decisions. This is typically the first choice model executed in the CT-RAMP system.

## Model Overview

### Purpose and Role
The Auto Ownership Model predicts household vehicle ownership levels based on:

- **Household characteristics**: Size, income, workers, lifecycle stage
- **Accessibility**: Transit and non-motorized accessibility to activities  
- **Policy variables**: Parking costs, vehicle taxes, fuel prices
- **Spatial context**: Urban density, land use mix, walkability

### Key Behavioral Assumptions

**Rational Decision-Making**
: Households choose vehicle ownership levels that maximize their utility given their constraints and preferences

**Long-term Decision**
: Auto ownership is treated as a medium to long-term household decision that provides the context for daily travel choices

**Household-Level Choice**
: The decision is made at the household level considering all household members' needs and constraints

**Accessibility Trade-off**
: Households balance the cost and convenience of vehicle ownership against accessibility provided by other modes

## Model Structure

### Choice Alternatives
The model typically includes these ownership levels:

| Alternative | Description | Typical Households |
|-------------|-------------|-------------------|
| **0 vehicles** | No household vehicles | Urban, low-income, transit-accessible |
| **1 vehicle** | One household vehicle | Small households, mixed accessibility |
| **2 vehicles** | Two household vehicles | Multi-worker households, suburban |
| **3 vehicles** | Three household vehicles | Large households, low transit access |
| **4+ vehicles** | Four or more vehicles | Large, high-income, rural households |

### Utility Function Structure

The utility of each alternative typically includes:

**Household Demographics**
```
Utility = β₁ × HH_Size + β₂ × Workers + β₃ × Income + β₄ × Children + ...
```

**Accessibility Measures**
```
+ β₅ × Transit_Accessibility + β₆ × Walk_Accessibility + ...
```

**Spatial Variables**
```
+ β₇ × Population_Density + β₈ × Employment_Density + ...
```

**Cost Variables**  
```
+ β₉ × Parking_Cost + β₁₀ × Vehicle_Cost + ...
```

### Segmentation
The model often uses segmentation by:

- **Income categories**: Low, medium, high income households
- **Lifecycle stage**: Young adults, families with children, seniors
- **Geographic context**: Urban core, suburban, rural areas

## Technical Implementation

### Model Type
**Multinomial Logit Model** with household-level choice among discrete alternatives

### Utility Specification
Typical utility function components:

**Demographic Variables**
- Household size (positive for higher ownership)
- Number of workers (positive for higher ownership)  
- Household income (positive for higher ownership)
- Presence of children (mixed effects by age)
- Presence of seniors (negative for higher ownership)

**Accessibility Variables**  
- Transit accessibility logsum (negative for higher ownership)
- Walk accessibility to retail (negative for higher ownership)
- Auto accessibility advantage (positive for higher ownership)

**Land Use Variables**
- Population density (negative for higher ownership)
- Employment density (negative for higher ownership)
- Retail density (negative for higher ownership)
- Mixed land use measures (negative for higher ownership)

**Cost and Policy Variables**
- Parking cost at home (negative for higher ownership)
- Vehicle operating costs (negative for higher ownership)
- Transit service quality (negative for higher ownership)

### Sample Utility Function
```
V(0 vehicles) = β₀

V(1 vehicle) = β₁ + β₂×Workers + β₃×Income + β₄×HH_Size + 
                β₅×Transit_Logsum + β₆×Density + β₇×Parking_Cost

V(2 vehicles) = β₈ + β₉×Workers + β₁₀×Income + β₁₁×HH_Size + 
                 β₁₂×Transit_Logsum + β₁₃×Density + β₁₄×Parking_Cost

V(3+ vehicles) = β₁₅ + β₁₆×Workers + β₁₇×Income + β₁₈×HH_Size + 
                  β₁₉×Transit_Logsum + β₂₀×Density + β₂₁×Parking_Cost
```

## Data Requirements

### Input Data Sources

**Household Data**
- Household ID and demographics
- Number of workers and their characteristics
- Household income category
- Presence of children by age group
- Presence of seniors

**Person Data**
- Person age, gender, employment status
- Student status and school location
- Driver's license availability

**Zonal Data**
- Population and employment density
- Land use mix and walkability measures
- Parking cost and availability
- Transit service frequency

**Accessibility Data**
- Mode-specific accessibility measures
- Auto, transit, walk, bike logsums
- Level-of-service matrices

### Required Preprocessing
1. **Accessibility Calculation**: Compute mode-specific accessibility measures
2. **Density Calculation**: Calculate population and employment densities
3. **Cost Data**: Assemble parking and transportation costs
4. **Validation**: Ensure data completeness and consistency

## Model Outputs

### Primary Outputs
**Household Auto Ownership Level**
- Number of vehicles (0, 1, 2, 3, 4+) for each household
- Choice probabilities for each alternative
- Household-specific utility values

### Derived Outputs
**Accessibility Impacts**
- Auto ownership logsum for use in location choice models
- Vehicle availability indicator for person-level models
- Household mobility constraints

### Validation Metrics
**Aggregate Validation**
- Auto ownership distribution by income category
- Ownership rates by household size and workers
- Geographic variation in ownership rates

**Disaggregate Validation**
- Individual household ownership prediction accuracy
- Sensitivity to key policy variables
- Comparison with observed survey data

## Calibration and Validation

### Parameter Estimation
**Data Sources for Calibration**
- Regional household travel surveys
- American Community Survey (commute mode data)
- Consumer Expenditure Survey (vehicle ownership costs)
- Local parking cost surveys

**Estimation Approach**
- Maximum likelihood estimation using observed choices
- Cross-validation with independent datasets
- Sensitivity testing of key parameters

### Validation Standards
**Regional Benchmarks**
- Match observed ownership rates by geography
- Reproduce income-ownership relationships  
- Capture density effects on ownership

**Policy Sensitivity**
- Reasonable response to parking cost changes
- Appropriate sensitivity to transit improvements
- Realistic response to demographic changes

## Implementation Considerations

### Configuration Parameters
Key parameters for regional adaptation:

**Alternative Definitions**
- Maximum ownership level (3 vs 4+ vehicles)
- Income category definitions
- Geographic segmentation approach

**Utility Function Specification**
- Variable transformation (linear vs log)
- Interaction terms and segmentation
- Constant term values for each alternative

**Accessibility Integration**
- Logsum calculation methodology
- Network representation and impedance functions
- Time-of-day and mode specification

### Performance Optimization
**Computational Efficiency**
- Pre-calculate accessibility measures
- Use vectorized utility calculations
- Cache repeated calculations across households

**Memory Management**
- Stream household processing for large populations
- Efficient data structures for accessibility matrices
- Garbage collection optimization

## Common Issues and Troubleshooting

### Convergence Problems
**Symptoms**: Model produces unrealistic ownership distributions
**Causes**: Poor accessibility measures, incorrect parameter signs
**Solutions**: Validate accessibility calculations, check utility function signs

### Validation Failures  
**Symptoms**: Model doesn't match observed ownership patterns
**Causes**: Incorrect segmentation, missing variables, poor calibration data
**Solutions**: Refine segmentation scheme, add policy variables, improve calibration

### Performance Issues
**Symptoms**: Slow model execution, memory problems
**Causes**: Large accessibility matrices, inefficient calculations
**Solutions**: Optimize accessibility calculation, use sparse matrices, parallel processing

## Usage Examples

### Basic Model Run
```python
# Example model configuration
auto_ownership_config = {
    "alternatives": [0, 1, 2, 3, 4],
    "segmentation": "income_lifecycle",
    "accessibility_logsum": "mode_choice_logsum",
    "utility_specification": "full_specification.csv"
}

# Run auto ownership model
results = auto_ownership_model.run(
    households=household_data,
    accessibility=accessibility_data,
    config=auto_ownership_config
)
```

### Scenario Analysis
```python
# Test parking policy scenario
baseline_results = run_auto_ownership(base_parking_costs)
policy_results = run_auto_ownership(increased_parking_costs)

# Compare ownership distributions
ownership_change = policy_results.ownership_dist - baseline_results.ownership_dist
print(f"Policy reduces average ownership by {ownership_change.mean():.2f} vehicles")
```

## Integration with Other Models

### Downstream Dependencies
Models that use auto ownership results:

**Coordinated Daily Activity Pattern (CDAP)**
- Vehicle availability affects activity pattern feasibility
- Influences joint vs individual activity decisions

**Tour Generation Models**
- Auto availability affects tour frequency
- Enables longer-distance tours and activities

**Mode Choice Models**
- Auto ownership level affects mode choice utilities
- Determines auto availability for mode choice

### Feedback Mechanisms
**Accessibility Feedback Loop**
- Auto ownership affects overall accessibility
- Changed accessibility influences future ownership decisions
- Equilibrium achieved through iteration

## Advanced Features

### Policy Variables
The model can incorporate various policy levers:

**Pricing Policies**
- Congestion pricing effects
- Parking pricing policies  
- Vehicle registration fees

**Service Policies**
- Transit service improvements
- Bike infrastructure investments
- Car-sharing program availability

### Behavioral Extensions
**Household Coordination**
- Joint optimization of ownership and location
- Intra-household vehicle allocation
- Lifecycle-based ownership transitions

**Uncertainty Modeling**
- Stochastic parameter variation
- Confidence intervals on predictions
- Sensitivity analysis automation

---

!!! tip "Model Foundation"
    Auto ownership is the foundation for all subsequent CT-RAMP models. Ensure this model is well-calibrated before proceeding to other components, as errors here propagate through the entire system.

!!! note "Regional Adaptation"
    Auto ownership patterns vary significantly across regions. Local calibration using regional household travel survey data is essential for accurate modeling.

**Related Components:**
- [CDAP](cdap.md) - Uses auto ownership results for household coordination
- [Tour Mode Choice](tour-mode-choice.md) - Auto availability affects mode choice utilities
- [Accessibility](../data/inputs.md#accessibility-data) - Auto ownership affects accessibility calculations

*Last updated: September 26, 2025*
