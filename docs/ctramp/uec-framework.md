# UEC Framework Documentation

## Overview

The Utility Expression Calculator (UEC) is the core mathematical framework that drives all choice modeling in CT-RAMP. UECs define the utility functions for discrete choice models, enabling the calculation of probabilities for various alternatives in travel decision-making processes.

## What is UEC?

The UEC (Utility Expression Calculator) framework provides:

- **Mathematical Foundation**: Systematic calculation of utility values for choice alternatives
- **Flexible Specification**: Configurable utility expressions through external control files
- **Model Integration**: Seamless integration with Java choice model implementations
- **Performance Optimization**: Efficient calculation for large-scale microsimulation

## UEC Architecture

### Core Components

```
UEC Framework
├── UEC Control Files (.xls)
│   ├── Alternative definitions
│   ├── Variable specifications  
│   ├── Coefficient values
│   └── Utility expressions
├── UEC Engine (Java)
│   ├── Expression parser
│   ├── Data interface
│   ├── Calculation engine
│   └── Result aggregation
└── Model Integration
    ├── Choice model interface
    ├── Data preparation
    └── Probability calculation
```

### Key Classes and Interfaces

#### UEC Core Classes

- **`UtilityExpressionCalculator`**: Main UEC calculation engine
- **`IndexValues`**: Manages alternative and zone indices
- **`TableDataSet`**: Data structure for input data
- **`ChoiceModelApplication`**: Integrates UEC with choice models

#### Model Integration Classes

- **`TourModeChoiceModel`**: Mode choice using UEC framework
- **`DestinationChoiceModel`**: Destination choice with UEC utilities
- **`StopLocationChoiceModel`**: Intermediate stop location choice
- **`AutoOwnershipChoiceModel`**: Household auto ownership decisions

## UEC Control File Structure

UEC control files are Excel spreadsheets (.xls) that define utility expressions for choice models.

### Standard Worksheet Structure

#### 1. **Model Sheet** (`Model`)
Defines overall model parameters and alternative structure.

| Column | Description | Example |
|--------|-------------|---------|
| `ModelDescription` | Text description of model | "Tour Mode Choice Model" |
| `ModelType` | Type of choice model | "MNL" (Multinomial Logit) |
| `Alternatives` | Number of alternatives | 17 |
| `Utility` | Utility worksheet reference | "Utility" |
| `ExpUtility` | Exponentiated utility reference | "ExpUtility" |

#### 2. **Alternatives Sheet** (`Alternatives`)
Lists all choice alternatives with their properties.

| Column | Description | Example |
|--------|-------------|---------|
| `alt` | Alternative number | 1, 2, 3, ... |
| `altName` | Alternative name | "DRIVEALONE", "SHARED2" |
| `available` | Availability expression | "1" or conditional expression |

#### 3. **Utility Sheet** (`Utility`)
Defines utility expressions for each alternative.

| Column | Purpose |
|--------|---------|
| `Expression` | Mathematical expression using variables |
| `alt` | Alternative number this expression applies to |
| `Description` | Human-readable description |

#### 4. **Variables Sheet** (`Variables`)
Defines input variables and their data sources.

| Column | Description |
|--------|-------------|
| `Field` | Variable name in expression |
| `Description` | Variable description |
| `Filter` | Data filtering conditions |

### Expression Syntax

#### Basic Operators
- **Arithmetic**: `+`, `-`, `*`, `/`, `^` (power)
- **Logical**: `&&` (AND), `||` (OR), `!` (NOT)
- **Comparison**: `==`, `!=`, `<`, `<=`, `>`, `>=`
- **Conditional**: `condition ? true_value : false_value`

#### Functions
- **Mathematical**: `ln()`, `log()`, `exp()`, `max()`, `min()`
- **Logical**: `if()`, `and()`, `or()`
- **Utility**: `logsum()` for nested logit models

#### Variable References
- **Direct Variables**: `@variable_name`
- **Alternative-Specific**: `@variable_name[alt]`
- **Zone Variables**: `@variable_name[orig]`, `@variable_name[dest]`
- **Matrix Variables**: `@matrix_name[orig][dest]`

### Example UEC Expression

```
// Tour mode choice utility expression
@mode_constant +
@ivt_coefficient * @in_vehicle_time +
@cost_coefficient * (@auto_operating_cost + @parking_cost) * @cost_sensitivity +
(@female == 1 ? @female_walk_constant : 0) * (@mode_is_walk == 1 ? 1 : 0) +
@income_coefficient * ln(@household_income / 30000)
```

## Model-Specific UEC Applications

### 1. Tour Mode Choice UEC

**File**: `TourModeChoice.xls`

**Key Components**:
- 17 transportation mode alternatives
- Hierarchical structure (motorized vs. non-motorized)
- Level-of-service variables (time, cost, reliability)
- Socio-demographic interactions
- Built environment factors

**Alternative Structure**:
```
1-3:   Drive Alone (Free, Pay, Transit Access)
4-6:   Shared Ride 2 (Free, Pay, Transit Access)  
7-9:   Shared Ride 3+ (Free, Pay, Transit Access)
10:    Walk
11:    Bike
12:    Walk-Transit-Walk
13-14: Park & Ride / Kiss & Ride
15-17: Taxi, TNC, School Bus
```

**Utility Categories**:
- **Mode Constants**: Base preference for each mode
- **Level-of-Service**: Time, cost, and reliability impacts
- **Personal Characteristics**: Age, gender, income interactions
- **Spatial Factors**: Density, accessibility measures
- **Tour Characteristics**: Purpose, time-of-day, duration

### 2. Destination Choice UEC

**File**: `DestinationChoice.xls`

**Components**:
- Size variables by activity type
- Accessibility measures  
- Distance decay functions
- Mode choice logsums
- Land use interaction terms

**Utility Structure**:
```
Utility = Size_Term + Accessibility_Term + Distance_Term + Personal_Interaction_Terms
```

**Size Variables**:
- Employment by industry (work destinations)
- Retail/service employment (shopping destinations)  
- Household population (social destinations)
- School enrollment (education destinations)

### 3. Auto Ownership UEC

**File**: `AutoOwnership.xls`

**Alternatives**: 0, 1, 2, 3+ vehicles per household

**Utility Factors**:
- **Household Characteristics**: Income, size, workers, life cycle
- **Residential Location**: Density, transit accessibility  
- **Regional Accessibility**: Auto vs. transit accessibility ratio
- **Costs**: Auto ownership and operating costs

### 4. Coordinated Daily Activity Pattern (CDAP) UEC

**File**: `CoordinatedDailyActivityPattern.xls`

**Pattern Types**:
- **M**: Mandatory activity (work/school)
- **N**: Non-mandatory activity only  
- **H**: Home-based (no out-of-home activity)

**Coordination Rules**:
- Household-level interaction terms
- Joint travel propensities
- Activity scheduling constraints

## UEC Data Interface

### Input Data Sources

#### 1. **Household/Person Data**
- Synthetic population characteristics
- Model-generated choices (auto ownership, etc.)
- Person type classifications

#### 2. **Zone Data** 
- Land use characteristics (MAZ level)
- Built environment measures
- Accessibility calculations

#### 3. **Level-of-Service Data**
- Highway skim matrices (time, distance, cost)
- Transit skim matrices (components by access mode)
- Walk/bike distance matrices

#### 4. **Model Parameters**
- Coefficients from model estimation
- Alternative-specific constants
- Interaction parameters

### Data Preparation

#### Variable Creation
```java
// Example variable calculation in UEC data setup
double walkTime = walkDistance / walkSpeed;
double transitAccess = walkTime <= maxWalkTime ? 1 : 0;
double costPerMile = fuelCost / fuelEfficiency + vehicleOperatingCost;
```

#### Matrix Lookup
```java
// Level-of-service matrix access
double autoTime = autoTimeMatrix.getValueAt(origin, destination);
double transitIVTT = transitMatrix.getValueAt(origin, destination, "IVTT");
```

## UEC Execution Process

### 1. Model Initialization
```java
UtilityExpressionCalculator uec = new UtilityExpressionCalculator(
    controlFile,        // UEC .xls file
    modelSheet,         // Sheet name in control file
    dataSheet,          // Data reference sheet
    alternativesSheet   // Alternatives definition sheet
);
```

### 2. Data Preparation
```java
IndexValues index = new IndexValues();
index.setOriginZone(tour.getOriginMAZ());
index.setDestinationZone(tour.getDestinationMAZ());
index.setHouseholdId(tour.getHouseholdId());

// Set decision-maker attributes
uec.setHouseholdObject(household);
uec.setPersonObject(person);
uec.setTourObject(tour);
```

### 3. Utility Calculation
```java
// Calculate utilities for all alternatives
double[] utilities = uec.solve(index, person, alternativesAvailable);

// Calculate choice probabilities
double[] probabilities = calculateProbabilities(utilities);
```

### 4. Choice Simulation
```java
// Make random choice based on probabilities
Random random = new Random(seed);
int chosenAlternative = makeRandomChoice(probabilities, random.nextDouble());
```

## Advanced UEC Features

### Nested Logit Implementation

For hierarchical choice structures:

```java
// Compute lower-level logsums
double motorizedLogsum = computeLogsum(motorizedUtilities, theta);
double nonMotorizedLogsum = computeLogsum(nonMotorizedUtilities, theta);

// Include logsums in upper-level utilities
upperUtility[MOTORIZED] += lambda * motorizedLogsum;
upperUtility[NON_MOTORIZED] += lambda * nonMotorizedLogsum;
```

### Alternative Availability

```java
// Check alternative availability
boolean[] available = new boolean[numAlts];
available[BIKE] = (bikeDistance <= maxBikeDistance) && (age >= minBikeAge);
available[TRANSIT] = (walkToTransit <= maxWalkAccess);
available[DRIVE_ALONE] = (numDrivers > 0) && (numAutos > 0);
```

### Segmentation and Interactions

```java
// Person type segmentation
String personType = getPersonType(age, employment, student);
double coefficient = getCoefficient("time_coefficient", personType);

// Income interaction  
double incomeSensitivity = household.getIncome() < 30000 ? 
    lowIncomeCoeff : highIncomeCoeff;
```

## Performance Optimization

### Computational Efficiency

#### 1. **Matrix Caching**
- Pre-load frequently accessed matrices
- Use efficient sparse matrix representations
- Cache computed accessibility measures

#### 2. **Expression Optimization** 
- Minimize complex mathematical operations
- Pre-compute invariant terms
- Use lookup tables for complex functions

#### 3. **Memory Management**
- Reuse UEC objects across similar calculations
- Minimize object creation in loops
- Use primitive arrays for large datasets

### Parallel Processing

```java
// Parallelize UEC calculations across households
households.parallelStream().forEach(household -> {
    UtilityExpressionCalculator uec = getThreadLocalUEC();
    processTours(household, uec);
});
```

## UEC Debugging and Validation

### Common Issues

#### 1. **Expression Errors**
- Undefined variable references
- Division by zero conditions  
- Missing data handling
- Syntax errors in expressions

#### 2. **Data Inconsistencies**
- Mismatched zone numbering
- Missing matrix values
- Inconsistent alternative numbering

#### 3. **Model Specification**
- Unreasonable coefficient values
- Identification problems  
- Alternative availability conflicts

### Debugging Tools

#### UEC Trace Output
```java
uec.setDebugOutput(true);
uec.setTraceTarget(alternativeNumber);
// Produces detailed calculation trace
```

#### Utility Validation
```java
// Check utility ranges and distributions
double[] utilities = uec.solve(index, person, available);
validateUtilityRange(utilities, -50.0, 50.0);
checkForNaNValues(utilities);
```

## UEC Best Practices

### Model Development

1. **Incremental Development**: Build UEC expressions incrementally
2. **Coefficient Testing**: Test individual coefficients before integration
3. **Alternative Validation**: Verify all alternatives can be chosen
4. **Sensitivity Analysis**: Test model response to parameter changes
5. **Cross-Validation**: Compare results across similar market segments

### Maintenance and Updates

1. **Version Control**: Track UEC file changes systematically
2. **Documentation**: Document all variable definitions and sources
3. **Validation Scripts**: Develop automated validation procedures
4. **Performance Monitoring**: Track computation times and memory usage
5. **Regression Testing**: Verify model behavior after updates

### Code Integration

1. **Consistent Interfaces**: Standardize UEC calling patterns
2. **Error Handling**: Implement robust error checking and recovery
3. **Logging**: Provide detailed logging for debugging
4. **Configuration Management**: Externalize model parameters
5. **Unit Testing**: Test UEC calculations with known inputs/outputs

This UEC framework documentation provides the foundation for understanding and implementing the mathematical core of CT-RAMP's choice modeling system, enabling sophisticated travel behavior simulation through flexible utility specifications.