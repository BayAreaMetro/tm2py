# Value of Time in Highway Assignment

## Overview

The Travel Model Two (TM2) highway assignment system uses value of time (VOT) parameters to convert monetary costs into time-equivalent units for generalized cost calculations. This enables the traffic assignment algorithm to find equilibrium paths that balance travel time, distance, and monetary costs (tolls and operating costs).

## Value of Time Parameters

### Primary Configuration

**Default Value of Time**: `$18.93/hour` (2010 dollars)

- **Source**: Defined in `tm2py.config.py` 
- **Configuration Parameter**: `highway.classes[].value_of_time`
- **Units**: US dollars per hour (2010 base year)
- **Application**: Used across all highway assignment components

### Supporting Parameters

**Auto Operating Cost**: `17.23 cents/mile` (2010 dollars)
- **Configuration Parameter**: `highway.classes[].operating_cost_per_mile`
- **Purpose**: Vehicle operating cost for distance-based cost calculations

**Conversion Factor**: `0.6`
- **Purpose**: Converts dollars per hour to minutes per cent
- **Formula**: `perception_factor = 0.6 / value_of_time`

## Implementation in Highway Assignment

### 1. Link Cost Calculation

In the MAZ-to-MAZ assignment (`highway_maz.py`):

```python
vot = self.config.value_of_time  # $18.93/hour
op_cost = self.config.operating_cost_per_mile  # 17.23 cents/mile

# Link cost formula (in minutes):
link_cost = travel_time + (0.6 / vot) * (link_length * op_cost)
```

**Components**:
- `travel_time`: Link travel time in minutes
- `(0.6 / vot)`: Conversion factor (≈ 0.0317 minutes/cent)
- `link_length * op_cost`: Distance-based operating cost in cents

### 2. Traffic Assignment Perception Factor

In the EMME traffic assignment specification (`highway_emme_spec.py`):

```python
perception_factor = 0.6 / self.class_config.value_of_time
# With default VOT: 0.6 / 18.93 = 0.0317 minutes/$0.01
```

**Purpose**: 
- Converts link costs (in cents) to time units for path building
- Used in EMME's SOLA traffic assignment algorithm
- Ensures consistent generalized cost calculations

### 3. Drive Access Skim Generation

In transit drive access calculations (`drive_access_skims.py`):

```python
value_of_time = 18.93  # $/hour
vot_factor = 0.6 / value_of_time  # minutes/cents conversion

# Generalized cost calculation:
total_cost = drive_time + walk_time + vot_factor * (operating_cost + toll_cost)
```

## Class-Specific Configurations

### Vehicle Class Differentiation

The system supports different VOT values for different vehicle classes:

| Class Type | Default VOT | Description |
|------------|-------------|-------------|
| Drive Alone (DA) | $18.93/hour | Single occupant vehicles |
| Shared Ride 2 (SR2) | $18.93/hour | Two-person carpools |
| Shared Ride 3+ (SR3) | $18.93/hour | Three+ person carpools |
| Trucks | Configurable | Commercial vehicles |

### Configuration Structure

```toml
[[highway.classes]]
name = "da"
description = "drive alone"
value_of_time = 18.93  # $ / hr
operating_cost_per_mile = 17.23  # cents / mile
mode_code = "d"
excluded_links = ["is_sr2", "is_sr3"]
```

## Mathematical Framework

### Generalized Cost Function

The highway assignment uses a generalized cost function that combines time and monetary costs:

```
GC = α₁ × Time + α₂ × Distance + α₃ × Tolls + α₄ × Operating_Cost
```

Where:
- `α₁ = 1.0` (time coefficient)
- `α₂ = (0.6 / VOT) × operating_cost_per_mile` (distance coefficient)
- `α₃ = (0.6 / VOT)` (toll coefficient)
- `α₄ = (0.6 / VOT)` (operating cost coefficient)

### Unit Consistency

**Time Units**: Minutes
**Distance Units**: Miles (converted from feet in network)
**Cost Units**: Cents (2010 dollars)
**VOT Units**: Dollars per hour (2010 dollars)

**Conversion Relationships**:
- `1 dollar = 100 cents`
- `1 hour = 60 minutes`
- `Conversion factor = 60 minutes/hour ÷ 100 cents/dollar = 0.6`

## Equilibrium Assignment Process

### Path Building

1. **Initialize**: Calculate link costs using VOT-adjusted generalized cost
2. **Shortest Paths**: Find minimum generalized cost paths for each O-D pair
3. **Load Traffic**: Assign demand to shortest paths
4. **Update Costs**: Recalculate link travel times based on volume-delay functions
5. **Iterate**: Repeat until convergence

### Convergence Criteria

The assignment reaches equilibrium when the difference in generalized costs between iterations falls below specified thresholds, ensuring that:
- No traveler can unilaterally reduce their generalized cost by changing routes
- The system reflects realistic trade-offs between time and money

## Sensitivity and Validation

### VOT Sensitivity

The highway assignment results are sensitive to VOT values:
- **Higher VOT**: Greater willingness to pay tolls to save time
- **Lower VOT**: More price-sensitive route choice, avoiding tolls
- **Impact**: Affects traffic distribution on toll vs. free facilities

### Calibration Considerations

**Observed Behavior**: VOT should reflect regional travel behavior patterns
**Income Distribution**: May warrant income-stratified VOT values
**Facility Usage**: Validation against observed toll facility usage patterns

## Technical Implementation

### Code Structure

```
tm2py/components/network/highway/
├── highway_assign.py      # Main assignment coordination
├── highway_maz.py         # MAZ-to-MAZ shortest paths  
├── highway_emme_spec.py   # EMME assignment specification
├── drive_access_skims.py  # Transit drive access costs
└── toll_choice.py         # Toll vs. non-toll choice
```

### Key Classes and Methods

**HighwayAssignmentConfig**: Configuration data class for VOT parameters
**EmmeHighwayClassSpec**: Generates EMME assignment specifications
**HighwayMAZSkims**: Calculates MAZ-to-MAZ generalized costs

## Related Documentation

- **[CT-RAMP Value of Time](../ctramp/VALUE_OF_TIME_ANALYSIS.md)**: Household-level VOT for mode choice
- **[Highway Assignment](highway_assignment.md)**: Complete highway assignment documentation
- **[Configuration Guide](../configuration.md)**: Model configuration parameters

---

*Last Updated: November 2025*  
*Model Version: Travel Model Two v2.2*  
*Authors: MTC Staff & GitHub Copilot*