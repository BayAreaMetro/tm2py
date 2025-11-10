# Volume Delay Functions (VDFs) in TM2PY

## Overview

Volume Delay Functions (VDFs) are mathematical relationships that calculate link travel times based on traffic volumes and link characteristics in the TM2PY highway assignment model. The system uses different function types for different facility classes, incorporating both basic congestion effects and reliability factors.

## VDF Implementation Structure

### Function Assignment by Facility Type

VDF numbers are assigned to network links based on the `@ft` (facility type) attribute:

```python
# From highway_network.py
link.volume_delay_func = int(link["@ft"])
# Special case: facility type 99 is remapped to type 7
if link.volume_delay_func == 99:
    link.volume_delay_func = 7
```

### VDF Categories

| VDF Numbers | Function Type | Facility Types | Usage |
|-------------|---------------|----------------|-------|
| 1, 2 | BPR | Freeways | High-speed, limited access |
| 3,4,5,7,8,10,11,12,13,14 | Akcelik | Arterials & Local Roads | Intersection-controlled facilities |
| 8 | Fixed | Special Cases | Constant travel time |
| 99 | Remapped to 7 | Legacy | Converted to local road type |

## Mathematical Formulations

### 1. BPR Function (VDF 1, 2)

**Base Formula**:

```text
Travel_Time = el1 * (1 + 0.20 * ((Volume/Capacity)/0.75)^6)
```

**Code Implementation**:

```python
bpr_tmplt = "el1 * (1 + 0.20 * (put((volau + volad)/el2)/0.75) ** 6)"
```

**Parameters**:

- `el1`: Free-flow travel time (minutes)
- `el2`: Link capacity (vehicles/hour)
- `volau + volad`: Total traffic volume (vehicles/hour)
- `0.20`: Congestion coefficient
- `6`: Congestion exponent
- `0.75`: Capacity utilization factor

### 2. Akcelik Function (VDF 3,4,5,7,10,11,12,13,14)

**Base Formula**:

```text
Travel_Time = el1 + 60 * (0.25 * (V/C - 1 + √((V/C - 1)² + el3 * V/C)))
```

**Code Implementation**:

```python
akcelik_tmplt = (
    "(el1 + 60 * (0.25 * (put((volau + volad)/el2) - 1 + "
    "((get(1) - 1) ** 2 + el3 * get(1)) ** 0.5)))"
)
```

**Parameters**:

- `el1`: Free-flow travel time (minutes)
- `el2`: Link capacity (vehicles/hour)  
- `el3`: Akcelik delay parameter (`@ja`)
- `volau + volad`: Total traffic volume (vehicles/hour)
- `0.25`: Akcelik coefficient
- `60`: Conversion factor (seconds to minutes)

**Akcelik Parameter Calculation**:

```python
# From highway_network.py
if link.volume_delay_func in akcelik_vdfs and link["@free_flow_speed"] > 0:
    dist = link.length
    critical_speed = critical_speed_map[link["@capclass"]]
    t_c = dist / critical_speed
    t_o = dist / link["@free_flow_speed"]
    link["@ja"] = 16 * (t_c - t_o) ** 2
```

Where:

- `t_c`: Travel time at critical speed
- `t_o`: Free-flow travel time
- `@ja`: Akcelik delay parameter

### 3. Fixed Function (VDF 8)

**Formula**:

```text
Travel_Time = el1
```

**Code Implementation**:

```python
fixed_tmplt = "el1"
```

## Reliability Factors

Both BPR and Akcelik functions include reliability adjustments that modify travel times based on volume-to-capacity ratios and level-of-service thresholds.

### Reliability Formula Structure

```text
Final_Travel_Time = Base_Travel_Time * (1 + el4 + LOS_Penalties)
```

Where:

- `el4`: Static reliability factor (`@static_rel`)
- `LOS_Penalties`: Dynamic penalties based on volume/capacity ratios

### Freeway Reliability Parameters

Applied to VDF 1, 2 (BPR functions):

| Level of Service | V/C Threshold | Penalty Factor |
|------------------|---------------|----------------|
| LOS C | 0.7 | 0.2429 |
| LOS D | 0.8 | 0.1705 |
| LOS E | 0.9 | -0.2278 |
| LOS FL | 1.0 | -0.1983 |
| LOS FH | 1.2 | 1.022 |

### Arterial Reliability Parameters

Applied to VDF 3,4,5,7,10,11,12,13,14 (Akcelik functions):

| Level of Service | V/C Threshold | Penalty Factor |
|------------------|---------------|----------------|
| LOS C | 0.7 | 0.1561 |
| LOS D | 0.8 | 0.0 |
| LOS E | 0.9 | 0.0 |
| LOS FL | 1.0 | -0.449 |
| LOS FH | 1.2 | 0.0 |

### Reliability Calculation

```python
reliability_tmplt = (
    "* (1 + el4 + "
    "( {factor[LOS_C]} * ( ((volau + volad)/el2).min.1.5 - {threshold[LOS_C]} + 0.01 ) ) * (((volau + volad)/el2) .gt. {threshold[LOS_C]})"
    "+ ( {factor[LOS_D]} * ( ((volau + volad)/el2).min.1.5 - {threshold[LOS_D]} + 0.01 )  ) * (((volau + volad)/el2) .gt. {threshold[LOS_D]})"
    "+ ( {factor[LOS_E]} * ( ((volau + volad)/el2).min.1.5 - {threshold[LOS_E]} + 0.01 )  ) * (((volau + volad)/el2) .gt. {threshold[LOS_E]})"
    "+ ( {factor[LOS_FL]} * ( ((volau + volad)/el2).min.1.5 - {threshold[LOS_FL]} + 0.01 )  ) * (((volau + volad)/el2) .gt. {threshold[LOS_FL]})"
    "+ ( {factor[LOS_FH]} * ( ((volau + volad)/el2).min.1.5 - {threshold[LOS_FH]} + 0.01 )  ) * (((volau + volad)/el2) .gt. {threshold[LOS_FH]})"
    ")"
)
```

## Implementation Details

### Function Creation Process

VDFs are created during scenario initialization in `create_tod_scenarios.py`:

```python
# Create BPR functions for freeways
for f_id in ["fd1", "fd2"]:
    if emmebank.function(f_id):
        emmebank.delete_function(f_id)
    emmebank.create_function(
        f_id, bpr_tmplt + reliability_tmplt.format(**parameters["freeway"])
    )

# Create Akcelik functions for arterials
for f_id in ["fd3", "fd4", "fd5", "fd6", "fd7", "fd9", "fd10", "fd11", "fd12", "fd13", "fd14", "fd99"]:
    if emmebank.function(f_id):
        emmebank.delete_function(f_id)
    emmebank.create_function(
        f_id, akcelik_tmplt + reliability_tmplt.format(**parameters["road"])
    )

# Create fixed function
if emmebank.function("fd8"):
    emmebank.delete_function("fd8")
emmebank.create_function("fd8", fixed_tmplt)
```

### Link Attribute Requirements

VDFs require the following link attributes:

| Attribute | Description | Usage |
|-----------|-------------|-------|
| `@ft` | Facility type | Determines VDF number |
| `@capacity` | Link capacity | Denominator in V/C calculations |
| `@free_flow_time` | Free-flow travel time | Base travel time (el1) |
| `@ja` | Akcelik delay parameter | el3 in Akcelik functions |
| `@static_rel` | Static reliability factor | el4 in reliability calculations |
| `volau` | Auto volume | Primary volume component |
| `volad` | Additional demand | Secondary volume component |

### Capacity Calculation

Link capacity is calculated in `highway_network.py`:

```python
cap_lanehour = capacity_map[link["@capclass"]]
link["@capacity"] = cap_lanehour * period_capacity_factor * link["@lanes"]
```

Where:

- `capacity_map`: Lookup table by capacity class
- `period_capacity_factor`: Time period adjustment factor
- `@lanes`: Number of lanes

## Emme Function Parameters

The VDF system uses Emme's extra function parameters:

```python
emmebank.extra_function_parameters.el1 = "@free_flow_time"
emmebank.extra_function_parameters.el2 = "@capacity" 
emmebank.extra_function_parameters.el3 = "@ja"
emmebank.extra_function_parameters.el4 = "@static_rel"
```

## Usage in Assignment

During traffic assignment, the VDFs calculate link travel times iteratively:

1. **Initial Assignment**: Uses free-flow times
2. **Volume Updates**: Assignment produces new link volumes
3. **Time Calculation**: VDFs compute new travel times based on volumes
4. **Convergence Check**: Process repeats until stable solution
5. **Final Results**: Both volumes and times are stored

## Configuration Files

VDF parameters can be customized through:

- **Capacity Classes**: `highway.capclass_lookup` in configuration
- **Reliability Factors**: Hard-coded in `create_tod_scenarios.py`
- **Function Assignments**: Based on network `@ft` attribute

## Related Documentation

- **[Highway Assignment Overview](index.md)** - Main assignment system documentation
- **[Highway Value of Time](highway_value_of_time.md)** - Generalized cost calculations
- **[Network Summary Component](../network_summary.md)** - Assignment result analysis

## Technical Notes

**Transportation Theory Context**: The BPR function is a classic volume-delay relationship from the 1950s Bureau of Public Roads research. The Akcelik function incorporates more sophisticated intersection delay modeling, particularly relevant for arterial streets with traffic signals.

**Computational Implementation**: The use of `put()` and `get()` functions in the expressions enables nested calculations within Emme's function evaluation system, avoiding computational complexity issues with deeply nested expressions.

---

*Source Files*:

- `tm2py/components/network/create_tod_scenarios.py` (lines 115-210)
- `tm2py/components/network/highway/highway_network.py` (lines 250-280)
- `tm2py/components/network/highway/highway_assign.py` (lines 350-390)

*Last Updated: November 2025*  
*Model Version: Travel Model Two v2.2*
 
 