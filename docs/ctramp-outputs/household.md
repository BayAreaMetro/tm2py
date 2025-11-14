# Household Output File Specification

**File:** `household.csv`  
**Level:** One record per household  
**Purpose:** Contains household-level attributes and choice model results

## File Structure

```csv
hh_id,home_mgra,income,autos,automated_vehicles,transponder,pre_et_cdap_pattern,cdap_pattern,jtf_choice,sampleRate,size,workers
```

## Field Definitions

### Core Identifiers

| Field | Type | Description | Constraints |
|-------|------|-------------|-------------|
| `hh_id` | Integer | Unique household identifier | Required, Unique, Positive |
| `home_mgra` | Integer | Household residence MGRA | Required, Valid MGRA ID |

### Household Characteristics

| Field | Type | Description | Constraints |
|-------|------|-------------|-------------|
| `size` | Integer | Total household members | 1-15 persons |
| `workers` | Integer | Number of workers in household | 0 ≤ workers ≤ size |
| `income` | Integer | Household income category | 1-4 (see [Income Categories](#income-categories)) |

### Vehicle Ownership

| Field | Type | Description | Constraints |
|-------|------|-------------|-------------|
| `autos` | Integer | Total vehicles owned | 0-6+ vehicles |
| `automated_vehicles` | Integer | Number of autonomous vehicles | 0 ≤ av ≤ autos |
| `transponder` | Integer | Has toll transponder | 0=No, 1=Yes |

### Activity Patterns

| Field | Type | Description | Constraints |
|-------|------|-------------|-------------|
| `pre_et_cdap_pattern` | String | Pre-employment/transit CDAP pattern | See [CDAP Patterns](#cdap-patterns) |
| `cdap_pattern` | String | Coordinated Daily Activity Pattern | See [CDAP Patterns](#cdap-patterns) |
| `jtf_choice` | Integer | Joint Tour Frequency choice | See [Joint Tour Frequency](#joint-tour-frequency) |

### Model Administration

| Field | Type | Description | Constraints |
|-------|------|-------------|-------------|
| `sampleRate` | Float | Sample expansion factor | Positive |

## Data Dictionaries

### Income Categories

Based on CTRAMP model structure, household income is categorized as:

| Code | Range | Description |
|------|-------|-------------|
| 1 | $0 - $29,999 | Low Income |
| 2 | $30,000 - $59,999 | Lower Middle Income |
| 3 | $60,000 - $99,999 | Upper Middle Income |
| 4 | $100,000+ | High Income |

*Note: Exact dollar thresholds may vary by model implementation and base year*

### CDAP Patterns

Coordinated Daily Activity Pattern represents the daily activity schedule for each household member:

**Pattern Notation:**
- `M` = Mandatory activity (work/school)
- `N` = Non-mandatory activity (shopping, personal business, etc.)
- `H` = Home (no out-of-home activity)

**Example Patterns:**
- `MN` = Mandatory tour + Non-mandatory tour
- `MH` = Mandatory tour only  
- `NH` = Non-mandatory tour only
- `HH` = Home all day

**Multi-Person Examples:**
- `MM_NH` = Person 1: Mandatory+Mandatory, Person 2: Non-mandatory+Home
- `MH_MN_HH` = 3-person household with varying activity levels

### Joint Tour Frequency

Result of joint tour frequency model indicating household's propensity for joint activities:

| Code | Description | Joint Tours |
|------|-------------|-------------|
| 0 | No joint tours | 0 |
| 1 | One joint tour | 1 |
| 2 | Two joint tours | 2 |
| 3+ | Multiple joint tours | 3+ |

*Joint tours involve multiple household members traveling together*

## Survey Data Mapping

### From Household Survey Data

Common survey fields and their CTRAMP mappings:

| Survey Field | CTRAMP Field | Transformation |
|--------------|--------------|----------------|
| `household_id` | `hh_id` | Direct mapping |
| `home_taz` | `home_mgra` | Use TAZ→MGRA crosswalk |
| `hh_income_detailed` | `income` | Apply income brackets |
| `vehicles` | `autos` | Direct mapping (cap at 6) |
| `hh_size` | `size` | Direct mapping |
| `num_workers` | `workers` | Direct mapping |

### Income Bracket Conversion

```python
def map_income_category(income_dollars, base_year=2010):
    """Map detailed income to CTRAMP categories"""
    if income_dollars < 30000:
        return 1
    elif income_dollars < 60000:
        return 2
    elif income_dollars < 100000:
        return 3
    else:
        return 4
```

### Vehicle Ownership Processing

```python
def process_vehicles(total_vehicles, av_vehicles=0):
    """Process vehicle ownership for CTRAMP"""
    # Cap total vehicles
    autos = min(total_vehicles, 6)
    
    # Ensure AV count is consistent
    automated_vehicles = min(av_vehicles, autos)
    
    return autos, automated_vehicles
```

## Validation Rules

### Required Relationships
1. `workers` ≤ `size` (workers cannot exceed household size)
2. `automated_vehicles` ≤ `autos` (AVs are subset of total vehicles)
3. `home_mgra` must exist in model geography
4. CDAP pattern length matches household size

### Data Quality Checks

```sql
-- Check worker consistency
SELECT COUNT(*) FROM household 
WHERE workers > size;

-- Verify vehicle relationships  
SELECT COUNT(*) FROM household
WHERE automated_vehicles > autos;

-- Income category validation
SELECT income, COUNT(*) 
FROM household 
WHERE income NOT IN (1,2,3,4)
GROUP BY income;
```

## Example Records

```csv
hh_id,home_mgra,income,autos,automated_vehicles,transponder,pre_et_cdap_pattern,cdap_pattern,jtf_choice,sampleRate,size,workers
1001,15623,3,2,0,1,MH_NH,MH_NH,0,1.0,2,1
1002,15624,2,1,0,0,MM_HH_HH,MN_HH_HH,1,1.0,3,1  
1003,15625,4,3,1,1,MN,MN,0,1.0,2,2
```

## Common Issues

### Geographic Mismatches
- **Problem**: Survey TAZ doesn't map to valid MGRA
- **Solution**: Use geographic crosswalk files or assign to nearest MGRA

### Income Inconsistencies  
- **Problem**: Survey income ranges don't align with model categories
- **Solution**: Adjust bracket boundaries or use continuous income if available

### Vehicle Ownership Edge Cases
- **Problem**: Households with very high vehicle counts
- **Solution**: Cap at reasonable maximum (typically 6) for model stability

## Related Files

- **[Person Data](person.md)** - Individual characteristics within households
- **[Joint Tours](joint-tours.md)** - Implementation of `jtf_choice` results
- **[Data Dictionaries](data-dictionaries.md)** - Complete value definitions

---

*This specification is based on the CTRAMP HouseholdDataWriter implementation in travel-model-two*