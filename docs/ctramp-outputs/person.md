# Person Output File (person.csv)

## Overview
The `person.csv` file contains detailed person-level attributes and choice model results from the CTRAMP travel demand model. Each row represents one individual with their demographics, employment status, education level, and travel behavior characteristics.

## File Structure
- **Primary Key**: `hh_id` + `person_num`
- **Foreign Key**: `hh_id` (references household.csv)
- **Row Count**: One row per person in the synthetic population
- **File Size**: Typically 2-8 million rows depending on region

## Field Definitions

*Based on actual CTRAMP output file structure from model runs.*

### Identification Fields

| Field Name | Data Type | Description | Valid Values | Notes |
|------------|-----------|-------------|--------------|-------|
| `hh_id` | INTEGER | Household identifier | 1 to MAX_HOUSEHOLDS | Links to household.csv |
| `person_id` | INTEGER | Unique person identifier | Unique across all persons | May be hh_id * 10 + person_num |
| `person_num` | INTEGER | Person number within household | 1 to household size | Sequential numbering |

### Demographics

| Field Name | Data Type | Description | Valid Values | Notes |
|------------|-----------|-------------|--------------|-------|
| `age` | INTEGER | Person's age in years | 0 to 115+ | From synthetic population |
| `gender` | INTEGER | Person's gender | 1 = Male, 2 = Female | From ACS/Census data |
| `type` | INTEGER | Person type classification | 1-8 (see data dictionary) | Primary demographic category |

### Transportation Economics

| Field Name | Data Type | Description | Valid Values | Notes |
|------------|-----------|-------------|--------------|-------|
| `value_of_time` | DECIMAL(6,2) | Value of time ($/hour) | Positive number | For mode choice modeling |
| `transitSubsidy_choice` | INTEGER | Transit subsidy eligibility | 0 = No, 1 = Yes | Modeled choice |
| `transitSubsidy_percent` | DECIMAL(5,2) | Transit subsidy percentage | 0.0-100.0 | Discount on transit fares |
| `transitPass_choice` | INTEGER | Transit pass ownership | 0 = No, 1 = Yes | Monthly/annual pass |
| `reimb_pct` | DECIMAL(5,2) | Parking reimbursement % | 0.0-100.0 | Employer parking benefit |

### Employment and Industry

| Field Name | Data Type | Description | Valid Values | Notes |
|------------|-----------|-------------|--------------|-------|
| `naicsCode` | INTEGER | Industry classification | NAICS 2-digit codes | Work industry sector |

### Activity and Location Choice

| Field Name | Data Type | Description | Valid Values | Notes |
|------------|-----------|-------------|--------------|-------|
| `preTelecommuteCdap` | STRING(1) | Pre-telecommute activity pattern | M, N, H | Before telecommute choice |
| `telecommute` | INTEGER | Telecommute choice | 0 = No, 1 = Yes | Work from home option |
| `cdap` | STRING(1) | Coordinated daily activity pattern | M, N, H | M=Mandatory, N=Non-mandatory, H=Home |
| `workDCLogsum` | DECIMAL(8,4) | Work destination choice logsum | Real number | Work location accessibility |
| `schoolDCLogsum` | DECIMAL(8,4) | School destination choice logsum | Real number | School location accessibility |

### Choice Model Results

| Field Name | Data Type | Description | Valid Values | Notes |
|------------|-----------|-------------|--------------|-------|
| `imf_choice` | INTEGER | Individual mandatory tour frequency | 0-2+ | Number of mandatory tours |
| `inmf_choice` | INTEGER | Individual non-mandatory tour freq | 0-3+ | Number of non-mandatory tours |
| `fp_choice` | INTEGER | Free parking choice | 0 = No, 1 = Yes | Free parking availability |

### Modeling Controls

| Field Name | Data Type | Description | Valid Values | Notes |
|------------|-----------|-------------|--------------|-------|
| `sampleRate` | DECIMAL(6,4) | Population sampling rate | 0.0-1.0 | Model expansion factor |

## Data Dictionary: Person Types

| Code | Description | Age Range | Employment Status | Notes |
|------|-------------|-----------|------------------|-------|
| 1 | Full-time worker | 16+ | Working full-time | 35+ hours/week |
| 2 | Part-time worker | 16+ | Working part-time | Less than 35 hours/week |
| 3 | University student | 18+ | Student (any employment) | College/university |
| 4 | Non-working adult | 16-64 | Not employed | Not retired |
| 5 | Retired adult | 65+ | Retired/not working | Senior non-worker |
| 6 | Driving-age student | 16-17 | Student | High school student |
| 7 | School-age child | 6-15 | Student | Grade/middle school |
| 8 | Preschool child | 0-5 | Not applicable | Too young for school |

## Data Dictionary: Transit Subsidy Choices

| Code | Description | Eligibility |
|------|-------------|-------------|
| 0 | No transit subsidy | Not eligible or declined |
| 1 | Has transit subsidy | Qualified for employer/agency subsidy |

## Data Dictionary: Transit Pass Choices

| Code | Description | Pass Type |
|------|-------------|-----------|
| 0 | No transit pass | Pay per ride |
| 1 | Has transit pass | Monthly or annual pass |

## Data Dictionary: CDAP Activity Patterns

| Code | Description | Meaning |
|------|-------------|---------|
| M | Mandatory | Person has mandatory activities (work/school) |
| N | Non-mandatory | Person has non-mandatory activities only |
| H | Home | Person stays home all day |

## Data Dictionary: Telecommute Choice

| Code | Description | Work Arrangement |
|------|-------------|------------------|
| 0 | No telecommute | Must travel to workplace |
| 1 | Telecommute | Works from home |

## Data Validation Rules

### Age and Person Type Consistency

```sql
-- Age should be consistent with person type
SELECT COUNT(*) FROM person 
WHERE (type = 8 AND age > 5) OR 
      (type = 7 AND (age < 6 OR age > 15)) OR
      (type = 6 AND (age < 16 OR age > 19)) OR
      (type IN (1,2,4) AND age < 16) OR
      (type = 5 AND age < 65);
```

### Transit Subsidy Consistency

```sql
-- Transit subsidy percent should be 0 when no subsidy
SELECT COUNT(*) FROM person 
WHERE transitSubsidy_choice = 0 AND 
      transitSubsidy_percent > 0;
```

### CDAP and Telecommute Consistency

```sql
-- Telecommute workers should have appropriate CDAP patterns
SELECT COUNT(*) FROM person p
WHERE telecommute = 1 AND 
      cdap NOT IN ('M', 'H');
```

### Value of Time Validation

```sql
-- Value of time should be positive for all persons
SELECT COUNT(*) FROM person 
WHERE value_of_time IS NULL OR 
      value_of_time <= 0;
```

## Survey Data Mapping Examples

### Person Type Assignment

```python
# Assign CTRAMP person type based on age, employment, and student status
def assign_person_type(age, employment_status, student_status):
    if employment_status == 'full_time':
        return 1  # Full-time worker
    elif employment_status == 'part_time':
        return 2  # Part-time worker
    elif student_status == 'university':
        return 3  # University student
    elif employment_status == 'unemployed':
        if age >= 65:
            return 5  # Retired
        elif age >= 16:
            return 4  # Non-working adult
        elif age >= 6:
            return 7  # School-age child
        else:
            return 8  # Preschool child
    elif student_status == 'high_school' and age >= 16:
        return 6  # Driving-age student
    elif student_status in ['elementary', 'middle_school'] and age >= 6:
        return 7  # School-age child
    else:
        return 8  # Preschool child
```

### Transit Subsidy Mapping

```python
# Map survey responses to transit subsidy fields
def map_transit_subsidy(has_employer_subsidy, subsidy_amount):
    if has_employer_subsidy and subsidy_amount > 0:
        return {
            'transitSubsidy_choice': 1,
            'transitSubsidy_percent': min(100.0, subsidy_amount)
        }
    else:
        return {
            'transitSubsidy_choice': 0,
            'transitSubsidy_percent': 0.0
        }
```

### Value of Time Calculation

```python
# Calculate value of time based on income and work patterns
def calculate_value_of_time(household_income, num_workers, person_type):
    # Base calculation: income per worker divided by annual work hours
    if person_type in [1, 2]:  # Workers
        base_vot = (household_income / num_workers) / (50 * 40)  # $/hour
        return max(5.0, min(50.0, base_vot))  # Cap between $5-50/hour
    else:
        # Non-workers get median value of time
        return 15.0
```

## File Relationships

- **Parent**: [`household.csv`](household.md) - Each person belongs to exactly one household
- **Children**: [`indiv_tour.csv`](individual-tours.md) - Tours made by this person
- **Children**: [`indiv_trip.csv`](individual-trips.md) - Trips made by this person  
- **Related**: [`joint_tour.csv`](joint-tours.md) - Tours involving this person with others
- **Related**: [`joint_trip.csv`](joint-trips.md) - Trips involving this person with others

## Common Analysis Applications

1. **Demographics Analysis**: Population pyramids, employment rates by age/gender
2. **Transportation Equity**: Access to opportunities by demographic groups
3. **Activity Patterns**: Daily activity engagement by person type
4. **Location Choice**: Work/school location accessibility and choice patterns
5. **Mode Choice Preparation**: Person attributes for mode choice models

## Data Quality Considerations

- **Missing Values**: Work/school locations may be 0/null for non-workers/students
- **Synthetic Population**: Represents statistical distributions, not real individuals
- **Model Consistency**: All fields should be internally consistent with choice model logic
- **Geographic Validity**: All location fields should reference valid MGRAs
- **Temporal Consistency**: Age and education should be realistic combinations

---

*This documentation is part of the [CTRAMP Output Files](index.md) specification suite.*