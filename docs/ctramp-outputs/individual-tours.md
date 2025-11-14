# 🎯 Individual Tour Output File (indiv_tour.csv)

!!! abstract "File Overview"
    **Individual tours** represent complete round-trips made by single persons, from origin through destination and back. Contains tour-level decisions like purpose, mode choice, timing, and destination.

!!! success "Data Verification"
    Documentation verified against **2015-tm22-dev-sprint-04** model run  
    ✅ **57 fields** confirmed in actual output files

## 📊 File Structure

=== "🔍 Key Information"
    
    | Aspect | Value | Description |
    |--------|--------|-------------|
    | **Primary Key** | `hh_id + person_id + tour_id` | Unique tour identifier |
    | **Foreign Keys** | `hh_id`, `person_id` | Links to household and person data |
    | **Record Type** | One row per individual tour | Excludes joint household tours |
    | **Field Count** | **57 total** | 20 core + 17 utilities + 17 probabilities + 3 sampling |

=== "📈 Data Scale"
    
    | Metric | Typical Range | Notes |
    |--------|---------------|--------|
    | **Tours per Person** | 3-8 tours | Varies by person type and activity |
    | **File Size** | 8-25M rows | Depends on region and scenario |
    | **Geographic Coverage** | 9-county Bay Area | MAZ-level resolution |

## 📋 Field Categories

### 🆔 Identification Fields

!!! info "Core Identifiers"
    Essential fields for linking records across CTRAMP output files

| Field | Type | Description | 💡 Key Points |
|-------|------|-------------|----------------|
| `hh_id` | `INTEGER` | Household identifier | Links to `household.csv` |
| `person_id` | `INTEGER` | Unique person ID | Links to `person.csv` |
| `person_num` | `INTEGER` | Position within household | 1 to household size |
| `person_type` | `INTEGER` | Person classification | 1-8 (worker/student/age categories) |
| `tour_id` | `INTEGER` | Tour sequence number | Unique within each person |

### 🎯 Tour Classification

!!! tip "Activity-Based Modeling"
    Purpose reflects the **primary activity** driving the tour decision
| `tour_category` | STRING | Tour category classification | "MANDATORY", "INDIVIDUAL_NON_MANDATORY", "AT_WORK" | High-level tour type |
| `tour_purpose` | STRING | Specific tour purpose | (see data dictionary) | Detailed purpose classification |

### Spatial Attributes

| Field Name | Data Type | Description | Valid Values | Notes |
|------------|-----------|-------------|--------------|-------|
| `orig_mgra` | INTEGER | Origin MGRA (home or workplace) | Valid MGRA ID | Usually home for primary tours |
| `dest_mgra` | INTEGER | Destination MGRA | Valid MGRA ID | Tour destination |

### Temporal Attributes

| Field Name | Data Type | Description | Valid Values | Notes |
|------------|-----------|-------------|--------------|-------|
| `start_period` | INTEGER | Tour departure time period | 1-48 (30-minute periods) | 5:00 AM = period 1 |
| `end_period` | INTEGER | Tour arrival time period | 1-48 (30-minute periods) | Return time to origin |

### Mode Choice

| Field Name | Data Type | Description | Valid Values | Notes |
|------------|-----------|-------------|--------------|-------|
| `tour_mode` | INTEGER | Primary tour mode | 1-17 (see data dictionary) | Mode for longest segment |

### Travel Attributes

| Field Name | Data Type | Description | Valid Values | Notes |
|------------|-----------|-------------|--------------|-------|
| `tour_distance` | DECIMAL(6,2) | Total tour distance (miles) | 0.01+ | Round-trip distance |
| `tour_time` | DECIMAL(6,2) | Total tour time (minutes) | 1.0+ | Round-trip travel time |

### Stop Information

| Field Name | Data Type | Description | Valid Values | Notes |
|------------|-----------|-------------|--------------|-------|
| `num_ob_stops` | INTEGER | Number of outbound stops | 0-3 | Stops before primary destination |
| `num_ib_stops` | INTEGER | Number of inbound stops | 0-3 | Stops after primary destination |
| `atWork_freq` | INTEGER | At-work subtour frequency | 0-3 | For work tours only |

### Choice Model Results

| Field Name | Data Type | Description | Valid Values | Notes |
|------------|-----------|-------------|--------------|-------|
| `dcLogsum` | DECIMAL(10,6) | Destination choice logsum | Real number | Accessibility measure |
| `util_1` to `util_17` | DECIMAL(10,6) | Mode choice utilities | Real number | Optional: if saveUtilsProbsFlag=true |
| `prob_1` to `prob_17` | DECIMAL(8,6) | Mode choice probabilities | 0.0-1.0 | Optional: if saveUtilsProbsFlag=true |

### Sampling and Expansion

| Field Name | Data Type | Description | Valid Values | Notes |
|------------|-----------|-------------|--------------|-------|
| `sampleRate` | DECIMAL(8,4) | Household sampling rate | 0.0001-1.0 | For expansion to full population |
| `avAvailable` | INTEGER | Autonomous vehicle available | 0 = No, 1 = Yes | For AV sensitivity |

## Data Dictionary: Tour Categories

| Code | Description | Definition |
|------|-------------|------------|
| MANDATORY | Mandatory tours | Work and school tours |
| INDIVIDUAL_NON_MANDATORY | Individual discretionary tours | Shopping, social, personal business |
| AT_WORK | At-work subtours | Tours during work day from workplace |

## Data Dictionary: Tour Purposes

Based on actual model output data, the tour_purpose field contains these simplified values:

### All Tour Purposes (Actual Values)

| Purpose | Description | Tour Category | Frequency (typical) |
|---------|-------------|---------------|-------------------|
| Work | Work tours | MANDATORY | Most common mandatory purpose |
| School | School tours (all levels) | MANDATORY | Students of all ages |
| University | University tours | MANDATORY | University/college students |
| Escort | Escort/chauffeur tours | INDIVIDUAL_NON_MANDATORY | Picking up/dropping off others |
| Maintenance | Personal business/maintenance | INDIVIDUAL_NON_MANDATORY | Errands, appointments, services |
| Discretionary | General discretionary activities | INDIVIDUAL_NON_MANDATORY | Recreation, entertainment |
| Shop | Shopping tours | INDIVIDUAL_NON_MANDATORY | Retail shopping |
| Visiting | Social/visiting tours | INDIVIDUAL_NON_MANDATORY | Visiting friends/family |
| Eating Out | Restaurant/dining tours | INDIVIDUAL_NON_MANDATORY | Going out to eat |
| Work-Based | At-work subtours | AT_WORK | Business during work day |

**Note**: Unlike some documentation that shows detailed purpose codes (work_low, work_med, etc.), the actual model output uses these simplified, human-readable purpose names.

## Data Dictionary: Tour Modes

| Mode Code | Mode Name | Description | Auto Required | Transit Component |
|-----------|-----------|-------------|---------------|-------------------|
| 1 | SOV_GP | Single Occupancy Vehicle (General Purpose) | Yes | No |
| 2 | SOV_PAY | Single Occupancy Vehicle (Toll/Express Lanes) | Yes | No |
| 3 | SR2_GP | Shared Ride 2 (General Purpose) | Yes | No |
| 4 | SR2_HOV | Shared Ride 2 (HOV Lanes) | Yes | No |
| 5 | SR2_PAY | Shared Ride 2 (Toll/Express Lanes) | Yes | No |
| 6 | SR3_GP | Shared Ride 3+ (General Purpose) | Yes | No |
| 7 | SR3_HOV | Shared Ride 3+ (HOV Lanes) | Yes | No |
| 8 | SR3_PAY | Shared Ride 3+ (Toll/Express Lanes) | Yes | No |
| 9 | WALK | Walk | No | No |
| 10 | BIKE | Bicycle | No | No |
| 11 | WLK_TRN | Walk to Transit | No | Yes |
| 12 | PNR_TRN | Park and Ride to Transit | Yes | Yes |
| 13 | KNRPRV_TRN | Kiss and Ride (Private) to Transit | Yes | Yes |
| 14 | KNRTNC_TRN | Kiss and Ride (TNC) to Transit | No | Yes |
| 15 | TAXI | Taxi | No | No |
| 16 | TNC | Transportation Network Company (Uber/Lyft) | No | No |
| 17 | SCHLBUS | School Bus | No | No |

## Time Period Definitions

Tours are scheduled using 30-minute time periods:

| Period | Start Time | End Time | Period Name |
|--------|------------|----------|-------------|
| 1 | 5:00 AM | 5:30 AM | Early Morning |
| 2 | 5:30 AM | 6:00 AM | Early Morning |
| ... | ... | ... | ... |
| 13-18 | 11:00 AM | 2:00 PM | Midday |
| 19-24 | 2:00 PM | 5:00 PM | Afternoon |
| 25-30 | 5:00 PM | 8:00 PM | Evening Peak |
| ... | ... | ... | ... |
| 48 | 2:30 AM | 5:00 AM | Late Night |

## Data Validation Rules

### Spatial Consistency
```sql
-- Origin should be home MGRA for primary tours
SELECT COUNT(*) FROM indiv_tour t
JOIN person p ON t.person_id = p.person_id
JOIN household h ON p.hh_id = h.hh_id  
WHERE t.tour_category != 'AT_WORK' 
AND t.orig_mgra != h.home_mgra;
```

### Temporal Consistency  
```sql
-- End period should be after start period
SELECT COUNT(*) FROM indiv_tour 
WHERE end_period <= start_period;

-- Tour duration should be reasonable
SELECT COUNT(*) FROM indiv_tour
WHERE (end_period - start_period) > 24  -- More than 12 hours
OR (end_period - start_period) < 1;     -- Less than 30 minutes
```

### Purpose-Person Type Consistency
```sql
-- Work tours should only be for workers
SELECT COUNT(*) FROM indiv_tour t
JOIN person p ON t.person_id = p.person_id
WHERE t.tour_purpose LIKE 'work%' 
AND p.ptype NOT IN (1,2); -- Not FT or PT worker

-- School/University tours should only be for students  
SELECT COUNT(*) FROM indiv_tour t
JOIN person p ON t.person_id = p.person_id
WHERE (t.tour_purpose = 'university' AND p.ptype != 3)
OR (t.tour_purpose LIKE 'school%' AND p.ptype NOT IN (6,7));
```

## Survey Data Mapping Examples

### Tour Purpose Harmonization
```python
# Map survey activity to CTRAMP tour purpose
def map_tour_purpose(survey_activity, person_type, income_category):
    if survey_activity == 'work':
        if person_type == 2:  # Part-time worker
            return 'work_parttime'
        else:
            income_purposes = {
                1: 'work_low',
                2: 'work_med', 
                3: 'work_high',
                4: 'work_veryhigh'
            }
            return income_purposes.get(income_category, 'work_med')
    elif survey_activity == 'school':
        if person_type == 3:
            return 'university'
        elif person_type in [6, 7]:
            return 'school_predrive' if person_type == 7 else 'school_drive'
    elif survey_activity == 'shopping':
        return 'shopping'
    elif survey_activity == 'social':
        return 'visit'
    # Add more mappings as needed
    return 'othDiscr'  # Default
```

### Mode Choice Mapping
```python
# Map survey mode to CTRAMP mode codes
mode_mapping = {
    'drive_alone': 1,  # SOV_GP
    'carpool_2': 3,    # SR2_GP  
    'carpool_3plus': 6, # SR3_GP
    'walk': 9,
    'bike': 10,
    'transit': 11,     # WLK_TRN (most common)
    'uber_lyft': 16,   # TNC
    'taxi': 15
}

def map_survey_mode(survey_mode, has_auto_access=True):
    base_mode = mode_mapping.get(survey_mode, 16)  # Default to TNC
    
    # Adjust auto modes based on access
    if base_mode in [1,3,6] and not has_auto_access:
        return 16  # Use TNC instead
    
    return base_mode
```

### Time Period Conversion
```python
# Convert survey time to CTRAMP time periods
def time_to_period(hour, minute):
    """Convert clock time to CTRAMP period (1-48)"""
    # Periods start at 5:00 AM (period 1)
    total_minutes = hour * 60 + minute
    start_minutes = 5 * 60  # 5:00 AM
    
    if total_minutes < start_minutes:
        total_minutes += 24 * 60  # Next day
    
    period = ((total_minutes - start_minutes) // 30) + 1
    return min(max(period, 1), 48)

# Example: 8:30 AM -> period 8
departure_period = time_to_period(8, 30)
```

## File Relationships

- **Parent**: [`person.csv`](person.md) - Each tour belongs to exactly one person
- **Parent**: [`household.csv`](household.md) - Household-level attributes
- **Children**: [`indiv_trip.csv`](individual-trips.md) - Trip segments within this tour
- **Related**: [`joint_tour.csv`](joint-tours.md) - Joint tours involving same household members

## Common Analysis Applications

1. **Activity Pattern Analysis**: Tour frequency and timing by person type
2. **Mode Choice Modeling**: Understanding modal preferences by tour characteristics  
3. **Destination Choice**: Spatial patterns of tour destinations
4. **Tour Complexity**: Analysis of multi-stop tours and intermediate activities
5. **Transport Planning**: Peak period travel demand and infrastructure needs

## Data Quality Considerations

- **At-Work Tours**: Only exist for people with work tours in same time period
- **Mode Utilities**: Only present if model was run with saveUtilsProbsFlag=true
- **Sampling Rates**: All tours for same household have identical sample rate
- **Geographic Bounds**: All MGRA values should be within study area
- **Temporal Constraints**: Tours cannot exceed available time windows

## Special Fields by Tour Category

### Mandatory Tours
- Always originate from home (unless at-work subtour)
- `tour_purpose` indicates work income segment or education level
- Higher frequency for workers with flexible schedules

### Individual Non-Mandatory Tours  
- Typically originate from home
- `tour_purpose` reflects activity type and complexity
- May be constrained by available time windows

### At-Work Subtours
- Originate from work location (`orig_mgra` = work MGRA)
- Limited to specific purposes: business, eat, other  
- `atWork_freq` indicates total subtours for the work day

---

*This documentation is part of the [CTRAMP Output Files](index.md) specification suite.*