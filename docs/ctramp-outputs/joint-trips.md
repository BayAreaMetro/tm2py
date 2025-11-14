# 🔗 Joint Trip Output File (joint_trip.csv)

!!! abstract "Multi-Person Trip Coordination"
    **Joint trips** represent individual trip segments where multiple household members travel together. Provides the most detailed view of coordinated household travel behavior and activity patterns.

!!! success "Data Verification"
    Documentation verified against **2015-tm22-dev-sprint-04** model run  
    ✅ **18 fields** confirmed in actual output files

## 🎯 File Purpose

=== "📊 Trip Detail"
    
    Captures granular information about coordinated household travel:
    
    - **Trip Chains** - Intermediate stops within joint tours
    - **Mode Coordination** - How households coordinate transportation choices
    - **Activity Sequencing** - Order of stops and purposes
    - **Participant Management** - Number of people on each trip segment

=== "🔗 Data Relationships"
    
    | Connection | Field(s) | Target File | Purpose |
    |------------|----------|-------------|----------|
    | **Household** | `hh_id` | `household.csv` | Demographic context |
    | **Joint Tour** | `hh_id + tour_id` | `joint_tour.csv` | Parent tour information |
    | **Trip Sequence** | `stop_id + inbound` | Same file | Trip ordering |

=== "🚗 Network Loading"
    
    Essential for transportation network assignment:
    
    - Origin-destination flows at **MAZ level**
    - Mode-specific trip volumes
    - Time-of-day travel patterns
    - Multi-occupancy vehicle loading

## 📋 Field Categories

### 🆔 Core Identifiers

!!! info "Trip Identification"
    Essential fields for uniquely identifying and sequencing joint trip segments

| Field | Type | Description | Example Values |
|-------|------|-------------|----------------|
| `hh_id` | INTEGER | Household identifier linking to household.csv | 5631, 13755 |
| `tour_id` | INTEGER | Tour identifier within household linking to joint_tour.csv | 0, 1 |

### Trip Sequencing

| Field | Type | Description | Valid Values |
|-------|------|-------------|-------------|
| `stop_id` | INTEGER | Stop/trip sequence identifier within tour | 0-based: 0, 1, 2; -1 for direct trips |
| `inbound` | INTEGER | Trip direction indicator | 0=outbound, 1=inbound |

**Trip Sequencing Logic:**
- `stop_id = -1`: Direct trip (no intermediate stops)
- `stop_id = 0`: First stop/trip on half-tour
- `stop_id = 1,2,3`: Subsequent stops/trips on half-tour
- Outbound trips (`inbound = 0`): From home toward tour destination
- Inbound trips (`inbound = 1`): From tour destination toward home

### Tour Context

| Field | Type | Description | Allowed Values |
|-------|------|-------------|----------------|
| `tour_purpose` | TEXT | Primary purpose of parent joint tour | "Shop", "Maintenance", "Discretionary", "Eating Out", "Visiting" |

**Tour Purpose Reference (Actual Values):**
- **Shop** - Shopping and retail activities (most common - 373 occurrences)
- **Maintenance** - Personal business, banking, appointments (242 occurrences)  
- **Discretionary** - General discretionary and recreational activities (166 occurrences)
- **Eating Out** - Restaurant and dining activities (119 occurrences)
- **Visiting** - Social/visiting activities (99 occurrences)

### Trip Purposes

| Field | Type | Description | Purpose Categories |
|-------|------|-------------|-------------------|
| `orig_purpose` | TEXT | Purpose at trip origin | Activity purposes or "Home"/"Work" |
| `dest_purpose` | TEXT | Purpose at trip destination | Activity purposes or "Home"/"Work" |

**Common Trip Purposes (Actual Values):**
- **Home Anchor:** "Home"
- **Activities:** "Shop", "Maintenance", "Discretionary", "Visiting", "Eating Out"
- **Note**: Joint trips are coordinated household activities, so purposes align with joint tour categories

### Spatial Attributes

| Field | Type | Description | Valid Range |
|-------|------|-------------|-------------|
| `orig_mgra` | INTEGER | Origin MGRA (Micro Geographic Analysis Zone) | Valid MGRA IDs |
| `dest_mgra` | INTEGER | Destination MGRA | Valid MGRA IDs |
| `parking_mgra` | INTEGER | Parking location MGRA (for auto modes) | Valid MGRA IDs or -1/0 |

### Temporal Attributes

| Field | Type | Description | Valid Range |
|-------|------|-------------|-------------|
| `stop_period` | INTEGER | Trip departure time period | 1-48 (30-minute periods) |

**Time Period Conversion:**
- Period 1 = 5:00-5:29 AM
- Period 2 = 5:30-5:59 AM  
- Period 12 = 8:30-8:59 AM (morning commute)
- Period 24 = 2:30-2:59 PM (early afternoon)
- Period 36 = 8:30-8:59 PM (evening)
- Period 48 = 2:30-2:59 AM (next day)

### Mode Choice

| Field | Type | Description | Valid Values |
|-------|------|-------------|-------------|
| `trip_mode` | INTEGER | Trip-specific transportation mode | 1-17 (see mode dictionary) |
| `tour_mode` | INTEGER | Parent tour transportation mode | 1-17 (same as trip_mode for tours without stops) |

**Transportation Mode Dictionary (17 modes):**

| Mode ID | Mode Name | Category | Description |
|---------|-----------|----------|-------------|
| 1 | SOV_GP | Auto | Single occupant vehicle, general purpose lanes |
| 2 | SOV_PAY | Auto | Single occupant vehicle, toll lanes |
| 3 | SR2_GP | Auto | Shared ride 2 person, general purpose |
| 4 | SR2_HOV | Auto | Shared ride 2 person, HOV lanes |
| 5 | SR2_PAY | Auto | Shared ride 2 person, toll lanes |
| 6 | SR3_GP | Auto | Shared ride 3+ person, general purpose |
| 7 | SR3_HOV | Auto | Shared ride 3+ person, HOV lanes |
| 8 | SR3_PAY | Auto | Shared ride 3+ person, toll lanes |
| 9 | WALK | Non-Motorized | Walk only |
| 10 | BIKE | Non-Motorized | Bicycle |
| 11 | WLK_TRN | Transit | Walk to Transit |
| 12 | PNR_TRN | Transit | Park and Ride to Transit |
| 13 | KNRPRV_TRN | Transit | Kiss and Ride (Private) to Transit |
| 14 | KNRTNC_TRN | Transit | Kiss and Ride (TNC) to Transit |
| 15 | TAXI | Other | Taxi |
| 16 | TNC | Other | Transportation Network Company (Uber/Lyft) |
| 17 | SCHLBUS | Other | School Bus |



### Performance Metrics

| Field | Type | Description | Units |
|-------|------|-------------|-------|
| `trip_dist` | REAL | Trip distance | Miles |

**Distance Calculation:**
- Calculated using shortest path algorithms
- May use auto or transit networks depending on trip mode
- For walk/bike trips, uses pedestrian/bicycle network distances

### Joint Tour Attributes

| Field | Type | Description | Valid Range |
|-------|------|-------------|-------------|
| `num_participants` | INTEGER | Number of household members on this trip | 1+ (typically 2-5) |

### Model Metadata

| Field | Type | Description | Valid Range |
|-------|------|-------------|-------------|
| `tranpath_rnum` | REAL | Transit path choice random number | 0.0-1.0 (or -1.0 for non-transit) |
| `sampleRate` | REAL | Household sampling rate | 0.0-1.0 |
| `avAvailable` | INTEGER | Autonomous vehicle availability flag | 0=no, 1=yes |

## Joint Trip Generation Logic

### Stop-Based Joint Trips

When joint tours include intermediate stops, multiple trip records are generated:

```
Example: Joint shopping tour with two stops
Tour: Home -> Store1 -> Store2 -> Home

Generated trips:
1. stop_id=-1, inbound=0: Home -> Store1 (direct outbound)
2. stop_id=0,  inbound=1: Store1 -> Store2 (first inbound stop) 
3. stop_id=1,  inbound=1: Store2 -> Home (final return)
```

### Direct Joint Trips

Simple round-trip joint tours generate two trip records:

```
Example: Direct joint restaurant trip  
Tour: Home -> Restaurant -> Home

Generated trips:
1. stop_id=-1, inbound=0: Home -> Restaurant (outbound)
2. stop_id=-1, inbound=1: Restaurant -> Home (inbound)
```

## Mode Choice Patterns

### Joint Trip Mode Logic

- **Auto modes (1-8)**: Most common for joint travel due to coordination needs
- **Walk mode (9)**: Used for short segments or when all participants can walk
- **Transit modes (11-17)**: Less common but used when household coordinates transit access
- **Shared ride modes (3-8)**: Dominant pattern since multiple people travel together

### Trip vs Tour Mode Consistency

- **Auto tour (`tour_mode = 1-8`)**: Trip modes typically match tour mode
- **Transit tour (`tour_mode = 11-17`)**: May include walk trips for station access  
- **Walk/Bike tour (`tour_mode = 9-10`)**: All trips use same non-motorized mode

For transit trips (`trip_mode = 11-17`):
- `tranpath_rnum` contains random number for path choice modeling
- Non-transit trips have `tranpath_rnum = -1.0`

## Data Quality and Validation

### Joint Trip Consistency Checks

```sql
-- Trip mode should be valid
SELECT COUNT(*) FROM joint_trip 
WHERE trip_mode NOT BETWEEN 1 AND 17;

-- Tour mode should be valid  
SELECT COUNT(*) FROM joint_trip
WHERE tour_mode NOT BETWEEN 1 AND 17;

-- Number of participants should be reasonable
SELECT COUNT(*) FROM joint_trip
WHERE num_participants < 1 OR num_participants > 10;

-- Trip distance should be positive
SELECT COUNT(*) FROM joint_trip
WHERE trip_dist <= 0;
```

### Temporal Consistency

```sql
-- Stop periods should be valid
SELECT COUNT(*) FROM joint_trip
WHERE stop_period NOT BETWEEN 1 AND 48;

-- Inbound indicator should be 0 or 1
SELECT COUNT(*) FROM joint_trip  
WHERE inbound NOT IN (0, 1);
```

### Spatial Consistency

```sql
-- MGRAs should be valid
SELECT COUNT(*) FROM joint_trip
WHERE orig_mgra <= 0 OR dest_mgra <= 0;

-- Origin and destination should be different
SELECT COUNT(*) FROM joint_trip  
WHERE orig_mgra = dest_mgra;
```

## File Relationships

- **Parent**: [`household.csv`](household.md) - Each trip belongs to a household
- **Parent**: [`joint_tour.csv`](joint-tours.md) - Each trip belongs to exactly one joint tour
- **Related**: [`indiv_trip.csv`](individual-trips.md) - Individual trips for same household members
- **Network**: Referenced by transportation assignment models for joint travel demand

## Common Analysis Applications

1. **Joint Activity Analysis**: Understanding coordinated household travel patterns
2. **Mode Choice Modeling**: Multi-person transportation mode preferences  
3. **Destination Choice**: Spatial patterns of joint household activities
4. **Trip Complexity**: Analysis of multi-stop joint tours and intermediate activities
5. **Transport Planning**: Household coordination effects on transportation demand
6. **Transit Planning**: Joint household access to transit services

## Data Processing Examples

### Trip Chain Analysis

```python
import pandas as pd

# Load joint trip data
trips = pd.read_csv('joint_trip.csv')

# Analyze trip chaining by household and tour
trip_chains = trips.groupby(['hh_id', 'tour_id']).agg({
    'stop_id': 'count',
    'trip_dist': 'sum', 
    'num_participants': 'first',
    'tour_purpose': 'first'
}).rename(columns={'stop_id': 'num_trips'})

# Joint tour complexity
complexity = trip_chains.groupby('num_trips').size()
print("Joint tour complexity distribution:")
print(complexity)
```

### Mode Share Analysis

```python
# Joint trip mode share
mode_share = trips['trip_mode'].value_counts(normalize=True)
print("Joint trip mode share:")
print(mode_share)

# Participant group size analysis
participant_dist = trips['num_participants'].value_counts().sort_index()
print("Participant distribution:")
print(participant_dist)
```

### Purpose-Destination Analysis

```sql
-- Most common joint activity destinations  
SELECT dest_purpose, COUNT(*) as trip_count
FROM joint_trip 
WHERE dest_purpose != 'Home'
GROUP BY dest_purpose
ORDER BY trip_count DESC;

-- Joint shopping trip patterns
SELECT orig_purpose, dest_purpose, COUNT(*) as trips
FROM joint_trip
WHERE tour_purpose = 'Shop'
GROUP BY orig_purpose, dest_purpose
ORDER BY trips DESC;
```

## Special Considerations

### Joint vs Individual Trip Differences

- **Coordination**: Joint trips require coordination of schedules and preferences across household members
- **Mode Choice**: Limited to modes that can accommodate multiple participants
- **Timing**: More constrained time windows due to multiple person schedules
- **Destinations**: May represent compromise choices for multiple participants

### Data Limitations

- **Participant Details**: Individual person characteristics not included in trip records
- **Internal Coordination**: Decision-making process within household not captured
- **Activity Duration**: Time spent at destinations not directly recorded
- **Participant Roles**: Driver vs passenger roles not specified

---

*This documentation is part of the [CTRAMP Output Files](index.md) specification suite.*