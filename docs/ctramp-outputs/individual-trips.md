# Individual Trips Documentation

## Overview

The `indiv_trip.csv` file contains detailed trip-level information for individual (person-specific) tours, representing the most granular level of travel behavior in CTRAMP. Each record represents a single trip segment, which may be either a stop-based trip (between intermediate stops) or a direct trip (when no stops are made).

**Note**: This documentation reflects the actual field structure found in model output files (verified against 2015-tm22-dev-sprint-04 run). The file contains exactly 19 fields as documented below.

**Actual fields**: hh_id, person_id, person_num, tour_id, stop_id, inbound, tour_purpose, orig_purpose, dest_purpose, orig_mgra, dest_mgra, trip_dist, parking_mgra, stop_period, trip_mode, tour_mode, tranpath_rnum, sampleRate, avAvailable

## File Purpose

Individual trips provide the foundation for transportation analysis and modeling by capturing:
- Detailed trip chains with intermediate stops
- Trip-level mode choices and timing
- Origin-destination patterns at the MGRA level
- Trip purposes and activity sequencing
- Transportation network loading data

## Key Relationships

- Links to `household.csv` via `hh_id`
- Links to `person.csv` via `person_id` and `person_num`
- Links to `indiv_tour.csv` via `hh_id`, `person_id`, and `tour_id`
- Referenced by transportation network models for trip assignment

## Field Specifications

### Identification Fields

| Field | Type | Description | Example Values |
|-------|------|-------------|----------------|
| `hh_id` | INTEGER | Household identifier linking to household.csv | 100001, 100002 |
| `person_id` | INTEGER | Person identifier linking to person.csv | 1000010101, 1000010102 |
| `person_num` | INTEGER | Person number within household (1-based) | 1, 2, 3, 4 |
| `tour_id` | INTEGER | Tour identifier within person (0-based) | 0, 1, 2 |

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
| `tour_purpose` | TEXT | Primary purpose of parent tour | See individual tour purposes |

**Tour Purpose Reference (Actual Values):**
- **Mandatory:** "Work", "School", "University"
- **Non-Mandatory:** "Escort", "Shop", "Maintenance", "Discretionary", "Visiting", "Eating Out"
- **At-Work:** "Work-Based"

### Trip Purposes

| Field | Type | Description | Purpose Categories |
|-------|------|-------------|-------------------|
| `orig_purpose` | TEXT | Purpose at trip origin | Activity purposes or "Home"/"Work" |
| `dest_purpose` | TEXT | Purpose at trip destination | Activity purposes or "Home"/"Work" |

**Common Trip Purposes (Actual Values):**
- **Home/Work Anchors:** "Home", "Work"
- **Activities:** "Shop", "Maintenance", "Discretionary", "Visiting", "Eating Out"
- **Education:** "School", "University"
- **Support:** "Escort" (drop-off/pick-up activities)
- **Work-Related:** "Work-Based", "work related"

### Spatial Attributes

| Field | Type | Description | Valid Range |
|-------|------|-------------|-------------|
| `orig_mgra` | INTEGER | Origin MGRA (Micro Geographic Analysis Zone) | Valid MGRA IDs |
| `dest_mgra` | INTEGER | Destination MGRA | Valid MGRA IDs |
| `parking_mgra` | INTEGER | Parking location MGRA (for auto modes) | Valid MGRA IDs or 0 |

### Temporal Attributes

| Field | Type | Description | Valid Range |
|-------|------|-------------|-------------|
| `stop_period` | INTEGER | Trip departure time period | 1-48 (30-minute periods) |

**Time Period Conversion:**
- Period 1 = 3:00-3:29 AM
- Period 2 = 3:30-3:59 AM  
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
| 2 | SOV_PAY | Auto | Single occupant vehicle, toll/express lanes |
| 3 | SR2_GP | Auto | Shared ride 2 person, general purpose lanes |
| 4 | SR2_HOV | Auto | Shared ride 2 person, HOV lanes |
| 5 | SR2_PAY | Auto | Shared ride 2 person, toll/express lanes |
| 6 | SR3_GP | Auto | Shared ride 3+ person, general purpose lanes |
| 7 | SR3_HOV | Auto | Shared ride 3+ person, HOV lanes |
| 8 | SR3_PAY | Auto | Shared ride 3+ person, toll/express lanes |
| 9 | WALK | Non-Motorized | Walk only |
| 10 | BIKE | Non-Motorized | Bicycle |
| 11 | WALK_LOC | Transit | Walk to local bus |
| 12 | WALK_LRF | Transit | Walk to light rail/ferry |
| 13 | WALK_EXP | Transit | Walk to express bus |
| 14 | WALK_HVY | Transit | Walk to heavy rail |
| 15 | WALK_COM | Transit | Walk to commuter rail |
| 16 | DRIVE_LOC | Transit | Drive to local bus |
| 17 | DRIVE_LRF | Transit | Drive to light rail/ferry |

### Performance Metrics

| Field | Type | Description | Units |
|-------|------|-------------|-------|
| `trip_dist` | REAL | Trip distance | Miles |

**Distance Calculation:**
- Calculated using shortest path algorithms
- May use auto or transit networks depending on trip mode
- For walk/bike trips, uses pedestrian/bicycle network distances

### Model Metadata

| Field | Type | Description | Valid Range |
|-------|------|-------------|-------------|
| `tranpath_rnum` | REAL | Transit path choice random number | 0.0-1.0 (or -999 for non-transit) |
| `sampleRate` | REAL | Household sampling rate | 0.0-1.0 |
| `avAvailable` | INTEGER | Autonomous vehicle availability flag | 0=no, 1=yes |

## Trip Generation Logic

### Stop-Based Trips

When tours include intermediate stops, multiple trip records are generated:

```
Example Outbound Half-Tour with 2 stops:
- stop_id=0, inbound=0: Home → Stop 1
- stop_id=1, inbound=0: Stop 1 → Stop 2  
- stop_id=2, inbound=0: Stop 2 → Tour Destination

Example Inbound Half-Tour with 1 stop:
- stop_id=0, inbound=1: Tour Destination → Stop 1
- stop_id=1, inbound=1: Stop 1 → Home
```

### Direct Trips (No Stops)

When tours have no intermediate stops:

```
Direct Tour Example:
- stop_id=-1, inbound=0: Home → Tour Destination
- stop_id=-1, inbound=1: Tour Destination → Home
```

### Purpose Assignment Logic

Trip purposes are assigned based on stop/destination activities:

1. **Home/Work Anchor:** Trips to/from home use "Home", trips to/from work use "Work"
2. **Stop Purposes:** Intermediate stops use activity-specific purposes (shopping, othMaint, etc.)
3. **Tour Purposes:** Final destinations use tour-level purposes
4. **Activity Chains:** Sequential stops create purpose chains (e.g., Home→Shopping→Bank→Home)

## Mode Choice Hierarchy

### Tour vs. Trip Mode Relationships

- **Tour Mode:** Overall transportation mode for entire tour
- **Trip Mode:** May differ from tour mode for individual trip segments
- **Mode Constraints:** Trip modes must be compatible with tour mode capabilities

**Examples:**
- Auto tour (`tour_mode = 1-8`): Trip modes typically match tour mode
- Transit tour (`tour_mode = 11-17`): May include walk trips for station access
- Walk/Bike tour (`tour_mode = 9-10`): All trips use same non-motorized mode

### Transit Path Choice

For transit trips (`trip_mode = 11-17`):
- `tranpath_rnum` contains random number for path choice modeling
- Used for selecting among multiple transit route options
- Critical for transit assignment and capacity analysis

## Survey Data Integration

### Mapping Survey Data to Trip Records

When transforming survey data to match CTRAMP trip format:

1. **Trip Chaining:** Group survey trips into tours, then decompose into trip segments
2. **Purpose Mapping:** Translate survey activity purposes to CTRAMP purpose categories
3. **Mode Harmonization:** Map survey transportation modes to 17-mode CTRAMP system
4. **Time Conversion:** Convert survey times to CTRAMP 30-minute time periods
5. **Spatial Mapping:** Geocode survey locations to MGRA system

### Common Survey Data Challenges

1. **Activity Classification:** Survey purposes may not directly map to CTRAMP categories
2. **Tour Definition:** Surveys may record trips without clear tour structure
3. **Mode Detail:** Survey modes may be more or less detailed than CTRAMP system
4. **Timing Precision:** Survey times may need rounding to 30-minute periods
5. **Transfer Trips:** Complex transit journeys may require trip segment decomposition

## Validation Rules

### Data Quality Checks

```sql
-- Check valid stop sequencing
SELECT hh_id, person_id, tour_id, stop_id, inbound 
FROM indiv_trip 
WHERE stop_id NOT IN (-1, 0, 1, 2, 3) OR inbound NOT IN (0, 1);

-- Verify mode consistency
SELECT DISTINCT trip_mode FROM indiv_trip 
WHERE trip_mode NOT BETWEEN 1 AND 17;

-- Check time periods
SELECT COUNT(*) FROM indiv_trip 
WHERE stop_period NOT BETWEEN 1 AND 48;

-- Validate trip sequencing within tours
SELECT hh_id, person_id, tour_id, COUNT(*) as trip_count,
       COUNT(CASE WHEN inbound = 0 THEN 1 END) as outbound_trips,
       COUNT(CASE WHEN inbound = 1 THEN 1 END) as inbound_trips
FROM indiv_trip 
GROUP BY hh_id, person_id, tour_id
HAVING outbound_trips = 0 OR inbound_trips = 0;  -- Tours should have both directions
```

### Geographic Consistency

```sql
-- Check for trips within same MGRA (very short trips)
SELECT COUNT(*) FROM indiv_trip 
WHERE orig_mgra = dest_mgra AND trip_dist > 0.1;  -- Flag suspicious cases

-- Validate MGRA codes exist in geography
SELECT DISTINCT orig_mgra FROM indiv_trip 
WHERE orig_mgra NOT IN (SELECT mgra_id FROM mgra_geography);
```

### Tour-Trip Linkages

```sql
-- Verify trip counts match tour stop attributes
SELECT t.hh_id, t.person_id, t.tour_id,
       t.num_ob_stops, t.num_ib_stops,
       COUNT(CASE WHEN it.inbound = 0 THEN 1 END) - 1 as actual_ob_stops,
       COUNT(CASE WHEN it.inbound = 1 THEN 1 END) - 1 as actual_ib_stops
FROM indiv_tour t
LEFT JOIN indiv_trip it ON t.hh_id = it.hh_id 
  AND t.person_id = it.person_id 
  AND t.tour_id = it.tour_id
GROUP BY t.hh_id, t.person_id, t.tour_id, t.num_ob_stops, t.num_ib_stops
HAVING actual_ob_stops != t.num_ob_stops 
    OR actual_ib_stops != t.num_ib_stops;
```

## Usage Examples

### Trip Chain Analysis

```sql
-- Analyze complete trip chains for shopping tours
SELECT 
  hh_id, person_id, tour_id,
  STRING_AGG(orig_purpose || '→' || dest_purpose, ' | ' ORDER BY inbound, stop_id) as trip_chain,
  COUNT(*) as trip_segments,
  SUM(trip_dist) as total_distance
FROM indiv_trip 
WHERE tour_purpose = 'shopping'
GROUP BY hh_id, person_id, tour_id
ORDER BY total_distance DESC
LIMIT 10;
```

### Mode Choice Analysis

```sql
-- Trip vs tour mode choice consistency
SELECT 
  tour_mode,
  trip_mode,
  COUNT(*) as trip_count,
  AVG(trip_dist) as avg_distance
FROM indiv_trip
GROUP BY tour_mode, trip_mode
HAVING COUNT(*) > 100
ORDER BY tour_mode, trip_mode;
```

### Temporal Distribution

```sql
-- Trip departure time patterns by purpose
SELECT 
  dest_purpose,
  CASE 
    WHEN stop_period BETWEEN 1 AND 12 THEN 'Early AM (3:00-8:59)'
    WHEN stop_period BETWEEN 13 AND 24 THEN 'Morning (9:00-14:59)'
    WHEN stop_period BETWEEN 25 AND 36 THEN 'Afternoon (15:00-20:59)'
    ELSE 'Evening (21:00-2:59)'
  END as time_of_day,
  COUNT(*) as trip_count,
  AVG(trip_dist) as avg_distance
FROM indiv_trip 
WHERE stop_id >= 0  -- Exclude direct trips
GROUP BY dest_purpose, time_of_day
ORDER BY dest_purpose, time_of_day;
```

### Stop Pattern Analysis

```sql
-- Distribution of stop patterns by tour purpose
SELECT 
  tour_purpose,
  MAX(CASE WHEN inbound = 0 THEN stop_id END) + 1 as outbound_stops,
  MAX(CASE WHEN inbound = 1 THEN stop_id END) + 1 as inbound_stops,
  COUNT(DISTINCT tour_id) as tour_count
FROM indiv_trip 
WHERE stop_id >= 0
GROUP BY hh_id, person_id, tour_id, tour_purpose
GROUP BY tour_purpose, outbound_stops, inbound_stops
ORDER BY tour_purpose, outbound_stops, inbound_stops;
```

### Transit Trip Analysis

```sql
-- Transit trip characteristics
SELECT 
  trip_mode,
  CASE 
    WHEN trip_mode BETWEEN 11 AND 15 THEN 'Walk Access'
    WHEN trip_mode BETWEEN 16 AND 17 THEN 'Drive Access'
  END as access_mode,
  COUNT(*) as trip_count,
  AVG(trip_dist) as avg_distance,
  AVG(tranpath_rnum) as avg_path_random
FROM indiv_trip 
WHERE trip_mode BETWEEN 11 AND 17
GROUP BY trip_mode
ORDER BY trip_mode;
```

## Network Loading Applications

Individual trip data provides the foundation for transportation network analysis:

1. **Auto Assignment:** Trip origins/destinations and departure times for traffic loading
2. **Transit Assignment:** Transit trip paths and boarding/alighting patterns  
3. **Active Transportation:** Walk/bike trip volumes for facility planning
4. **Time-of-Day Analysis:** Temporal trip distribution for capacity planning
5. **Mode Share Analysis:** Transportation mode usage patterns by geography and demographics

The granular nature of trip data enables detailed network performance evaluation and infrastructure investment analysis.