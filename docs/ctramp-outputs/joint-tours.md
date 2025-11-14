# Joint Tours Documentation

## Overview

The `joint_tour.csv` file contains information about household-level joint tours, where multiple household members travel together. Joint tours are coordinated household activities that include both adults and/or children participating in shared travel patterns.

**Note**: This documentation reflects the actual field structure found in model output files (verified against 2015-tm22-dev-sprint-04 run). The file contains exactly 51 fields as documented below.

**Actual fields**: hh_id, tour_id, tour_category, tour_purpose, tour_composition, tour_participants, orig_mgra, dest_mgra, start_period, end_period, tour_mode, tour_distance, tour_time, num_ob_stops, num_ib_stops, sampleRate, avAvailable, dcLogsum, util_1 through util_17, prob_1 through prob_17

## File Purpose

Joint tours represent household coordination decisions where multiple family members participate in the same tour. Unlike individual tours which are person-specific, joint tours involve coordination across household members with shared travel timing, destinations, and mode choices.

## Key Relationships

- Links to `household.csv` via `hh_id`
- References participants through `tour_participants` field
- Connects to `joint_trip.csv` for detailed trip segments
- Coordinates with individual tour schedules for time window management

## Field Specifications

### Identification Fields

| Field | Type | Description | Example Values |
|-------|------|-------------|----------------|
| `hh_id` | INTEGER | Household identifier linking to household.csv | 100001, 100002 |
| `tour_id` | INTEGER | Unique tour identifier within household (0-based) | 0, 1 |

### Tour Classification

| Field | Type | Description | Allowed Values |
|-------|------|-------------|----------------|
| `tour_category` | TEXT | Tour category classification | "JOINT_NON_MANDATORY" |
| `tour_purpose` | TEXT | Primary purpose of the joint tour | "Shop", "Maintenance", "Discretionary", "Eating Out", "Visiting" |
| `tour_composition` | INTEGER | Type of household composition on tour | 1=adults, 2=children, 3=mixed |
| `tour_participants` | TEXT | Space-separated list of person numbers | "1 2", "2 3 4" |

### Tour Purpose Categories

Joint tours are limited to non-mandatory purposes (Actual Values):

**Maintenance Purposes:**
- `Shop` - Shopping and retail activities
- `Maintenance` - Personal business, banking, appointments, etc.

**Discretionary Purposes:**
- `Eating Out` - Restaurant and dining activities  
- `Visiting` - Social/visiting activities
- `Discretionary` - General discretionary and recreational activities

**Note**: All joint tours use tour_category "JOINT_NON_MANDATORY"

### Tour Composition Types

Based on `tour_composition` field values from `JointTourModels.java`:

| Value | Type | Description | Participant Requirements |
|-------|------|-------------|-------------------------|
| 1 | adults | Adults only | 2+ adults, 0 children |
| 2 | child | Children only | 0 adults, 2+ children |
| 3 | mixed | Mixed party | 1+ adults AND 1+ children |

### Spatial Attributes

| Field | Type | Description | Valid Range |
|-------|------|-------------|-------------|
| `orig_mgra` | INTEGER | Origin MGRA (typically home) | Valid MGRA IDs |
| `dest_mgra` | INTEGER | Destination MGRA | Valid MGRA IDs |

### Temporal Attributes

| Field | Type | Description | Valid Range |
|-------|------|-------------|-------------|
| `start_period` | INTEGER | Tour departure time period | 1-48 (30-minute periods, 3:00 AM - 2:30 AM+1) |
| `end_period` | INTEGER | Tour arrival time period | 1-48 (30-minute periods, 3:00 AM - 2:30 AM+1) |

**Time Period Conversion:**
- Period 1 = 3:00-3:29 AM
- Period 2 = 3:30-3:59 AM
- Period 12 = 8:30-8:59 AM
- Period 24 = 2:30-2:59 PM
- Period 36 = 8:30-8:59 PM
- Period 48 = 2:30-2:59 AM (next day)

### Mode Choice

| Field | Type | Description | Valid Values |
|-------|------|-------------|-------------|
| `tour_mode` | INTEGER | Primary transportation mode | 1-17 (see mode dictionary below) |

**Mode Choice Dictionary (17 modes):**

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
| `tour_distance` | REAL | Round-trip distance | Miles |
| `tour_time` | REAL | Round-trip travel time | Minutes |

### Stop Attributes

| Field | Type | Description | Valid Range |
|-------|------|-------------|-------------|
| `num_ob_stops` | INTEGER | Number of outbound stops (excluding final destination) | 0-3 |
| `num_ib_stops` | INTEGER | Number of inbound stops (excluding home return) | 0-3 |

### Model Metadata

| Field | Type | Description | Valid Range |
|-------|------|-------------|-------------|
| `sampleRate` | REAL | Household sampling rate | 0.0-1.0 |
| `avAvailable` | INTEGER | Autonomous vehicle availability flag | 0=no, 1=yes |
| `dcLogsum` | REAL | Destination choice logsum value | Real number |

### Optional Mode Choice Details

If `saveUtilsProbsFlag` is enabled, additional fields for each mode alternative:

| Field Pattern | Type | Description |
|--------------|------|-------------|
| `util_{mode}` | REAL | Mode choice utility for mode {mode} |
| `prob_{mode}` | REAL | Mode choice probability for mode {mode} |

Where {mode} ranges from 1 to 17 for each transportation mode.

## Joint Tour Coordination

### Household Participation Logic

Joint tours require coordination across multiple household members based on:

1. **Coordinated Daily Activity Pattern (CDAP):** Household must have "j" (joint) in CDAP pattern
2. **Time Window Overlaps:** Participants must have overlapping available time windows  
3. **Composition Constraints:** Party must meet minimum size requirements for chosen composition type
4. **Person Type Compatibility:** Participants must be appropriate for composition (adults/children/mixed)

### Participant Selection Process

From `JointTourModels.java` participation logic:

1. **Composition Choice:** Household chooses tour composition type (1=adults, 2=children, 3=mixed)
2. **Individual Participation:** Each eligible person makes binary participation choice
3. **Validation:** Check if resulting party meets composition requirements
4. **Retry Logic:** If invalid composition, repeat participation choices

### Time Window Management

Joint tours must fit within participants' available time windows:
- Uses `scheduleJointTourTimeWindows()` method to coordinate schedules
- Considers existing mandatory tours, individual non-mandatory tours
- Ensures all participants available for chosen departure/arrival times

## Survey Data Integration

### Mapping Survey Data to Joint Tours

When transforming survey data to match CTRAMP joint tour format:

1. **Household Coordination:** Identify survey trips where multiple household members travel together
2. **Tour Grouping:** Group related trips into complete tours (outbound + return + stops)
3. **Purpose Mapping:** Map survey purposes to CTRAMP joint tour purposes
4. **Composition Classification:** Determine adult/child/mixed composition from participant demographics
5. **Participant Encoding:** Format participant list as space-separated person numbers

### Common Survey Data Challenges

1. **Tour Definition:** Surveys may record individual trips, requiring tour reconstruction
2. **Purpose Harmonization:** Survey purposes may not directly map to CTRAMP categories
3. **Time Formats:** Convert survey time formats to CTRAMP 30-minute periods
4. **Mode Mapping:** Translate survey transportation modes to CTRAMP 17-mode system
5. **Household Structure:** Ensure consistent person numbering across files

## Validation Rules

### Data Quality Checks

```sql
-- Check valid tour compositions
SELECT tour_composition, COUNT(*) as count
FROM joint_tour 
GROUP BY tour_composition
HAVING tour_composition NOT IN (1, 2, 3);

-- Verify participant counts match composition
SELECT hh_id, tour_id, tour_composition, tour_participants,
       LENGTH(tour_participants) - LENGTH(REPLACE(tour_participants, ' ', '')) + 1 as participant_count
FROM joint_tour 
WHERE 
  (tour_composition = 1 AND participant_count < 2) OR  -- Adults only needs 2+
  (tour_composition = 2 AND participant_count < 2) OR  -- Children only needs 2+  
  (tour_composition = 3 AND participant_count < 2);    -- Mixed needs 2+

-- Check valid time periods
SELECT hh_id, tour_id FROM joint_tour 
WHERE start_period NOT BETWEEN 1 AND 48 
   OR end_period NOT BETWEEN 1 AND 48;

-- Validate mode choices
SELECT DISTINCT tour_mode FROM joint_tour 
WHERE tour_mode NOT BETWEEN 1 AND 17;
```

### Consistency Checks

```sql
-- Joint tours should start/end at home (typically)
SELECT COUNT(*) FROM joint_tour jt
JOIN household h ON jt.hh_id = h.hh_id
WHERE jt.orig_mgra != h.home_mgra;

-- Tour end time should be after start time (handling day rollover)
SELECT hh_id, tour_id, start_period, end_period 
FROM joint_tour
WHERE end_period <= start_period AND end_period > 1;  -- Exclude overnight tours
```

## Usage Examples

### Basic Joint Tour Summary

```sql
-- Joint tour frequency by purpose and composition
SELECT 
  tour_purpose,
  CASE tour_composition 
    WHEN 1 THEN 'Adults Only'
    WHEN 2 THEN 'Children Only'
    WHEN 3 THEN 'Mixed Party'
  END as composition_type,
  COUNT(*) as tour_count,
  AVG(tour_distance) as avg_distance,
  AVG(CASE 
    WHEN tour_mode BETWEEN 1 AND 8 THEN 1 ELSE 0 
  END) as auto_share
FROM joint_tour
GROUP BY tour_purpose, tour_composition
ORDER BY tour_purpose, tour_composition;
```

### Tour Timing Analysis

```sql
-- Joint tour departure time patterns
SELECT 
  CASE 
    WHEN start_period BETWEEN 1 AND 12 THEN 'Early AM (3:00-8:59)'
    WHEN start_period BETWEEN 13 AND 24 THEN 'Morning (9:00-14:59)'
    WHEN start_period BETWEEN 25 AND 36 THEN 'Afternoon (15:00-20:59)'
    ELSE 'Evening (21:00-2:59)'
  END as departure_window,
  tour_purpose,
  COUNT(*) as tour_count
FROM joint_tour
GROUP BY departure_window, tour_purpose
ORDER BY departure_window, tour_purpose;
```

### Household Participation Analysis

```sql
-- Household joint tour participation patterns
SELECT 
  h.hhsize,
  h.workers,
  COUNT(jt.tour_id) as joint_tours,
  AVG(LENGTH(jt.tour_participants) - LENGTH(REPLACE(jt.tour_participants, ' ', '')) + 1) as avg_participants
FROM household h
LEFT JOIN joint_tour jt ON h.hh_id = jt.hh_id
GROUP BY h.hhsize, h.workers
HAVING COUNT(jt.tour_id) > 0
ORDER BY h.hhsize, h.workers;
```