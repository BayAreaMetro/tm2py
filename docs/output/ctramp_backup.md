# CTRAMP Output Files

The Comprehensive Travel Demand Model (CTRAMP) generates detailed microsimulation outputs that capture individual and household travel behavior patterns.

## Core Output Files

### Individual Tour Data
**File Pattern**: `ctramp_output/indivTourData_[iteration].csv`

**Description**: Complete tour-level data for all individuals in the synthetic population

**Key Fields**:
- `hh_id` - Household ID
- `person_id` - Person ID within household
- `person_num` - Person number
- `tour_id` - Unique tour identifier
- `tour_category` - Tour type (mandatory, non-mandatory, at-work)
- `tour_purpose` - Specific purpose (work, school, shop, etc.)
- `orig_taz` - Origin TAZ
- `dest_taz` - Destination TAZ
- `orig_maz` - Origin MAZ
- `dest_maz` - Destination MAZ
- `start_period` - Tour start time period
- `end_period` - Tour end time period
- `tour_mode` - Primary tour mode
- `num_ob_stops` - Outbound stops
- `num_ib_stops` - Inbound stops

### Individual Trip Data
**File Pattern**: `ctramp_output/indivTripData_[iteration].csv`

**Description**: Trip-level disaggregate data showing each individual trip

**Key Fields**:
- `hh_id` - Household ID
- `person_id` - Person ID
- `tour_id` - Parent tour ID
- `stop_id` - Stop sequence number
- `inbound` - Direction (0=outbound, 1=inbound)
- `trip_purpose` - Trip purpose
- `orig_taz`, `dest_taz` - Origin/destination TAZ
- `orig_maz`, `dest_maz` - Origin/destination MAZ
- `trip_mode` - Trip mode
- `trip_period` - Time period
- `trip_distance` - Trip distance
- `trip_time` - Trip time

### Joint Tour Data
**File Pattern**: `ctramp_output/jointTourData_[iteration].csv`

**Description**: Tours taken by multiple household members together

**Key Fields**:
- `hh_id` - Household ID
- `tour_id` - Joint tour ID
- `tour_purpose` - Joint tour purpose
- `tour_composition` - Participating household members
- `num_participants` - Number of people on tour
- `orig_taz`, `dest_taz` - Origin/destination
- `start_period`, `end_period` - Timing
- `tour_mode` - Transportation mode

### Joint Trip Data
**File Pattern**: `ctramp_output/jointTripData_[iteration].csv`

**Description**: Individual trips within joint tours

## Transit-Specific Outputs

### Resimulated Transit Trips
**File Pattern**: `ctramp_output/indivTripDataResim_[iteration]_[inner_iteration].csv`

**Description**: Transit trips that have been resimulated with updated level-of-service

**Purpose**: 
- Accounts for transit capacity constraints
- Incorporates crowding effects
- Updates path choices based on congested conditions

**Additional Fields**:
- `orig_tap` - Origin Transit Access Point
- `dest_tap` - Destination Transit Access Point
- `set` - Route set used
- `actual_time` - Actual travel time experienced
- `crowding_penalty` - Additional time due to crowding

## Park-and-Ride Outputs

### Unconstrained PNR Demand
**File Pattern**: `ctramp_output/unconstrainedPNRDemand_[iteration]0.csv`

**Description**: Park-and-ride demand without parking capacity constraints

### Constrained PNR Demand  
**File Pattern**: `ctramp_output/constrainedPNRDemand_[iteration]1.csv`

**Description**: Park-and-ride demand after applying parking capacity limits

**Fields**:
- `TAP` - Transit Access Point ID
- `period` - Time period
- `demand` - Number of parking spaces demanded
- `capacity` - Available parking capacity
- `utilization` - Capacity utilization rate

## Household and Person Files

### Household Results
**File Pattern**: `ctramp_output/householdData_[iteration].csv`

**Description**: Household-level model results and characteristics

**Key Fields**:
- `hh_id` - Household ID
- `home_taz` - Home location TAZ
- `home_maz` - Home location MAZ
- `income` - Household income category
- `autos` - Number of automobiles owned
- `workers` - Number of workers
- `size` - Household size
- `auto_ownership_logsum` - Auto ownership utility
- `cdap_pattern` - Coordinated Daily Activity Pattern

### Person Results
**File Pattern**: `ctramp_output/personData_[iteration].csv`

**Description**: Person-level model results

**Key Fields**:
- `hh_id` - Household ID
- `person_id` - Person ID
- `person_num` - Person number in household
- `age` - Person age
- `gender` - Gender
- `type` - Person type (full-time worker, student, etc.)
- `value_of_time` - Personal value of time
- `activity_pattern` - Daily activity pattern

## Mode and Time Period Codes

### Tour/Trip Mode Codes
```
1  = DRIVEALONE
2  = SHARED2GP  
3  = SHARED3GP
4  = WALK
5  = BIKE
6  = WALK_LOC
7  = WALK_LRF
8  = WALK_EXP
9  = WALK_HVY
10 = WALK_COM
11 = DRIVE_LOC
12 = DRIVE_LRF
13 = DRIVE_EXP
14 = DRIVE_HVY
15 = DRIVE_COM
16 = TAXI
17 = TNC_SINGLE
18 = TNC_SHARED
```

### Time Period Codes
```
1 = EA (3:00-6:00)
2 = AM (6:00-10:00)
3 = MD (10:00-15:00)
4 = PM (15:00-19:00)
5 = EV (19:00-3:00)
```

### Tour Purpose Codes
```
Mandatory:
- work
- school
- university

Non-Mandatory:
- escort
- shopping
- othmaint
- othdiscr
- eatout
- social

At-Work:
- atwork
```

## Data Processing Notes

- **Iteration Numbers**: Files are generated for each global iteration (typically 1-3)
- **Sample Rates**: Early iterations may use sampling to reduce computational time
- **Coordinate Systems**: TAZ coordinates in State Plane, MAZ coordinates available
- **Time Representation**: Periods are half-hour increments starting at 3:00 AM
- **Missing Values**: -1 typically indicates not applicable or missing data
- **File Sizes**: Full population files can be several GB; consider sampling for analysis

## Quality Assurance

### Key Validation Checks
1. **Total Population**: Verify person/household counts match input synthetic population
2. **Trip Rates**: Compare against observed survey data
3. **Mode Shares**: Validate against census and survey benchmarks
4. **Temporal Distribution**: Check peak spreading and time-of-day patterns
5. **Spatial Distribution**: Verify geographic trip patterns
