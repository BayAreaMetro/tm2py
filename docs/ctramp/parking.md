# Parking Costs in Activity-Based Models

## Overview

Parking costs affect mode choice and destination choice in the Travel Model through three behavioral model components:
1. **Parking Provision Model** - Determines employer parking benefits (free/reimbursed/paid)
2. **Parking Location Choice Model** - Determines where vehicles park in downtown areas
3. **Mode Choice Models** - Apply parking costs to drive mode utilities based on tour characteristics

**Key Input Data:** Parking cost rates (monthly/daily/hourly) per microzone, along with parking supply data

This documentation explains how parking costs flow through the model system and how they are implemented in Travel Model Two (based on SANDAG ABM).

## Reference Documentation

For detailed model estimation methodology, see:
- [SANDAG ABM Model Estimation Documentation](https://bayareametro.github.io/tm2py/ctramp/SANDAG_ABM_Model_Estimation.docx)

## Data Sources and Model Estimation

### SANDAG Data Collection (2009-2011)

All parking models in TM2 were originally developed and estimated by SANDAG using San Diego region data:

#### 2010-2011 Parking Behavior Survey

**Survey Design:**
- **Sample:** 1,563 persons parking at 48 selected garages and lots throughout downtown San Diego
- **Period:** May 2010 to February 2011
- **Method:** Paper-and-pencil intercept survey at parking facilities

**Data Collected:**
- Demographics (age, gender, income, occupation)
- Trip origin and destination (home location, workplace, activity locations)
- Trip purpose
- Parking payment amount and payment schedule (hourly/daily/monthly)
- Activity duration at destination
- **Employer parking reimbursement** (amount and percentage)
- Parking facility chosen (for location choice modeling)

**Uses:**
- **Parking Provision Model:** Employer reimbursement responses used to estimate the multinomial logit model of free/reimbursed/paid parking alternatives
- **Parking Location Choice Model:** Parking facility choice data used to estimate destination choice within downtown
- **Mode Choice Model:** Parking payment and reimbursement data used to calibrate cost sensitivity parameters

#### 2009-2010 Parking Inventory

**Data Collection:**
- Collected from operators of parking lots and garages throughout San Diego region
- Field visits and spatial data collection for metered street parking
- Private parking supply from CoStar commercial real estate database

**Data Elements:**
- Number of parking stalls by facility type (garage, surface lot, metered street, free on-street)
- Parking rates offered (hourly, daily, monthly)
- Payment schedules and terms
- Operating hours and restrictions
- Geographic location (MGRA/microzone)

**Uses:**
- **Parking Cost Data:** Source for monthly/daily/hourly rates per MGRA
- **Parking Location Choice Model:** Stall counts used as capacity/availability measures in utility functions and to weight survey observations
- **Model Application:** Supply data used to calculate attraction sizes for parking location choice

#### Free Parking Estimation Methodology

For areas with free on-street parking (outside constrained downtown area):
- Estimated from street network data and frontage formulas
- Private parking ratios from CoStar database (stalls per 1000 sq ft by building type)
- Validated against aerial imagery and field observation

### TM2 Implementation Status

**What is Complete:**
- ✅ Parking Provision Model: SANDAG model structure and coefficients used directly
- ✅ Mode Choice Integration: SANDAG utility functions and parking cost variables implemented
- ✅ Code structure: All Java classes and UEC files present

**What is NOT YET Complete:**
- ❌ **Bay Area Parking Cost Data:** Monthly/daily/hourly rates per TAZ/MGRA NOT collected yet
  - Currently using placeholder or SANDAG default values
  - Requires equivalent data collection effort:
    - Commercial parking operator surveys
    - Municipal parking authority data
    - Metered street parking inventory
    - Private parking supply estimates
- ❌ **Parking Location Choice Model for Bay Area:** Not implemented
  - No "parking area 1" defined for SF/Oakland/San Jose downtowns
  - Model code present but not activated
  - Would require identifying constrained parking areas
- ❌ **Bay Area Calibration/Validation:**
  - No re-estimation using Bay Area household travel survey
  - Using SANDAG parking provision rates (41% pay, 35% reimbursed, 12% free)
  - Not validated against Bay Area employer parking benefit data
  - May not reflect Bay Area parking policies and supply conditions

**Data Gaps Impact:**
- Model currently cannot distinguish parking costs between Bay Area locations
- All areas may be treated as "free parking" zones
- Parking provision model may not reflect Bay Area employer practices
- Cannot analyze parking pricing policies accurately for Bay Area

**Recommended Next Steps:**
1. Collect parking rate data for SF, Oakland, San Jose CBDs and major employment centers
2. Define "parking constrained areas" (equivalent to SANDAG parking area 1)
3. Validate parking provision model against Bay Area employer surveys
4. Consider re-estimation using Bay Area household travel survey parking questions
5. Calibrate parking cost sensitivity parameters to observed Bay Area mode shares

## Parking Cost Data

### Three Rate Types

Parking costs are stored as **three separate rates per MGRA** (microzone):

| Rate Type | Variable Name | Usage | Source |
|-----------|---------------|-------|--------|
| Monthly | `mparkcost[mgra]` | Transit pass comparison, regular commuters | Commercial operator data |
| Daily | `dparkcost[mgra]` | Full-day work tours (most common) | Commercial operator data |
| Hourly | `hparkcost[mgra]` | Part-time tours, trip mode choice | Metered spaces + commercial rates |

**Why three rates?** Survey data shows people select payment terms based on trip duration to minimize cost:
- 2-hour trip: Pay $10 hourly (2×$5) vs $20 daily
- 9-hour trip: Pay $20 daily vs $45 hourly (9×$5)
- Regular commuter: Pay $350 monthly vs $440 for 22 daily trips

### Parking Supply Fields

In addition to parking costs, the model uses **parking capacity (number of stalls)** by pricing type to represent supply constraints and availability:

#### Monthly Stalls
- `mstallsoth[mgra]` - Monthly stalls in **other/off-site** facilities (public garages, distant lots)
- `mstallssam[mgra]` - Monthly stalls in **same/on-site** facilities (building parking, adjacent lots)

**Use:** Parking Provision Model and Parking Location Choice Model
- Higher stall counts increase probability of choosing that parking type
- "Same building" stalls proxy for employer-provided on-site parking
- Used to calibrate relative availability of monthly vs daily vs hourly options

#### Daily Stalls  
- `dstallsoth[mgra]` - Daily stalls in **other/off-site** facilities
- `dstallssam[mgra]` - Daily stalls in **same/on-site** facilities

**Use:** Parking Location Choice Model
- Daily stalls represent transient/visitor parking capacity
- Public garages typically have high daily stall counts
- Private facilities may restrict daily parking

#### Hourly Stalls
- `hstallsoth[mgra]` - Hourly stalls in **other/off-site** facilities (metered street, public lots)
- `hstallssam[mgra]` - Hourly stalls in **same/on-site** facilities (building visitor parking)

**Use:** Parking Location Choice Model and Trip Mode Choice
- Metered street parking typically counts as hourly "other" stalls
- Short-term parking for errands, meetings, meal stops
- Critical for intermediate stop trip mode choice

#### Additional Fields
- `numfreehrs[mgra]` - Number of free hours before parking charges apply
  - **Use:** Some facilities offer "first 2 hours free" or validation
  - Affects effective hourly cost for short-duration activities
  - Used in parking location choice utility calculations

#### Parking Area Flag
- `mgraParkArea[mgra]` - Indicates if MGRA is in "parking area 1" (constrained downtown area)
  - **Values:** 1 = parking location choice applies, 0 or null = no location choice needed
  - **Use:** Determines whether to run parking location choice model
  - Only MGRAs with `parkArea == 1` trigger detailed parking facility selection

### How Fields Are Used Together

**In Tour Mode Choice:**
- Uses `mparkcost`, `dparkcost`, or `hparkcost` based on tour duration
- Stall counts not directly used (already embedded in logsum-weighted costs)

**In Parking Location Choice Model:**
- **Cost fields** determine utility penalties for expensive parking
- **Stall fields** represent capacity/availability:
  - Higher stalls = higher probability of selection
  - "Same" vs "other" distinction captures on-site vs off-site preference
  - Weighted by payment term (monthly/daily/hourly) to match traveler's duration
- **Free hours** offset short-duration hourly costs
- Model produces **logsum-weighted average costs** that reflect both price and availability

**In Trip Mode Choice (Intermediate Stops):**
- Uses `hparkcost` and `hstalls` for stops where person drives from tour destination
- Accounts for parking availability near stop activities

### Field Relationships and Data Quality

**Expected Patterns:**
```
Monthly stalls ≤ Daily stalls ≤ Hourly stalls  
  (facilities typically offer multiple payment terms for same spaces)

Monthly cost/22 ≈ Daily cost < Hourly cost × 8
  (monthly discount exist, daily cheaper than all-day hourly)

Hourly stalls (other) >> Hourly stalls (same)
  (street parking and public lots dominate short-term parking)

Daily stalls (same) >> Monthly stalls (same)  
  (employer parking typically monthly, building visitor parking typically daily/hourly)
```

**Missing Data Handling:**
- Cost = $0 indicates free parking (explicit zero, not missing)
- Stalls = 0 indicates no parking available for that payment type
- Missing MGRA records default to free parking with unlimited supply

**Data Sources** (from [SANDAG documentation](https://bayareametro.github.io/tm2py/ctramp/SANDAG_ABM_Model_Estimation.docx)):
- Commercial lots/garages: Payment terms from operators
- Metered spaces: City spatial data + field visits
- Free on-street: Estimated from street frontage formulas
- Private stalls: CoStar database parking ratios

**TM2 Data Status:** \u274c Bay Area parking cost data (M/D/H rates per TAZ/MGRA) has **NOT been collected yet**. Model may be using placeholder values or no cost differentiation between zones.

### Geographic Scope

- **Constrained areas** (downtown + select zones): All three rates specified
- **Outside constrained areas**: Parking assumed **FREE** (cost = $0)
- **TM2:** Parking constrained areas not yet defined for Bay Area

### Implementation

**Java:**
```java
// MgraDataManager.java
calculateMgraAvgParkingCosts()  // Loads M/D/H arrays from input data
mparkcost[]  // Monthly rate per MGRA
dparkcost[]  // Daily rate per MGRA  
hparkcost[]  // Hourly rate per MGRA
lsWgtAvgCostM[]  // Logsum-weighted monthly cost
lsWgtAvgCostD[]  // Logsum-weighted daily cost
lsWgtAvgCostH[]  // Logsum-weighted hourly cost
```

**UEC Files:**
- `uec/TourModeChoice.xls` - Defines parking cost variables (rows 91-93)
- `uec/TripModeChoice.xls` - Parking costs for intermediate stops

## Parking Provision Model

### Purpose

Determines which workers have employer-provided parking benefits. Runs BEFORE mode choice as a pre-processor.

**Model Type:** Multinomial Logit with 3 alternatives

### Estimation Data

**Source:** 2010-2011 SANDAG Parking Behavior Survey
- 1,563 respondents asked about employer parking benefits
- Responses categorized into free/reimbursed/pay alternatives
- Cross-tabulated with workplace characteristics and demographics

**Observed Distribution (San Diego):**
- 41% pay without reimbursement
- 35% receive reimbursement (full or partial)
- 12% have free on-site parking
- 11% partial reimbursement

**Note:** These percentages from San Diego may not reflect Bay Area employer practices. TM2 has not been re-validated with Bay Area data.

### Alternatives

| Alternative | Code | Meaning | Mode Choice Impact |
|-------------|------|---------|-------------------|
| Free On-Site | `FP_MODEL_FREE_ALT = 1` | Free parking at workplace | Parking cost = $0 |
| Reimbursement | `FP_MODEL_REIMB_ALT = 3` | Partial/full reimbursement | Cost × (1 - reimburseProportion) |
| Pay for Parking | `FP_MODEL_PAY_ALT = 2` | No benefits | Full parking cost applies |
| No Reimbursement | `FP_MODEL_NO_REIMBURSEMENT_CHOICE = -1` | (constant for outside downtown) | Full parking cost applies |

### Critical Distinction

**Free On-Site ≠ Full Reimbursement**

- **Free On-Site**: Person must park at workplace location (no choice)
- **Reimbursement**: Person can choose where to park and may accept partial reimbursement to park closer to destination

### Model Variables

**Transportation System** (ease of attracting workers without parking benefits):
- Average monthly parking costs in nearby MGRAs
- Transit accessibility to workplace
- Walk distance to rail station
- Workplace location choice shadow price

**Workplace Characteristics** (urban form):
- Parking stall density at workplace
- Employment density
- Employment by industry
- College enrollment, office tower presence, zoning

**Person Characteristics** (demographics):
- Income, occupation
- Commute distance
- Full/part-time worker/student status
- Age, gender

### Implementation

**Java:**
```java
// ParkingProvisionModel.java
person.setFreeParkingAvailableResult(chosenAlt)  // Store result

// TourModeChoiceDMU.java  
person.getFreeParkingAvailableResult()  // Retrieve in mode choice
```

**UEC:**
```
uec/ParkingProvision.xls
```

### Geographic Rule

**Workers outside downtown San Diego area automatically receive FREE parking** (no model run needed).

## Mode Choice Integration

### How Parking Costs Enter Utilities

Parking costs affect **drive mode alternatives only** (DA, SR2, SR3+).

**UEC Variables:**
```
@monthlyParkingCost   - Row 93, TourModeChoice.xls
@dailyParkingCost     - Row 91, TourModeChoice.xls  
@hourlyParkingCost    - Row 92, TourModeChoice.xls
@freeOnsite           - Indicator: 1 if free parking, 0 otherwise
@reimburseProportion  - Proportion of cost reimbursed (0 to 1)
```

**Typical Utility Expression:**
```
Utility(DriveAlone) = ASC + β_time × time + β_cost × effectiveParkingCost + ...

where:
effectiveParkingCost = parkingCost × (1 - @freeOnsite) × (1 - @reimburseProportion)
```

### Trip vs Tour Parking Behavior

#### Tour-Level Parking (Primary)

For the **tour mode choice**, parking cost is incurred at the **tour destination** (e.g., workplace):
- Person drives from home to work → pays for parking at work MGRA
- Parking cost factors into tour mode choice (drive vs transit vs other)
- If person has free parking provision, cost = $0

#### Trip-Level Parking (Intermediate Stops)

For **intermediate stops** on a tour (e.g., lunch during work tour), the traveler can:
1. **Walk/bike/transit** from tour destination to stop → no additional parking cost
2. **Drive** from tour destination to stop → incur parking cost at stop destination

**Implementation:**
- Trip mode choice includes all modes (walk, bike, transit, drive)
- If drive mode chosen for trip, **hourly parking cost** applies at trip destination
- Trip mode choice DMU provides:
  - `getHourlyParkingCostTripOrig()` - cost at trip origin (usually tour destination)
  - `getHourlyParkingCostTripDest()` - cost at stop destination
  - `getHourlyParkingCostTourDest()` - cost at tour destination

**Example: Work tour with lunch stop**
- Tour: Home → Work → Home (drive to work)
  - Parking cost at Work MGRA (daily rate)
- Intermediate Stop: Work → Restaurant → Work
  - Option 1: Walk to restaurant (no additional parking cost, car stays at work)
  - Option 2: Drive to restaurant (hourly parking cost at restaurant, may need to re-park at work)

**Key Code:**
```java
// IntermediateStopChoiceModels.java
if ( modelStructure.getTripModeIsSovOrHov( modeAlt ) ) {
    park = selectParkingLocation( household, tour, stop );                
    stop.setPark( park );
}
```

The model explicitly selects parking location for drive mode trips to intermediate stops.

#### Parking Cost Accumulation

Total parking costs for a tour:
```
Total = ParkingCost(TourDestination) + Σ ParkingCost(IntermediateStop_i if drive mode)
```

Most travelers keep their vehicle parked at the tour destination and walk/transit to nearby intermediate stops, avoiding additional parking costs.

### Rate Selection Logic

The model **selects** which rate to use based on tour characteristics:

| Tour Type | Rate Used | Reasoning |
|-----------|-----------|-----------|
| Full-time work tour (8-11 hours) | `@dailyParkingCost` | Cost effective for full day |
| Part-time work tour (< 8 hours) | `@hourlyParkingCost` | Cost effective for short duration |
| Regular commuter | `@monthlyParkingCost / 22` | Comparing monthly pass option |
| Intermediate stops (trip MC) | `@hourlyParkingCostTripDest` | Short-duration parking |

**Java Methods:**
```java
// SandagTourModeChoiceDMU.java
getMonthlyParkingCost()  // Method index 23
getDailyParkingCost()    // Method index 24
getHourlyParkingCost()   // Method index 25

// TripModeChoiceDMU.java
getMonthlyParkingCostTourDest()  // Returns lsWgtAvgCostM[tour.getTourDestMgra()]
getDailyParkingCostTourDest()    // Returns lsWgtAvgCostD[tour.getTourDestMgra()]
getHourlyParkingCostTourDest()   // Returns lsWgtAvgCostH[tour.getTourDestMgra()]
getHourlyParkingCostTripOrig()   // For intermediate stops
getHourlyParkingCostTripDest()   // For intermediate stops
getFreeOnsite()                  // Returns 1 if free, 0 otherwise
```

### Nested Logit Structure

Parking costs enter at the **lowest nest level** and flow up through logsums:

```
Level 3 (Lowest): DA-Free, DA-Pay, SR2-Free, SR2-Pay, ...
                  ↑ Parking cost applied here
Level 2:          Drive-Alone, Shared-Ride-2, Shared-Ride-3+, ...
                  ↑ Logsum aggregates lower alternatives
Level 1 (Top):    Auto, Transit, Non-Motorized
                  ↑ Final mode choice logsum
```

**Logsum Calculation:**
```
Logsum_lower = ln(Σ exp(Utility_i / μ_lower))
Logsum_upper = ln(Σ exp(Logsum_lower / μ_upper))
```

Higher parking costs → Lower DA-Pay/SR2-Pay utilities → Lower drive mode logsums → Lower overall accessibility

## Flow to Destination Choice

### Accessibility via Mode Choice Logsums

Mode choice logsums (which embed parking costs) feed into destination choice as **accessibility measures**:

```
Utility(destination) = ... + β_logsum × ModeChoiceLogsum(destination)
```

**Effect of Parking Costs on Destination Choice:**
1. High parking cost at destination → Lower drive mode utilities
2. Lower drive utilities → Lower mode choice logsum
3. Lower logsum → Lower destination accessibility
4. Lower accessibility → Destination less likely to be chosen

**Result:** Workers without free parking avoid high-cost parking destinations.

### Parking Location Choice Model

#### Overview

For trips with destinations in "parking area 1" (downtown San Diego in SANDAG), an additional **Parking Location Choice Model** determines the specific parking facility where the vehicle is parked.

**Purpose:** 
- Assigns parking to specific MGRAs/lots within downtown
- Produces more accurate traffic assignments at small geographic scales
- Generates logsum-weighted average parking costs for mode choice

**When Applied:**
- Only for trips where destination MGRA is in parking area 1
- Only for drive modes (SOV/HOV)
- Skipped if person has free on-site parking (parks at destination MGRA)
- Skipped if person worked at home (no parking provision choice made)

#### Model Structure

**Model Type:** Multinomial logit choice among a sample of parking facilities near the trip destination

**Alternatives:** Sample of MGRAs within parking area 1, drawn based on:
- Distance from trip origin
- Distance from trip destination  
- Parking supply (stall counts)
- Parking costs

**Choice Set:** Maximum sample size defined by `MAX_PLC_SAMPLE_SIZE` parameter

#### Utility Function Variables

**Trip Characteristics:**
- Activity duration (number of time intervals from arrival to departure)
- Trip origin and destination MGRAs
- Trip purpose (work, shopping, other)

**Parking Alternative Attributes:**
- Distance from parking MGRA to trip origin (`altOsDistances`)
- Distance from parking MGRA to trip destination (`altSdDistances`)
- Parking costs by rate type:
  - Monthly: `altMstallsoth` (off-site), `altMstallssam` (on-site), `altMparkcost`
  - Daily: `altDstallsoth` (off-site), `altDstallssam` (on-site), `altDparkcost`
  - Hourly: `altHstallsoth` (off-site), `altHstallssam` (on-site), `altHparkcost`
  - Number of free hours before charges apply: `altNumfreehrs`
  
*See "Parking Supply Fields" section above for detailed explanation of stalls fields*

**How Stalls Affect Choice:**
- Higher stall counts increase alternative selection probability (capacity effect)
- "Same building" (on-site) stalls may have preference over "other" (off-site)
- Model selects among payment types based on trip duration and stall availability
- Cost and capacity jointly determine parking location utility

**Person/Tour Attributes:**
- Person type (full-time worker, part-time worker, student, etc.)
- **Employer parking reimbursement percentage** from Parking Provision Model
  - For work trips, reimbursement reduces effective parking cost
  - `effectiveCost = parkingCost × (1 - reimbursePct)`

#### Key Modeling Detail: Reimbursement Application

For **work trips only**, the model subtracts employer reimbursement from parking costs:

```java
parkingChoiceDmuObj.setReimbPct( tour.getPersonObject().getParkingReimbursement() );
```

**In UEC utility function:**
```
Net Cost = Daily Parking Cost × (1 - Reimbursement %)
```

**Effect:** Workers with higher reimbursement percentages are less sensitive to parking prices and may choose more expensive (but closer/more convenient) parking facilities.

#### Logsum-Weighted Average Costs

The **coefficients from this model** are used to calculate accessibility-weighted average parking costs:

```java
lsWgtAvgCostM[]  // Logsum-weighted monthly cost per MGRA
lsWgtAvgCostD[]  // Logsum-weighted daily cost per MGRA  
lsWgtAvgCostH[]  // Logsum-weighted hourly cost per MGRA
```

**Calculation Method:**
- For each destination MGRA in parking area 1
- Sample nearby parking alternatives
- Calculate expected minimum parking cost using logsum formula:

```
LsWgtAvgCost = -ln(Σ exp(Utility_i)) / β_cost
```

Where `Utility_i` contains parking costs and distance penalties from parking location choice model.

**Usage:** These logsum-weighted costs are used in:
- Tour mode choice utilities (instead of raw parking costs)
- Trip mode choice utilities
- Destination choice logsums

**Advantage:** Accounts for substitution among parking options—high cost at destination is offset if cheaper alternatives exist nearby.

#### Estimation Data

**Dataset:** 2010-2011 SANDAG Parking Behavior Survey
- Observed parking facility choices from 1,563 survey respondents
- Trip characteristics (origin, destination, duration, purpose)
- Employer reimbursement amounts
- Weighted by 2009-2010 Parking Inventory stall counts

**Key Coefficients Estimated:**
- Distance sensitivity (walk distance penalty)
- Parking cost coefficient by rate type
- Reimbursement interaction effect
- Purpose-specific constants

#### Implementation

**Java Methods:**
```java
// IntermediateStopChoiceModels.java
selectParkingLocation(household, tour, stop)  // Main choice method
setupParkLocationChoiceAlternativeArrays(tripOrigMgra, tripDestMgra)  // Sample alternatives

// ParkLocationChoiceDMU.java
setActivityIntervals(intervals)    // Duration
setDestPurpose(purposeIndex)       // Trip purpose  
setReimbPct(percentage)            // Employer reimbursement
setParkingCostsM/D/H(costs)        // Alternative costs
```

**UEC File:**
```
uec/ParkLocationChoice.xls
```

**Data Setup:**
```java
// MgraDataManager.java
calculateMgraAvgParkingCosts()  // Computes logsum-weighted costs
```

#### TM2 Status

**Currently:** Parking location model code is present but **NOT activated** for Bay Area:
- No "parking area 1" defined for SF/Oakland/San Jose downtowns
- Model would always return `-1` (no parking choice needed)
- Logsum-weighted costs may default to raw costs

**To Activate:**
1. Define parking constrained areas (`parkArea` attribute in MGRA data)
2. Set `mgraAltParkArea.get(mgra) == 1` for downtown zones
3. Collect detailed parking facility data for these areas
4. Consider re-estimation or calibration for Bay Area

## Model Sequence

```
1. Parking Provision Model
   ↓ (person.freeParkingAvailable)

2. Workplace/School Destination Choice  
   ← (uses mode choice logsums with parking costs)

3. Tour Mode Choice
   ↓ (selects appropriate M/D/H rate, applies free parking status)

4. Mode Choice Logsum  
   ↓ (aggregates utilities with parking costs)

5. Accessibility Calculations
   ← (uses logsums for higher-level models)
```

## Policy Analysis Examples

### Scenario 1: Increase Downtown Parking Costs

**Change:** Increase `dparkcost[]` and `hparkcost[]` for downtown MGRAs by 50%

**Expected Effects:**
- Drive mode utilities decrease for affected zones
- Mode choice shifts toward transit (for workers without free parking)
- Workplace locations may shift away from high-cost areas
- VMT decreases concentrated in downtown

**Who is Affected:**
- Workers without free parking provision (41% in SANDAG survey)
- Visitors/shoppers making intermediate stops downtown

**Who is NOT Affected:**
- Workers with free on-site parking (12% in SANDAG survey)
- Workers with full reimbursement (35% in SANDAG survey)
- Workers outside constrained area (automatic free parking)

### Scenario 2: Employer Parking Cash-Out Program

**Change:** Modify `ParkingProvisionModel` parameters to increase reimbursement alternative probability

**Expected Effects:**
- More workers choose reimbursement over free on-site
- Increased flexibility in parking location choice
- May increase transit mode share (workers "cash out" and take transit)
- Parking demand spreads over larger geographic area

### Scenario 3: Remove Monthly Parking Discount

**Change:** Set `mparkcost[] = dparkcost[] × 22` (eliminate monthly discount)

**Expected Effects:**
- Monthly pass becomes less attractive vs daily payment
- Increased transit pass enrollment (via transit subsidy model)
- Primarily affects regular commuters
- Minimal impact on part-time workers or occasional trips

## Validation Checks

When implementing or modifying parking costs, validate:

1. **Rate Usage Patterns:**
   - Monthly used primarily for work tours (8-11 hour duration)
   - Daily used for full-day tours (peak at 9 hours)
   - Hourly used for part-time tours (< 5 hours) and trip mode choice

2. **Provision Model Results:**
   - 41% pay without reimbursement
   - 35% pay with full reimbursement  
   - 12% free on-site
   - 11% partial reimbursement
   
   **(From SANDAG 2010-2011 Parking Behavior Survey - may differ for Bay Area)**

3. **Geographic Distribution:**
   - Parking costs concentrated in constrained areas only
   - Free parking outside downtown
   - **TM2 Note:** Bay Area parking cost geography not yet defined

4. **Mode Share Sensitivity:**
   - Workers without free parking more sensitive to parking costs
   - Drive mode share decreases with parking cost increases in constrained areas
   - **TM2 Note:** Cannot validate without Bay Area parking cost data

5. **Parking Location Choice:**
   - Only activated for parking area 1 destinations
   - Logsum-weighted costs reflect spatial distribution of parking alternatives
   - **TM2 Note:** Not currently activated; no parking area 1 defined for Bay Area

6. **Data Quality Checks:**
   - Verify M/D/H rates follow expected pattern: Monthly/22 ≈ Daily < Hourly × 8
   - Check for missing data (should be explicit $0 for free areas, not null)
   - Validate parking area flags match high-cost downtown zones
   - Confirm logsum-weighted costs calculated correctly from parking location model

## Files and Code References

### UEC Files
- `uec/TourModeChoice.xls` - Tour mode utilities with parking costs (rows 91-93)
- `uec/TripModeChoice.xls` - Trip mode utilities  
- `uec/ParkingProvision.xls` - Parking provision model specification
- `uec/ParkLocationChoice.xls` - Downtown parking location choice
- `uec/Accessibilities.xls` - Mode choice logsums usage

### Java Classes  
- `MgraDataManager.java` - Load parking cost data, calculate logsum-weighted costs
- `ParkingProvisionModel.java` - Employer parking provision model
- `TourModeChoiceDMU.java` - Base tour mode choice DMU
- `SandagTourModeChoiceDMU.java` - SANDAG-specific parking cost methods
- `TripModeChoiceDMU.java` - Trip mode choice with parking costs
- `TourModeChoiceModel.java` - Tour mode choice model execution
- `Person.java` - Stores `freeParkingAvailable` result

### Key Methods
```java
// Data Loading
MgraDataManager.calculateMgraAvgParkingCosts()
MgraDataManager.getMParkCost()
MgraDataManager.getDParkCost()
MgraDataManager.getHParkCost()

// Provision Model
ParkingProvisionModel.applyModel()
Person.setFreeParkingAvailableResult(int)
Person.getFreeParkingAvailableResult()

// Mode Choice DMU
TourModeChoiceDMU.getFreeParkingEligibility()
SandagTourModeChoiceDMU - methods 23, 24, 25 for M/D/H costs
TripModeChoiceDMU.getMonthlyParkingCostTourDest()
TripModeChoiceDMU.getDailyParkingCostTourDest()
TripModeChoiceDMU.getHourlyParkingCostTourDest()
TripModeChoiceDMU.getFreeOnsite()
```

## Summary

Parking costs in the activity-based model:

**Model Structure (from SANDAG):**
- Are represented by **three separate rates** (M/D/H) per microzone
- Are **conditionally selected** based on tour characteristics (not averaged)
- **Only affect** drive modes in areas with parking charges
- Flow through **nested logsums** to destination choice
- Are **zeroed out** for workers with free parking provision
- Impact concentrated on **downtown workers without parking benefits**

**Data Requirements:**
- Parking Provision Model: Estimated from 2010-2011 SANDAG Parking Behavior Survey (1,563 respondents)
- Parking Location Model: Estimated from same survey + 2009-2010 SANDAG Parking Inventory
- Parking Cost Data (M/D/H rates): Derived from parking inventory and operator surveys

**TM2 Implementation Status:**
- \u2705 Model code and structure: Complete (transferred from SANDAG)
- \u274c **Bay Area parking cost data: NOT collected**  
- \u274c **Parking constrained areas: NOT defined for Bay Area**
- \u274c **Parking location model: Code present but not activated**
- \u274c **Calibration/validation: Not performed with Bay Area data**

**Critical Data Gaps:**
- Without Bay Area parking cost data, model cannot distinguish parking costs between zones
- All areas may effectively be treated as free parking
- Cannot accurately analyze parking pricing policies or downtown development impacts
- Parking provision model uses San Diego percentages, may not reflect Bay Area employer practices

Understanding this system is critical for analyzing policies affecting parking pricing, employer provisions, or downtown development. **However, TM2 requires completion of Bay Area data collection before parking cost models can be fully utilized for policy analysis.**
