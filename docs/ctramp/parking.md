# Parking Costs in Activity-Based Models

## Overview

Parking costs affect mode choice and destination choice in the Travel Model through three separate components:
1. **Parking Provision Model** - Determines employer parking benefits (free/reimbursed/paid)
2. **Parking Cost Data** - Three rates (monthly/daily/hourly) per microzone
3. **Mode Choice Models** - Apply parking costs to drive mode utilities based on tour characteristics

This documentation explains how parking costs flow through the model system and how they are implemented in Travel Model Two (based on SANDAG ABM).

## Related Documentation

- **[CT-RAMP System Overview](overview.md)** - Introduction to the activity-based modeling system
- **[Tour Mode Choice Model](tour_mode_choice_documentation.md)** - Tour-level mode choice where parking costs apply
- **[Trip Mode Choice Model](trip_mode_choice_documentation.md)** - Trip-level mode choice for intermediate stops
- **[Tour Destination Choice Model](tour_destination_choice_documentation.md)** - How parking costs affect destination choice through logsums
- **[UEC Framework](uec-framework.md)** - Mathematical framework for utility calculations
- **[Value of Time Analysis](VALUE_OF_TIME_ANALYSIS.md)** - Income-stratified time values used with parking costs

## Reference Documentation

For detailed model estimation methodology, see:
- [SANDAG ABM Model Estimation Documentation](https://bayareametro.github.io/tm2py/ctramp/SANDAG_ABM_Model_Estimation.docx)

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

**Data Sources** (from [SANDAG documentation](https://bayareametro.github.io/tm2py/ctramp/SANDAG_ABM_Model_Estimation.docx)):
- Commercial lots/garages: Payment terms from operators
- Metered spaces: City spatial data + field visits
- Free on-street: Estimated from street frontage formulas
- Private stalls: CoStar database parking ratios

### Geographic Scope

- **Constrained areas** (downtown + select zones): All three rates specified
- **Outside constrained areas**: Parking assumed **FREE** (cost = $0)

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

**UEC Files:** (see [UEC Framework](uec-framework.md) for details)
- `uec/TourModeChoice.xls` - Defines parking cost variables (rows 91-93)
- `uec/TripModeChoice.xls` - Parking costs for intermediate stops

## Parking Provision Model

### Purpose

Determines which workers have employer-provided parking benefits. Runs BEFORE mode choice as a pre-processor.

**Model Type:** Multinomial Logit with 3 alternatives

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

Parking costs affect **drive mode alternatives only** (DA, SR2, SR3+) in both [tour mode choice](tour_mode_choice_documentation.md) and [trip mode choice](trip_mode_choice_documentation.md).

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

### Rate Selection Logic

The [tour mode choice model](tour_mode_choice_documentation.md) **selects** which rate to use based on tour characteristics:

| Tour Type | Rate Used | Reasoning |
|-----------|-----------|-----------|
| Full-time work tour (8-11 hours) | `@dailyParkingCost` | Cost effective for full day |
| Part-time work tour (< 8 hours) | `@hourlyParkingCost` | Cost effective for short duration |
| Regular commuter | `@monthlyParkingCost / 22` | Comparing monthly pass option |
| Intermediate stops ([trip mode choice](trip_mode_choice_documentation.md)) | `@hourlyParkingCostTripDest` | Short-duration parking |

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

Mode choice logsums (which embed parking costs) feed into [destination choice](tour_destination_choice_documentation.md) as **accessibility measures**:

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

For downtown San Diego, an additional **Parking Location Choice Model** determines WHERE vehicles are parked:

**Purpose:** Assigns parking to specific lots/garages within downtown for detailed traffic assignment

**Key for Mode Choice:** Coefficients from parking location model used to calculate **logsum-weighted average parking costs** (`lsWgtAvgCostM/D/H` arrays)

**UEC:**
```
uec/ParkLocationChoice.xls
```

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
   (From SANDAG 2010-2011 Parking Behavior Survey)

3. **Geographic Distribution:**
   - Parking costs concentrated in constrained areas only
   - Free parking outside downtown

4. **Mode Share Sensitivity:**
   - Workers without free parking more sensitive to parking costs
   - Drive mode share decreases with parking cost increases in constrained areas

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
- Are represented by **three separate rates** (M/D/H) per microzone
- Are **conditionally selected** based on tour characteristics (not averaged)
- **Only affect** drive modes in areas with parking charges
- Flow through **nested logsums** to destination choice
- Are **zeroed out** for workers with free parking provision
- Impact concentrated on **downtown workers without parking benefits**

Understanding this system is critical for analyzing policies affecting parking pricing, employer provisions, or downtown development.
