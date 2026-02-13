# Parking Costs

## Overview

Parking costs affect mode choice and destination choice in the Travel Model through three separate components:
1. **Parking Provision Model** - Determines employer parking benefits (free/reimbursed/paid)
2. **Parking Cost Data** - Three rates (monthly/daily/hourly) per microzone
3. **Mode Choice Models** - Apply parking costs to drive mode utilities based on tour characteristics

This documentation explains how parking costs flow through the model system and how they are implemented in Travel Model Two (based on SANDAG ABM). It includes detailed information on **data collection** (2010-2011 Parking Behavior Survey, 2009-2010 Parking Inventory), **model estimation** methodology, and **variable selection** rationale.

**Quick Navigation:**
- [Data Collection & Sources](#data-collection-and-sources) - Survey methodology, sample characteristics, inventory sources
- [Parking Cost Data](#parking-cost-data) - Three rate types and why they were selected  
- [Data Flow](#data-flow-from-survey-to-model-variables) - How raw data becomes model variables
- [Parking Provision Model](#parking-provision-model) - Employer benefits model with estimation details
- [Mode Choice Integration](#mode-choice-integration) - How costs enter utilities
- [Parking Location Choice](#parking-location-choice-model) - Downtown spatial assignment
- [Policy Analysis](#policy-analysis-examples) - Example scenarios
- [Summary](#summary) - Key takeaways and design decisions

## Related Documentation

- **[CT-RAMP System Overview](overview.md)** - Introduction to the activity-based modeling system
- **[Tour Mode Choice Model](tour_mode_choice_documentation.md)** - Tour-level mode choice where parking costs apply
- **[Trip Mode Choice Model](trip_mode_choice_documentation.md)** - Trip-level mode choice for intermediate stops
- **[Tour Destination Choice Model](tour_destination_choice_documentation.md)** - How parking costs affect destination choice through logsums
- **[UEC Framework](uec-framework.md)** - Mathematical framework for utility calculations
- **[Value of Time Analysis](VALUE_OF_TIME_ANALYSIS.md)** - Income-stratified time values used with parking costs

## Reference Documentation

This documentation draws extensively from:
- **SANDAG ABM Model Design** - [Local file](SANDAG_ABM_Model_Design.txt) | [Online (docx)](https://bayareametro.github.io/tm2py/ctramp/SANDAG_ABM_Model_Design.docx)
- **SANDAG ABM Model Estimation** - [Online (docx)](https://bayareametro.github.io/tm2py/ctramp/SANDAG_ABM_Model_Estimation.docx)

The sections below on data collection, model estimation, and variable selection are synthesized from these sources.

## Data Collection and Sources

### Primary Data Sources

Two major data collection efforts support the parking models:

1. **2010-2011 Parking Behavior Survey** - Behavioral data on parking choices
2. **2009-2010 Parking Inventory** - Supply data on parking spaces and pricing
3. **Transit On-Board Survey** - Enrichment data to address survey bias

### Parking Behavior Survey

**Survey Period:** May 2010 - February 2011  
**Sample Size:** 1,563 persons  
**Locations:** 48 selected commercial garages and lots throughout downtown San Diego  
**Method:** Paper-and-pencil intercept survey

**Questions Asked:**
- Demographics (income, occupation, age, gender)
- Trip origin and destination (cross-streets)
- Trip purpose (work, school, other)
- Payment amount and schedule (hourly/daily/monthly)
- Activity duration
- Employer reimbursement (amount and percentage)
- Employment status (full-time/part-time)
- Parking location characteristics

**Operator Data Collected:**
- Number of stalls by classification
- Prices offered
- Payment schedules available

**Survey Sample Characteristics:**

*Payment Terms by Purpose (524 observations after filtering):*

| Payment Term | Work | School | Other | Total |
|--------------|------|--------|-------|-------|
| Free | 1% (5) | 5% (3) | 0% (0) | 1% (8) |
| Hourly | 4% (14) | 4% (2) | 15% (21) | 6% (37) |
| Daily | 43% (148) | 85% (28) | 81% (116) | 56% (292) |
| Monthly | 52% (179) | 6% (2) | 5% (6) | 37% (187) |
| **Total** | **66%** | **7%** | **27%** | **100%** |

*Activity Duration by Payment Term:*

| Duration (hours) | Free | Hourly | Daily | Monthly | Total |
|------------------|------|--------|-------|---------|-------|
| 1-2 | 0% | 52% | 8% | 1% | 8% |
| 3-5 | 0% | 25% | 22% | 0% | 14% |
| 6-7 | 0% | 5% | 9% | 5% | 8% |
| 8-10 (peak work) | 56% | 18% | 50% | 79% | 60% |
| 11+ | 44% | 0% | 11% | 15% | 10% |

**Key Behavioral Findings:**
- **Monthly rates used almost exclusively for work trips** (8-11 hour duration)
- **Daily rates peak at 9 hours** but also used for shorter trips when daily ≈ 2× hourly
- **Hourly rates dominate for <5 hour activities** (shopping, appointments)
- **69% of downtown workers in professional occupations**
- **89% full-time workers, 11% part-time workers**

*Employer Reimbursement by Income (work trips):*

| Reimbursement Status | $0-30k | $60-100k | $100-150k | $150k+ | Overall |
|----------------------|--------|----------|-----------|--------|----------|
| Free on-site | 4% | 10% | 9% | 20% | 12% |
| Pay, full reimb. | 26% | 36% | 37% | 22% | 35% |
| Pay, partial reimb. | 9% | 12% | 12% | 12% | 11% |
| Pay, no reimb. | 61% | 40% | 42% | 46% | 41% |

**Survey Bias and Enrichment:**

The parking behavior survey had a critical bias: it **only sampled people who drove and parked** in surveyed lots. This excluded:
- Transit riders to downtown (who may have chosen transit to avoid parking costs)
- People parking at non-commercial locations
- Walk/bike commuters

To address this, the sample was **enriched with transit riders from the transit on-board survey** who worked downtown. These riders were treated as **choice-based sample observations** and assigned to "pay, no reimbursement" category (assumed no parking benefits). This enrichment was essential for unbiased parameter estimation.

### Parking Inventory (2009-2010)

Comprehensive tabulation of parking supply in downtown San Diego (parkarea == 1).

**Commercial Lots and Garages** (most detailed):
- **Source:** Direct request from commercial parking operators
- **Data:** Number of stalls by classification, payment terms in effect
- **Coverage:** Nearly every commercial lot/garage in parking-charged areas
- **Detail Level:** Individual facility records with multiple rate options

**Metered Spaces:**
- **Source:** Spatial layer maintained by City of San Diego
- **Validation:** Field visits to other municipalitie areas
- **Data:** Location, hourly rates, time limits

**Free On-Street Parking:**
- **Source:** Estimated using spatial formulas
- **Method:** Street frontage length × driveway density
- **Assumption:** Spaces available where no meters/restrictions exist

**Private Parking Stalls** (most uncertain):
- **Source:** CoStar commercial real estate database
- **Method:** Published parking ratios extrapolated to buildings
- **Data:** Average stalls per square foot by building type
- **Note:** Represents unobservable supply (not publicly available)

**Why Inventory is Critical:**
- **Size variables** in parking location choice model (number of stalls)
- **Weighting** of survey observations (spaces per lot)
- **Availability constraints** (private stalls only at destination MGRA)
- **Supply totals** for validation and capacity restraint

## Parking Cost Data

### Three Rate Types

Parking costs are stored as **three separate rates per MGRA** (microzone):

| Rate Type | Variable Name | Usage | Source |
|-----------|---------------|-------|--------|
| Monthly | `mparkcost[mgra]` | Transit pass comparison, regular commuters | Commercial operator data |
| Daily | `dparkcost[mgra]` | Full-day work tours (most common) | Commercial operator data |
| Hourly | `hparkcost[mgra]` | Part-time tours, trip mode choice | Metered spaces + commercial rates |

**Why three rates?** Survey data (Table 13 above) demonstrates people **rationally select payment terms** based on trip duration to minimize cost:
- 2-hour trip: Pay $10 hourly (2×$5) vs $20 daily → **choose hourly**
- 9-hour trip: Pay $20 daily vs $45 hourly (9×$5) → **choose daily**
- Regular commuter: Pay $350 monthly vs $440 for 22 daily trips → **choose monthly**

This is **not an average** - the model must select the appropriate rate based on tour characteristics.

**Data Sources** (from parking inventory):
- **Commercial lots/garages:** Payment terms from operator records
- **Metered spaces:** City spatial data + hourly rates
- **Free on-street:** Estimated from street frontage formulas
- **Private stalls:** CoStar database parking ratios

### Geographic Scope and Aggregation

**Spatial Definition:**
- **Constrained areas (parkarea == 1):** Downtown San Diego and select high-density zones - all three rates specified
- **Outside constrained areas (parkarea != 1):** Parking assumed **FREE** (cost = $0.00 for all rates)

**Why Free Parking Outside Downtown?**

Survey data and inventory showed:
1. **Abundant free parking** in suburban areas (on-street, surface lots)
2. **Workplace parking provision** nearly universal outside downtown (>95%)
3. **Computational efficiency** - no need to model parking choice where it's not constrained
4. **Policy focus** - downtown parking pricing is the primary policy lever

**How MGRA-Level Costs are Calculated:**

Raw parking inventory data is at **facility level** (individual lots/garages). These must be aggregated to **MGRA level** for model application:

**Method 1: Simple Average** (not used)
```
avgCost[mgra] = sum(cost_facility) / count(facilities)
```
Problem: Gives equal weight to 10-space lot and 1000-space garage

**Method 2: Capacity-Weighted Average** (likely used for base costs)
```
avgCost[mgra] = sum(cost_facility * spaces_facility) / sum(spaces_facility)
```
Weights by supply - larger facilities have more influence

**Method 3: Logsum-Weighted Average** (used for mode choice)
```
lsWgtAvgCost[mgra] = -log(Σ exp(β_cost*cost + β_walk*dist + γ*log(spaces))) / β_cost
```
From parking location choice model - captures **accessibility-weighted** cost considering distance and availability (see Parking Location Choice section above)

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

## Data Flow: From Survey to Model Variables

This section explains how raw data sources flow through processing steps into model variables.

### Parking Cost Variables Flow

```
┌─────────────────────────────────────────────────────────────┐
│ RAW DATA SOURCES                                            │
├─────────────────────────────────────────────────────────────┤
│ • 2009-2010 Parking Inventory                               │
│   - Commercial lot records (operator data)                  │
│   - Metered space spatial data (city GIS)                   │
│   - Free on-street estimates (frontage formulas)            │
│   - Private stall estimates (CoStar ratios)                 │
│                                                              │
│ • 2010-2011 Parking Behavior Survey                         │
│   - 524 observations with payment terms                     │
│   - Activity durations by payment term                      │
│   - Walk distances from parking to destination              │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ PROCESSING STEP 1: Facility-Level Data                      │
├─────────────────────────────────────────────────────────────┤
│ For each parking facility (lot/garage/meter zone):          │
│  • facilityID, MGRA, coordinates                            │
│  • numSpaces (total capacity)                               │
│  • hourlyRate, dailyRate, monthlyRate                       │
│  • spaceType (commercial/metered/on-street/private)         │
│  • restrictions (time limits, private access)               │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ PROCESSING STEP 2: MGRA-Level Aggregation                   │
├─────────────────────────────────────────────────────────────┤
│ Capacity-weighted average by MGRA:                          │
│  • hparkcost[mgra] - hourly rate                            │
│  • dparkcost[mgra] - daily rate                             │
│  • mparkcost[mgra] - monthly rate                           │
│  • totalSpaces[mgra] - sum of all spaces                    │
│  • parkarea[mgra] - 1 if downtown, 0 otherwise              │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ PROCESSING STEP 3: Logsum-Weighted Costs                    │
├─────────────────────────────────────────────────────────────┤
│ Using Parking Location Choice Model coefficients:           │
│  • lsWgtAvgCostM[mgra] - accessibility-weighted monthly      │
│  • lsWgtAvgCostD[mgra] - accessibility-weighted daily        │
│  • lsWgtAvgCostH[mgra] - accessibility-weighted hourly       │
│                                                              │
│ Formula (for each MGRA):                                     │
│   logsumCost = -ln(Σ_lots exp(β*cost + β*walk + γ*ln(spaces))) / β_cost
│                                                              │
│ Captures: distance to parking, cost trade-offs, availability│
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ MODEL INPUT VARIABLES (Java Arrays)                         │
├─────────────────────────────────────────────────────────────┤
│ MgraDataManager.java stores:                                │
│  • mparkcost[mgra] - base monthly rate                      │
│  • dparkcost[mgra] - base daily rate                        │
│  • hparkcost[mgra] - base hourly rate                       │
│  • lsWgtAvgCostM[mgra] - logsum monthly (mode choice)       │
│  • lsWgtAvgCostD[mgra] - logsum daily (mode choice)         │
│  • lsWgtAvgCostH[mgra] - logsum hourly (mode choice)        │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ MODE CHOICE MODEL USAGE                                      │
├─────────────────────────────────────────────────────────────┤
│ UEC Variables (TourModeChoice.xls):                         │
│  • @monthlyParkingCost = lsWgtAvgCostM[tourDestMgra]        │
│  • @dailyParkingCost   = lsWgtAvgCostD[tourDestMgra]        │
│  • @hourlyParkingCost  = lsWgtAvgCostH[tourDestMgra]        │
│                                                              │
│ Rate Selection Logic:                                        │
│  • Full-time work (8-11hr) → @dailyParkingCost              │
│  • Part-time work (<8hr)   → @hourlyParkingCost             │
│  • Regular commuter        → @monthlyParkingCost/22         │
│  • Trip mode choice        → @hourlyParkingCostTripDest     │
│                                                              │
│ Applied to drive mode utilities:                            │
│   effectiveCost = cost × (1 - freeOnsite) × (1 - reimbursePct)
└─────────────────────────────────────────────────────────────┘
```

### Parking Provision Variables Flow

```
┌─────────────────────────────────────────────────────────────┐
│ RAW DATA SOURCES                                            │
├─────────────────────────────────────────────────────────────┤
│ • 2010-2011 Parking Behavior Survey                         │
│   - 346 work trips with reimbursement data                  │
│   - Income, occupation, employer reimbursement %            │
│   - Workplace MGRA, home MGRA                               │
│                                                              │
│ • Transit On-Board Survey (enrichment)                      │
│   - Transit riders to downtown                              │
│   - Assumed: no parking benefits                            │
│   - Used to correct for survey bias (sampled only drivers)  │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ PROCESSING: Create Estimation Dataset                       │
├─────────────────────────────────────────────────────────────┤
│ Combine and code:                                           │
│  • Parking survey: observed choice = {Free, Reimb, Pay}     │
│  • Transit survey: fixed choice = Pay (no benefits)         │
│  • Merge with MGRA attributes:                              │
│    - Employment by industry                                 │
│    - Average parking costs in nearby MGRAs                  │
│    - Transit accessibility                                  │
│  • Code person attributes:                                  │
│    - Income categories                                      │
│    - Occupation type                                        │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ MODEL ESTIMATION                                             │
├─────────────────────────────────────────────────────────────┤
│ Multinomial Logit with 3 Alternatives:                      │
│  1. Free on-site parking                                    │
│  2. Reimbursement (partial or full)                         │
│  3. Pay (no provision)                                      │
│                                                              │
│ Estimated Coefficients (β):                                 │
│  • Income effects (high earners → more benefits)            │
│  • Industry effects (education/health → more reimb.)        │
│  • Area cost effects (high cost → more reimb.)              │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ MODEL APPLICATION (ParkingProvisionModel.java)               │
├─────────────────────────────────────────────────────────────┤
│ For each worker with workplace in parkarea==1:              │
│                                                              │
│  Input Variables (from person/household data):              │
│   • person.income                                           │
│   • person.occupation                                       │
│   • mgra.employmentByIndustry                               │
│   • mgra.logsumWeightedAvgMonthlyCost                       │
│                                                              │
│  Model Predicts Probability:                                │
│   P(Free)   = exp(U_free)   / Σ exp(U_i)                    │
│   P(Reimb)  = exp(U_reimb)  / Σ exp(U_i)                    │
│   P(Pay)    = exp(U_pay)    / Σ exp(U_i)                    │
│                                                              │
│  Monte Carlo Draw → chosen alternative                       │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ STORED PERSON ATTRIBUTE                                      │
├─────────────────────────────────────────────────────────────┤
│ person.setFreeParkingAvailableResult(chosenAlt)             │
│                                                              │
│  Values:                                                     │
│   • FP_MODEL_FREE_ALT = 1  (free on-site)                   │
│   • FP_MODEL_PAY_ALT = 2   (no benefits)                    │
│   • FP_MODEL_REIMB_ALT = 3 (reimbursement)                  │
│   • FP_MODEL_NO_REIMBURSEMENT_CHOICE = -1 (outside downtown)│
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ MODE CHOICE MODEL USAGE                                      │
├─────────────────────────────────────────────────────────────┤
│ TourModeChoiceDMU.java retrieves:                           │
│  • freeParkingResult = person.getFreeParkingAvailableResult()│
│                                                              │
│ UEC Variable Calculations:                                  │
│  • @freeOnsite = (freeParkingResult == 1) ? 1 : 0           │
│  • @reimburseProportion = getReimbursementProportion()      │
│                                                              │
│ Applied to Drive Mode Utilities:                            │
│   effectiveParkingCost = selectedRateCost                   │
│                           × (1 - @freeOnsite)               │
│                           × (1 - @reimburseProportion)      │
│                                                              │
│ Result:                                                      │
│  • Free on-site → effectiveCost = $0                        │
│  • Full reimb. → effectiveCost = $0                         │
│  • 50% reimb. → effectiveCost = cost × 0.5                  │
│  • No benefits → effectiveCost = full cost                  │
└─────────────────────────────────────────────────────────────┘
```

### Why These Variables Were Selected

**Parking Cost Variables (M/D/H rates):**

✓ **Selected:** Three separate rates  
✗ **Not selected:** Single average rate

**Rationale:**
- Survey data (Table 13) shows **distinct behavioral patterns** by activity duration
- Monthly: 8-11 hour peak (work trips)
- Daily: 9-hour peak but broader distribution
- Hourly: <5 hour concentration (shopping, appointments)
- People **rationally minimize cost** by selecting appropriate payment term
- Model must **replicate this selection logic** to forecast correctly

**Logsum-Weighted vs Simple Average:**

✓ **Selected:** Accessibility-weighted cost from parking location choice  
✗ **Not selected:** Simple or capacity-weighted average

**Rationale:**
- Workers don't pay "average" parking cost in an area
- They **optimize trade-off** between parking cost and walk distance
- Close expensive parking vs. far cheap parking
- Logsum captures this **expected minimum cost** given spatial distribution
- Consistent with **discrete choice theory** (logsums = accessibility)

**Provision Model Variables:**

✓ **Selected:** Income, industry, area parking cost  
✗ **Not selected:** Age, gender, commute distance, workplace density

**Rationale:**
- **Limited by survey bias** - many variables unstable in estimation
- **Income effect strong and logical** - high earners get better benefits
- **Industry effect observable** - education/health have structured programs
- **Area cost effect rational** - employers compete for workers in high-cost areas
- Many workplace variables **collinear with downtown location** (all sampled locations dense)
- Person demographics (age/gender) **not significant** after controlling for income/occupation

**Three Alternatives (Free/Reimb/Pay) vs Two:**

✓ **Selected:** 3-alternative MNL (Free on-site, Reimbursement, Pay)  
✗ **Not selected:** Binary (Benefits vs No benefits)

**Rationale:**
- Survey data (Table 15) shows **three distinct patterns** (12% free, 46% reimb., 41% pay)
- Free on-site **behaviorally different** from reimbursement:
  - Free on-site: Must park at workplace (no choice)
  - Reimbursement: Can choose closer parking, accept partial reimbursement
- Critical for **parking location choice model** - reimbursed workers still make location trade-offs
- Binary model would **mis-specify mode choice** (free ≠ reimbursement in utility)

## Parking Provision Model

### Purpose

Determines which workers have employer-provided parking benefits. Runs BEFORE mode choice as a pre-processor.

**Model Type:** Multinomial Logit with 3 alternatives

**See detailed sections above for:**
- [Data Collection](#data-collection-and-sources) - 2010-2011 Parking Behavior Survey methodology
- [Model Estimation](#model-estimation) - Parameter estimates and survey bias correction  
- [Why These Variables](#why-these-variables-were-selected) - Rationale for variable selection

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

### Model Estimation

**Estimation Method:** Multinomial Logit  
**Sample Size:** 346 work trips (parking survey) + transit riders (on-board survey)  
**Estimation Technique:** Choice-based sample with bias correction

**Why Enrichment Was Necessary:**

The parking behavior survey was inherently biased - it only captured people who **drove and parked**. Estimating a provision model from this sample alone would incorrectly predict that everyone receives parking benefits (because you can't observe people who avoided driving due to parking costs).

Solution: Add **transit riders to downtown** from on-board survey, treating them as workers who "pay, no reimbursement". This creates a choice-based sample where the model can learn what differentiates workers with benefits from those without.

**Estimated Parameters:**

| Variable | Alternative | Coefficient | t-value | Interpretation |
|----------|-------------|-------------|---------|----------------|
| Income > $100k | Free on-site | 1.870 | 4.01 | High-income workers more likely to have free parking |
| Income > $100k | Reimbursement | 0.612 | 3.21 | High-income workers also more likely to be reimbursed |
| Income $60k-$100k | Free on-site | 0.858 | 1.65 | Middle-income effect smaller but still positive |
| Day equiv. of logsum-wtd. avg. monthly cost | Reimbursement | 0.368 | 3.23 | Higher area parking costs → more likely employer reimburses |
| % blue collar employment | Reimbursement | -1.840 | -2.04 | Blue collar jobs less likely to offer reimbursement |
| % education/health employment | Reimbursement | 2.260 | 4.20 | Education/health sectors more likely to reimburse |
| Constant | Free on-site | -5.150 | -5.43 | Base probability (low without other factors) |
| Constant | Reimbursement | -4.370 | -10.63 | Base probability (low without other factors) |

**Note on Limited Variables:**

The parking behavior survey bias prevented estimation of most variables initially planned. Many workplace and person characteristics showed weak or unstable parameters because the sample didn't have enough variation in outcomes (too few non-drivers). The transit on-board enrichment helped but still limited the final model to the most robust predictors.

### Model Variables

**Variables Tested But Not Estimated** (due to survey bias):

Many theoretically relevant variables could not be reliably estimated:

**Transportation System:**
- ~~Average monthly parking costs in nearby MGRAs~~ → partially captured in logsum-weighted cost
- ~~Transit accessibility to workplace~~ → variation insufficient in sample
- ~~Walk distance to rail station~~ → transit riders added as fixed choice
- ~~Workplace location choice shadow price~~ → not significant

**Workplace Characteristics:**
- ~~Parking stall density at workplace~~ → correlated with downtown location
- ~~Employment density~~ → all sampled locations in dense downtown
- Employment by industry → **ESTIMATED** (blue collar vs education/health)
- ~~College enrollment, office tower presence, zoning~~ → insufficient variation

**Person Characteristics:**
- Income → **ESTIMATED** (>$100k, $60k-$100k)
- ~~Occupation~~ → only professional workers well-represented (69%)
- ~~Commute distance~~ → not significant
- ~~Full/part-time worker/student status~~ → almost all full-time (89%)
- ~~Age, gender~~ → not significant

**Variables That DID Work:**

1. **Household Income** - Strong predictor. Higher income correlates with:
   - Jobs with parking benefits
   - Willingness to pay for parking location choice flexibility
   - Professional occupations with benefits

2. **Logsum-Weighted Average Monthly Cost** - Employers in high-cost areas more likely to **offer reimbursement** to remain competitive for workers. This variable captures:
   - Market pressure to compensate for parking costs
   - Geographic variation in provision policies
   - Connection to parking location choice model

3. **Industry Composition** - Why industry matters:
   - **Blue collar** (-): Construction, manufacturing jobs less likely to reimburse (workers need vehicles for job duties, park on-site)
   - **Education/health** (+): Hospitals, universities offer parking benefits as employee perk, often have structured parking programs

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

For downtown San Diego (parkarea == 1), a **Parking Location Choice Model** determines WHERE vehicles are parked at specific lots/garages.

**Purpose:** 
1. Assigns parking to specific lots for detailed downtown traffic assignment
2. **Calculates logsum-weighted average parking costs** used in mode choice

**Estimation Dataset:** Same 524 observations from 2010-2011 Parking Behavior Survey

**Estimation Method:** Multinomial Logit with Size Variables  
**Estimator:** Weighted Exogenous Sample Maximum Likelihood (WESMLE)  
**Weights:** `(spaces in chosen lot) / (samples from that lot)`

**Why Weights?** The survey over-sampled large garages (easier to survey). Weights correct for this by down-weighting popular survey locations and up-weighting under-sampled lots.

### Model Specification

Utility function:
```
u_ni = β_i * X_ni + γ * log(S_ni) + ε_ni
```

Where:
- `X_ni` = attributes (walk distance, cost, etc.)
- `S_ni` = **size variable** = number of parking stalls in MGRA i
- `γ` = scale parameter (fixed to 1.0 due to choice-based sample)
- `ε_ni` = extreme value type I errors

**Size Variable Rules (Application):**
- **Private stalls:** Only available in destination MGRA (can't park 3 blocks away in office building garage)
- **On-street stalls:** Only available for activities ≤ 3 hours (time limits)
- **Commercial lots:** Available within 1-mile radius of destination

**Estimation Challenges:**

The choice-based sample created severe problems:
1. **Alternatives limited to sampled lots** - Can't include non-sampled locations because probability of observing them was zero
2. **Few alternative lots in choice set** - Often only 1-2 other sampled lots near destination
3. **Little revealed preference information** - Hard to estimate trade-offs when alternatives are far away
4. **Destination imputed from cross-streets** - Introduces measurement error
5. **Aggregate cost variable** - Can't observe variation in detailed pricing

**Solution: Simple Model with Constrained VOT**

Due to estimation difficulties, a **very simple model** was estimated with **value of time constrained to $15/hour** (walk speed = 3 mph).

**Estimated Parameters:**

| Variable | Work | Other | Implied VOT |
|----------|------|-------|--------------|
| Walk distance (miles) | -8.59 | -4.93 | — |
| Cost (dollars) | -0.72 | -0.41 | — |
| **Walk time coefficient / Cost coefficient** | **8.59 / 0.72 = 11.9 ≈ $15/hr** | **4.93 / 0.41 = 12.0 ≈ $15/hr** | **$15/hour** |

**Interpretation:**
- Workers willing to walk 0.72/8.59 = **0.084 miles** (440 feet) to save $1
- At 3 mph walk speed, this is 1.7 minutes per dollar
- Equivalent to valuing walking time at $15/hour (or $0.25/minute)
- "Other" trips half as sensitive to walk distance (more willing to walk for shopping/errands)

**How Logsum-Weighted Cost is Calculated:**

The coefficients from this model generate **accessibility-weighted average costs** for each destination MGRA:

```
lsWgtAvgCostM[destMGRA] = -log(Σ_lots exp(β_cost*monthlyCost + β_walk*walkDist + γ*log(stalls))) / β_cost
```

This is the **expected minimum cost** considering:
- Trade-offs between cost and walk distance
- Availability of parking (number of stalls)
- Distribution of costs in nearby lots

These weighted costs are used in **tour mode choice** (see `lsWgtAvgCostM/D/H` arrays in implementation).

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

### Data Input Summary

Quick reference for understanding what data goes where:

| Data Source | Raw Data | Processed Into | Used By |
|-------------|----------|----------------|---------|
| **Parking Inventory** | Facility-level rates ($/hr, $/day, $/month) | `hparkcost[mgra]`, `dparkcost[mgra]`, `mparkcost[mgra]` | Mode choice rate selection |
| **Parking Inventory** | Number of stalls per facility | `totalSpaces[mgra]`, size variables | Parking location choice |
| **Parking Location Choice Model** | Estimated coefficients | `lsWgtAvgCostM/D/H[mgra]` | Mode choice utilities |
| **Parking Behavior Survey** | 346 work trips with reimbursement | Provision model coefficients | Parking provision model |
| **Transit On-Board Survey** | Transit riders to downtown | Choice-based sample enrichment | Provision model estimation |
| **Person Attributes** | Income, occupation | Input variables | Provision model application |
| **MGRA Attributes** | Employment by industry | Input variables | Provision model application |
| **Provision Model Result** | `freeParkingAvailable` | Person attribute | Mode choice cost adjustment |

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

### Model Foundation

Parking costs in Travel Model Two are based on extensive data collection:
- **2010-2011 Parking Behavior Survey** - 1,563 persons at 48 locations capturing payment behavior, employer reimbursement, and location choices
- **2009-2010 Parking Inventory** - Comprehensive tabulation of parking supply and pricing from commercial operators, city records, and spatial analysis
- **Transit On-Board Survey enrichment** - Bias correction by including non-drivers

### Key Design Decisions

**Three Separate Rates (Not a Single Average):**
- Survey evidence shows rational payment term selection by duration
- Monthly for 8-11 hour work trips (52% of work parkers)
- Daily for full-day activities (43% of work parkers, 9-hour peak)  
- Hourly for short activities (dominated by <5 hour trips)

**Logsum-Weighted Costs (Not Simple Averages):**
- Captures accessibility-weighted expected minimum cost
- Accounts for walk distance-cost trade-offs
- Derived from parking location choice model estimation
- Consistent with discrete choice theory

**Three Provision Alternatives (Not Binary):**
- Free on-site (12%) behaviorally distinct from reimbursement (46%)
- Free = no location choice; Reimbursement = flexible location choice
- Critical for parking location model application

### System Integration

Parking costs in the activity-based model:
- Are represented by **three separate rates** (M/D/H) per microzone based on inventory data
- Are **conditionally selected** based on tour characteristics (duration, purpose)
- **Only affect** drive modes in parkarea==1 (downtown San Diego)
- Flow through **nested logsums** to destination choice as accessibility measures
- Are **zeroed out** for workers with free parking provision (12% of downtown workers)
- Are **partially borne** by workers with reimbursement (46% of downtown workers)
- Impact concentrated on **downtown workers without parking benefits** (41% pay full cost)

### Policy Relevance

Understanding this system is critical for analyzing:
- Parking pricing policies (changes to hourly/daily/monthly rates)
- Employer parking cash-out programs (reimbursement alternative)
- Downtown development with parking constraints
- Transit investments competing with parking costs
- Work-from-home vs in-office trade-offs

The detailed data collection and estimation methodology documented here ensures the model responds realistically to these policy scenarios.
