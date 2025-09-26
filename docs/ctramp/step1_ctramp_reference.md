
# CT-RAMP Model Documentation Reference
**Generated:** 2025-09-25 16:40:42
**Source:** CT-RAMP_family_of_Activity-Based_Models.pdf
**Purpose:** Reference document for TM2 CT-RAMP implementation analysis

## Executive Summary

CT-RAMP (Coordinated Travel - Regional Activity-based Modeling Platform) is a comprehensive 
activity-based travel demand modeling system. This document provides extracted key concepts 
and structure for analyzing the Travel Model Two implementation.

## Document Structure Analysis

### Key Sections Identified:


### Introduction
**Lines extracted:** 26
**Key points:**
- See discussions, st ats, and author pr ofiles f or this public ation at : https://www .researchgate.ne t/public ation/267862957
- CT-RAMP family of Activity-Based Models
- Article  · Januar y 2010
- 6 author s, including:
- Resour ce Syst ems Gr oup
- ... (21 more lines)

### Population Synthesis
**Lines extracted:** 58
**Key points:**
- 1. Population synthesis .  Creates a list of hous eholds, with all household and person
- attributes based on the input (controlled) variables defined for each traffic zone .  The
- procedure create s a household distribution in each zone that matches controlled
- variables and generate s a list of discrete households with additional (uncontrolled)
- variables by drawing them from the microsample provided by the Population Census.
- ... (53 more lines)

### Tour Models
**Lines extracted:** 65
**Key points:**
- 4.2.1.  Frequency of mandatory tours .
- 4.3.1.  Joint tour frequency .
- 4.3.2.  Travel  party composition (adults, children, or mixed) .
- 4.3.3.  Person participation in each joint tour .
- 4.3.4.  Primary destination for each joint tour .
- ... (60 more lines)

### Destination Choice
**Lines extracted:** 83
**Key points:**
- 7 by various accessibility measures that are sensitive to travel time and land -use densities.
- The entire model system is integr ated with highway and transit network simulation
- procedures and applied iteratively with special provisions for reaching global demand -supply
- equilibrium [ Vovsha Donnelly & Gupta, 2008 ].
- 4. Current Members of the CT -RAMP Family
- ... (78 more lines)

### Mode Choice
**Lines extracted:** 162
**Key points:**
- 6.1. Trip mode choice conditional upon the tour mode  combination.
- 6.2. Auto trip parking location choice .
- 6.3. Trip assignment s for auto and transit trips (route choice in the network
- equilibrium framework).
- ATRF 2010 Proceedings
- ... (157 more lines)

### Time Of Day
**Lines extracted:** 44
**Key points:**
- dimensions such as destination, mode, and time of day.  Further, the whole spectrum of
- travel dimensions (mode, destination, and time of day) related t o non -home -based travel
- can be properly linked to home -based travel  [Vovsha & Freedman, 2008 ].
-  An activity -based platform  that implies that modeled travel is derived within the general
- framework of the daily activities undertaken by households and persons.  This allows for
- ... (39 more lines)

### Stop Generation
**Lines extracted:** 15
**Key points:**
- number of intermediate stops on each half -tour (5.2), stop location (5.3), and stop departure
- time (5.4 ).  It is followed by the last set of sub -models that add details for each trip including
- trip mode details (6.1) and parking location for auto trips (6.2).  The parking location does not
- necessarily coincide with the trip destination.  If parking capacity  is constrained and/or
- parking cost is high, drivers may choose to park remotely and then walk to the destination.
- ... (10 more lines)

### Joint Travel
**Lines extracted:** 31
**Key points:**
- mandatory  activities  and person participation in joint travel .  Joint travel tours
- are generated  conditional upon the available time window left for each person
- after the scheduling of mandatory activities .  The following sequence of sub -
- the previous CT -RAMP ABMs, joint travel was generated after the DAP type and
- work/school tour schedules were defined for ea ch person.  Person availability to
- ... (26 more lines)

### Auto Ownership
**Lines extracted:** 50
**Key points:**
- or expansion High-Occupancy Vehicle  (HOV) and H igh-Occupancy Toll (HOT) lane facilities
- as well as other projects and policies that specifica lly target vehicle occupancy .
- The general design of the CT -RAMP model system is presented in Figure 1  below.  The
- following main sub-models and associated travel choices are included in the CT -RAMP
- While transit time components like in -vehicle time, wai t time, and walk time have long
- ... (45 more lines)

### Validation
**Lines extracted:** 5
**Key points:**
- calibration and will be applied in 3rd quater  2010.  The Atlanta and Bay Area Models have
- included the following refinements relative to the Columbus/Lake Tahoe models:
-  Consideration of long -term choices  (i.e. usual workplace, usual school location) prior to
- the car ownership model and subsequent chain of travel choices.  In the Columbus/Lake
- Tahoe model, these choices were not modeled explicitly.  They were “blended” with the
- ... (0 more lines)

### Implementation
**Lines extracted:** 212
**Key points:**
- This paper describes the structure, implementation, and application experience of seven
- different regional Activity -Based Models (ABMs) that share the Coordinated Travel -
- Regional Activity Modeling Platform (CT -RAMP) design and software platform.  The CT -
- RAMP models are characterized by a number of features, including a full simulation of travel
- decisions for discrete households and persons; explicit tracking of time in half -hourly
- ... (207 more lines)

## Key Concepts Extracted

### Models
- The choice model
- destination choice model
- location choice model
- mode choice model
- parking choice model

### Algorithms
- logsum
- logsums
- synthesis algorithm

### Data Structures
- Activity
- Household
- Person
- Tour
- Trip
- activity
- household
- person
- tour
- trip
- ... (1 more items)

### Outputs
- Assignment
- assignment


## Implementation Analysis Framework

### Model Component Hierarchy
1. **Population Synthesis** - Synthetic households and persons
2. **Long-term Choices** - Auto ownership, usual workplace/school location  
3. **Daily Activity Pattern** - Activity pattern type for each person
4. **Tour Formation** - Mandatory and non-mandatory tour generation
5. **Tour-level Models** - Destination, mode, and time-of-day choice
6. **Trip-level Models** - Stop generation, location, mode, and time choice
7. **Assignment** - Trip assignment to transportation network

### Key Data Flows
- **Input:** Zone data, network data, land use data
- **Synthetic Population:** Households, persons, tours, trips
- **Choices:** Destinations, modes, departure times, stops
- **Output:** Trip tables by mode and time period

### Validation Framework
- **Observed Data:** Survey data, counts, census data
- **Model Outputs:** Synthetic travel patterns  
- **Validation Metrics:** Trip rates, mode shares, distance distributions
- **Calibration:** Adjustment of model parameters

## Next Steps for TM2 Analysis

1. **Map Excel UECs** to model components identified above
2. **Trace Java implementation** of each model component
3. **Document parameter flows** from Excel to Java execution
4. **Validate model structure** against CT-RAMP specification
5. **Identify TM2-specific customizations** and extensions

---
*This reference document will be updated as additional analysis is completed.*
