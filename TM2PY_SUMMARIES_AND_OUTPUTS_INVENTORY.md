# TM2PY Summaries and Outputs Inventory

**Comprehensive list of all summaries, reports, and outputs produced by Travel Model Two (TM2PY)**

Last Updated: January 5, 2026

---

## 📁 Directory Structure Overview

```
model_run_directory/
├── output_summaries/           # Network and model summaries
├── skims/                      # Level-of-service matrices
├── skim_matrices/              # Organized skim outputs
│   ├── highway/
│   ├── transit/
│   └── non_motorized/
├── ctramp_output/             # Activity-based model outputs
├── demand_matrices/           # Trip demand matrices
│   ├── highway/
│   │   ├── household/
│   │   ├── air_passenger/
│   │   ├── internal_external/
│   │   ├── commercial/
│   │   └── maz_demand/
│   └── transit/
├── hwy/                       # Highway assignment results
├── trn/                       # Transit assignment results
├── updated_output/            # Post-processed outputs
├── logs/                      # Model run logs
└── emme_project/             # EMME network databases
    ├── Database_highway/
    └── Database_transit/
```

---

## 📊 NETWORK SUMMARY OUTPUTS

**Primary Directory**: `output_summaries/`

### Excel Workbook (All-in-One Report)
- **network_summary_report.xlsx** - Main comprehensive summary workbook
  - Multiple sheets with highway and transit performance
  - VMT/VHT/Delay by facility type
  - County-level summaries
  - Lane mile inventory
  - Transit operator summaries

### Highway Network Performance

#### By Facility Type
- **car_vmt_by_facility.csv** - Car VMT by facility type and time period
- **truck_vmt_by_facility.csv** - Truck VMT by facility type and time period
- **vmt_by_facility.csv** - Total VMT by facility type and time period
- **vht_by_facility.csv** - Vehicle hours traveled by facility type
- **total_delay_by_facility.csv** - Delay hours by facility type

#### By Time Period
- **summary_by_time_period.csv** - Aggregate metrics for each time period
  - Car VMT, Truck VMT, Total VMT
  - VHT and average speeds
  - Total delay hours

#### By Geography
- **county_summary.csv** - Performance metrics by county
  - VMT, VHT, delay by county
  - Average network speeds
- **lane_mile_inventory.csv** - Lane miles by county and facility type
  - Includes daily VMT context

#### Overall System
- **overall_summary.csv** - Top-level system performance
- **facility_type_summary.csv** - Comprehensive facility analysis

### Transit Network Performance

#### Boardings Analysis
- **transit_boardings_by_line_ea.csv** - Early AM boardings by line
- **transit_boardings_by_line_am.csv** - AM peak boardings by line
- **transit_boardings_by_line_md.csv** - Midday boardings by line
- **transit_boardings_by_line_pm.csv** - PM peak boardings by line
- **transit_boardings_by_line_ev.csv** - Evening boardings by line
- **transit_boardings_by_line_daily.csv** - All-day totals by line

#### Operator Summaries
- **transit_boardings_by_operator.csv** - Ridership by transit operator
  - Modeled boardings by operator and mode type
  - Line counts per operator

#### Service Type Analysis
- **transit_boardings_by_service_type.csv** - Performance by mode
  - Boardings by mode and time period
  - Number of lines and segments
  - Average frequencies and headways
  - Route miles and boardings per mile

### Validation & Quality Assurance
- **topsheet.csv** - Key regional performance metrics (when available)
- **trimmed_demand_report_[period].csv** - Congested transit demand adjustments

---

## 🗺️ SKIM OUTPUTS (Level-of-Service Matrices)

**Primary Directory**: `skims/` and `skim_matrices/`

### Highway Skims (TAZ Level)

#### Main Highway Skim Matrices (OMX)
- **HWYSKMEA_taz.omx** - Early AM highway skims
- **HWYSKAM_taz.omx** - AM peak highway skims
- **HWYSKMMD_taz.omx** - Midday highway skims
- **HWYSKMPM_taz.omx** - PM peak highway skims
- **HWYSKMEV_taz.omx** - Evening highway skims

**Contents** (for each time period):
- Travel time by vehicle class (DA, S2, S3, DA_TOLL, S2_TOLL, S3_TOLL, TRK, etc.)
- Distance matrices
- Bridge tolls by toll group
- Value tolls (non-bridge)
- HOV facility distances
- Toll facility distances
- Free-flow times
- Reliability measures
- Operating costs

### Highway Skims (MAZ Level)

- **HWYSKIM_MAZ_MAZ_DA.csv** - MAZ-to-MAZ drive alone costs
  - Columns: FROM_ZONE, TO_ZONE, COST, DISTANCE, BRIDGETOLL

### Drive Access Skims
- **drive_access_skims.csv** - Drive access to transit
  - Columns: FTAZ, MODE, PERIOD, TTAP, TMAZ, TTAZ, DTIME, DDIST, DTOLL, WDIST

### Transit Skims (OMX by Access Mode)

#### Walk-Transit-Walk
- **trnsk<EA|AM|MD|PM|EV>_WLK_TRN_WLK.omx**

#### Park & Ride Transit
- **trnsk<EA|AM|MD|PM|EV>_PNR_TRN_WLK.omx** - Park and walk from transit
- **trnsk<EA|AM|MD|PM|EV>_WLK_TRN_PNR.omx** - Walk to transit and drive from

#### Kiss & Ride Transit
- **trnsk<EA|AM|MD|PM|EV>_KNR_TRN_WLK.omx** - Drop-off and walk from transit
- **trnsk<EA|AM|MD|PM|EV>_WLK_TRN_KNR.omx** - Walk to transit and pickup

**Transit Skim Components** (each file):
- IWAIT - Initial wait time
- XWAIT - Transfer wait time
- WAIT - Total wait time
- FARE - Transit fare
- BOARDS - Number of boardings
- IVT - Total in-vehicle time
- IVTCOM, IVTEXP, IVTFRY, IVTHVY, IVTLOC, IVTLTR - Mode-specific IVT
- WACC - Walk access time
- WEGR - Walk egress time
- WAUX - Walk auxiliary time
- DTIME - Drive access time
- DDIST - Drive access distance
- DTOLL - Drive toll
- XBOATIME - Transfer boarding penalty
- CROWD - Crowding penalty (if enabled)
- IN_VEHICLE_COST - In-vehicle cost

### Active Mode Skims

#### Pedestrian Skims
- **ped_distance_maz_maz.txt** - MAZ-to-MAZ walking distances
- **ped_distance_maz_tap.txt** - MAZ-to-TAP walking distances
- **ped_distance_tap_tap.txt** - TAP-to-TAP walking distances

#### Bicycle Skims
- **bike_distance_maz_maz.txt** - MAZ-to-MAZ cycling distances
- **bike_distance_maz_tap.txt** - MAZ-to-TAP cycling distances
- **bike_distance_taz_taz.txt** - TAZ-to-TAZ cycling distances

**Format**: CSV with columns: from_zone, to_zone, to_zone (duplicate), dist, dist_feet

---

## 🏠 CTRAMP OUTPUTS (Activity-Based Model)

**Primary Directory**: `ctramp_output/`

### Core Household & Person Files

- **householdData_[iter].csv** - Household demographics and attributes
- **personData_[iter].csv** - Individual person characteristics
- **wsLocResults_[iter].csv** - Work and school location choice results
- **aoResults.csv** - Auto ownership model results
- **accessibilities.csv** - Accessibility measures by zone

### Tour Files

#### Individual Tours
- **indivTourData_[iter].csv** - Individual person tours (57 fields)
  - Tour purpose, mode, time of day
  - Origin/destination zones
  - Tour-level accessibility
  - Mode choice logsum
  - Stop frequencies

#### Joint Household Tours
- **jointTourData_[iter].csv** - Joint household tours (51 fields)
  - Joint tour composition
  - Participating household members
  - Joint tour purposes and modes

### Trip Files

#### Individual Trips
- **indivTripData_[iter].csv** - Individual trip segments (19 fields)
  - Trip origin/destination MAZs and TAZs
  - Trip mode and purpose
  - Departure and arrival times
  - Trip distance and time

#### Joint Household Trips
- **jointTripData_[iter].csv** - Joint household trips (18 fields)
  - Joint trip segments
  - Participant information

### Transit Resimulation (Congested Assignment)
- **indivTripDataResim_[iter]_[inner_iter].csv** - Resimulated transit trips
  - Updated after capacity-constrained transit assignment

### Parking Demand
- **unconstrainedPNRDemand_[iter]0.csv** - Unconstrained park-and-ride demand
- **constrainedPNRDemand_[iter]1.csv** - Constrained park-and-ride demand

### Shadow Pricing
- **ShadowPricingOutput_work_0.csv** - Work location shadow prices
- **ShadowPricingOutput_school_0.csv** - School location shadow prices

### Updated Post-Processed Outputs (Parquet Format)
**Directory**: `updated_output/`
- **indivTripData_3.parquet**
- **jointTripData_3.parquet**
- **indivTourData_3.parquet**
- **jointTourData_3.parquet**

---

## 🚗 HIGHWAY ASSIGNMENT OUTPUTS

**Primary Directory**: `hwy/`

### Network Files

#### By Time Period
- **maz_preload_EA.net** - MAZ-level preloaded network (Early AM)
- **maz_preload_AM.net** - MAZ-level preloaded network (AM peak)
- **maz_preload_MD.net** - MAZ-level preloaded network (Midday)
- **maz_preload_PM.net** - MAZ-level preloaded network (PM peak)
- **maz_preload_EV.net** - MAZ-level preloaded network (Evening)

- **loadEA.net** - Final loaded network (Early AM)
- **loadAM.net** - Final loaded network (AM peak)
- **loadMD.net** - Final loaded network (Midday)
- **loadPM.net** - Final loaded network (PM peak)
- **loadEV.net** - Final loaded network (Evening)

#### Averaged Networks
- **avgloadEA.net** - Multi-iteration averaged (Early AM)
- **avgloadAM.net** - Multi-iteration averaged (AM peak)
- **avgloadMD.net** - Multi-iteration averaged (Midday)
- **avgloadPM.net** - Multi-iteration averaged (PM peak)
- **avgloadEV.net** - Multi-iteration averaged (Evening)

#### Iteration Networks
- **iter[X]loadEA.net** - Assignment iteration networks

### Key Link Attributes in Networks
- `@auto_volume` - Total auto volume
- `@auto_time` - Congested travel time
- `@auto_cost` - Generalized cost
- `@v_over_c` - Volume/capacity ratio
- `@cspd` - Congested speed
- `@delay` - Delay vs free-flow
- `@vol_da` - Drive alone volume
- `@vol_s2` - Shared-2 volume
- `@vol_s3` - Shared-3+ volume
- `@vol_sm` - Small truck volume
- `@vol_hv` - Heavy vehicle volume
- `@vol_tot` - Total volume

### Summary Files
- **assign_summary_EA.txt** - Assignment convergence stats (Early AM)
- **assign_summary_AM.txt** - Assignment convergence stats (AM peak)
- **assign_summary_MD.txt** - Assignment convergence stats (Midday)
- **assign_summary_PM.txt** - Assignment convergence stats (PM peak)
- **assign_summary_EV.txt** - Assignment convergence stats (Evening)

### Select Link Analysis
- **selectlink_[analysis_name].txt** - Traffic using specific facilities

### Shapefiles (from Post Processor)
**Directory**: `output_summaries/Scenario_[id]/`
- **links.shp** - Highway link geometries with volumes
- **nodes.shp** - Highway node locations
- **turns.shp** - Turn movement data

---

## 🚋 TRANSIT ASSIGNMENT OUTPUTS

**Primary Directory**: `trn/`

### Boardings Files

#### By Line
- **boardings_by_line_EA.txt** - Early AM boardings by line
- **boardings_by_line_AM.txt** - AM peak boardings by line
- **boardings_by_line_MD.txt** - Midday boardings by line
- **boardings_by_line_PM.txt** - PM peak boardings by line
- **boardings_by_line_EV.txt** - Evening boardings by line

**Format**: CSV with columns: line_id, line_name, mode, boardings, passenger_miles

#### By Segment
- **boardings_by_segment_EA.txt** - Early AM segment-level boardings
- **boardings_by_segment_AM.txt** - AM peak segment-level boardings
- **boardings_by_segment_MD.txt** - Midday segment-level boardings
- **boardings_by_segment_PM.txt** - PM peak segment-level boardings
- **boardings_by_segment_EV.txt** - Evening segment-level boardings

**Format**: CSV with columns: line_id, direction, stop_sequence, from_stop, to_stop, boardings, alightings, load

#### Special Formats (from Post Processor)
- **boardings_by_segment_am.csv** - Detailed segment data with network attributes
- **boardings_by_segment_am.geojson** - GeoJSON format for mapping

### Network Files
- **transit_assignment_EA.net** - Assigned network (Early AM)
- **transit_assignment_AM.net** - Assigned network (AM peak)
- **transit_assignment_MD.net** - Assigned network (Midday)
- **transit_assignment_PM.net** - Assigned network (PM peak)
- **transit_assignment_EV.net** - Assigned network (Evening)

### Key Transit Link Attributes
- `@transit_boardings` - Total boardings
- `@transit_volume` - Passenger volume
- `@capacity_utilization` - Load factor
- `@dwell_time` - Station dwell time

### Shapefiles (from Post Processor)
**Directory**: `output_summaries/Scenario_[id]/`
- **transit_lines.shp** - Transit line geometries
- **transit_segments.shp** - Transit segment data with ridership

---

## 📦 DEMAND MATRICES

**Primary Directory**: `demand_matrices/`

### Highway Household Demand
**Directory**: `demand_matrices/highway/household/`

By Purpose & Time Period (OMX format):
- Trip matrices by purpose: SOV, HOV2, HOV3, etc.
- Time periods: EA, AM, MD, PM, EV
- Purposes: work, school, escort, shop, other maintenance, eating out, visiting, discretionary, work-based

### MAZ-Level Demand
**Directory**: `demand_matrices/highway/maz_demand/`
- Local MAZ-to-MAZ trip tables
- Short-distance travel matrices

### Commercial Vehicle Demand
**Directory**: `demand_matrices/highway/commercial/`

- **truck_generation_very_small.csv** - Very small truck trip generation
- **truck_generation_small.csv** - Small truck trip generation
- **truck_generation_medium.csv** - Medium truck trip generation
- **truck_generation_large.csv** - Large truck trip generation

- **truck_od_very_small_[period].omx** - Very small truck O-D (by period)
- **truck_od_small_[period].omx** - Small truck O-D (by period)
- **truck_od_medium_[period].omx** - Medium truck O-D (by period)
- **truck_od_large_[period].omx** - Large truck O-D (by period)

- **friction_factors_[truck_class].csv** - Distance impedance factors
- **k_factors_applied_[truck_class].csv** - Spatial adjustment factors
- **blended_impedance_[truck_class].omx** - Combined time/distance impedance

### Internal-External Demand
**Directory**: `demand_matrices/highway/internal_external/`
- I-E trip matrices by vehicle class
- External station trip tables

### Air Passenger Demand
**Directory**: `demand_matrices/highway/air_passenger/`
- Airport access trips
- Ground transportation to/from airports

### Transit Demand
**Directory**: `demand_matrices/transit/`
- Transit trip tables by access mode and time period
- By class: WLK_TRN_WLK, PNR_TRN_WLK, WLK_TRN_PNR, KNR_TRN_WLK, WLK_TRN_KNR

---

## 📈 COMMERCIAL VEHICLE SUMMARIES

**Directory**: `output_summaries/` or `commercial/`

### Generation Analysis
- **truck_generation_by_employment.csv** - Trip rates by employment sector
  - Retail, food/personal services, health/education, manufacturing, etc.
- **truck_generation_by_households.csv** - Service vehicle trips from households

### Distribution Analysis
- **commercial_vehicle_summary.csv** - Overall commercial vehicle statistics
  - Total trips by class
  - Average trip lengths
  - VMT by truck type

### Temporal Distribution
- **time_of_day_factors.csv** - TOD factors by truck class
- **directional_factors.csv** - AM/PM directional splits

### Special Generators
- **airport_freight_SFO.csv** - San Francisco Airport freight
- **airport_freight_OAK.csv** - Oakland Airport freight
- **airport_freight_SJC.csv** - San Jose Airport freight

---

## 📝 R MARKDOWN SUMMARY SCRIPTS

**Directory**: `scripts/`

### Roadway Summaries
- **make-version-2.1-roadway-summaries.Rmd** - Highway validation summaries
  - Traffic count comparisons
  - Facility type error analysis
  - Outputs detailed validation CSV

### Transit Summaries
- **make-version-2.1-transit-summaries.Rmd** - Transit validation summaries
  - Operator boardings comparison
  - BART, Caltrain, bus ridership
  - Mode-specific analysis

### Journey Level Analysis
- **make-journey-level-summary.Rmd** - Journey-level travel patterns
  - Multi-segment trip analysis
  - Transfer patterns

### Crosswalk Corrections
- **correct-onboard-to-standard-crosswalk.Rmd** - Onboard survey crosswalks

---

## 🔧 UTILITY & REFERENCE FILES

### Zone Crosswalks
- **node_seq_id_xwalk.csv** - Node ID to sequential zone ID mapping
  - TAZSEQ, MAZSEQ, EXTSEQ, TAPSEQ mappings

### Model Configuration
- **scenario.toml** - Scenario-specific configuration
- **model.toml** - Model-wide configuration

### Supporting Data
- **ACS 2013-2017 MAZ Zero-Vehicle Households.csv** - Zero-vehicle household data

---

## 📊 JUPYTER NOTEBOOKS (Analysis Examples)

**Directory**: `notebooks/`

- **Post_processing_v2.ipynb** - Network export and analysis
  - Transit boarding extracts
  - Highway network exports
  - Shapefile generation
  
- **trim-demand-for-congested-transit-assignment.ipynb** - Transit capacity analysis
  - Demand trimming for crowding
  - Capacity constraint outputs

- **journey-levels.twb** - Tableau workbook for journey analysis

---

## 🎯 KEY OUTPUT LOCATIONS SUMMARY

| Output Type | Primary Directory | File Format | Time-Specific |
|-------------|------------------|-------------|---------------|
| Network Summaries | `output_summaries/` | CSV, XLSX | Yes |
| Highway Skims | `skim_matrices/highway/` | OMX, CSV | Yes (5 periods) |
| Transit Skims | `skim_matrices/transit/` | OMX | Yes (5 periods) |
| Active Mode Skims | `skim_matrices/non_motorized/` | TXT/CSV | No |
| CTRAMP Outputs | `ctramp_output/` | CSV, Parquet | By iteration |
| Highway Networks | `hwy/` | .NET, TXT | Yes (5 periods) |
| Transit Networks | `trn/` | .NET, TXT | Yes (5 periods) |
| Demand Matrices | `demand_matrices/` | OMX, CSV | Yes (5 periods) |
| Shapefiles | `output_summaries/Scenario_[id]/` | SHP | Yes (by scenario) |

---

## 🕐 TIME PERIODS

All time-specific outputs use these standard periods:

| Code | Period | Time Range | Duration (hours) |
|------|--------|------------|------------------|
| EA | Early AM | 3:00 - 6:00 AM | 3 |
| AM | AM Peak | 6:00 - 10:00 AM | 4 |
| MD | Midday | 10:00 - 15:00 PM | 5 |
| PM | PM Peak | 15:00 - 19:00 PM | 4 |
| EV | Evening | 19:00 - 3:00 AM | 8 |

---

## 📌 ITERATIONS

Most CTRAMP outputs include iteration numbers:

- **Iteration 0**: Initial setup
- **Iteration 1**: Sample population, preliminary skims
- **Iteration 2**: Full population, updated networks
- **Iteration 3**: Final converged results

---

## 🏢 FACILITY TYPES (Highway Classification)

| Code | Facility Type | Used In Summaries |
|------|---------------|-------------------|
| 1 | Freeway | ✓ |
| 2 | Freeway (Principal Arterial) | ✓ |
| 3 | Arterial (Principal) | ✓ |
| 4 | Arterial (Minor) | ✓ |
| 5 | Collector (Major) | ✓ |
| 6 | Collector (Minor) | ✓ |
| 7 | Local | ✓ |
| 8 | Connector/Ramp | ✓ |
| 99 | Other/Special | ✓ |

---

## 📚 DOCUMENTATION REFERENCES

- **Main Outputs Documentation**: `docs/outputs.md`
- **Network Summary Usage**: `docs/network_summary.md`
- **Network Analysis Reference**: `docs/output/network-analysis.md`
- **CTRAMP Specifications**: `docs/ctramp-outputs/index.md`
- **Skim Details**: `docs/output/skims.md`
- **Assignment Details**: `docs/output/assignment.md`
- **Commercial Vehicle Details**: `docs/output/commercial.md`

---

## 🔍 FILE NAMING CONVENTIONS

### Common Patterns:
- `[period]` = EA, AM, MD, PM, EV
- `[iter]` = 0, 1, 2, 3 (iteration number)
- `[truck_class]` = very_small, small, medium, large
- `[transit_class]` = WLK_TRN_WLK, PNR_TRN_WLK, etc.
- `_taz` = TAZ-level geography
- `_maz` = MAZ-level geography
- `.omx` = OpenMatrix format (binary)
- `.csv` = Comma-separated values
- `.net` = EMME network format

---

## 🔧 TM2PY-UTILS VALIDATION SUMMARIES

**External Repository**: [tm2py-utils](https://github.com/BayAreaMetro/tm2py-utils)  
**Documentation**: [bayareametro.github.io/tm2py-utils](https://bayareametro.github.io/tm2py-utils/)  
**Primary Tool**: `tm2py_utils/summary/validation/summarize_model_run.py`

### Overview

tm2py-utils is a separate repository that provides **30+ configured validation summaries** from CTRAMP model outputs. These summaries are config-driven (defined in YAML) and provide comprehensive validation statistics.

**Key Features**:
- 30 configured summaries covering households, tours, trips, and activity patterns
- Config-driven - Add summaries by editing YAML, no Python coding required
- Automatic validation with built-in quality checks
- Fast - Process full model run in ~10 minutes
- Simple CSV outputs for easy analysis

### Running Summaries

```bash
cd tm2py_utils/summary/validation
python summarize_model_run.py "C:/path/to/ctramp_output"

# Custom output location
python summarize_model_run.py "C:/path/to/ctramp_output" --output "my_results"

# Strict validation mode
python summarize_model_run.py "C:/path/to/ctramp_output" --strict
```

### Output Directory Structure

```
outputs/
├── auto_ownership_regional.csv
├── auto_ownership_by_income.csv
├── auto_ownership_by_hhsize.csv
├── person_type_distribution.csv
├── age_distribution.csv
├── cdap_patterns_by_person_type.csv
├── cdap_patterns_regional.csv
├── tour_frequency_by_purpose.csv
├── tour_mode_choice.csv
├── tour_mode_choice_by_purpose.csv
├── tour_distance_distribution.csv
├── tour_tod_patterns.csv
├── trip_mode_choice.csv
├── trip_mode_choice_by_purpose.csv
├── trip_purpose_distribution.csv
├── trip_distance_distribution.csv
├── trip_duration_distribution.csv
├── work_location_commute_distance.csv
├── journey_to_work_patterns.csv
└── [additional custom summaries]
```

### Household Summaries

#### Auto Ownership
- **auto_ownership_regional.csv** - Regional auto ownership distribution
  - Households by number of vehicles (0, 1, 2, 3+)
  - Total households and shares
  
- **auto_ownership_by_income.csv** - Auto ownership by income category
  - Cross-tabulation of vehicles by income bins (<30K, 30-60K, 60-100K, 100-150K, 150K+)
  - Households and shares within each income group
  
- **auto_ownership_by_hhsize.csv** - Auto ownership by household size
  - Cross-tabulation of vehicles by household size (1, 2, 3, 4+)
  - Households and shares within each size group

### Person & Activity Pattern Summaries

#### Person Characteristics
- **person_type_distribution.csv** - Distribution of person types
  - Full-time worker, part-time worker, university student, non-worker, retired, driving age student, pre-driving student, pre-school child
  - Persons and shares by type
  
- **age_distribution.csv** - Age distribution across the region
  - Age groups (0-4, 5-15, 16-17, 18-24, 25-34, 35-49, 50-64, 65+)
  - Persons and shares by age group

#### Coordinated Daily Activity Pattern (CDAP)
- **cdap_patterns_by_person_type.csv** - Activity patterns by person type
  - Mandatory, non-mandatory, home patterns
  - Cross-tabulated by person type
  - Shows shares within each person type
  
- **cdap_patterns_regional.csv** - Regional CDAP pattern distribution
  - Overall distribution of activity patterns
  - Total persons by pattern type

### Tour Summaries

#### Tour Generation
- **tour_frequency_by_purpose.csv** - Number of tours by purpose
  - Work, school, escort, shop, maintenance, eating out, visiting, discretionary, work-based
  - Total tours and shares by purpose
  
#### Tour Mode Choice
- **tour_mode_choice.csv** - Overall tour mode distribution
  - All modes (Drive Alone, Carpool 2, Carpool 3+, Walk, Bike, Walk-Transit-Walk, Park & Ride, Kiss & Ride)
  - Tours and modal shares
  
- **tour_mode_choice_by_purpose.csv** - Tour mode by purpose
  - Cross-tabulation of mode and tour purpose
  - Shows modal shares within each purpose

#### Tour Characteristics
- **tour_distance_distribution.csv** - Tour distance bins
  - Distance categories (0-1, 1-3, 3-5, 5-10, 10-20, 20-40, 40+ miles)
  - Tours and shares by distance bin
  
- **tour_tod_patterns.csv** - Time-of-day patterns
  - Tour start and end time distributions
  - By time period (EA, AM, MD, PM, EV)
  
- **tour_duration_distribution.csv** - Tour duration statistics
  - Duration bins (0-2, 2-4, 4-8, 8-12, 12+ hours)
  - Tours by duration category

### Trip Summaries

#### Trip Mode Choice
- **trip_mode_choice.csv** - Overall trip mode distribution
  - All modes including transit submodes
  - Trips and modal shares
  
- **trip_mode_choice_by_purpose.csv** - Trip mode by purpose
  - Cross-tabulation of mode and trip purpose
  - Shows modal shares within each purpose category

#### Trip Characteristics
- **trip_purpose_distribution.csv** - Trip purpose breakdown
  - Work, school, escort, shop, other maintenance, eating out, visiting, discretionary, work-based
  - Trips and shares by purpose
  
- **trip_distance_distribution.csv** - Trip distance bins
  - Distance categories (0-1, 1-2, 2-5, 5-10, 10-20, 20+ miles)
  - Trips and shares by distance bin
  - Mean and median distances
  
- **trip_duration_distribution.csv** - Trip duration statistics
  - Duration bins (0-10, 10-20, 20-30, 30-45, 45-60, 60+ minutes)
  - Trips by duration category
  - Mean and median durations
  
- **trip_tod_distribution.csv** - Time-of-day distribution
  - Trips by departure time period
  - Hourly distributions available

### Work & School Location Summaries

- **work_location_commute_distance.csv** - Journey to work statistics
  - Average commute distance by worker category
  - Commute distance bins
  - By residence location (county or district)
  
- **journey_to_work_patterns.csv** - Commute flow patterns
  - Origin-destination flows
  - By county or transportation analysis district
  - Mode choice for commute trips
  
- **school_location_patterns.csv** - Student travel patterns
  - Average school trip distance
  - School trip mode choice
  - By student type (grade school, high school, university)

### Transit-Specific Summaries

- **transit_access_mode.csv** - Access/egress mode to transit
  - Walk, Park & Ride, Kiss & Ride
  - By transit mode type (local bus, express bus, light rail, heavy rail, commuter rail)
  
- **transit_boarding_by_line_type.csv** - Boardings by service type
  - Local bus, express bus, BRT, light rail, heavy rail, commuter rail, ferry
  - Total daily boardings
  
- **transit_line_of_work.csv** - Commute transit usage
  - Transit usage for work trips
  - By line and time period

### Special Population Summaries

- **household_income_distribution.csv** - Household income bins
  - Income categories aligned with model segmentation
  - Households and shares by income group
  
- **household_size_distribution.csv** - Household size distribution
  - Size categories (1, 2, 3, 4, 5+ persons)
  - Households and shares by size
  
- **household_workers_distribution.csv** - Workers per household
  - 0, 1, 2, 3+ workers
  - Cross-tabulated with household size and income
  
- **vehicle_availability.csv** - Vehicles per worker ratio
  - Vehicles per worker (0, <1, 1, >1)
  - By income category

### Geographic Summaries

- **tours_by_origin_county.csv** - Tour generation by county
  - Tours originated in each county
  - By tour purpose
  
- **trips_by_origin_county.csv** - Trip generation by county
  - Trips originated in each county
  - By trip mode and purpose
  
- **population_by_county.csv** - Population distribution
  - Persons and households by county
  - Validates against control totals

### Validation Summaries

- **mandatory_tour_validation.csv** - Work/school tour checks
  - Workers vs work tours consistency
  - Students vs school tours consistency
  
- **household_person_consistency.csv** - Household-person linkage checks
  - Persons per household validation
  - Workers per household validation
  
- **tour_trip_consistency.csv** - Tour-trip linkage validation
  - Trips per tour by purpose
  - Stop frequency validation

### Configuration Files

**Location**: `tm2py_utils/summary/validation/data_model/`

- **ctramp_data_model.yaml** - Main configuration file
  - File patterns for CTRAMP outputs
  - Column mappings (trip_mode, tour_purpose, etc.)
  - Value labels (Mode 1 → "SOV_GP", etc.)
  - Aggregations (group modes into categories)
  - Binning specifications (age groups, distance bins)
  - All 30+ summary definitions
  
- **variable_labels.yaml** - Display labels
  - Human-readable labels for variables
  - Used in output CSV headers

### Data Sources

The validation tool reads from:
- `householdData_[iter].csv`
- `personData_[iter].csv`
- `indivTourData_[iter].csv`
- `indivTripData_[iter].csv`
- `jointTourData_[iter].csv` (optional)
- `jointTripData_[iter].csv` (optional)
- `wsLocResults_[iter].csv` (optional)

### Validation Features

Automatic validation checks for:
- ✓ Negative values in count/share fields
- ✓ Share totals summing to ~1.0 within groups
- ✓ Zero or very small totals (< 100)
- ✓ Statistical outliers using IQR method
- ✓ Logical consistency (invalid time periods, impossible household sizes)
- ✓ Data completeness (missing required columns)
- ✓ File existence and readability

### Performance

Typical runtime for full model run (7.4M persons, 2.8M households):
- Loading data: ~2-3 minutes
- Labeling & preprocessing: ~1-2 minutes
- Generating summaries: ~3-5 minutes
- Validation: ~30 seconds
- **Total: ~7-11 minutes**

Memory usage: ~2-4 GB

### Custom Summaries

Users can add custom summaries by editing `ctramp_data_model.yaml`:

```yaml
custom_summaries:
  - name: "my_new_summary"
    summary_type: "validation"
    description: "What this summarizes"
    data_source: "individual_trips"
    group_by: ["trip_mode", "tour_purpose"]
    weight_field: "sample_rate"
    count_name: "trips"
    share_within: "tour_purpose"
```

### Output Format

All summaries are CSV files with consistent formatting:
- Clear column headers
- Counts and shares
- Weighted by sample rate
- Ready for Excel, Python pandas, R, or other analysis tools

Example `tour_mode_choice.csv`:
```csv
tour_mode_name,tours,share
Drive Alone,450000,0.425
Carpool 2,180000,0.170
Walk-Transit-Walk,95000,0.090
...
```

### Comparing Multiple Runs

```bash
# Generate summaries for multiple runs
python summarize_model_run.py "run1/ctramp_output" --output "outputs/run1"
python summarize_model_run.py "run2/ctramp_output" --output "outputs/run2"

# Compare using pandas, Excel, or other tools
```

### Related Tools

**PopulationSim Integration** (also in tm2py-utils):
- Household demographics validation
- Person demographics by county
- Geographic distribution analysis
- Validation against ACS data

### Documentation Links

- Main documentation: https://bayareametro.github.io/tm2py-utils/
- User guide: https://bayareametro.github.io/tm2py-utils/user-guide/
- Summary reference: https://bayareametro.github.io/tm2py-utils/summaries/
- Data model: https://bayareametro.github.io/tm2py-utils/data-model/
- GitHub repository: https://github.com/BayAreaMetro/tm2py-utils

---

**END OF INVENTORY**

*This document inventories all known summaries and outputs from TM2PY and tm2py-utils. For the most up-to-date information, refer to the official documentation in the `docs/` directory and the [tm2py-utils documentation](https://bayareametro.github.io/tm2py-utils/).*
