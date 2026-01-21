# Highway Network Processing Pipeline

This document describes the complete flow of highway network data through tm2py, including scenario IDs, emmebank locations, and attribute transformations at each step.

## Overview Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              INPUT (Base Network)                                │
├─────────────────────────────────────────────────────────────────────────────────┤
│  Emmebank: Database_highway/emmebank                                            │
│  Scenario ID: 1 (all_day_scenario_id from config)                               │
│  Title: "Base Network" or similar                                               │
│                                                                                 │
│  LINK ATTRIBUTES (period-specific, lowercase suffix):                           │
│    @lanes_ea, @lanes_am, @lanes_md, @lanes_pm, @lanes_ev                        │
│    @useclass_ea, @useclass_am, @useclass_md, @useclass_pm, @useclass_ev         │
│    @ft (facility type: 1-8, 99)                                                 │
│    @free_flow_speed (may be 0, set later from capclass lookup)                  │
│    @tollbooth, @tollseg (toll facility coding, optional)                        │
│    @drive_link (1=driveable, 0=walk/bike only)                                  │
│    @bike_link, @bus_only                                                        │
│    #link_county (county ID)                                                     │
│                                                                                 │
│  STANDARD ATTRIBUTES:                                                           │
│    num_lanes (EMME standard, may be 0 in some networks)                         │
│    length (link length in miles)                                                │
└─────────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    STEP 1: create_tod_scenarios                                  │
│                    Component: CreateTODScenarios                                 │
├─────────────────────────────────────────────────────────────────────────────────┤
│  1a. _create_highway_scenarios()                                                │
│      - Creates/ensures these attributes exist on ref scenario:                  │
│        @area_type, @capclass, @free_flow_speed, @free_flow_time, @lanes         │
│      - If @lanes is all zeros, copies from num_lanes (legacy network fix)       │
│      - Calls _set_area_type(): calculates @area_type from MAZ density           │
│      - Calls _set_capclass(): @capclass = 10*@area_type + @ft                   │
│      - Calls _set_speed(): @free_flow_speed from capclass_lookup table          │
│                            @free_flow_time = 60 * length / speed                │
│                                                                                 │
│  1b. _prepare_scenarios_and_attributes()                                        │
│      - Copies Scenario 1 → Scenario 11, 12, 13, 14, 15 (per time period)        │
│      - For each period scenario:                                                │
│        • Copies @lanes_am → @lanes (case-insensitive matching)                  │
│        • Copies @useclass_am → @useclass                                        │
│        • Deletes other period suffixes (@lanes_ea, @lanes_pm, etc.)             │
└─────────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    INTERMEDIATE STATE (After create_tod_scenarios)               │
├─────────────────────────────────────────────────────────────────────────────────┤
│  Emmebank: Database_highway/emmebank                                            │
│                                                                                 │
│  Scenario 1 (Reference, unchanged):                                             │
│    Still has @lanes_am, @lanes_pm, etc.                                         │
│                                                                                 │
│  Scenario 11 (EA), 12 (AM), 13 (MD), 14 (PM), 15 (EV):                          │
│    @lanes      ← copied from @lanes_{period}                                    │
│    @useclass   ← copied from @useclass_{period}                                 │
│    @area_type  ← calculated from MAZ density (1=CBD to 5=rural)                 │
│    @capclass   ← 10*area_type + ft (e.g., 34 = suburban arterial)               │
│    @free_flow_speed ← from capclass lookup table                                │
│    @free_flow_time  ← 60 * length / speed                                       │
│    @ft         ← unchanged from base                                            │
└─────────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    STEP 2: prepare_network_highway                               │
│                    Component: PrepareHighwayNetwork                              │
├─────────────────────────────────────────────────────────────────────────────────┤
│  For each time period scenario (11, 12, 13, 14, 15):                            │
│                                                                                 │
│  2a. _set_tolls() - from inputs/hwy/tolls.csv                                   │
│      @toll_{class}  ← toll cost by vehicle class (da, s2, s3, vsm, etc.)        │
│      @bridgetoll_{class}                                                        │
│      @valuetoll_{class}                                                         │
│                                                                                 │
│  2b. _set_vdfs() - Volume Delay Functions                                       │
│      Sets emmebank.extra_function_parameters:                                   │
│        el1 = @free_flow_time                                                    │
│        el2 = @capacity                                                          │
│        el3 = @ja (Akcelik parameter)                                            │
│        el4 = @static_rel (reliability factor)                                   │
│                                                                                 │
│  2c. _set_capacity_speed_vdf()                                                  │
│      @capacity = cap_lanehour * highway_capacity_factor * @lanes                │
│      @ja       ← Akcelik parameter for VDF types 3,4,5,7,8,10-14                │
│      link.volume_delay_func = @ft (VDF number 1-8)                              │
│      link.num_lanes = max(min(9.9, @lanes), 1.0)                                │
│                                                                                 │
│  2d. _set_link_modes()                                                          │
│      Sets allowed modes on each link based on @useclass:                        │
│        d=drive, h=HOV2, i=HOV3, H=HOV2toll, I=HOV3toll, etc.                    │
│                                                                                 │
│  2e. _set_link_costs()                                                          │
│      @cost_{class} = @free_flow_time + VOT_factor * @toll_{class}               │
└─────────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    OUTPUT STATE (Ready for Assignment)                           │
├─────────────────────────────────────────────────────────────────────────────────┤
│  Emmebank: Database_highway/emmebank                                            │
│                                                                                 │
│  Scenario 11 (EA), 12 (AM), 13 (MD), 14 (PM), 15 (EV):                          │
│                                                                                 │
│  LINK ATTRIBUTES FOR ASSIGNMENT:                                                │
│    @lanes           Number of lanes for this period                             │
│    @capacity        Hourly capacity (lanes × cap/lane × period_factor)          │
│    @free_flow_time  Minutes at free-flow speed                                  │
│    @free_flow_speed Speed in mph                                                │
│    @useclass        HOV/toll restriction class                                  │
│    @ft              Facility type (VDF selector)                                │
│    @capclass        Capacity class (area_type*10 + ft)                          │
│    @area_type       1=CBD, 2=urban business, 3=urban, 4=suburban, 5=rural       │
│    @ja              Akcelik VDF parameter                                       │
│    @static_rel      Static reliability factor                                   │
│                                                                                 │
│  TOLL ATTRIBUTES (per vehicle class):                                           │
│    @toll_da, @toll_s2, @toll_s3, @toll_vsm, @toll_sml, @toll_med, @toll_lrg     │
│    @bridgetoll_{class}, @valuetoll_{class}                                      │
│                                                                                 │
│  COST ATTRIBUTES (per vehicle class):                                           │
│    @cost_da, @cost_s2, @cost_s3, @cost_vsm, @cost_sml, @cost_med, @cost_lrg     │
│    @cost_datoll, @cost_s2toll, @cost_s3toll                                     │
│                                                                                 │
│  LINK MODES:                                                                    │
│    d=drive, h=HOV2, i=HOV3, c=truck, x=MAZ, w=walk, b=bike                      │
│    H=HOV2toll, I=HOV3toll, D=SOVtoll                                            │
└─────────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    STEP 3: highway (Assignment)                                  │
│                    Component: HighwayAssignment                                  │
├─────────────────────────────────────────────────────────────────────────────────┤
│  SOLA Traffic Assignment using:                                                 │
│    - VDFs fd1-fd8, fd99 (indexed by @ft)                                        │
│    - el1=@free_flow_time, el2=@capacity, el3=@ja, el4=@static_rel               │
│    - Demand matrices from TAZ_Demand_{period}.omx                               │
│                                                                                 │
│  OUTPUT ATTRIBUTES (added during assignment):                                   │
│    volau        Auto volume (vehicles/period)                                   │
│    volad        Additional volume                                               │
│    @vol_{class} Volume by vehicle class                                         │
│                                                                                 │
│  SKIM MATRICES:                                                                 │
│    Time, distance, cost, toll skims by vehicle class                            │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## Scenario ID Mapping

| Config Setting | Period | Scenario ID | Title |
|----------------|--------|-------------|-------|
| `emme.all_day_scenario_id` | Reference | 1 | Base Network |
| `time_periods[0].emme_scenario_id` | EA | 11 | EA Base Network |
| `time_periods[1].emme_scenario_id` | AM | 12 | AM Base Network |
| `time_periods[2].emme_scenario_id` | MD | 13 | MD Base Network |
| `time_periods[3].emme_scenario_id` | PM | 14 | PM Base Network |
| `time_periods[4].emme_scenario_id` | EV | 15 | EV Base Network |

## Emmebank Structure

```
{run_directory}/
└── emme_project/
    ├── mtc_emme.emp                    # EMME project file
    ├── Database_highway/
    │   └── emmebank                    # Highway emmebank
    │       ├── Scenario 1  (reference) 
    │       ├── Scenario 11 (EA)        
    │       ├── Scenario 12 (AM)        # ← Used for AM assignment
    │       ├── Scenario 13 (MD)        
    │       ├── Scenario 14 (PM)        
    │       └── Scenario 15 (EV)        
    └── Database_transit/
        └── emmebank                    # Transit emmebank (separate)
```

## Attribute Reference

### Input Attributes (Base Network)

| Attribute | Type | Domain | Description |
|-----------|------|--------|-------------|
| `@lanes_{period}` | float | LINK | Number of lanes by time period (ea, am, md, pm, ev) |
| `@useclass_{period}` | int | LINK | HOV/toll restriction by time period |
| `@ft` | int | LINK | Facility type: 1=freeway, 2=expressway, 3-4=arterial, 5-6=collector, 7=local, 8=ramp, 99=connector |
| `@tollbooth` | int | LINK | Toll booth ID (0=no toll) |
| `@tollseg` | int | LINK | Toll segment ID |
| `@drive_link` | int | LINK | 1=driveable, 0=walk/bike only |
| `num_lanes` | float | LINK | EMME standard lanes (fallback if @lanes is empty) |

### Calculated Attributes (create_tod_scenarios)

| Attribute | Formula/Source | Description |
|-----------|----------------|-------------|
| `@area_type` | MAZ density calculation | 1=CBD, 2=urban business, 3=urban, 4=suburban, 5=rural |
| `@capclass` | `10 * @area_type + @ft` | Combined capacity class (e.g., 34=suburban minor arterial) |
| `@free_flow_speed` | capclass_lookup table | Speed in mph |
| `@free_flow_time` | `60 * length / @free_flow_speed` | Minutes at free-flow |
| `@lanes` | Copied from `@lanes_{period}` | Period-specific lanes |
| `@useclass` | Copied from `@useclass_{period}` | Period-specific restrictions |

### Calculated Attributes (prepare_network_highway)

| Attribute | Formula/Source | Description |
|-----------|----------------|-------------|
| `@capacity` | `cap_lanehour * capacity_factor * @lanes` | Hourly capacity |
| `@ja` | Akcelik formula | VDF parameter for congested conditions |
| `@toll_{class}` | tolls.csv lookup | Toll cost by vehicle class |
| `@cost_{class}` | `@free_flow_time + VOT * @toll_{class}` | Generalized cost |

### Volume Delay Function Parameters

```
el1 = @free_flow_time    (minutes)
el2 = @capacity          (vehicles/hour)
el3 = @ja                (Akcelik parameter)
el4 = @static_rel        (reliability factor)
```

### VDF Formulas by Facility Type

| VDF | Facility Type | Formula Type |
|-----|---------------|--------------|
| fd1 | Freeway | BPR with reliability adjustments |
| fd2 | Expressway | BPR with reliability adjustments |
| fd3-fd7 | Arterials/Collectors | Akcelik with reliability adjustments |
| fd8 | Ramps/Connectors | Free-flow only (el1) |
| fd99 | Special | Akcelik |

## Common Issues

### @capacity = 0 (Division by Zero)

**Symptom**: "Illegal character '+'" error during assignment

**Root Cause Chain**:
1. `@lanes_{period}` not copied to `@lanes` (case sensitivity issue)
2. `@capacity = cap_lanehour * @lanes` = 0
3. VDF divides by `@capacity` → infinity
4. EMME formats as `0.135972+124` → lexer crash

**Fix**: Case-insensitive matching for period attribute names (applied January 2026)

### Missing @useclass

**Symptom**: "MISSING NETWORK ATTRIBUTES" warning, tolls skipped

**Cause**: Network doesn't have `@useclass_{period}` attributes coded

**Solution**: Either:
- Add toll coding to base network
- Accept that toll assignment will be skipped

## See Also

- [Network Data Inputs](network.md)
- [Network Data Issues (Troubleshooting)](../troubleshooting/network-data-issues.md)
- [Highway Assignment](../assignment/index.md)
