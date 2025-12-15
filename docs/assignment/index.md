# Travel Model Two Assignment System

## Overview

The Travel Model Two (TM2) assignment system converts travel demand into traffic flows and transit ridership by simulating traveler route choice on transportation networks. The assignment system consists of two primary components: highway assignment and transit assignment, each using equilibrium-based algorithms to model realistic travel patterns.

## System Architecture

```mermaid
graph TD
    START[Base Network] --> TOD[Create TOD Scenarios]
    TOD --> READY[Period-Specific Networks<br/>EA, AM, MD, PM, EV]
    READY --> A[Travel Demand<br/>From CT-RAMP]
    
    A --> B{Assignment Type}
    B -->|Auto Trips| C[Highway Assignment]
    B -->|Transit Trips| D[Transit Assignment]
    
    C --> E[Highway Network Loading]
    E --> F[Volume-Delay Functions]
    F --> G[Updated Travel Times]
    G -->|Feedback| C
    
    D --> H[Transit Network Loading]
    H --> I[Capacity Constraints]
    I --> J[Updated Transit Times]
    J -->|Feedback| D
    
    C --> K[Highway Skims]
    D --> L[Transit Skims]
    K --> M[Level-of-Service Feedback<br/>to CT-RAMP]
    L --> M
```

## Network Preparation

### Create Time-of-Day Scenarios

**Component**: `create_tod_scenarios` (runs first in iteration 0)

The `create_tod_scenarios` component creates time-of-day specific network scenarios in Emme from a single all-day base scenario. This component must run before any assignment can occur, as it establishes the fundamental network infrastructure for all time periods.

#### Time Periods

The model operates with five distinct time periods:

| Period | Name | Description | Typical Hours | Scenario ID |
|--------|------|-------------|---------------|-------------|
| EA | Early AM | Early morning off-peak | 3:00 AM - 6:00 AM | 1 |
| AM | AM Peak | Morning peak period | 6:00 AM - 10:00 AM | 2 |
| MD | Midday | Midday off-peak | 10:00 AM - 3:00 PM | 3 |
| PM | PM Peak | Evening peak period | 3:00 PM - 7:00 PM | 4 |
| EV | Evening | Evening/night off-peak | 7:00 PM - 3:00 AM | 5 |

#### Highway TOD Scenario Creation

**Volume Delay Functions (VDFs)**: The component sets up congestion functions that relate traffic volume to travel time:

**Freeway VDFs** (fd1, fd2):
```
BPR Formula: time = free_flow_time × (1 + 0.20 × ((volume/capacity)/0.75)^6)
Plus reliability factors based on Level of Service thresholds
```

**Arterial/Road VDFs** (fd3-fd7, fd9-fd14, fd99):
```
Akcelik Formula: time = free_flow_time + 60 × (0.25 × (v/c - 1 + sqrt((v/c - 1)^2 + ja × v/c)))
Plus reliability factors
```

**Fixed Time** (fd8): No congestion effects

**Link Attributes Created**:

- **@area_type**: Density-based area classification (0-5)
  - 0: Regional core (density ≥ 300)
  - 1: CBD (100 ≤ density < 300)
  - 2: Urban business (55 ≤ density < 100)
  - 3: Urban (30 ≤ density < 55)
  - 4: Suburban (6 ≤ density < 30)
  - 5: Rural (density < 6)
  - *Density = (1 × population + 2.5 × employment) / acres within 1-mile buffer*

- **@capclass**: Capacity class = 10 × area_type + facility_type
- **@free_flow_speed**: Speed from capclass lookup table (mph)
- **@free_flow_time**: 60 × link_length / free_flow_speed (minutes)

**Reliability Parameters**: Congestion-based travel time reliability factors

| Facility Type | LOS Threshold | Volume/Capacity | Reliability Factor |
|---------------|---------------|-----------------|-------------------|
| Freeway LOS C | 0.7 | Moderate congestion | 0.2429 |
| Freeway LOS D | 0.8 | Heavy congestion | 0.1705 |
| Freeway LOS E | 0.9 | Near capacity | -0.2278 |
| Freeway LOS F-Low | 1.0 | At capacity | -0.1983 |
| Freeway LOS F-High | 1.2 | Over capacity | 1.022 |
| Arterial LOS C | 0.7 | Moderate congestion | 0.1561 |
| Arterial LOS F-Low | 1.0 | At capacity | -0.449 |

#### Transit TOD Scenario Creation

**Transit Modes**: Configured from model settings with attributes:
- In-vehicle perception factor (default: 1.0)
- Initial boarding penalty (default: 10 min)
- Transfer boarding penalty (default: 10 min)
- Headway fraction for wait time (default: 0.5)
- Transfer wait perception factor (default: 1.0)

**Transit Link Times**:

Fixed guideway (calculated from link length and mode speed):

| Mode | Speed | Application |
|------|-------|-------------|
| CRAIL | 45 mph | Commuter rail |
| HRAIL | 40 mph | Heavy rail (BART) |
| LRAIL | 30 mph | Light rail |
| FERRY | 15 mph | Ferry service |

Bus on streets: `time = 60 × length/speed + boarding_time`

**Transit Line Attributes**:
- `@invehicle_factor`: Mode-specific in-vehicle time perception
- `@iboard_penalty`: Initial boarding time penalty
- `@xboard_penalty`: Transfer boarding penalty
- `@orig_hdw`: Original headway from schedule

**Period Filtering**: Transit lines only operate in their designated time periods. Lines with `#time_period` not matching the scenario period are removed.

#### Period-Specific Attribute Copying

The component automatically handles time-of-day specific attributes:

1. **Identifies TOD attributes**: Finds all attributes ending with period names (e.g., `@volume_EA`, `@volume_AM`)
2. **Creates period scenarios**: Copies base scenario for each time period
3. **Copies period values**: Transfers period-specific values to non-suffixed attributes
4. **Removes other periods**: Deletes attributes for other periods to save memory

**Example**: For AM period scenario:
- `@volume_EA`, `@volume_MD`, `@volume_PM`, `@volume_EV` → deleted
- `@volume_AM` → copied to `@volume` → `@volume_AM` deleted

#### Coordinate Projection

Networks are projected to **NAD83(HARN) California State Plane Zone 6 (feet)** if not already in this coordinate system. This ensures consistent spatial calculations for area type classification.

#### Runtime

Typical execution time: **20-27 minutes** (based on full regional model)
- Highway scenarios: ~5-10 minutes
- Transit scenarios: ~10-15 minutes  
- Attribute processing: ~2-5 minutes

## Highway Assignment

### Highway Assignment Overview

The highway assignment component simulates vehicle route choice on the road network, accounting for congestion effects and traveler preferences for time, cost, and convenience.

### Highway Assignment Features

**Multi-Class Assignment**: Supports different vehicle types with distinct characteristics:

| Vehicle Class | Description | Network Access |
|---------------|-------------|----------------|
| Drive Alone (da) | Single-occupant vehicles | All facilities |
| Shared Ride 2 (sr2) | Two-person carpools | Includes HOV-2 lanes |
| Shared Ride 3+ (sr3) | Three+ person carpools | Includes HOV-3+ lanes |
| Very Small Trucks (vsm) | Light commercial vehicles | Most facilities |
| Small Trucks (sml) | Small commercial vehicles | Truck-restricted access |
| Medium Trucks (med) | Medium commercial vehicles | Truck-restricted access |
| Large Trucks (lrg) | Heavy commercial vehicles | Truck routes only |

**Equilibrium Algorithm**: Uses Emme's SOLA (Second Order Linear Approximation) algorithm for:

- **Traffic Assignment**: Route choice based on generalized cost minimization
- **Convergence**: Iterative process until stable flow patterns
- **Volume-Delay Functions**: Dynamic link travel times based on traffic volumes

### Network Representation

**Geographic Coverage**: 9-county San Francisco Bay Area
**Network Size**: ~180,000 directional links, ~85,000 nodes *(approximate - varies by model version)*
**Temporal Resolution**: Five time periods (AM peak, PM peak, midday, evening, early AM)

**Link Attributes**:

- **Capacity**: Maximum vehicle throughput
- **Free-Flow Speed**: Uncongested travel speed  
- **Volume-Delay Function**: Congestion relationship
- **Facility Type**: Freeway, arterial, local, ramp
- **Toll Information**: Bridge tolls, express lane tolls
- **Vehicle Restrictions**: HOV requirements, truck restrictions

### Generalized Cost

Highway assignment uses **value of time** to convert monetary costs to time-equivalent units:

**[Value of Time in Highway Assignment](highway_value_of_time.md)** - Detailed documentation of VOT parameters and implementation

**[Volume Delay Functions](volume_delay_functions.md)** - Mathematical specifications for link travel time calculations

**Components**:

- **Travel Time**: Link-specific travel times (minutes)
- **Distance Cost**: Operating cost × distance (cents/mile)
- **Toll Cost**: Bridge tolls, express lane tolls (cents)
- **Value of Time Conversion**: $18.93/hour (2010 dollars)

## Transit Assignment

### Transit Assignment Overview

The transit assignment component simulates passenger route choice on the public transportation network, including buses, rail, ferry, and multi-modal connections.

### Transit Assignment Features

**Capacity-Constrained Assignment**:

- **Initial Assignment**: Unconstrained shortest path assignment
- **Capacity Evaluation**: Check vehicle capacity limits
- **Crowding Effects**: Increased perceived travel time for overcrowded vehicles
- **Reassignment**: Iterative process with capacity constraints

**Multi-Modal Network**:

| Mode Type | Examples | Special Considerations |
|-----------|----------|----------------------|
| Bus | AC Transit, Muni Bus, VTA | Frequency-based headways |
| Rail | BART, Caltrain, VTA Light Rail | Schedule-based operations |
| Ferry | Golden Gate, SF Bay Ferry | Weather and capacity constraints |
| Cable Car | San Francisco Cable Car | Tourist vs. commuter usage |

### Access/Egress Modes

**Walk Access**: Direct walking to/from transit stops
**Drive Access**: Park-and-ride facilities
**Kiss-and-Ride**: Drop-off/pick-up access
**Bike Access**: Bicycle access to transit (bike parking/bike-on-board)

### Temporal Representation

**Service Periods**: Coordinated with highway time periods
**Headway-Based**: Frequent services represented by average wait times
**Schedule-Based**: Lower frequency services with specific departure times

## Level-of-Service Feedback

### Highway-Transit Integration

**Competitive Modes**: Highway congestion affects transit competitiveness
**Park-and-Ride**: Highway access costs influence transit ridership
**Cordon Pricing**: Area-based tolls affect mode choice

### CT-RAMP Integration

**Skim Generation**: Assignment produces level-of-service matrices
**Accessibility Measures**: Network performance influences activity location and mode choice
**Feedback Loops**: Multi-iteration process between demand and assignment

## Technical Implementation

### Software Platform

**Emme**: Primary assignment platform

- **Highway**: SOLA traffic assignment algorithm *(confirmed in code)*
- **Transit**: Strategy-based transit assignment *(uses OPTIMAL_STRATEGY)*
- **Integration**: Python API for TM2 coordination

**TM2 Python Framework**:

```python
tm2py/components/
├── network/
│   ├── highway/           # Highway assignment components
│   │   ├── highway_assign.py
│   │   ├── highway_maz.py  
│   │   └── highway_emme_spec.py
│   └── transit/           # Transit assignment components
│       ├── transit_assign.py
│       ├── transit_skims.py
│       └── transit_capacity.py
```

### Performance and Scalability

**Computational Requirements** *(estimated based on typical model performance)*:

- **Highway Assignment**: ~15-30 minutes per time period *(varies by network size and convergence criteria)*
- **Transit Assignment**: ~10-20 minutes per time period *(varies by service complexity and capacity constraints)*
- **Total Runtime**: 2-4 hours for complete assignment set *(including all time periods and iterations)*
- **Memory Usage**: 8-16 GB RAM recommended *(depends on network size and number of zones)*
- **Parallel Processing**: Multi-core CPU utilization *(configurable via emme.num_processors)*

## Validation and Calibration

### Highway Assignment Validation

**Traffic Counts**: Comparison with observed link volumes
**Travel Time Validation**: GPS-based travel time comparisons
**Toll Facility Usage**: Express lane and bridge crossing validation

### Transit Assignment Validation

**Ridership Counts**: Operator-provided boardings data
**Load Profiles**: Peak-direction, peak-period capacity utilization
**Transfer Patterns**: Multi-operator journey validation

## Configuration and Customization

### Assignment Parameters

**Convergence Criteria**: Gap functions and iteration limits
**Volume-Delay Functions**: Facility-specific congestion relationships  
**Value of Time**: Mode and income-specific time valuations
**Capacity Constraints**: Transit vehicle capacity and crowding effects

### Scenario Analysis

**Infrastructure Projects**: New facilities and capacity improvements
**Service Changes**: Transit service modifications
**Pricing Policies**: Tolling and fare policy evaluation
**Land Use Scenarios**: Development pattern impacts

## Related Documentation

### Technical Documentation

- **[Highway Value of Time](highway_value_of_time.md)** - VOT parameters and calculations
- **[Network Summary Component](../network_summary.md)** - Assignment result analysis
- **[Configuration Guide](../configuration.md)** - Assignment parameter setup

### Model Integration

- **[CT-RAMP Overview](../ctramp/index.md)** - Demand model integration
- **[Model Workflow](../run.md)** - Complete model execution process
- **[Outputs](../outputs.md)** - Assignment result interpretation

---

*Last Updated: November 2025*  
*Model Version: Travel Model Two v2.2*  
*Authors: MTC Staff & GitHub Copilot*
