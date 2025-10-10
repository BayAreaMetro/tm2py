# Network Analysis Reference

This page provides comprehensive technical reference documentation for TM2PY network analysis capabilities.

## Highway Network Attributes

### Complete Attribute Reference

**Source**: Actual TM2PY Bay Area database export  
**File**: [`scripts/emme_link_attributes.txt`](../scripts/emme_link_attributes.txt)

The TM2PY highway network contains **85 link attributes** extracted from the actual Bay Area EMME database:

#### Core Performance Attributes
- `auto_volume` - Total automobile volume
- `auto_time` - Congested travel time (minutes)
- `@free_flow_time` - Free flow travel time (minutes)
- `@capacity` - Link capacity (vehicles/hour)
- `@ft` - Facility type code
- `length` - Link length (miles)

#### Vehicle Type Flows
- `@flow_da` - Drive alone flow
- `@flow_sr2` - Shared 2-person flow
- `@flow_sr3` - Shared 3+ person flow
- `@flow_trk` - Truck flow

#### Cost and Performance
- `@cost_da` - Drive alone cost
- `@bridgetoll_da` - Bridge toll costs
- `@reliability` - Reliability measure

**[→ View complete attribute list](../scripts/emme_link_attributes.txt)**

### Database Structure

**Location**: `E:\2015-tm22-dev-sprint-04\emme_project\Database_highway\emmebank`  
**Network Size**: 839,834 links per scenario  
**Scenarios**: 6 time periods (EA, AM, MD, PM, EV, EA2)

### Facility Type Classification

| Code | Facility Type | Description |
|------|---------------|-------------|
| 1 | Freeway | Interstate highways and freeways |
| 2 | Freeway | Principal arterial - freeway facilities |
| 3 | Arterial | Principal arterial roads |
| 4 | Arterial | Minor arterial roads |
| 5 | Collector | Major collector roads |
| 6 | Collector | Minor collector roads |
| 7 | Local | Local streets and roads |
| 8 | Connector | Highway ramps and connectors |
| 99 | Other | Special facilities and other links |

## Transit Network Attributes

### Complete Attribute Reference

**Source**: TM2PY source code analysis

#### Transit Line Attributes
- `line.id` - Transit line identifier
- `line.mode.id` - Mode character ('b', 'l', 'h', 'r', 'f', 'e')
- `line.headway` - Service headway in minutes
- `line.vehicle.total_capacity` - Total vehicle capacity
- `line["#description"]` - Line description/name
- `line["#src_mode"]` - Source mode for fare calculations
- `line["#faresystem"]` - Fare system ID (1-50)

#### Transit Segment Attributes
- `segment.transit_volume` - **PRIMARY** passenger volume/boardings
- `segment.transit_boardings` - Alternative boarding attribute
- `segment.dwell_time` - Dwell time at stops (minutes)
- `segment.link.length` - Segment length (miles)

### Transit Mode Classification

| Code | Mode | Description |
|------|------|-------------|
| b | Local Bus | Local bus service (modes 10-99) |
| e | Express Bus | Express bus service (modes 80-99) |
| l | Light Rail | Light rail transit (modes 110-119) |
| h | Heavy Rail | Heavy rail/subway (modes 120-129) |
| r | Commuter Rail | Commuter rail service (modes 130-139) |
| f | Ferry | Ferry service (modes 100-109) |

## Analysis Tools

### Network Summary Script

**Location**: `scripts/network_summary.py`

Enhanced script providing comprehensive highway network analysis:

- **Input Validation**: 5-phase validation of database structure and data quality
- **Highway Analysis**: VMT, VHT, delay calculations by facility type and time period
- **Comprehensive Logging**: Detailed progress reporting and diagnostic information

**📖 [→ View Complete Usage Guide](network-summary-usage.md)**

## Transit Network Attributes

### Transit Network Structure

**Location**: `E:\2015-tm22-dev-sprint-04\emme_project\Database_transit\emmebank`  
**Scenarios**: 6 time periods matching highway (EA, AM, MD, PM, EV, EA2)  
**Network Elements**: Transit lines, segments, and nodes

### Transit Line Attributes

**Primary Service Attributes**:
- `line.id` - Unique line identifier
- `line.headway` - Service headway (minutes between vehicles)
- `line.vehicle.total_capacity` - Total vehicle capacity (standing + seated)
- `line.vehicle.seated_capacity` - Seated vehicle capacity only
- `line.mode` - Service mode (bus, rail, etc.)

**Calculated Service Metrics**:
- **Hourly Total Capacity**: `60 × total_capacity ÷ headway`
- **Hourly Seated Capacity**: `60 × seated_capacity ÷ headway`
- **Service Frequency**: `60 ÷ headway` (vehicles per hour)

### Transit Segment Attributes

**Ridership Data**:
- `segment.transit_volume` - **PRIMARY** passenger boardings per hour
- `segment.dwell_time` - Vehicle dwell time at stops (minutes)
- `segment.transit_time_func` - Transit travel time function code

**Geographic Attributes**:
- `segment.i_node.id` - Origin node ID  
- `segment.j_node.id` - Destination node ID
- `segment.link.length` - Segment length (miles)

**Additional Data Fields**:
- `segment.data1` - Custom data field 1
- `segment.data2` - Custom data field 2  
- `segment.data3` - Custom data field 3

### Transit Mode Classification

**TM2PY Mode Coding**:
| Mode ID | Mode Type | Description | Service Characteristics |
|---------|-----------|-------------|------------------------|
| 10-39 | Local Bus | Local bus service | Frequent stops, moderate speed |
| 40-79 | Limited Bus | Limited-stop bus | Fewer stops, higher speed |
| 80-99 | Express Bus | Express bus service | Minimal stops, freeway operation |
| 100-109 | Ferry | Ferry service | Water-based transit |
| 110-119 | Light Rail | Light rail transit | Electric rail, grade separation |
| 120-129 | Heavy Rail | Heavy rail/subway | Grade-separated rapid transit |
| 130-139 | Commuter Rail | Commuter rail | Regional rail service |

### Transit Performance Metrics

**Ridership Metrics**:
- **Line Boardings**: Sum of `segment.transit_volume` across all segments
- **Segment Boardings**: Individual `segment.transit_volume` values
- **Daily Boardings**: Sum across all time periods for all-day totals

**Capacity Utilization**:
- **Load Factor**: `boardings ÷ hourly_total_capacity`
- **Seated Load Factor**: `boardings ÷ hourly_seated_capacity`
- **Peak Load Point**: Maximum boardings along line route

**Service Productivity**:
- **Boardings per Route Mile**: `total_boardings ÷ total_route_length`
- **Passengers per Vehicle Hour**: `boardings ÷ (vehicles_per_hour)`
- **Revenue per Mile**: Ridership-based efficiency metric

**Coverage Metrics**:
- **Route Miles**: Total length of transit routes
- **Line Count**: Number of unique transit lines
- **Segment Count**: Number of route segments with service

## Analysis Tools

### Network Summary Script

**Location**: `scripts/network_summary.py`

Enhanced script providing comprehensive multimodal network analysis:

- **Input Validation**: 6-phase validation of highway and transit database structure
- **Highway Analysis**: VMT, VHT, delay calculations by facility type and time period
- **Transit Analysis**: Boarding volumes, ridership patterns, and service performance
- **Comprehensive Logging**: Detailed progress reporting and diagnostic information

## Performance Metrics

### Highway Network Metrics
- **VMT**: Vehicle Miles Traveled = Volume × Link Length
- **VHT**: Vehicle Hours Traveled = Volume × Travel Time  
- **Delay**: Additional time due to congestion = Volume × (Congested Time - Free Flow Time)
- **Speed**: Average travel speed = VMT / VHT

### Transit Performance Metrics

**Core Ridership Metrics**:
- **Boardings**: `segment.transit_volume` - Passengers boarding at each segment
- **Line Boardings**: `Sum(segment.transit_volume)` - Total boardings per line
- **Daily Boardings**: Sum across all time periods for all-day totals
- **Mode Boardings**: Aggregated ridership by service type

**Capacity and Service Metrics**:
- **Line Capacity**: `60 × vehicle.total_capacity ÷ headway` (passengers/hour)
- **Seated Capacity**: `60 × vehicle.seated_capacity ÷ headway` (seated passengers/hour)
- **Service Frequency**: `60 ÷ headway` (vehicles per hour)
- **Load Factor**: `boardings ÷ line_capacity` (utilization ratio)

**Productivity and Efficiency**:
- **Boardings per Route Mile**: `total_boardings ÷ route_length`
- **Passenger Miles**: `boardings × segment_length` 
- **Revenue Miles**: Total route distance operated
- **Passengers per Vehicle Hour**: Service efficiency metric

**Geographic Coverage**:
- **Route Miles**: `Sum(segment.link.length)` by line/mode
- **Service Area**: Geographic extent of transit coverage
- **Stop Density**: Stops per route mile
- **Network Connectivity**: Transfer opportunities and accessibility

## Usage Guidelines

### For Developers
- Reference attribute files when writing analysis code
- Use consistent transit attribute naming: `transit_volume` is primary boarding attribute
- Follow multimodal analysis patterns established in network summary script
- Implement proper validation for both highway and transit databases

### For Analysts  
- Use network summary script for comprehensive multimodal performance analysis
- Access detailed attribute documentation for custom analysis
- Validate results against expected ranges for Bay Area highway and transit networks
- Consider both modes when evaluating transportation system performance

### For Documentation Maintenance
- Keep attribute files synchronized with database changes
- Update transit mode classifications as service types evolve
- Maintain consistency between highway and transit documentation standards
- Document any changes to ridership data sources or calculation methods