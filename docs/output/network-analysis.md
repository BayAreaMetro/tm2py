# Network Analysis Reference

This page provides comprehensive technical reference documentation for TM2PY network analysis capabilities.

## Highway Network Attributes

### Complete Attribute Reference

**Source**: Actual TM2PY Bay Area database export  
**File**: [`scripts/emme_link_attributes.txt`](../scripts/emme_link_attributes.txt)

The TM2PY highway network contains **85 link attributes** extracted from the actual Bay Area EMME database:

#### Core Performance Attributes
- `auto_volume` - Total automobile volume = 2.58455491065979
- `auto_time` - Congested travel time (minutes) = 0.574384868144989
- `@free_flow_time` - Free flow travel time (minutes) = 0.574384868144989
- `@capacity` - Link capacity (vehicles/hour) = 6150.0
- `@ft` - Facility type code = 8.0
- `length` - Link length (miles) = 0.17231546342372894

#### Vehicle Type Flows
- `@flow_da` - Drive alone flow = 1.0269575119018555
- `@flow_sr2` - Shared 2-person flow = 0.049851808696985245
- `@flow_sr3` - Shared 3+ person flow = 0.037745535373687744
- `@flow_trk` - Truck flow = 1.4399999380111694

#### Cost and Performance
- `@cost_da` - Drive alone cost = 2.9689953327178955
- `@bridgetoll_da` - Bridge toll costs
- `@reliability` - Reliability measure = 0.0

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

## Performance Metrics

### Highway Network Metrics
- **VMT**: Vehicle Miles Traveled = Volume × Link Length
- **VHT**: Vehicle Hours Traveled = Volume × Travel Time  
- **Delay**: Additional time due to congestion = Volume × (Congested Time - Free Flow Time)
- **Speed**: Average travel speed = VMT / VHT

### Transit Performance Metrics
- **Line Capacity**: 60 × Vehicle Capacity ÷ Headway
- **Load Factor**: Boarding Volume ÷ Line Capacity
- **Passenger Miles**: Volume × Segment Length
- **Service Frequency**: 60 ÷ Headway (vehicles per hour)

## Data Validation

### Highway Validation Ranges (Bay Area)
- **Daily VMT**: 280-320 million vehicle-miles
- **Average Speed**: 25-35 mph network-wide
- **Freeway Speeds**: 45-65 mph average
- **Arterial Speeds**: 25-40 mph average

### Transit Validation Ranges
- **Daily Boardings**: 1.5-2.0 million
- **Peak Load Factors**: 0.6-0.8 on major lines
- **System Load Factor**: 0.3-0.5 average
- **Service Coverage**: 50+ transit agencies

## Usage Guidelines

### For Developers
- Reference attribute files when writing analysis code
- Use shared `TransitBoardingAnalyzer` to avoid code duplication
- Follow documented attribute priority: `transit_volume` is primary boarding attribute

### For Analysts  
- Use network summary script for comprehensive performance analysis
- Access detailed attribute documentation for custom analysis
- Validate results against expected ranges for Bay Area

### For Documentation Maintenance
- Run `extract_transit_attributes.py` to update transit documentation
- Keep attribute files synchronized with database changes
- Update validation ranges based on model improvements