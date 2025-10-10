# Network Data 🛣️

!!! info "Network Preparation"
    For information on how to prepare and process network files for the base year, see **[Creating Base Year Inputs](../create-base-year-inputs.md#network-data)** 🛣️

## Roadway Network

The all streets highway network, walk network, and bicycle network were developed from [OpenStreetMap](http://www.openstreetmap.org/). The *projection* is [**NAD 1983 StatePlane California VI FIPS 0406 Feet**](https://epsg.io/102646).

### Highway Link Attributes

The TM2PY highway network contains **85 link attributes** in the EMME database:

**[→ View complete attribute list](../scripts/emme_link_attributes.txt)**

#### Complete Attribute Reference

##### Node and Link Identifiers
- `#a_node` - A-node ID (int32)
- `#b_node` - B-node ID (int32)
- `#cntype` - Connector type (str)
- `#link_county` - Link county (str)
- `#link_id` - Link ID (int32)
- `#shstgeometryid` - Shared streets geometry ID (str)

##### Area and Network Classification
- `@area_type` - Area type code (float)
- `@assignable` - Link assignable flag (float)
- `@auto_time` - Auto travel time (float)
- `@bike_link` - Bicycle link flag (float)
- `@bus_only` - Bus-only link flag (float)
- `@capacity` - Link capacity (float)
- `@capclass` - Capacity class (float)
- `@drive_link` - Drive access flag (float)
- `@ft` - Facility type code (float)
- `@hov_length` - HOV length (float)
- `@lanes` - Number of lanes (float)
- `@managed` - Managed lane flag (float)
- `@rail_link` - Rail link flag (float)
- `@transit` - Transit link flag (float)
- `@useclass` - Use class (float)
- `@walk_link` - Walk access flag (float)

##### Bridge Tolls
- `@bridgetoll_da` - Bridge toll drive alone (float)
- `@bridgetoll_lrg` - Bridge toll large truck (float)
- `@bridgetoll_med` - Bridge toll medium truck (float)
- `@bridgetoll_sml` - Bridge toll small truck (float)
- `@bridgetoll_sr2` - Bridge toll shared ride 2 (float)
- `@bridgetoll_sr3` - Bridge toll shared ride 3+ (float)
- `@bridgetoll_vsm` - Bridge toll very small truck (float)

##### Travel Costs
- `@cost_da` - Cost drive alone (float)
- `@cost_datoll` - Cost drive alone with toll (float)
- `@cost_lrgtrk` - Cost large truck (float)
- `@cost_lrgtrktoll` - Cost large truck with toll (float)
- `@cost_sr2` - Cost shared ride 2 (float)
- `@cost_sr2toll` - Cost shared ride 2 with toll (float)
- `@cost_sr3` - Cost shared ride 3+ (float)
- `@cost_sr3toll` - Cost shared ride 3+ with toll (float)
- `@cost_trk` - Cost truck (float)
- `@cost_trktoll` - Cost truck with toll (float)

##### Traffic Flows
- `@flow_da` - Flow drive alone (float)
- `@flow_datoll` - Flow drive alone toll (float)
- `@flow_lrgtrk` - Flow large truck (float)
- `@flow_lrgtrktoll` - Flow large truck toll (float)
- `@flow_sr2` - Flow shared ride 2 (float)
- `@flow_sr2toll` - Flow shared ride 2 toll (float)
- `@flow_sr3` - Flow shared ride 3+ (float)
- `@flow_sr3toll` - Flow shared ride 3+ toll (float)
- `@flow_trk` - Flow truck (float)
- `@flow_trktoll` - Flow truck toll (float)

##### Performance and Timing
- `@free_flow_speed` - Free flow speed (float)
- `@free_flow_time` - Free flow time (float)
- `@intdist_down` - Intersection distance downstream (float)
- `@intdist_up` - Intersection distance upstream (float)
- `@ja` - Junction adjustment (float)
- `@maz_flow` - MAZ flow (float)
- `@reliability` - Reliability measure (float)
- `@reliability_sq` - Reliability squared (float)
- `@segment_id` - Segment ID (float)
- `@static_rel` - Static reliability (float)

##### Toll and Value Pricing
- `@toll_length` - Toll length (float)
- `@tollbooth` - Tollbooth flag (float)
- `@tollseg` - Toll segment flag (float)
- `@valuetoll_da` - Value toll drive alone (float)
- `@valuetoll_lrg` - Value toll large truck (float)
- `@valuetoll_med` - Value toll medium truck (float)
- `@valuetoll_sml` - Value toll small truck (float)
- `@valuetoll_sr2` - Value toll shared ride 2 (float)
- `@valuetoll_sr3` - Value toll shared ride 3+ (float)
- `@valuetoll_vsm` - Value toll very small truck (float)

##### Core EMME Attributes
- `additional_volume` - Additional volume (int)
- `auto_time` - Auto travel time (float)
- `auto_volume` - Auto volume (float)
- `data1` - Data field 1 (float)
- `data2` - Data field 2 (float)
- `data3` - Data field 3 (float)
- `id` - Link ID string (str)
- `length` - Link length (float)
- `modes` - Available modes (frozenset)
- `num_lanes` - Number of lanes (float)
- `numpy_vertices` - Vertex array (ndarray)
- `reverse_link` - Reverse link reference (Link)
- `shape_length` - Shape length (float)
- `type` - Link type (int)
- `vertices` - Vertex list (list)
- `volume_delay_func` - Volume delay function (int)

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

## County Node Numbering System

The highway network uses a numbering system whereby each county has a reserved block of nodes. Within each county's block:

- Nodes 1 through 9,999 are reserved for TAZs
- Nodes 10,001 through 89,999 are for MAZs  
- Nodes 90,001 through 99,999 are for transit access points (TAPs)

The blocks are assigned to the nine counties per MTC's numbering scheme, as shown in the table below.

Roadway, walk, bicycle, and transit network nodes are numbered by county as well and range from 1,000,000 to 10,000,000 as shown below.

| Code | County | TAZs | MAZs | TAPs | Network Node | HOV Lane Node |
|------|--------|------|------|------|--------------|----------------|
| 1 | San Francisco | 1–9,999 | 10,001–89,999 | 90,001–99,999 | 1,000,000–1,500,000 | 5,500,000–6,000,000 |
| 2 | San Mateo | 100,001–109,999 | 110,001–189,999 | 190,001–199,999 | 1,500,000–2,000,000 | 6,000,000–6,500,000 |
| 3 | Santa Clara | 200,001–209,999 | 210,001–289,999 | 290,001–299,999 | 2,000,000–2,500,000 | 6,500,000–7,000,000 |
| 4 | Alameda | 300,001–309,999 | 310,001–389,999 | 390,001–399,999 | 2,500,000–3,000,000 | 7,000,000–7,500,000 |
| 5 | Contra Costa | 400,001–409,999 | 410,001–489,999 | 490,001–499,999 | 3,000,000–3,500,000 | 7,500,000–8,000,000 |
| 6 | Solano | 500,001–509,999 | 510,001–589,999 | 590,001–599,999 | 3,500,000–4,000,000 | 8,000,000–8,500,000 |
| 7 | Napa | 600,001–609,999 | 610,001–689,999 | 690,001–699,999 | 4,000,000–4,500,000 | 8,500,000–9,000,000 |
| 8 | Sonoma | 700,001–709,999 | 710,001–789,999 | 790,001–799,999 | 4,500,000–5,000,000 | 9,000,000–9,500,000 |
| 9 | Marin | 800,001–809,999 | 810,001–889,999 | 890,001–899,999 | 5,000,000–5,500,000 | 9,500,000–10,000,000 |
| External | 900,001–999,999 | | | | | |

### Node Attributes

The following node attributes are included in the master network.

| Field | Description | Data Type |
|-------|-------------|-----------|
| N | Node Number | Integer (see Node Numbering) |
| X | X coordinate (feet) | Float |
| Y | Y coordinate (feet) | Float |
| OSM_NODE_ID | OpenStreetMap node identifier | Integer |
| COUNTY | County Name | String |
| DRIVE_ACCESS | Node is used by automobile and/or bus links | Boolean |
| WALK_ACCESS | Node is used by pedestrian links | Boolean |
| BIKE_ACCESS | Node is used by bicycle links | Boolean |
| RAIL_ACCESS | Node is used by rail links | Boolean |
| FAREZONE | Unique sequential fare zone ID for transit skimming and assignment | Integer |
| TAP_ID | Transit access point (TAP) associated connected to this node | Integer |

#### External Nodes

| N | Gateway |
|-----|-----------|
| 900001 | State Route 1 (Sonoma) |
| 900002 | State Route 28 (Sonoma) |
| 900003 | U.S. Route 101 (Sonoma) |
| 900004 | State Route 29 (Napa) |
| 900005 | State Route 128 (Solano) |
| 900006 | Interstate 505 (Solano) |
| 900007 | State Route 113 (Solano) |
| 900008 | Interstate 80 (Solano) |
| 900009 | State Route 12 (Solano) |
| 900010 | State Route 160 (Contra Costa) |
| 900011 | State Route 4 (Contra Costa) |
| 900012 | County Route J-4 (Contra Costa) |
| 900013 | Interstate 205 + Interstate 580 (Alameda) |
| 900014 | State Route 152 (Santa Clara/East) |
| 900015 | State Route 156 (Santa Clara) |
| 900016 | State Route 25 (Santa Clara) |
| 900017 | U.S. Route 101 (Santa Clara) |
| 900018 | State Route 152 (Santa Clara/West) |
| 900019 | State Route 17 (Santa Clara) |
| 900020 | State Route 9 (Santa Clara) |
| 900021 | State Route 1 (San Mateo) |

## Transit Network 🚌

Transit network data includes lines, stations, fares, and service attributes.

### Transit Network Attributes

The TM2PY transit network contains **39 total attributes** (22 line + 17 segment):

#### Complete Transit Line Attributes

##### Line Identification
- `#description` - Line description (str)
- `#short_name` - Short name (str) 
- `#mode` - Mode code (int32)
- `#vehtype` - Vehicle type (int32)
- `id` - Line ID string (str)
- `mode` - EMME Mode object

##### Operations and Timing
- `headway` - Service headway in minutes (float)
- `speed` - Operating speed (float)
- `layover_time` - Layover time (float)
- `@orig_hdw` - Original headway (float)

##### Fare and Penalties
- `#faresystem` - Fare system ID (int32)
- `@iboard_penalty` - Initial boarding penalty (float)
- `@xboard_penalty` - Transfer boarding penalty (float)
- `@invehicle_factor` - In-vehicle time factor (float)

##### Service Period
- `#time_period` - Time period (str) - e.g., "AM", "PM"
- `#line_haul_name` - Line haul mode name (str) - e.g., "Local bus"

##### Standard EMME Attributes
- `data1`, `data2`, `data3` - General data fields (float)
- `description` - EMME description field (str)
- `network` - Network reference
- `vehicle` - Transit vehicle reference

#### Complete Transit Segment Attributes

##### Stop Information
- `#stop_name` - Stop name (str) - e.g., "MacArthur BART Station"
- `i_node` - From node (Node)
- `j_node` - To node (Node)

##### Access Control
- `allow_alightings` - Passengers can exit (bool)
- `allow_boardings` - Passengers can board (bool)

##### Timing and Operations
- `dwell_time` - Dwell time at stop (float)
- `@nntime` - Non-negative time (float)
- `transit_time_func` - Transit time function (int)
- `factor_dwell_time_by_length` - Scale dwell by length (bool)

##### Network References
- `id` - Segment ID string (str)
- `line` - Parent transit line (TransitLine)
- `link` - Associated network link (Link)
- `loop_index` - Loop index (int)
- `number` - Segment number (int)

##### Standard EMME Attributes
- `data1`, `data2`, `data3` - General data fields (float)

### Transit Mode Classification

| Code | Mode | Description |
|------|------|-------------|
| b | Local Bus | Local bus service (modes 10-99) |
| e | Express Bus | Express bus service (modes 80-99) |
| l | Light Rail | Light rail transit (modes 110-119) |
| h | Heavy Rail | Heavy rail/subway (modes 120-129) |
| r | Commuter Rail | Commuter rail service (modes 130-139) |
| f | Ferry | Ferry service (modes 100-109) |

### Transit Files

| File | Directory | Description |
|------|-----------|-------------|
| `transitLines.lin` | trn/ | Transit lines definition |
| `station_attribute_data_input.csv` | trn/ | Station attributes |
| `vehtype.pts` | trn/ | Vehicle types |
| `roadway-assignment-names-helper.csv` | trn/ | Names for model links |
| `fareMatrix.txt` | trn/ | Matrix containing transit fares |
| `fares.far` | trn/ | Used to run fare calculations for EMME scenario |

### Legacy TM2.1 Transit Files

| File name | Purpose | Folder location |
|-----------|---------|-----------------|
| `transitFactors_MMMM.fac` | Cube Public Transport (PT) factor files by transit line haul mode | trn/transit_support |
