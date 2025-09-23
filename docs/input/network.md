# Network Data 🛣️

!!! info "Network Preparation"
    For information on how to prepare and process network files for the base year, see **[Creating Base Year Inputs](../create-base-year-inputs.md#network-data)** 🛣️

## Roadway Networketwork Data �️

## Roadway Network

The all streets highway network, walk network, and bicycle network were developed from [OpenStreetMap](http://www.openstreetmap.org/). The *projection* is [**NAD 1983 StatePlane California VI FIPS 0406 Feet**](https://epsg.io/102646).

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
