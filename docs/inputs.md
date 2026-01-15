# Input

## Input File List

The table below contains brief descriptions of the input files required to execute the travel model.

### Current TM2.2+ Files (tm2py)

| Directory | File | Description |
|-----------|------|-------------|
| hwy/ | complete_network.net | Highway, bike, walk network |
| hwy/ | tolls.csv | Contains toll prices for all facilities and all time periods |
| hwy/ | interchange_nodes.csv | Identifies nodes connected to interchanges |
| landuse/ | mazData.csv | Micro zone data |
| landuse/ | tazData.csv | Travel analysis zone data |
| nonres/ | truckFF.dat | Friction factors for the commercial vehicle distribution models |
| nonres/ | truck_kfactors_taz.csv | "K-factors" for the commercial vehicle distribution models |
| nonres/ | ixDaily2015.tpp | Internal-external fixed trip table for year 2015 |
| nonres/ | ixDaily2015_totals.dbf | Internal-external total trips table for year 2015 |
| nonres/ | YYYY_fromtoAAA.csv | Airport passenger fixed trips for year YYYY and airport AAA |
| nonres/ | ixex_config.dbf | Station-specific growth rates and commute shares for each forecast year |
| popsyn/ | households.csv | Synthetic population household file |
| popsyn/ | persons.csv | Synthetic population person file |
| trn/ | transitLines.lin | Transit lines |
| trn/ | station_attribute_data_input.csv | Station attributes |
| trn/ | vehtype.pts | Vehicle types |
| trn/ | roadway-assignment-names-helper.csv | Names for model links |
| trn/ | fareMatrix.txt | Matrix containing transit fares |
| trn/ | fares.far | Used to run fare calculations for EMME scenario |

### Legacy TM2.1 Files (travel-model-two)

| **File name** | **Purpose** | **Folder location** | **File type** | **File format** |
|---------------|-------------|---------------------|---------------|-----------------|
| `mtc_final_network.net` | Highway, bike, walk network | hwy\ | [Citilabs Cube](http://citilabs.com/products/cube)| [Roadway Network](#roadway-network) |
| `truckkfact.k22.z1454.mat` | "K-factors" for the commercial vehicle distribution models | nonres\ | [Citilabs Cube](http://citilabs.com/products/cube) | [Truck Distribution](#truck-distribution) |
| `ixDailyYYYY.tpp` | Internal-external fixed trip table for year YYYY | nonres\ | [Citilabs Cube](http://citilabs.com/products/cube) | [Fixed Demand](#fixed-demand) |
| `IXDaily2006x4.may2208.new` | Internal-external input fixed trip table | nonres\ | [Citilabs Cube](http://citilabs.com/products/cube) | [Fixed Demand](#fixed-demand) |
| `transitFactors_MMMM.fac` | Cube Public Transport (PT) factor files by transit line haul mode MMMM | trn\transit_support | [Citilabs Cube](http://citilabs.com/products/cube) | TransitNetwork |

## Time Periods

Time periods in Travel Model Two are consistent with Travel Model One:

| Time Period | Times | Duration |
|-------------|-------|----------|
| EA (early AM) | 3 am to 6 am | 3 hours |
| AM (AM peak period) | 6 am to 10 am | 4 hours |
| MD (mid-day) | 10 am to 3 pm | 5 hours |
| PM (PM peak period) | 3 pm to 7 pm | 4 hours |
| EV (evening) | 7 pm to 3 am | 8 hours |

## Roadway Network

The all streets highway network, walk network, and bicycle network were developed from [OpenStreetMap](https://www.openstreetmap.org/). The projection is [NAD 1983 StatePlane California VI FIPS 0406 Feet](https://epsg.io/2227).

### County Node Numbering System

The highway network uses a numbering system whereby each county has a reserved block of nodes. Within each county's block:

- Nodes 1 through 9,999 are reserved for TAZs
- Nodes 10,001 through 89,999 are for MAZs

The blocks are assigned to the nine counties per MTC's numbering scheme, as shown in the table below.

Roadway, walk, bicycle, and transit network nodes are numbered by county as well and range from 1,000,000 to 10,000,000 as shown below.

| Code | County | TAZs | MAZs | Network Node | HOV Lane Node |
|------|--------|------|------|------|--------------|----------------|
| 1 | San Francisco | 1–9,999 | 10,001–89,999 | 1,000,000–1,500,000 | 5,500,000–6,000,000 |
| 2 | San Mateo | 100,001–109,999 | 110,001–189,999 | 1,500,000–2,000,000 | 6,000,000–6,500,000 |
| 3 | Santa Clara | 200,001–209,999 | 210,001–289,999 | 2,000,000–2,500,000 | 6,500,000–7,000,000 |
| 4 | Alameda | 300,001–309,999 | 310,001–389,999 | 2,500,000–3,000,000 | 7,000,000–7,500,000 |
| 5 | Contra Costa | 400,001–409,999 | 410,001–489,999 | 3,000,000–3,500,000 | 7,500,000–8,000,000 |
| 6 | Solano | 500,001–509,999 | 510,001–589,999 | 3,500,000–4,000,000 | 8,000,000–8,500,000 |
| 7 | Napa | 600,001–609,999 | 610,001–689,999 | 4,000,000–4,500,000 | 8,500,000–9,000,000 |
| 8 | Sonoma | 700,001–709,999 | 710,001–789,999 | 4,500,000–5,000,000 | 9,000,000–9,500,000 |
| 9 | Marin | 800,001–809,999 | 810,001–889,999 | 5,000,000–5,500,000 | 9,500,000–10,000,000 |
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
