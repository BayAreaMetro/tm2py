# Input Files

## Overview

The CT-RAMP travel demand model requires comprehensive input data covering transportation networks, land use patterns, demographic characteristics, and travel behavior. This page provides an overview of all required input files and their organization.

## Input File List

The table below contains brief descriptions of the input files required to execute the travel model.

| **Directory** | **File** | **Purpose** | **File Type** | **Documentation** |
|---------------|----------|-------------|---------------|------------------|
| **Highway Network** |||||
| hwy\ | `complete_network.net` | Highway, bike, walk network | Citilabs Cube | [Roadway Network](network.md#roadway-network) |
| hwy\ | `tolls.csv` | Toll prices for all facilities and time periods | CSV | [Tolls](network.md#tolls) |
| hwy\ | `interchange_nodes.csv` | Nodes connected to interchanges | CSV | [Interchanges](network.md#interchanges) |
| **Land Use Data** |||||
| landuse\ | `mazData.csv` | Micro Analysis Zone data | CSV | [Micro Zonal Data](landuse.md#micro-analysis-zones-maz-data) |
| landuse\ | `tazData.csv` | Traffic Analysis Zone data | CSV | [Zonal Data](landuse.md#traffic-analysis-zones-taz-data) |
| **Population Data** |||||
| popsyn\ | `households.csv` | Synthetic population household file | CSV | [Households](population.md#households) |
| popsyn\ | `persons.csv` | Synthetic population person file | CSV | [Persons](population.md#persons) |
| **Transit Network** |||||
| trn\ | `transitLines.lin` | Transit lines | Citilabs Cube | [Transit Network](transit.md#transit-network) |
| trn\ | `station_attribute_data_input.csv` | Station attributes | CSV | [Transit Stations](transit.md#station-attributes) |
| trn\ | `vehtype.pts` | Vehicle types | Citilabs Cube | [Vehicle Types](transit.md#vehicle-types) |
| trn\ | `fareMatrix.txt` | Transit fare matrix | Text | [Transit Fares](transit.md#transit-fares) |
| trn\ | `fares.far` | EMME fare calculations | EMME | [Transit Fares](transit.md#transit-fares) |
| **Commercial Vehicle Data** |||||
| nonres\ | `truckFF.dat` | Friction factors for truck distribution | ASCII | [Truck Distribution](commercial.md#truck-distribution) |
| nonres\ | `truck_kfactors_taz.csv` | K-factors for truck distribution | CSV | [Truck Distribution](commercial.md#truck-distribution) |
| **Fixed Demand** |||||
| nonres\ | `ixDaily2015.tpp` | Internal-external fixed trip table | Citilabs Cube | [Fixed Demand](demand.md#fixed-demand) |
| nonres\ | `ixDaily2015_totals.dbf` | Internal-external total trips | DBF | [Fixed Demand](demand.md#fixed-demand) |
| nonres\ | `YYYY_fromtoAAA.csv` | Airport passenger fixed trips | CSV | [Fixed Demand](demand.md#fixed-demand) |
| nonres\ | `ixex_config.dbf` | Station growth rates and commute shares | DBF | [Fixed Demand](demand.md#fixed-demand) |

## Time Periods

Time periods in Travel Model Two are consistent with Travel Model One:

| **Time Period** | **Times** | **Duration** |
|-----------------|-----------|--------------|
| EA (early AM) | 3 am to 6 am | 3 hours |
| AM (AM peak period) | 6 am to 10 am | 4 hours |
| MD (midday) | 10 am to 3 pm | 5 hours |
| PM (PM peak period) | 3 pm to 7 pm | 4 hours |
| EV (evening) | 7 pm to 3 am | 8 hours |

## Categories

The input files are organized into the following categories:

- [**Network Data**](network.md) 🛣️ - Highway and roadway network information
- [**Transit Data**](transit.md) 🚌 - Transit lines, modes, and fares  
- [**Land Use Data**](landuse.md) 🏘️ - Zonal and micro-zonal land use information
- [**Population Data**](population.md) 👥 - Synthetic population households and persons
- [**Commercial Vehicle Data**](commercial.md) 🚛 - Truck distribution models
- [**Fixed Demand Data**](demand.md) ✈️ - Internal/external and air passenger demand

For detailed information about each category, please see the individual pages linked above.

## Creating Base Year Input Files

For guidance on how to create and prepare many of these input files for the base year, see:

📋 **[Creating Base Year Inputs](../create-base-year-inputs.md)** - Step-by-step guide for generating base year input files, including synthetic population, land use data, and network preparation.
