# Input Files

## Input File List

The table below contains brief descriptions of the input files required to execute the travel model. 

| **File name** | **Purpose** | **Folder location** | **File type** | **File format** |
|---------------|-------------|---------------------|---------------|-----------------|
| `mtc_final_network.net` | Highway, bike, walk network | hwy\ | [Citilabs Cube](http://citilabs.com/products/cube)| [Roadway Network](network.md#roadway-network) |
| `mazData.csv` | Micro zone data  | landuse\ | CSV | [Micro Zonal Data](landuse.md#micro-zonal-data) |
| `tazData.csv` | Travel analysis zone data | landuse\ | CSV | [Zonal Data](landuse.md#zonal-data) |
| `truckFF.dat` | Friction factors for the commercial vehicle distribution models | nonres\ | ASCII | [Truck Distribution](commercial.md#truck-distribution) |
| `truckkfact.k22.z1454.mat` | "K-factors" for the commercial vehicle distribution models | nonres\ | [Citilabs Cube](http://citilabs.com/products/cube) | [Truck Distribution](commercial.md#truck-distribution) |
| `truck_kfactors_taz.csv` | "K-factors" for the commercial vehicle distribution models | nonres\ | CSV | [Truck Distribution](commercial.md#truck-distribution) |
| `ixDailyYYYY.tpp` | Internal-external fixed trip table for year YYYY | nonres\ | [Citilabs Cube](http://citilabs.com/products/cube) | [Fixed Demand](demand.md#fixed-demand) |
| `IXDaily2006x4.may2208.new` | Internal-external input fixed trip table | nonres\ | [Citilabs Cube](http://citilabs.com/products/cube) | [Fixed Demand](demand.md#fixed-demand) |
|  `YYYY_fromtoAAA.csv` |  Airport passenger fixed trips for year YYYY and airport AAA  | nonres\ | CSV | [Fixed Demand](demand.md#fixed-demand) |
| `households.csv` | Synthetic population household file | popsyn\ | CSV | [Synthetic Population](population.md#households) |
| `persons.csv` | Synthetic population person file | popsyn\ | CSV | [Synthetic Population](population.md#persons) |
| `transitLines.lin` | Transit lines | trn\transit_lines | [Citilabs Cube](http://citilabs.com/products/cube)| [Transit Network](transit.md#transit-network) |
| `transitFactors_MMMM.fac` | Cube Public Transport (PT) factor files by transit line haul mode MMMM | trn\transit_support | [Citilabs Cube](http://citilabs.com/products/cube) | [Transit Network](transit.md#transit-network) |

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
