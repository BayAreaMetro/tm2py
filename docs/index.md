# Travel Model Two

## Welcome to tm2py 🎂

### The python package developed to run Travel Model Two

Travel Model Two, currently under development, is an important extension of Travel Model One. Fundamental to the foundation of Travel Model Two is the use of a wide ranging set of on-board transit rider surveys as well as the California Household travel survey, which obtained a statistical larger sample in the nine-county Bay Area.

Travel Model Two runs with a ***CTRAMP household demand model*** and ***EMME skimming/assignment*** procedures.

## Changes from Travel Model One

Important travel behavior enhancements in Travel Model Two include:

* A much more detailed spatial representation of transportation system supply including an accurate all-streets network for entire 9-county Bay Area, pedestrian paths\sidewalks from OpenStreetMap, bicycle facilities from MTC's BikeMapper, and transit networks from MTC's RTD network
* Land-use and demographic forecast integration with [Bay Area UrbanSim Two](https://github.com/BayAreaMetro/bayarea_urbansim) represented at a 40,000 micro-analysis zone (MAZ) level
* Detailed transit access/egress based on actual origin/destinations at the MAZ level considering boarding and alighting at specific transit stops allowing for a more accurate representation of walk times
* More detailed temporal resolution using half-hourly time windows compared to hourly time windows in Travel Model One
* The effects of transit capacity and crowding
* More detailed auto assignments, most notably with the loading of short trips to local streets
* The inclusion of Taxis and Transportation Network Companies (TNCs) such as Uber and Lyft as a mode choice option
* Representation of Automated Vehicles

## Versions

* **TM2.0**: Initial TM2 with Cube, CTRAMP core, 3-zone system. In use by TAM (Marin)
* **TM2.1**: TM2 with transit CCR implemented in Emme (uses Cube and Emme), CTRAMP core. Anticipated release: Summer 2022. This work is being performed in the branch [transit-ccr](https://github.com/BayAreaMetro/travel-model-two/tree/transit-ccr)
* **TM2.2**: TM2 with Emme only (not Cube). CTRAMP core. This work is being performed in the [tm2py repository](https://github.com/BayAreaMetro/tm2py) -- **these docs are for this version**
* **TM2.3**: TM2 with Emme only and ActivitySim core.

## Documentation Sections

* [Architecture](architecture.md) - System design and components
* [Installation](install.md) - Setup and installation instructions
* [Input](inputs.md) - Input data requirements and formats
* [Creating Base Year Inputs](create-base-year-inputs.md) - How to generate base year input files
* [Run](run.md) - How to execute the model
* [Outputs](outputs.md) - Model outputs and analysis
* [Network Summary Component](network_summary.md) - Comprehensive network analysis component
* [API](api.md) - Programming interface documentation
* [Server Setup](server-setup.md) - Server configuration

### Legacy Documentation Sections

* [Guide](guide.md) - Detailed user guide for TM2.1
* [Process](process.md) - Model process documentation
* [Geographies](geographies.md) - Geographic data information
* [Papers](papers.md) - Research papers and publications
* [Network QA](network_qa.md) - Network quality assurance

## References

* [Travel Model One documentation wiki](https://github.com/BayAreaMetro/modeling-website/wiki/TravelModel)
* [Travel Model One github repo](https://github.com/BayAreaMetro/travel-model-one)
* [Legacy TM2 documentation](https://bayareametro.github.io/travel-model-two/develop/)

## Contributing 🎂

How do you create and update these pages? See [Contributing/Documentation](contributing/documentation/)
