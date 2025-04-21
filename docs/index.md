## Welcome to tm2py

### The python package developed to run Travel Model Two

Currently [a large pile of documentation](https://bayareametro.github.io/travel-model-two/develop/) exists for an earlier incarnation of this project, we need to port that documentation here.
 
Travel Model Two runs with a ***CTRAMP household demand model*** and ***EMME skimming/assignment*** procedures.


Important travel behavior enhancements in Travel Model Two include:

* A much more detailed spatial representation of transportation system supply including an accurate all-streets network for entire 9-county Bay Area, pedestrian paths\sidewalks from OpenStreetMap, bicycle facilities from MTC’s BikeMapper, and transit networks from MTC’s RTD network

* Land-use and demographic forecast integration with Bay Area UrbanSim Two represented at a 40,000 micro-analysis zone (MAZ) level

* Detailed transit access/egress based on actual origin/destinations at the MAZ level considering boarding and alighting at specific transit stops allowing for a more accurate representation of walk times

* More detailed temporal resolution using half-hourly time windows compared to hourly time windows in Travel Model One

* The effects of transit capacity and crowding

* More detailed auto assignments, most notably with the loading of short trips to local streets

* The inclusion of Taxis and Transportation Network Companies (TNCs) such as Uber and Lyft as a mode choice option
* Representation of Automated Vehicles


How do you create and update these pages? [Instructions for deploying GitHub pages with mkdocs](https://www.mkdocs.org/user-guide/deploying-your-docs/)