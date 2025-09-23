# How to Create Base Year Inputs 📊

The current base year is **2023**.

## Synthetic Population 👥👨‍👩‍👧‍👦

### Overview
The 2023 population is synthesized using the code in this GitHub repository: 
[populationsim/tm2/bay_area](https://github.com/BayAreaMetro/populationsim/tree/tm2/bay_area)

### Getting Started
To get started creating the 2023 population, read the README on this page: 
[populationsim/tm2](https://github.com/BayAreaMetro/populationsim/tree/tm2)

### Output Files
When you complete the synthetic population run, there will be **3 files** that need to be moved and copied to your model input directory on Box. 

!!! info "Update Required"
    You will need to update this script with your Box location: 
    [copy_tm2_landuse_files.py](https://github.com/BayAreaMetro/populationsim/blob/tm2/bay_area/copy_tm2_landuse_files.py)

#### Files Generated:

**MAZ Data Files:**
- `maz_data.csv`
- `maz_data_withDensity.csv`

**Population Files:**
- `households_2023_tm2.csv` → renamed to `households.csv`
- `persons_2023_tm2.csv` → renamed to `persons.csv`

## Land Use Data 🏘️

### MAZ Data Files
The MAZ households and persons data comes from the new synthetic population process. 

!!! warning "Network Compatibility"
    We have a new MAZ specification with some of the MAZs merged, so we could run into problems with network mismatch later (because the MAZs on the network haven't been updated).

### Data Processing
This processing happens in the `create_baseyear_controls_23_tm2.py` script.
[View on GitHub](https://github.com/BayAreaMetro/populationsim/tree/tm2)

### Employment Data 💼
The employment data comes from **ESRI Business Analyst**. We needed to figure out a way to quickly get to the needed 23 categories of data. 

!!! note "Data Quality"
    We aren't 100% sure about its quality, but we've done some basic checks. We hope to move to an open source option in the future.

### Other Data Sources 📋
- **Parking data** and other data has not been updated since 2015
- We need to figure out an approach for updating this legacy data

### Data Integration
To combine the updated employment data with other legacy data like enrollment, use this script:
[update_emp.py](https://github.com/BayAreaMetro/tm2py-utils/blob/main/tm2py_utils/inputs/maz_taz/emp/update_emp.py)

## Process Summary 🔄

1. **Generate Synthetic Population** 👥 
   - Run populationsim for 2023 base year
   - Produces households and persons files

2. **Process MAZ Data** 🏘️
   - Combine population data with employment data
   - Integrate with legacy parking and enrollment data

3. **Copy to Model Directory** 📂
   - Move files to Box input directory
   - Rename files according to model conventions

## Detailed Guides 📚

For more detailed information on specific aspects of base year input preparation:

- **[How to Make Base Year Networks](network-base-year.md)** - Comprehensive guide to preparing highway, walk, bike, and transit networks for the base year
- **[How the MAZ Layer Was Updated](maz-updates.md)** - Documentation of the micro-zone layer update process and methodology

