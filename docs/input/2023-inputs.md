# 2023 Base Year Inputs

This page documents the specific input files and data sources used for the 2023 base year model run.

## Overview

The 2023 base year model represents the regional transportation system and travel patterns for calendar year 2023. This includes updated networks, land use data, and demographic information specific to this time period.

**Base Directory**: `Box\Modeling and Surveys\Development\Travel Model Two Conversion\Model Inputs\2023-tm22-dev-version-05`

!!! tip "Starting a New Model Run"
    To set up a new 2023 model run, copy the `2023-tm22-dev-version-05` directory as your starting point.

## Population Synthesis

### Overview
The synthetic population for 2023 is generated using PopulationSim, which creates a microsimulation of households and persons that match control totals from various data sources while maintaining realistic distributions of demographic characteristics.

### Running PopulationSim

- **Code Repository**: [github.com/BayAreaMetro/populationsim (tm2 branch)](https://github.com/BayAreaMetro/populationsim/tree/tm2)
- **Documentation**: [bayareametro.github.io/populationsim](https://bayareametro.github.io/populationsim/)
- **Runtime**: Approximately 6 hours

### Output Files

**Location**: `Box\Modeling and Surveys\Development\Travel Model Two Conversion\Model Inputs\populationsim\bay_area\output_2023\populationsim_working_dir\output`

The population synthesis process generates two primary output files:

- `households_2023_tm2.csv` - Household records with necessary encoding for TM2 CTRAMP
- `persons_2023_tm2.csv` - Person records with necessary encoding for TM2 CTRAMP

The complete run directory also includes validation summaries and diagnostic outputs.

### Data Quality Notes

!!! warning "Known Considerations"
    - **Zero-vehicle ownership**: The uncontrolled zero-vehicle ownership rates may appear high compared to American Community Survey (ACS) estimates
    - **Validated distributions**: Household income and household size distributions have been validated and appear reasonable

## Land Use Data

### MAZ Data File

**File**: `maz_data_withDensity.csv`

This consolidated MAZ (Micro Analysis Zone) data file contains:

- **Synthetic population data** - From PopulationSim output
- **Employment data** - By industry sector
- **Enrollment data** - School enrollment
- **Parking data** - Costs and availability
- **Density metrics** - Calculated from network analysis

### Data Processing Scripts

#### Employment Data Update
Updates MAZ employment data while preserving other attributes like enrollment.

- **Script**: [update_emp.py](https://github.com/BayAreaMetro/tm2py-utils/blob/main/tm2py_utils/inputs/maz_taz/emp/update_emp.py)
- **Repository**: tm2py-utils

!!! note "Future Enhancement"
    This script will need to be updated to correctly perform calculations when real (non-placeholder) enrollment and other data become available.

#### Population Data Integration
Transfers PopulationSim outputs to the MAZ/TAZ land use files.

- **Script**: [copy_tm2_landuse_files.py](https://github.com/BayAreaMetro/populationsim/blob/tm2/bay_area/copy_tm2_landuse_files.py)
- **Repository**: populationsim (tm2 branch)

#### Density Calculations
Network-based density metrics are calculated using scripts adapted from Travel Model Two (legacy).

This needs to be rewritten.

**Source References**:
- [maz_densities.job](https://github.com/BayAreaMetro/travel-model-two/blob/562dd7da3d8c10743fcc1e99b6aa4f9aa33f0e8a/model-files/scripts/preprocess/maz_densities.job#L10)
- [createMazDensityFile.py](https://github.com/BayAreaMetro/travel-model-two/blob/562dd7da3d8c10743fcc1e99b6aa4f9aa33f0e8a/model-files/scripts/preprocess/createMazDensityFile.py)

**Required Input Files**:
- `hwy\maz_nodes.csv` - MAZ centroid nodes
- `hwy\intersection_nodes.csv` - Network intersection nodes

## Related Documentation

- [General Input File Documentation](../inputs.md)
- [Population Synthesis Documentation](https://bayareametro.github.io/populationsim/)
- [Land Use Data Overview](landuse.md)

---

*Last Updated: January 27, 2026*