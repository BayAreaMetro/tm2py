# Land Use Data 🏙️

## Overview

Land use data provides the spatial distribution of households, population, employment, and other activities that generate and attract travel. The travel model uses two levels of spatial detail: Traffic Analysis Zones (TAZ) for regional analysis and Micro Analysis Zones (MAZ) for detailed local accessibility and mode choice modeling.

!!! info "Creating Land Use Files"
    For detailed instructions on how to prepare land use data files for the base year, see **[Creating Base Year Inputs](../create-base-year-inputs.md#land-use-data)** 🏙️

## File Structure

Land use data consists of two main files located in the `landuse\` directory:

- **`mazData.csv`** - Micro Analysis Zone level data (detailed land use characteristics)
- **`tazData.csv`** - Traffic Analysis Zone level data (regional characteristics)

## Micro Analysis Zones (MAZ Data)

The `mazData.csv` file contains detailed land use characteristics at the micro-zone level, providing the fine-grained spatial detail needed for accessibility calculations and local travel modeling.

!!! tip "Data Model Validation"
    The MAZ data file structure is validated using Pandera data models to ensure data quality and consistency. See the [complete field specifications and validation rules](#maz-data-model-specification) below.

!!! info "Cross Reference"
    For detailed API documentation and programmatic access to MAZ data validation, see [MAZ Data Model API Reference](../api.md#tm2py.data_models.maz_data.MAZData) 📖

### MAZ Data Model Specification

The following fields are required in the `mazData.csv` file. All field names are **case-sensitive** and must match exactly:

### Zone ID Naming Convention

TM2 uses a standardized naming convention for zone identifiers to distinguish between sequential IDs (for matrix operations) and network node IDs (for geographic referencing):

| Column Name | Description | Example Values |
|-------------|-------------|----------------|
| `MAZ_SEQ` | Sequential MAZ ID (1-based, for matrix indexing) | 1, 2, 3, ... |
| `TAZ_SEQ` | Sequential TAZ ID (1-based, for matrix indexing) | 1, 2, 3, ... |
| `MAZ_NODE` | Network node ID for MAZ centroid | 10001, 110002, 210003, ... |
| `TAZ_NODE` | Network node ID for TAZ centroid | 1, 100001, 200001, ... |

!!! note "Backward Compatibility"
    The data loader automatically maps older column names to the new convention:
    
    - `MAZ` → `MAZ_SEQ`
    - `TAZ` → `TAZ_SEQ`
    - `MAZ_ORIGINAL` → `MAZ_NODE`
    - `TAZ_ORIGINAL` → `TAZ_NODE`

::: tm2py.data_models.maz_data.MAZData
    options:
      show_root_heading: true
      show_root_toc_entry: false
      heading_level: 4
      show_bases: false
      show_source: true
      members_order: source
      group_by_category: true
      show_signature_annotations: true
      separate_signature: true
      docstring_section_style: table
      show_object_full_path: false
      show_symbol_type_heading: true
      show_symbol_type_toc: true
      filters: ["!^_", "!^Config"]
      extra:
        show_attributes: true
      members:
        - MAZ_SEQ
        - TAZ_SEQ
        - MAZ_NODE
        - TAZ_NODE
        - DistID
        - DistName
        - CountyID
        - CountyName
        - ACRES
        - HH
        - POP
        - ag
        - art_rec
        - constr
        - eat
        - ed_high
        - ed_k12
        - ed_oth
        - fire
        - gov
        - health
        - hotel
        - info
        - lease
        - logis
        - man_bio
        - man_lgt
        - man_hvy
        - man_tech
        - natres
        - prof
        - ret_loc
        - ret_reg
        - serv_bus
        - serv_pers
        - serv_soc
        - transp
        - util
        - emp_total
        - publicEnrollGradeKto8
        - privateEnrollGradeKto8
        - publicEnrollGrade9to12
        - privateEnrollGrade9to12
        - comm_coll_enroll
        - EnrollGradeKto8
        - EnrollGrade9to12
        - collegeEnroll
        - otherCollegeEnroll
        - AdultSchEnrl
        - hstallsoth
        - hstallssam
        - dstallsoth
        - dstallssam
        - mstallsoth
        - mstallssam
        - park_area
        - hparkcost
        - numfreehrs
        - dparkcost
        - mparkcost
        - ech_dist
        - hch_dist
        - parkarea
        - TERMINAL
        - MAZ_X
        - MAZ_Y
        - TotInt
        - EmpDen
        - RetEmpDen
        - DUDen
        - PopDen
        - IntDenBin
        - EmpDenBin
        - DuDenBin
        - PopEmpDenPerMi

## Traffic Analysis Zones (TAZ Data)

The `tazData.csv` file contains zone-level data used for specific model components, particularly the transponder ownership model.

### Required Fields

| Column Name | Description | Used by | Source |
|-------------|-------------|---------|--------|
| `TAZ_NODE` | Network node ID for TAZ centroid (see [Zone ID Naming Convention](#zone-id-naming-convention)) | | Zone system definition |
| `AVGTTS` | Average travel time savings for transponder ownership | [TazDataManager] | Highway network analysis |
| `DIST` | Distance for transponder ownership model | [TazDataManager] | Highway network analysis |
| `PCTDETOUR` | Percent detour for transponder ownership model | [TazDataManager] | Highway network analysis |
| `TERMINALTIME` | Terminal time | [TazDataManager] | Highway network analysis |

## Data Integration and Processing

### Zone System Coordination

- **MAZ to TAZ Mapping**: Each MAZ must be assigned to exactly one TAZ
- **Numbering Convention**: Original numbers preserved, but model renumbers zones during execution
- **Consistency Checks**: Population and employment totals must be consistent between MAZ and TAZ levels

### Employment Allocation

1. **Industry Classification**: Employment data classified by detailed NAICS codes
2. **Spatial Distribution**: Employment allocated to MAZ level for accessibility calculations
3. **Validation**: Total employment should match regional control totals
4. **Special Generators**: Major employers (airports, universities) require special treatment

### Density Calculations

Density measures calculated using [TBD](https://github.com/BayAreaMetro/tm2py/pull/216):

- **Dwelling Unit Density**: Households per acre
- **Employment Density**: Jobs per acre  
- **Population Density**: Persons per acre
- **Intersection Density**: Total intersections (walkability measure)

## Model Applications

### Accessibility Calculations

Land use data drives accessibility calculations used throughout the model:

- **Employment Accessibility**: By industry sector for location choice
- **Population Accessibility**: For service and retail accessibility
- **Education Accessibility**: For school location choice
- **Mixed-Use Measures**: Combined residential/commercial accessibility

### Mode Choice Integration

- **Parking Supply**: Available spaces by type and duration
- **Parking Costs**: Hourly, daily, and monthly rates
- **Built Environment**: Density measures for walk/bike mode choice
- **Activity Density**: Combined employment and population measures

### Location Choice Models

- **Work Location**: Industry-specific employment accessibility
- **School Location**: Enrollment and capacity by education level
- **Non-Mandatory Activities**: Retail, service, and recreational accessibility

## Data Quality Requirements

### Validation Checks

1. **Completeness**: No missing values in required fields
2. **Consistency**: Employment totals match across classification levels
3. **Geographic Integrity**: All MAZ assigned to valid TAZ
4. **Logical Relationships**: Enrollment consistent with education employment
5. **Density Calculations**: Consistent with zone area measurements

### Common Issues

- **Missing Employment**: Zones with population but no employment data
- **Inconsistent Totals**: MAZ totals not matching TAZ aggregations  
- **Parking Data Gaps**: Missing parking supply or cost information
- **Enrollment Mismatches**: School enrollment not aligned with capacity
- **Density Anomalies**: Unrealistic density calculations

### Update Procedures

1. **Base Year Preparation**: Align with most recent Census/survey data
2. **Forecast Year Development**: Apply land use forecasts and development scenarios
3. **Validation Process**: Compare against observed patterns and trends
4. **Sensitivity Testing**: Verify model response to land use changes
5. **Documentation**: Maintain metadata and processing documentation

This comprehensive land use data structure supports detailed spatial analysis and realistic travel behavior modeling in the CT-RAMP framework.

[Accessibilities]: https://github.com/BayAreaMetro/travel-model-two/blob/master/uec/Accessibilities.xls
[AutoOwnership]: https://github.com/BayAreaMetro/travel-model-two/blob/master/uec/AutoOwnership.xls
[MgraDataManager]: https://github.com/BayAreaMetro/travel-model-two/blob/master/src/java/com/pb/mtctm2/abm/ctramp/MgraDataManager.java#L47
[NAICS]: https://www.census.gov/eos/www/naics/
[TazDataManager]: https://github.com/BayAreaMetro/travel-model-two/blob/master/src/java/com/pb/mtctm2/abm/ctramp/TazDataManager.java#L37
[TourModeChoice.xls]: https://github.com/BayAreaMetro/travel-model-two/blob/master/uec/TourModeChoice.xls