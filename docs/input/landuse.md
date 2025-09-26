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

### Geographic and Administrative Fields

| Column Name | Description | Used by | Source |
|-------------|-------------|---------|--------|
| `MAZ_ORIGINAL` | Original micro zone number (renumbered during model run) | [MgraDataManager] | Zone system definition |
| `TAZ_ORIGINAL` | Original TAZ number (renumbered during model run) | [MgraDataManager] | Zone system definition |
| `CountyID` | County ID number | [MgraDataManager] | Administrative boundaries |
| `CountyName` | County name string | | Administrative boundaries |
| `DistID` | District ID number | [TourModeChoice.xls] | District system definition |
| `DistName` | District name | | District system definition |
| `ACRES` | MAZ area in acres | [createMazDensityFile.py] | Calculated from shapefile |

### Population and Household Data

| Column Name | Description | Used by | Source |
|-------------|-------------|---------|--------|
| `HH` | Total number of households | [MgraDataManager] | PopulationSim allocation |
| `POP` | Total population | [MgraDataManager] | PopulationSim allocation |

### Employment by Industry Category

The model uses detailed employment data by industry sector based on [NAICS] codes:

#### Core Industries

| Column Name | Description | NAICS Codes | Used by |
|-------------|-------------|-------------|---------|
| `ag` | Agriculture, forestry, fishing | 11 | [Accessibilities] |
| `const` | Construction | 23 | [Accessibilities] |
| `natres` | Mining and resource extraction | 21 | [Accessibilities] |
| `util` | Utilities | 22 | [Accessibilities] |

#### Manufacturing

| Column Name | Description | NAICS Codes | Used by |
|-------------|-------------|-------------|---------|
| `man_lgt` | Light manufacturing | 31-33 subset | [Accessibilities] |
| `man_hvy` | Heavy manufacturing | 31-33 subset | [Accessibilities] |
| `man_tech` | High-tech manufacturing | 334 | [Accessibilities] |
| `man_bio` | Biological/drug manufacturing | 325411, 325412, 325313, 325414 | [Accessibilities] |

#### Trade, Transportation, and Utilities

| Column Name | Description | NAICS Codes | Used by |
|-------------|-------------|-------------|---------|
| `ret_loc` | Local-serving retail | 444130, 444190, 444210, 444220, 445110, 445120, 445210, 445220, 445230, 445291, 445292, 445299, 445310, 446110, 446120, 446130, 446191, 446199, 447110, 447190, 448110, 448120, 448130, 448140, 448150, 448190, 448210, 448310, 448320, 451110, 451120, 451130, 451140, 451211, 451212, 452910, 452990, 453110, 453220, 453310, 453910, 453920, 453930, 453991, 453998, 454111, 454112, 454113 | [Accessibilities] |
| `ret_reg` | Regional retail | 441110, 441120, 441210, 441222, 441228, 441310, 441320, 442110, 442210, 442291, 442299, 443141, 443142, 444110, 444120, 452111, 452112, 453210, 454210, 454310, 454390 | [Accessibilities] |
| `transp` | Transportation | 48 (most), 49 (excluding logistics) | [Accessibilities] |
| `logis` | Logistics/warehousing and distribution | 42, 493 | [Accessibilities] |

#### Information and Professional Services

| Column Name | Description | NAICS Codes | Used by |
|-------------|-------------|-------------|---------|
| `info` | Information-based services | 51 | [Accessibilities] |
| `fire` | Finance, insurance, real estate | 52, 53 (excluding leasing) | [Accessibilities] |
| `lease` | Leasing | 532 | [Accessibilities] |
| `prof` | Professional and technical services | 54 | [Accessibilities] |

#### Education and Health Services

| Column Name | Description | NAICS Codes | Used by |
|-------------|-------------|-------------|---------|
| `ed_k12` | K-12 schools | 6111 | [Accessibilities] |
| `ed_high` | Junior colleges, colleges, universities | 6112, 6113, 6114, 6115 | [Accessibilities] |
| `ed_oth` | Other schools, libraries, educational services | 6116, 6117 | [Accessibilities] |
| `health` | Health care | 62 (excluding social services) | [Accessibilities] |

#### Leisure, Hospitality, and Other Services

| Column Name | Description | NAICS Codes | Used by |
|-------------|-------------|-------------|---------|
| `art_rec` | Arts, entertainment, recreation | 71 | [Accessibilities] |
| `hotel` | Hotels and other accommodations | 721 | [Accessibilities] |
| `eat` | Food services and drinking places | 722 | [Accessibilities] |
| `serv_pers` | Personal and other services | 53, 81 | [Accessibilities] |
| `serv_bus` | Managerial, administrative, business services | 55, 56 | [Accessibilities] |
| `serv_soc` | Social services and childcare | 624 | [Accessibilities] |

#### Government and Other

| Column Name | Description | NAICS Codes | Used by |
|-------------|-------------|-------------|---------|
| `gov` | Government | 92 | [Accessibilities] |
| `unclass` | Employment not classified | N/A | |
| `emp_total` | Total employment | Sum of all categories | [Accessibilities] |

### School Enrollment Data

| Column Name | Description | Used by | Source |
|-------------|-------------|---------|--------|
| `EnrollGradeKto8` | Elementary school (K-8) enrollment | [MgraDataManager] | School district data |
| `EnrollGrade9to12` | High school (9-12) enrollment | [MgraDataManager] | School district data |
| `collegeEnroll` | Major college enrollment | [MgraDataManager] | Higher education institutions |
| `otherCollegeEnroll` | Other college enrollment | [MgraDataManager] | Community colleges, trade schools |
| `AdultSchEnrl` | Adult school enrollment | [MgraDataManager] | Continuing education programs |
| `ech_dist` | Elementary school district | [MgraDataManager] | School district boundaries |
| `hch_dist` | High school district | [MgraDataManager] | School district boundaries |

### Parking Data

Detailed parking supply and cost information for mode choice modeling:

| Column Name | Description | Used by | Source |
|-------------|-------------|---------|--------|
| `parkarea` | Parking area type (see codes below) | [MgraDataManager] | Parking inventory |
| `hstallsoth` | Hourly stalls for trips to other MAZs | [MgraDataManager] | Parking inventory |
| `hstallssam` | Hourly stalls for trips within same MAZ | [MgraDataManager] | Parking inventory |
| `hparkcost` | Average hourly parking cost (dollars) | [MgraDataManager] | Parking fee data |
| `numfreehrs` | Hours of free parking before charges begin | [MgraDataManager] | Parking regulations |
| `dstallsoth` | Daily stalls for trips to other MAZs | [MgraDataManager] | Parking inventory |
| `dstallssam` | Daily stalls for trips within same MAZ | [MgraDataManager] | Parking inventory |
| `dparkcost` | Average daily parking cost (dollars) | [MgraDataManager] | Parking fee data |
| `mstallsoth` | Monthly stalls for trips to other MAZs | [MgraDataManager] | Parking inventory |
| `mstallssam` | Monthly stalls for trips within same MAZ | [MgraDataManager] | Parking inventory |
| `mparkcost` | Monthly parking cost amortized over 22 workdays | [MgraDataManager] | Parking fee data |

#### Parking Area Type Codes (`parkarea`)

| Code | Description |
|------|-------------|
| 1 | Downtown: trips may park in different MAZ, charges apply |
| 2 | Downtown buffer: quarter-mile buffer, charges might apply |
| 3 | Outside downtown paid: only destination trips park here, charges apply |
| 4 | Outside downtown free: only destination trips park here, no charges |

### Built Environment and Density Measures

| Column Name | Description | Used by | Source |
|-------------|-------------|---------|--------|
| `TotInt` | Total intersections | [MgraDataManager], [AutoOwnership] | [createMazDensityFile.py] |
| `DUDen` | Dwelling unit density | [MgraDataManager] | [createMazDensityFile.py] |
| `EmpDen` | Employment density | [MgraDataManager] | [createMazDensityFile.py] |
| `PopDen` | Population density | | [createMazDensityFile.py] |

## Traffic Analysis Zones (TAZ Data)

The `tazData.csv` file contains zone-level data used for specific model components, particularly the transponder ownership model.

### Required Fields

| Column Name | Description | Used by | Source |
|-------------|-------------|---------|--------|
| `TAZ_ORIGINAL` | Original TAZ number (renumbered during model run) | | Zone system definition |
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

Density measures calculated using [createMazDensityFile.py]:

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

[Accessibilities]: https://github.com/BayAreaMetro/travel-model-two/blob/master/model-files/model/Accessibilities.xls
[AutoOwnership]: https://github.com/BayAreaMetro/travel-model-two/blob/master/model-files/model/AutoOwnership.xls
[createMazDensityFile.py]: https://github.com/BayAreaMetro/travel-model-two/blob/master/model-files/scripts/preprocess/createMazDensityFile.py
[MgraDataManager]: https://github.com/BayAreaMetro/travel-model-two/blob/master/core/src/java/com/pb/mtctm2/abm/ctramp/MgraDataManager.java#L47
[NAICS]: https://www.census.gov/eos/www/naics/
[TazDataManager]: https://github.com/BayAreaMetro/travel-model-two/blob/master/core/src/java/com/pb/mtctm2/abm/ctramp/TazDataManager.java#L37
[TourModeChoice.xls]: https://github.com/BayAreaMetro/travel-model-two/blob/master/model-files/model/TourModeChoice.xls