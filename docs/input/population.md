# Population Data 👥

## Overview

The synthetic population serves as the foundation for the CT-RAMP activity-based travel demand model. It represents the demographic and socio-economic characteristics of all households and persons in the modeling region, providing the basis for travel behavior simulation.

## Synthetic Population Generation

The synthetic population is generated using [PopulationSim](https://github.com/BayAreaMetro/populationsim), which creates a statistically representative synthetic population that matches observed demographic distributions while protecting individual privacy.

!!! info "Creating Synthetic Population Files"
    For detailed instructions on how to generate synthetic population files for the base year, see **[Creating Base Year Inputs](../create-base-year-inputs.md#synthetic-population)** 👥👨‍👩‍👧‍👦

### Key Features
- **Statistical Matching**: Matches marginal distributions from Census data
- **Spatial Allocation**: Assigns households to specific geographic zones (MAZ level)
- **Privacy Protection**: Creates synthetic records rather than using actual Census records
- **Model Integration**: Designed specifically for activity-based travel modeling needs

## Input File Structure

The synthetic population consists of two main files that must be located in the `popsyn\` directory:

- **`households.csv`** - Household-level characteristics and location
- **`persons.csv`** - Individual person characteristics within households

## Households (`households.csv`)

Contains household-level characteristics including location, income, size, and structure.

### Required Fields

| Column Name | Description | Used by | Data Source/Notes |
|-------------|-------------|---------|-------------------|
| `HHID` | Unique household ID | [HouseholdDataManager] | Primary key, must be unique across all households |
| `TAZ` | TAZ of residence | [HouseholdDataManager] | Traffic Analysis Zone (1454 zones in Bay Area) |
| `MAZ` | MAZ of residence | [HouseholdDataManager] | Micro Analysis Zone (more detailed than TAZ) |
| `MTCCountyID` | County of residence | [HouseholdDataManager] | MTC county identifier code |
| `HHINCADJ` | Household income in 2010 dollars | [HouseholdDataManager] | Inflation-adjusted income for consistent modeling |
| `NWRKRS_ESR` | Number of workers | [HouseholdDataManager] | Count of employed persons, ranges 0-20 |
| `VEH` | Number of vehicles owned | [HouseholdDataManager] | From PUMS: 0-6 vehicles, -9 for group quarters |
| `NP` | Number of persons in household | [HouseholdDataManager] | From PUMS: ranges 1-20 persons |
| `HHT` | Household type | [HouseholdDataManager] | PUMS household type classification |
| `BLD` | Units in structure | [HouseholdDataManager] | PUMS building type classification |
| `TYPE` | Type of unit | [HouseholdDataManager] | Housing unit vs. group quarters |

### Household Type Codes (`HHT`)

Based on [PUMS](https://www2.census.gov/programs-surveys/acs/tech_docs/pums/data_dict/PUMS_Data_Dictionary_2007-2011.pdf) classification:

| Code | Description |
|------|-------------|
| 1 | Married-couple family household |
| 2 | Other family household, Male householder, no wife present |
| 3 | Other family household, Female householder, no husband present |
| 4 | Nonfamily household, Male householder, Living alone |
| 5 | Nonfamily household, Male householder, Not living alone |
| 6 | Nonfamily household, Female householder, Living alone |
| 7 | Nonfamily household, Female householder, Not living alone |
| -9 | N/A recoded for group quarters |

### Building Type Codes (`BLD`)

Based on PUMS units in structure classification:

| Code | Description |
|------|-------------|
| 1 | Mobile home or trailer |
| 2 | One-family house detached |
| 3 | One-family house attached |
| 4 | 2 Apartments |
| 5 | 3-4 Apartments |
| 6 | 5-9 Apartments |
| 7 | 10-19 Apartments |
| 8 | 20-49 Apartments |
| 9 | 50 or more apartments |
| 10 | Boat, RV, van, etc. |
| -9 | N/A recoded for group quarters |

### Unit Type Codes (`TYPE`)

| Code | Description |
|------|-------------|
| 1 | Housing unit |
| 2 | Institutional group quarters (typically excluded) |
| 3 | Noninstitutional group quarters |

## Persons (`persons.csv`)

Contains individual-level characteristics for all persons within households.

### Required Fields

| Column Name | Description | Used by | Data Source/Notes |
|-------------|-------------|---------|-------------------|
| `HHID` | Unique household ID | [HouseholdDataManager] | Foreign key linking to households table |
| `PERID` | Unique person ID | [HouseholdDataManager] | Primary key, unique across all persons |
| `AGEP` | Age of person | [HouseholdDataManager] | From PUMS: ranges 0-99 years |
| `SEX` | Sex of person | [HouseholdDataManager] | From PUMS: 1=Male, 2=Female |
| `SCHL` | Educational attainment | [HouseholdDataManager] | PUMS education level codes |
| `OCCP` | Occupation category | [HouseholdDataManager] | Recoded from PUMS occupation data |
| `WKHP` | Usual hours worked per week | [HouseholdDataManager] | From PUMS: -9 if N/A, otherwise 1-99 |
| `WKW` | Weeks worked during past 12 months | [HouseholdDataManager] | PUMS work weeks classification |
| `EMPLOYED` | Employment status | [HouseholdDataManager] | Derived from ESR: 1=Employed, 0=Unemployed |
| `ESR` | Employment status recode | [HouseholdDataManager] | PUMS employment status |
| `SCHG` | Grade level attending | [HouseholdDataManager] | PUMS school attendance level |

### Educational Attainment Codes (`SCHL`)

Based on PUMS education classification:

| Code | Description |
|------|-------------|
| -9 | N/A recoded for less than 3 years old |
| 1 | No schooling completed |
| 2 | Nursery school to grade 4 |
| 3 | Grade 5 or grade 6 |
| 4 | Grade 7 or grade 8 |
| 5 | Grade 9 |
| 6 | Grade 10 |
| 7 | Grade 11 |
| 8 | 12th grade, no diploma |
| 9 | High school graduate |
| 10 | Some college, but less than 1 year |
| 11 | One or more years of college, no degree |
| 12 | Associate's degree |
| 13 | Bachelor's degree |
| 14 | Master's degree |
| 15 | Professional school degree |
| 16 | Doctorate degree |

### Occupation Codes (`OCCP`)

Recoded from PUMS SOCP codes in [create_seed_population.py]:

| Code | Description |
|------|-------------|
| -999 | N/A (under 16 or not in labor force >5 years) |
| 1 | Management |
| 2 | Professional |
| 3 | Services |
| 4 | Retail |
| 5 | Manual |
| 6 | Military |

### Employment Status Codes (`ESR`)

Based on PUMS employment status recode:

| Code | Description |
|------|-------------|
| 0 | N/A recoded for persons less than 16 years old |
| 1 | Civilian employed, at work |
| 2 | Civilian employed, with a job but not at work |
| 3 | Unemployed |
| 4 | Armed forces, at work |
| 5 | Armed forces, with a job but not at work |
| 6 | Not in labor force |

### Work Weeks Classification (`WKW`)

| Code | Description |
|------|-------------|
| -9 | N/A (under 16 or didn't work past 12 months) |
| 1 | 50 to 52 weeks |
| 2 | 48 to 49 weeks |
| 3 | 40 to 47 weeks |
| 4 | 27 to 39 weeks |
| 5 | 14 to 26 weeks |
| 6 | 13 weeks or less |

### School Grade Level (`SCHG`)

Current school attendance level:

| Code | Description |
|------|-------------|
| -9 | N/A (not attending school) |
| 1 | Nursery school/preschool |
| 2 | Kindergarten |
| 3 | Grade 1 to grade 4 |
| 4 | Grade 5 to grade 8 |
| 5 | Grade 9 to grade 12 |
| 6 | College undergraduate |
| 7 | Graduate or professional school |

## Data Quality and Validation

### Required Checks

1. **Referential Integrity**: All persons must link to valid households via `HHID`
2. **Geographic Consistency**: TAZ/MAZ combinations must exist in land use data
3. **Demographic Consistency**: Age, education, employment relationships must be logical
4. **Census Alignment**: Marginal totals should match control data within tolerance
5. **Completeness**: No missing values in required fields

### Common Issues

- **Orphaned Records**: Persons without matching households
- **Geographic Mismatches**: Invalid TAZ/MAZ combinations
- **Logical Inconsistencies**: Children with employment records, etc.
- **Missing Counties**: Households without valid county assignments
- **Scale Discrepancies**: Population totals not matching expected values

## Model Integration

### CT-RAMP Usage

The synthetic population serves multiple purposes in CT-RAMP:

1. **Demographic Segmentation**: Person types for model application
2. **Geographic Distribution**: Spatial allocation of travel demand
3. **Household Structure**: Joint travel and activity coordination
4. **Economic Factors**: Income-based choice model segmentation
5. **Life Stage Factors**: Age and employment-based behavior patterns

### Processing Flow

1. **Population Loading**: HouseholdDataManager reads synthetic population
2. **Person Classification**: Assigns model person types based on demographics
3. **Accessibility Calculation**: Computes zone-based accessibility measures
4. **Activity Generation**: Generates daily activity patterns by person type
5. **Location Choice**: Uses workplace/school location for mandatory activities

## File Format Requirements

### CSV Format Specifications

- **Encoding**: UTF-8 or ASCII
- **Headers**: First row must contain column names exactly as specified
- **Delimiters**: Comma-separated values
- **Missing Values**: Use -9 for missing categorical data, leave numeric fields empty only if explicitly allowed
- **ID Fields**: Must be integer values, no leading zeros except for single zero

### Performance Considerations

- **File Size**: Typical Bay Area files: ~650MB for households, ~1.5GB for persons
- **Memory Usage**: Full population loaded into memory during model execution
- **I/O Optimization**: Use efficient CSV parsing libraries for large datasets
- **Validation**: Run validation checks before full model execution

## Regional Customization

### Bay Area Specifics

- **County Codes**: Use MTC standard county identifiers
- **Zone System**: Compatible with MTC TAZ/MAZ structure  
- **Income Adjustment**: All income values in 2010 dollars
- **Special Populations**: Group quarters handling for universities, military bases

### Adaptation Guidelines

- **Geographic Codes**: Update TAZ/MAZ systems for different regions
- **Income Years**: Adjust inflation factors for different base years
- **Classification Systems**: May need region-specific occupation/industry codes
- **Control Data**: Use local Census and survey data for population synthesis

This synthetic population structure provides the demographic foundation for comprehensive activity-based travel demand modeling in the CT-RAMP framework.

[create_seed_population.py]: https://github.com/BayAreaMetro/populationsim/blob/master/bay_area/create_seed_population.py
[HouseholdDataManager]: https://github.com/BayAreaMetro/travel-model-two/blob/master/core/src/java/com/pb/mtctm2/abm/ctramp/HouseholdDataManager.java#L44
[PUMS]: https://www2.census.gov/programs-surveys/acs/tech_docs/pums/data_dict/PUMS_Data_Dictionary_2007-2011.pdf