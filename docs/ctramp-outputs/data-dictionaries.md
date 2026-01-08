# 📖 Data Dictionaries

!!! abstract "Reference Guide"
    Comprehensive field definitions and standardized codes for all **CTRAMP** output files. Essential reference for data analysis and model interpretation.

## 🎯 Overview

The **CT-RAMP** (Coordinated Travel-Regional Activity Modeling Platform) generates standardized output files with consistent data structures. This reference ensures proper interpretation of:

- 🚗 Transportation modes and choices
- 🎯 Travel purposes and activities  
- 🗺️ Geographic zone systems
- 📊 Common field definitions

## Output Files

### Core Person and Household Data

- **[Household Data](household.md)** - Household demographic and socioeconomic characteristics
- **[Person Data](person.md)** - Individual person attributes and characteristics

### Location Choice Results

- **[Workplace & School Locations](workplace-school-location.md)** - Long-term location choice model results for work and education destinations

### Travel Pattern Data

- **[Individual Tours](individual-tours.md)** - Person-level tour generation and characteristics (57 fields)
- **[Individual Trips](individual-trips.md)** - Trip segments within individual tours (19 fields)
- **[Joint Tours](joint-tours.md)** - Household joint tour coordination and planning (51 fields)
- **[Joint Trips](joint-trips.md)** - Trip segments within joint household tours (18 fields)

## Common Data Elements

### 🚗 Mode Dictionary

!!! tip "Standardized Across All Files"
    The **17-mode transportation dictionary** is used consistently across all travel output files.

=== "🚙 Auto Modes"
    
    | ID | Mode | Description | 💡 Notes |
    |----|------|-------------|----------|
    | 1 | `SOV_GP` | Single Occupant Vehicle - General Purpose | Standard freeway/arterial |
    | 2 | `SOV_PAY` | Single Occupant Vehicle - Express/Toll | Premium lanes |
    | 3 | `SR2_GP` | Shared Ride 2 - General Purpose | 2-person carpool |
    | 4 | `SR2_HOV` | Shared Ride 2 - HOV lanes | 2+ HOV access |
    | 5 | `SR2_PAY` | Shared Ride 2 - Express/Toll | Premium 2-person |
    | 6 | `SR3_GP` | Shared Ride 3+ - General Purpose | 3+ person carpool |
    | 7 | `SR3_HOV` | Shared Ride 3+ - HOV lanes | 3+ HOV access |
    | 8 | `SR3_PAY` | Shared Ride 3+ - Express/Toll | Premium 3+ person |

=== "🚶 Active Modes"
    
    | ID | Mode | Description | 💡 Use Case |
    |----|------|-------------|-------------|
    | 9 | `WLK_LOC` | Walk | Local pedestrian trips |
    | 10 | `BIKE_LOC` | Bicycle | Local bike trips |

=== "🚌 Transit Modes"
    
    | ID | Mode | Description | 💡 Access Type |
    |----|------|-------------|----------------|
    | 11 | `WLK_TRN` | Walk to Transit | Pedestrian access |
    | 12 | `PNR_TRN` | Park-and-Ride to Transit | Drive + park |
    | 13 | `KNRPRV_TRN` | Kiss-and-Ride Private | Drop-off by private vehicle |
    | 14 | `KNRTNC_TRN` | Kiss-and-Ride TNC | Drop-off by rideshare |

=== "🚕 Other Modes"
    
    | ID | Mode | Description | 💡 Use Case |
    |----|------|-------------|-------------|
    | 15 | `TAXI` | Taxi | Traditional taxi service |
    | 16 | `TNC` | Transportation Network Company | Uber, Lyft, etc. |
    | 17 | `SCHLBUS` | School Bus | K-12 student transport |

### 🎯 Purpose Classifications

!!! info "Activity-Based Modeling"
    Travel purposes reflect the **activity-based modeling** approach, categorizing trips by their underlying activity motivation.

| 🎯 Purpose | Description | 📝 Examples |
|-----------|-------------|-------------|
| **💼 Work** | Employment-related travel | Commuting, work meetings, business trips |
| **🏫 School** | K-12 education travel | Elementary/high school commute, school activities |
| **🎓 University** | Higher education travel | College/university commute, campus activities |
| **🛒 Shop** | Shopping and errands | Grocery, retail, personal services |
| **🔧 Maintenance** | Personal business | Banking, medical, government services |
| **🎉 Discretionary** | Social & recreational | Dining, entertainment, visiting friends |

### Geographic Identifiers

- **TAZ** - Traffic Analysis Zone (regional level)
- **MAZ** - Micro Analysis Zone (detailed level, ~40,000 zones)
- **TAP** - Transit Access Point

## Data Verification Notes

All documentation has been verified against actual model output files from the `2015-tm22-dev-sprint-04` model run. Field counts, value ranges, and data structures reflect real model outputs rather than theoretical specifications.

## File Relationships

The output files are interconnected through common identifiers:

- Tours link to persons via `person_id`
- Trips link to tours via `tour_id`
- Joint tours coordinate multiple household members
- Location choice results inform tour destination modeling

## Usage Guidelines

When working with CTRAMP output data:

1. Use consistent mode and purpose dictionaries across analyses
2. Verify field structures against current model version
3. Account for missing values and data quality flags
4. Consider temporal relationships in tour and trip sequencing