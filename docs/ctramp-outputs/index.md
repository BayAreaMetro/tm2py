# 📊 CTRAMP Output File Specifications

!!! info "About This Documentation"
    Comprehensive field-level documentation for **CTRAMP** (Coordinated Travel-Regional Activity Modeling Platform) output files. All specifications are verified against real model data for maximum accuracy.

## 🔍 Model Run Verification

!!! success "Data Source Verified"
    All documentation is based on analysis of the **2015-tm22-dev-sprint-04** model run:
    
    ```
    C:\Box\Modeling and Surveys\Development\Travel Model Two Conversion\Model Outputs\2015-tm22-dev-sprint-04\ctramp_output\
    ```

## 📁 Output File Categories

=== "👥 Demographics"
    
    Core population and household characteristics
    
    - **[👪 Household Data](household.md)** - Demographics and socioeconomic attributes
    - **[👤 Person Data](person.md)** - Individual characteristics and travel patterns

=== "📍 Locations"
    
    Long-term location choice modeling results
    
    - **[🏢 Workplace & School Locations](workplace-school-location.md)** - Employment and education destinations (wsLocResults)

=== "🚗 Travel Patterns"
    
    Daily travel behavior and trip generation
    
    - **[🎯 Individual Tours](individual-tours.md)** - Person-level tour patterns (indivTourData) - *57 fields*
    - **[➡️ Individual Trips](individual-trips.md)** - Trip segments within tours (indivTripData) - *19 fields*
    - **[👨‍👩‍👧‍👦 Joint Tours](joint-tours.md)** - Household coordination (jointTourData) - *51 fields*
    - **[🔗 Joint Trips](joint-trips.md)** - Joint household trip segments (jointTripData) - *18 fields*

=== "📚 Reference"
    
    Common definitions and data standards
    
    - **[📖 Data Dictionaries](data-dictionaries.md)** - Modes, purposes, and field definitions

## ✅ Quality Assurance

!!! check "Verification Standards"
    
    === "Field Accuracy"
        All field counts and data structures verified against actual CSV files
    
    === "Value Validation"
        Purpose classifications and mode choices reflect real model outputs
    
    === "Consistency"
        Standardized 17-mode transportation dictionary across all files

## Documentation Purpose

This documentation serves to:

1. **Define exact output format** - Field names, data types, constraints
2. **Specify allowed values** - Valid codes, categories, ranges  
3. **Provide data dictionaries** - Meaning of categorical codes
4. **Enable survey mapping** - Transform survey data to CTRAMP format
5. **Support validation** - Ensure data integrity and consistency

## Key Design Principles

### Geographic Hierarchy
- **MGRA (Micro-Analysis Zone)**: Finest geographic resolution (MAZ in model code)
- **TAZ (Traffic Analysis Zone)**: Aggregated zones for trip matrices
- **County/District**: Administrative boundaries

### Temporal Structure  
- **Time Periods**: Model uses discrete time periods (typically 1-48 half-hour periods)
- **Tour Organization**: Tours contain trips (outbound and inbound legs)
- **Activity Patterns**: Daily activity coordination across household members

### Choice Model Results
- **Utilities/Probabilities**: Optional detailed model outputs
- **Logsum Values**: Accessibility measures from nested models
- **Random Numbers**: For reproducibility and debugging

## Common Field Types

### Identifiers
- `hh_id`: Unique household identifier (integer)
- `person_id`: Unique person identifier across model (integer)  
- `person_num`: Person number within household (1-based, integer)
- `tour_id`: Unique tour identifier (integer)
- `stop_id`: Stop number within tour half (0-based, integer)

### Geographic References
- `home_mgra`, `orig_mgra`, `dest_mgra`: MGRA identifiers
- `TAZ`: Traffic Analysis Zone identifier
- `parking_mgra`: Parking location MGRA

### Categorical Codes
- `person_type`: Worker/student/age categories (1-8)
- `tour_purpose`: Trip purpose codes (work, school, shopping, etc.)
- `tour_mode`/`trip_mode`: Transportation mode (1-N, varies by model)
- `income`: Household income category (1-4 or 1-5)

### Model Results
- `sampleRate`: Expansion factor for sampling (float)
- `avAvailable`: Autonomous vehicle availability (0/1)
- `dcLogsum`: Destination choice logsum value (float)

## File Relationships

```
Household (1) ──── (N) Person
    │                   │
    │                   ├── (N) Individual Tours ──── (N) Individual Trips
    │                   │
    └── (N) Joint Tours ──── (N) Joint Trips
```

## Survey Data Integration

When mapping survey data to CTRAMP format:

1. **Establish crosswalks** between survey categories and CTRAMP codes
2. **Validate geographic references** ensure MGRAs/TAZs are in model network
3. **Handle missing data** following model conventions (typically -1 or 0)
4. **Apply expansion factors** to match model population targets
5. **Verify consistency** across related tables (tours ↔ trips, person ↔ household)

## Getting Started

1. Review the **[Data Dictionaries](data-dictionaries.md)** for categorical value definitions
2. Examine your specific output file documentation:
   - [Household Output Format](household.md)
   - [Person Output Format](person.md)
   - [Workplace & School Location Results](workplace-school-location.md)
   - [Individual Tour Output Formats](individual-tours.md)
   - [Joint Tour Output Formats](joint-tours.md)
   - [Individual Trip Output Formats](individual-trips.md)
   - [Joint Trip Output Formats](joint-trips.md)
3. Use **[Survey Mapping Templates](survey-mapping.md)** for data transformation
4. Validate output using **[Quality Checks](validation.md)**

## Source References

This documentation is derived from:
- `travel-model-two` CTRAMP implementation (Java source code)
- `tm2py` model orchestration and validation logic
- SANDAG ABM model structure and conventions
- MTC Travel Model 2.0 specifications

---

*Last Updated: November 2025 - Documentation verified against actual 2015-tm22-dev-sprint-04 model output files*