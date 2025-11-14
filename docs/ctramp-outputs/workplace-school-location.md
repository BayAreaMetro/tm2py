# 🏢 Workplace and School Location Results (wsLocResults.csv)

!!! abstract "Long-Term Location Choices"
    **Workplace and school locations** represent the fundamental spatial decisions that anchor daily travel patterns. These long-term choices drive mandatory travel demand and shape household activity patterns.

!!! success "Data Verification"
    Documentation verified against **2015-tm22-dev-sprint-04** model run  
    ✅ **17 fields** confirmed in actual output files

## 🎯 Model Context

=== "🏭 Employment Modeling"
    
    Workplace location choice considers:
    
    - **Job accessibility** from home location
    - **Industry type** and employment categories
    - **Wage levels** and income constraints
    - **Transportation costs** and travel times

=== "🎓 Education Modeling"
    
    School location choice incorporates:
    
    - **School type** (elementary, high school, university)
    - **Capacity constraints** at education facilities
    - **Distance preferences** for different age groups
    - **Household transportation** resources

=== "🔗 Travel Integration"
    
    Location results feed into:
    
    | Model Component | Usage | Impact |
    |-----------------|--------|---------|
    | **Tour Generation** | Mandatory tour origins/destinations | Work/school commute patterns |
    | **Mode Choice** | Accessibility calculations | Transportation mode selection |
    | **Time-of-Day** | Scheduling constraints | Peak period travel timing |
    | **Route Assignment** | O-D matrix generation | Network loading patterns |

## 📊 Market Segmentation

### 💼 Employment Categories

!!! info "Worker Segmentation"
    Employment categories reflect industry types with distinct spatial patterns

| Field | Type | Description | Example Values |
|-------|------|-------------|----------------|
| `HHID` | INTEGER | Household identifier linking to household.csv | 17483, 17493 |
| `HomeMGRA` | INTEGER | Home location MGRA | Valid MGRA IDs |
| `Income` | INTEGER | Household income (dollars) | 128323, 167807 |
| `PersonID` | INTEGER | Person identifier linking to person.csv | 35180, 35190 |
| `PersonNum` | INTEGER | Person number within household | 1, 2, 3, 4 |

### Person Characteristics

| Field | Type | Description | Valid Values |
|-------|------|-------------|-------------|
| `PersonType` | INTEGER | CTRAMP person type classification | 1-8 (see person type dictionary) |
| `PersonAge` | INTEGER | Person age in years | 0-120+ |
| `EmploymentCategory` | INTEGER | Employment status category | 1-4 (see employment dictionary) |
| `StudentCategory` | INTEGER | Student status category | 1-4 (see student dictionary) |

### Market Segmentation

| Field | Type | Description | Valid Values |
|-------|------|-------------|-------------|
| `WorkSegment` | INTEGER | Work location choice market segment | 1-4, -1 for non-workers |
| `SchoolSegment` | INTEGER | School location choice market segment | 1-3, -1 for non-students |

### Location Choice Results

#### Workplace Location

| Field | Type | Description | Valid Values |
|-------|------|-------------|-------------|
| `WorkLocation` | INTEGER | Chosen work location MGRA | Valid MGRA ID, 0 if no work location |
| `WorkLocationDistance` | REAL | Home to work distance (miles) | 0.0+ for workers, 0.0 for non-workers |
| `WorkLocationLogsum` | REAL | Work location choice logsum | Real number, -999.0 for non-workers |

#### School Location

| Field | Type | Description | Valid Values |
|-------|------|-------------|-------------|
| `SchoolLocation` | INTEGER | Chosen school location MGRA | Valid MGRA ID, 0 if no school location |
| `SchoolLocationDistance` | REAL | Home to school distance (miles) | 0.0+ for students, 0.0 for non-students |
| `SchoolLocationLogsum` | REAL | School location choice logsum | Real number, -999.0 for non-students |

## Data Dictionaries

### Person Type Classification (PersonType)

| Code | Person Type | Description | Work Location | School Location |
|------|-------------|-------------|---------------|-----------------|
| 1 | Full-time worker | Adult with full-time employment | Yes | No |
| 2 | Part-time worker | Adult with part-time employment | Yes | No |
| 3 | University student | College/university student | Optional | Yes |
| 4 | Non-working adult | Adult not in labor force | No | No |
| 5 | Retired | Retired adult | No | No |
| 6 | Driving age student | Student 16-19 years old | No | Yes |
| 7 | Non-driving student | Student 6-15 years old | No | Yes |
| 8 | Preschooler | Child under 6 years old | No | No |

### Employment Category Classification (EmploymentCategory)

| Code | Category | Description | Work Assignment |
|------|----------|-------------|-----------------|
| 1 | Full-time employed | Full-time worker | Required work location |
| 2 | Part-time employed | Part-time worker | Required work location |
| 3 | Not employed | Not in labor force | No work location |
| 4 | Unemployed | Unemployed but seeking work | No work location |

### Student Category Classification (StudentCategory)

| Code | Category | Description | School Assignment |
|------|----------|-------------|------------------|
| 1 | Pre-K | Preschool age | No school location |
| 2 | K-12 student | Elementary/middle/high school | Required school location |
| 3 | University student | College/university | Required school location |
| 4 | Non-student | Not enrolled | No school location |

### Work Location Market Segments (WorkSegment)

| Code | Segment | Description | Income Level | Industry Focus |
|------|---------|-------------|--------------|----------------|
| 1 | Low-income worker | Lower income employment | Low | Service, retail |
| 2 | Mid-income worker | Middle income employment | Medium | Mixed sectors |
| 3 | High-income worker | Higher income employment | High | Professional, tech |
| 4 | Very high-income worker | Very high income employment | Very High | Executive, specialized |
| -1 | Non-worker | No employment | N/A | N/A |

### School Location Market Segments (SchoolSegment)

| Code | Segment | Description | Age Range | Education Level |
|------|---------|-------------|-----------|-----------------|
| 1 | K-12 student | Elementary through high school | 5-18 | Primary/secondary |
| 2 | University undergraduate | College undergraduate | 18-25 | Higher education |
| 3 | University graduate | Graduate/professional school | 22-35 | Graduate education |
| -1 | Non-student | Not enrolled in school | N/A | N/A |

## Location Choice Modeling

### Workplace Location Choice

**Model Structure:**
- Uses distance decay and employment accessibility measures
- Considers industry-specific employment opportunities
- Accounts for transportation costs and travel time
- Includes household income and person characteristics

**Key Variables:**
- Employment by industry type at each MGRA
- Transportation cost (mode choice logsums) from home to work
- Distance decay parameters by income segment
- Regional accessibility measures

### School Location Choice

**Model Structure:**
- Uses distance decay and school capacity constraints
- Considers school type (K-12 vs university) availability
- Accounts for transportation access and costs
- May include school quality or preference factors

**Key Variables:**
- School enrollment capacity at each MGRA
- Transportation cost from home to school
- Distance decay parameters by student type
- Age-appropriate school availability

## Data Quality and Validation

### Work Location Consistency

```sql
-- Workers should have work locations
SELECT COUNT(*) FROM wsLocResults 
WHERE PersonType IN (1, 2) 
AND WorkLocation = 0;

-- Non-workers should not have work locations
SELECT COUNT(*) FROM wsLocResults
WHERE PersonType NOT IN (1, 2, 3) 
AND WorkLocation > 0;

-- Work distances should be reasonable
SELECT COUNT(*) FROM wsLocResults
WHERE WorkLocation > 0 
AND (WorkLocationDistance <= 0 OR WorkLocationDistance > 200);
```

### School Location Consistency

```sql
-- Students should have school locations
SELECT COUNT(*) FROM wsLocResults
WHERE PersonType IN (3, 6, 7)
AND SchoolLocation = 0;

-- Non-students should not have school locations  
SELECT COUNT(*) FROM wsLocResults
WHERE PersonType NOT IN (3, 6, 7)
AND SchoolLocation > 0;

-- School distances should be reasonable
SELECT COUNT(*) FROM wsLocResults
WHERE SchoolLocation > 0
AND (SchoolLocationDistance <= 0 OR SchoolLocationDistance > 100);
```

### Segmentation Consistency

```sql
-- Work segments should align with employment
SELECT COUNT(*) FROM wsLocResults
WHERE (EmploymentCategory IN (1, 2) AND WorkSegment = -1)
OR (EmploymentCategory IN (3, 4) AND WorkSegment != -1);

-- School segments should align with student status
SELECT COUNT(*) FROM wsLocResults  
WHERE (StudentCategory IN (2, 3) AND SchoolSegment = -1)
OR (StudentCategory IN (1, 4) AND SchoolSegment != -1);
```

## Analysis Examples

### Commute Distance Analysis

```python
import pandas as pd
import numpy as np

# Load workplace location data
wsloc = pd.read_csv('wsLocResults.csv')

# Filter workers with work locations
workers = wsloc[(wsloc['PersonType'].isin([1, 2])) & (wsloc['WorkLocation'] > 0)]

# Commute distance statistics by person type
commute_stats = workers.groupby('PersonType')['WorkLocationDistance'].agg([
    'count', 'mean', 'median', 'std'
]).round(2)

print("Commute distance statistics:")
print(commute_stats)

# Distribution of work market segments
work_segments = workers['WorkSegment'].value_counts().sort_index()
print("\nWork market segment distribution:")
print(work_segments)
```

### School Location Analysis

```sql
-- School enrollment by distance bands
SELECT 
    CASE 
        WHEN SchoolLocationDistance <= 1 THEN '0-1 mile'
        WHEN SchoolLocationDistance <= 3 THEN '1-3 miles' 
        WHEN SchoolLocationDistance <= 5 THEN '3-5 miles'
        WHEN SchoolLocationDistance <= 10 THEN '5-10 miles'
        ELSE '10+ miles'
    END as distance_band,
    COUNT(*) as students,
    AVG(SchoolLocationDistance) as avg_distance
FROM wsLocResults 
WHERE SchoolLocation > 0
GROUP BY distance_band
ORDER BY avg_distance;
```

### Accessibility Analysis

```python
# Analyze accessibility by market segment
accessibility = wsloc[wsloc['WorkLocation'] > 0].groupby('WorkSegment').agg({
    'WorkLocationLogsum': ['mean', 'median', 'std'],
    'WorkLocationDistance': ['mean', 'median']
}).round(3)

print("Work accessibility by market segment:")
print(accessibility)

# Income and commute relationship
income_commute = workers.groupby(pd.cut(workers['Income'], bins=5))['WorkLocationDistance'].mean()
print("\nAverage commute distance by income quintile:")
print(income_commute)
```

### Spatial Distribution

```sql
-- Top work destinations
SELECT 
    WorkLocation as mgra,
    COUNT(*) as workers,
    AVG(WorkLocationDistance) as avg_commute_distance,
    AVG(Income) as avg_worker_income
FROM wsLocResults
WHERE WorkLocation > 0
GROUP BY WorkLocation
HAVING COUNT(*) >= 10
ORDER BY workers DESC
LIMIT 20;

-- School concentration analysis
SELECT
    SchoolLocation as mgra,
    COUNT(*) as students, 
    AVG(SchoolLocationDistance) as avg_distance,
    COUNT(CASE WHEN PersonType = 3 THEN 1 END) as university_students,
    COUNT(CASE WHEN PersonType IN (6,7) THEN 1 END) as k12_students
FROM wsLocResults
WHERE SchoolLocation > 0  
GROUP BY SchoolLocation
HAVING COUNT(*) >= 5
ORDER BY students DESC;
```

## Model Applications

### Land Use Planning
- Employment and school location patterns
- Transportation infrastructure needs assessment
- Regional accessibility evaluation

### Transportation Planning  
- Commute flow estimation for network assignment
- Transit route planning based on work/school destinations
- Mode choice model calibration and validation

### Economic Analysis
- Labor market accessibility by location
- Educational facility service area analysis
- Regional economic impact assessment

## Special Considerations

### Non-mandatory Locations
- File only contains mandatory (work/school) locations
- Shopping, recreation, and other discretionary destinations modeled separately
- Some university students may also have work locations

### Model Sensitivity
- Location choice is sensitive to transportation network changes
- Results reflect base year land use and employment data
- Future year forecasting requires land use projections

### Data Limitations
- Represents modeled locations, not observed behavior
- May not capture all workplace flexibility (telework, multiple locations)
- School choice constraints may be simplified compared to actual policies

---

*This documentation is part of the [CTRAMP Output Files](index.md) specification suite.*