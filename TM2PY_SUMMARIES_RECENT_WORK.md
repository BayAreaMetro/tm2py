# TM2PY Summaries - Recent Development (Past 6 Months)

**Prepared for**: Management Review  
**Date**: January 5, 2026  
**Focus**: New automated summary tools developed in the past 6 months

---

## 📊 Slide 1: Overview - What We Built

### Three New Tools

✅ **Network Performance Reports** - Highway and transit system performance  
✅ **Travel Behavior Summaries** - 23 standard summaries of how people travel  
✅ **Data Export Tools** - Prepare data for acceptance testing and visualization  

### What This Means

- **Save Time**: What used to take hours now takes 10 minutes
- **More Reliable**: Same calculations every time, no manual errors
- **Catch Problems Early**: Automatic checks flag data issues before they become bigger problems

---

## 📊 Slide 2: Network Performance Reports (NEW)

### What It Does

Automatically creates reports showing how the transportation system performs - traffic speeds, congestion, transit ridership.

### What You Get

| Report Type | Shows You... | Coverage |
|-------------|--------------|----------|
| **Highway** | How much traffic, where delays occur, speeds by road type | Morning, midday, evening |
| **Transit** | How many people ride each line, which buses/trains are full | All day, by operator |
| **Geography** | Performance by county, which areas have most traffic | 9 Bay Area counties |
| **Quality Checks** | Flags unusual patterns or data problems | Automatic warnings |

### Reports You Get

**Main Summary** (everything in one Excel file):
- Network performance by road type (freeway, arterial, local)
- Transit ridership by operator (BART, Muni, AC Transit, etc.)
- County comparisons

**Individual Reports** (30+ CSV files for detailed analysis):
- Traffic volumes and speeds
- Transit boardings by line and time of day  
- Infrastructure inventory

### Easy to Use

One command generates all reports automatically

---

## 📊 Slide 3: Travel Behavior Summaries (NEW)

### What It Does

Automatically summarizes how people in the Bay Area travel - what modes they use, when they travel, where they go.

### Key Benefits

- **23 Standard Reports** - Covers everything planners need to see
- **Quality Checks Built-In** - Warns you if numbers look unusual
- **Fast** - Get all summaries in 10 minutes
- **Easy to Customize** - Add new summaries without programming

### Types of Travel Summaries

1. **Households** (5 reports) - Car ownership, household size, workers
2. **People** (4 reports) - Age groups, worker types, daily activities
3. **Tours** (7 reports) - Trip purposes, how people travel, distances, timing
4. **Trips** (7 reports) - Individual trip segments, modes, purposes, lengths

**Total: 23 Core Summaries** covering:
- How many cars households own (by income level)
- What modes people use (drive, transit, walk, bike)
- When people travel (morning, midday, evening)
- How far people travel (distance distributions)
- Work commute patterns
- Transit usage patterns

### Example Output Files

- `auto_ownership_regional.csv` - How many cars households have
- `tour_mode_choice.csv` - What transportation modes people use
- `trip_distance_distribution.csv` - How far people travel
- `person_type_distribution.csv` - Full-time workers, part-time, students, retirees
- `journey_to_work_patterns.csv` - Commute distances and patterns
- ...and 18 more standard reports

### Simple to Run

One command creates all 23 summaries automatically

---

## 📊 Slide 4: What the Summaries Show (Examples)

### How People Travel
Shows what transportation modes people use for their daily activities.

| Mode | Tours | Share |
|------|-------|-------|
| Drive Alone | 2,154,000 | 23.5% |
| Carpool (2 person) | 2,221,000 | 24.3% |
| Carpool (3+ person) | 1,722,000 | 18.8% |
| Walk | 1,185,000 | 12.9% |
| Bike | 652,000 | 7.1% |
| Transit | 503,700 | 5.5% |
| Other (TNC, Taxi, School Bus) | 226,000 | 2.5% |

### Travel Distances
Shows how far people typically travel for their daily tours.

| Distance | Tours | Share |
|----------|-------|-------|
| 0-5 miles | 5,406,000 | 64.6% |
| 5-10 miles | 1,482,000 | 17.7% |
| 10-20 miles | 884,000 | 10.6% |
| 20-50 miles | 496,000 | 5.9% |
| 50+ miles | 94,000 | 1.1% |

---

## 📊 Slide 5: Network Performance Examples

### Daily Traffic by Road Type

| Road Type | Daily Vehicle Miles | Share |
|-----------|---------------------|-------|
| Freeway | 78.7M | 51% |
| Arterial | 23.4M | 15% |
| Collector | 21.9M | 14% |
| Connector | 12.5M | 8% |
| Local | 6.8M | 4% |
| **Total** | **154.6M** | **100%** |

### Trip Purposes
Shows why people travel throughout the day.

| Purpose | Daily Trips | Share |
|----------|-----------|-------|
| Work | 6,428,000 | 22.2% |
| Discretionary | 4,343,000 | 15.0% |
| Maintenance | 4,016,000 | 13.8% |
| School | 3,825,000 | 13.2% |
| Shop | 3,720,000 | 12.8% |
| Escort | 2,277,000 | 7.8% |
| Visiting | 2,067,000 | 7.1% |
| Work-Based | 757,000 | 2.6% |

### County Car Ownership Patterns

| County | No Car | 1 Car | 2+ Cars | Households |
|--------|--------|-------|---------|------------|
| San Francisco | 40% | 34% | 26% | 440,500 |
| Alameda | 14% | 32% | 54% | 716,200 |
| Santa Clara | 8% | 30% | 62% | 727,400 |
| Contra Costa | 9% | 26% | 65% | 443,900 |
| San Mateo | 9% | 32% | 59% | 298,600 |

---

## 📊 Slide 6: How It Works

### Network Performance Reports

```
Model Results
  ↓
Automatic Processing
  ├─ Reads network data (morning, midday, evening)
  ├─ Reads transit ridership data
  ├─ Calculates traffic volumes, speeds, delays
  ├─ Organizes by road type, county, transit operator
  └─ Checks for unusual values
  ↓
Reports: Excel file + detailed CSV files
```

### Travel Behavior Summaries

```
Model Household/Person/Trip Data
  ↓
Automatic Processing
  ├─ Reads configuration (what to summarize)
  ├─ Labels data (converts codes to readable names)
  └─ Creates 23 standard summaries
  ↓
Summary & Quality Check
  ├─ Groups and counts (e.g., trips by mode)
  ├─ Calculates percentages
  └─ Flags unusual patterns
  ↓
Reports: 23 CSV files ready for analysis
```

---

## 📊 Slide 7: Why This Matters

### Benefits for Planning

✅ **Save Time** - Hours of manual work → 10 minutes automated  
✅ **More Reliable** - Same calculations every time, no manual errors  
✅ **Find Problems Early** - Automatic checks catch issues before they compound  
✅ **Easy to Compare** - Standard format makes scenario comparison simple  
✅ **Easy to Understand** - Clear labels and organization  

### Current Status

- ✅ **Network Reports**: Ready to use, integrated into model system
- ✅ **Travel Summaries**: Ready to use, fully documented
- ✅ **Export Tools**: Available for detailed analysis
- 🔄 **In Use Now**: Being used to validate 2015 base year model

### What's Next

1. **Standardize** - Make these reports part of every model run
2. **Train Team** - Show planners how to use these summaries
3. **Expand** - Add more summaries based on what planners need
4. **Compare Scenarios** - Build tools to easily compare alternatives
5. **Visualize** - Create interactive dashboards for exploring results

---

## 📊 Slide 8: Getting Started

### Documentation Available

- **Network Reports Guide**: See model documentation folder
- **Travel Summaries Guide**: https://bayareametro.github.io/tm2py-utils/
- **Complete List**: All outputs documented in inventory file

### Where to Find Reports

After a model run completes:

**Network Performance Reports**:
- Main file: `output_summaries/network_summary_report.xlsx`
- Detailed files: `output_summaries/*.csv`

**Travel Behavior Summaries**:
- All summaries: `outputs/*.csv` (23 files)
- Easy to open in Excel or other tools

### How Long Does It Take?

| Tool | Time to Run | What You Get |
|------|-------------|--------------|
| Network Reports | 5-10 minutes | Highway & transit performance (30+ files) |
| Travel Summaries | 7-11 minutes | Travel behavior patterns (23 files) |

**Note**: These run automatically as part of the model - no extra work needed!

---

**END OF PRESENTATION SUMMARY**

*This is a condensed summary focused on recent development (past 6 months). For complete inventory of all outputs, see `TM2PY_SUMMARIES_AND_OUTPUTS_INVENTORY.md`*
