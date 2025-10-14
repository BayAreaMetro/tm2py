# Outputs

Travel Model Two (TM2) generates a comprehensive set of output files that capture travel behavior, network performance, and system-wide metrics. This page provides an overview of the major output categories with links to detailed documentation.

!!! info "Cross-Reference"
    See [Create Base Year Inputs](create-base-year-inputs.md) for information about preparing data inputs that generate these outputs.

## Major Output Categories

### [Skim Outputs](output/skims.md)
Level-of-service matrices representing travel impedances and costs between zones.

- **Highway Skims**: Travel time, distance, and cost matrices for automobile modes
- **Transit Skims**: Comprehensive transit level-of-service indicators including wait times, fares, and in-vehicle times
- **Active Mode Skims**: Walking and bicycling distance matrices between zones
- **Drive Access Skims**: Park-and-ride access impedances

[→ View detailed skim output documentation](output/skims.md)

### [CTRAMP Outputs](output/ctramp.md)
Detailed microsimulation results from the household travel behavior model.

- **Individual Travel**: Person-level tour and trip data with full demographic context
- **Joint Travel**: Household joint tours and coordinated travel patterns
- **Transit Resimulation**: Capacity-constrained transit trip results
- **Park-and-Ride**: Constrained and unconstrained parking demand

[→ View detailed CTRAMP output documentation](output/ctramp.md)

### [Assignment Outputs](output/assignment.md)
Network loading results showing traffic and transit flows on infrastructure.

- **Highway Assignment**: Link volumes, speeds, and congestion levels by time period
- **Transit Assignment**: Ridership by line and segment, capacity utilization
- **Network Performance**: System-wide metrics and convergence statistics
- **Validation Data**: Count comparisons and screenline analysis

[→ View detailed assignment output documentation](output/assignment.md)

### [Commercial Vehicle Outputs](output/commercial.md)
Freight and service vehicle demand modeling results.

- **Truck Trip Generation**: Employment and household-based commercial trip production
- **Trip Distribution**: Origin-destination patterns by truck class and commodity
- **Temporal Distribution**: Time-of-day and directional flow patterns
- **Special Generators**: Airport, port, and industrial facility freight movements

[→ View detailed commercial vehicle output documentation](output/commercial.md)

### [Summary Reports](output/summaries.md)
Aggregated performance metrics and validation summaries.

- **Model Performance**: System-wide travel and network performance indicators
- **Geographic Summaries**: County and jurisdiction-level travel patterns
- **Market Segments**: Travel behavior by income, auto ownership, and demographics
- **Accessibility Analysis**: Employment and services accessibility by mode

[→ View detailed summary report documentation](output/summaries.md)

### [Network Summary Component](../network_summary.md)
Comprehensive network analysis component and standalone script for highway and transit performance.

- **Component Integration**: Run as part of TM2PY workflow or standalone script
- **Highway Analysis**: VMT, VHT, speeds, and congestion by facility type and time period
- **Transit Analysis**: Boardings, ridership patterns, and operator summaries
- **Landuse Integration**: Population, household, and employment totals
- **Validation**: Built-in data quality checks and troubleshooting guide

[→ View Network Summary component documentation](../network_summary.md)

### [Network Analysis Reference](output/network-analysis.md)
Technical reference documentation for TM2PY network analysis capabilities.

- **Highway Attributes**: Complete reference of 85+ EMME link attributes from actual database
- **Transit Attributes**: Line and segment attributes extracted from TM2PY source code
- **Analysis Tools**: Shared modules, network summary script, and extraction utilities
- **Performance Metrics**: VMT, VHT, boarding analysis, and validation ranges

[→ View network analysis reference documentation](output/network-analysis.md)

## Quick Reference

### File Locations
```
model_run_directory/
├── skims/                    # All skim matrices (OMX and text files)
├── ctramp_output/           # CTRAMP microsimulation results
├── hwy/                     # Highway assignment networks and summaries
├── trn/                     # Transit assignment results
├── demand_matrices/         # Trip matrices by purpose and mode
├── output_summaries/        # Aggregated reports and validation
└── logs/                    # Model run logs and diagnostics
```

### Common File Formats

- **OMX Files** (.omx): Matrix data - use OpenMatrix library
- **CSV Files** (.csv): Tabular data - any spreadsheet software
- **Emme Networks** (.net): Network files - Emme software required
- **Text Files** (.txt): Various formatted outputs

### Time Periods
All time-specific outputs are generated for five periods:

- **EA**: Early AM (3:00-6:00)
- **AM**: AM Peak (6:00-10:00)  
- **MD**: Midday (10:00-15:00)
- **PM**: PM Peak (15:00-19:00)
- **EV**: Evening (19:00-3:00)

### Model Iterations
Files include iteration numbers reflecting the feedback process:

- **Iteration 1**: Sample population, preliminary skims
- **Iteration 2**: Full population, updated network conditions
- **Iteration 3**: Final iteration with converged network performance

## Usage Guidelines

### File Size Considerations
- Full model outputs can exceed 10 GB
- OMX files are compressed and efficient for large matrices
- Consider sampling trip files for analysis (except final validation)

### Coordinate Systems
- **TAZ Level**: Regional analysis (1,454 zones)
- **MAZ Level**: Local access analysis (40,000+ zones)  
- **Geographic**: California State Plane Zone III

### Quality Assurance
- Validation outputs compare model results with observed data
- Convergence logs document assignment and feedback stability
- Summary reports highlight key performance indicators

### Analysis Applications
- **Planning**: Long-range transportation plan development
- **Project Evaluation**: Transportation investment analysis  
- **Policy Assessment**: Pricing, land use, and service change impacts
- **Equity Analysis**: Transportation access and burden evaluation