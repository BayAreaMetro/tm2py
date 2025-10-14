# Summary Report Outputs

Travel Model Two generates summary reports for model validation, performance monitoring, and planning analysis through a modular summary system currently under development.

!!! info "Summary System Development"
    TM2PY's summary system is actively being built out with modular components. Currently available summary tools include the Network Summary Component and Post Processor, with additional summary modules planned for future development.

## Current Summary Tools

### Network Summary Component

The [Network Summary Component](../network_summary.md) provides comprehensive analysis and reporting of transportation network performance, including highway, transit, and landuse summaries.

**Key Features:**
- Highway network performance (speeds, volumes, congestion)
- Transit ridership and operator analysis
- Landuse totals (households, population, jobs)
- Network inventory and capacity analysis

**Usage:**
- Can be run as part of the TM2PY model workflow
- Available as a standalone analysis tool
- Generates detailed CSV reports for validation and analysis

[→ View complete Network Summary Component documentation](../network_summary.md)

### Post Processor

The [Post Processor](standalone-postprocessor.md) exports EMME network data to various formats for visualization and acceptance testing.

**Key Features:**
- Network shapefiles (highway links/nodes, transit lines/segments)
- Transit boarding data (CSV and GeoJSON formats)
- Data exports for acceptance criteria validation

**Usage:**
- Can be run standalone from existing model results
- Generates files needed for model acceptance testing
- Exports data for GIS visualization and analysis

[→ View complete Post Processor documentation](standalone-postprocessor.md)

## Planned Summary Modules

The summary system is designed to be modular and extensible. Additional summary components are planned for future development to cover:

- **Travel Behavior Summaries** - Trip generation, mode choice, and temporal distribution analysis
- **Geographic Summaries** - County, jurisdiction, and regional analysis
- **Market Segment Analysis** - Income, auto ownership, and demographic breakdowns
- **Accessibility Analysis** - Employment and services accessibility metrics
- **Equity Analysis** - Transportation equity and environmental justice indicators
- **Validation Summaries** - Count validation and survey comparison tools
- **Scenario Comparison** - Policy impact and scenario difference analysis

!!! note "Contributing to Summary Development"
    The summary system is actively under development. For information on contributing new summary modules or enhancing existing ones, see the [contributing documentation](../contributing/).

## Getting Started

To begin using the summary tools:

1. **For network analysis**: Start with the [Network Summary Component](../network_summary.md)
2. **For data exports**: Use the [Post Processor](standalone-postprocessor.md)
3. **For custom analysis**: Review the component architecture and consider contributing new summary modules
