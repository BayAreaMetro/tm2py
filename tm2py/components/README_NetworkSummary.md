# NetworkSummary Component

## Quick Start

The NetworkSummary component generates comprehensive transportation network performance reports including highway analysis, transit ridership summaries, and landuse totals.

### Run as Component
```python
controller.run_component("network_summary")
```

### Requirements
- EMME highway database with assignment results
- Transit database (optional)
- MAZ landuse file
- Time period configuration

### Key Outputs
- `topsheet.csv` - Main summary with key regional metrics
- `highway_performance_{period}.csv` - Highway performance by time period
- `transit_boardings_by_line_{period}.csv` - Transit boardings by line
- `transit_boardings_by_operator.csv` - Operator-level summaries

### Configuration
```toml
[network_summary]
output_directory = "output_summaries"
output_filename = "topsheet.csv"

[scenario]
landuse_file = "input/maz_data.csv"
```

## Full Documentation

See [docs/network_summary.md](../../docs/network_summary.md) for complete documentation including:
- Detailed input requirements
- All output file specifications
- Configuration options
- Troubleshooting guide
- Integration examples

## Files Generated

### Core Summary
- **topsheet.csv** - Key regional performance metrics

### Highway Analysis  
- **highway_performance_{period}.csv** - Performance by time period and facility
- **highway_performance_daily.csv** - All-day totals
- **lane_mile_inventory.csv** - Network infrastructure inventory

### Transit Analysis (if available)
- **transit_boardings_by_line_{period}.csv** - Line boardings by time period  
- **transit_boardings_by_line_daily.csv** - All-day line totals
- **transit_boardings_by_operator.csv** - Operator summaries
- **transit_boardings_by_service_type.csv** - Mode summaries

## Common Issues

**No highway database found**: Check EMME project path configuration  
**Missing boardings column**: Verify transit assignment completed  
**Landuse file not found**: Check `landuse_file` path in config  

Enable debug logging for detailed diagnostics:
```python
import logging
logging.getLogger('tm2py.components.network_summary').setLevel(logging.DEBUG)
```