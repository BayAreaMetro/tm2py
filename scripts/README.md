# TM2PY Scripts

This directory contains utility scripts for TM2PY network analysis and processing.

## Network Summary Script

**File**: `network_summary.py`

Comprehensive highway network performance analysis tool that generates VMT, VHT, and delay summaries from TM2PY model results.

### Quick Start

```bash
# Activate TM2PY environment
conda activate tm2pyenv-acceptance

# Run basic analysis
python network_summary.py E:\2015-tm22-dev-sprint-04

# Run with custom output directory and verbose logging
python network_summary.py E:\2015-tm22-dev-sprint-04 --output C:\results --verbose
```

### Documentation

- **📖 Complete Usage Guide**: [`../docs/output/network-summary-usage.md`](../docs/output/network-summary-usage.md)
- **📊 Network Reference**: [`../docs/output/network-analysis.md`](../docs/output/network-analysis.md)  
- **🧪 Unit Tests**: [`../tests/test_network_summary.py`](../tests/test_network_summary.py)

### Key Features

- ✅ **5-Phase Validation**: Comprehensive input and data validation
- 📊 **Performance Metrics**: VMT, VHT, delay calculations by facility type
- 🌍 **Geographic Analysis**: County-level and facility type summaries  
- 📝 **Detailed Logging**: Complete processing history and error tracking
- 📂 **CSV Outputs**: Multiple summary files for further analysis
- 🧪 **Unit Tested**: Comprehensive test coverage for reliability

### Output Files

| File | Description |
|------|-------------|
| `highway_summary_by_facility.csv` | Performance by facility type |
| `highway_summary_by_county.csv` | County-level summaries |
| `highway_summary_by_time_period.csv` | Time period comparisons |
| `lane_mile_inventory.csv` | Infrastructure inventory |
| `network_summary.log` | Detailed processing log |

### Requirements

- TM2PY model run directory with EMME highway database
- tm2pyenv-acceptance Python environment
- EMME API access

### Support

For troubleshooting and advanced usage, see the [complete usage guide](../docs/output/network-summary-usage.md).