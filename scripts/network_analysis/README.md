# Network Analysis Tools

Tools for analyzing, documenting, and validating Emme transportation networks for TM2PY.

## Overview

These tools help you:
- **Document network structure** - Field definitions, value ranges, record counts
- **Validate networks** - Compare actual networks to code expectations
- **Generate reports** - Comprehensive markdown documentation
- **Identify issues** - Missing attributes, unexpected values, schema mismatches

## Tools

### `analyze_network.py`

Comprehensive network analysis and documentation generator.

**Features**:
- Analyzes all network scenarios
- Documents link and node attributes with statistics
- Compares network to code expectations
- Generates markdown reports
- Exports raw data as JSON

**Usage**:

```bash
# Basic analysis
python scripts/network_analysis/analyze_network.py "M:/path/to/emme"

# With comparison to expectations
python scripts/network_analysis/analyze_network.py "M:/path/to/emme" --compare-expectations

# Specify output location
python scripts/network_analysis/analyze_network.py "M:/path/to/emme" --output docs/network_analysis.md

# Export JSON data
python scripts/network_analysis/analyze_network.py "M:/path/to/emme" --json network_data.json

# Analyze specific scenario
python scripts/network_analysis/analyze_network.py "M:/path/to/emme" --scenario 1
```

## Example Output

The tool generates a comprehensive markdown report with sections:

### Scenarios Summary
Lists all scenarios with node/link counts and result status

### Attribute Analysis
For each attribute:
- **Numeric attributes**: min, max, mean, median, std dev, percentiles
- **Categorical attributes**: unique values, top values, distributions

### Comparison to Expectations
- Missing expected attributes
- Unexpected additional attributes  
- Value range violations
- Type mismatches

## Sample Report

```markdown
# Network Analysis Report

**Network Path**: `M:/Development/Travel Model Two/.../emme`
**Generated**: 2025-12-15 14:23:45

## Scenarios

| ID | Title | Nodes | Links | Transit Lines | Traffic Results | Transit Results |
|---|---|---|---|---|---|---|
| 1 | EA Highway | 85,421 | 179,832 | 0 | ✓ | ✗ |
| 2 | AM Highway | 85,421 | 179,832 | 0 | ✓ | ✗ |

## Link Attributes

**Scenario**: 1
**Total Links**: 179,832
**Total Attributes**: 85

### Numeric Attributes

| Attribute | Count | Null % | Min | Max | Mean | Median | Std Dev | Unique |
|---|---|---|---|---|---|---|---|---|
| `@capacity` | 179,832 | 0.0% | 0.00 | 12000.00 | 2145.32 | 1800.00 | 1523.45 | 156 |
| `@free_flow_speed` | 179,832 | 0.0% | 3.00 | 75.00 | 32.18 | 30.00 | 15.67 | 42 |
| `@lanes` | 179,832 | 0.0% | 1.00 | 8.00 | 2.34 | 2.00 | 1.12 | 8 |

## Comparison to Code Expectations

### ✓ All Expected Attributes Present

### Attribute Validation Issues

| Attribute | Issues |
|---|---|
| `@free_flow_speed` | Value range mismatch: expected [0, 100], found [3.0, 125.0] |
```

## Integration with tm2py-utils

To use this in tm2py-utils:

1. **Copy this directory** to tm2py-utils repository
2. **Update imports** if needed for different package structure
3. **Add to tm2py-utils CLI** as a command

Or keep it in tm2py and use it as a utility script.

## Code Expectations

The tool knows about expected attributes from:
- `create_tod_scenarios` component
- `highway_network` component
- `transit_network` component
- Network documentation

Add more expectations by editing the `_get_expected_attributes_from_code()` method.

## Requirements

- Python 3.7+
- Emme (must run in Emme Python environment)
- pandas
- numpy

## Future Enhancements

- [ ] Scan tm2py code to auto-discover expected attributes
- [ ] Compare multiple networks (before/after analysis)
- [ ] Validate against network schema files
- [ ] Generate HTML reports with interactive charts
- [ ] Network topology validation
- [ ] Transit line validation
- [ ] Turn restrictions analysis

## See Also

- [Network Documentation](../../docs/input/network.md)
- [Network Summary Component](../../docs/network_summary.md)
- [Create TOD Scenarios](../../docs/assignment/index.md#create-time-of-day-scenarios)
