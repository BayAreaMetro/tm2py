# Testing

Tests are run with [pyTest](https://pytest.org).

## County Highway Test Framework

For rapid testing of highway assignment/skimming on a single county:

**→ [Quick Start Guide](quick-start.md)** - All paths you need to change when input data changes

1. Edit `tests/county_test_config.toml` with your paths and settings
2. Run: `python tests\run_county_test.py`

**Key Features**:
- Demand filtering (intra-county trips only)
- Automatic zone detection from crosswalk files
- ~5,000 TAZs vs ~30,000 (83% reduction)
- 2-5 minute runtime vs 15-30 minutes
- Components: create_tod_scenarios → prepare_network_highway → highway

### Documentation

- **[Quick Start](quick-start.md)** - ⚡ All paths to update when input data changes
- **[County Test Guide](county-test-guide.md)** - Complete usage guide, architecture, and troubleshooting
- **[Configuration Reference](configuration.md)** - Complete configuration file documentation
- **[Data Flow](data-flow.md)** - Input files, transformations, and output files explained
- **[Setup & Configuration](setup.md)** - Pre-flight checklist and detailed setup instructions
- **[Quick Reference](quick-reference.md)** - Command reference and examples
- **[Trip Filtering](filtering-trips.md)** - How to filter CTRAMP output trip files
- **[Network Thinning](network-thinning.md)** - Network optimization for faster testing
- **[EMME Manager Flow](emme-manager-flow.md)** - EMME initialization and database management
- **[Field Name Mapping](../assignment/field-name-mapping.md)** - Vehicle naming conventions (s2/s3 vs sr2/sr3)

## Test Structure

- Tests marked with `@pytest.mark.skipci` will not run by the continuous integration tests

## Setup

Pytest can be installed using one of the following options.

Install along with all development requirements (recommended):

```sh
pip install -r dev-requirements.txt
```

Install using PIP:

```sh
pip install pytest
```

Install using Conda:

```sh
conda install pytest
```

## Running Tests

### All tests

```sh
pytest
```

### Tests in a specific file

```sh
pytest tests/test_basic.py
```

### Tests with a specific decorator

```sh
pytest -m favorites
```

### Continuous Integration Tests

```sh
pytest -v -m "not skipci"
```
