# Testing

Tests are run with [pyTest](pytest.org).

## County Highway Test Framework

For rapid testing of highway assignment/skimming on a single county:

```powershell
python tests\run_county_test.py
```

**Documentation**:
- **[Quick Start Guide](../docs/testing/quick-start.md)** - Get started in 5 minutes
- **[County Test Framework Guide](../docs/testing/county-test-guide.md)** - Complete usage guide, troubleshooting, architecture
- [Configuration Reference](../docs/testing/configuration.md) - All config options explained
- [Setup Component Guide](../docs/testing/setup.md) - File copying and EMME database setup

**Key Features**:
- **Setup component integration** - Automatically copies files and configures EMME databases
- **Selective file copying** - Control what gets copied (network only, demand, land use, etc.)
- Demand filtering (intra-county trips only)
- ~5,000 TAZs vs ~30,000 (83% reduction)
- 15-30 minute runtime including setup
- Components: setup → create_tod_scenarios → prepare_network_highway → highway

## Test structure

- Tests marked with `@pytest.mark.skipci` will not run by the continuous integration tests

## Setup

Pytest can be installed using one of the following options.

Install along with all development requirements (recommended):

=== pip

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

## Running tests

=== All tests

```sh
pytest
```

=== Tests in a specific file

```sh
pytest tests/test_basic.py
```

=== Tests with a specific decorator

```sh
pytest -m favorites
```

=== Continuous Integration Tests

```sh
pytest -v -m "not skipci"
```
