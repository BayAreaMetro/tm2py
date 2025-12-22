# Testing

Tests are run with [pyTest](pytest.org).

## County Highway Test Framework

For rapid testing of highway assignment/skimming on a single county:

```powershell
python tests\run_county_test.py --output-dir "E:\Tests\san_mateo_test" --county "San Mateo"
```

**Documentation**:
- **[County Test Framework Guide](COUNTY_TEST_FRAMEWORK_GUIDE.md)** - Complete usage guide, troubleshooting, architecture
- [Field Name Mapping](COMPLETE_FIELD_NAME_MAPPING.md) - Vehicle naming conventions (s2/s3 vs sr2/sr3)
- [EMME Manager Flow](EMME_MANAGER_FLOW.md) - EMME initialization and database management
- [Original Framework Update](COUNTY_TEST_FRAMEWORK_UPDATE.md) - Design documentation

**Key Features**:
- Demand filtering (intra-county trips only)
- ~5,000 TAZs vs ~30,000 (83% reduction)
- 2-5 minute runtime vs 15-30 minutes
- Components: create_tod_scenarios → prepare_network_highway → highway

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
