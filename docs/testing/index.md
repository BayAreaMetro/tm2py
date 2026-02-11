# Testing

Tests are run with [pyTest](https://pytest.org).

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
