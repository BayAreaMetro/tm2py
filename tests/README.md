# Testing

Tests are run with the [pyTest](pytest.org)/

## Test structure

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

## Running tests

1. Run all tests
```sh
pytest 
```

2. Run tests in `test_basic.py`
```sh
pytest tests/test_basic.py
```

3. Run tests decorated with @pytest.mark.favorites decorator
```sh
pytest -m favorites
```

4. Run all tests and print out stdout
```sh
pytest -s
```

5. Run all tests which are run on the CI server
```sh
pytest -v -m "not skipci"
```
