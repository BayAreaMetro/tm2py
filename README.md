# Travel Model 2 Python Package

A python package to run the San Francisco Bay Area's Travel Model.

**Owner:** Metropolitan Transportation Commission (MTC)

[![Tests](https://github.com/BayAreaMetro/tm2py/actions/workflows/test.yml/badge.svg?branch=develop)](https://github.com/BayAreaMetro/tm2py/actions/workflows/test.yml)

[![Documentation](https://github.com/BayAreaMetro/tm2py/actions/workflows/docs.yml/badge.svg?branch=develop)](https://github.com/BayAreaMetro/tm2py/actions/workflows/docs.yml)

[![Package Published](https://github.com/BayAreaMetro/tm2py/actions/workflows/publish.yml/badge.svg?branch=develop)](https://github.com/BayAreaMetro/tm2py/actions/workflows/publish.yml)

## Installation

Recommended install in a virtual environment.

Stable (to come - use bleeding edge for now):

```bash
pip install tm2py
```

Bleeding edge:

```bash
conda env create -f environment.yml
conda activate tm2py
pip install git+https://github.com/bayareametro/tm2py@develop
```

See [starting out](http://bayareametro.github.com/tm2py) section of documentation for more details.

### Example Data

This respository doesn't come with example data due to its size. However, it does provide helper functions to access it from an online bucket:

```bash
get_test_data location/for/test/data
```

Alternatively, you can access it from [example_union_test_highway.zip](https://mtcdrive.box.com/s/3entr016e9teq2wt46x1os3fjqylfoge)

See [starting out](http://bayareametro.github.com/tm2py) section of documentation for more details.

### Usage

#### Python

```python
import tm2py
controller = RunController(
    ["scenario_config.toml", "model_config.toml"],
    run_dir="UnionCity",
)
controller.run()
```

- `run_dir` specifies specific run directory. Otherwise will use location of first `config.toml` file.

#### Terminal

```sh
<path to tm2py>\bin\tm2py -s examples\scenario_config.toml -m examples\model.toml [-r <location>]
```

- `-s scenario.toml` file location with scenario-specific parameters
- `-m model.toml` file location with general model parameters
- `-r run_dir` specifies specific run directory. Otherwise will use location of first `config.toml` file.

Additional functionality for various use cases can be found in [Examples](examples).

## Contributing

Details about contributing can be found on our documentation website: [](https://bayareametro.github.io/tm2py/contributing)
