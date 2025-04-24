# Travel Model 2 Python Package

A python package to run the San Francisco Bay Area's Travel Model.

**Owner:** Metropolitan Transportation Commission (MTC)

[![Tests](https://github.com/BayAreaMetro/tm2py/actions/workflows/test.yml/badge.svg?branch=develop)](https://github.com/BayAreaMetro/tm2py/actions/workflows/test.yml)

[![Documentation](https://github.com/BayAreaMetro/tm2py/actions/workflows/docs.yml/badge.svg?branch=develop)](https://github.com/BayAreaMetro/tm2py/actions/workflows/docs.yml)

[![Package Published](https://github.com/BayAreaMetro/tm2py/actions/workflows/publish.yml/badge.svg?branch=develop)](https://github.com/BayAreaMetro/tm2py/actions/workflows/publish.yml)

## Installation
Note: `tm2py` is fully tested on Windows only.

### Create Virtual Environment
Since April 2025, `tm2py` has been updated to use OpenPaths' Python distribution (OpenPaths EMME 24.01.00). To launch an OpenPaths Python instance, in the system search bar, type "OpenPaths EMME Shell 24.01.00" and click it to launch.

We recommend creating a virtual environment for `tm2py` installation and deployment. To create a virtual environment, use the following steps:

1) Launch OpenPaths EMME Shell 24.01.00
2) Create a virtual environment 
```bash
python -m venv /path/to/virtual/env/<your_tm2py_env_name>
```
3) Copy the "emme.pth" file from the OpenPaths EMME installation folder to the virtual environment. "emme.pth" is usually found under "C:\Program Files\Bentley\OpenPaths\EMME 24.01.00". Copy it to: `/path/to/virtual/env/<your_tm2py_env_name>/Lib/site-packages/`
4) You've just created a virtual environment. To activate it
```bash
/path/to/virtual/env/<your_tm2py_env_name>/Scripts/activate
```

### Stable Installation (From Release):
In the active virtual environment, 
```bash
pip install tm2py
```
Note: this option is only available when `tm2py` is released on pypi. Before that, use the Developer Installation option below.

### Developer Installation (From Clone):
In the active virtual environment,
1) Clone `tm2py` to your local directory
```bash
cd path/to/your/local/git/repo/location
git clone https://github.com/BayAreaMetro/tm2py.git
```
2) Install tm2py
```bash
cd tm2py
pip install -e .
```
3) If developers would like to install dev or doc dependencies
```bash
cd tm2py
pip install -e .[dev,doc]
```
 
### Example Data

This respository doesn't come with example data due to its size. However, it does provide helper functions to access it from an online bucket:
```bash
get_test_data location/for/test/data
```

Alternatively, you can access it from [example_union_test_highway.zip](https://mtcdrive.box.com/s/3entr016e9teq2wt46x1os3fjqylfoge)

Copy and unzip [example_union_test_highway.zip](https://mtcdrive.box.com/s/3entr016e9teq2wt46x1os3fjqylfoge) to a local
drive and from within that directory run:
```sh
get_test_data <location>
tm2py -s scenario.toml -m model.toml
```

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

### Common Issues
If the above instructions are installed and the model fails, there are some common issues depending in the computer being installed 

#### Running With Multiple Emme Versions Installed
This model has compatibility with emme version 4.6.1 (as of the TM2.2.1.1 release). If multiple versions of emme are installed, the above install instructions will launch the latest version of emme, regardless of which directory the emme.pth file was copied from. The correct emme version (4.6.1) can be launched using the following steps:
1) Open emme.pth file in the notepad and replace the os.environ["EMMEPATH"] with the path to the emme version you would like, in this case 4.6.1
```python
import os, site; site.addsitedir(os.path.join(r"C:\\Program Files\\INRO\\Emme\\Emme 4\\Emme-4.6.1", "Python37/Lib/site-packages"))
```
2) At the beginning of first python file run_model.py add this line
```python
os.environ["EMMEPATH"] = "C:\\Program Files\\Bentley\\OpenPaths\\EMME 24.01.00"
```

3) tm2py uses EMME's matrix API for matrix calculation. If the user gets the error below in transit skimming, it suggests the Python library `numexpr` version is not consistent with what EMME uses. The user can upgrade or rollback their `numexpr` to 2.7.3 for EMME 4.6 and 4.7.
```python
File "c:\users\wangs1\documents\github\tm2py\tm2py\components\network\transit\transit_skim.py", line 341, in _calc_xfer_wait
    num_processors=self.controller.num_processors,
  File "standard\inro\emme\matrix_calculation\matrix_calculator.py", line 212, in __call__
  File "standard\inro\emme\matrix_calculation\matrix_calculator.py", line 246, in _matrix_calculator
  File "inro\emme\procedure\parallelmatrixcalculation.py", line 101, in __init__
inro.emme.core.exception.Error: Current environment is inadequate to perform a parallel matrix calculation
```

## Contributing

Details about contributing can be found on our documentation website: [https://bayareametro.github.io/tm2py/contributing](https://bayareametro.github.io/tm2py/contributing)
