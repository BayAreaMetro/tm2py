# Travel Model 2 Python Package

A python package to run the San Francisco Bay Area's Travel Model.

**Owner:** Metropolitan Transportation Commission (MTC)

[![Tests](https://github.com/BayAreaMetro/tm2py/actions/workflows/test.yml/badge.svg?branch=develop)](https://github.com/BayAreaMetro/tm2py/actions/workflows/test.yml)

[![Documentation](https://github.com/BayAreaMetro/tm2py/actions/workflows/docs.yml/badge.svg?branch=develop)](https://github.com/BayAreaMetro/tm2py/actions/workflows/docs.yml)

[![Package Published](https://github.com/BayAreaMetro/tm2py/actions/workflows/publish.yml/badge.svg?branch=develop)](https://github.com/BayAreaMetro/tm2py/actions/workflows/publish.yml)

## Installation

It is recommended that tm2install in a virtual environment.

Stable (to come - use bleeding edge for now):
```bash
pip install tm2py
```

For Developers, it is recomended that the following instructions are used to install  
*Note: The Python Environment has recently been updated to python 3.11, there may be some instabilites with current build*
```bat
git clone --branch develop https://github.com/BayAreaMetro/tm2py.git

conda create -n tm2py python=3.11.9
conda activate tm2py
conda install gdal
conda install pyproj
conda install fiona
conda install shapely
conda install geopandas

cd <path to tm2py git directory>
git 
pip install -e .
conda env config vars set GDAL_VERSION=3.3.2
```
Finally, install the Emme python packages using the Emme GUI. This effectively creates a file,
`C:\Users\%USERNAME%\.conda\envs\tm2py\Lib\site-packages\emme.pth` with the following contents, so you could create the file yourself.

```python
import os, site; site.addsitedir(os.path.join(os.environ["EMMEPATH"], "Python311/Lib/site-packages"))
```

*This should start Emme OpenPath, if it does not you should be able to manually set the correct version of emme such as below*
```python
import os, site
os.environ["EMMEPATH"] = r"C:\Program Files\Bentley\OpenPaths\EMME 24.00.00"
site.addsitedir(os.path.join(os.environ["EMMEPATH"], "Python311/Lib/site-packages"))
```


In troubleshooting, sometimes DLL load failure errors would occur which may be resolved by importing gdal before importing emme packages. Emme support explained this thusly:

At load time, the EMME API will always load the geos_c co-located with the EMME API, unless it was already loaded from some other location, which is the case when you import GDAL first. EMME API seems to be compatible with the newer GDAL/geos_c (reminder: not tested!). But this does not appear to be the case the other way around (newer GDAL is not compatible with older geos_c).

Copy and unzip [example_union_test_highway.zip](https://mtcdrive.box.com/s/3entr016e9teq2wt46x1os3fjqylfoge) to a local
drive and from within that directory run:

```sh
get_test_data <location>
tm2py -s scenario.toml -m model.toml
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
os.environ["EMMEPATH"] = "C:\\Program Files\\INRO\\Emme\\Emme 4\\Emme-4.6.1"
```
When running the model verify that the correct version eof emme is opened.


![Correct Emme Version Logo](docs/images/emme_open_46.PNG)


## Contributing

Details about contributing can be found on our documentation website: [](https://bayareametro.github.io/tm2py/contributing)
