# Travel Model 2 Python Package

[![Tests](https://github.com/BayAreaMetro/tm2py/actions/workflows/test.yml/badge.svg?branch=develop)](https://github.com/BayAreaMetro/tm2py/actions/workflows/test.yml)

[![Documentation](https://github.com/BayAreaMetro/tm2py/actions/workflows/docs.yml/badge.svg?branch=develop)](https://github.com/BayAreaMetro/tm2py/actions/workflows/docs.yml)

[![Package Published](https://github.com/BayAreaMetro/tm2py/actions/workflows/publish.yml/badge.svg?branch=develop)](https://github.com/BayAreaMetro/tm2py/actions/workflows/publish.yml)

## Installation

If you are managing multiple python versions, we suggest using [`virtualenv`](https://virtualenv.pypa.io/en/latest/) or [`conda`](https://conda.io/en/latest/) virtual environments.

The following instructions create and activate a conda environment (recommended) in which you can install:

```bash
conda env create -f environment.yml
conda activate tm2py
```

Basic installation instructions are as follows:

```bash
pip install tm2py
```

#### Bleeding Edge
If you want to install a more up-to-date or development version, you can do so by installing it from the `develop` branch as follows:

```bash
conda env create -f environment.yml
conda activate tm2py
pip install git+https://github.com/bayareametro/tm2py@develop
```

#### Developers (from clone)
If you are going to be working on Lasso locally, you might want to clone it to your local machine and install it from the clone.  The -e will install it in [editable mode](https://pip.pypa.io/en/stable/reference/pip_install/?highlight=editable#editable-installs).


```bash
conda env create -f environment.yml
conda activate tm2py
git clone https://github.com/bayareametro/tm2py
cd tm2py
pip install -e .
```

Note that you'll also need to install Emme's python packages into this conda environment.
Following these instructions from an INRO community forum post: In the Emme Desktop application, open Tools->Application Options->Modeller, change your Python path as desired and click the "Install Modeller Package" button.
Alternatively, you can copy the emme.pth from the Emme install to the site packages. This looks like:
```bash
cd tm2py
REM ... or from the root of the venv / conda environment
copy %EMMEPATH%\emme.pth Lib\site-packages\
```

If this is successful, the following packages will be visible in your environment when you type `pip list`:
* inro-dynameq
* inro-emme
* inro-emme-agent
* inro-emme-engine
* inro-modeller

Note that doing the emme package install will also install the package *pywin32*; if *pywin32* gets installed by other means (like
conda or pip), then I got DLL load errors when trying to import the emme packages, so I recommend uninstalling *pywin32* before
installing the emme packages.

## Basic Usage

Copy and unzip [example_union_test_highway.zip](https://mtcdrive.box.com/s/3entr016e9teq2wt46x1os3fjqylfoge) to a local
drive and from within that directory run:

```sh
get_test_data <location>
<path to tm2py>\bin\tm2py -s examples\scenario_config.toml -m examples\model.toml [-r <location>] 
```

The tm2py\bin directory can also be added to the system path in order to call tm2py directory from anywhere.

The -r run_dir option can be used in the config.toml files are not in the same root directory as the rest of the project data.
The model operation and details are controlled via these config files. See the Config section of the API doc for details.

There is also an example `Run model.ipynb` notebook under the notebooks directory which has this same example.

## Contributing

Details can be found in [CONTRIBUTING]