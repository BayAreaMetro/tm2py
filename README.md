# Travel Model 2 Python Package

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


## Basic Usage


## Contributing

Details can be found in [CONTRIBUTING]