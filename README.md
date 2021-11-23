# Travel Model 2 Python Package

## Installation

If you are managing multiple python versions, we suggest using [`virtualenv`](https://virtualenv.pypa.io/en/latest/) for your 
 virtual environments. [`conda`](https://conda.io/en/latest/) can also be used but the installed libraries should be checked
against the Emme distribution for conflicts.

The following instructions create and activate a venv environment (recommended) in which you can install. 
Note that for full compatibility with Emme these should be run from the Emme Shell after installing
the latest version.

```bash
python -m venv venv
venv\scripts\activate
pip install git+https://github.com/bayareametro/tm2py
copy "%EMMEPATH%\emme.pth" venv\Lib\site-packages\
```

Basic conda installation instructions are as follows:

```bash
conda config --add channels conda-forge
conda create python=3.7  -n <my_conda_environment>
conda activate <my_conda_environment>
pip install git+https://github.com/bayareametro/tm2py
```

#### Bleeding Edge
If you want to install a more up-to-date or development version, you can do so by installing it from the `develop` branch as follows:

```bash
python -m venv venv
venv\scripts\activate
pip install git+https://github.com/bayareametro/tm2py@develop
copy "%EMMEPATH%\emme.pth" venv\Lib\site-packages\
```

#### Developers (from clone)
If you are going to be working on Lasso locally, you might want to clone it to your local machine and install it from the clone.  
The -e will install it in [editable mode](https://pip.pypa.io/en/stable/reference/pip_install/?highlight=editable#editable-installs).


```bash
python -m venv venv
venv\scripts\activate
git clone https://github.com/bayareametro/tm2py
cd tm2py
pip install -e .
cd ..
copy "%EMMEPATH%\emme.pth" venv\Lib\site-packages\
```


## Basic Usage


## Contributing

Details can be found in [CONTRIBUTING]