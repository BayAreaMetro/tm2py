# Travel Model 2 Python Package

## Installation

If you are managing multiple python versions, we suggest using [`virtualenv`](https://virtualenv.pypa.io/en/latest/) for your 
virtual environments. [`conda`](https://conda.io/en/latest/) can also be used but the installed libraries should be checked
against the Emme distribution for conflicts. Note that Emme versions 4.5 and 4.6 are compatible with Python 3.7, and 
come with their own Python installation.

The following instructions create and activate a venv environment (recommended) in which you can install. 
Note that for full compatibility with Emme these should be run from the Emme Shell after installing
the latest version. A git tool will need to be installed and available in your path. If you see an error
such as "ERROR: Cannot find command 'git' - do you have 'git' installed and in your PATH?" confirm git is 
installed and if so add it to your path using 
`set path=%path%;C:\Program Files\Git\bin\git.exe;C:\Program Files\Git\cmd`

You may need download and install GDAL and Fiona wheels separately if you do not have a C++ compiler available. 
You can find these on https://www.lfd.uci.edu/~gohlke/pythonlibs/, search for each of the packages, 
Python 3.7 (cp37-win_amd64) versions and pip install from path.


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
copy "%EMMEPATH%\emme.pth" <my_conda_environment>\Lib\site-packages\
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
If you are going to be working on tm2py locally, you might want to clone it to your local machine and install it from the clone.  
The -e will install it in [editable mode](https://pip.pypa.io/en/stable/reference/pip_install/?highlight=editable#editable-installs).


```bash
python -m venv venv
venv\scripts\activate
git clone https://github.com/bayareametro/tm2py
pip install -e tm2py
copy "%EMMEPATH%\emme.pth" venv\Lib\site-packages\
```


## Basic Usage


```python
import tm2py.controller as _controller

controller = _controller.Controller
    "example/scenario.toml",
    "example/model.toml"
)
controller.run()
```

## Contributing

Details can be found in [CONTRIBUTING]