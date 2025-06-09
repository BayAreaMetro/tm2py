# Documentation

Documentation is developed using the Python package [mkdocs](https://www.mkdocs.org/).

## Installing

Running the Travel Model via tm2py involves running from an Emme virtual environment (see [run]())

Assuming you don't want to deal with installing documentation-related packages into that virtual environment, then you might want to make an environment for documentation:

```powershell
# Create the environment, specifying the location environment
# This is useful since MTC virtual machines typically have small C drives and big E drives
(base) PS E:\> conda create python=3.11.9 --prefix E:\conda\envs\tm2py-docs
# Activate that environment
(base) PS E:\> conda activate E:\conda\envs\tm2py-docs
# Navigate to your tm2py clone for which you want to build docs
(tm2py-docs) PS E:\>cd E:\GitHub\tm2\tm2py\
# Install tm2py in editable mode; this should install requirements as well
(tm2py-docs) PS E:\GitHub\tm2\tm2py> pip install -e .
# Install docs requirements
(tm2py-docs) PS E:\GitHub\tm2\tm2py> pip install -r .\docs\requirements.txt
```

## Building Locally

Mkdocs documentation webpages can be built locally and viewed at the URL specified in the terminal:

```sh
mkdocs serve
```

## Linting

Documentation should be linted before deployment:

```sh
pre-commit run --all-files
pre-commit run --hook-stage manual --all-files
```

## Deploying documentation

Documentation is built and deployed to [http://bayareametro.github.io/tm2py] using the [`mike`](https://github.com/jimporter/mike) package and Github Actions configured in `.github/workflows/` for each "ref" (i.e. branch) in the tm2py repository.
