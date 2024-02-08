# Starting Out

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

Notes:

1. The -e installs it in editable mode.
2. If you are not part of the project team and want to contribute code bxack to the project, please fork before you clone and then add the original repository to your upstream origin list per [these directions on github](https://help.github.com/en/articles/fork-a-repo).
3. if you wanted to install from a specific tag/version number or branch, replace `@main` with `@<branchname>`  or `@tag`
4. If you want to make use of frequent developer updates for network wrangler as well, you can also install it from clone by copying the instructions for cloning and installing Lasso for Network Wrangler

If you are going to be doing development, we also recommend:
 -  a good IDE such as [VS Code](https://code.visualstudio.com/), Sublime Text, etc.
 with Python syntax highlighting turned on.
 - [GitHub Desktop](https://desktop.github.com/) to locally update your clones

## Brief Intro


### Typical Workflow



## Running Quickstart Jupyter Notebooks

To learn basic lasso functionality, please refer to the following jupyter notebooks in the `/notebooks` directory:

- ADDME

 Jupyter notebooks can be started by activating the lasso conda environment and typing `jupyter notebook`:

 ```bash
 conda activate tm2py
 jupyter notebook
 ```
