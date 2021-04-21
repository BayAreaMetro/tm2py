# Documentation

Documentation is developed using the Python package [Sphinx](https://www.sphinx-doc.org/).

## Installing

1. Along with all over development tools ( recommended )
```sh
pip install -r dev-requirements.txt
```

2. Using pip
```sh
pip install sphinx sphinx-autodoc-typehints sphinx_rtd_theme
```

2. Using conda
```sh
conda install sphinx sphinx-autodoc-typehints sphinx_rtd_theme
```

## Developing

...

## Building

Sphinx documentation webpages can be built using the following shell command from the `docs` folder:
```sh
make html
```

## Deploying

Documentation is built and deployed to {EDITME} upon the master branch successfully passing continuous integration tests.
