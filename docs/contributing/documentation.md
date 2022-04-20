# Documentation

Documentation is developed using the Python package [mkdocs](https://www.mkdocs.org/).

## Installing

Using pip:
```sh
pip install -r docs/requirements.txt
```
## Building Locally

Mkdocs documentation webpages can be built using the following shell command from the `docs` folder:
```sh
mkdocs build
mkdocs serve
```

## Deploying documentation

Documentation is built and deployed to [http://bayareametro.github.io/tm2py] upon the `develop` branch successfully passing continuous integration tests (to be updated to `master` when released) as specified in `.github/workflows/docs.yml`.
