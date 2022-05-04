# Documentation

Documentation is developed using the Python package [mkdocs](https://www.mkdocs.org/).

## Installing

The requirements for building the documentation are the same as those for the `tm2py` package.

## Building Locally

Mkdocs documentation webpages can be built locally and viewed at the URL specified in the terminal:

```sh
mkdocs serve
```

## Linting

Documentation should be linted before deployment:

```sh
pre-commit run --all-files
```

## Deploying documentation

Documentation is built and deployed to [http://bayareametro.github.io/tm2py] using the [`mike`](https://github.com/jimporter/mike) package and Github Actions configured in `.github/workflows/` for each "ref" (i.e. branch) in the tm2py repository.
