# tm2py Documentation

This directory contains the source files for the tm2py documentation website, which is automatically built and deployed to [https://bayareametro.github.io/tm2py](https://bayareametro.github.io/tm2py).

## Quick Start

### For Simple Edits
Edit `.md` files directly on GitHub - the site rebuilds automatically.

### For Local Development
```powershell
# Set up development environment (one-time setup)
conda create -n tm2py-docs python=3.11
conda activate tm2py-docs
pip install -r docs/requirements.txt

# Start development server
mkdocs serve
# Open http://127.0.0.1:8000 in your browser
```

## Documentation Structure

- `index.md` - Homepage
- `install.md`, `run.md` - Getting started guides  
- `inputs.md`, `outputs.md` - Data documentation
- `guide.md`, `process.md` - Detailed model documentation
- `architecture.md`, `api.md` - Technical documentation
- `contributing/` - Contribution guides

## Contributing

See [contributing/documentation.md](contributing/documentation.md) for detailed instructions on:
- Setting up local development
- Writing and formatting documentation
- Adding new pages
- Testing changes
- Deployment process

## Automatic Deployment

Documentation is automatically built and deployed when changes are pushed to the `develop` branch. The GitHub Actions workflow uses MkDocs with the Material theme to generate a professional-looking static site.

## Help

- **MkDocs Documentation**: https://www.mkdocs.org/
- **Material Theme**: https://squidfunk.github.io/mkdocs-material/
- **Markdown Guide**: https://www.markdownguide.org/
