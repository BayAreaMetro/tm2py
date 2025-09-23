# Documentation Contributing Guide 🎂

Documentation for tm2py is built using [MkDocs](https://www.mkdocs.org/) with the Material theme and automatically deployed via GitHub Actions to [https://bayareametro.github.io/tm2py](https://bayareametro.github.io/tm2py).

## Quick Start for Documentation Updates 🚀

### Simple Edits ✏️

For simple edits (fixing typos, updating content), you can edit directly on GitHub:

1. Navigate to the file in the `docs/` folder on GitHub
2. Click the pencil icon to edit
3. Make your changes
4. Commit with a descriptive message
5. The site will automatically rebuild and deploy within 2-3 minutes

### Local Development Setup 💻

For more complex changes or when adding new pages, set up a local development environment:

#### Prerequisites
- Python 3.8+ 
- Git

#### Installation Steps

```powershell
# Clone the repository (if you haven't already)
git clone https://github.com/BayAreaMetro/tm2py.git
cd tm2py

# Create a dedicated environment for documentation work
# This keeps docs dependencies separate from the main tm2py environment
conda create -n tm2py-docs python=3.11
conda activate tm2py-docs

# Install documentation requirements
pip install -r docs/requirements.txt

# Optional: Install tm2py in editable mode for API documentation generation
pip install -e .
```

#### Running the Local Development Server

```powershell
# Activate your docs environment
conda activate tm2py-docs

# Navigate to the tm2py directory
cd path/to/tm2py

# Start the local development server
mkdocs serve
```

This will start a local server at `http://127.0.0.1:8000` that automatically reloads when you make changes to the documentation files.

## Documentation Structure

```
docs/
├── index.md                 # Homepage
├── install.md              # Installation guide
├── run.md                  # Running the model
├── inputs.md               # Input data documentation
├── outputs.md              # Output documentation
├── architecture.md         # System architecture
├── api.md                  # API reference
├── server-setup.md         # Server configuration
├── guide.md                # Detailed user guide (from TM2.1)
├── process.md              # Model process details
├── geographies.md          # Geographic information
├── network_qa.md           # Network quality assurance
├── papers.md               # Research papers
├── contributing/           # Contributing guides
│   ├── development.md
│   └── documentation.md
├── css/                    # Custom styling
├── images/                 # Documentation images
└── requirements.txt        # Documentation dependencies
```

## Documentation Formats

### Markdown Files (.md)
All documentation content is written in Markdown with some extensions:

- **Standard Markdown**: Headers, lists, links, emphasis
- **Tables**: GitHub-flavored table syntax
- **Code blocks**: Fenced code blocks with syntax highlighting
- **Admonitions**: Special callout boxes (see below)

#### Admonitions (Callout Boxes)
```markdown
!!! note
    This is a note admonition

!!! warning
    This is a warning admonition

!!! info
    This is an info admonition
```

#### Code Documentation
For Python code documentation, we use docstrings that are automatically generated:

```python
def example_function(param1: str, param2: int = 0) -> bool:
    """Brief description of the function.
    
    Longer description if needed.
    
    Args:
        param1: Description of parameter 1
        param2: Description of parameter 2, defaults to 0
        
    Returns:
        Description of what is returned
        
    Example:
        ```python
        result = example_function("hello", 42)
        ```
    """
    return True
```

## Common Tasks 🛠️

### Adding a New Page 📄

1. Create a new `.md` file in the `docs/` directory
2. Add content using Markdown
3. Update `mkdocs.yml` to include the page in navigation:

```yaml
nav:
  - Home: index.md
  - Your New Section:
    - New Page: your-new-page.md
```

### Adding Images 🖼️

1. Place images in `docs/images/` directory
2. Reference in Markdown: `![Alt text](images/your-image.png)`
3. For better organization, create subdirectories: `docs/images/section-name/`

### Updating Navigation

Edit the `nav` section in `mkdocs.yml`:

```yaml
nav:
  - Home: index.md
  - Section Name:
    - Page Title: filename.md
    - Another Page: another-file.md
```

### Cross-referencing Between Pages

Use relative links to reference other documentation pages:
```markdown
See the [Installation Guide](install.md) for setup instructions.
See the [API Documentation](api.md#specific-function) for details.
```

## Testing Your Changes 🧪

### Local Preview 👀
Always preview your changes locally before committing:
```powershell
mkdocs serve
```

### Building the Full Site
To build the complete site (what will be deployed):
```powershell
mkdocs build
```
This creates a `site/` directory with the complete HTML site.

### Linting Documentation
Run pre-commit hooks to check for issues:
```powershell
pre-commit run --all-files
```

## Deployment Process 🚀

Documentation deployment is fully automated:

1. **Push to `develop` branch** → Triggers GitHub Actions workflow
2. **Workflow runs** → Builds the MkDocs site
3. **Deploys to GitHub Pages** → Updates https://bayareametro.github.io/tm2py
4. **Takes 2-3 minutes** → Site is live

### Manual Deployment (if needed)
If you need to manually trigger deployment:
1. Go to the repository's Actions tab on GitHub
2. Select "Publish docs" workflow
3. Click "Run workflow"

## Style Guidelines

### Writing Style
- Use clear, concise language
- Write in present tense
- Use active voice when possible
- Define technical terms on first use

### Formatting
- Use descriptive headers (H1 `#`, H2 `##`, etc.)
- Break up large blocks of text with headers and lists
- Use code blocks for commands and code samples
- Include examples where helpful

### File Naming
- Use lowercase with hyphens: `network-qa.md`
- Be descriptive: `installation-guide.md` not `install.md`
- Group related files in subdirectories when appropriate

## Troubleshooting

### Common Issues

**MkDocs not found**: Make sure you've activated the right environment
```powershell
conda activate tm2py-docs
```

**Missing dependencies**: Reinstall requirements
```powershell
pip install -r docs/requirements.txt
```

**Site not updating**: Clear the site cache
```powershell
mkdocs build --clean
```

**GitHub Actions failing**: Check the Actions tab for error details

### Getting Help

- **MkDocs Documentation**: [mkdocs.org](https://www.mkdocs.org/)
- **Material Theme**: [squidfunk.github.io/mkdocs-material](https://squidfunk.github.io/mkdocs-material/)
- **Markdown Guide**: [markdownguide.org](https://www.markdownguide.org/)

## Contributing Workflow

### For Small Changes

1. Edit directly on GitHub web interface
2. Commit with descriptive message
3. Wait 2-3 minutes for deployment

### For Larger Changes

1. Clone the repository if you haven't already
2. Work directly on the `develop` branch
3. Make changes locally and test with `mkdocs serve`
4. Commit changes: `git commit -m "Improve installation documentation"`
5. Push to develop: `git push origin develop`
6. Documentation will auto-deploy within 2-3 minutes

This simplified workflow ensures that documentation stays up-to-date and maintains high quality while being easy to contribute to.
