# Documentation Contributing Guide 🎂

Documentation for tm2py is built using [MkDocs](https://www.mkdocs.org/) with the Material theme and automatically deployed via GitHub Actions to [https://bayareametro.github.io/tm2py](https://bayareametro.github.io/tm2py).

## Quick Start for Documentation Updates 🚀

### Simple Edits ✏️

For simple edits (fixing typos, updating content), you can edit directly on GitHub:

**Quick Overview:** Go to GitHub → Switch to `develop` branch → Edit file → Commit → Wait 2-3 minutes for live site update

1. **Make sure you're on the `develop` branch** (GitHub Actions only triggers from develop)
2. Navigate to the file in the `docs/` folder on GitHub
3. Click the pencil icon to edit
4. Make your changes
5. Commit with a descriptive message **to the `develop` branch**
6. The site will automatically rebuild and deploy within 2-3 minutes

### Local Documentation Setup 💻

For more complex changes or when adding new pages, set up a local development environment:

**Quick Overview for New Pages:** Clone repo → Switch to `develop` → Create `.md` file in `docs/` → Add to `mkdocs.yml` navigation → Push to `develop`

#### Adding Pages to Navigation

To make your new page appear in the site navigation, edit `mkdocs.yml` and add your page to the `nav` section:

```yaml
nav:
  - Home: index.md
  - Installation: install.md
  - Your New Section: your-new-page.md
  # Or nested under an existing section:
  - Contributing:
    - Development: contributing/development.md
    - Documentation: contributing/documentation.md
    - Your New Guide: contributing/your-new-guide.md
```

**Important**: The file path in `mkdocs.yml` should be relative to the `docs/` folder (e.g., `your-page.md` not `docs/your-page.md`).

#### Prerequisites
- Python 3.8+ 
- Git

#### Installation Steps

```powershell
# Clone the repository (if you haven't already)
git clone https://github.com/BayAreaMetro/tm2py.git
cd tm2py

# Switch to the develop branch (required for deployment)
git checkout develop

# Create a dedicated environment for documentation work
# This keeps docs dependencies separate from the main tm2py environment
conda create -n tm2py-docs python=3.11
conda activate tm2py-docs

# Install documentation requirements
pip install -r docs/requirements.txt

# Optional: Install tm2py in editable mode for API documentation generation
pip install -e .
```

#### Making and Pushing Changes

Once you have the local environment set up, here's how to make documentation changes:

```powershell
# 1. Make sure you're on the develop branch
git status
git checkout develop

# 2. Edit documentation files with your preferred editor
# For example, edit docs/install.md, docs/outputs.md, etc.

# 3. Check what files you've changed
git status

# 4. Add your changes to git
git add docs/

# 5. Commit your changes with a descriptive message
git commit -m "Update installation instructions with new requirements"

# 6. Push to the develop branch to trigger automatic deployment
git push origin develop
```

**That's it!** GitHub Actions will automatically build and deploy your changes to the live documentation site within 2-3 minutes.

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

Documentation deployment is **fully automated** and reliable:

1. **Push to `develop` branch** → Triggers GitHub Actions workflow
2. **Workflow builds site** → Uses MkDocs with minimal dependencies (no GDAL conflicts)
3. **Deploys via GitHub Pages** → Uses official GitHub Pages deployment action
4. **Live in 2-3 minutes** → Updates <https://bayareametro.github.io/tm2py>

### Automatic Deployment ✅

The GitHub Actions workflow (`.github/workflows/docs.yml`) is now optimized and should work reliably:

- **No GDAL dependencies** → Minimal requirements prevent conflicts
- **Proper permissions** → Uses official GitHub Pages deployment actions
- **Environment isolation** → Builds only what's needed for documentation

**Key improvements made:**

- Removed problematic MkDocs watch directive that caused GDAL imports
- Uses `actions/deploy-pages@v4` instead of direct git push
- Includes proper GitHub Pages permissions: `pages: write`, `id-token: write`

### Manual Deployment Options

While automatic deployment should work reliably, you still have manual options:

#### Option 1: Trigger Workflow via Github actions

1. Go to **Actions** tab on GitHub
2. Select **"Publish docs"** workflow  
3. Click **"Run workflow"** → Choose `develop` branch
4. Wait 2-3 minutes for completion

#### Option 2: Local Deployment (Backup Method)

If you need immediate deployment or GitHub Actions is temporarily unavailable:

```powershell
# Make sure you have the tm2py-docs environment activated
conda activate tm2py-docs

# Deploy directly from your local machine
mkdocs gh-deploy --clean
```

**Note**: Local deployment bypasses the GitHub Actions workflow but produces the same result.

### Troubleshooting Deployment

#### Common Solutions

**Documentation not updating after push**:

- Check the **Actions** tab for workflow status
- Wait 3-5 minutes (GitHub Pages can have a delay)
- Clear browser cache or try incognito mode

**GitHub Actions workflow failing**:

- Most previous issues have been resolved with the new workflow
- Check the Actions tab for specific error messages
- Try triggering the workflow manually (Option 1 above)

**Local deployment fails with git errors**:

```powershell
git config --global user.name "Your Name"
git config --global user.email "your.email@example.com"
mkdocs gh-deploy --clean
```

#### Verifying Successful Deployment

After any deployment:

1. Visit <https://bayareametro.github.io/tm2py/>
2. Check that your changes are visible  
3. Test navigation to any new pages
4. Verify all links work correctly
5. Check the commit timestamp at the bottom of pages

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

1. **Ensure you're on the `develop` branch** on GitHub web interface
2. Edit files directly on GitHub
3. Commit with descriptive message **to the `develop` branch**
4. Wait 2-3 minutes for automatic deployment

### For Larger Changes

1. Clone the repository if you haven't already
2. **Switch to and work on the `develop` branch** (required for deployment)
3. Make changes locally with your preferred text editor
4. Commit changes: `git commit -m "Improve installation documentation"`
5. **Push to develop**: `git push origin develop`
6. Documentation will auto-deploy within 2-3 minutes

**Important**: Only changes pushed to the `develop` branch will trigger the GitHub Actions workflow that deploys the documentation to GitHub Pages.

This simplified workflow ensures that documentation stays up-to-date and maintains high quality while being easy to contribute to.

## Optional: Local Development Server 💻

If you want to preview your changes locally before committing (recommended for complex changes), you can run a local development server:

### Prerequisites for Local Preview

- Have completed the installation steps above
- Activated the tm2py-docs environment

### Running Local Preview

```powershell
# Activate your docs environment
conda activate tm2py-docs

# Navigate to the tm2py directory
cd path/to/tm2py

# Start the local development server
mkdocs serve
```

This will:

- Start a local server at `http://127.0.0.1:8000`
- Automatically reload when you make changes to documentation files
- Let you verify formatting, links, and navigation before pushing
- Show you exactly what the deployed site will look like

**Note**: This is completely optional. You can edit documentation files directly and rely on the automatic deployment to see your changes live.

