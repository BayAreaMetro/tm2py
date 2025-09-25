# GitHub Issue: Setup Separate API Documentation Workflow with GDAL Support

## Title
Setup separate API documentation workflow with GDAL support

## Labels
`documentation`, `infrastructure`, `enhancement`

## Description

### Problem
The current documentation workflow fails when `mkdocstrings` tries to import tm2py modules that have GDAL dependencies. We've temporarily disabled API documentation to keep the main docs building.

### Solution
Create a dedicated GitHub Actions workflow for API documentation that properly handles GDAL dependencies using conda instead of pip.

### Implementation Plan

#### 1. Create `.github/workflows/api-docs.yml`
```yaml
name: Build API Documentation
on:
  workflow_dispatch:
  push:
    branches: [develop]
    paths: ['tm2py/**/*.py', 'docs/api.md']

jobs:
  api-docs:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Setup Miniconda
        uses: conda-incubator/setup-miniconda@v2
        with:
          auto-update-conda: true
          python-version: 3.9
          channels: conda-forge,defaults
          
      - name: Install GDAL and dependencies
        shell: bash -l {0}
        run: |
          conda install -c conda-forge gdal geopandas
          pip install -e .
          pip install mkdocstrings[python] mkdocs-material
          
      - name: Generate API Documentation
        shell: bash -l {0}
        run: |
          mkdocs build -f mkdocs-api.yml
          
      - name: Upload API docs artifact
        uses: actions/upload-artifact@v3
        with:
          name: api-docs
          path: site-api/
```

#### 2. Create `mkdocs-api.yml`
Separate config file for API documentation with mkdocstrings enabled.

#### 3. Modify main docs workflow
Update main workflow to download and merge API docs artifact before deployment.

### Benefits
- ✅ Comprehensive API documentation with proper GDAL support
- ✅ Fast main documentation builds (no GDAL dependencies)
- ✅ Automated API updates when code changes
- ✅ Better developer experience with full code introspection

### Implementation Steps
1. [ ] Create basic API docs workflow with conda environment
2. [ ] Test GDAL installation and tm2py imports
3. [ ] Create separate mkdocs config for API generation
4. [ ] Integrate with main documentation deployment
5. [ ] Add caching and optimization

### Alternatives Considered
- **Docker approach**: Use pre-built container with GDAL
- **Self-hosted runner**: More control but requires maintenance
- **Separate documentation site**: Keep API docs completely separate

### Testing
```bash
# Local testing
conda create -n tm2py-api-test python=3.9
conda activate tm2py-api-test
conda install -c conda-forge gdal
pip install -e .
pip install mkdocstrings[python]
mkdocs serve -f mkdocs-api.yml
```

### Success Criteria
- [ ] API documentation builds successfully in GitHub Actions
- [ ] All tm2py modules documented with cross-references
- [ ] Combined with main docs and deployed to gh-pages
- [ ] Build completes in reasonable time (<15 minutes)

---

**Estimated Effort**: 2-3 days for basic implementation
**Priority**: Medium (nice to have but not blocking main documentation)