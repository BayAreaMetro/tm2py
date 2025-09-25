---
name: Set up separate API documentation workflow with GDAL support
about: Create a dedicated workflow for generating API documentation that properly handles GDAL dependencies
title: "Setup separate API documentation workflow with GDAL support"
labels: ["documentation", "infrastructure", "enhancement"]
assignees: []
---

## Summary
Create a separate GitHub Actions workflow specifically for API documentation generation that properly handles GDAL and other complex dependencies, allowing the main documentation workflow to remain fast and reliable.

## Background
The current documentation workflow fails when `mkdocstrings` tries to import tm2py modules that have GDAL dependencies. We've temporarily disabled API documentation to keep the main docs building, but we need a robust solution for generating comprehensive API docs.

## Proposed Solution
Create a dedicated `.github/workflows/api-docs.yml` workflow that:

1. **Uses conda instead of pip** for better GDAL dependency management
2. **Runs separately** from main documentation workflow  
3. **Generates API docs** and merges them with existing documentation
4. **Handles complex dependencies** that pip can't resolve reliably

## Technical Implementation Details

### 1. Workflow Structure
```yaml
name: Build API Documentation
on:
  workflow_dispatch:
  push:
    branches: [develop]
    paths: 
      - 'tm2py/**/*.py'
      - 'docs/api.md'
      - '.github/workflows/api-docs.yml'
  schedule:
    - cron: '0 2 * * 0'  # Weekly on Sunday at 2 AM
```

### 2. Environment Setup Requirements
```yaml
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
    conda install -c conda-forge libgdal-dev
```

### 3. Python Dependencies Installation
```yaml
- name: Install tm2py with dependencies
  shell: bash -l {0}
  run: |
    pip install -e .
    pip install -r docs/requirements.txt
```

### 4. API Documentation Generation Strategy

**Option A: Full mkdocstrings Integration**
- Re-enable mkdocstrings plugin in a separate mkdocs-api.yml config
- Generate API docs with full introspection
- Merge with main documentation site

**Option B: Standalone API Generation**
- Use `pdoc` or `sphinx-autoapi` for API generation
- Convert output to markdown compatible with mkdocs
- Include in main documentation build

### 5. Deployment Strategy

**Two-Stage Deployment:**
1. **Main docs workflow** (fast, no GDAL) - builds user guides, input/output docs
2. **API docs workflow** (slower, with GDAL) - builds API reference
3. **Merge step** - combines both outputs and deploys to gh-pages

## Required Files

### 1. `.github/workflows/api-docs.yml`
Main workflow file with conda environment setup and GDAL installation.

### 2. `mkdocs-api.yml` 
Separate MkDocs config for API documentation:
```yaml
site_name: TM2py API Documentation
plugins:
  - mkdocstrings:
      handlers:
        python:
          paths: [tm2py]
          options:
            show_source: true
            show_root_heading: true
```

### 3. `docs/requirements-api.txt`
Extended requirements including API generation tools:
```
mkdocs
mkdocstrings[python]
mkdocstrings-python
# Alternative options:
# pdoc3
# sphinx-autoapi
```

### 4. Update `docs/requirements.txt`
Remove mkdocstrings-related packages from main requirements to keep main workflow lightweight.

## Implementation Steps

### Phase 1: Basic API Workflow
- [ ] Create `.github/workflows/api-docs.yml` with conda environment
- [ ] Test GDAL installation in GitHub Actions
- [ ] Verify tm2py package imports successfully
- [ ] Generate basic API documentation

### Phase 2: Integration
- [ ] Create separate mkdocs config for API docs
- [ ] Set up artifact sharing between workflows
- [ ] Test API documentation generation

### Phase 3: Deployment Integration
- [ ] Modify main docs workflow to accept API docs artifact
- [ ] Create merge step to combine documentation
- [ ] Deploy unified documentation to gh-pages

### Phase 4: Optimization
- [ ] Add caching for conda environment
- [ ] Optimize build times with dependency caching
- [ ] Add error handling and fallback options

## Alternative Approaches

### Option 1: Docker-based approach
```yaml
- name: Build API docs in Docker
  uses: docker://condaforge/mambaforge
  with:
    args: |
      mamba install gdal geopandas -y
      pip install -e .
      mkdocs build -f mkdocs-api.yml
```

### Option 2: Self-hosted runner
- Use self-hosted runner with pre-installed GDAL
- Faster builds, more control over environment
- Requires infrastructure maintenance

### Option 3: Pre-built environment
- Create and maintain a Docker image with all dependencies
- Use as base image in workflow
- Requires image maintenance but faster startup

## Expected Benefits

1. **Comprehensive API Documentation**: Full tm2py API reference with examples
2. **Reliable Main Docs**: Fast, dependency-free documentation builds
3. **Better Developer Experience**: IDE-like navigation of code documentation
4. **Automated Updates**: API docs stay current with code changes

## Testing Strategy

### Local Testing
```bash
# Test API doc generation locally
conda create -n tm2py-api-test python=3.9
conda activate tm2py-api-test
conda install -c conda-forge gdal
pip install -e .
pip install mkdocstrings[python]
mkdocs serve -f mkdocs-api.yml
```

### CI Testing
- Test in GitHub Actions with matrix of Python versions
- Verify GDAL installation across different OS (ubuntu, windows)
- Check generated documentation quality

## Success Criteria

- [ ] API documentation builds successfully in GitHub Actions
- [ ] All tm2py modules are documented with proper cross-references  
- [ ] Documentation includes code examples and parameter descriptions
- [ ] Build time is reasonable (< 15 minutes)
- [ ] Main documentation workflow remains fast (< 5 minutes)
- [ ] Combined documentation is properly integrated and navigable

## Potential Challenges

1. **GDAL Installation Complexity**: Different approaches needed for different OS
2. **Import Dependency Chain**: Some modules may have circular imports
3. **Build Time**: API generation can be slow with large codebases
4. **Memory Usage**: Code introspection may require significant memory
5. **Version Compatibility**: Ensuring GDAL/Python/conda versions align

## References

- [conda-incubator/setup-miniconda](https://github.com/conda-incubator/setup-miniconda)
- [MkDocs Material API documentation](https://squidfunk.github.io/mkdocs-material/setup/setting-up-navigation/#navigation-sections)
- [mkdocstrings Python handler](https://mkdocstrings.github.io/python/)
- [GDAL conda-forge installation](https://anaconda.org/conda-forge/gdal)

## Implementation Priority

**High Priority**: Basic API workflow that can generate documentation
**Medium Priority**: Integration with main documentation workflow  
**Low Priority**: Performance optimizations and advanced features

---

**Labels**: `documentation`, `infrastructure`, `enhancement`, `api`
**Milestone**: Documentation Infrastructure  
**Estimated Effort**: 2-3 days for basic implementation, 1 week for full integration