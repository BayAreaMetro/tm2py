
# mkdocs.yml Integration Instructions

To integrate the CT-RAMP documentation into tm2py, add the following section to the `nav:` section of `C:\GitHub\tm2py\mkdocs.yml`:

```yaml
nav:
  - Home: index.md
  - Getting Started:
    - Server Setup: server-setup.md
    - Installing tm2py: install.md
    - Running tm2: run.md
  - Process: process.md
  - Codebase:
    - Architecture: architecture.md
  - CT-RAMP Models:  # <-- ADD THIS SECTION
    - Overview: ctramp/index.md
    - System Architecture: ctramp/architecture.md  
    - Model Components:
      - Components Overview: ctramp/components/index.md
      - Auto Ownership: ctramp/components/auto-ownership.md
      - Daily Activity Pattern: ctramp/components/cdap.md
      - Mandatory Tours: ctramp/components/mandatory-tours.md
      - Joint Tours: ctramp/components/joint-tours.md
      - Individual Tours: ctramp/components/individual-tours.md
      - Tour Destination: ctramp/components/tour-destination.md
      - Tour Mode Choice: ctramp/components/tour-mode-choice.md
      - Tour Time-of-Day: ctramp/components/tour-tod.md
      - Stop Frequency: ctramp/components/stop-frequency.md
      - Stop Location: ctramp/components/stop-location.md
      - Trip Mode Choice: ctramp/components/trip-mode-choice.md
      - At-Work Subtours: ctramp/components/at-work-subtours.md
    - UEC Framework:
      - UEC Overview: ctramp/uec/index.md
      - Technical Framework: ctramp/uec/framework.md
      - UEC Files Reference: ctramp/uec/files.md
      - Usage Examples: ctramp/uec/examples.md
    - Execution:
      - Execution Overview: ctramp/execution/index.md
      - Workflow Guide: ctramp/execution/workflow.md
      - Configuration: ctramp/execution/configuration.md
      - Troubleshooting: ctramp/execution/troubleshooting.md
    - Data:
      - Input Requirements: ctramp/data/inputs.md
      - Output Specifications: ctramp/data/outputs.md
      - Data Validation: ctramp/data/validation.md
      - File Formats: ctramp/data/formats.md
    - Examples: ctramp/examples/index.md
  - Input & Output:
    - Input: input/index.md
    # ... rest of existing navigation
```

## Cross-Reference Updates

Also update these existing files to reference the new CT-RAMP documentation:

1. **docs/index.md**: Add CT-RAMP overview section
2. **docs/process.md**: Link to detailed CT-RAMP workflow
3. **docs/architecture.md**: Reference CT-RAMP architecture  
4. **docs/output/ctramp.md**: Link to detailed CT-RAMP data documentation
