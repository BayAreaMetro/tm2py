# Setup Component - Testing TODO

## Status: Implementation Complete ✅, Testing In Progress

### Completed:
- ✅ Added "setup" to ComponentNames in config.py
- ✅ Created Setup component class (components/setup.py)
- ✅ Integrated into controller.py component_cls_map
- ✅ Backwards compatibility verified (old configs work without changes)
- ✅ Documentation created (docs/setup-component-integration.md)

### Testing TODO:

#### 1. County Network Test (IN PROGRESS)
**Goal**: Verify setup component works with county test framework
- [ ] Test that setup component doesn't break county test workflow
- [ ] Verify backwards compatibility with existing county tests
- [ ] Document any county-specific considerations

#### 2. Full Model Testing (REQUIRED BEFORE MERGE)
**Goal**: Validate setup component in production-like full model runs

##### Test Scenarios:
- [ ] **Fresh run with setup component**
  - Create new test directory from scratch
  - Add "setup" to initial_components in scenario_config.toml
  - Run full model with setup as first component
  - Verify files copied correctly
  - Verify EMME networks initialized properly
  - Verify subsequent components run successfully

- [ ] **Existing run without setup component**
  - Use existing model run directory (files already in place)
  - Run model WITHOUT "setup" in initial_components
  - Verify backwards compatibility (should work exactly as before)
  - Confirm no errors or warnings about missing setup

- [ ] **Error handling tests**
  - Test missing setupmodel_config.toml (should error with clear message)
  - Test invalid setupmodel_config.toml (should error with clear message)
  - Test missing input source directories (should error with clear message)

##### Test Environments:
- [ ] Test on Windows development machine
- [ ] Test on modeling server (if applicable)
- [ ] Test with Box sync vs local files
- [ ] Test with different model years (2015, 2035, etc.)

##### Performance & Integration:
- [ ] Verify setup logging works correctly
- [ ] Check setup.log is created in run directory
- [ ] Verify setup progress is reported in main model log
- [ ] Test setup with warmstart enabled/disabled
- [ ] Test setup.verify() method works correctly

#### 3. Documentation Review
- [ ] Review and update docs/setup-component-integration.md based on test results
- [ ] Add examples from actual test runs
- [ ] Update troubleshooting section with any issues found
- [ ] Add performance notes (how long does setup take?)

#### 4. Code Review Items
- [ ] Review error messages for clarity
- [ ] Check for any edge cases in file path handling
- [ ] Verify SetupModel integration is robust
- [ ] Consider adding progress bars for large file copies (future enhancement)

### Known Issues / Considerations:
- Setup component requires setupmodel_config.toml in run directory (not configurable path yet)
- No dry-run mode for setup (shows what would be copied without copying)
- File validation helper exists but not automatically called (design decision for backwards compatibility)

### Before Merging:
1. ✅ Backwards compatibility verified
2. ⏳ County network testing complete
3. ⏳ Full model testing complete (all scenarios above)
4. ⏳ Documentation updated with test results
5. ⏳ Code review complete
6. ⏳ PR created with comprehensive description

### Notes:
- Current focus: County network test
- After county test: Switch to full model testing
- Keep this file updated as testing progresses
