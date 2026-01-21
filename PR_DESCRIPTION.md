# County Highway Test Framework

## Summary
This PR adds a **new** county-level highway testing framework and includes fixes to existing tm2py code discovered during development.

---

## Key Changes

### 1. 🆕 New County Test Framework
See section 4 below for details on the new test runner.

### 2. 📚 New Documentation (~2,500 lines)
**New testing documentation in `docs/testing/`:**
- **county-test-guide.md** (417 lines) - Complete guide for county-level highway testing
- **configuration.md** (360 lines) - Detailed configuration documentation
- **data-flow.md** (473 lines) - Visual data flow diagrams and explanations
- **emme-manager-flow.md** (262 lines) - EMME manager interaction flows
- **quick-start.md** (389 lines) - Quick start guide for new users
- **network-thinning.md** (202 lines) - Network thinning strategies
- **index.md** (87 lines) - Testing documentation index

**Also added:**
- **highway-network-processing.md** - Complete pipeline documentation with diagrams
- **field-name-mapping.md** (157 lines) - CTRAMP field mapping reference
- Updated **mkdocs.yml** with new testing section

### 3. 🛡️ Defensive Attribute Checking (Production Code)
**Enhanced `tm2py/components/network/highway/highway_network.py`** (+151/-4):

Added comprehensive attribute checking in 4 methods:

- **`_set_tolls()`**: Checks for `@tollbooth`, `@tollseg`, `@useclass`
  - Gracefully skips toll processing if missing (logs warning)
  - Useful for networks without toll facility coding

- **`_set_vdf_attributes()`**: Checks for `@capclass`, `@lanes`, `@ft`, `@free_flow_speed`
  - Raises clear KeyError if missing (critical attributes)
  
- **`_set_link_modes()`**: Checks for `@drive_link`
  - Raises clear KeyError if missing (critical attribute)
  
- **`_calc_link_skim_lengths()`**: Checks for `@useclass`, `@tollbooth`
  - Gracefully skips HOV/toll length calculations if missing

**Benefits:**
- Clear, actionable error messages instead of cryptic KeyErrors
- Helps diagnose network attribute initialization issues
- Backward compatible with existing networks
- Documents required vs optional attributes

### 3. 📊 Resource Monitoring
**Enhanced `tm2py/components/network/highway/highway_assign.py`** (+88/-4):

Added `monitor_resources()` context manager:
- Monitors CPU and memory usage during SOLA assignment
- Logs to console and separate `resource_monitor.log` file
- Updates every 5 minutes during long-running assignments
- Non-blocking threaded implementation

**Applied to:**
- SOLA assignment without path analysis
- SOLA assignment with path analysis

### 4. 🔧 New Test Framework
**New `tests/run_county_test.py`** - Complete county test runner:
- Prerequisite checking
- Enhanced logging and error handling
- Flexible configuration system

**New `tests/county_test_config.toml`** (102 lines):
- Centralized configuration
- Supports separate EMME project, inputs, and demand sources

**New `tests/highway_assign_skim_controller.py`**:
- Test controller for highway assignment

### 5. 🔍 Network Inspection Tools (New Utilities)
Created comprehensive network diagnostic tools:
- **inspect_emme_network.py** (257 lines) - Full network inspection
- **analyze_osm_network.py** (128 lines) - OSM network distribution analysis
- **list_network_attributes.py** (78 lines) - Attribute lister
- **quick_network_check.py** (76 lines) - Fast scenario checker
- **check_modes.py** (16 lines) - Mode validation
- **check_network_volumes.py** (53 lines) - Volume validation

### 6. 📝 OSM Network Investigation
Documentation of OSM network attribute investigation:
- **OSM_Network_Investigation_Summary.md** (101 lines) - Investigation findings
- **OSM_Network_Available_Attributes.md** (84 lines) - Attribute inventory
- **osm_network_issues.md** (66 lines) - Issue tracker

### 7. ⚙️ Configuration Templates
- New **fixed_san_mateo_model.toml** (972 lines) - Complete configuration
- New **fixed_san_mateo_scenario.toml** (76 lines) - Scenario config
- Binary capclass files added
- Empty placeholder files for tolls and interchange nodes
- Removed outdated san_mateo configs

### 8. 🧹 Documentation Cleanup
Removed outdated documentation (content moved to `docs/testing/`):
- `tests/COUNTY_TEST_FRAMEWORK_UPDATE.md` (205 lines)
- `tests/HIGHWAY_ASSIGN_SKIM_README.md` (405 lines)
- `tests/TESTING_INSTRUCTIONS.md` (185 lines)

### 9. 🐛 Minor Fixes
**`tm2py/emme/manager.py`** (1 line):
- Removed `.lower()` on time_period lookup (preserves case sensitivity)

---

## Testing
- ✅ San Mateo county highway assignment completes successfully (~24 min)
- ✅ Case-sensitivity fix validated with Sprint-04 data
- ✅ Defensive attribute checking validated with OSM network
- ✅ Resource monitoring tested during assignments
- ✅ All network inspection tools executed successfully
- ✅ Documentation builds correctly with mkdocs

## Backward Compatibility
- ✅ All changes are backward compatible
- ✅ Existing networks with proper attributes work as before
- ✅ Networks without attributes now get clear error messages instead of cryptic failures
- ✅ Both uppercase (`AM`) and lowercase (`am`) period names now work

## Dependencies
- Uses `psutil` for resource monitoring (already in requirements)
- Uses `threading` (stdlib)

---

## Review Notes
1. **Documentation**: Comprehensive testing docs now in proper location (`docs/testing/`)
2. **Error Messages**: Much clearer for users when network attributes are missing
3. **Debugging**: New inspection tools make network troubleshooting easier
4. **Monitoring**: Resource usage visibility during long-running assignments
5. **OSM Investigation**: Documents findings about OSM-derived networks lacking TM2 attributes

## Questions for Reviewers
1. Should the OSM investigation docs stay in `tests/` or move to `docs/`?
2. Any concerns about the defensive attribute checking approach?
3. Is the resource monitoring frequency (5 minutes) appropriate?

---

**Reviewer:** @lmz (Lisa Zorn)
