# OSM San Mateo Network - Issues and Solutions

## Test Run: January 9, 2026

### ✅ RESOLVED ISSUES

1. **Scenario ID** - FIXED
   - OSM network uses scenario 1 instead of 100
   - Updated config: `all_day_scenario_id = 1`

2. **EMME Project Filename** - FIXED  
   - OSM network uses `emme_project.emp` instead of `mtc_emme.emp`
   - Fixed in scenario.toml

3. **UTF-8 BOM in Config** - FIXED
   - Config file had BOM that caused TOML parsing error
   - Removed BOM from scenario.toml

### ❌ CURRENT ISSUE - CRITICAL

**OSM Network Has NO User-Defined Attributes**

**Discovery:** The OSM San Mateo network has ZERO @ attributes!
- Available: `type`, `length`, `num_lanes`, `data1/2/3`, `volume_delay_func`
- Missing: ALL @ attributes including `@ft`, `@lanes`, `@capclass`, `@free_flow_speed`, `@drive_link`, etc.

**Network Stats:**
- Links: 91,807
- Nodes: 44,091  
- Zones: 4,865

**Root Cause:** The OSM import process created a bare-bones EMME network without TM2-specific attribute coding.

**Solution Required:** 
We need to create an "OSM network initialization" component that:
1. Creates all required @ attributes
2. Maps standard EMME fields to @ attributes:
   - `num_lanes` → `@lanes`
   - `type` → `@ft` (with mapping table)
   - Calculate `@capclass` from functional type
   - Calculate `@free_flow_speed` from functional type
   - Set `@drive_link` = 1 for all links
   - Initialize toll attributes to 0
3. Runs BEFORE `create_tod_scenarios`

**Immediate Options:**
A. Create an initialization script to populate attributes in source network
B. Modify `prepare_network_highway` to handle missing attributes and create them
C. Use a different OSM network that has been properly coded

### 🔍 ATTRIBUTES TO INVESTIGATE

Standard TM2 attributes that may be missing:
- `@tollbooth` - toll plaza indicator
- `@ft` - functional type (need to verify attribute name in OSM)
- `@useclass` - usage class for vehicle restrictions
- `@lanecap` - lane capacity
- `@truckclass` - truck restrictions
- Various time-of-day specific attributes

### 📋 ACTION PLAN

1. **Inspect OSM network attributes** - Use inspection script on scenario 1
2. **Create attribute mapping** - Map OSM attributes to TM2 expectations
3. **Add attribute creation code** - Initialize missing attributes before use
4. **Make code more defensive** - Check for attribute existence before access
