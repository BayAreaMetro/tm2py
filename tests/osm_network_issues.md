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

### ❌ CURRENT ISSUE

**Missing Network Attribute: `@tollbooth`**

**Error Location:** `highway_network.py` line 201 in `_set_tolls()`

**Problem:** The code expects `@tollbooth` attribute on links, but OSM network doesn't have this attribute.

**Code attempting:**
```python
link["@tollbooth"] > 0
```

**Next Steps:**
1. Create all missing network attributes with default values
2. Modify prepare_network_highway to handle missing attributes gracefully
3. Document which attributes OSM network has vs. what TM2 expects

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
