# OSM San Mateo Network Investigation Summary
**Date:** January 9, 2026

## Network Location
```
M:\Development\Travel Model Two\Supply\Network Creation 2025\from_OSM\SanMateo\7_scenario\emme\emme_project
```

## Key Findings

### ✅ What Exists
- EMME project file: `emme_project.emp` (valid, opens successfully)
- Database: `Database_highway/emmebank`
- **6 scenarios:**
  - Scenario 1: "drive, all day" (base network)
  - Scenarios 11-15: Time-of-day networks (EA, AM, MD, PM, EV)
- **Network geometry:**
  - 91,807 links
  - 44,091 nodes  
  - 4,865 zones
  - Standard EMME fields: `type`, `length`, `num_lanes`, `volume_delay_func`

### ❌ What's Missing - CRITICAL ISSUE
**ZERO user-defined @ attributes on links or nodes**

The network has no TM2-specific attribute coding. Missing attributes include:
- `@ft` - functional type
- `@lanes` - lane count
- `@capclass` - capacity class  
- `@free_flow_speed` - free flow speed
- `@drive_link` - driveable link indicator
- `@tollbooth`, `@tollseg`, `@useclass` - toll attributes
- And ~15+ other @ attributes required by TM2

### Impact
**Cannot run TM2 model components without attribute initialization.**

The `prepare_network_highway` component fails immediately with:
```
KeyError: 'Required network attributes missing: @capclass, @lanes, @ft, @free_flow_speed. 
These must exist in the base EMME network before prepare_network_highway can run.'
```

## Questions for Network Creator

1. **Is this the correct/final network to use?**
   - Or is there a coded version elsewhere?
   - Is attribute coding a separate step that hasn't been done yet?

2. **Is there documentation on the OSM → TM2 network workflow?**
   - What's the process for coding attributes?
   - Are there mapping tables (OSM type → TM2 functional type)?

3. **Are there other network locations to check?**
   - We found: `from_OSM/` and `from_2015v12/`
   - BayArea network also exists in `from_OSM/BayArea/`

4. **What was the intended workflow?**
   - Was there supposed to be an attribute initialization script?
   - Should we build one, or does one exist?

## Test Framework Status

### ✅ What's Working
- Config file structure (updated for split EMME/inputs paths)
- Prerequisite checks
- EMME Desktop initialization  
- Network loading
- Comprehensive error logging and diagnostics

### 🔧 What's Ready
- Enhanced logging for missing attributes
- Defensive coding for optional attributes (tolls, HOV)
- Clear error messages showing exactly what's missing

### ⏸️ What's Blocked
- Cannot proceed with any model runs until attributes are initialized

## Next Steps (Pending Decision)

**Option A:** Use coded network if available
- Get path to coded network from creator
- Update config and test

**Option B:** Initialize attributes ourselves  
- Create initialization component
- Map OSM fields to TM2 attributes
- Requires ~2-3 hours development

**Option C:** Test with existing regional network first
- Validate test framework works
- Come back to OSM network later

## Files Created/Modified
- `tests/county_test_config.toml` - Split paths for EMME/inputs
- `tests/run_county_test.py` - Uses separate paths
- `tests/inspect_emme_network.py` - Network inspection tool
- `tests/quick_network_check.py` - Fast scenario checker
- `tests/list_network_attributes.py` - Attribute lister
- `tests/osm_network_issues.md` - Issue tracking
- `tm2py/components/network/highway/highway_network.py` - Added defensive attribute checks and logging
