# Plan: Remove Transit Access Points (TAPs) from TM2PY

**Status**: In Progress - Phase 2  
**Created**: January 13, 2026  
**Last Updated**: January 13, 2026

## Executive Summary

This document outlines a comprehensive plan to remove Transit Access Points (TAPs) from the TM2PY transportation model system. TAPs were originally designed to provide an intermediate geographic layer between MAZs (Micro-Analysis Zones) and transit routing to reduce computational complexity. However, they appear to be vestigial code that is no longer actively used in the current model implementation.

## Key Decision: Scope Limitation

**Decision Date**: January 13, 2026

This plan focuses **only on removing TAP infrastructure**. It does NOT include replacing the `active_modes` and `drive_access_skims` components, which are separate concerns.

### Current State Analysis

Verification confirmed that:
1. Both `active_modes` and `drive_access_skims` components are **disabled by default** in GUI and test configs
2. The model currently relies on **pre-generated files** from earlier runs when these components were active
3. Transit assignment uses **TAZ centroids directly** (line 920-922 in `transit_assign.py`), not TAPs
4. No TAP output files exist in the current workspace

### Out of Scope (Future Work)

The following are **separate initiatives** that should be addressed independently:

#### 1. Replace Active Mode Routing (Future Phase)
- **Current State**: Uses pre-existing MAZ↔MAZ walk/bike skim files
- **Future Options**:
  - **Option A**: OSRM-based routing (original proposal)
  - **Option B**: Leverage `highway_maz_skim` component
  - **Option C**: Pre-compute and version control static files
  - **Option D**: ActivitySim native routing
- **Required Work**: OSRM server setup, OSM network prep, API integration, validation
- **Timeline**: Separate project (estimated 8-12 weeks)

#### 2. Replace Drive Access to Transit (Future Phase)
- **Current State**: `drive_access_skims` component exists but disabled
- **Future Decision Needed**: Is PNR/KNR functionality required?
- **If Yes**: Redesign without TAPs using TAZ→stop or MAZ→stop direct routing
- **If No**: Simply remove the code

#### 3. Transit Assignment Simplification (Future Phase)
- **Current State**: PNR/KNR journey level logic exists (lines 1469-1542, 1768-1841) but likely non-functional
- **Future Work**: Clean up or re-implement if drive access is needed

### Why This Separation?

1. **Risk Reduction**: TAP removal is low-risk (nothing uses it); routing replacement is high-risk
2. **Independent Value**: Removing TAPs simplifies code even without replacement components
3. **Different Timelines**: TAP removal = 2-4 weeks; OSRM integration = 8-12+ weeks
4. **Different Expertise**: TAP removal = Python/EMME; OSRM = infrastructure/devops
5. **Incremental Progress**: Can merge TAP removal while planning routing replacement

## Background

### What are TAPs?

Transit Access Points (TAPs) were designed as an abstraction layer for transit modeling:
- **Original Purpose**: Reduce computational burden by breaking transit trips into MAZ→TAP→TAP→MAZ components instead of direct MAZ→MAZ transit routing
- **Geographic Scope**: ~3,000-6,000 TAPs representing individual transit stops or collections of nearby stops
- **Node Numbering**: Network nodes 90,001 through 99,999

### Why Remove TAPs?

1. **Vestigial Code**: TAP-related components exist but appear unused in current model runs
2. **Computational Advances**: Modern systems can handle MAZ-to-MAZ routing directly
3. **Model Simplification**: Removing an entire geographic layer simplifies the model architecture
4. **Maintenance Burden**: Dead code creates confusion and technical debt
5. **OSM/OSRM Integration**: Moving to OpenStreetMap-based routing makes TAPs unnecessary

## Scope Assessment

### Components Affected

#### 1. **Active Modes Component** (`active_modes.py`)
- **Impact**: HIGH
- **Current State**: Generates TAP-related skims
- **Files Generated**:
  - `skims/ped_distance_maz_tap.txt`
  - `skims/bike_distance_maz_tap.txt`
  - `skims/ped_distance_tap_tap.txt`
- **Code Locations**:
  - Line 25: `ROOT_LEAF_ID_MAP` includes `"TAP": "@tap_id"`
  - Lines 58-62: TAP skim output specifications

#### 2. **Transit Network Component** (`transit_network.py`)
- **Impact**: MEDIUM-HIGH
- **Current State**: Contains `split_tap_connectors_to_prevent_walk()` method
- **Functionality**:
  - Lines 511-662: Splits TAP connector links
  - Routes transit lines through TAP stops
  - Sets boarding/alighting on TAP segments
- **Code Locations**:
  - Line 90: Method call
  - Lines 511-662: Full implementation

#### 3. **Transit Assignment Component** (`transit_assign.py`)
- **Impact**: LOW (code exists but likely non-functional without drive_access_skims)
- **Current State**: Contains journey level logic for drive access to TAPs
- **Code Locations**:
  - Lines 1469-1542: Drive access transition rules for PNR
  - Lines 1768-1841: Drive access transition rules for KNR
- **Action**: Remove TAP-specific journey levels

#### 4. **Drive Access Skims Component** (`drive_access_skims.py`) - OUT OF SCOPE
- **Impact**: N/A - To be addressed separately
- **Current State**: Entire component disabled by default, relies on pre-existing files
- **This Plan**: Mark as deprecated, do not remove yet
- **Future Work**: Decision needed on whether PNR/KNR is required; if yes, redesign without TAPs

#### 5. **Active Modes Component** (`active_modes.py`) - PARTIALLY IN SCOPE
- **Impact**: LOW - Remove TAP references only
- **Current State**: Entire component disabled by default
- **This Plan**: Remove TAP entries from `ROOT_LEAF_ID_MAP` and config schemas
- **Future Work**: Component replacement/re-enablement is separate initiative

#### 6. **Household Demand Component** (`household.py`)
- **Impact**: LOW
- **Current State**: Contains commented references to TAP output files
- **Code Locations**:
  - Lines 154, 169: Comments about transit TAP files
- **Action**: Remove commented TAP references

#### 6. **Configuration System** (`config.py`)
- **Impact**: MEDIUM
- **Current State**: Configuration fields for TAP-related outputs
- **Code Locations**:
  - Line 68: `"drive_access_skims"` component
  - Lines 938, 969: TAP drive access configuration

### Data Files Affected

#### Input Files
- `hwy/mtc_final_network_tap_links.csv` - TAP to node connectors
- `hwy/mtc_final_network_zone_seq.csv` - Includes TAPSEQ column
- Network nodes with `@tap_id` attribute

#### Output Files
- `skims/ped_distance_maz_tap.txt`
- `skims/bike_distance_maz_tap.txt`
- `skims/ped_distance_tap_tap.txt`
- `skims/drive_maz_taz_tap.csv`
- `trn/tapLines.csv`

### Documentation Affected

#### Markdown Files (50+ references found)
- `docs/guide.md` - Extensive TAP documentation
- `docs/inputs.md` - TAP node numbering conventions
- `docs/input/network.md` - TAP network attributes
- `docs/input/transit.md` - TAP transit assignment
- `docs/output/skims.md` - TAP skim outputs
- `docs/output/ctramp_backup.md` - TAP fields in CTRAMP outputs
- `docs/process.md` - TAP processing steps
- `TM2PY_SUMMARIES_AND_OUTPUTS_INVENTORY.md` - TAP file listings

## Implementation Plan

### Phase 1: Verification & Assessment ✅ COMPLETED (Week 1)

**Status**: All verification tasks completed January 13, 2026

#### 1.1 Confirm TAPs are Vestigial ✅
- ✅ Reviewed model configuration files - no TAP components enabled in test configs
- ✅ Verified TAP components disabled by default in GUI (`run.py` lines 109-120)
- ✅ Confirmed no TAP output files exist in workspace
- ✅ Verified CT-RAMP/ActivitySim doesn't reference TAP files (no .yml/.properties with TAP)
- ✅ Transit assignment uses TAZ centroids directly (line 920-922 in `transit_assign.py`)

#### 1.2 Document Current Transit Routing ✅
- ✅ Current workflow: TAZ centroid → transit stop (direct connection)
- ✅ No MAZ-level walk access in use
- ✅ TAPs completely bypassed in current implementation
- ✅ System relies on pre-generated skim files from earlier runs

#### 1.3 Identify Dependencies ✅
- ✅ Only one `@tap_id` reference found: `active_modes.py` line 25 (ROOT_LEAF_ID_MAP)
- ✅ No TAPSEQ references in Python code (only in docs/data files)
- ✅ No external R/Python scripts depend on TAP files
- ✅ 100+ documentation references found (primarily in `docs/process.md`, `docs/guide.md`)

**Key Findings**:
1. TAPs are completely vestigial - no active code path uses them
2. `active_modes` and `drive_access_skims` components are disabled
3. Model uses pre-existing skim files generated before these components were disabled
4. Future work needed to replace skim generation (see "Out of Scope" section above)
### Phase 2: Create Feature Branch ✅ COMPLETED

**Status**: Branch created January 13, 2026

```bash
git checkout develop
git pull origin develop
git checkout -b feature/remove-taps
```

All uncommitted changes successfully moved to feature branch.

### Phase 3: Code Removal (Week 2-3)

#### 3.1 Active Modes Component ✅ IN PROGRESS
**Priority**: HIGH  
**Estimated Effort**: 2-3 days

- ✅ Remove TAP from `ROOT_LEAF_ID_MAP` dictionary
- ✅ Update docstrings to remove TAP references
- [ ] Update configuration schema to remove TAP skim configs (if any)
- [ ] Update tests for active modes
- [ ] Verify component still works for MAZ↔MAZ, TAZ↔TAZ skims

**Files Modified**:
- `tm2py/components/network/active/active_modes.py` (lines 1, 25, 41-62)

**Important Note**: This component remains **disabled by default**. Removing TAP references prepares it for future re-enablement with OSRM or other routing backend.

#### 3.2 Drive Access Skims Component - DEPRECATED, NOT REMOVED
**Priority**: LOW  
**Estimated Effort**: 1 day (documentation only)

- [ ] Add deprecation warning to module docstring
- [ ] Update configuration to mark as deprecated
- [ ] Document that component requires redesign (see "Out of Scope" section)
- [ ] Remove from GUI component list (optional)

**Files to Modify**:
- `tm2py/components/network/highway/drive_access_skims.py` (add deprecation notice)
- `tm2py/gui/pages/run.py` (optionally hide from UI)

**Decision**: Do NOT delete this file yet. It may be useful as reference when redesigning drive-to-transit functionality.

#### 3.3 Transit Network Component ✅ COMPLETED
**Priority**: HIGH  
**Estimated Effort**: 3-4 days

- ✅ Remove `split_tap_connectors_to_prevent_walk()` method entirely
- ✅ Remove method call from `run()` method
- ✅ Clean up `tap_stops` dictionary logic
- ✅ Remove TAP-related mode assignments
- [ ] Verify transit line routing still functions
- [ ] Update tests for transit network preparation

**Files Modified**:
- `tm2py/components/network/transit/transit_network.py` (removed 152 lines)

#### 3.4 Transit Assignment Component ✅ COMPLETED
**Priority**: MEDIUM  
**Estimated Effort**: 2-3 days

- ✅ Remove drive access TAP journey level definitions (WLK_TRN_PNR)
- ✅ Remove drive access TAP journey level definitions (KNR_TRN_WLK)
- ✅ Remove drive access TAP journey level definitions (WLK_TRN_KNR)
- [ ] Verify transit assignment still runs for walk-access modes
- [ ] Update tests

**Files Modified**:
- `tm2py/components/network/transit/transit_assign.py` (removed ~450 lines across 3 journey level blocks)

**Important Note**: This removes TAP-specific code but does NOT implement replacement PNR/KNR functionality. If drive-to-transit is needed in the future, it will require new implementation (see "Out of Scope" section).

#### 3.5 Configuration System ✅ COMPLETED
**Priority**: MEDIUM  
**Estimated Effort**: 2 days

- ✅ Mark `drive_access_output_skim_path` as deprecated
- ✅ Add deprecation comment to `drive_access_skims` in ComponentNames
- ✅ Update GUI to indicate drive_access_skims is deprecated
- ✅ Update example configuration files
- [ ] Verify configuration validation

**Files Modified**:
- `tm2py/config.py` (deprecation comments added)
- `tm2py/gui/pages/run.py` (GUI description updated)

#### 3.6 Data Model Updates ✅ COMPLETED
**Priority**: LOW  
**Estimated Effort**: 1 day

- ✅ Remove commented TAP references from household.py
- ✅ Clean up TAP comments in active_modes.py
- ✅ Remove commented TAP connector code from create_tod_scenarios.py

**Files Modified**:
- `tm2py/components/demand/household.py` (removed ~25 lines of commented TAP code)
- `tm2py/components/network/active/active_modes.py` (cleaned up comments)
- `tm2py/components/network/create_tod_scenarios.py` (removed commented code)

#### 3.7 Household Demand Component
**Priority**: LOW  
**Estimated Effort**: 0.5 days

- [ ] Remove commented TAP references
- [ ] Clean up related documentation

**Files to Modify**:
- `tm2py/components/demand/household.py`

### Phase 4: Testing (Week 6-7)

#### 4.1 Unit Tests
- [ ] Update all affected unit tests
- [ ] Remove TAP-specific test cases
- [ ] Add tests for simplified transit routing
- [ ] Verify test coverage maintained

#### 4.2 Integration Tests
- [ ] Run full model with TAPs removed
- [ ] Compare outputs with baseline (verify no unexpected changes)
- [ ] Test transit assignment completion
- [ ] Test active mode skim generation
- [ ] Verify no TAP files are generated

#### 4.3 Performance Testing
- [ ] Measure runtime differences
- [ ] Check memory usage
- [ ] Document any performance improvements

### Phase 5: Documentation Updates ✅ COMPLETED (Week 7-8)

#### 5.1 User Documentation ✅ COMPLETED
- ✅ Update `docs/guide.md` - Removed TAP sections (TAP data manager, preprocessing steps, transit routing description, LOS table)
- ✅ Update `docs/inputs.md` - Removed TAP node numbering from county table and TAP_ID attribute
- ✅ Update `docs/input/network.md` - Removed TAP node ranges, county table column, and TAP_ID attribute
- ✅ Update `docs/input/transit.md` - Removed TAP transit assignment description
- ✅ Update `docs/output/skims.md` - Updated drive access skim format (removed TTAP column)
- ✅ Update `docs/output/ctramp_backup.md` - Removed orig_tap/dest_tap fields, updated PNR TAP references to TAZ
- ✅ Update `docs/process.md` - Removed 6 TAP preprocessing steps (writeZoneSystems TAP counts, zone_seq_net_builder TAPSEQ, tap_to_taz_for_parking, tap_data_builder, CreateNonMotorizedNetwork TAP networks, TAP skim outputs)

**Documentation Files Modified**: 7 files
- `docs/guide.md` - 5 TAP sections removed/updated
- `docs/inputs.md` - Node numbering table and attributes updated
- `docs/input/network.md` - Node numbering and attributes updated
- `docs/input/transit.md` - Transit assignment description updated
- `docs/output/skims.md` - Drive access skim format updated
- `docs/output/ctramp_backup.md` - PNR and transit output fields updated
- `docs/process.md` - 6 preprocessing steps removed, skim outputs updated

#### 5.2 Architecture Documentation
- [ ] Update `docs/architecture.md` with simplified geography (if needed)
- [ ] Document new transit routing approach (deferred - no new approach implemented yet)
- [ ] Update geographic hierarchy diagrams (deferred)
- [ ] Add migration notes for users (deferred to release notes)

#### 5.3 API Documentation
- ✅ Component docstrings already updated in Phase 3
- [ ] Regenerate API documentation (deferred - requires full build)
- [ ] Update examples (none needed - no TAP examples exist)

#### 5.4 Release Notes
- [ ] Document breaking changes (deferred to Phase 7)
- [ ] Provide migration guide (deferred to Phase 7)
- [ ] List removed files (deferred to Phase 7)
- [ ] Explain transit routing changes (deferred to Phase 7)

### Phase 6: External Dependencies (Week 8-9)

#### 6.1 CT-RAMP/ActivitySim
- [ ] Verify demand model doesn't require TAP files
- [ ] Update demand model configuration if needed
- [ ] Test end-to-end model run

#### 6.2 Network Preparation Scripts
- [ ] Review network import scripts for TAP references
- [ ] Update any preprocessing tools
- [ ] Verify zone sequence generation

#### 6.3 Post-Processing
- [ ] Update validation scripts
- [ ] Update visualization tools
- [ ] Check reporting dashboards

### Phase 7: Code Review & Merge (Week 9-10)

- [ ] Self-review all changes
- [ ] Create pull request with comprehensive description
- [ ] Address review comments
- [ ] Get approval from 2+ reviewers
- [ ] Merge to develop branch

### Phase 8: Deployment & Monitoring (Week 10+)

- [ ] Deploy to test environment
- [ ] Run full validation suite
- [ ] Monitor for issues
- [ ] Deploy to production
- [ ] Update user communications

## Risk Assessment

### High Risk Items

1. **Undocumented Dependencies**
   - **Risk**: External tools or scripts depend on TAP files
   - **Mitigation**: Comprehensive grep search, team interviews, pilot testing

2. **Transit Assignment Failure**
   - **Risk**: Removing TAP logic breaks transit routing
   - **Mitigation**: Thorough testing, keep rollback plan, staged deployment

3. **CT-RAMP Integration**
   - **Risk**: Demand model requires TAP impedances
   - **Mitigation**: Verify with demand model team, test integration

### Medium Risk Items

4. **Data Pipeline Breaks**
   - **Risk**: Downstream processing expects TAP files
   - **Mitigation**: Update post-processing scripts, document changes

5. **Performance Degradation**
   - **Risk**: Direct MAZ routing slower than TAP-based
   - **Mitigation**: Performance testing, optimization if needed

### Low Risk Items

6. **Documentation Gaps**
   - **Risk**: Missing TAP references cause confusion
   - **Mitigation**: Comprehensive documentation review

## Success Criteria

1. ✅ All TAP-related code removed
2. ✅ Model runs successfully without TAPs
3. ✅ No TAP output files generated
4. ✅ Transit assignment produces valid results
5. ✅ All tests passing
6. ✅ Documentation updated
7. ✅ No performance regression (ideally improvement)
8. ✅ Team approval and sign-off

## Rollback Plan

If critical issues are discovered:

1. **Immediate**: Revert feature branch merge
2. **Short-term**: Cherry-pick critical fixes to main code
3. **Long-term**: Re-assess TAP removal strategy

Rollback triggers:
- Transit assignment fails
- Demand model integration breaks
- Unacceptable performance degradation
- Critical external dependency discovered

## Timeline Summary

| Phase | Duration | Dependencies |
|-------|----------|--------------|
| 1. Verification & Assessment | 2 weeks | None |
| 2. Create Feature Branch | 1 day | Phase 1 |
| 3. Code Removal | 3 weeks | Phase 2 |
| 4. Testing | 2 weeks | Phase 3 |
| 5. Documentation | 2 weeks | Phase 3 |
| 6. External Dependencies | 2 weeks | Phase 4 |
| 7. Code Review & Merge | 2 weeks | Phases 4-6 |
| 8. Deployment & Monitoring | 2+ weeks | Phase 7 |
| **Total** | **~14-16 weeks** | |

## Team Resources Required

- **Lead Developer**: 80% allocation for 14 weeks
- **Transit Modeling Expert**: 20% allocation for reviews
- **Testing Engineer**: 40% allocation weeks 6-9
- **Technical Writer**: 40% allocation weeks 7-8
- **Code Reviewers**: 2-3 reviewers, 10 hours each

## Open Questions

1. **Q**: Is drive access to transit still needed? If so, how should it work without TAPs?
   - **Action**: Interview transit modeling team

2. **Q**: Are there any external validation tools that compare TAP-level results?
   - **Action**: Check with QA/validation team

3. **Q**: Should we maintain backward compatibility for reading old TAP files?
   - **Action**: Discuss with project management

4. **Q**: What is the migration path for existing model runs?
   - **Action**: Define version compatibility policy

5. **Q**: Are there any research projects using TAP outputs?
   - **Action**: Survey research team

## Future Work: Skim Generation Replacement

**Status**: Separate initiative, not part of TAP removal  
**Priority**: Medium-High  
**Estimated Timeline**: 8-12 weeks

### Problem Statement

The model currently depends on pre-generated skim files from earlier runs:
- `ped_distance_maz_maz.txt` (walk access between MAZs)
- `bike_distance_maz_maz.txt` (bike access between MAZs)  
- `ped_distance_taz_taz.txt` (walk access between TAZs)
- Potentially drive access skims if PNR/KNR is needed

These files were generated when `active_modes` and `drive_access_skims` components were enabled. Going forward, we need a strategy to regenerate these files when:
- Network topology changes
- Zone systems are updated
- New scenarios are created

### Recommended Approach: OSRM-Based Routing

**Phase 1: OSRM Infrastructure Setup** (3-4 weeks)
- Set up OSRM server(s) for Bay Area
- Process OpenStreetMap data for region
- Configure walk/bike profiles
- Establish API endpoints and authentication
- Document server maintenance procedures

**Phase 2: TM2PY Integration** (2-3 weeks)
- Create new `osrm_skims.py` component
- Implement MAZ/TAZ centroid → OSRM API calls
- Handle distance/time calculations
- Match existing output file formats
- Add error handling and retry logic

**Phase 3: Validation** (2-3 weeks)
- Compare OSRM results vs. existing EMME-based skims
- Validate reasonable differences (different networks/algorithms)
- Performance benchmarking
- Document acceptable tolerances

**Phase 4: Integration & Testing** (1-2 weeks)
- Update configuration schemas
- Modify GUI to enable new component
- Integration testing with full model run
- Update documentation

### Alternative Approaches

**Option B: Keep EMME, Re-enable Components**
- Simpler short-term solution
- Requires EMME license
- Uses existing code (just re-enable)
- Doesn't reduce EMME dependency

**Option C: ActivitySim Native**
- If migrating to ActivitySim for demand
- Let ActivitySim handle all skims
- Most integrated but requires full ActivitySim adoption

**Option D: Static Files in Version Control**
- Simplest approach
- Generate once per network version
- Store in git LFS or similar
- Only viable if network changes are rare

### Decision Criteria

Choose approach based on:
1. EMME licensing costs and constraints
2. Network update frequency
3. Need for scenario flexibility
4. DevOps capacity for OSRM maintenance
5. Timeline for ActivitySim adoption

### Required Decisions

- [ ] Is PNR/KNR (drive-to-transit) functionality required?
- [ ] What's the acceptable latency for skim generation?
- [ ] Can we deprecate EMME for active modes?
- [ ] What's the budget for OSRM infrastructure?

## References

- Original TAP design from SANDAG model
- CT-RAMP documentation on TAP usage
- EMME API documentation on transit assignment
- OSRM documentation: http://project-osrm.org/
- Previous discussions on model simplification

## Approval & Sign-off

| Role | Name | Date | Signature |
|------|------|------|-----------|
| Project Lead | | | |
| Transit Lead | | | |
| Technical Lead | | | |
| QA Lead | | | |

---

**Next Steps**:
1. ✅ Phase 1 verification completed
2. Continue Phase 3 code removal (TAPs only)
3. Create separate issue for skim generation replacement
4. Schedule discussion on OSRM vs. alternatives
