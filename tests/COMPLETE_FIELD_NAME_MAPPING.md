# Complete Field Name Mapping for TM2PY Highway Test

## Critical Prerequisites

### Component Execution Order
**REQUIRED**: `create_tod_scenarios` must run before `prepare_network_highway`

Why: The `create_tod_scenarios` component copies time-period-specific attributes to generic names:
- Base scenario has: `@useclass_am`, `@useclass_ea`, `@useclass_md`, `@useclass_pm`, `@useclass_ev`
- TOD scenarios need: `@useclass` (generic, used by highway_network.py)
- `create_tod_scenarios` performs this copy operation

Without this step, you'll get `KeyError: '@useclass'` during toll calculations.

## Source Data Files

### 1. Tolls CSV (E:\2015_TM2_20250619\inputs\hwy\tolls.csv)
**Column Pattern**: `toll{period}_{vehicle}`
```
tollea_da, tollam_da, tollmd_da, tollpm_da, tollev_da
tollea_s2, tollam_s2, tollmd_s2, tollpm_s2, tollev_s2
tollea_s3, tollam_s3, tollmd_s3, tollpm_s3, tollev_s3
tollea_vsm, tollam_vsm, tollmd_vsm, tollpm_vsm, tollev_vsm
tollea_sml, tollam_sml, tollmd_sml, tollpm_sml, tollev_sml
tollea_med, tollam_med, tollmd_med, tollpm_med, tollev_med
tollea_lrg, tollam_lrg, tollmd_lrg, tollpm_lrg, tollpm_lrg, tollev_lrg
```

**Vehicle suffixes**: `da`, `s2`, `s3`, `vsm`, `sml`, `med`, `lrg`

### 2. EMME Base Network Attributes
**Time-Period-Specific Attributes** (in base scenario):
```
@useclass_am, @useclass_ea, @useclass_md, @useclass_pm, @useclass_ev
```
Values: 1=all vehicles, 2=HOV2+, 3=HOV3+, 4=autos only

**Generic Attributes** (created by create_tod_scenarios in TOD scenarios):
```
@useclass  # Copied from @useclass_{period} for each time period scenario
```

## Code-Generated Network Attributes

### From highway_network.py
The code creates these attributes dynamically based on `dst_vehicle_group_names`:

```python
# For each vehicle in dst_vehicle_group_names:
@bridgetoll_{vehicle}  # Bridge tolls
@valuetoll_{vehicle}   # Value (non-bridge) tolls
is_toll_{vehicle}      # Boolean: link has valuetoll > 0
```

## Config Requirements

### highway.tolls section
```toml
[highway.tolls]
file_path = "inputs/hwy/tolls.csv"
valuetoll_start_tollbooth_code = 1000000
src_vehicle_group_names = ["da", "s2", "s3", "vsm", "sml", "med", "lrg"]  # Must match CSV columns
dst_vehicle_group_names = ["da", "s2", "s3", "vsm", "sml", "med", "lrg"]  # Used in network attributes
```

### highway.classes entries
For each class, these fields must reference dst_vehicle_group_names:

```toml
veh_group_name = "da"  # Must be one of dst_vehicle_group_names
excluded_links = ["is_toll_da"]  # Pattern: is_toll_{veh_group_name}
toll = ["@bridgetoll_da"]  # Pattern: @bridgetoll_{veh_group_name} or @valuetoll_{veh_group_name}
```

## Validation Rules

### ✓ CORRECT Naming (s2/s3 pattern)
```toml
[[highway.classes]]
name = "SR2"
veh_group_name = "s2"  # ✓ Matches dst_vehicle_group_names
excluded_links = ["is_toll_s2"]  # ✓ Correct pattern: is_toll_s2
toll = ["@bridgetoll_s2"]  # ✓ Correct pattern: @bridgetoll_s2
```

### ✗ INCORRECT Naming (sr2/sr3 pattern - WRONG!)
```toml
[[highway.classes]]
name = "SR2"
veh_group_name = "sr2"  # ✗ Not in dst_vehicle_group_names
excluded_links = ["is_toll_sr2"]  # ✗ Won't be created by code
toll = ["@bridgetoll_sr2"]  # ✗ Won't be created by code
```

## Complete Expected Config Pattern

```toml
# Tolls configuration
[highway.tolls]
src_vehicle_group_names = ["da", "s2", "s3", "vsm", "sml", "med", "lrg"]
dst_vehicle_group_names = ["da", "s2", "s3", "vsm", "sml", "med", "lrg"]

# DA class
[[highway.classes]]
name = "DA"
veh_group_name = "da"
excluded_links = ["is_toll_da"]
toll = []

# DA with toll
[[highway.classes]]
name = "DATOLL"
veh_group_name = "da"
excluded_links = []
toll = ["@bridgetoll_da"]

# Shared ride 2
[[highway.classes]]
name = "SR2"
veh_group_name = "s2"  # ← s2, not sr2
excluded_links = ["is_toll_s2"]  # ← is_toll_s2, not is_toll_sr2
toll = []

# Shared ride 2 with toll
[[highway.classes]]
name = "SR2TOLL"
veh_group_name = "s2"  # ← s2, not sr2
excluded_links = []
toll = ["@bridgetoll_s2"]  # ← @bridgetoll_s2, not @bridgetoll_sr2

# Shared ride 3
[[highway.classes]]
name = "SR3"
veh_group_name = "s3"  # ← s3, not sr3
excluded_links = ["is_toll_s3"]  # ← is_toll_s3, not is_toll_sr3
toll = []

# Shared ride 3 with toll
[[highway.classes]]
name = "SR3TOLL"
veh_group_name = "s3"  # ← s3, not sr3
excluded_links = []
toll = ["@bridgetoll_s3"]  # ← @bridgetoll_s3, not @bridgetoll_sr3
```

## Summary

**Key Rule**: All vehicle references must use `s2`/`s3`, NEVER `sr2`/`sr3`

**Chain of consistency**:
1. CSV columns use `s2`/`s3` → `tollam_s2`, `tollam_s3`
2. Config src_vehicle_group_names = `["da", "s2", "s3", ...]`
3. Config dst_vehicle_group_names = `["da", "s2", "s3", ...]`
4. Code generates `is_toll_s2`, `@bridgetoll_s2`, `@valuetoll_s2`
5. Config classes reference `veh_group_name = "s2"`, `excluded_links = ["is_toll_s2"]`, `toll = ["@bridgetoll_s2"]`

**Any deviation breaks the chain and causes validation errors.**
