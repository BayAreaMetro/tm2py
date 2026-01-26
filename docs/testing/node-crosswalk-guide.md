# MOVED TO docs/input/node-crosswalks.md

## Overview

The node crosswalk system maps between different node ID numbering schemes used in TM2 network building and model execution. This ensures consistent geographic referencing across the modeling pipeline.

## Why Node Crosswalks Exist

### The Problem
Transportation models use different tools at different stages:
1. **Network Building** (Lasso): Creates networks with "model node IDs" 
2. **EMME Software**: Uses its own internal "EMME node IDs"
3. **Travel Model**: Needs sequential zone IDs (1, 2, 3...) for efficient matrix operations

### The Solution
Two crosswalk files bridge these numbering systems:
- **Input Crosswalk**: Maps model IDs to EMME IDs (from network build)
- **Output Crosswalk**: Maps to sequential zone IDs (created by TM2)

## The Two Crosswalk Files

### 1. model_to_emme_node_id_xwalk (INPUT)

**File**: `emme_project/Database_highway/emme_drive_network_node_id_crosswalk.csv`

**Source**: Created during Lasso → EMME network build process

**Format**:
```csv
emme_node_id,model_node_id
1,1
2,2
3,3
...
```

**Purpose**: 
- Maps original model node IDs (from Lasso network build) to EMME node IDs
- Usually these are identical (1:1 mapping)
- Created when EMME imports the network
- Stored alongside the EMME database

**Config Setting**:
```toml
[highway]
model_to_emme_node_id_xwalk = "emme_project/Database_highway/emme_drive_network_node_id_crosswalk.csv"
```

### 2. output_node_sequential_id_xwalk (OUTPUT)

**File**: `inputs/landuse/mtc_final_network_zone_seq.csv` (or `inputs/hwy/mtc_final_network_zone_seq.csv`)

**Source**: Either pre-computed during network build OR created by TM2 using `model_to_emme_node_id_xwalk`

**Format**:
```csv
N,TAZSEQ,MAZSEQ,TAPSEQ,EXTSEQ
1,1,0,0,0
2,2,0,0,0
3,3,0,0,0
10001,0,1,0,0
10002,0,2,0,0
...
```

**Columns**:
- `N`: Original node ID (model_node_id)
- `TAZSEQ`: Sequential TAZ (Traffic Analysis Zone) ID (1-based, 0 if not a TAZ)
- `MAZSEQ`: Sequential MAZ (Micro Analysis Zone) ID (1-based, 0 if not a MAZ)
- `TAPSEQ`: Sequential TAP (Transit Access Point) ID (1-based, 0 if not a TAP) - deprecated
- `EXTSEQ`: Sequential external zone ID (1-based, 0 if not external)

**Purpose**: 
- Provides sequential zone numbering for efficient matrix operations
- Used to validate MAZ land use data geographic consistency
- Different node types identified by ID ranges (defined in maz_data.py)

**Config Setting**:
```toml
[highway]
output_node_sequential_id_xwalk = "inputs/landuse/mtc_final_network_zone_seq.csv"
```

## How Crosswalks Are Used

### During MAZ Data Loading

When `controller.maz_data` is accessed:

1. **Load the input crosswalk** (`model_to_emme_node_id_xwalk`)
2. **Create sequential IDs** by calling `create_sequential_index()`
   - Categorize nodes into TAZ/MAZ/External based on ID ranges
   - Assign sequential IDs (1, 2, 3...) within each category
3. **Validate MAZ data** against crosswalk:
   - Check `MAZ_NODE` → `MAZSEQ` mapping matches
   - Check `TAZ_NODE` → `TAZSEQ` mapping matches
   - Log warning if mismatches found (allows testing with different network versions)

!!! note "Zone ID Naming Convention"
    MAZ data uses a standardized naming convention:
    
    - `MAZ_SEQ` / `TAZ_SEQ` - Sequential IDs for matrix operations
    - `MAZ_NODE` / `TAZ_NODE` - Network node IDs for geographic referencing
    
    See [Land Use Zone ID Naming Convention](../input/landuse.md#zone-id-naming-convention) for details.

```python
# From controller.py
@property
def maz_data(self):
    if self._maz_data is None:
        # Load crosswalk
        xwalk_file = self.get_abs_path(
            self.config.highway.model_to_emme_node_id_xwalk
        )
        crosswalk = create_sequential_index(xwalk_file)
        
        # Load and validate MAZ data
        maz_data_file = self.get_abs_path(self.config.scenario.landuse_file)
        self._maz_data = load_maz_data(maz_data_file, crosswalk)
    return self._maz_data
```

### During create_tod_scenarios

The `_set_area_type()` method needs MAZ data to calculate link area types based on nearby land use density. This triggers MAZ data loading which requires the crosswalk.

## Node ID Ranges (TM2.2)

Defined in `tm2py/data_models/maz_data.py`:

```python
# External zones (gateway zones for external trips)
external_N_list = list(range(900001, 1000000))

# TAZ (Traffic Analysis Zones) - ranges by county
taz_N_list = (
    list(range(1, 10000))           # Base TAZ range
    + list(range(100001, 110000))   # County-specific ranges
    + list(range(200001, 210000))
    # ... more county ranges
)

# MAZ (Micro Analysis Zones) - finer resolution
maz_N_list = (
    list(range(10001, 90000))       # Base MAZ range
    + list(range(110001, 190000))   # County-specific ranges
    + list(range(210001, 290000))
    # ... more county ranges
)
```

Example node categorization:
- Node 1 → TAZ (TAZSEQ=1, MAZSEQ=0, EXTSEQ=0)
- Node 10001 → MAZ (TAZSEQ=0, MAZSEQ=1, EXTSEQ=0)
- Node 900001 → External (TAZSEQ=0, MAZSEQ=0, EXTSEQ=1)

## For Older Datasets (sprint-03, sprint-04)

### What if crosswalk files don't exist?

**Option 1: Use pre-computed output crosswalk**
If `mtc_final_network_zone_seq.csv` exists but `emme_drive_network_node_id_crosswalk.csv` doesn't:
- Point `output_node_sequential_id_xwalk` to the pre-computed file
- Set `model_to_emme_node_id_xwalk` to a dummy path
- Modify code to use output crosswalk directly (skip creation step)

**Option 2: Make crosswalk optional**
```python
# In controller.py
@property
def node_seq_id_xwalk(self):
    if not hasattr(self.config.highway, 'model_to_emme_node_id_xwalk'):
        self.logger.log("node_xwalk not configured, skipping sequential ID creation", level="WARN")
        return None
    # ... rest of code
```

Current code returns `None` if crosswalk is missing, and validation is skipped.

### Generating crosswalk from EMME network

If you have an EMME database but no crosswalk file:

```python
# tests/generate_node_xwalk.py
from inro.emme.database.emmebank import Emmebank
import pandas as pd

emmebank = Emmebank("path/to/emmebank")
scenario = emmebank.scenario(1)
network = scenario.get_network()

# Extract node IDs
nodes = []
for node in network.nodes():
    nodes.append({
        'emme_node_id': node.id,
        'model_node_id': node['#node_id'] if '#node_id' in node.data1 else node.id
    })

# Save crosswalk
df = pd.DataFrame(nodes)
df.to_csv('emme_drive_network_node_id_crosswalk.csv', index=False)
```

## Configuration Example

Complete highway config with crosswalks:

```toml
[highway]
generic_highway_mode_code = "c"
# ... other settings ...

# Input crosswalk from network build
model_to_emme_node_id_xwalk = "emme_project/Database_highway/emme_drive_network_node_id_crosswalk.csv"

# Output crosswalk (pre-computed or generated by TM2)
output_node_sequential_id_xwalk = "inputs/landuse/mtc_final_network_zone_seq.csv"
```

## Troubleshooting

### Error: "No such file or directory: node_xwalk.csv"
**Cause**: Config points to wrong path or file doesn't exist  
**Solution**: 
1. Check if file exists in Box source data
2. Verify setup component copies it correctly
3. Update config paths to match actual file locations

### Error: "Node ID crosswalk mismatch: X MAZ, Y TAZ"
**Cause**: MAZ land use data doesn't match network geography  
**Solution**:
1. Verify MAZ data and network are from same model version
2. Check if network was rebuilt without updating land use
3. Regenerate crosswalk from current EMME network

### Warning: "node_xwalk file not found, skipping sequential ID creation"
**Cause**: Crosswalk optional, using older dataset  
**Impact**: MAZ data validation skipped - OK for highway-only tests
**Solution**: Generate crosswalk if MAZ validation needed

## See Also

- [tm2py/data_models/maz_data.py](../../tm2py/data_models/maz_data.py) - Implementation
- [tm2py/controller.py](../../tm2py/controller.py) - Usage in controller
- [tests/generate_node_xwalk.py](../../tests/generate_node_xwalk.py) - Utility to create crosswalk
