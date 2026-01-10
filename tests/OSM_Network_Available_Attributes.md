# OSM San Mateo Network - Available Attributes

## Network: M:\Development\Travel Model Two\...\SanMateo\7_scenario\emme\emme_project

## AVAILABLE LINK ATTRIBUTES

### User-Defined @ Attributes
**Count: 0**
- None found

### Standard EMME Link Attributes
Based on sample link (ID: '1-20611', from node 1 to node 20611):

| Attribute | Value | Type | Available? | Notes |
|-----------|-------|------|------------|-------|
| `id` | '1-20611' | str | ✅ | Link identifier |
| `i_node` | Node(1) | Node | ✅ | From-node |
| `j_node` | Node(20611) | Node | ✅ | To-node |
| `length` | 0.165 | float | ✅ | Link length in miles |
| `type` | 1 | int | ✅ | **Link type code - could map to @ft** |
| `num_lanes` | 7.0 | float | ✅ | **Number of lanes - could map to @lanes** |
| `volume_delay_func` | 0 | int | ✅ | VDF number |
| `data1` | 0.0 | float | ✅ | Generic data field 1 |
| `data2` | 0.0 | float | ✅ | Generic data field 2 |
| `data3` | 0.0 | float | ✅ | Generic data field 3 |
| `lanes` | N/A | - | ❌ | Attribute does not exist |
| `vdf` | N/A | - | ❌ | Attribute does not exist |

## AVAILABLE NODE ATTRIBUTES

### User-Defined @ Attributes  
**Count: 0**
- None found

### Standard EMME Node Attributes
- Node IDs exist
- Coordinates exist (x, y)
- Standard topology fields

## POTENTIAL MAPPING STRATEGY

### What We Can Use
1. **`type`** → Could initialize `@ft` (functional type)
   - Need to understand what values exist (1, 2, 3, etc.)
   - Need mapping table: type value → functional type
   
2. **`num_lanes`** → Could initialize `@lanes`
   - Already numeric
   - Direct copy possible

3. **`length`** → Already available
   - Used for calculations

### What We'd Need to Calculate/Assume
- `@capclass` - Capacity class (could derive from type or lanes)
- `@free_flow_speed` - Free flow speed (could derive from type)
- `@drive_link` - Set to 1 for all links (assume all driveable)
- `@tollbooth` - Set to 0 (no tolls)
- `@tollseg` - Set to 0 (no tolls)
- `@useclass` - Set to 1 (general purpose) or 0

## NEXT INVESTIGATION NEEDED

### 1. What `type` values exist in the network?
Run analysis to see distribution of `type` field:
- What values: 1, 2, 3, 4, ...?
- How many links per type?
- Can we map these to TM2 functional types (1-7)?

### 2. Check `num_lanes` distribution
- Range of values?
- Any zeros or invalid values?

### 3. Check if modes are set
- What modes exist on links?
- Are any mode restrictions coded?

## RECOMMENDATION

Build a script to:
1. Analyze `type` value distribution
2. Create type → functional type mapping
3. Initialize all required @ attributes from standard fields
4. Set defaults for attributes that can't be derived
