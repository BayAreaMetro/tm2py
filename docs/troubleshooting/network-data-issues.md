# Network Data Issues

Common network data problems and solutions when using legacy or filtered networks.

## Transit Lines with Highway Modes

**Symptom**: Error during `create_tod_scenarios`: `KeyError: 'x'`

**Full Error**:
```
File "tm2py/components/network/create_tod_scenarios.py", line 435
    line["@invehicle_factor"] = in_vehicle_factors[line.vehicle.mode.id]
KeyError: 'x'
```

**Cause**: 
- Transit lines in the EMME network have been assigned highway mode 'x' (MAZ-to-MAZ mode)
- Mode 'x' is not defined in `[transit.modes]` config
- Common in networks built before November 2025 (sprint-04 and earlier)

**Root Cause**:
- Network build process contamination where MAZ centroid connectors or highway modes leaked into transit line definitions
- Possible filtering artifacts when extracting subregional networks

**Solution**: 
Code now handles this gracefully (as of January 2026):
```python
# Checks if mode exists before accessing
if line.vehicle.mode.id in in_vehicle_factors:
    line["@invehicle_factor"] = in_vehicle_factors[line.vehicle.mode.id]
else:
    # Logs warning and skips
```

**Warning Message**:
```
Warning: Transit line 12345 uses mode 'x' not defined in transit.modes config. 
This may indicate network build issues. Skipping perception factors for this line.
```

**Prevention**:
- Use networks from sprint-05 or later (post-November 2025)
- Verify transit line modes during network build QA
- Check that all transit lines use only transit modes (not 'x', 'c', etc.)

## "Illegal character '+'" in reportlexer.py

**Symptom**: SOLA traffic assignment crashes with lexer error:
```
File "inro\emme\procedure\reportlexer.py", line 103
LexError: Illegal character '+' at position X
```

**Full Error Context**:
Assignment runs for ~30 seconds then crashes during EMME's internal report parsing.

**Root Cause Chain**:
1. `@lanes` = 0 for all links (attribute not copied from base network)
2. `@capacity` = 0 (calculated from `@lanes` in VDF/capacity formulas)
3. VDF divides by `@capacity` → produces infinity
4. EMME formats infinity as `0.135972+124` (scientific notation without 'e')
5. Report lexer crashes on unexpected `+` character

**Why @lanes Was Zero**:

Two potential causes:

1. **Case sensitivity in period attribute copying** (fixed January 2026):
   - Period-specific attributes like `@lanes_am` use lowercase suffixes
   - Period names in config are uppercase (`"AM"`)
   - The `endswith("AM")` check failed for `@lanes_am`
   - Fix: Use case-insensitive comparison and try lowercase suffix first

2. **get_attribute_values() API misuse**:
   The API returns `[id_array, value_array]`, not just values:
   ```python
   # WRONG - iterates over 2 arrays, not individual values
   lanes_result = scenario.get_attribute_values("LINK", ["@lanes"])
   if all(v == 0 for v in lanes_result):  # Always False!
       ...

   # CORRECT - extract the value array (index 1)
   lanes_result = scenario.get_attribute_values("LINK", ["@lanes"])
   lanes_values = lanes_result[1]  # The actual values
   if all(v == 0 for v in lanes_values):
       ...
   ```

**Fix Applied**: `create_tod_scenarios.py` now:
- Uses case-insensitive matching for period attributes
- Correctly extracts `lanes_result[1]` for value comparisons
- Uses `link.num_lanes` (not `link.lanes`) for EMME standard attribute

## Missing Node Crosswalk Files

See [Node ID Crosswalk Documentation](../input/node-crosswalks.md)

## See Also

- [Network Data Inputs](../input/network.md)
- [Creating Base Year Inputs](../create-base-year-inputs.md)
