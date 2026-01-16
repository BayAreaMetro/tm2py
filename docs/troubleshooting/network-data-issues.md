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

## Missing Node Crosswalk Files

See [Node ID Crosswalk Documentation](../input/node-crosswalks.md)

## See Also

- [Network Data Inputs](../input/network.md)
- [Creating Base Year Inputs](../create-base-year-inputs.md)
