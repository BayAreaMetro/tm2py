# Copilot Instructions for tm2py

## Python Environment

**Always use this Python executable:**
```
C:\GitHub\tm2pyenv\Scripts\python.exe
```

## Running County Tests

```powershell
cd c:\GitHub\tm2py
C:\GitHub\tm2pyenv\Scripts\python.exe tests\run_county_test.py --config tests\county_test_config.toml
```

**⚠️ IMPORTANT: When a test is running in the background:**
- Do NOT send new commands to the same terminal
- Use `Get-Process python` in a DIFFERENT terminal to check if still running
- Use `Get-Content <logfile> -Tail 50` in a DIFFERENT terminal to check progress
- Wait for the test to complete before sending new commands

## Key Paths

| Item | Path |
|------|------|
| Python Environment | `C:\GitHub\tm2pyenv\Scripts\` |
| Test Output | `E:\Tests\` |
| Sprint-04 Data | `E:\Data\tm2_inputs\2015_TM2_20250619_Sprint-04\` |
| EMME 24 | `C:\Program Files\Bentley\OpenPaths\EMME 24.01.00\` |

## Known Issues & Solutions

### "Illegal character '+'" in reportlexer.py

**Symptom**: SOLA traffic assignment crashes with lexer error on `+` character

**Root Cause Chain**:
1. `@lanes` = 0 for all links (attribute not copied from base network)
2. `@capacity` = 0 (calculated from `@lanes`)
3. VDF divides by `@capacity` → infinity
4. EMME formats infinity as `0.135972+124` (no 'e' in exponent)
5. Lexer crashes parsing unexpected `+`

**Fix**: The `get_attribute_values()` API returns `[id_array, value_array]`, not just values. Code must extract `result[1]` for actual values. Fixed in `create_tod_scenarios.py`.

### Sprint-04 Network Issues

- Databases lack toll attributes (`@useclass`, `@tollbooth`, `@tollseg`) - must be created
- Transit lines may have highway mode 'x' contamination - handled gracefully now

## Notes

- EMME template structure: copy from `template/emme_project/` not `template/`
- `get_attribute_values()` returns `[id_array, value_array]`, not a dictionary
- Use `link.num_lanes` not `link.lanes` (EMME attribute naming)
