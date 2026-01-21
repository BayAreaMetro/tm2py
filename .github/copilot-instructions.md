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
| EMME 25 | `C:\Program Files\Bentley\OpenPaths\EMME 25.00.01\` |

## Current Issues Being Debugged

- **EMME 25.00.01 bug**: `reportlexer.py` throws "Illegal character '+'" during SOLA traffic assignment
  - Error occurs in `inro\emme\procedure\reportlexer.py` line 103
  - Happens ~30 seconds into assignment when parsing internal report
  - NOT a tm2py issue - the assignment spec is valid
  - Workaround: Downgrade to EMME 24.x
  - Consider reporting to Bentley support

## Notes

- Sprint-04 databases lack toll attributes (@useclass, @tollbooth, @tollseg)
- EMME template structure: copy from `template/emme_project/` not `template/`
- `get_attribute_values()` returns a list, not a dictionary
