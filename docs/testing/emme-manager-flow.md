# EMME Manager Flow Documentation

## Overview
The EmmeManager is tm2py's interface to the INRO EMME API. It manages EMME project connections, database access, and provides tools for network operations.

## Architecture Hierarchy

```
RunController
    └── EmmeManager (singleton per project)
            ├── EmmeProject (Desktop API wrapper)
            ├── highway_emmebank (ProxyEmmebank - lazy)
            ├── transit_emmebank (ProxyEmmebank - lazy)
            ├── active_north_emmebank (ProxyEmmebank - lazy)
            └── active_south_emmebank (ProxyEmmebank - lazy)
```

## Initialization Flow

### 1. RunController Creation
```python
# In tests/highway_assign_skim_controller.py or tm2py/controller.py
controller = RunController(config_file, run_dir)
```

### 2. EmmeManager Initialization (Lazy)
When a component first accesses `controller.emme_manager`:

```python
# tm2py/controller.py line 232
@property
def emme_manager(self):
    if self._emme_manager is None:
        self._emme_manager = EmmeManager(self, self.config.emme)
    return self._emme_manager
```

### 3. EmmeManager.__init__() - Project Setup
```python
# tm2py/emme/manager.py line 446
def __init__(self, controller, emme_config):
    # 1. Store controller reference and config
    self.controller = controller
    self.config = emme_config
    
    # 2. Get absolute project path
    project_path = controller.get_abs_path(config.project_path)
    
    # 3. Call parent EmmeProject.__init__()
    #    This starts the EMME Desktop application
    super().__init__(project_path)
    
    # 4. Resolve all database paths to absolute
    self.highway_database_path = get_abs_path(config.highway_database_path)
    self.transit_database_path = get_abs_path(config.transit_database_path)
    self.active_north_database_path = get_abs_path(config.active_north_database_path)
    self.active_south_database_path = get_abs_path(config.active_south_database_path)
    
    # 5. Initialize lazy properties to None
    self._highway_emmebank = None
    self._transit_emmebank = None
    self._active_north_emmebank = None
    self._active_south_emmebank = None
```

### 4. EmmeProject.__init__() - EMME Desktop Startup
```python
# tm2py/emme/manager.py line 200
def __init__(self, project_path, emmebank_path=None):
    # Check if project already open in _EMME_PROJECT_REGISTRY
    if project_path in _EMME_PROJECT_REGISTRY:
        return existing_project
    
    # Start EMME Desktop API
    from inro.emme.desktop.app import start_dedicated
    self._project = start_dedicated(
        project=project_path,
        visible=False,      # No GUI
        user_initials="TM2PY"
    )
    
    # Register in global registry
    _EMME_PROJECT_REGISTRY[project_path] = self
```

## Database Access Flow (Lazy Loading)

### Component Requests Database
```python
# Example: tm2py/components/network/highway/highway_network.py line 101
@property
def highway_emmebank(self):
    if not self._highway_emmebank:
        self._highway_emmebank = self.controller.emme_manager.highway_emmebank
    return self._highway_emmebank
```

### EmmeManager Creates ProxyEmmebank (First Access Only)
```python
# tm2py/emme/manager.py line 482
@property
def highway_emmebank(self):
    if self._highway_emmebank is None:
        self._highway_emmebank = ProxyEmmebank(self, self.highway_database_path)
    return self._highway_emmebank
```

### ProxyEmmebank.__init__() - Database Connection
```python
# tm2py/emme/manager.py line 95
def __init__(self, emme_manager, path):
    self.emme_manager = emme_manager
    self.path = path
    
    # Open the Emmebank database
    self._emmebank = emme_manager.project.open_database(path)
    
    # Get the desktop object for tools
    self._desktop = self._emmebank.desktop
```

## Key Concepts

### Lazy Initialization
- Databases are **only opened when first accessed**
- This saves resources - if a component doesn't need transit, that database never opens
- Pattern: `if self._database is None: self._database = connect()`

### ProxyEmmebank Wrapper
Wraps the raw INRO Emmebank with convenience methods:
- `scenario(time_period)` - Get scenario by time period name (e.g., "AM")
- `tool(name)` - Access EMME modeller tools
- `emmebank` - Access raw INRO Emmebank object
- `create_zero_matrix()` - Initialize zero-valued matrices

### Singleton Pattern
- One EmmeProject per project path (stored in `_EMME_PROJECT_REGISTRY`)
- One EmmeManager per RunController
- Multiple ProxyEmmebanks (one per database type)

## Typical Component Flow

### PrepareNetwork (Highway)
```
1. Component.__init__()
   └── self.controller = controller
   
2. Component.run()
   └── scenario = self.highway_emmebank.scenario("AM")
       │
       ├── Triggers: self.highway_emmebank property
       │   └── Triggers: controller.emme_manager property
       │       ├── Creates: EmmeManager(controller, config.emme)
       │       │   ├── Starts EMME Desktop API
       │       │   └── Resolves database paths
       │       │
       │       └── Returns: emme_manager instance
       │   
       └── Triggers: emme_manager.highway_emmebank property
           ├── Creates: ProxyEmmebank(manager, highway_database_path)
           │   └── Opens: EMME database at path
           │
           └── Returns: proxy_emmebank instance
```

### Why active_north Opens First
The error message showed active_north opening first because:
1. Config validation reads ALL database paths during init
2. The paths are resolved using `get_abs_path()` in order
3. If a path is invalid/missing, it fails on the FIRST problematic path
4. In our case: config had relative paths like `"emme_project/Database_active_north"`
5. RunController tried to resolve it relative to the run directory
6. Active_north was listed before highway in the code order

**Solution**: Use absolute paths in config so resolution is deterministic

## Database Types in tm2py

| Database | Purpose | Used By |
|----------|---------|---------|
| `highway` | Highway network, assignment, skims | PrepareNetwork, HighwayAssignment, HighwayMAZ |
| `transit` | Transit network, assignment, skims | PrepareTransitNetwork, TransitAssignment |
| `active_north` | Active modes (walk/bike) - North region | ActiveModes component |
| `active_south` | Active modes (walk/bike) - South region | ActiveModes component |

All databases are separate EMME databanks with independent networks and scenarios.

## Config Requirements

### Minimal (Highway-Only Test)
```toml
[emme]
project_path = "E:/path/to/emme_project/project.emp"
highway_database_path = "E:/path/to/emme_project/Database_highway"
# Others still required but won't be opened:
active_north_database_path = "E:/path/to/emme_project/Database_active_north"
active_south_database_path = "E:/path/to/emme_project/Database_active_south"
transit_database_path = "E:/path/to/emme_project/Database_transit"
all_day_scenario_id = 100
num_processors = 1
```

### Path Resolution Rules
1. **Absolute paths**: Used as-is (e.g., `"E:/Data/emme_project"`)
2. **Relative paths**: Resolved relative to `run_dir` (the directory with config files)
3. **Best practice**: Always use absolute paths for EMME databases to avoid ambiguity

## Tool Access

Components access EMME tools through the manager:

```python
# Get an EMME modeller tool
create_attribute = controller.emme_manager.tool(
    "inro.emme.data.extra_attribute.create_extra_attribute"
)

# Use the tool
create_attribute(
    extra_attribute_type="LINK",
    extra_attribute_name="@capacity",
    extra_attribute_description="Link capacity",
    overwrite=True,
    scenario=scenario
)
```

## Cleanup

EmmeManager handles cleanup automatically:
- Desktop API connections are closed when Python exits
- Databases are closed when the EmmeProject is destroyed
- No manual cleanup required

## Common Issues and Solutions

### KeyError: '@useclass'
- **Cause**: Missing `create_tod_scenarios` component - it copies time-period-specific attributes
- **Fix**: Add `"create_tod_scenarios"` to component list BEFORE `"prepare_network_highway"`
- **Details**: Base scenario has `@useclass_am`, etc. TOD scenarios need generic `@useclass`

### KeyError: 'am' (scenario lookup)
- **Cause**: Case mismatch - `scenario_dict` built with uppercase keys ("AM") but lookup used `.lower()`
- **Fix**: Removed `.lower()` from line 146 in tm2py/emme/manager.py
- **Details**: `scenario_dict` keys come from `config.time_periods[].name` (uppercase "AM")

### "Invalid EMME project file"
- **Cause**: Relative path in config, resolved to wrong location
- **Fix**: Use absolute paths (e.g., `E:/2015_TM2_20250619/emme_project/Database_highway/emmebank`)

### Database not found
- **Cause**: Path doesn't exist or is incorrect
- **Fix**: Verify database path exists and contains `emmebank` file

### Multiple processes conflict
- **Cause**: Multiple RunControllers trying to open same project
- **Fix**: Use `num_processors` config and let tm2py manage parallelism

### EMME Desktop opens all databases (slow startup)
- **Cause**: The .emp project file lists all databases in `OpenDatabases` and `ProjectDatabases[]`
- **Not fixable in Python**: EMME Desktop loads databases based on .emp file, not code requests
- **Workaround**: Create separate .emp files for different workflows (highway-only, transit-only)
