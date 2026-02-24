# Processor Configuration Guide

This document provides a comprehensive guide to configuring the number of processors used throughout tm2py for optimal performance. Proper processor configuration is critical for model runtime and system stability.

## Overview

tm2py uses parallel processing in several components to reduce model runtime. The number of processors must be configured in multiple locations throughout the system, each serving different purposes:

1. **EMME Configuration** - Core assignment and skimming procedures
2. **Highway Distribution** - Parallel highway assignment by time period
3. **Setup Configuration** - CTRAMP threads and batch file environment variables
4. **Component-Level Processing** - Individual model components

!!! warning "Important Considerations"
    - **Server Systems**: Recommend using `MAX-2` to `MAX-4` to leave capacity for background tasks
    - **Desktop Systems**: Recommend using `MAX-1` to maintain system responsiveness
    - **Memory Usage**: More processors = higher memory requirements
    - **EMME Limits**: Some EMME procedures may have processor limits
    - **Stability**: Higher processor counts may cause instability on some systems

## CTRAMP Runtime Files Reference

After running `setup_model.py`, several processor-related files are created in the model run directory. These files contain the actual processor settings used by CTRAMP and JPPF during model execution:

### Key Files in Model Run Directory

```
{model_run_dir}/
├── CTRAMP/
│   └── runtime/
│       ├── CTRampEnv.bat              # Environment variables including NUMBER_OF_PROCESSORS
│       ├── mtctm2.properties          # Main CTRAMP config with acc.without.jppf.numThreads  
│       ├── logsum.properties          # Logsum config with acc.without.jppf.numThreads
│       └── config/
│           ├── jppf-node1.properties  # JPPF node 1: processing.threads 
│           ├── jppf-node2.properties  # JPPF node 2: processing.threads
│           ├── jppf-nodeN.properties  # Additional nodes as needed
│           └── jppf-clientLocal.properties  # Local execution: jppf.local.execution.threads
└── RunModel.py                        # Main model execution script
```

### File Contents Examples

**CTRampEnv.bat:**
```batch
set JAVA_PATH=C:\Program Files\Java\jdk1.8.0_111
set NUMBER_OF_PROCESSORS=12
set MATRIX_MANAGER_PORT=1191
set HH_MANAGER_PORT=1129
```

**mtctm2.properties:**
```properties
# JPPF thread configuration
acc.without.jppf.numThreads=8

# Server addresses (set by setup process)
RunModel.MatrixServerAddress=192.168.1.100
RunModel.HouseholdServerAddress=192.168.1.100
```

**jppf-node1.properties:**
```properties
# JPPF node configuration
jppf.server.host=192.168.1.100
processing.threads=12

# Memory allocation
other.jvm.options=-Xms48000m -Xmx48000m -Dnode.name=node1
```

!!! warning "Manual File Edits"
    While these files are automatically updated by `setup_model.py`, you can manually edit them for fine-tuning. However, re-running setup will overwrite manual changes. For permanent changes, modify the setup configuration instead.

## Configuration Locations

### 1. EMME Configuration (`model_config.toml`)

The primary EMME processor configuration is set in the `[emme]` section:

```toml
[emme]
# General EMME procedures (highway assignment, skimming, transit assignment)
num_processors = "MAX-2"

# Transit skimming specific (can be different from general procedures)
num_processors_transit_skim = "MAX-2"
```

**Format Options:**
- `"MAX"` - Use all available processors
- `"MAX-N"` - Use all processors minus N (e.g., `"MAX-2"`)
- `"MAX/N"` - Use maximum divided by N (e.g., `"MAX/3"`)
- `"12"` - Use specific number (as string)

**Used By:**
- [Highway assignment procedures](tm2py/components/network/highway/highway_emme_spec.py#L131)
- [Transit assignment procedures](tm2py/components/network/transit/transit_assign.py#L1363)
- [Highway MAZ-to-MAZ skimming](tm2py/components/network/highway/highway_maz.py#L457)
- [Active mode calculations](tm2py/components/network/active/active_modes.py#L323)
- [Commercial vehicle procedures](tm2py/components/demand/commercial.py#L739)

### 2. Highway Distribution Configuration

For parallel highway assignment by time period groups:

```toml
# Example: Run 3 time period groups in parallel, each using 1/3 of processors
[[emme.highway_distribution]]
    time_periods = ["AM"]
    num_processors = "MAX/3"
    
[[emme.highway_distribution]]
    time_periods = ["PM"] 
    num_processors = "MAX/3"
    
[[emme.highway_distribution]]
    time_periods = ["EA", "MD", "EV"]
    num_processors = "MAX/3"
```

**For Serial Assignment** (single-threaded):
```toml
[[emme.highway_distribution]]
    time_periods = ["EA", "AM", "MD", "PM", "EV"]
    num_processors = "1"
```

**Configuration Reference:** [tm2py/config.py#L1420-L1430](tm2py/config.py#L1420-L1430)

### 3. Setup Model Configuration

During model setup, processor counts are configured for CTRAMP and batch files.

**In Setup Configuration Class** ([tm2py/setup_model/setup.py#L64-L67](tm2py/setup_model/setup.py#L64-L67)):
```python
# Defaults if not specified
self.NUM_PROCESSORS = os.cpu_count() or 48
self.CTRAMP_NUM_THREADS = max(1, (os.cpu_count() or 48) - 4)
```

**CTRAMP Runtime Files Updated** (in model run directory):

1. **CTRampEnv.bat** ([tm2py/setup_model/setup.py#L722](tm2py/setup_model/setup.py#L722)):
   - Location: `{model_run_dir}/CTRAMP/runtime/CTRampEnv.bat`
   - Updates: `set NUMBER_OF_PROCESSORS={num_processors}`
   - Pattern: `r'((?:set|SET) NUMBER_OF_PROCESSORS=)\d+'`

2. **CTRAMP Properties Files** ([tm2py/setup_model/setup.py#L731](tm2py/setup_model/setup.py#L731)):
   - **mtctm2.properties**: `{model_run_dir}/CTRAMP/runtime/mtctm2.properties`
   - **logsum.properties**: `{model_run_dir}/CTRAMP/runtime/logsum.properties` 
   - Updates: `acc.without.jppf.numThreads={ctramp_threads}`
   - Pattern: `r'(acc\.without\.jppf\.numThreads\s*=\s*)\d+'`

3. **JPPF Node Configuration Files** (in model run directory):
   - **jppf-node{X}.properties**: `{model_run_dir}/CTRAMP/runtime/config/jppf-node{X}.properties`
   - Updates: `processing.threads={threads_per_node}`
   - **jppf-clientLocal.properties**: `jppf.local.execution.threads={local_threads}`

**Usage in Components** ([tm2py/components/demand/household.py#L46](tm2py/components/demand/household.py#L46)):
```bash
# CTRampEnv.bat is called by household model:
CALL {model_run_dir}\CTRAMP\runtime\CTRampEnv.bat
```

### 4. Code Implementation Details

**Parser Function** ([tm2py/emme/manager.py#L61-L89](tm2py/emme/manager.py#L61-L89)):
```python
def parse_num_processors(value: Union[str, int]) -> int:
    """Parse processor specification to integer count.
    
    Supports formats:
    - "MAX": Use all available processors
    - "MAX-N": Use all minus N processors  
    - "MAX/N": Use all divided by N processors
    - "12": Use specific count
    """
    cpu_processors = multiprocessing.cpu_count()
    # ... parsing logic ...
    return min(cpu_processors, max(1, num_processors))
```

**Controller Access** ([tm2py/controller.py#L210-L215](tm2py/controller.py#L210-L215)):
```python
@property
def num_processors(self) -> int:
    return self.emme_manager.num_processors

@property  
def num_processors_transit_skim(self) -> int:
    return self.emme_manager.num_processors_transit_skim
```

## Performance Optimization Guidelines

### Desktop Development (8-16 cores)
```toml
[emme]
num_processors = "MAX-1"              # Leave 1 core for OS
num_processors_transit_skim = "MAX-1"

# Serial assignment for development
[[emme.highway_distribution]]
    time_periods = ["EA", "AM", "MD", "PM", "EV"]
    num_processors = "1"
```

### Server Production (24+ cores)
```toml
[emme]
num_processors = "MAX-4"              # Leave cores for other processes  
num_processors_transit_skim = "MAX-2" # Transit often needs fewer processors

# Parallel assignment for production
[[emme.highway_distribution]]
    time_periods = ["AM"]
    num_processors = "MAX/3"
[[emme.highway_distribution]] 
    time_periods = ["PM"]
    num_processors = "MAX/3"
[[emme.highway_distribution]]
    time_periods = ["EA", "MD", "EV"]
    num_processors = "MAX/3"
```

### High-Memory Systems (48+ cores)
```toml
[emme]
num_processors = "MAX-8"              # Conservative for stability
num_processors_transit_skim = "MAX-4"

# Aggressive parallelization
[[emme.highway_distribution]]
    time_periods = ["EA"]
    num_processors = "MAX/5"
[[emme.highway_distribution]]
    time_periods = ["AM"] 
    num_processors = "MAX/5"
[[emme.highway_distribution]]
    time_periods = ["MD"]
    num_processors = "MAX/5"
[[emme.highway_distribution]]
    time_periods = ["PM"]
    num_processors = "MAX/5"
[[emme.highway_distribution]]
    time_periods = ["EV"]
    num_processors = "MAX/5"
```

## Troubleshooting

### Common Issues

**Memory Exhaustion:**
- Reduce `num_processors` values
- Use `MAX/2` or `MAX/3` instead of `MAX-N`
- Monitor memory usage during runs

**EMME Instability:**
- Some EMME procedures may crash with high processor counts
- Try reducing to `MAX-4` or lower
- Check EMME logs for processor-related errors

**System Unresponsiveness:**
- Always leave at least 1-2 processors for the OS
- Use `MAX-1` on desktop systems
- Monitor CPU usage during model runs

**Slow Performance:**
- Too few processors: increase configuration
- Too many processors: may cause overhead, try reducing
- Check if bottlenecks are elsewhere (I/O, memory)

### Debugging Processor Usage

**Check Current Configuration:**
```python
# In Python/debugging context
from tm2py.controller import RunController
controller = RunController(config_file="model_config.toml")
print(f"EMME processors: {controller.num_processors}")
print(f"Transit skim processors: {controller.num_processors_transit_skim}")
```

**Monitor Actual Usage:**
- Use Task Manager (Windows) or `htop` (Linux) during model runs
- Check EMME log files for processor-related messages
- Monitor memory usage alongside CPU usage

## Best Practices

1. **Start Conservative**: Begin with `MAX-2` and adjust based on performance
2. **Test Configuration**: Run short test scenarios before full model runs
3. **Monitor Resources**: Watch CPU and memory usage during runs
4. **Document Changes**: Keep notes on what configurations work best for your system
5. **Environment Specific**: Different configurations for development vs. production
6. **Regular Review**: Processor configuration may need updates with system changes

## Future Improvements

The current processor configuration system has several areas for improvement. For detailed analysis and recommendations for simplifying and enhancing processor configuration management in tm2py, see:

**[Processor Configuration Improvements](processor-configuration-improvements.md)** - Comprehensive recommendations for system redesign

## Related Documentation

- [Run the Model](run.md) - General model execution
- [Server Setup](server-setup.md) - Production server configuration
- [Architecture](architecture.md) - System design and components