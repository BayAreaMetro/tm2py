# Processor Configuration Improvement Notes

This document outlines recommendations for improving processor configuration management in tm2py to reduce complexity, improve maintainability, and enhance user experience.

## Current Issues

### 1. Multiple Configuration Points
Currently, processor counts must be configured in multiple locations:
- **EMME config**: `num_processors` and `num_processors_transit_skim`
- **Highway distribution**: `num_processors` per time period group
- **Setup configuration**: `NUM_PROCESSORS` and `CTRAMP_NUM_THREADS`
- **Component level**: Individual components access via `controller.num_processors`

**Problem**: Risk of inconsistent configurations, user confusion, maintenance overhead.

### 2. String-Based Configuration Parsing
The current system uses string patterns like `"MAX-2"`, `"MAX/3"` that require parsing.

**Location**: [tm2py/emme/manager.py#L61-L89](tm2py/emme/manager.py#L61-L89)

**Problems**:
- Runtime parsing overhead
- Complex regex validation in config schema
- Error-prone string manipulation
- Limited validation until runtime

### 3. Inconsistent Defaults
Different components have different default processor calculation logic:

**Setup Model** ([tm2py/setup_model/setup.py#L64-L67](tm2py/setup_model/setup.py#L64-L67)):
```python
self.NUM_PROCESSORS = os.cpu_count() or 48
self.CTRAMP_NUM_THREADS = max(1, (os.cpu_count() or 48) - 4)
```

**Parse Function** ([tm2py/emme/manager.py#L70](tm2py/emme/manager.py#L70)):
```python
cpu_processors = multiprocessing.cpu_count()
```

**Problem**: Inconsistent fallback values, different CPU detection methods.

### 4. Limited Configuration Validation
Current validation is basic regex patterns in config schema.

**Location**: [tm2py/config.py#L1430](tm2py/config.py#L1430)
```python
num_processors: str = Field(pattern=r"^MAX$|^MAX-\d+$|^\d+$|^MAX/\d+$")
```

**Problems**:
- No validation of actual processor availability
- No checking for memory/resource constraints
- No guidance on optimal values for system type

### 5. CTRAMP Runtime File Management Issues

**File Proliferation Problem**: Multiple files require processor configuration with risk of inconsistency:
- `CTRampEnv.bat` - Sets `NUMBER_OF_PROCESSORS` environment variable
- `mtctm2.properties` - Contains `acc.without.jppf.numThreads` for main processing
- `logsum.properties` - Contains `acc.without.jppf.numThreads` for logsum calculations
- Multiple `jppf-node*.properties` - Contains `processing.threads` per distributed node
- `jppf-clientLocal.properties` - Contains `jppf.local.execution.threads` for local testing

**Regex-Based Updates Problem**: 
- Fragile pattern matching: `r'((?:set|SET) NUMBER_OF_PROCESSORS=)\d+'`
- No validation that file replacements succeed
- Manual edits get overwritten by setup process

**Hard-Coded Defaults Problem**:
- `os.cpu_count() or 48` fallback may be inappropriate for system type
- `max(1, (os.cpu_count() or 48) - 4)` formula doesn't consider memory constraints
- No differentiation between desktop vs. server environments

**No Runtime Validation Problem**:
- Setup doesn't verify CTRAMP files were updated correctly
- No consistency checking across multiple configuration files
- No validation that processor counts don't exceed system capabilities

## Recommended Improvements

### 1. Centralized Processor Configuration

**Create a dedicated processor configuration class:**

```python
@dataclass(frozen=True)  
class ProcessorConfig(ConfigItem):
    """Centralized processor configuration for all tm2py components."""
    
    # Base configuration
    total_available: Optional[int] = Field(default=None)  # Auto-detect if None
    reserve_for_system: int = Field(default=2, ge=0)      # Cores to leave for OS
    
    # Component-specific settings
    emme_general: ProcessorSpec = Field(default_factory=lambda: ProcessorSpec("MAX-2"))
    emme_transit_skim: ProcessorSpec = Field(default_factory=lambda: ProcessorSpec("MAX-2")) 
    ctramp_threads: ProcessorSpec = Field(default_factory=lambda: ProcessorSpec("MAX-4"))
    
    # Highway distribution strategy
    highway_distribution_strategy: Literal["serial", "parallel_by_period", "parallel_by_group"] = "serial"
    highway_parallel_groups: Optional[List[List[str]]] = None  # e.g., [["AM"], ["PM"], ["EA","MD","EV"]]
    
    @validator("total_available", always=True)
    def set_total_available(cls, v):
        """Set total available processors if not specified."""
        return v or multiprocessing.cpu_count()
        
    def get_processor_count(self, spec: ProcessorSpec) -> int:
        """Calculate actual processor count for a specification."""
        return spec.resolve(self.total_available, self.reserve_for_system)

@dataclass(frozen=True)
class ProcessorSpec:
    """Processor specification that can be resolved to actual count."""
    
    spec: Union[int, str]  # e.g., 8, "MAX-2", "MAX/3", "50%"
    
    def resolve(self, total_available: int, reserve: int) -> int:
        """Resolve specification to actual processor count."""
        available = max(1, total_available - reserve)
        
        if isinstance(self.spec, int):
            return min(self.spec, available)
        elif self.spec == "MAX":
            return available
        elif self.spec.startswith("MAX-"):
            subtract = int(self.spec[4:])
            return max(1, available - subtract)
        elif self.spec.startswith("MAX/"):
            divide = int(self.spec[4:])
            return max(1, available // divide)
        elif self.spec.endswith("%"):
            percent = int(self.spec[:-1])
            return max(1, int(available * percent / 100))
        else:
            raise ValueError(f"Invalid processor spec: {self.spec}")
```

### 2. Simplified Configuration Interface

**Enable simple configuration with smart defaults:**

```toml
[processors]
# Simple interface - just specify what you want to reserve
reserve_for_system = 2

# Or specify exact counts
emme_general = 12
emme_transit_skim = 8
ctramp_threads = 16

# Or use percentage
emme_general = "75%"
emme_transit_skim = "50%" 

# Highway distribution strategy
highway_distribution_strategy = "parallel_by_period"
# Auto-generates parallel groups, or specify custom:
# highway_parallel_groups = [["AM"], ["PM"], ["EA","MD","EV"]]
```

### 3. Auto-Configuration Profiles

**Provide system-type based profiles:**

```python
class ProcessorProfiles:
    """Pre-defined processor configuration profiles."""
    
    DESKTOP_DEVELOPMENT = ProcessorConfig(
        reserve_for_system=1,
        emme_general=ProcessorSpec("MAX-1"),
        emme_transit_skim=ProcessorSpec("MAX-1"),
        ctramp_threads=ProcessorSpec("MAX-2"),
        highway_distribution_strategy="serial"
    )
    
    SERVER_PRODUCTION = ProcessorConfig(
        reserve_for_system=4,
        emme_general=ProcessorSpec("MAX-4"),
        emme_transit_skim=ProcessorSpec("MAX-2"),
        ctramp_threads=ProcessorSpec("MAX-6"),
        highway_distribution_strategy="parallel_by_period"
    )
    
    HIGH_PERFORMANCE = ProcessorConfig(
        reserve_for_system=8,
        emme_general=ProcessorSpec("75%"),
        emme_transit_skim=ProcessorSpec("50%"),
        ctramp_threads=ProcessorSpec("80%"),
        highway_distribution_strategy="parallel_by_group",
        highway_parallel_groups=[["EA"], ["AM"], ["MD"], ["PM"], ["EV"]]
    )

# Usage in config:
[processors]
profile = "SERVER_PRODUCTION"  # Use predefined profile
# Override specific settings:
emme_general = 16  # Override the profile default
```

### 4. Runtime Validation and Auto-Tuning

**Add intelligent validation and suggestions:**

```python
class ProcessorValidator:
    """Validate and optimize processor configurations."""
    
    def __init__(self, config: ProcessorConfig):
        self.config = config
        self.system_info = self._get_system_info()
    
    def validate(self) -> List[ValidationMessage]:
        """Validate configuration against system capabilities."""
        messages = []
        
        total_requested = (
            self.config.get_processor_count(self.config.emme_general) +
            self.config.get_processor_count(self.config.emme_transit_skim) +
            self.config.get_processor_count(self.config.ctramp_threads)
        )
        
        if total_requested > self.system_info.cpu_count:
            messages.append(ValidationMessage(
                level="warning",
                message=f"Total requested processors ({total_requested}) exceeds available ({self.system_info.cpu_count})"
            ))
        
        if self.system_info.memory_gb < 32 and total_requested > 8:
            messages.append(ValidationMessage(
                level="warning", 
                message="High processor count with low memory may cause instability"
            ))
        
        return messages
    
    def suggest_optimizations(self) -> List[str]:
        """Suggest configuration optimizations based on system."""
        suggestions = []
        
        if self.system_info.is_desktop and self.config.highway_distribution_strategy != "serial":
            suggestions.append("Consider 'serial' highway distribution on desktop systems")
        
        if self.system_info.cpu_count > 32 and self.config.emme_general.spec == "MAX":
            suggestions.append("Consider 'MAX-4' instead of 'MAX' on high-core systems for stability")
            
        return suggestions
```

### 5. Dynamic Configuration Management

**Runtime adjustment capabilities:**

```python
class ProcessorManager:
    """Manage processor configuration at runtime."""
    
    def __init__(self, config: ProcessorConfig):
        self.config = config
        self.current_usage = {}
        
    def get_processor_count_for_component(self, component: str, operation: str = "default") -> int:
        """Get processor count for specific component and operation."""
        # Allow operation-specific overrides
        if component == "emme" and operation == "transit_skim":
            return self.config.get_processor_count(self.config.emme_transit_skim)
        elif component == "emme":
            return self.config.get_processor_count(self.config.emme_general)
        elif component == "ctramp":
            return self.config.get_processor_count(self.config.ctramp_threads)
        else:
            return self.config.get_processor_count(self.config.emme_general)
    
    def adjust_for_memory_pressure(self) -> None:
        """Dynamically reduce processor counts if memory pressure detected."""
        # Implementation would monitor memory usage and adjust
        pass
        
    def log_usage_stats(self) -> None:
        """Log processor usage statistics for optimization."""
        pass
```

### 6. CTRAMP Configuration Management

**Create unified CTRAMP configuration management to address file proliferation and consistency issues:**

```python
class CtrampConfigManager:
    """Centralized CTRAMP runtime file management."""
    
    def __init__(self, processor_config: ProcessorConfig):
        self.config = processor_config
        
    def generate_ctramp_env(self) -> str:
        """Generate CTRampEnv.bat from template."""
        template = """
set JAVA_PATH={java_path}
set NUMBER_OF_PROCESSORS={num_processors}
set MATRIX_MANAGER_PORT={matrix_port}
set HH_MANAGER_PORT={hh_port}
        """
        return template.format(
            java_path=self.config.java_path,
            num_processors=self.config.get_processor_count("ctramp_env"),
            matrix_port=self.config.matrix_port,
            hh_port=self.config.hh_port
        )
    
    def generate_properties_files(self) -> Dict[str, str]:
        """Generate all CTRAMP properties files from templates."""
        templates = {
            "mtctm2.properties": """
acc.without.jppf.numThreads={threads}
RunModel.MatrixServerAddress={server_ip}
RunModel.HouseholdServerAddress={server_ip}
            """.strip(),
            "logsum.properties": """
acc.without.jppf.numThreads={threads}
RunModel.MatrixServerAddress={server_ip}
RunModel.HouseholdServerAddress={server_ip}
            """.strip()
        }
        
        return {
            filename: template.format(
                threads=self.config.get_processor_count("ctramp_threads"),
                server_ip=self.config.server_ip
            ) for filename, template in templates.items()
        }
    
    def generate_jppf_node_configs(self, num_nodes: int) -> Dict[str, str]:
        """Generate JPPF node configuration files."""
        configs = {}
        threads_per_node = self.config.get_threads_per_node(num_nodes)
        
        for i in range(1, num_nodes + 1):
            config = f"""
jppf.server.host={self.config.server_ip}
processing.threads={threads_per_node}
other.jvm.options=-Xms{self.config.get_node_memory(i)}m -Xmx{self.config.get_node_memory(i)}m -Dnode.name=node{i}
            """.strip()
            configs[f"jppf-node{i}.properties"] = config
            
        return configs
        
    def auto_detect_optimal_nodes(self) -> int:
        """Determine optimal number of JPPF nodes based on system resources."""
        total_memory_gb = psutil.virtual_memory().total // (1024**3)
        cpu_count = self.config.total_available
        
        # Conservative: each node needs ~8GB, leave memory for OS
        max_nodes_by_memory = max(1, (total_memory_gb - 4) // 8)
        max_nodes_by_cpu = max(1, cpu_count // 4)  # 4 cores per node minimum
        
        return min(max_nodes_by_memory, max_nodes_by_cpu, 8)  # Cap at 8 nodes

class CtrampValidator:
    """Validate CTRAMP configuration files."""
    
    def validate_file_updates(self, model_dir: Path) -> ValidationResult:
        """Verify CTRAMP files were updated correctly."""
        results = []
        
        # Check CTRampEnv.bat
        env_file = model_dir / "CTRAMP" / "runtime" / "CTRampEnv.bat"
        if env_file.exists():
            content = env_file.read_text()
            if "NUMBER_OF_PROCESSORS=" not in content:
                results.append("CTRampEnv.bat missing processor setting")
        else:
            results.append("CTRampEnv.bat not found")
            
        # Check properties files
        for props_file in ["mtctm2.properties", "logsum.properties"]:
            file_path = model_dir / "CTRAMP" / "runtime" / props_file
            if file_path.exists():
                content = file_path.read_text()
                if "acc.without.jppf.numThreads=" not in content:
                    results.append(f"{props_file} missing thread setting")
            else:
                results.append(f"{props_file} not found")
                
        return ValidationResult(success=len(results) == 0, errors=results)
    
    def check_ctramp_consistency(self, model_dir: Path) -> List[ConsistencyIssue]:
        """Check for inconsistent processor settings across CTRAMP files."""
        issues = []
        processor_counts = {}
        
        # Extract processor counts from all files
        env_file = model_dir / "CTRAMP" / "runtime" / "CTRampEnv.bat"
        if env_file.exists():
            match = re.search(r'NUMBER_OF_PROCESSORS=(\d+)', env_file.read_text())
            if match:
                processor_counts['env'] = int(match.group(1))
                
        # Properties files  
        for props_name in ["mtctm2", "logsum"]:
            props_file = model_dir / "CTRAMP" / "runtime" / f"{props_name}.properties"
            if props_file.exists():
                match = re.search(r'acc\.without\.jppf\.numThreads=(\d+)', props_file.read_text())
                if match:
                    processor_counts[props_name] = int(match.group(1))
        
        # Check for inconsistencies
        if len(set(processor_counts.values())) > 1:
            issues.append(ConsistencyIssue(
                severity="warning",
                message=f"Inconsistent processor counts: {processor_counts}",
                recommendation="Verify configuration and re-run setup if needed"
            ))
            
        return issues

class ResourceAllocator:
    """Smart resource allocation for CTRAMP components."""
    
    def __init__(self, system_info: SystemInfo):
        self.system = system_info
        
    def recommend_ctramp_settings(self) -> Dict[str, int]:
        """Recommend CTRAMP processor settings based on system resources."""
        recommendations = {}
        
        # Base recommendations on system type
        if self.system.is_desktop:
            recommendations.update({
                'num_processors': self.system.cpu_count - 1,
                'ctramp_threads': max(1, self.system.cpu_count - 2),
                'jppf_nodes': 1,
                'threads_per_node': max(1, self.system.cpu_count // 2)
            })
        elif self.system.is_server:
            recommendations.update({
                'num_processors': max(1, self.system.cpu_count - 4),
                'ctramp_threads': max(1, self.system.cpu_count - 6),
                'jppf_nodes': min(4, self.system.cpu_count // 8),
                'threads_per_node': max(4, self.system.cpu_count // 6)
            })
            
        # Adjust for memory constraints
        memory_gb = self.system.memory_gb
        if memory_gb < 32:
            # Reduce all counts for low-memory systems
            for key in recommendations:
                recommendations[key] = max(1, recommendations[key] // 2)
        elif memory_gb > 128:
            # Can be more aggressive on high-memory systems
            recommendations['ctramp_threads'] = min(
                recommendations['ctramp_threads'] * 2, 
                self.system.cpu_count
            )
            
        return recommendations
```

### 7. Implementation Plan

#### Phase 1: Backward Compatible Addition
1. Add new `ProcessorConfig` class alongside existing system
2. Add validation and profiling utilities  
3. Maintain existing string-based configuration support

#### Phase 2: Migration Helper
1. Add configuration migration utility to convert old to new format
2. Add warnings for deprecated configuration patterns
3. Provide side-by-side comparison tools

#### Phase 3: Default Switch
1. Make new system the default for new configurations
2. Add deprecation warnings for old system
3. Update documentation and examples

#### Phase 4: Legacy Removal
1. Remove old string-based parsing
2. Simplify configuration schema
3. Clean up redundant code paths

### 8. Testing Strategy

**Unit Tests:**
- Test ProcessorSpec resolution logic
- Test validation rules
- Test profile generation

**Integration Tests:**  
- Test on various system configurations (2-core, 8-core, 64-core)
- Test memory usage patterns with different processor counts
- Test configuration migration from old to new format

**Performance Tests:**
- Measure overhead of new configuration system
- Compare runtime performance with different processor configurations
- Profile memory usage patterns

## Benefits of Proposed Changes

1. **Simplified User Experience**: Single configuration point with smart defaults
2. **Better Validation**: Early detection of configuration issues  
3. **Improved Performance**: System-aware configuration recommendations
4. **Easier Maintenance**: Centralized logic, reduced code duplication
5. **Better Documentation**: Configuration intent is clearer
6. **Runtime Flexibility**: Dynamic adjustment capabilities
7. **Profile-Based Setup**: Easy configuration for different deployment types
8. **CTRAMP Consistency**: Unified management of all CTRAMP runtime files
9. **Robust File Management**: Template-based generation replaces fragile regex patterns

## Migration Considerations

- **Backward Compatibility**: Existing configurations must continue to work
- **Documentation Updates**: Comprehensive update of all processor-related docs
- **User Communication**: Clear migration path and timeline
- **Testing**: Extensive testing on different system configurations
- **Performance Testing**: Ensure no regression in model performance