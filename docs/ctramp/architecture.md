# CT-RAMP Architecture

!!! info "Technical Architecture"
    This document provides detailed technical architecture information for CT-RAMP implementation in tm2py. It covers system design patterns, data flows, and integration approaches.

## System Architecture Overview

CT-RAMP follows a modular, pipeline-based architecture that separates concerns and enables flexible model development and execution.

### Core Design Principles

**Separation of Concerns**
: Clear boundaries between data management, model logic, coordination, and execution control

**Dependency Management**  
: Explicit model dependencies with automatic sequencing and validation

**Household Coordination**
: Centralized coordination mechanisms for joint household decisions

**Performance Optimization**
: Parallelization opportunities and caching strategies for computational efficiency

## Component Architecture

``` mermaid
graph TD
    subgraph "Application Layer"
        A[Model Controller]
        B[Configuration Manager]
        C[Validation Framework]
    end
    
    subgraph "Model Pipeline Layer"  
        D[Pipeline Manager]
        E[Dependency Resolver]
        F[Execution Context]
    end
    
    subgraph "Model Layer"
        G[Auto Ownership]
        H[CDAP Coordinator]
        I[Tour Models]
        J[Choice Models]
    end
    
    subgraph "Coordination Layer"
        K[Household Coordinator]
        L[Resource Manager]
        M[Conflict Resolver]
    end
    
    subgraph "Data Layer"
        N[Data Repository]
        O[Cache Manager]  
        P[I/O Controllers]
    end
    
    subgraph "Infrastructure Layer"
        Q[UEC Framework]
        R[Utility Calculator]
        S[Performance Monitor]
    end
    
    A --> D
    B --> A
    C --> A
    D --> E
    E --> F
    F --> G
    F --> H
    F --> I
    F --> J
    G --> K
    H --> K
    I --> K
    J --> K
    K --> L
    L --> M
    N --> O
    O --> P
    Q --> R
    R --> S
    
    style A fill:#e3f2fd
    style K fill:#f3e5f5
    style N fill:#e8f5e8
    style Q fill:#fff3e0
```

### Application Controller Layer

The top-level application layer manages overall system execution and configuration:

**Model Controller**
- Main application entry point and orchestrator
- Manages model execution lifecycle
- Coordinates between pipeline and validation systems

**Configuration Manager**
- Loads and validates configuration files
- Manages model parameters and settings
- Handles scenario-specific configurations

**Validation Framework**
- Input data validation and quality checks
- Model result validation and reasonableness tests
- Performance monitoring and reporting

### Model Pipeline Layer

The pipeline layer manages model sequencing and execution:

**Pipeline Manager**
- Coordinates execution of model sequence
- Manages data flow between models
- Handles error recovery and retry logic

**Dependency Resolver**  
- Analyzes model dependencies
- Determines optimal execution order
- Validates data availability for each model

**Execution Context**
- Maintains model state and intermediate results
- Provides consistent data access interface
- Manages transaction-like model execution

### Model Implementation Layer

Individual model components implement specific choice behaviors:

**Choice Models**
- Discrete choice model implementations
- Utility calculation and probability computation
- Alternative generation and sampling

**Coordination Models**
- Household-level coordination logic
- Joint decision-making processes
- Resource allocation and conflict resolution

### Data Management Architecture

``` mermaid
graph LR
    subgraph "Data Sources"
        A[Population Data]
        B[Land Use Data]
        C[Network Data]
        D[Skim Matrices]
    end
    
    subgraph "Data Repository"
        E[Data Loader]
        F[Data Validator]
        G[Data Cache]
        H[Data Transformer]
    end
    
    subgraph "Model Access"
        I[Household Data]
        J[Person Data]
        K[Tour Data]
        L[Accessibility Data]
    end
    
    A --> E
    B --> E
    C --> E
    D --> E
    E --> F
    F --> G
    G --> H
    H --> I
    H --> J
    H --> K
    H --> L
    
    style E fill:#e3f2fd
    style G fill:#f3e5f5
```

**Data Repository Pattern**
- Centralized data access with consistent interface
- Lazy loading and intelligent caching
- Data validation and quality assurance
- Format conversion and standardization

**Caching Strategy**
- Multi-level caching (memory, disk, distributed)
- Invalidation triggers and dependency tracking  
- Performance optimization for repeated access
- Memory management and cleanup

## Execution Architecture

### Sequential Execution Pattern

CT-RAMP follows a carefully orchestrated sequence ensuring proper dependencies:

```python
# Example execution sequence
execution_sequence = [
    "population_synthesis",    # Input: demographic forecasts
    "accessibility",          # Input: networks, land use
    "auto_ownership",         # Input: households, accessibility
    "cdap",                  # Input: persons, auto ownership  
    "mandatory_tours",        # Input: persons, CDAP results
    "joint_tours",           # Input: households, CDAP
    "individual_tours",       # Input: persons, household coordination
    "tour_destination",       # Input: tours, accessibility
    "tour_mode_choice",       # Input: tours, level of service
    "tour_tod",              # Input: tours, time constraints
    "stop_frequency",        # Input: tours, activity patterns
    "stop_location",         # Input: stops, accessibility  
    "trip_mode_choice"       # Input: trips, level of service
]
```

### Parallel Execution Opportunities

**Household-Level Parallelization**
- Process independent households simultaneously
- Shared read-only data (networks, land use)
- Separate write spaces for household results

**Model-Level Parallelization**  
- Execute independent model components in parallel
- Coordinate through synchronization points
- Aggregate results at completion

### Coordination Architecture

``` mermaid
graph TD
    subgraph "Household Coordination"
        A[Joint Activity Planning]
        B[Vehicle Allocation]
        C[Escort Responsibilities]
        D[Temporal Coordination]
    end
    
    subgraph "Coordination Mechanisms"
        E[Conflict Detection]
        F[Resolution Strategies]
        G[Consistency Checking]
        H[Validation Rules]
    end
    
    subgraph "Implementation Patterns"
        I[Observer Pattern]
        J[Mediator Pattern]  
        K[Command Pattern]
        L[State Pattern]
    end
    
    A --> E
    B --> E
    C --> E
    D --> E
    E --> F
    F --> G
    G --> H
    
    E --> I
    F --> J
    G --> K
    H --> L
    
    style A fill:#e3f2fd
    style E fill:#f3e5f5
    style I fill:#e8f5e8
```

**Coordination Patterns**

*Observer Pattern*
: Models subscribe to household state changes and react automatically

*Mediator Pattern*
: Central coordinator manages interactions between household members

*Command Pattern*
: Coordination decisions recorded as commands for undo/redo capability

*State Pattern*
: Household coordination state machines manage complex interactions

## Data Flow Architecture

### Input Data Processing

```python
# Data flow pipeline example
class DataPipeline:
    def load_population_data(self):
        # Load synthetic population
        # Validate demographic consistency
        # Create person and household objects
        
    def load_landuse_data(self):
        # Load zonal employment and households
        # Validate geographic consistency  
        # Create accessibility measures
        
    def load_network_data(self):
        # Load transportation networks
        # Process level-of-service matrices
        # Create mode-specific accessibility
```

### Model Data Exchange

**Standardized Interfaces**
- Consistent data access patterns across models
- Type-safe data contracts and validation
- Version control and backward compatibility

**State Management**
- Immutable data structures where possible
- Copy-on-write for performance optimization
- Transaction-like rollback capabilities

### Output Data Generation

**Incremental Results**
- Models generate outputs incrementally
- Intermediate validation at each stage
- Early error detection and recovery

**Result Aggregation**
- Collect results from distributed processing
- Merge and validate consistency
- Generate final output files and reports

## Performance Architecture

### Memory Management

**Streaming Processing**
- Process households in batches to control memory usage
- Lazy evaluation and generator patterns
- Garbage collection optimization

**Data Compression**
- Compress intermediate results and caches
- Use efficient data structures (numpy arrays)
- Memory mapping for large datasets

### Computational Optimization

**Vectorization**
- Use numpy/pandas vectorized operations
- Batch utility calculations across alternatives
- Efficient probability calculations

**Caching Strategies**
- Cache expensive calculations (logsums, utilities)
- Invalidate caches based on dependencies
- Persistent caching across model runs

**Parallel Processing**
- Multi-processing for household-level parallelism
- Thread-safe shared data structures
- Load balancing and work distribution

## Error Handling and Recovery

### Validation Framework

**Input Validation**
- Data completeness and consistency checks
- Range validation and outlier detection  
- Cross-reference validation between datasets

**Runtime Validation**
- Model convergence monitoring
- Choice probability validation
- Household consistency checks

**Output Validation**
- Aggregate control validation
- Reasonableness checks against observed data
- Quality metrics and diagnostic reporting

### Recovery Strategies

**Graceful Degradation**
- Continue processing with warnings for non-critical errors
- Use default values or imputation for missing data
- Provide diagnostic information for troubleshooting

**Retry Mechanisms**
- Automatic retry with different parameters
- Progressive relaxation of convergence criteria
- Alternative algorithm selection for problematic cases

## Integration Architecture

### tm2py Integration

**Component Integration**
- CT-RAMP as pluggable component in tm2py pipeline
- Standardized interfaces for data exchange
- Configuration management through tm2py framework

**Data Exchange**
- Input from population synthesis and accessibility
- Output to network assignment and validation
- Feedback loops for iterative convergence

### External System Integration

**UrbanSim Integration**
- Land use forecasts as CT-RAMP inputs
- Accessibility feedback to land use model
- Coordinated scenario management

**EMME Integration**
- Network representation and skimming
- Assignment of CT-RAMP trip tables
- Feedback of congested travel times

## Development Architecture

### Modular Design

**Plugin Architecture**
- Models as interchangeable components
- Standardized model interfaces
- Dynamic model loading and configuration

**Extension Points**
- Custom model implementations
- Alternative utility specifications
- Regional model adaptations

### Testing Framework

**Unit Testing**
- Individual model component testing
- Mock data and dependency injection
- Automated test suite execution

**Integration Testing**
- End-to-end pipeline testing
- Data consistency validation
- Performance regression testing

**Validation Testing**
- Comparison with observed data
- Sensitivity analysis automation
- Benchmark scenario testing

## Configuration Architecture

### Hierarchical Configuration

**System Configuration**
- Global settings and resource allocation
- Logging and monitoring configuration
- Performance tuning parameters

**Model Configuration**
- Model-specific parameters and settings
- Utility function specifications
- Alternative set definitions

**Scenario Configuration**
- Scenario-specific overrides
- Input data specifications
- Output requirements and formats

### Configuration Management

**Version Control**
- Configuration versioning and tracking
- Reproducible model runs
- Configuration diffing and merging

**Validation**
- Configuration schema validation
- Parameter range checking
- Dependency consistency validation

---

!!! tip "Architecture Benefits"
    - **Modularity**: Easy to modify and extend individual components
    - **Performance**: Optimized for large-scale microsimulation
    - **Reliability**: Robust error handling and recovery mechanisms
    - **Maintainability**: Clear separation of concerns and standardized interfaces

*This architecture provides the foundation for scalable, maintainable CT-RAMP implementation in tm2py.*

*Last updated: September 26, 2025*
