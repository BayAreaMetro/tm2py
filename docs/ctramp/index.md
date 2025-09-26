# CT-RAMP: Comprehensive Travel Model

!!! info "About CT-RAMP"
    CT-RAMP (Comprehensive Travel-demand forecasting Research And Modeling Platform) is the activity-based travel demand modeling system used in Travel Model Two. It simulates individual and household travel decisions through a comprehensive set of choice models that represent realistic behavioral decision-making processes.

## Quick Start Guide

### New to Activity-Based Modeling?
**Start Here:** [System Overview](overview.md) provides a comprehensive introduction to activity-based modeling concepts, CT-RAMP's behavioral foundations, and how it differs from traditional trip-based approaches.

### Ready to Understand the System?
**Architecture:** [Technical Architecture](architecture.md) details the system design, data flows, and implementation patterns used in CT-RAMP.

### Want to Run Models?
**Execution:** [Workflow Guide](execution/workflow.md) provides step-by-step instructions for model execution and configuration.

## What Makes CT-RAMP Unique?

### Activity-Based Foundation
CT-RAMP recognizes that **travel is derived from activities**, not trips. This fundamental insight leads to:

- **Realistic Behavior**: Models how people actually make activity and travel decisions
- **Household Coordination**: Captures joint decision-making within households
- **Time-Space Integration**: Explicitly considers temporal and spatial constraints
- **Policy Sensitivity**: Responds to detailed policy variables and scenarios

### Comprehensive Model System
CT-RAMP includes 15 integrated model components covering:

``` mermaid
graph TD
    A[Population Synthesis] --> B[Auto Ownership]
    B --> C[Daily Activity Pattern]
    C --> D[Mandatory Tours]
    C --> E[Joint Tours]
    D --> F[Individual Tours]
    E --> F
    F --> G[Tour Destination]
    G --> H[Tour Mode Choice]
    H --> I[Tour Time-of-Day]
    I --> J[Stop Frequency]
    J --> K[Stop Location]
    K --> L[Trip Mode Choice]
    D --> M[At-Work Subtours]

    subgraph "Long-term Choices"
        B
    end

    subgraph "Daily Planning"
        C
        D
        E
        F
    end

    subgraph "Tour Characteristics"
        G
        H
        I
    end

    subgraph "Trip Details"
        J
        K
        L
        M
    end

    style A fill:#e1f5fe
    style C fill:#f3e5f5
    style G fill:#e8f5e8
    style L fill:#fff3e0
```

## Technical Framework

### Core Systems

#### Utility Expression Calculator (UEC) Framework
CT-RAMP's mathematical foundation for all choice modeling:

- **[UEC Framework Documentation](uec-framework.md)** - Comprehensive guide to utility calculation system
- **Flexible Specification**: Mathematical expressions for choice utilities through Excel control files
- **Model Integration**: Seamless interface with Java choice model implementations
- **Performance Optimization**: Efficient calculation for large-scale microsimulation

## Model Components by Category

### Long-term and Coordination Models
These models establish the context for daily travel decisions:

| Component | Purpose | Key Outputs |
|-----------|---------|-------------|
| [**Auto Ownership**](components/auto-ownership.md) | Vehicle availability decisions | Household auto ownership level |
| [**Daily Activity Pattern**](components/cdap.md) | Household activity coordination | Person-level activity patterns |

### Tour Generation Models
These models determine what tours each person will make:

| Component | Purpose | Key Outputs |
|-----------|---------|-------------|
| [**Mandatory Tours**](components/mandatory-tours.md) | Work and school tour generation | Mandatory tour frequency |
| [**Joint Tours**](components/joint-tours.md) | Multi-person household tours | Joint tour participation |
| [**Individual Tours**](components/individual-tours.md) | Personal discretionary tours | Individual tour frequency |

### Spatial and Temporal Choice Models
These models determine where and when travel occurs:

| Component | Purpose | Key Outputs |
|-----------|---------|-------------|
| [**Tour Destination**](components/tour-destination.md) | Primary destination selection | Tour destination zones |
| [**Tour Mode Choice**](components/tour-mode-choice.md) | Transportation mode selection | Tour primary modes |
| [**Tour Time-of-Day**](components/tour-tod.md) | Departure and arrival timing | Tour start/end times |

### Trip-Level Models
These models add detail about intermediate stops and trip characteristics:

| Component | Purpose | Key Outputs |
|-----------|---------|-------------|
| [**Stop Frequency**](components/stop-frequency.md) | Intermediate stop decisions | Stop frequencies by direction |
| [**Stop Location**](components/stop-location.md) | Stop destination choices | Stop locations and purposes |
| [**Trip Mode Choice**](components/trip-mode-choice.md) | Trip-level mode decisions | Individual trip modes |
| [**At-Work Subtours**](components/at-work-subtours.md) | Tours during work hours | Subtour characteristics |

### Execution and Coordination
The [**Execution System**](execution/index.md) manages:

- **Dependency Resolution**: Automatic model sequencing based on data requirements
- **Household Coordination**: Joint decision-making across household members
- **Performance Optimization**: Parallel processing and intelligent caching
- **Quality Assurance**: Comprehensive validation and error checking

## Scale and Performance

### Computational Scale
Modern CT-RAMP implementations in the Bay Area handle:

- **Population**: 1+ million households, 2.5+ million persons
- **Geography**: 40,000+ micro-zones, 1,500+ traffic zones
- **Travel**: 10+ million tours, 25+ million trips annually
- **Time Resolution**: Half-hourly periods with peak spreading

### Data Requirements
Comprehensive input data enables realistic microsimulation:

- **[Input Data](../input/index.md)**: Population, land use, networks, accessibility
- **[Output Data](../output/ctramp.md)**: Individual travel patterns, household coordination
- **[Validation Data](data/validation.md)**: Observed behavior for calibration

## Integration with Travel Model Two

CT-RAMP operates as the demand forecasting component in Travel Model Two:

**Upstream Integration**
- **UrbanSim**: Provides land use forecasts and synthetic population
- **Network Processing**: Highway and transit network preparation
- **Accessibility Calculation**: Level-of-service matrix generation

**Downstream Integration**
- **Network Assignment**: CT-RAMP trips loaded onto transportation networks  
- **Feedback Mechanisms**: Congested travel times update accessibility measures
- **Validation and Reporting**: Model results compared to observed patterns

## Getting Started by User Type

### For Model Users
1. **[System Overview](overview.md)** - Understand activity-based modeling concepts
2. **[Model Components](components/index.md)** - Learn what each model does
3. **[Execution Workflow](execution/workflow.md)** - Run models step-by-step
4. **[Output Analysis](../output/ctramp.md)** - Interpret and use model results

### For Model Developers
1. **[Architecture](architecture.md)** - Technical implementation details
2. **[UEC Framework](uec-framework.md)** - Utility calculation system
3. **[Configuration](execution/configuration.md)** - Model setup and customization
4. **[Examples](examples/index.md)** - Development templates and patterns

### For Policy Analysts
1. **[Overview](overview.md)** - Behavioral foundations and policy sensitivity
2. **[Validation](data/validation.md)** - Model performance and limitations
3. **[Examples](examples/index.md)** - Analysis templates and use cases
4. **[Troubleshooting](execution/troubleshooting.md)** - Common issues and solutions

## Support Resources

### Documentation
- **[Comprehensive Guides](components/index.md)**: Detailed model component documentation
- **[UEC Framework](uec-framework.md)**: Utility calculation system and implementation details
- **[User Examples](examples/index.md)**: Practical analysis templates and workflows

### Quality Assurance
- **[Validation Framework](data/validation.md)**: Model performance metrics and benchmarks
- **[Troubleshooting](execution/troubleshooting.md)**: Common issues and resolution strategies
- **[Configuration Management](execution/configuration.md)**: Setup and customization guidance

---

!!! tip "Next Steps"
    - **New to CT-RAMP?**  [System Overview](overview.md)
    - **Ready to run models?**  [Execution Workflow](execution/workflow.md)
    - **Need technical details?**  [Architecture](architecture.md) or [UEC Framework](uec-framework.md)
    - **Want examples?**  [Usage Examples](examples/index.md)

!!! warning "Model Complexity"
    CT-RAMP is a sophisticated microsimulation system. We recommend starting with the [System Overview](overview.md) to understand the behavioral foundations before diving into technical implementation details.

*Last updated: September 26, 2025*
