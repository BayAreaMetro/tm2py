# CT-RAMP Model Components

!!! info "Model Components Overview"
    CT-RAMP consists of 15 integrated model components that work together to simulate individual and household travel behavior. Each component addresses a specific aspect of the travel decision-making process.

## Model Component Categories

CT-RAMP components are organized into logical categories based on their role in the travel decision hierarchy:

### Long-term and Coordination Models
These models establish the context and constraints for daily travel decisions:

| Component | Purpose | Key Decisions |
|-----------|---------|---------------|
| [**Auto Ownership**](auto-ownership.md) | Vehicle availability | Number of household vehicles |
| [**Coordinated Daily Activity Pattern (CDAP)**](cdap.md) | Household coordination | Person activity patterns |

### Tour Generation Models  
These models determine what tours each person will make:

| Component | Purpose | Key Decisions |
|-----------|---------|---------------|
| [**Mandatory Tours**](mandatory-tours.md) | Work and school tours | Tour frequency by purpose |
| [**Joint Tours**](joint-tours.md) | Multi-person household tours | Joint tour participation |
| [**Individual Tours**](individual-tours.md) | Personal discretionary tours | Individual tour frequency |
| [**At-Work Subtours**](at-work-subtours.md) | Tours during work hours | Subtour generation |

### Spatial Choice Models
These models determine where travel occurs:

| Component | Purpose | Key Decisions |
|-----------|---------|---------------|
| [**Tour Destination Choice**](tour-destination.md) | Primary destination selection | Destination zones and locations |

### Modal Choice Models
These models determine how travel occurs:

| Component | Purpose | Key Decisions |
|-----------|---------|---------------|
| [**Tour Mode Choice**](tour-mode-choice.md) | Primary transportation mode | Tour-level mode selection |
| [**Trip Mode Choice**](trip-mode-choice.md) | Trip-level mode decisions | Individual trip modes |

### Temporal Choice Models
These models determine when travel occurs:

| Component | Purpose | Key Decisions |
|-----------|---------|---------------|
| [**Tour Time-of-Day**](tour-tod.md) | Departure and arrival timing | Tour start and end times |

### Trip Detail Models
These models add complexity and realism to travel patterns:

| Component | Purpose | Key Decisions |
|-----------|---------|---------------|
| [**Stop Frequency**](stop-frequency.md) | Intermediate stop decisions | Number of stops by direction |
| [**Stop Location**](stop-location.md) | Stop destination choices | Stop locations and purposes |

## Model Execution Sequence

The components execute in a carefully orchestrated sequence to ensure proper dependencies:

``` mermaid
graph TD
    A[Auto Ownership] --> B[CDAP]
    B --> C[Mandatory Tours]
    B --> D[Joint Tours]
    C --> E[Individual Tours]
    D --> E
    E --> F[Tour Destination]
    F --> G[Tour Mode Choice]
    G --> H[Tour Time-of-Day]
    H --> I[Stop Frequency]
    I --> J[Stop Location]
    J --> K[Trip Mode Choice]
    C --> L[At-Work Subtours]
    
    subgraph "Phase 1: Context"
        A
        B
    end
    
    subgraph "Phase 2: Tour Generation"
        C
        D
        E
        L
    end
    
    subgraph "Phase 3: Tour Characteristics"
        F
        G
        H
    end
    
    subgraph "Phase 4: Trip Details"
        I
        J
        K
    end
    
    style A fill:#e3f2fd
    style B fill:#f3e5f5
    style F fill:#e8f5e8
    style K fill:#fff3e0
```

## Model Dependencies and Data Flow

Understanding how models depend on each other is crucial for system comprehension:

### Data Dependencies
- **Auto Ownership** requires: household demographics, accessibility measures
- **CDAP** requires: person characteristics, auto ownership results
- **Tour Generation** requires: CDAP patterns, person types, accessibility
- **Tour Characteristics** require: generated tours, level-of-service data
- **Trip Details** require: tour characteristics, stop purposes

### Feedback Mechanisms
Some models have iterative relationships:

**Accessibility Feedback**
: Tour destination and mode choice results update accessibility measures

**Household Coordination**  
: Joint decisions influence individual tour generation and characteristics

**Capacity Constraints**
: Transit ridership affects level-of-service for subsequent iterations

## Common Model Patterns

All CT-RAMP components share common design patterns:

### Choice Model Structure
Most components use **discrete choice models** with these elements:

**Alternative Generation**
: Define the set of available choices (destinations, modes, times, etc.)

**Utility Calculation**
: Compute the attractiveness of each alternative using utility functions

**Probability Calculation**
: Convert utilities to choice probabilities using logit formulations

**Choice Selection**
: Select alternatives based on computed probabilities and random draws

### UEC Integration
Components use the **Utility Expression Calculator (UEC)** framework:

**Utility Specifications**
: Mathematical expressions defining alternative attractiveness

**Parameter Management**
: Organized storage and access to model coefficients

**Logsum Calculation**
: Generation of accessibility measures for nested choice structures

## Model Customization and Configuration

### Parameter Customization
Each model component can be customized through:

**Utility Function Parameters**
: Coefficients that control the importance of different factors

**Alternative Set Definitions**
: Which choices are available in different contexts

**Segmentation Schemes**
: How the population is divided for different model applications

### Regional Adaptation
Models can be adapted for different regions through:

**Local Calibration**
: Parameter estimation using regional observed data

**Alternative Specifications**
: Regional variations in available choices

**Validation Standards**
: Local benchmarks for model performance

## Component Documentation Structure

Each component documentation page includes:

### Model Overview
- Purpose and role in the overall system
- Key behavioral assumptions and theory
- Relationship to other model components

### Technical Specification  
- Model structure and choice alternatives
- Utility function specifications
- Parameter definitions and typical values

### Data Requirements
- Input data sources and formats
- Required preprocessing and validation
- Output data products and uses

### Implementation Guidance
- Configuration and setup procedures
- Calibration and validation approaches  
- Common issues and troubleshooting

### Examples and Use Cases
- Practical applications and scenarios
- Analysis templates and workflows
- Interpretation guidance for results

## Getting Started with Components

### For New Users
1. **Start with [Auto Ownership](auto-ownership.md)** - Foundation model that affects all others
2. **Understand [CDAP](cdap.md)** - Key to household coordination concepts
3. **Follow the execution sequence** - Work through models in dependency order

### For Technical Users
1. **Review [UEC specifications](../uec/framework.md)** for each component
2. **Understand [coordination mechanisms](cdap.md)** for household interactions
3. **Study [integration patterns](../architecture.md)** for system design

### For Model Developers
1. **Examine [utility functions](tour-destination.md)** for specification patterns
2. **Review [calibration procedures](mandatory-tours.md)** for parameter estimation
3. **Study [validation approaches](trip-mode-choice.md)** for quality assurance

---

!!! tip "Component Navigation"
    Each component page is self-contained but includes cross-references to related models. Use the dependency diagram above to understand the logical flow between components.

!!! note "Model Complexity"
    Components vary in complexity from simple frequency models to sophisticated nested choice structures. Start with simpler components to build understanding before tackling complex spatial choice models.

*This overview provides the roadmap for understanding CT-RAMP's comprehensive modeling system.*

*Last updated: September 26, 2025*
