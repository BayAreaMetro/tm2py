# CT-RAMP System Overview

!!! info "About This Guide"
    This overview provides a comprehensive introduction to CT-RAMP (Comprehensive Travel-demand forecasting Research And Modeling Platform), the activity-based travel demand modeling system used in Travel Model Two.

## What is Activity-Based Modeling?

Activity-based modeling represents a fundamental shift from traditional trip-based approaches to understanding travel behavior. Rather than predicting trips directly, activity-based models recognize that **travel is derived from the need to participate in activities** distributed across time and space.

### Key Concepts

**Individual Decision-Making**
: Models individual persons and their daily activity patterns rather than aggregate zone-to-zone flows

**Household Interactions**
: Recognizes that household members coordinate their activities and share resources (vehicles, childcare, etc.)

**Time-Space Constraints**
: Explicitly considers the temporal and spatial constraints that shape activity participation

**Behavioral Realism**
: Incorporates detailed behavioral theories about how people make activity and travel decisions

## CT-RAMP System Architecture

CT-RAMP implements activity-based modeling through a comprehensive system of interconnected choice models that simulate the daily activity and travel patterns of each person in a synthetic population.

### System Components

``` mermaid
graph TB
    subgraph 1["Population & Context"]
        direction LR
        1a[Synthetic Population]
        1b[Land Use Data]
        1c[Transportation Networks]
        1a ~~~ 1b ~~~ 1c
    end
    
   1 --> 2
    subgraph 2["Long-term Choices"]
        direction LR
        2a[Auto Ownership]
        2b[Work & School Location]
        2a ~~~ 2b
    end

    2 --> 3
    subgraph 3["Mobility"]
        direction LR
        3a[Auto Ownership]
        3b[Transponder Choice]
        3c[Free Parking]
        3a ~~~ 3b ~~~ 3c
    end

    3 --> 4
    subgraph 4["Daily Activity Planning"]

        direction TB
        subgraph 4a["Coordinated Daily Activity Pattern"]
            direction LR
            4a1[CDAP]
            4a2[Explicit Telecommute]
            4a1 --> 4a2
        end

        4b[Mandatory]
        4c[Non-Mandatory]
        4d[Home]

        subgraph 4f["Individual Mandatory Tours"]
            direction TB
            4f1[Frequency]
            4f2[Time of Day]
            4f3[Tour Mode]
            4f1 --> 4f2
            4f2 --> 4f3
        end
        subgraph 4g["Joint Non-Mandatory Tours"]
            direction TB
            4g1[Frequency/Composition]
            4g2[Participation]
            4g3[Destination]
            4g4[Time of Tour]
            4g5[Tour Mode]
            4g1 --> 4g2
            4g2 --> 4g3
            4g3 --> 4g4
            4g4 --> 4g5
        end
        subgraph 4h["Individual Non-Mandatory Tours"]
            direction TB
            4h1[Frequency]
            4h2[Destination]
            4h3[Time of Day]
            4h4[Tour Mode]
            4h1 --> 4h2
            4h2 --> 4h3
            4h3 --> 4h4
        end
        subgraph 4i["At-Work Subtours"]
            direction TB
            4i1[Frequency]
            4i2[Destination]
            4i3[Time of Day]
            4i4[Tour Mode]
            4i1 --> 4i2
            4i2 --> 4i3
            4i3 --> 4i4
        end

        4a --> 4b
        4a --> 4c
        4a --> 4d
        4b --> 4f
        4f --> 4i
        4c --> 4g
        4c --> 4h

    end
    
    4 --> 5
    subgraph 5["Stop-level Choices"]
        direction LR
        5a[Stop Frequency]
        5b[Stop Location]
        5a --> 5b
    end
    
    5 --> 6
    subgraph 6["Trip-level Choices"]
        direction LR
        6a[Trip mode]
        6b[Auto Parking]
        6a --> 6b
    end
    

    classDef Household fill:#e8f5e8
    class 2a,2b,4a1,4g1 Household
    
    
```

## Model Hierarchy and Flow

CT-RAMP follows a logical hierarchy from long-term decisions to immediate travel choices:

### 1. Long-term Choices
**Auto Ownership Model**
- Determines household vehicle availability
- Influences all subsequent mobility decisions
- Considers household size, income, accessibility

### 2. Daily Activity Pattern Formation
**Coordinated Daily Activity Pattern (CDAP)**
- Coordinates activity patterns among household members
- Determines mandatory vs. non-mandatory activity participation
- Resolves household resource conflicts

### 3. Tour Generation
Tours represent round trips from home for specific purposes:

**Mandatory Tours**
- Work and school tours (required activities)
- Generated based on person type and obligations

**Joint Tours**  
- Multi-person household tours
- Shopping, recreation, social activities

**Individual Non-Mandatory Tours**
- Personal discretionary activities
- Conditional on household coordination results

### 4. Tour Characteristics
For each generated tour, the system determines:

**Destination Choice**
- Where to conduct the primary activity
- Based on accessibility, land use, competition

**Mode Choice**
- How to travel (auto, transit, walk, bike, etc.)
- Considers tour characteristics and level-of-service

**Time-of-Day Choice**
- When to depart and return
- Balances activity duration with travel conditions

### 5. Intermediate Stops
**Stop Frequency**
- How many stops to make en route
- Separate models for outbound and return directions

**Stop Location**
- Where to make intermediate stops
- Considers stop purpose and routing constraints

**Trip Mode Choice**
- Mode for each trip segment
- May differ from primary tour mode

## Behavioral Foundations

### Choice Modeling Framework
CT-RAMP uses **discrete choice models** based on random utility theory:

**Utility Functions**
: Mathematical representations of decision-maker preferences combining observed attributes and random variation

**Logit Models**
: Probabilistic choice models that translate utilities into choice probabilities

**Nested Structures**
: Hierarchical choice structures that capture correlations between similar alternatives

### Coordination Mechanisms

**Household Resource Allocation**
- Vehicle assignment and sharing
- Childcare and escort responsibilities
- Joint activity participation

**Temporal Coordination**
- Synchronized departure times
- Shared ride arrangements  
- Activity duration coordination

**Spatial Coordination**
- Destination clustering for efficiency
- Escort tour routing
- Joint activity locations

## Data Requirements and Scale

### Input Data Requirements
CT-RAMP requires detailed input data to support microsimulation:

**Synthetic Population**
- Individual persons with demographics, employment, school enrollment
- Households with income, size, vehicle ownership constraints
- Typically 2.5+ million persons in 1+ million households for Bay Area

**Land Use Data**
- Employment by industry and occupation category
- Households and population by income and demographic segments
- Commercial floor space and attractions
- Detailed geographic representation (40,000+ micro-zones)

**Transportation Networks**
- All-streets highway network with traffic controls
- Complete transit system with routes, schedules, fares
- Pedestrian and bicycle facilities
- Level-of-service matrices (travel times, costs) by time period

### Computational Scale
Modern CT-RAMP implementations handle:

**Population Size**
- 1+ million households, 2.5+ million persons
- 10+ million tours, 25+ million trips annually

**Geographic Detail**  
- 40,000+ micro-analysis zones (MAZs)
- 1,500+ traffic analysis zones (TAZs)
- 3,000+ transit access points (TAPs)

**Temporal Resolution**
- Half-hourly time periods (48 periods per day)
- Peak spreading and congestion feedback

## Model Innovation and Enhancements

### Advanced Features

**Transit Capacity and Crowding**
- Explicit modeling of transit vehicle capacity
- Crowding discomfort in mode choice
- Dynamic capacity constraints

**Detailed Spatial Resolution**
- Micro-zone level destination choice
- Walk access to specific transit stops
- Realistic pedestrian routing

**Emerging Mobility Options**
- Transportation Network Company (TNC) services
- Ride-sharing and ride-hailing
- Autonomous vehicle scenarios

### Validation and Calibration

**Observed Data Integration**
- California Household Travel Survey (CHTS)
- On-board transit surveys  
- Census commute data
- Regional travel behavior studies

**Performance Metrics**
- Trip generation rates by purpose and person type
- Mode share validation by geography and demographics
- Temporal distribution and peak spreading
- Accessibility and equity measures

## Benefits and Applications

### Planning Applications

**Scenario Analysis**
- Land use and development patterns
- Transportation infrastructure investments
- Policy interventions (pricing, regulations)

**Performance Evaluation**
- Accessibility impacts
- Environmental outcomes
- Social equity analysis
- Economic development effects

### Technical Advantages

**Behavioral Realism**
- Captures complex decision-making processes
- Represents household interactions
- Models activity-travel trade-offs

**Policy Sensitivity**
- Responds to detailed policy variables
- Captures distributional effects
- Models emerging mobility options

**Forecasting Capability**
- Robust response to demographic changes
- Adaptive to new transportation technologies
- Consistent with activity-based behavior

## Implementation Considerations

### Computational Requirements

**Processing Time**
- Full population model: 8-24 hours on modern hardware
- Parallelization opportunities at household level
- Memory requirements: 32+ GB RAM for full implementation

**Data Management**
- Large input datasets (10+ GB)
- Detailed output files (5+ GB per iteration)
- Quality assurance and validation procedures

### Model Development Process

**Calibration and Validation**
- Parameter estimation using observed data
- Sensitivity testing and reasonableness checks  
- Iterative refinement and improvement

**Quality Assurance**
- Extensive validation against observed patterns
- Cross-validation with independent datasets
- Sensitivity analysis and robustness testing

## Integration with Regional Modeling

CT-RAMP operates as part of an integrated regional modeling system:

**UrbanSim Integration**
- Land use forecasts provide demographic and employment inputs
- Feedback from accessibility to land development

**Network Assignment**
- Trip matrices from CT-RAMP loaded onto transportation networks
- Congested travel times feed back to accessibility calculations

**Economic Modeling**
- Commercial vehicle models for freight and service trips
- Economic impact analysis of transportation investments

## Learning Resources

### For New Users
1. **Conceptual Foundation**: Understanding activity-based modeling principles
2. **System Architecture**: How CT-RAMP components work together
3. **Data Requirements**: Input preparation and quality standards
4. **Interpretation**: Understanding and using model outputs

### For Technical Users  
1. **Model Specifications**: Detailed utility functions and parameters
2. **Calibration Procedures**: Parameter estimation and validation methods
3. **Customization**: Adapting models for local conditions
4. **Performance Optimization**: Computational efficiency improvements

### For Policy Analysts
1. **Scenario Development**: Creating meaningful policy alternatives
2. **Output Analysis**: Interpreting results for decision-making
3. **Uncertainty Assessment**: Understanding model limitations
4. **Communication**: Presenting results to stakeholders

---

!!! tip "Getting Started"
    - **New to CT-RAMP?** Start with [Model Components](components/index.md) for detailed descriptions
    - **Ready to run models?** See [Execution Workflow](execution/workflow.md) for step-by-step guidance
    - **Need technical details?** Check [Architecture](architecture.md) for implementation specifics

*Last updated: September 26, 2025*
