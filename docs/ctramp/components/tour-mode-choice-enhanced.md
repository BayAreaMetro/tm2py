# Tour Mode Choice Model

!!! warning "Documentation Update Required"
    This component documentation needs to be updated based on the SANDAG ABM Model Design document to ensure accuracy. The current content may contain inaccuracies compared to the original CT-RAMP specifications.

!!! info "SANDAG Design Foundation"
    TM2's CT-RAMP implementation is based on the San Diego SANDAG design. Please refer to the [SANDAG ABM Model Design](../SANDAG_ABM_Model_Design.docx) document for accurate specifications.

## Status: Pending Update

This component documentation is scheduled for update with accurate specifications from the SANDAG CT-RAMP design document. The update will include:

- Correct nested logit structure and mode alternatives
- Accurate utility functions and level-of-service variables
- Proper market segmentation by tour purpose and person type
- Valid behavioral assumptions and choice constraints
- Correct integration with accessibility and assignment systems

## Component Overview

The Tour Mode Choice Model determines the primary transportation mode for each tour based on level-of-service, demographics, and policy variables.

**Model Type**: To be verified against SANDAG design document
**Decision Unit**: Individual tours
**Purpose**: Select primary mode for tour-level travel

## Temporary Reference

Until this documentation is updated, please refer to:
- [SANDAG ABM Model Design Document](../SANDAG_ABM_Model_Design.docx)
- [SANDAG ABM Model Estimation Document](../SANDAG_ABM_Model_Estimation.docx)
*Last updated: November 10, 2025 - Marked for SANDAG design alignment*

**Segmentation Effects**: Realistic demographic and geographic variations
**Policy Sensitivity**: Reasonable response to policy changes
**Temporal Stability**: Consistent behavior across time periods
**Cross-Validation**: Performance on holdout datasets

### Implementation Considerations

**Computational Performance**: Execution time for large populations
**Numerical Stability**: Robust handling of extreme values
**Convergence Properties**: Stability in iterative feedback processes
**Maintainability**: Clear structure for updates and modifications

This comprehensive Tour Mode Choice Model forms the backbone of transportation policy analysis in CT-RAMP, providing detailed behavioral representation of how individuals choose among available transportation alternatives based on their personal circumstances, trip characteristics, and the built environment context.