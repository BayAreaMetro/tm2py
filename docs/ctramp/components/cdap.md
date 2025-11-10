# Coordinated Daily Activity Pattern (CDAP)

!!! warning "Documentation Update Required"
    This component documentation needs to be updated based on the SANDAG ABM Model Design document to ensure accuracy. The current content may contain inaccuracies compared to the original CT-RAMP specifications.

!!! info "SANDAG Design Foundation"
    TM2's CT-RAMP implementation is based on the San Diego SANDAG design. Please refer to the [SANDAG ABM Model Design](../SANDAG_ABM_Model_Design.docx) document for accurate specifications.

## Status: Pending Update

This component documentation is scheduled for update with accurate specifications from the SANDAG CT-RAMP design document. The update will include:

- Correct model structure and formulation
- Accurate choice alternatives and utility functions  
- Proper behavioral assumptions and market segmentation
- Valid input/output specifications
- Correct estimation and calibration details

## Component Overview

The Coordinated Daily Activity Pattern (CDAP) model determines the daily activity pattern for each household member and coordinates these patterns across the household.

**Model Type**: To be verified against SANDAG design document
**Decision Unit**: Household level with individual member coordination
**Purpose**: Establish daily activity commitments before tour-level modeling

## Temporary Reference

Until this documentation is updated, please refer to:
- [SANDAG ABM Model Design Document](../SANDAG_ABM_Model_Design.docx)
- [SANDAG ABM Model Estimation Document](../SANDAG_ABM_Model_Estimation.docx)

*Last updated: November 10, 2025 - Marked for SANDAG design alignment*

*Last updated: November 10, 2025 - Marked for SANDAG design alignment*

**Childcare Policy**
- Effects of childcare availability on activity patterns
- Impact on parent work participation and coordination

### Behavioral Research
**Household Decision-Making**
- Understanding coordination mechanisms in travel behavior
- Gender roles and activity allocation within households

**Life Course Analysis**
- How household coordination changes over the lifecycle
- Adaptation to changing household composition and needs

---

!!! tip "Coordination Foundation"
    CDAP is fundamental to household-based modeling. Ensure coordination mechanisms are well-understood and calibrated, as they affect all subsequent household and individual models.

!!! warning "Complexity Management"
    Large households create computational challenges. Consider simplification strategies for households with 5+ members while maintaining behavioral realism.

**Related Components:**
- [Auto Ownership](auto-ownership.md) - Provides vehicle availability constraints
- [Mandatory Tours](mandatory-tours.md) - Uses CDAP patterns for tour generation
- [Joint Tours](joint-tours.md) - Builds on CDAP coordination results

*Last updated: September 26, 2025*
