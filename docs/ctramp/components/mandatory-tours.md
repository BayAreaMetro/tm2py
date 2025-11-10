# Mandatory Tours Model

!!! warning "Documentation Update Required"
    This component documentation needs to be updated based on the SANDAG ABM Model Design document to ensure accuracy. The current content may contain inaccuracies compared to the original CT-RAMP specifications.

!!! info "SANDAG Design Foundation"
    TM2's CT-RAMP implementation is based on the San Diego SANDAG design. Please refer to the [SANDAG ABM Model Design](../SANDAG_ABM_Model_Design.docx) document for accurate specifications.

## Status: Pending Update

This component documentation is scheduled for update with accurate specifications from the SANDAG CT-RAMP design document. The update will include:

- Correct model structure and tour frequency alternatives
- Accurate choice alternatives for work, school, and university tours
- Proper utility functions and explanatory variables
- Valid market segmentation and behavioral assumptions
- Correct integration with CDAP and tour scheduling models

## Component Overview

The Mandatory Tours Model generates work and school tours for persons whose CDAP pattern includes mandatory activities.

**Model Type**: To be verified against SANDAG design document
**Decision Unit**: Individual persons with mandatory patterns
**Purpose**: Determine frequency and characteristics of mandatory tours

## Temporary Reference

Until this documentation is updated, please refer to:
- [SANDAG ABM Model Design Document](../SANDAG_ABM_Model_Design.docx)
- [SANDAG ABM Model Estimation Document](../SANDAG_ABM_Model_Estimation.docx)
*Last updated: November 10, 2025 - Marked for SANDAG design alignment*

**Flexible Work Policies**
- Impact of telecommuting on travel demand
- Compressed work week effects on peak period travel

### Education Planning
**School Transportation Policy**
- Effects of school choice on tour generation
- Transportation service impacts on attendance

**University Development**
- Campus accessibility effects on enrollment and tours
- Mixed-use development impacts on student travel

---

!!! tip "Foundation for Tour Models"
    Mandatory tours provide the foundation for all other tour models. Ensure these generation rates are well-calibrated before proceeding to tour characteristic models.

!!! note "Policy Sensitivity"
    This model is key for analyzing telecommuting, flexible work, and transportation demand management policies. Careful calibration of policy variables is essential.

**Related Components:**
- [CDAP](cdap.md) - Provides mandatory activity pattern inputs
- [Tour Destination](tour-destination.md) - Uses mandatory tour generation results
- [At-Work Subtours](at-work-subtours.md) - Depends on work tour generation

*Last updated: September 26, 2025*
