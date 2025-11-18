# Stop Frequency Model

## Model Overview

**Number of Models:** 10 (By Purpose plus one model for at-work subtours)  
**Decision-Making Unit:** Person  
**Model Form:** Multinomial Logit  
**Alternatives:** 16, with a maximum of 3 stops per tour direction, 6 total stops on tour

### Model Purpose and Role
The stop frequency choice models determines the number of intermediate stops on the way to and from the primary destination. The model allows more than one stop in each direction (up to a maximum of 3) for a total of 8 trips per tour, four on each tour leg. 

The stop frequency model is based on the following explanatory variables: 
- Household Income
- Number of full time workers in the household
- Number of part time workers in the household
- Number of non-workers in the household
- Number of children in the household
- Number of individual/joint mandatory and non-mandatory tours made by the household
- Person Type
- Age
- Tour Mode 
- Tour distance from home MAZ to primary destination
- Shopping Accessibilities
- Maintenance Accessibilities
- Discretionary Accessibilities

*Not all explanatory variables will be used in each model*

The model will first process the joint tours for the household and then loop through each person and process individual tours for each person of the household. 