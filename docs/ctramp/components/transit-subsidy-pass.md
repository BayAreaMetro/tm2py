# Transit Subsidy and Transit Pass Model

!! Model Purpose
    The Transit Subsidy and Tranist Pass Model determines whether a person receives a transit subsidy and/or transit pass


## Model Overview

**Number of Models:** 2 (Transit Subsidy, Transit Pass)
**Decision-Making Unit:** Person  
**Model Form:** Binomial Logit  
**Alternatives:** 2 (Yes or No)

### Purpose and Role
The transit subsidy and pass model predicts whether a person receives a transit subsidy and/or transit pass. 

The model will first run the transit subsidy choice model to see if the person receives a transit subsidy and the percentage of transit subsidy. The transit subsidy model is based on the following variables:

- Person Type 
- Job Type
- Parking Cost
- Access to Transit

Transit Subsidy amount is then set randomly based on a transit subsidy distribution table.

Following the transit subsidy model, the transit pass model will run to determine if a person receives a transit pass based on the following variables:

- Person Type
- Job Type
- Household Income
- Auto minus transit generalized time to work/school
- Subsidy offered

If the person does not have a work or school location, auto minus transit generalized time to work/school is 0.

Auto generalized time to work/school is based on AM drive alone values, value of time, auto operating cost, origin and destination terminal time, and parking cost. Auto generalized time is the summation of in-vehicle time, operating cost, bridge toll, parking cost, and terminal time. 

Transit Generalized time to work/school is based on the total travel time on transit for the AM Period. This includes total in-vehicle time as well as access, egress, auxiliary time and wait times. 