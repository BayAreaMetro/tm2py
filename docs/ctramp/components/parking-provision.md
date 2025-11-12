# Parking Provision Model

!!! info "Model Purpose" 
    The Parking Provision Model determines whether a household receives free parking or reimbursed parking at their work location

## Model Overview

**Number of Models**:1  
**Decision-Making Unit:** Workers who has a workplace location with a park area type == 1 (downtown locations)  
**Model Form:** Multinomial Logit  
**Alternatives:** 3 (Free Parking, Paid Parking (no parking provision), Reimbursed Parking)

### Purpose and Role
The Parking Provision Model predicts which workers have free onsite parking, reimbursed parking, or paid parking (no parking provision). Workers eligible for this parking provisons work in a downtown location (*park_area == 1*). Reimbursement can be full and partial reimbursement. 

Persons with workplace outside of *park_area == 1* are assumed to receive free parking from their workplace. 

The model predict the parking provisions based on a multinomial logit discrete choice model based on the following explanatory variables:  

- Household Income
- Occupation
- Weighted daily equivalent of average monthly cost

**Important Note**: Free onsite parking is not the same as full reimbursement. Those with free onsite parking will always park at their destination location. Those with reimbursement will determine their parking location based on the parking location choice model.



*Last updated: November 12, 2025*
