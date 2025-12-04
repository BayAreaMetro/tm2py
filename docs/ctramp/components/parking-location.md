# Parking Location Model

!!! info "Model Purpose" 
    The Parking Location Model determines where vehicles are parked at the end of each trip with a destination in *parkarea 1*

## Model Overview

**Number of Models**: 1  
**Decision-Making Unit:** Trips with non-home destination in areas with paid parking  
**Model Form:** Multinomial Logit  
**Alternatives:** MAZs within walking distance of trip destination  

### Purpose and Role
The Parking Location Choice Model predicts where vehicles are parked at the end of each trip. This model will only run if the stop location is in *parkarea == 1*, the person does not work from home, and the person does not have free parking. If there are no MAZs within walking distances of the destination MAZ, it is also not necessary to make a parking location choice. 

The model predict the parking location based on a multinomial logit discrete choice model based on the following explanatory variables:  

- Number of stalls available to the driver
- Parking Cost
- Walk distance to destination

**Important Notes**
The ParkLocationAlt.csv file in the UEC folder is used to determine the destination MAZ park area. If MAZ inputs are updated, this file will also need to be updated to reflect the MAZ input files. 

Parking inputs include both [hourly, daily, monthly] stalls in same MAZ and in different MAZs. While these variables are both used in the UEC and choice model, it serves more as way to note if the MAZ considered as an alternative is the same MAZ as the destination MAZ. The values for number of stalls in same MAZ and in different MAZ are the same in the MAZ input files. 