# Outputs

Travel Model Two (TM2) generates a variety of output files across multiple categories.

## Skims

Skims represent level-of-service (LOS) indicators across transportation modes and time periods.

- **Highway Skim Matrices**  
  `skims\HWYSKM[TimePeriod]_taz.tpp`  
  Include travel time, distance, bridge tolls, and other metrics for different vehicle types (e.g., DA, S2, S3).

- **MAZ to MAZ Distances**  
  - `skims\bike_distance_maz_maz.txt`  
  - `skims\ped_distance_maz_maz.txt`  
  Provide bike and pedestrian distances between Micro Analysis Zones (MAZs).

- **TAZ to TAZ Bike Distances**  
  `skims\bike_distance_taz_taz.txt`  
  Show bike distances between Traffic Analysis Zones (TAZs).

- **Transit Skims**  
  `skims\transit_skims_[TimePeriod]_[Iteration]_[Inner Iteration].omx`  
  Contain transit LOS indicators for various time periods and iterations.

## CTRAMP Output

CTRAMP outputs provide detailed data on individual and joint travel behaviors.

- **Individual Tours File**  
  `indivTourData_[iteration].csv`  
  Records individual tour data.

- **Individual Trips File**  
  `indivTripData_[iteration].csv`  
  Logs individual trip details.

- **Joint Tours File**  
  `jointTourData_[iteration].csv`  
  Captures joint tour information.

- **Joint Trips File**  
  `jointTripData_[iteration].csv`  
  Details joint trip data.

- **Resimulated Transit Trips File**  
  `ctramp_output/indivTripDataResim_[iteration]_[inner_iteration].csv`  
  Contains resimulated transit trip data.

- **Unconstrained Parking Demand File**  
  `ctramp_output/unconstrainedPNRDemand_[iteration]0.csv`  
  Shows parking demand without constraints.

- **Constrained Parking Demand File**  
  `ctramp_output/constrainedPNRDemand_[iteration]1.csv`  
  Displays parking demand with constraints.

- **Tour and Trip Mode Codes**  
  Provides codes for different travel modes.

- **Time Period Codes**  
  Lists codes representing various time periods.

## Assignment Outputs

Assignment outputs reflect the results of network assignments.

- **Highway Assignment Networks**  
  - `hwy\maz_preload_[TimePeriod].net`  
  - `hwy\load[TimePeriod].net`  
  - Other intermediate and final highway network assignment files.

- **Transit Assignment Tables**  
  - `trn\boardings_by_line_[TimePeriod].txt`  
  - `trn\boardings_by_segment_[TimePeriod].txt`  
  Detail transit boardings by line and by segment.