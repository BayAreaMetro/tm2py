# Commercial Vehicle Data 🚛

!!! info "Commercial Vehicle Data Preparation"
    For information on how to prepare commercial vehicle data files, see **[Creating Base Year Inputs](../create-base-year-inputs.md#commercial-vehicle-data)** 🚛

## Truck Distribution

MTC uses a simple three-step (generation, distribution, and assignment) commercial vehicle model to generate estimates of four types of commercial vehicles. The four vehicle types are very small (two-axle, four-tire), small (two-axle, six-tire), medium (three-axle), and large or combination trucks (four-or-more-axle).

### Friction Factors

The trip distribution step uses a standard gravity model with a blended travel time impedance measure. This file sets the friction factors, which are vehicle type specific, using an ASCII fixed format text file with the following data:

* Travel time in minutes (integer, starting in column 1, left justified);
* Friction factors for very small trucks (integer, starting in column 9, left justified);
* Friction factors for small trucks (integer, starting in column 17, left justified);
* Friction factors for medium trucks (integer, starting in column 25, left justified); and,
* Friction factors for large trucks (integer, starting in column 33, left justified).

### K-Factors

The trip distribution step also uses a matrix of K-factors to adjust the distribution results to better match observed data. This matrix contains a unit-less adjustment value; the higher the number, the more attractive the production/attraction pair.
