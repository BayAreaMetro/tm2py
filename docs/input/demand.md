# Fixed Demand Data ✈️

MTC uses representations of internal/external and air passenger demand that is year-, but not scenario-, specific -- meaning simple sketch methods are used to estimate this demand from past trends. This demand is then fixed for each forecast year and does not respond to changes in land use or the transport network.

## Internal/External

So-called internal/external demand is travel that either begins or ends in the nine county Bay Area. This demand is based on Census journey-to-work data and captures all passenger (i.e. non-commercial) vehicle demand. This demand is introduced to the model via a matrix that contains the following four demand tables in production-attraction format:

* Daily single-occupant vehicle flows;
* Daily two-occupant vehicle flows;
* Daily three-or-more occupant vehicle flows; and,
* Daily vehicle flows, which is the sum of the first three tables and not used by the travel model.

## Air Passenger

Air passenger demand is based on surveys of air passenger and captures demand from the following travel modes: passenger vehicles, rental cars, taxis, limousines, shared ride vans, hotel shuttles, and charter buses. This demand is introduced to the model via time period specific matrices that contain the following six flow tables:

* Single-occupant vehicles;
* Two-occupant vehicles;
* Three-occupant vehicles;
* Single-occupant vehicles that are willing to pay a high-occupancy toll lane fee;
* Two-occupant vehicles that are willing to pay a high-occupancy toll lane fee; and,
* Three-occupant vehicles that are willing to pay a high-occupancy toll lane fee.
