# Skim Outputs

Travel Model Two generates comprehensive level-of-service (LOS) matrices called "skims" that represent impedances and costs between zones for different transportation modes and time periods.

## Highway Skims

### Highway Skim Matrices (OMX Format)
**File Pattern**: `skim_matrices/highway/HWYSKM[TimePeriod]_taz.omx`

Contains multiple matrices with highway travel impedances between Traffic Analysis Zones (TAZs):

- **Travel Time** - Peak and off-peak periods
- **Distance** - Network distance in miles
- **Bridge Tolls** - Toll costs for bridge crossings
- **Operating Costs** - Vehicle operating costs
- **Generalized Cost** - Combined time and cost impedance

**Vehicle Types Included**:

| Vehicle | Description|
|---|---|
|`DA`| Drive Alone|
|`S2`| Shared 2-person|
|`S3` | Shared 3+ person|
| `DATOLL`| Drive Alone with toll facilities|
| `S2TOLL` | Shared 2-person with toll facilities|  
| `S3TOLL` | Shared 3+ person with toll facilities|
| `TRK` | Truck|
| `TRKTOLL` | Truck with toll facilities|
| `LRGTRK` | Large Truck|
| `LRGTRKTOLL` | Large Truck with toll facilities|

**Skim Components**:

| Component| Description|
|---|---|
| `time` | pure travel time in minutes
| `dist` | distance in miles
| `cost` | cost 
| `hovdist` | distance on HOV facilities
| `tolldist` | distance on toll facilities
| `freeflowtime` | free flow travel time in minutes
| `bridgetoll_{vehicle}` | bridge tolls, {vehicle} refers to toll group
| `valuetoll_{vehicle}` | other, non-bridge tolls, {vehicle} refers to toll group
| `rlbty` | Reliability
| `autotime`| Auto Time


### MAZ-to-MAZ Highway Skims
**File Pattern**: `skim_matrices/highway/HWYSKIM_MAZ_MAZ_DA.csv`

**Format**: CSV with columns:
```
FROM_ZONE, TO_ZONE, COST, DISTANCE, BRIDGETOLL
```

**Content**:
- Shortest path costs between Micro Analysis Zones (MAZs)
- Distance in miles
- Bridge toll costs
- Generalized cost (time + operating cost + tolls)

### Drive Access Skims
**File Path**: `skims/drive_access_skims.csv`

**Format**: CSV with columns:
```
FTAZ,MODE,PERIOD,TTAP,TMAZ,TTAZ,DTIME,DDIST,DTOLL,WDIST
```

**Content**:
- Drive access times and costs to transit stops
- Walking distances from parking to transit access points
- Used for park-and-ride and kiss-and-ride mode choice

## Transit Skims

### Transit Skim Matrices (OMX Format)
**File Pattern**: `skims/trnskm[TimePeriod]_[TransitClass].omx`

**Transit Classes**:

|Transit Class| Description|
|---|---|
| `WLK_TRN_WALK` | Walk to and from transit
| `PNR_TRN_WLK` | Drive to transit (Park and Ride) and Walk from transit
| `WLK_TRN_PNR` | Walk to transit and Drive from transit
| `KNR_TRN_WLK` | Kiss N Ride to Transit (Drop Off) and walk from transit
| `WLK_TRN_KNR` | Walk from transit and kiss n ride from transit (Pick Up)

**Skim Components**:

|Components| Description|
|---|---|
| `IWAIT` | Initial wait time
| `XWAIT` | Transfer wait time
| `WAIT` | Total wait time
| `FARE` | Transit fare cost
| `BOARDS` | Number of boardings
| `WAUX` | Walk auxiliary time
| `DTIME` | Drive access time
| `DDIST` | Drive access distance
| `IVT` | Total in-vehicle time
| `IN_VEHICLE_COST` | In-vehicle cost
| `WACC` | Walk access time
| `WEGR` | Walk egress time
| `CROWD` | Crowding penalty (if enabled)
| `XBOATIME` | Transfer Boarding Time Penalty
| `DTOLL` | Drive access/egress toll price
| `TRIM` |  Used to Trim demand

**Mode-Specific In-Vehicle Times**:

|Mode|Description|
|---|---|
| `IVTCOM` | Commuter rail in-vehicle time
| `IVTEXP` | Express bus in-vehicle time
| `IVTFRY` | Ferry in-vehicle time
| `IVTHVY` | Heavy rail in-vehicle time
| `IVTLOC` | Local bus in-vehicle timee
| `IVTLTR` | Light rail in-vehicle time

### Congested Transit Skims (Optional)

When congested transit assignment is enabled, additional skim components:

* `LINKREL` - Link reliability
* `EAWT` - Extra added wait time
* `CAPPEN` - Capacity penalty

## Active Mode Skims

### Pedestrian Distance Skims
**Files**:

- `skims/ped_distance_maz_maz.txt` - MAZ to MAZ walking distances

### Bicycle Distance Skims
**Files**:

- `skims/bike_distance_maz_maz.txt` - MAZ to MAZ cycling distances
- `skims/bike_distance_taz_taz.txt` - TAZ to TAZ cycling distances

**Format**: CSV with columns:
```
from_zone,to_zone,dist
```

## Time Periods

All skim matrices are generated for these time periods:

- `EA` - Early AM (3:00-6:00)
- `AM` - AM Peak (6:00-10:00)
- `MD` - Midday (10:00-15:00)
- `PM` - PM Peak (15:00-19:00)
- `EV` - Evening (19:00-3:00)

## Usage Notes

- **OMX Files**: Use the OpenMatrix Python library or Emme to read OMX format files
- **Matrix Names**: Follow the pattern `[TimePeriod]_[VehicleType]_[SkimComponent]`
- **Zone Systems**: TAZ-based for regional analysis, MAZ-based for local access analysis
- **Units**: Time in minutes, distance in miles, costs in dollars (2000$)
- **Missing Values**: Large values (>1e19) indicate no connection between zones
