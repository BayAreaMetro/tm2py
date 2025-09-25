# Skim Outputs

Travel Model Two generates comprehensive level-of-service (LOS) matrices called "skims" that represent impedances and costs between zones for different transportation modes and time periods.

## Highway Skims

### Highway Skim Matrices (OMX Format)
**File Pattern**: `skims/HWYSKM[TimePeriod]_taz.omx`

Contains multiple matrices with highway travel impedances between Traffic Analysis Zones (TAZs):

- **Travel Time** - Peak and off-peak periods
- **Distance** - Network distance in miles
- **Bridge Tolls** - Toll costs for bridge crossings
- **Operating Costs** - Vehicle operating costs
- **Generalized Cost** - Combined time and cost impedance

**Vehicle Types Included**:
- `DA` - Drive Alone
- `S2` - Shared 2-person
- `S3` - Shared 3+ person
- `DATOLL` - Drive Alone with toll facilities
- `S2TOLL` - Shared 2-person with toll facilities  
- `S3TOLL` - Shared 3+ person with toll facilities

### MAZ-to-MAZ Highway Skims
**File Pattern**: `skims/maz_to_maz_skims_[period].csv`

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
**File Pattern**: `skims/transit_skims_[TimePeriod]_[TransitClass].omx`

**Transit Classes**:
- `wlk_loc` - Walk to local transit
- `wlk_lrf` - Walk to light rail/ferry
- `wlk_exp` - Walk to express transit
- `wlk_hvy` - Walk to heavy rail
- `wlk_com` - Walk to commuter rail
- `drv_loc` - Drive to local transit
- `drv_lrf` - Drive to light rail/ferry
- `drv_exp` - Drive to express transit
- `drv_hvy` - Drive to heavy rail
- `drv_com` - Drive to commuter rail

**Skim Components**:
- `IWAIT` - Initial wait time
- `XWAIT` - Transfer wait time
- `FARE` - Transit fare cost
- `BOARDS` - Number of boardings
- `WAUX` - Walk auxiliary time
- `DTIME` - Drive access time
- `DDIST` - Drive access distance
- `WACC` - Walk access time
- `WEGR` - Walk egress time
- `IVT` - Total in-vehicle time
- `CROWD` - Crowding penalty (if enabled)

**Mode-Specific In-Vehicle Times**:
- `IVTBUS` - Bus in-vehicle time
- `IVTLRT` - Light rail in-vehicle time
- `IVTFRY` - Ferry in-vehicle time
- `IVTHSR` - Heavy rail in-vehicle time
- `IVTCMR` - Commuter rail in-vehicle time

### Congested Transit Skims (Optional)
When congested transit assignment is enabled, additional skim components:
- `LINKREL` - Link reliability
- `EAWT` - Extra added wait time
- `CAPPEN` - Capacity penalty

## Active Mode Skims

### Pedestrian Distance Skims
**Files**:
- `skims/ped_distance_maz_maz.txt` - MAZ to MAZ walking distances
- `skims/ped_distance_maz_tap.txt` - MAZ to Transit Access Point distances
- `skims/ped_distance_tap_tap.txt` - Transit Access Point to Transit Access Point

### Bicycle Distance Skims
**Files**:
- `skims/bike_distance_maz_maz.txt` - MAZ to MAZ cycling distances
- `skims/bike_distance_maz_tap.txt` - MAZ to Transit Access Point cycling distances
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
- **Matrix Names**: Follow the pattern `[SkimType]_[VehicleType]_[TimePeriod]`
- **Zone Systems**: TAZ-based for regional analysis, MAZ-based for local access analysis
- **Units**: Time in minutes, distance in miles, costs in dollars (2000$)
- **Missing Values**: Large values (>1e19) indicate no connection between zones
