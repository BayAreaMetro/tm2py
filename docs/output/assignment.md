# Assignment Output Files

Travel Model Two produces detailed assignment results showing how travel demand is loaded onto the transportation network infrastructure.

## Time Pe### Network Performance Summaries

### System Performance Report
**File Pattern**: `output_summaries/network_performance_[TimePeriod].csv`

**Highway Metrics**:
- Total VMT by facility type
- Average speeds by area type
- Congestion duration and extent
- Level of service distribution

**Transit Metrics**: 
- Total transit ridership
- Average load factors
- Service miles and hours
- Passenger-miles per revenue-mile

!!! info "Network Analysis Reference"
    For complete technical documentation on network attributes, analysis tools, and performance metrics, see the **[Network Analysis Reference](network-analysis.md)** page.

### Screenline Analysisels travel across five time periods:

| Code | Name | Time Range | Duration |
|------|------|------------|----------|
| `EA` | Early AM | 3:00 AM - 6:00 AM | 3 hours |
| `AM` | AM Peak | 6:00 AM - 10:00 AM | 4 hours |
| `MD` | Midday | 10:00 AM - 3:00 PM | 5 hours |
| `PM` | PM Peak | 3:00 PM - 7:00 PM | 4 hours |
| `EV` | Evening | 7:00 PM - 3:00 AM | 8 hours |

## Highway Facility Types

Highway links are classified by functional class using the `@ft` attribute:

| Code | Facility Type | Description |
|------|---------------|-------------|
| `1` | Freeway | Interstate highways and freeways |
| `2` | Freeway | Principal arterial - freeway facilities |
| `3` | Arterial | Principal arterial roads |
| `4` | Arterial | Minor arterial roads |
| `5` | Collector | Major collector roads |
| `6` | Collector | Minor collector roads |
| `7` | Local | Local streets and roads |
| `8` | Connector | Highway ramps and connectors |
| `99` | Other | Special facilities and other links |

**Note**: Facility types 1 and 2 are treated as freeways in TM2PY's reliability and delay calculations, while types 3-8 use arterial/road parameters.

## Highway Assignment Outputs

### Highway Network Files

#### MAZ Preload Networks
**File Pattern**: `hwy/maz_preload_[TimePeriod].net`

**Description**: Highway networks with MAZ-level demand loaded
- Contains short local trips between MAZs
- Shows local street utilization
- Includes centroid connector flows

#### Final Assignment Networks
**File Pattern**: `hwy/load[TimePeriod].net`

**Description**: Complete highway networks with all demand loaded

**Key Link Attributes**:
- `@auto_volume` - Total auto volume
- `@auto_time` - Congested travel time
- `@auto_cost` - Generalized cost
- `@v_over_c` - Volume over capacity ratio
- `@cspd` - Congested speed
- `@delay` - Delay compared to free-flow

**Vehicle Type Volumes**:
- `@vol_da` - Drive alone volume
- `@vol_s2` - Shared 2-person volume
- `@vol_s3` - Shared 3+ person volume
- `@vol_sm` - Small truck volume
- `@vol_hv` - Heavy vehicle volume
- `@vol_tot` - Total volume

#### Intermediate Networks
**File Patterns**:
- `hwy/iter[X]loadEA.net` - Assignment iteration networks
- `hwy/avgload[TimePeriod].net` - Multi-iteration averaged networks

### Highway Assignment Summary Files

#### Volume-Delay Function Performance
**File Pattern**: `hwy/assign_summary_[TimePeriod].txt`

**Content**:
- Convergence statistics by iteration
- Total vehicle-miles traveled (VMT)
- Total vehicle-hours traveled (VHT)  
- Average network speed
- Relative gap convergence measures

#### Select Link Analysis
**File Pattern**: `hwy/selectlink_[analysis_name].txt`

**Description**: Traffic volumes using specific highway facilities
- Origin-destination patterns for selected links
- Used for corridor analysis and impact assessment

## Transit Assignment Outputs

### Transit Ridership Files

#### Boardings by Line
**File Pattern**: `trn/boardings_by_line_[TimePeriod].txt`

**Format**: CSV with columns:
```
line_id,line_name,mode,boardings,passenger_miles
```

**Content**:
- Total boardings per transit line
- Passenger-miles by line
- Mode-specific ridership (bus, rail, etc.)

#### Boardings by Segment
**File Pattern**: `trn/boardings_by_segment_[TimePeriod].txt`

**Format**: CSV with columns:
```
line_id,direction,stop_sequence,from_stop,to_stop,boardings,alightings,load
```

**Content**:
- Segment-level ridership between stops
- Directional passenger flows
- Maximum load points identification

### Transit Network Files

#### Assigned Transit Networks
**File Pattern**: `trn/transit_assignment_[TimePeriod].net`

**Link Attributes**:
- `@transit_boardings` - Total boardings
- `@transit_volume` - Passenger volume
- `@capacity_utilization` - Load factor
- `@dwell_time` - Station dwell time

**Transit Line Attributes**:
- `@line_ridership` - Total line ridership
- `@headway` - Service frequency
- `@capacity` - Vehicle capacity

### Advanced Transit Outputs

#### Station-to-Station Flows
**File Pattern**: `trn/station_flows_[TimePeriod].csv`

**Description**: Passenger flows between major transit stations
**Format**:
```
origin_station,destination_station,passengers,avg_time,distance
```

#### Transfer Analysis
**File Pattern**: `trn/transfers_[TimePeriod].csv`

**Content**:
- Transfer volumes at major stations
- Transfer walking times
- Mode-to-mode transfer patterns

#### Transit Path Flows
**File Pattern**: `trn/path_flows_[TimePeriod].omx`

**Description**: OMX matrices showing passenger flows by transit path and access mode

### Congested Transit Outputs
When congested transit assignment is enabled:

#### Capacity Constraint Summary
**File Pattern**: `trn/capacity_summary_[TimePeriod].txt`

**Content**:
- Lines/segments operating at capacity
- Passenger denial statistics  
- Crowding penalties applied

#### Reliability Impacts
**File Pattern**: `trn/reliability_[TimePeriod].csv`

**Content**:
- Schedule adherence impacts
- Passenger delay due to crowding
- Service reliability metrics

## Network Performance Summaries

### System Performance Report
**File Pattern**: `output_summaries/network_performance_[TimePeriod].csv`

**Highway Metrics**:
- Total VMT by facility type
- Average speeds by area type
- Congestion duration and extent
- Level of service distribution

**Transit Metrics**: 
- Total transit ridership
- Average load factors
- Service miles and hours
- Passenger-miles per revenue-mile

### Screenline Analysis
**File Pattern**: `output_summaries/screenlines_[TimePeriod].csv`

**Content**:
- Traffic volumes crossing defined screenlines
- Mode split across major barriers
- Directional flow patterns
- Comparison with count data

### Corridor Performance
**File Pattern**: `output_summaries/corridors_[TimePeriod].csv`

**Content**:
- Travel times along major corridors  
- Volume and speed by corridor segment
- Parallel route usage patterns

## Quality Assurance Outputs

### Assignment Convergence
**File Pattern**: `logs/assignment_convergence_[TimePeriod].log`

**Content**:
- Relative gap by iteration
- Maximum link volume changes
- Convergence criteria achievement

### Network Validation
**File Pattern**: `output_summaries/network_validation.csv`

**Content**:
- Comparison with traffic counts
- Speed validation against floating car data
- Transit ridership vs. operator data
- Geographic distribution accuracy

## File Formats and Usage

### Network Files (.net)
- **Software**: Emme transportation planning software
- **Content**: Node/link topology with attributes
- **Size**: 50-200 MB per time period

### OMX Files (.omx)
- **Software**: OpenMatrix library (Python, R, Java)
- **Content**: Origin-destination matrices
- **Compression**: Built-in HDF5 compression

### Text/CSV Files
- **Software**: Any spreadsheet or statistical package
- **Encoding**: UTF-8
- **Delimiters**: Comma or tab-separated

### Processing Notes
- **Time Periods**: All files generated for 5 time periods (EA, AM, MD, PM, EV)
- **Iterations**: Assignment typically converges in 10-25 iterations
- **File Retention**: Intermediate files may be cleaned up automatically
- **Coordinate System**: California State Plane Zone III
- **Units**: Miles, hours, passengers, vehicles
