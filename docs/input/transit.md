# Transit Network 🚌

The transit network is made up of three core components: transit lines, transit modes, and transit fares. The transit lines were built GTFS feeds from around 2015. The lines are coded with a mode (see below) and serve a series of stop nodes. Transit fares are coded according to Cube's Public Transport program (see below).

Transit trips are assigned between transit access points (TAPs), which represent individual or collections of transit stops for transit access/egress. TAPs are essentially transit specific TAZs that are automatically coded based on the transit network. See the [Level of Service Information](#level-of-service-information).

## Link Attributes

|Field                |Description                                                                             |Data Type|
|---------------------|----------------------------------------------------------------------------------------|---------|
|trip_id              |unique identifier for each trip                                                         |Integer  |
|is_stop_A            |if node A is a transit stop                                                             |boolean  |
|access_A             |                                                                                        |         |
|stop_sequence_A      |stop sequence of node A, if node A is a stop                                            |Integer  |
|shape_pt_sequence_B  |sequence of node A in the route                                                         |Integer  |
|shape_model_node_id_B|model_node_id of node B                                                                 |Integer  |
|NAME                 |name of route with TOD                                                                  |string   |
|agency_id            |transit agency id                                                                       |string   |
|TM2_line_haul_name   |'Commuter rail', 'Express bus', 'Local bus', 'Light rail', 'Ferry service', 'Heavy rail'|string   |
|TM2_mode             |see mode dictionary                                                                     |Integer  |
|faresystem           |faresystem (1-50)                                                                       |Integer  |
|tod                  |time of day (1, 2, 3, 4, 5)                                                             |Integer  |
|HEADWAY              |transit service headway in minute                                                       |Integer  |
|A                    |A of link                                                                               |Integer  |
|B                    |B of link                                                                               |Integer  |
|model_link_id        |model_link_id                                                                           |Integer  |
|shstGeometryId       |the shstGeometryId of the link                                                          |Integer  |

## Transit Modes

The following transit modes are defined based on the [Open511](https://511.org/developers/list/apis/) attributes (but not completely, since they came from the GTFS database predecessor, the Regional Transit Database). These modes represent combinations of operators and technology.

|TM2_operator|agency_name|TM2_mode                                     |TM2_line_haul_name|faresystem   |
|------------|-----------|---------------------------------------------|------------------|-------------|
|30          |AC Transit |84                                           |Express bus       |9            |
|30          |AC Transit |30                                           |Local bus         |9            |
|30          |AC Transit |30                                           |Local bus         |11           |
|5           |ACE Altamont Corridor Express|133                                          |Commuter rail     |1            |
|26          |Bay Area Rapid Transit|120                                          |Heavy rail        |2            |
|3           |Blue & Gold Fleet|103                                          |Ferry service     |13           |
|3           |Blue & Gold Fleet|103                                          |Ferry service     |14           |
|3           |Blue & Gold Fleet|103                                          |Ferry service     |12           |
|17          |Caltrain   |130                                          |Commuter rail     |3            |
|23          |Capitol Corridor|131                                          |Commuter rail     |4            |
|19          |Cloverdale Transit|63                                           |Local bus         |7            |
|17          |Commute.org Shuttle|14                                           |Local bus         |46           |
|15          |County Connection|86                                           |Express bus       |16           |
|15          |County Connection|42                                           |Local bus         |15           |
|15          |County Connection|42                                           |Local bus         |17           |
|10          |Emery Go-Round|12                                           |Local bus         |18           |
|28          |Fairfield and Suisun Transit|92                                           |Express bus       |10           |
|28          |Fairfield and Suisun Transit|52                                           |Local bus         |10           |
|35          |Golden Gate Transit|87                                           |Express bus       |8            |
|20          |Golden Gate Transit|101                                          |Ferry service     |19           |
|20          |Golden Gate Transit|101                                          |Ferry service     |20           |
|35          |Golden Gate Transit|70                                           |Local bus         |8            |
|99          |MVgo Mountain View|16                                           |Local bus         |21           |
|39          |Marin Transit|71                                           |Local bus         |23           |
|39          |Marin Transit|71                                           |Local bus         |24           |
|21          |Petaluma Transit|68                                           |Local bus         |47           |
|13          |Rio Vista Delta Breeze|52                                           |Local bus         |5            |
|6           |SamTrans   |80                                           |Express bus       |6            |
|6           |SamTrans   |24                                           |Local bus         |6            |
|25          |San Francisco Bay Ferry|101                                          |Ferry service     |28           |
|25          |San Francisco Bay Ferry|101                                          |Ferry service     |30           |
|25          |San Francisco Bay Ferry|101                                          |Ferry service     |31           |
|25          |San Francisco Bay Ferry|101                                          |Ferry service     |32           |
|25          |San Francisco Bay Ferry|101                                          |Ferry service     |29           |
|22          |San Francisco Municipal Transportation Agency|110                                          |Light rail        |25           |
|22          |San Francisco Municipal Transportation Agency|20                                           |Local bus         |25           |
|22          |San Francisco Municipal Transportation Agency|21                                           |Local bus         |26           |
|1           |Santa Rosa CityBus|66                                           |Local bus         |33           |
|12          |SolTrans   |91                                           |Express bus       |35           |
|12          |SolTrans   |49                                           |Local bus         |34           |
|12          |SolTrans   |49                                           |Local bus         |35           |
|19          |Sonoma County Transit|63                                           |Local bus         |7            |
|7           |Stanford Marguerite Shuttle|13                                           |Local bus         |22           |
|4           |Tri Delta Transit|95                                           |Express bus       |36           |
|4           |Tri Delta Transit|44                                           |Local bus         |37           |
|4           |Tri Delta Transit|44                                           |Local bus         |36           |
|36          |Union City Transit|38                                           |Local bus         |38           |
|31          |VTA        |81                                           |Express bus       |40           |
|31          |VTA        |81                                           |Express bus       |41           |
|31          |VTA        |111                                          |Light rail        |41           |
|31          |VTA        |28                                           |Local bus         |41           |
|31          |VTA        |28                                           |Local bus         |39           |
|14          |Vacaville City Coach|56                                           |Local bus         |48           |
|38          |Vine (Napa County)|94                                           |Express bus       |43           |
|38          |Vine (Napa County)|60                                           |Local bus         |42           |
|38          |Vine (Napa County)|60                                           |Local bus         |44           |
|37          |WestCat (Western Contra Costa)|90                                           |Express bus       |49           |
|37          |WestCat (Western Contra Costa)|90                                           |Express bus       |50           |
|37          |WestCat (Western Contra Costa)|46                                           |Local bus         |49           |
|24          |Wheels Bus |17                                           |Local bus         |45           |

## Transit Fares

Transit fares are modeled in Cube's Public Transport (PT) program as follows:

1. Each transit mode is assigned a fare system in the Cube factor files
2. Each fare system is defined in the fares.far fare file
3. Each fare system is either FROMTO (fare zone based) or FLAT (initial + transfer in fare)
4. For FROMTO fare systems:
   1. Each stop node is assigned a FAREZONE ID in the master network
   2. The fare is looked up from the fare matrix (fareMatrix.txt), which is a text file with the following columns: MATRIX ZONE_I ZONE_J VALUE
   3. The fare to transfer in from other modes is defined via the FAREFROMFS array (of modes) in the fares.far file
5. For FLAT fare systems:
   1. The initial fare is defined via the IBOARDFARE in the fares.far file
   2. The fare to transfer in from other modes is defined via the FAREFROMFS array (of modes) in the fares.far file
