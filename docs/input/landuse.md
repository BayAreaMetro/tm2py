# Land Use Data 🏘️

!!! info "Creating Land Use Files"
    For detailed instructions on how to prepare land use data files for the base year, see **[Creating Base Year Inputs](../create-base-year-inputs.md#land-use-data)** 🏘️

## Micro Zonal Dataand Use Data �️

## Micro Zonal Data

| *Column Name* | *Description* | *Used by* | *Source* |
|---------------|---------------|-----------|----------|
| *MAZ_ORIGINAL* | Original micro zone number. It's original because these will get renumbered during the model run assuming [the node numbering conventions](network.md#county-node-numbering-system) | |
| *TAZ_ORIGINAL* | Original TAZ number. It's original because these will get renumbered during the model run assuming [the node numbering conventions](network.md#county-node-numbering-system)  | |
| *CountyID* | County ID Number | MAZAutoTripMatrix via [MgraDataManager] | |
| *CountyName* | County name string | | |
| *DistID* | District ID Number (TODO: link district map) | [TourModeChoice.xls] | District system definition |
| *DistName* | District Name (TODO: link district map) | | District system definition |
| *ACRES* | MAZ acres | [createMazDensityFile.py] | Calculated from shapefile |
| *HH* | Total number of households | [MgraDataManager] | |
| *POP* | Total population | [MgraDataManager] | |
| **Employment Industry Categories** |||
| *ag* | Employment in agriculture: [NAICS] 11 | [Accessibilities] |
| *art_rec* | Employment in arts, entertainment and recreation: [NAICS] 71 | [Accessibilities] |
| *const* | Employment in construction: [NAICS] 23 | [Accessibilities] |
| *eat* | Employment in food services and drinking places: [NAICS] 722 | [Accessibilities] |
| *ed_high* | Employment in junior colleges, colleges, universities: [NAICS] 6112, 6113, 6114, 6115 | [Accessibilities] |
| *ed_k12* | Employment in K-12 schools: [NAICS] 6111 | [Accessibilities] |
| *ed_oth* | Employment in other schools, libraries and educational services: [NAICS] 6116, 6117 | [Accessibilities] |
| *fire* | Employment in FIRE (finance, insurance and real estate): NAICS 52, 53 not in leasing | [Accessibilities] |
| *gov* | Employment in government: [NAICS] 92 | [Accessibilities] |
| *health* | Employment in health care: [NAICS] 62 except those in *serv_soc* | [Accessibilities] |
| *hotel* | Employment in hotels and other accomodations: [NAICS] 721 | [Accessibilities] |
| *info* | Employment in information-based services: [NAICS] 51 | [Accessibilities] |
| *lease* | Employment in leasing: [NAICS] 532 | [Accessibilities] |
| *logis* | Employment in logistics/warehousing and distribution: [NAICS] 42, 493 | [Accessibilities] |
| *man_bio* | Employment in biological/drug manufacturing: [NAICS] 325411, 325412, 325313, 325414 | [Accessibilities] |
| *man_hvy* | Employment in heavy manufacturing: [NAICS] 31-33 subset | [Accessibilities] |
| *man_lgt* | Employment in light manufacturing: [NAICS] 31-33 subset | [Accessibilities] |
| *man_tech* | Employment in high-tech manufacturing: [NAICS] 334 | [Accessibilities] |
| *natres* | Employment in mining and resource extraction: [NAICS] 21 | [Accessibilities] |
| *prof* | Employment in professional and technical services: [NAICS] 54 | [Accessibilities] |
| *ret_loc* | Employment in local-serving retail: [NAICS] 444130, 444190, 444210, 444220, 445110, 445120, 445210, 445220, 445230, 445291, 445292, 445299, 445310, 446110, 446120, 446130, 446191, 446199, 447110, 447190, 448110, 448120, 448130, 448140, 448150, 448190, 448210, 448310, 448320, 451110, 451120, 451130, 451140, 451211, 451212, 452910, 452990, 453110, 453220, 453310, 453910, 453920, 453930, 453991, 453998, 454111, 454112, 454113 | [Accessibilities] |
| *ret_reg* | Employment in regional retail: [NAICS] 441110, 441120, 441210, 441222, 441228, 441310, 441320, 442110, 442210, 442291, 442299, 443141, 443142, 444110, 444120, 452111, 452112, 453210, 454210, 454310, 454390 | [Accessibilities] |
| *serv_bus* | Employment in managerial services, administrative and business services: [NAICS] 55,56 | [Accessibilities] |
| *serv_pers* | Employment in personal and other services: [NAICS] 53, 81 | [Accessibilities] |
| *serv_soc* | Employment in social services and childcare: [NAICS] 624 | [Accessibilities] |
| *transp* | Employment in transportation: [NAICS] 48 (most of it), 49 (not in *logis*) | [Accessibilities] |
| *util* | Employment in utilities: [NAICS] 22, 56 | [Accessibilities] |
| *unclass* | Employment not classified | is this used? |
| *emp_total* | Total employment | [Accessibilities] |

## Zonal Data

| *Field* | *Description* | *Used by* |
|---------|---------------|-----------|
| *TAZ_ORIGINAL* | Original TAZ number. It's original because these will get renumbered during the model run assuming [the node numbering conventions](network.md#county-node-numbering-system)  |
| *AVGTTS* | Average travel time savings for transponder ownership model | [TazDataManager] |
| *DIST* | Distance for transponder ownership model | [TazDataManager] |
| *PCTDETOUR* | Percent detour for transponder ownership model | [TazDataManager] |
| *TERMINALTIME* | Terminal time | [TazDataManager] |

[Accessibilities]: https://github.com/BayAreaMetro/travel-model-two/blob/master/model-files/model/Accessibilities.xls
[createMazDensityFile.py]: https://github.com/BayAreaMetro/travel-model-two/blob/master/model-files/scripts/preprocess/createMazDensityFile.py
[MgraDataManager]: https://github.com/BayAreaMetro/travel-model-two/blob/master/core/src/java/com/pb/mtctm2/abm/ctramp/MgraDataManager.java#L47
[NAICS]: https://www.census.gov/eos/www/naics/
[TazDataManager]: https://github.com/BayAreaMetro/travel-model-two/blob/master/core/src/java/com/pb/mtctm2/abm/ctramp/TazDataManager.java#L37
[TourModeChoice.xls]: https://github.com/BayAreaMetro/travel-model-two/blob/master/model-files/model/TourModeChoice.xls  

The persons file contains the following key fields:

| Field | Description | Data Type |
|-------|-------------|-----------|
| HHID | Household ID | Integer |
| PERID | Person ID | Integer |
| AGE | Person Age | Integer |
| SEX | Person Gender | Integer |
| PEMPLOY | Employment Status | Integer |
| PSTUDENT | Student Status | Integer |
