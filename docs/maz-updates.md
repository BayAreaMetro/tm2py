# How the MAZ Layer Was Updated

This page documents the process and methodology used to update the Micro Analysis Zone (MAZ) layer for Travel Model Two.

## Overview

The MAZ (Micro Analysis Zone) layer represents the finest level of geographic detail in Travel Model Two, with approximately 40,000 zones covering the 9-county Bay Area. This layer was significantly updated and refined from previous versions. 

## What are MAZs?

MAZs are small geographic units that provide detailed spatial representation for:

- **Land Use Data** - Employment and household locations
- **Trip Generation** - Origin and destination points for travel
- **Transit Access** - Walk access to transit stops and stations
- **Mode Choice** - Local accessibility measures

## Axioms

We maintained the following axioms when we updated the MAZ layer

- MAZs and TAZs are _defined_ as a union of 2020 vintage Census 2010 Blocks
- There is one 2020 vintage Census 2010 Block Group (and therefore Tract and County) per MAZ
- There is one 2020 vintage Census 2010 Tracr (and therefore County) per TAZ
- 2020 vintage Census 2010 Blocks with zero land area (ALAND10) are **not** assigned a MAZ or TAZ
- 2020 vintage Census 2010 Blocks with nonzero land area area assigned a mAZ and TAZ

### Axiom Exceptions

There are a handful of exceptions to the above axioms

- Blocks "06 075 980401 100[1,2,3]" (maz 16084, taz 287) are the Farallon Islands.  It's a standalone maz but the
  taz spans tracts because it's not worth it's own taz.
- Block "06 081 608002 2004" (maz 112279, taz 100178) spans a block group boundary but not doing so would split up
  and island with two blocks.
- Blocks "06 075 017902 10[05,80]" (maz 16495, taz 312) is a tiny sliver that's barely land so not worth
  making a new maz, so that maz includes a second tract (mostly water)
- Blocks "06 041 104300 10[17,18,19]" (maz 810745, taz 800095) spans a block group/tract boundary but the're a
  tiny bit on the water's edge and moving them would separate them from the rest of the maz/taz
- Blocks "06 041 122000 100[0,1,2]" (maz 813480, taz 800203) are a tract that is inside another tract so keeping
  as is so as not to create a donut hole maz
- Block "06 013 301000 3000" (maz 410304, taz 400507) is a block that Census 2010 claims has no land area ("Webb Tract")
  but appears to be a delta island so it's an exception to the zero-land/non-zero water blocks having a maz/taz
- Blocks with very small percent of land were not assigned a MAZ and TAZ

## Update Process and History

### Version 2.5
* Manual fixes incorporated into [blocks_mazs_tazs_2.5](https://github.com/BayAreaMetro/tm2py-utils/blob/main/tm2py_utils/inputs/maz_taz/blocks_mazs_tazs_2.5.csv). Fixes were to addresses MAZs that were nested within another MAZ. See [excel](https://mtcdrive.box.com/s/rg8k2rcs39y45l3do82gmdsyk8jrslm2) for more details about the changes made.

### Version 2.4
* Updated [maz_taz_checker.py](https://github.com/BayAreaMetro/tm2py-utils/blob/main/tm2py_utils/inputs/maz_taz/maz_taz_checker.py) to run with GeoPandas, move iteration within the python script, and passed the crosswalk file and version number as arguments
* Using 2020 vintage of Census 2010 blocks
* Manual fixes incorporated into [blocks_mazs_tazs_2.3.csv](https://github.com/BayAreaMetro/tm2py-utils/blob/main/tm2py_utils/inputs/maz_taz/blocks_mazs_tazs_2.3.csv). Fixes were to addressed blocks with very small percent land that was previously coded as zero land area

### Version 2.2

* Manual fixes incorporated into [blocks_mazs_tazs_v2.1.1.csv](https://github.com/BayAreaMetro/tm2py-utils/blob/main/tm2py_utils/inputs/maz_taz/blocks_mazs_tazs_v2.1.1.csv)
* Updated [maz_taz_checker.py](https://github.com/BayAreaMetro/tm2py-utils/blob/main/tm2py_utils/inputs/maz_taz/maz_taz_checker.py) to fix problems (moving blocks to neighboring mazs,
  splitting tazs -- see the script for more detail)
* [maz_taz_checker.bat](https://github.com/BayAreaMetro/tm2py-utils/blob/main/tm2py_utils/inputs/maz_taz/maz_taz_checker.bat) is used to iterate, producing the v2.1.X files
* The final v2.1.X file is the v2.2 file
* [(Internal) Asana Task](https://app.asana.com/0/610230255351992/626340099942965/f)
* Work performed in M:\Data\GIS layers\TM2_maz_taz_v2.2
* See [web map](https://arcg.is/1n9XfL) with Version 2.2 and 1.0 of TM2 MAZs and TAZs, along with Census 2010 places, tracts, block groups.

### Version 2.1
* Started development of [maz_taz_checker.py](https://github.com/BayAreaMetro/tm2py-utils/blob/main/tm2py_utils/inputs/maz_taz/maz_taz_checker.py) to automatically generate shapefiles
* Using 2010 vintage of Census 2010 blocks because the 2017 vintage had problems
* All blocks with nonzero land area (ALAND10) are assigned to a maz/taz
* All blocks with zero land area (ALAND10) are not assigned a maz/taz
* Work performed in M:\Data\GIS layers\TM2_maz_taz_v2.1

### Version 2.0
* Required to fix some problems where some MAZs were mistakenly part of TAZs for which the other MAZs were far away
* We also decided not to break up Census 2010 blocks into pieces, so we defined MAZs (and TAZs) as a union of Census 2010 blocks (2017 vintage) and that
  TAZs should nest within counties
* [(Internal) Asana Task](https://app.asana.com/0/610230255351992/578257153057158/f)
* Work performed in M:\Data\GIS layers\TM2_maz_taz_v2.0

### Version 1.0
* [Map](http://www.arcgis.com/apps/OnePane/basicviewer/index.html?appid=4ca5bf25e2ed46ebb7c25796b29c33d1)
* [Revised Travel Analysis and Micro-Analysis Zone Boundaries](https://mtcdrive.box.com/travel-model-two-revised-space) - May 22, 2014
* [Initial Draft Travel Analysis and Micro-Analysis Zone Boundaries](https://mtcdrive.box.com/travel-model-two-first-space) - Jan 17, 2013

## Future Steps

The MAZ layers were revised based on the original drawn boundaries in 2014. If we are to move the MAZs forward to future Census Block boundaries, we should draw boundaries based on population and employment similar to the ArcGIS function, [Build Balanced Zones](https://pro.arcgis.com/en/pro-app/3.4/tool-reference/spatial-statistics/learnmore-buildbalancedzones.htm). 
