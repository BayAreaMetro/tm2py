# ACS 2013-2017 Zero-Vehicle Households to MAZs.R
# Get block group level zero vehicle household data and distribute to MAZs


library (tidyverse)
library (tidycensus)

# Locations and data items

baycounties          <- c("01","13","41","55","75","81","85","95","97")
censuskey            <- readLines("M:/Data/Census/API/api-key.txt")

USERPROFILE          <- gsub("\\\\","/", Sys.getenv("USERPROFILE"))
github_location      <- file.path(USERPROFILE, "Documents","GitHub")
tm2py_location       <- file.path(github_location, "tm2py","notebooks")
maz_cross_location   <- file.path(github_location,"travel-model-two","maz_taz","crosswalks")

maz_cross            <- read.csv(file.path(maz_cross_location,
                                           "Census 2010 hhs maz share of blockgroups_ACS2017.csv"),header=T) 


acs_variables <- c(zero_veh_own_          ="B25044_003",          # Zero vehicle owned households
                   zero_veh_rent_         ="B25044_010"           # Zero vehicle rented households
                   ) 
  
# Import block group vehicle data, sum owner and renter 0-veh. hhs
# Ensure joining fields of same class

block_group_0 <- get_acs(geography = "cbg", variables = acs_variables,
                     state = "06", 
                     county = baycounties,
                     year=2017,
                     output="wide",
                     survey = "acs5",
                     key = censuskey) %>% 
  mutate(zero_vehicle_tot=zero_veh_own_E+zero_veh_rent_E) %>% 
  select(GEOID,zero_vehicle_tot) %>% 
  mutate(GEOID=as.numeric(GEOID)) 

# Join block group and maz data. Apportion households to mazs from blockgroups

maz <- block_group_0 %>% 
  left_join(.,maz_cross,by=c("GEOID"="blockgroup"))

maz_summary <- maz %>% 
  mutate(maz_hhs=zero_vehicle_tot*maz_share) %>% 
  group_by(maz) %>% 
  summarize(maz_0veh_hhs=sum(maz_hhs)) %>%
  mutate(rounded_maz_0veh_hhs = round(maz_0veh_hhs,0)) %>% 
  filter(maz!=0) %>% 
  ungroup()

# Output file

write.csv(maz_summary, file.path(tm2py_location,"ACS 2013-2017 MAZ Zero-Vehicle Households.csv"), row.names = FALSE)
