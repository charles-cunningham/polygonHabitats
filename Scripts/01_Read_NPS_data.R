# HEADER -----------------------------------------------------------------------
#
# Author: Charles Cunningham
# 
# Script Name: Read in Land Registry National Polygon Service (NPS) Polygons
#
# Script Description: Reads in the NPS data, then filters above a minimum area,
# and binds them together into a single spatial object

### LOAD LIBRARIES -------------------------------------------------------------

# Load packages
library(tidyverse)
library(brickster)
library(sf)

### SET PARAMETERS -------------------------------------------------------------

# Set minimum area cutoff for filtering NPS polygons
minArea <- 10000 # Units = m^2

# Set token here
token <- "*****"

### DATA MANAGEMENT ------------------------------------------------------------

# Set data directory
dataDir <- "/dbfs/mnt/lab/unrestricted/charles.cunningham@defra.gov.uk/LandRegistry/"

# Create directory if it doesn't exist
if (!file.exists(paste0(dataDir, "NPS_data/Raw"))) {
  dir.create(paste0(dataDir, "NPS_data/Raw"), recursive = TRUE)
}

### CONNECT TO LAND REGISTRY DATA -------------------------------------------------------

# Set system variables
Sys.setenv(DATABRICKS_TOKEN = token)
Sys.setenv(DATABRICKS_HOST = "https://adb-2353967604677522.2.azuredatabricks.net/")
Sys.setenv(DATABRICKS_WSID = "2353967604677522")

# Set up workspace
open_workspace(host = db_host(), token = db_token(), name = NULL)

# Define directory to download data from 
NPS_directory <- "/Volumes/prd_dash_bronze/data_gov_hm_land_registry_restricted/nps_national_polygon/format_SHP_nps_national_polygon/LATEST_nps_national_polygon/"

### DOWNLOAD LAND REGISTRY DATA ------------------------------------------------

# Create list of file extensions that compose a standard shapefile
shapefileExt <- c(".dbf",
                  ".prj",
                  ".shp",
                  ".shx")

# Generate complete list of files to download
# N.B. The NPS data is saved as 10 different shapefiles (due to large size) with 
# '_0' to '_9' suffixes
NPS_files <- paste0(NPS_directory,
                    "LR_POLY_FULL_MAY_2024_",
                    rep(0:9, each = length(shapefileExt)),
                    shapefileExt)

# Loop through all NPS_files to download
lapply(NPS_files, function(x) {
  
  # Read data to assigned file path
  db_volume_read(path = x,
                 paste0(dataDir,
                        "NPS_data/Raw/",
                        basename(x))
                 )
})

### FILTER POLYGONS ABOVE CERTAIN AREA -----------------------------------------

# For every NPS shapefile...
for(i in 0:9) {
  
  # Print update
  print(paste("Processing part", i))
  
  # Read in shapefile
  NPS_part <- paste0(
    "/dbfs/mnt/lab/unrestricted/charles.cunningham@defra.gov.uk/LandRegistry/NPS_data/Raw/LR_POLY_FULL_MAY_2024_",
    i,
    ".shp") %>%
    st_read(., quiet = TRUE)

  # Add polygon area column
  NPS_part$area <- st_area(NPS_part) 

  # Filter out small polygons below set minimum area
  NPS_part <- NPS_part %>%
    filter(as.numeric(area) >= minArea)
  
  # If first iteration, set the combined dataset to NPS_part
  if (i == 0) { 
    
    NPS_complete <- NPS_part 
     
    # Else... 
    } else {
     
      # Use rbind to join current NPS_part to complete file
      NPS_complete <- rbind(NPS_complete, NPS_part)
    }
}

### SAVE COMBINED POLYGONS -----------------------------------------------------

# Save in NPS_data directory
saveRDS(NPS_complete, paste0(dataDir, "NPS_data/NPS_polygons.Rds"))

### REMOVE FILES NO LONGER NEEDED ----------------------------------------------

# Remove raw donloaded files; takes up memory unnecessarily
unlink(paste0(dataDir, "NPS_data/Raw"), recursive = TRUE)
gc()
