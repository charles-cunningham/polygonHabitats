# HEADER -----------------------------------------------------------------------
#
# Author: Charles Cunningham
# 
# Script Name: Read in Land Registry National Polygon Service (NPS) Polygons
#
# Script Description: Reads in the NPS data (which comprises several parts which
# cannot all be read in at the same time due to large memory requirements), 
# then create a data frame containing title number area and whether polygon is
# is within or touches England and is outside Wales. This is used to 
# filter title numbers (which can comprise multiple polygons across different
# parts of the NPS polygon data). The filtered title numbers are used to select 
# polygons from different parts, which are then merged together and saved.

### LOAD LIBRARIES -------------------------------------------------------------

# Load packages
library(tidyverse)
library(brickster)
library(sf)

### SET PARAMETERS -------------------------------------------------------------

# Set minimum area cutoff for filtering NPS polygons
minArea <- 4000 # Units = m^2

# Set token here
token <- "*****"

### DATA MANAGEMENT ------------------------------------------------------------

# Set data directory
dataDir <- "/dbfs/mnt/lab/unrestricted/charles.cunningham@defra.gov.uk/LandRegistry/"

# Create directory if it doesn't exist
if (!file.exists(paste0(dataDir, "NPS_data/Raw"))) {
  dir.create(paste0(dataDir, "NPS_data/Raw"), recursive = TRUE)
}

### DOWNLOAD COUNTRY BOUNDARY DATA [MANUAL] ------------------------------------

# We use the Countries (December 2024) Boundaries UK BGC data from ONS
# (Boundary, Super Generalised Clipped)

# Information on data here, and can also download the .geojson file in Download tab:
# https://geoportal.statistics.gov.uk/datasets/6f18dfc308d04372929dea6afa44b2c7_0/explore

# Once downloaded, file is moved to dataDir

### PROCESS COUNTRY DATA -------------------------------------------------------

# Read UK country boundaries
boundaryUK <- paste0(dataDir,
                     "Countries_December_2024_Boundaries_UK_BSC_-2880276037664496196.geojson") %>%
  read_sf(.) %>%
  .["CTRY24NM"] %>%
  st_transform(., crs = 27700)

# Create country boundaries
england <- filter(boundaryUK, CTRY24NM == "England")
wales <- filter(boundaryUK, CTRY24NM == "Wales")

### CONNECT TO LAND REGISTRY DATA ----------------------------------------------

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
                    "LR_POLY_FULL_APR_2025_", # Will need to update this
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

### CREATE MERGED NPS TIBBLE WITH ADDED TIBBLE AREA & COUNTRY COLUMNS ----------

# Create empty tibble
NPS_df <- tibble()

# List NPS shapefile parts
NPS_shp <- list.files(paste0(dataDir, "NPS_data/Raw"),
                      ignore.case = TRUE,
                      pattern = "\\.shp$",
                      full.names = TRUE)

# For every NPS shapefile...
for(i in NPS_shp) {

  # Print update
  print(paste("Processing:", basename(i)))

  # Read in shapefile
  NPS_part <- read_sf(i)

  # Add polygon area column
  NPS_part$POLY_AREA <- st_area(NPS_part)
  
  # Check if polygon is within or touches England (intersects)
  NPS_part$inEngland <- st_intersects(NPS_part, england, sparse = FALSE)[,1]
  
  # Check if polygon is outside Wales (does not intersect)
  NPS_part$outWales <- st_disjoint(NPS_part, wales, sparse = FALSE)[,1]

  # Convert NPS_part to tibble with POLY_ID, TITLE_NO, and POLY_AREA columns
  NPS_part_df <- NPS_part %>%
    as_tibble() %>%
    select(POLY_ID, TITLE_NO, POLY_AREA, inEngland, outWales)
  
  # Add rows to cumulative NPS tibble
  NPS_df <- rbind(NPS_df, NPS_part_df)
}

# Garbage clean
rm(NPS_part, NPS_part_df)
gc()

### CALCULATE AREA BY TITLE NUMBER AND FILTER ----------------------------------

# Create title summaries of area and suitable location
NPS_df <- NPS_df %>%
  # Group polygons to titles, so for every polygon for each title number...
  group_by(TITLE_NO) %>% 
  # Sum polygon area
  mutate(TITLE_AREA = as.numeric(POLY_AREA) %>% sum()) %>% 
  # Create column whether title number intersects with England and not Wales 
  mutate(TITLE_ENGLAND = all(inEngland) & all(outWales)) %>%
  # Ungroup back
  ungroup()

# Create filtered vector of title numbers
titleFilt <- NPS_df %>%
  # Filter above minimum area
  filter(TITLE_AREA >= minArea) %>%
  # Filter location that intersects with England but outside Wales
  filter(TITLE_ENGLAND == TRUE) %>%
  # Select title numbers as vector
  .[["TITLE_NO"]]

### FILTER AND MERGE NPS POLYGONS ----------------------------------------------

# For every NPS shapefile...
for(i in NPS_shp) {
  
  # Print update
  print(paste("Processing:", basename(i)))
  
  # Read in shapefile
  NPS_part <- st_read(i, quiet = TRUE) %>%
    select(!c(INSERT, UPDATE, REC_STATUS )) # Drop columns not needed

  # Filter out polygons which belong to title numbers below minimum area
  NPS_part <- filter(NPS_part, TITLE_NO %in% titleFilt)
  
  # Add polygon area and title number area from NPS_df
  NPS_part <- left_join(NPS_part, NPS_df,
                        by = c("POLY_ID" , "TITLE_NO"))
  
  # If first iteration... 
  if ( grepl("_0.shp", i) ) { 

    # Set the combined dataset to NPS_part
    NPS_complete <- NPS_part
    
    } else { # Else...
     
      # Use rbind to join current NPS_part to complete file
      NPS_complete <- rbind(NPS_complete, NPS_part)
    }
}

### SAVE COMBINED POLYGONS -----------------------------------------------------

# Save in NPS_data directory
saveRDS(NPS_complete, paste0(dataDir, "NPS_data/NPS_polygons.Rds"))

### REMOVE FILES NO LONGER NEEDED ----------------------------------------------

# Remove raw downloaded files; takes up memory unnecessarily
unlink(paste0(dataDir, "NPS_data/Raw"), recursive = TRUE)
gc()
