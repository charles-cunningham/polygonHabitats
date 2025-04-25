# HEADER -----------------------------------------------------------------------
#
# Author: Charles Cunningham
# 
# Script Name: Process land cover data
#
# Script Description: Takes 1 week to run

### LOAD LIBRARIES -------------------------------------------------------------

# Load packages
library(tidyverse)
library(terra)
library(sf)

# Set terra options to speed up
terraOptions(memfrac = 0.9)

### DATA MANAGEMENT ------------------------------------------------------------

# Set data directory
# If working on Databricks: "/dbfs/mnt/lab/unrestricted/charles.cunningham@defra.gov.uk/Pesticides/Data/"
# If working locally: "../Data/"
dataDir <- "/dbfs/mnt/lab/unrestricted/charles.cunningham@defra.gov.uk/LandRegistry/"

# Set land cover map directory
LCM_dir <- paste0(dataDir, "LCM_data/")

# Create directory if it doesn't exist
if (!file.exists(LCM_dir)) {
  dir.create(LCM_dir, recursive = TRUE)
}

### DOWNLOAD LAND COVER DATA [MANUAL] ------------------------------------------

# We use the Land Cover Map 2023 (25m rasterised land parcels, GB).

# Information on data here:
# https://www.ceh.ac.uk/data/ukceh-land-cover-maps

# This must be ordered through the EIDC data request:
# https://catalogue.ceh.ac.uk/documents/ab10ea4a-1788-4d25-a6df-f1aff829dfff

# Once downloaded:
# (i) File is unzipped
# (ii) Contents are moved to LCM_dir
# (iii) Converted to flat structure (i.e. contents of all directories moved to 
# 'LCM_dir' and empty directories removed)

### READ IN NPS AND LAND COVER DATA --------------------------------------

### READ IN LAND COVER DATA

# Read land cover data as spatRast file (first layer is land cover class)
lcm2023 <- paste0(LCM_dir, "gblcm2023_25m.tif") %>%
  rast(., lyrs = 1)

# Rename lcm2015 spatRast layer
names(lcm2023) <- "Identifier"

# Optional: Read spatRast to memory (speeds up later extraction)
lcm2023 <- toMemory(lcm2023)

# Specify the LCM classes (ignore '0's which are marine cells)
classLCM <- c(
  "Deciduous woodland",
  "Coniferous woodland",
  "Arable",
  "Improved grassland",
  "Neutral grassland",
  "Calcareous grassland",
  "Acid grassland",
  "Fen",
  "Heather",
  "Heather grassland",
  "Bog",
  "Inland rock",
  "Saltwater",
  "Freshwater",
  "Supralittoral rock",
  "Supralittoral sediment",
  "Littoral rock",
  "Littoral sediment",
  "Saltmarsh",
  "Urban",
  "Suburban"
)

# Create LCM identifier/class data frame
# N.B. 'Marine' identifier is '0'
LCM_df <- data.frame("Identifier" = 1:length(classLCM),
                     "Class" = classLCM)

### READ IN LAND REGISTRY POLYGON DATA

# Read NPS polygons .Rds
NPS_data <- readRDS(paste0(dataDir,
                           "NPS_data/NPS_polygons.Rds"))

# Create data frame from NPS_data
# N.B Assigning values using this tibble speeds up significantly later
landNPS_data <-  matrix(ncol = length(classLCM),
                        nrow = NROW(NPS_data)) %>%
  data.frame() %>%
  setNames(., classLCM)

# Add total area column (25x25m cell count) to populate
landNPS_data[, "totalArea"] <- NA

# Add in POLY_ID,  TITLE_NO, and area in m^2
landNPS_data[, "POLY_ID"] <- NPS_data$POLY_ID
landNPS_data[, "TITLE_NO"] <- NPS_data$TITLE_NO
landNPS_data[, "area_m2"] <- NPS_data$area

# Find columns that match to LCM classes
colNumsLCM <- names(landNPS_data) %in% classLCM %>%
  which(.)

# EXTRACT COVERAGE (IN 25x25M CELLS) -------------------------------------------

# Create progress bar
progressBar = txtProgressBar(
  min = 0,
  max = NROW(NPS_data),
  initial = 0,
  style = 3
)

  # Start loop iterating through every landNPS_data row
  for (i in 1:NROW(landNPS_data)) { # (Same row numbers as NPS_data)
    
    # Extract all 25x25m cells for each land cover class present for NPS polygon i
    # N.B. This is how rows are connected
    polygonCells <- terra::extract(lcm2023, NPS_data[i,])
    
    # Count number of cells for each class
    # N.B. some classes may not be included as count is 0
    polygonCount <- count(polygonCells, Identifier, name = "Cover")
    
    # Add class names by joining coverage values to LCM data frame
    polygonCount <- left_join(LCM_df, polygonCount, by = "Identifier") %>%
      replace_na(list(Cover = 0)) # Convert 'Cover' NAs to 0
    
    # Add cell coverage for each class to landNPS_data (row i)
    landNPS_data[i, colNumsLCM] <- polygonCount$Cover
    
    # Add total number of cells
    landNPS_data[i, "totalArea"] <- sum(polygonCount$Cover)
    
    # Add main cover
    landNPS_data[i, "MainCover"] <- classLCM[max.col(landNPS_data[i, colNumsLCM],
                                                     ties.method="first")]
    
    # Add cover proportion
    landNPS_data[i, "MainPercent"] <- ( max(landNPS_data[i, colNumsLCM]) /
                                          landNPS_data[i, "totalArea"] ) * 100
    
    # Iterate progress bar
    setTxtProgressBar(progressBar, i)
    
  }

# Close progress bar
close(progressBar)

### SAVE -----------------------------------------------------------------------

# Save
saveRDS(landNPS_data,
        file = paste0(dataDir,
                      "polygon_land_data.Rds"))
