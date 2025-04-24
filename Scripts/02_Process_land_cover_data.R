# HEADER -----------------------------------------------------------------------
#
# Author: Charles Cunningham
# 
# Script Name: Process land cover data
#
# Script Description:

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

LCM_dir <- paste0(dataDir, "LCM_data/")

# Create directory if it doesn't exist
if (!file.exists(LCM_dir)) {
  dir.create(LCM_dir, recursive = TRUE)
}

### DOWNLOAD LAND COVER DATA [MANUAL] ------------------------------------------

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
lcm2023 <- paste0(LCM_dir, "lcm2015gb25m.tif") %>%
  rast(., layers = 1)

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

### READ IN WATERSHED DATA

# Read watershed .Rds
NPS_data <- readRDS(paste0(dataDir,
                                "NPS_data/NPS_polygons.Rds"))

# Create data frame from watershedData
# N.B Assigning values using this tibble speeds up significantly later
NPS_LandData <-  matrix(ncol = length(classLCM),
                             nrow = NROW(NPS_data)) %>%
  data.frame() %>%
  setNames(., classLCM)

# Add total area column (25x25m cell count) to populate
NPS_LandData[, "totalArea"] <- NA

# Add ID column
NPS_LandData[, "ID"] <- 1:NROW(NPS_LandData)

# Find columns that match to LCM classes
colNumsLCM <- names(NPS_LandData) %in% classLCM %>%
  which(.)

# EXTRACT COVERAGE (IN 25x25M CELLS) -------------------------------------------

# Create progress bar
progressBar = txtProgressBar(
  min = 0,
  max = NROW(NPS_data),
  initial = 0,
  style = 3
)

# Start loop iterating through every watershedLandData row
for (i in 1:NROW(NPS_data)) { # (Same row numbers as watershedData)
  
  # Extract all 25x25m cells for each land cover class present for watershed i
  # N.B. This is how rows are connected
  NPS_Cells <- terra::extract(lcm2023, NPS_data[i,])
  
  # Count number of cells for each class
  # N.B. some classes may not be included as count is 0
  watershedCount <- count(watershedCells, Identifier, name = "Cover")
  
  # Add class names by joining coverage values to LCM data frame
  watershedCount <- left_join(LCM_df, watershedCount, by = "Identifier") %>%
    replace_na(list(Cover = 0)) # Convert 'Cover' NAs to 0
  
  # Add coverage for each class to watershedLandData (row i)
  watershedLandData[i, colNumsLCM] <- watershedCount$Cover
  
  # Add total cover
  watershedLandData[i, "totalArea"] <- sum(watershedCount$Cover)
  
  # Iterate progress bar
  setTxtProgressBar(progressBar, i)
  
}

# Close progress bar
close(progressBar)

# Save
saveRDS(watershedLandData,
        file = paste0(dataDir,
                      "Processed/Watersheds/Watershed_land_data.Rds"))
