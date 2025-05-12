# HEADER -----------------------------------------------------------------------
#
# Author: Charles Cunningham
# 
# Script Name: Process land cover data
#
# Script Description: For each title number, this script extract the land cover
# using the 2023 land cover map for each of the 21 land cover classes.

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

# Also create a table in .csv format which shows the UKCEH Aggregate classes, 
# UK BAP Broad Habitats, UKCEH Land Cover class and associated integer 
# identifiers (from Table 1, page 5 of the Product Documentation for 
# the 2023 land cover map). Name this LCM_classes.csv and save in LCM_dir.

### READ IN NPS AND LAND COVER DATA --------------------------------------------

### READ IN LAND COVER DATA

# Read land cover data as spatRast file (first layer is land cover class)
lcm2023 <- paste0(LCM_dir, "gblcm2023_25m.tif") %>%
  rast(., lyrs = 1)

# Rename lcm2015 spatRast layer
names(lcm2023) <- "Identifier"

# Optional: Read spatRast to memory (speeds up later extraction)
lcm2023 <- toMemory(lcm2023)

### READ IN LAND COVER CLASSES

# Read in LCM classes
classLCM <- paste0(LCM_dir, "LCM_classes.csv") %>%
  read.csv()

# Create LCM identifier/class data frame
# N.B. 'Marine' identifier is '0'
LCM_df <- data.frame("Identifier" = classLCM$LC.Identifier %>% unique(),
                     "Class" = classLCM$UKCEH.Land.Cover.Class %>% unique() )

### READ IN LAND REGISTRY POLYGON DATA

# Read NPS polygons .Rds
NPS_data <- readRDS(paste0(dataDir,
                           "NPS_data/NPS_polygons.Rds"))

# Create data frame from NPS_data
# N.B Assigning values using this tibble speeds up significantly later. Columns
# are LCM classes and rows are title numbers
titleLCM_df <-  matrix(
  ncol = length(classLCM),
  nrow = unique(NPS_data$TITLE_NO) %>% length()
  ) %>%
  data.frame() %>%
  setNames(., classLCM)

# Add total area column (25x25m cell count) to populate
titleLCM_df[, "totalArea_25m2"] <- NA

# Add in title numbers
titleLCM_df[, "TITLE_NO"] <- unique(NPS_data$TITLE_NO)

# Add in area associated with titles (in m2)
titleLCM_df <- as_tibble(NPS_data) %>% # Convert NPS_data to tibble
  select(TITLE_NO, TITLE_AREA) %>% # Select only TITLE_NO and TITLE_AREA columns
  distinct() %>% # Only keep unique rows (so every row is a title not polygon)
  left_join(titleLCM_df, # Join to new data frame using TITLE_NO
            .,
            by = "TITLE_NO")

# Find columns that match to LCM classes
colNumsLCM <- names(titleLCM_df) %in% classLCM %>%
  which(.)

# EXTRACT COVERAGE (IN 25x25M CELLS) -------------------------------------------

# Create progress bar
progressBar = txtProgressBar(
  min = 0,
  max = NROW(NPS_data),
  initial = 0,
  style = 3
)

# Start loop iterating through every titleLCM_df row (every title number)
for (i in 1:NROW(titleLCM_df)) {
  
  # Select title number polygons
  titlePolygons <- NPS_data %>%
    filter(TITLE_NO == titleLCM_df[i, "TITLE_NO"])
  
  # Extract all 25x25m cells for each land cover class present for title i
  titleCells <- terra::extract(lcm2023, titlePolygons, ID = FALSE)
  
  # Count number of cells for each class
  # N.B. some classes may not be included as count is 0
  titleCount <- count(titleCells, Identifier, name = "Cover")
  
  # Add class names by joining coverage values to LCM data frame
  titleCount <-
    left_join(LCM_df, titleCount, by = "Identifier") %>%
    replace_na(list(Cover = 0)) # Convert 'Cover' NAs to 0
  
  # Add total number of cells
  titleLCM_df[i, "totalArea_25m2"] <- sum(titleCount$Cover)
  
  # Add cell coverage for each class to titleLCM_df (row i)
  titleLCM_df[i, colNumsLCM] <- (titleCount$Cover /
                                   titleLCM_df[i, "totalArea_25m2"]) * 100
  
  # Add main cover
  titleLCM_df[i, "MainCover"] <- classLCM[max.col(titleLCM_df[i, colNumsLCM],
                                                  ties.method = "first")]
  
  # Add max cover proportion
  titleLCM_df[i, "MainPercent"] <- max(titleLCM_df[i, colNumsLCM])
  
  # Iterate progress bar
  setTxtProgressBar(progressBar, i)
}

# Close progress bar
close(progressBar)

### SAVE -----------------------------------------------------------------------

# Save
saveRDS(titleLCM_df,
        file = paste0(dataDir,
                      "title_LCM_data.Rds"))
