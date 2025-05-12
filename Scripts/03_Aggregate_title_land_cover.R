# HEADER -----------------------------------------------------------------------
#
# Author: Charles Cunningham
# 
# Script Name: Aggregate title land cover
#
# Script Description: Use the LCM aggregate classes to group the LCM coverage
# amounts

### LOAD LIBRARIES -------------------------------------------------------------

# Load packages
library(tidyverse)

### DATA MANAGEMENT ------------------------------------------------------------

# Set data directory
# If working on Databricks: "/dbfs/mnt/lab/unrestricted/charles.cunningham@defra.gov.uk/Pesticides/Data/"
# If working locally: "../Data/"
dataDir <- "/dbfs/mnt/lab/unrestricted/charles.cunningham@defra.gov.uk/LandRegistry/"

### READ IN DATA ---------------------------------------------------------------

# Read title - land cover data frame
titleLCM_df <- readRDS(paste0(dataDir,
                              "title_LCM_data.Rds"))
  
# Read in LCM classes
classLCM <- paste0(dataDir, "LCM_data/LCM_classes.csv") %>%
  read.csv()
  
### AGGREGATE LAND COVER CLASSES -----------------------------------------------

# List aggregate classes
aggList <- unique(classLCM$UKCEH.Aggregate.Class..AC.)

# For each aggregate class 'aggClass'
for (aggClass in aggList) {
  
  # Find which rows in classLCM correspond to 'aggClass'
  aggRows <- which(classLCM$UKCEH.Aggregate.Class..AC. %in% aggClass)
  
  # Find which columns in titleLCM_df match the corresponding LCM classes
  aggCols <- which(colnames(titleLCM_df) %in%
                     classLCM$UKCEH.Land.Cover.Class[aggRows])
  
  # If more than one column to aggregate...
  if (length(aggCols) > 1) {
    
    # Sum aggCols for each row, and add to new column named agg_'aggClass'
    titleLCM_df[, paste0("agg_", aggClass)] <- rowSums(titleLCM_df[, aggCols])
    
    # Else if only one...
    } else {
      
      # Assign the single column to agg_'aggClass'
      titleLCM_df[, paste0("agg_", aggClass)] <- titleLCM_df[, aggCols]
  }
}

### SUMMARISE AGGREGATED CLASSES -----------------------------------------------

# Find which columns are aggregate classes
aggColsAll <- which(colnames(titleLCM_df) %in%
                      paste0("agg_", classLCM$UKCEH.Aggregate.Class..AC.))

# Add main cover
titleLCM_df[, "agg_MainCover"] <- aggList[max.col(titleLCM_df[, aggColsAll],
                                                   ties.method = "first")]

# Add max cover proportion
titleLCM_df[, "agg_MainPercent"] <- apply(titleLCM_df[, aggColsAll], 1, max)


### SAVE -----------------------------------------------------------------------

# Save as .Rds
saveRDS(titleLCM_df,
        file = paste0(dataDir,
                      "title_agg_cover_data.Rds"))

# Save as .csv
write.csv(titleLCM_df,
        file = paste0(dataDir,
                      "title_agg_cover_data.csv"))

