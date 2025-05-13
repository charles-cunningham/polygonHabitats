# HEADER -----------------------------------------------------------------------
#
# Author: Charles Cunningham
# 
# Script Name: Sample title deeds
#
# Script Description:

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
                              "title_agg_cover_data.Rds"))

# Read in LCM classes
classLCM <- paste0(dataDir, "LCM_data/LCM_classes.csv") %>%
  read.csv()

### CREATE SAMPLING TABLE ------------------------------------------------------

# Summarise titleLCM_df
coverSample <- titleLCM_df %>%
  group_by(agg_MainCover) %>%
  summarise(Count = length(agg_MainCover),
            Area = sum(TITLE_AREA))

# Remove NA from the table as this represents marine cells
coverSample <- coverSample %>%
  filter(!is.na(agg_MainCover))

# Estimate appropriate sample size for given population size (N)
sampleSize <- function(N,
                       # Set confidence level (Z score) [Default = 95% interval]
                       Z = 1.96, 
                       # Set sample proportion [Default = 0.5]
                       p = 0.5,
                       # Set margin of error [Default = 5%]
                       MOE = 0.05) {
  
  # Calculate sample size
  n <- ( Z^2 * p * (1 - p) )/(MOE^2)
  
  # Apply finite population correction
  nAdj <- (N * n) / (n + N - 1)

  # Round sample size up
  nAdj <- ceiling(nAdj)
  
  # Return
  return(nAdj)
  
}

# Use formula to calculate estimated necessary sample sizes for survey
coverSample$SampleSize <- sampleSize(N = coverSample$Count,
                                     Z = 1.96, p = 0.5, MOE = 0.05)

# Calculate every
coverSample$SelectN <- round(coverSample$Count / coverSample$SampleSize)

### SAMPLE TITLE DEEDS ---------------------------------------------------------




