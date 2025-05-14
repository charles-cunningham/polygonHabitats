# HEADER -----------------------------------------------------------------------
#
# Author: Charles Cunningham
# 
# Script Name: Sample title deeds
#
# Script Description: Calculate appropriate sampling size then select that 
# number from title data frame using stratified sampling approach

### LOAD LIBRARIES -------------------------------------------------------------

# Load packages
library(tidyverse)

### DATA MANAGEMENT ------------------------------------------------------------

# Set data directory
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
                       MOE = 0.05,
                       # Set estimated response rate [Default = 50%]
                       r = 0.5) {
  
  # Calculate sample size
  n <- ( Z^2 * p * (1 - p) )/(MOE^2)
  
  # Apply finite population correction
  nAdj <- (N * n) / (n + N - 1)
  
  # Apply response rate correction
  # (adjust sample size upwards to reflect not all (of the sample will respond)
  nAdj <- nAdj / r

  # Conservatively round sample size up
  nAdj <- ceiling(nAdj)
  
  # Return
  return(nAdj)
  
}

# Use formula to calculate estimated necessary sample sizes for survey
coverSample$SampleSize <- sampleSize(N = coverSample$Count,
                                     Z = 1.96, p = 0.5, MOE = 0.05, r = 0.5)

# Calculate every nth row to select to reach the target sample size and
# conservatively round down (more selected)
coverSample$SelectN <- floor(coverSample$Count / coverSample$SampleSize)

### SAMPLE TITLE DEEDS ---------------------------------------------------------

# Loop through each row in coverSample
titleSample  <- lapply(1:NROW(coverSample), function(x) {
  
  # Filter titleLCM_df by agg_MainCover of row x
  titleLCM_x <- titleLCM_df %>%
    filter(agg_MainCover == coverSample$agg_MainCover[x])
  
  # Sort in ascending order
  titleLCM_x <- titleLCM_x %>%
    arrange(TITLE_AREA)
  
  # Select every nth row
  titleLCM_x %>%
    filter(row_number() %% coverSample$SelectN[x] == 1)
  
}) %>%
  
  # Join together
  bind_rows()
  
### SAVE -----------------------------------------------------------------------

# Save as .Rds
saveRDS(titleSample,
        file = paste0(dataDir,
                      "title_sample_data.Rds"))

# Save as .csv
write.csv(titleSample,
          file = paste0(dataDir,
                        "title_sample_data.csv"))
