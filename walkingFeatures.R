# Walk feature extraction for training data: demographics:syn10146552 , table:syn10146553

# load all necessary libraries
library(synapseClient)
synapseLogin()
library(plyr)
library(dplyr)
library(ggplot2)
library(doMC)
library(jsonlite)
library(parallel)
library(tidyr)
library(lubridate)
library(stringr)
library(sqldf)
library(plyr)
library(mpowertools) # to install mpowertools run: library(devtools); devtools::install_github("Sage-Bionetworks/mpowertools")

# read in the healthCodes of interest from demographics training table
demo_syntable <- synTableQuery("SELECT * FROM syn10146552")
demo <- demo_syntable@values
healthCodeCol <- c(as.character(demo$healthCode))
healthCodeList <- paste0(sprintf("'%s'", healthCodeCol), collapse = ", ")

# Query table of interest, walking training table
INPUT_WALKING_ACTIVITY_TABLE_SYNID = 'syn10146553'
actv_walking_syntable <- synTableQuery(paste0("SELECT 'recordId', 'healthCode','deviceMotion_walking_outbound.json.items' FROM ", INPUT_WALKING_ACTIVITY_TABLE_SYNID, " WHERE healthCode IN ", "(", healthCodeList, ")"))
actv_walking <- actv_walking_syntable@values
actv_walking$idx <- rownames(actv_walking)

# save for later
selected_records <- actv_walking$recordId

######################
# Download JSON Files
######################
#download outbound walking json files
outbound_Walking_json_files <- synDownloadTableColumns(actv_walking_syntable, "deviceMotion_walking_outbound.json.items")
outbound_Walking_json_files <- data.frame(outbound_Walking_json_fileId =names(outbound_Walking_json_files),
                                          outbound_Walking_json_file = as.character(outbound_Walking_json_files))

outbound_Walking_json_files <- outbound_Walking_json_files %>%
  distinct(outbound_Walking_json_file, .keep_all = TRUE)

actv_walking <- merge(actv_walking,outbound_Walking_json_files, by.x="deviceMotion_walking_outbound.json.items", by.y="outbound_Walking_json_fileId", all=TRUE)

# add walking json files columns
actv_walking <- actv_walking %>% mutate(outbound_Walking_json_file = as.character(outbound_Walking_json_file))

# remove duplicates
actv_walking <- actv_walking %>%
  distinct(outbound_Walking_json_file, .keep_all = TRUE)

#############
# Feature Extraction
##############
if (detectCores() >= 2) {
  runParallel <- TRUE
} else {
  runParallel <- FALSE
}
registerDoMC(detectCores() - 2)

walkFeatures <-
  ddply(
    .data = actv_walking, .variables = colnames(actv_walking),
    .fun = function(row) {
      getWalkFeatures(row$outbound_Walking_json_file)
    },
    .parallel = TRUE
  )


# Only keep the non-redundant data
walkingFeatures <- walkFeatures %>% filter(recordId %in% selected_records)

# Remove unecessary columns
columns_to_drop <- c("deviceMotion_walking_outbound.json.items",
                     "idx",
                     "outbound_Walking_json_file",
                     "error")
cols_to_keep <- !colnames(walkingFeatures) %in% columns_to_drop
walkingFeatures <- walkingFeatures[, cols_to_keep]

# View the data
View(walkingFeature)
