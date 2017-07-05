# Walk feature extraction for training data: demographics:syn10146552 , table:syn10146553
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
library(parsedate)
library(mpowertools) # to install mpowertools run: library(devtools); devtools::install_github("Sage-Bionetworks/mpowertools")

# read in the healthCodes of interest from demographics training table
demo_syntable <- synTableQuery("SELECT * FROM syn10146552") 
demo <- demo_syntable@values
healthCodeCol <- c(as.character(demo$healthCode))
healthCodeList <- paste0(sprintf("'%s'", healthCodeCol), collapse = ", ")

# Query table of interest, walking testing table
# for training features: change synId here to match the training table synId listed at top
INPUT_REST_ACTIVITY_TABLE_SYNID = "syn10146553"
actv_rest_syntable <- synTableQuery(paste0("SELECT 'recordId', 'healthCode', 'deviceMotion_walking_rest.json.items' FROM ", INPUT_REST_ACTIVITY_TABLE_SYNID, " WHERE healthCode IN ", "(", healthCodeList, ")"))
actv_rest <- actv_rest_syntable@values
actv_rest$idx <- rownames(actv_rest)

# saving for later
actvRest_selected_rows <- actv_rest$idx

######################
# Download JSON Files
######################
# rest JSON files
rest_json_files <- synDownloadTableColumns(actv_rest_syntable, "deviceMotion_walking_rest.json.items")
rest_json_files <- data.frame(rest_json_fileId =names(rest_json_files),
                              rest_json_file = as.character(rest_json_files))
actv_rest <- merge(actv_rest,rest_json_files, by.x="deviceMotion_walking_rest.json.items", by.y="rest_json_fileId", all=T)

# only use the selected non-redundant data
actv_rest <- actv_rest %>% filter(idx %in% actvRest_selected_rows)

#############
# Feature Extraction
##############
if (detectCores() >= 2) {
  runParallel <- TRUE
} else {
  runParallel <- FALSE
}
registerDoMC(detectCores() - 2)

# convert to character to be able to read in rest_json_file
actv_rest[] <- lapply(actv_rest, as.character)

#extract Rest features
restFeaturesDf <- 
  ddply(
    .data=actv_rest,
    .variables = colnames(actv_rest), 
    .fun = function(row) { 
      getRestFeatures(row$rest_json_file) 
    }, 
    .parallel = TRUE )

# remove unnecessary columns
columns_to_drop <- c("deviceMotion_walking_rest.json.items",
                     "idx",
                     "rest_json_file",
                     "error")
cols_to_keep <- !colnames(restFeaturesDf) %in% columns_to_drop
restFeatures <- restFeaturesDf[, cols_to_keep]

# view the data
View(restFeatures)
