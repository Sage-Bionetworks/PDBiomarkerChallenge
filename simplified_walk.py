# simplified_walk.py

# Walk feature extraction for training data: demographics:syn10146552 , table: syn10146553

import synapseclient
# use syn = synapseclient.login() if you've already set up your config file
syn = synapseclient.login(email="user@example.net", password="mysecretpassword", rememberMe=True)
import pandas as pd
import json
import numpy as np

# read in the healthCodes of interest from demographics training table
demo_syntable = syn.tableQuery("SELECT * FROM syn10146552")
demo = demo_syntable.asDataFrame()
healthCodeList = ", ".join( repr(i) for i in demo["healthCode"]) 

# Query 'walking training table' for walk data recordIDs and healthCodes. 
INPUT_WALKING_ACTIVITY_TABLE_SYNID = "syn10146553"
actv_walking_syntable = syn.tableQuery(('SELECT "recordId", "healthCode", "deviceMotion_walking_outbound.json.items" FROM {0} WHERE healthCode IN ({1}) AND "deviceMotion_walking_outbound.json.items" is not null LIMIT 500').format(INPUT_WALKING_ACTIVITY_TABLE_SYNID, healthCodeList))
actv_walking = actv_walking_syntable.asDataFrame()
actv_walking['idx'] = actv_walking.index

######################
# Download JSON Files
######################
# bulk download walk JSON files containing sensor data
walk_json_files = syn.downloadTableColumns(actv_walking_syntable, "deviceMotion_walking_outbound.json.items")
items = walk_json_files.items()

# create pandas dataframe of JSON filepaths and filehandleIDs
walk_json_files_temp = pd.DataFrame({"deviceMotion_walking_outbound.json.items": [i[0] for i in items], "outbound_walk_json_file": [i[1] for i in items]})

# convert ints to strings for merging
actv_walking["deviceMotion_walking_outbound.json.items"] = actv_walking["deviceMotion_walking_outbound.json.items"].astype(str)

# merge IDs/healthCodes with JSON data
actv_walk_temp = pd.merge(actv_walking, walk_json_files_temp, on="deviceMotion_walking_outbound.json.items")

####################
# Feature Extraction
####################
## PLACE YOUR FEATURE EXTRACTION CODE HERE ##
## THE FOLLOWING IS AN EXAMPLE CODE PLACEHOLDER ##

# temp example of feature extraction
# get average(mean) x-coordinate UserAcceleration for each file
x_accel = [] # initialize empty list for storing x-acceleration values
avg_items = [] # initialize empty list for storing mean values

# loop through each row in dataframe to read in json file
# grab the userAcceleration x-values and calculate the means
for row in actv_walk_temp["outbound_walk_json_file"]: 
	with open(row) as json_data:
		data = json.load(json_data)
		for item in data:
			x = item.get("userAcceleration").get("x")
			x_accel.append(x)
		avg = np.mean(x_accel)
		avg_items.append(avg)

# create new column in dataframe from list of averaged values
actv_walk_temp["meanXaccel"] = avg_items

# Remove unnecessary columns
actv_walk = actv_walk_temp.drop(["deviceMotion_walking_outbound.json.items", "idx", "outbound_walk_json_file"], axis=1)

#### END FEATURE EXTRACTION SNIPPET EXAMPLE ##
