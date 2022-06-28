# -*- coding: utf-8 -*-
"""
Created on Mon Nov 15 10:17:49 2021

@author: SnowBe
"""

import exiftool
from pathlib import Path
import re
import pandas as pd
import time
import math

########################################## EDITS THESE VALUES ########################################################################

#Root directory where GoPro video files are stored

root_dir = Path("C:/Users/SnowBe/Downloads/sharknado")

#Directory with the NAV files are stored

nav_dir = Path("C:/Users/SnowBe/Documents/Projects/Apr2021_Phantom_Cruise_PAC2021_035/Data/Final_Exports")

#Project Name

project = "PAC2021-035"

###################################### RUN EXIFTOOL TO EXTRACT GOPRO RECORDINGS EXIF TAGS ############################################

#Empty list to hold GoPro files names, this will be filled using the .iterdir() method on the Path object for the root directory 
#where all the files are stored.

recording_names = []


#Iterate through the contents of the root directory, and append the filenames within this directory to the empty list.
for each in root_dir.iterdir():
    recording_names.append(each)
    
#List comprenhension to convert the Path objects in the recording_names list to strings, since exiftool cannot work with Path objects    
    
recordings = [str(files) for files in recording_names]
    
#Instantiate PyExifTool, get the file created date, as well as it's duration.
#May need to revisits specific tags if using different GoPros. Need file name, CreateDate and duration (seconds)

tags = ["FileName", "CreateDate", "Duration"]
with exiftool.ExifTool(r'C:\Users\SnowBe\exiftool.exe') as et:  #Path to exiftool can either be specified directly, as shown here, or added to PythonPath
    metadata = et.get_tags_batch(tags, recordings)

#Convert the metadata extracted by exfitool to a Pandas dataframe
    
metadata_df = pd.DataFrame(metadata)

#Rename the columns to more user friendly column names

metadata_df = metadata_df.rename(columns = {"SourceFile": "SourcePath", "File:FileName": "FileName", "QuickTime:CreateDate": "CreatedDate", "QuickTime:Duration": "DurationSeconds"})


#By default, exiftool seems to extract date time stamps as YYYY:MM:DD, with colon delimeters rather than slashes. Convert these
#delimeters to slashes, then convert this column type to a datetime dtype.

for row, content in metadata_df["CreatedDate"].items():
    match = re.split(":", content)  #Break the timestamp up based on the presence of a colon
    new_timestamp = str(match[0] + "/" + match[1] + "/" + match[2] + ":" + match[3] + ":" + match[4]) #Concatenate parts into a new string
    metadata_df.at[row, "CreatedDate"] = new_timestamp #Overwrite the created date column with a the newly formated timestamps

#Convert the newly created timestamps to a datetime type.
    
metadata_df["CreatedDate"] = metadata_df["CreatedDate"].astype("datetime64[ns]")

#Convert the duration column to an integer, drop the milliseconds.

metadata_df["DurationSeconds"] = metadata_df["DurationSeconds"].astype("int")

#Save a copy of the CreatedData value as a datetime64 data type, for use later.

metadata_df["StartTime"] = metadata_df["CreatedDate"]

#Remove the file extension from the GoPro filename. The regular expression in this loop searches for a period followed by any 3
#characters, and will work for any video file extension type. 

for row, content in metadata_df["FileName"].items():
    pattern = re.compile("\.(\w{3})$") #Match the file extension
    extension = pattern.split(content)
    name_only = str(extension[0])
    metadata_df.at[row, "FileName"] = name_only


###############################################CREATE SECOND BY SECOND TIME SERIES FOR ALL VIDEOS ###########################

#Empty data frame to fill with new timeseries, using same column names as above.

timesdf = pd.DataFrame(data = None, columns = ['Datetime', 'FileName'])

#For each unique video file, generate a second by second time series index from the "CreatedDate" timestamp, by corresponding
#Length of the "DurationsSeconds" value for that same video file. Generate a time series indexed DataFrame and append it to the 
#empty dataframe.

for i in range(len(metadata_df)):
    videoseconds = pd.Series(pd.date_range(metadata_df.at[i, "CreatedDate"], periods = metadata_df.at[i, "DurationSeconds"], freq = "S"))
    videotimes = pd.DataFrame({"Datetime": videoseconds, "FileName": metadata_df.at[i, "FileName"]}) #Make sure the date_time column has the same column title as the processed NAV data
    timesdf = timesdf.append(videotimes)

################################################READ IN THE PROCESSED NAV DATA ##############################################
    
#Empty list to hold list of files in NAV dir
    
nav_paths = []

#Get file names, append to empty list. It is possible that the Hypack Data processing may generate RData files in this directory
#if the file does not have a .CSV extension, do no append to list of directory names.
#There is likely also a .CSV file in this directory that contains all transect date, with the suffix "_All_Transects". Don't read this in.

all_transects = re.compile("All_Transects")

for each in nav_dir.iterdir():
    if(each.suffix == ".csv" and all_transects.search(each.stem) is None): 
        nav_paths.append(each)

#Read and concatenate the files to a single dataframe in the step below.

navdf = pd.concat((pd.read_csv(f) for f in nav_paths), ignore_index = True)


#Convert the date_time column to a datetime64[ns] dtype

navdf["date_time"] = navdf["date_time"].astype("datetime64[ns]")

############################################## CONVERT TO BIIGLE VIDEO TIMESTAMP DATA STRUCTURE ############################

"""

BIIGLE video timestamp data structure documentation is available at the following link:

    https://biigle.de/manual/tutorials/volumes/file-metadata
    
This script will generate a 1 Hz video metadata file, using the following BIIGLE data structure:
    
filename,taken_at,lng,lat,gps_altitude,distance_to_ground,area
video_1.mp4,2016-12-19 17:09:00,52.112,28.001,-1500.5,30.25,2.6
video_1.mp4,2016-12-19 17:10:00,52.122,28.011,-1505.5,25.0,5.5

filename is the video filename extracted from the EXIF tool, taken_at is the UTC timestamp when the video frame was collected, 
lng and lat and longitude and latitude in decimal degrees, gps_altitude is the vehicles's dept (CTD depth, nominally) and
distance to groung is the vehicles altitude. Any missing values are left empty, and will be filled in with 'null' by BIIGLE


"""
#Extract relevant columns from processed data DF, and created a new DataFrame, with the correct titles

biigle_overlay = pd.DataFrame({"filename": None, "taken_at": navdf["date_time"], "lng": navdf["Beacon_Long_loess"], "lat": navdf["Beacon_Lat_loess"],
                               "gps_altitude": navdf["Depth_m"], "distance_to_ground": navdf["Altitude_m"]})

#Convert the navdf column to a string, to allow empty strings to be inserted. I believe BIIGLE expext litteral empty values 
#that it will then fill with 'null'. Not expecting 'N/A", or 'None' or some other representation of an empty cell. To do this, 
#cast the "distance to ground" column to a string.

biigle_overlay["distance_to_ground"] = biigle_overlay["distance_to_ground"].apply(str)

#Replace any altitude data that reads -9999 with empty values. Also replace and any NA values with empty strings.

for key, value in biigle_overlay["distance_to_ground"].items():
    if(value == "-9999" or value == "NA"):
        biigle_overlay["distance_to_ground"].at[key] = ""
    


###############################################JOIN THE DATA FROM THE NAV FILES TO THE BIIGLE VIDEO TIME STAMPS ###################

#Let the time series df be the left data frame; use pd.merge() with a left join, on the 'date_time' column as a key.

mergeddf = pd.merge(timesdf, biigle_overlay, how="left", on = "Datetime")

#Replace the biigle_overlay file with the video file name on the same row of the merged database

mergeddf["filename"].replace(mergeddf["FileName"], inplace=True)


#Drop any row indices that don't contain a video file name

mergeddf.dropna(subset=["filename"])

############################################ GENERATE THE .CSV FILE ###############################################################

#Function to generate the line structure for the videotimestamps.

def videotimestamp(i, df):
    line = f'{df["filename"].at[i]},{df["taken_at"].at[i]},{df["lng"].at[i]},{df["lat"].at[i]},{df["gps_altitude"].at[i]},{df["distance_to_ground"].at[i]}\n'
    return line
    
#Write one .CSV file per video file. Write to the same directory specified as the root directory earlier in this script.

for each in pd.unique(mergeddf["filename"]): #First level is unique file names
   filtered = mergeddf.loc[mergeddf["filename"] == each] #Rows in the dataframe corresponding to each unique file name.
   filtered = filtered.reset_index() #Make sure the index is reset to start at 0, for the next nested loop.
   with open( f'{root_dir.joinpath(each)}.csv', 'w') as csv:
       for i in range(len(filtered)): #Row numbers of the subsetted rows (row numbers for each unique value of filename)
           entry = videotimestamp(i, filtered)
           csv.write(entry)
