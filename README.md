# BIIGLE-Video-Metadata-Generator
Python script to generate video metadata to be ingested in BIIGLE.

This script is intended to generate video metadata for ingestion into BIIGLE, as described here:

https://biigle.de/manual/tutorials/volumes/file-metadata

The script will generate a 1 Hz video metadata file, using the following BIIGLE data structure:
    
filename,taken_at,lng,lat,gps_altitude,distance_to_ground,area
video_1.mp4,2016-12-19 17:09:00,52.112,28.001,-1500.5,30.25,2.6
video_1.mp4,2016-12-19 17:10:00,52.122,28.011,-1505.5,25.0,5.5

Where:
filename is the video filename extracted from the EXIF tool, taken_at is the UTC timestamp when the video frame was collected, 
lng and lat and longitude and latitude in decimal degrees, gps_altitude is the vehicles's dept (CTD depth, nominally) and
distance to groung is the vehicles altitude. Any missing values are left empty, and will be filled in with 'null' by BIIGLE

The script required the use of the exiftool library for python, which is documented here: https://pypi.org/project/PyExifTool/. Exiftool extra's the start timestamp (UTC time) from collected video files, as well the filename and duration. A pandas dataframe is then generated usign this data, and the timestamps are matched against the processed data from the NDST data processing scripts written in R, which are available in a seperate repository on this GitHub.

This script requires the use of the processed data outputs, and the collected NAV data must be processed using R scripts first, before trying to execute this script.
