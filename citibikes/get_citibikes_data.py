""""
Get all the NYC data from the citibike project.
This data will be used for Tableau visualisations.

Data description
Several zip stored on the website and containing csv files.
One line represents a trip.
One line includes:

    Trip Duration (seconds)
    Start Time and Date
    Stop Time and Date
    Start Station Name
    End Station Name
    Station ID
    Station Lat/Long
    Bike ID
    User Type (Customer = 24-hour pass or 3-day pass user; Subscriber = Annual Member)
    Gender (Zero=unknown; 1=male; 2=female)
    Year of Birth
"""

#Web scrapping libs
import urllib.request, urllib.parse, urllib.error
import requests
from bs4 import BeautifulSoup
import webbrowser
from time import sleep
#Filtering libs
import re
import fnmatch
#File libs
import shutil
import os
import csv
from zipfile import ZipFile
#Data analyses libs
import pandas as pd

import citibikes.dir_names

print("PROCESS BEGINS")

url ="https://s3.amazonaws.com/tripdata/"
downloads_dir= citibikes.dir_names.DOWNLOADS_DIR #To replace with the local downloads dir path
data_src_dir= citibikes.dir_names.DATA_DIR  #A refactorer data_dir #To replace with the local dir path
zip_dir=data_src_dir+"zip/"
csv_dir=data_src_dir+"csv/"
final_filepath=csv_dir+"citibike_all.csv"
"""
data_files=[]
response= requests.get(url)

if response.status_code==200: #Transformer en erreur
    soup=BeautifulSoup(response.text, "html.parser")
else:
    print("The URL is not responding")
    exit(0)


#Get all the data (excluding those starting with JC).
data_files=soup.find_all("key")
for element in data_files:
    file_name=element.text
    if re.search("^(?!JC).*zip$", file_name):
        print("DOWNLOADING ", file_name)
        webbrowser.open_new(url + file_name)
        sleep(120)


#Move from Download folder to data folder
for item in os.listdir(downloads_dir):
    print("MOVE ", item)
    shutil.move(downloads_dir + item, zip_dir)
  """
#Unzip the files and put it all in one file
#Create the file
df=pd.DataFrame(columns=["trip_duration","start_time","stop_time",
                         "start_station_id","start_station_name","start_station_latitude","start_station_longitude",
                         "end_station_id","end_station_name","end_station_latitude","end_station_longitude",
                         "bike_id","user_type","birth_year","gender",
                         "source_archive"])


df.to_csv(final_filepath, index=False, quoting=csv.QUOTE_NONNUMERIC)

for element in os.listdir(zip_dir)[:2]:
    print("UNZIP AND WRITE ",element)
    if element.endswith(".zip"):

        with ZipFile(zip_dir+element) as zipped_files:
            data_file= zipped_files.open(fnmatch.filter( zipped_files.namelist(), "[0-9]*.csv")[0])


        df_el=pd.read_csv(data_file)
        df_el["source_archive"] = element
        df_el.head().to_csv(final_filepath, index=False,header=False, mode="a", quoting=csv.QUOTE_ALL)


print("PROCESS ENDS")
