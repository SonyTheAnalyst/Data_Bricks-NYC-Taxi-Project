# Databricks notebook source
import urllib.request
import shutil
import os


url= "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

#-----OPEN A CONNECTION TO THE REMOTE URL AND FETCH THE PARQUET FILE AS A STREM
response= urllib.request.urlopen(url)

#-----CREATE THE DESTINATION DIRECTORY FOR STORING THE DOWNLOADED PARQUET FILE
dir_path = "/Volumes/nyctaxi/00_landing/data_sources/lookup"
os.makedirs(dir_path, exist_ok=True)

#-----DEFINE THE FULL LOCAL PATH (INCLUDING FILENAME) WHERE THE FILE WILL BE SAVED
local_path = "/Volumes/nyctaxi/00_landing/data_sources/lookup/taxi_zone_lookup.csv"

#-----WRITE THE CONTENTS OF THE RESPONSE STREAM TO SPECIFIED LOCAL FILE PATH
with open(local_path, 'wb') as f:
  shutil.copyfileobj(response, f)