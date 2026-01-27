# Databricks notebook source
# Module for opening URLs and retrieving data over HTTP/HTTPS
import urllib.request

# Module providing high-level file operations (copying, moving, etc.)
import shutil

# Module for interacting with the operating system (paths, directories, environment)
import os

# COMMAND ----------

dates_to_process = ['2025-06', '2025-07', '2025-08', '2025-09', '2025-10', '2025-11']

for date in dates_to_process:
  
    # URL of the remote Parquet file to download
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{date}.parquet"

    # Open a network connection and retrieve the file as a binary stream
    response = urllib.request.urlopen(url)

    # Local directory where the downloaded file will be stored
    dir_path = f"/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/{date}"

    # Create the directory structure if it does not already exist
    os.makedirs(dir_path, exist_ok=True)

    # Full local path (directory + filename) for saving the Parquet file
    local_path = f"{dir_path}/yellow_tripdata_{date}.parquet"

    # Write the downloaded data stream to the local file in binary mode
    with open(local_path, 'wb') as f:
        shutil.copyfileobj(response, f)   # Efficiently copy the response stream into the file

# COMMAND ----------

# MAGIC %md
# MAGIC # URL of the remote Parquet file to download
# MAGIC url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet"
# MAGIC
# MAGIC # Open a network connection and retrieve the file as a binary stream
# MAGIC response = urllib.request.urlopen(url)
# MAGIC
# MAGIC # Local directory where the downloaded file will be stored
# MAGIC dir_path = "/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/2025-01"
# MAGIC
# MAGIC # Create the directory structure if it does not already exist
# MAGIC os.makedirs(dir_path, exist_ok=True)
# MAGIC
# MAGIC # Full local path (directory + filename) for saving the Parquet file
# MAGIC local_path = dir_path + "/yellow_tripdata_2025-01.parquet"
# MAGIC
# MAGIC # Write the downloaded data stream to the local file in binary mode
# MAGIC with open(local_path, 'wb') as f:
# MAGIC     shutil.copyfileobj(response, f)   # Efficiently copy the response stream into the file