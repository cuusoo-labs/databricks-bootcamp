# Databricks notebook source
# MAGIC %run ./Create_User_DB

# COMMAND ----------

# Get the email address entered by the user on the calling notebook
db_name = spark.conf.get("com.databricks.training.spark.dbName")

# Get user name

#username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
#username_replaced = username.replace(".", "_").replace("@","_")
username = spark.conf.get("com.databricks.training.spark.userName").replace('.', '_')
#username = dbutils.widgets.get("user_name")
base_table_path = f"dbfs:/FileStore/{username}/bootcamp_data/"
local_data_path = f"/dbfs/FileStore/{username}/bootcamp_data/"

# Construct the unique database name
database_name = db_name
print(f"Database Name: {database_name}")

# DBFS Path is
print(f"DBFS Path is: {base_table_path}")

#Local Data path is
print(f"Local Data Path is: {local_data_path}")


# COMMAND ----------

spark.sql(f"DROP DATABASE IF EXISTS {database_name} CASCADE")

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")

# COMMAND ----------

spark.sql(f"USE {database_name}")

# COMMAND ----------

#donwload the file to local file path and move it to DBFS

import subprocess
#
#
## Delete local directories that may be present from a previous run
#
process = subprocess.Popen(['rm', '-f', '-r', local_data_path],
                     stdout=subprocess.PIPE, 
                     stderr=subprocess.PIPE)
stdout, stderr = process.communicate()

stdout.decode('utf-8'), stderr.decode('utf-8')




# COMMAND ----------

# Create local directories used in the workshop

process = subprocess.Popen(['mkdir', '-p', local_data_path],
                     stdout=subprocess.PIPE, 
                     stderr=subprocess.PIPE)
stdout, stderr = process.communicate()

stdout.decode('utf-8'), stderr.decode('utf-8')

# COMMAND ----------

!pip install google-api-python-client google_auth_oauthlib tqdm

# COMMAND ----------

import pickle
import os
import re
import io
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.http import MediaIoBaseDownload
import requests
from tqdm import tqdm

username = spark.conf.get("com.databricks.training.spark.userName").replace('.', '_')

def download_file_from_google_drive(id, destination):
    def get_confirm_token(response):
        for key, value in response.cookies.items():
            if key.startswith('download_warning'):
                return value
        return None

    def save_response_content(response, destination):
        CHUNK_SIZE = 32768
        # get the file size from Content-length response header
        file_size = int(response.headers.get("Content-Length", 0))
        # extract Content disposition from response headers
        content_disposition = response.headers.get("content-disposition")
        # parse filename
        filename = re.findall("filename=\"(.+)\"", content_disposition)[0]
        print("[+] File size:", file_size)
        print("[+] File name:", filename)
        progress = tqdm(response.iter_content(CHUNK_SIZE), f"Downloading {filename}", total=file_size, unit="Byte", unit_scale=True, unit_divisor=1024)
        with open(destination, "wb") as f:
            for chunk in progress:
                if chunk: # filter out keep-alive new chunks
                    f.write(chunk)
                    # update the progress bar
                    progress.update(len(chunk))
        progress.close()

    # base URL for download
    URL = "https://docs.google.com/uc?export=download"
    # init a HTTP session
    session = requests.Session()
    # make a request
    response = session.get(URL, params = {'id': id}, stream=True)
    print("[+] Downloading", response.url)
    # get confirmation token
    token = get_confirm_token(response)
    if token:
        params = {'id': id, 'confirm':token}
        response = session.get(URL, params=params, stream=True)
    # download to disk
    save_response_content(response, destination)  


# COMMAND ----------

### Historical Sensor data

local_file_his = local_data_path + "historical_sensor_data.csv"

download_file_from_google_drive("17ph7fNX8Wua9rAsAnmN87vf_Ikp9DPUJ", local_file_his)


#dbutils.fs.cp(f"file:/databricks/driver/{local_file_his}", f"{base_table_path}historical_sensor_data.csv")

# COMMAND ----------

### Backfill Sensor data

local_file_bf = local_data_path + "backfill_sensor_data_final.csv"

download_file_from_google_drive("1jGE_vm7JVAA0gvXvJx3hheI5Ztoz2qMG", local_file_bf)

#dbutils.fs.cp(f"file:/databricks/driver/{local_file_bf}", f"{base_table_path}backfill_sensor_data_final.csv")

# COMMAND ----------

### Current Labelled Sensor data

local_file_cl = local_data_path + "sensor_readings_current_labeled.csv"

download_file_from_google_drive("1Ed9CHIELEJHJVIMfML8ytQicaRLlKuYh", local_file_cl)

#dbutils.fs.cp(f"file:/databricks/driver/{local_file_cl}", f"{base_table_path}sensor_readings_current_labeled.csv")

# COMMAND ----------

### Plant Data


local_file_pd = local_data_path + "plant_data.csv"

download_file_from_google_drive("1eMB5wy1wa9hh1qgk_pEwvOICn367UdfJ", local_file_pd)

#dbutils.fs.cp(f"file:/databricks/driver/{local_file_pd}", f"{base_table_path}plant_data.csv")

# COMMAND ----------

# dataPath1 = f"{base_table_path}/plant_data.csv"

# df1 = spark.read\
#   .option("header", "true")\
#   .option("delimiter", ",")\
#   .option("inferSchema", "true")\
#   .csv(dataPath1)

# display(df1)

# COMMAND ----------

# df1.createOrReplaceTempView("plant_vw")

# COMMAND ----------

# %sql
# DROP TABLE IF EXISTS dim_plant;

# CREATE TABLE dim_plant 
# USING DELTA
# AS (
#   SELECT * FROM plant_vw
# )

# COMMAND ----------

# Return to the caller, passing the variables needed for file paths and database

response = local_data_path + " " + base_table_path + " " + database_name

dbutils.notebook.exit(response)

# COMMAND ----------


