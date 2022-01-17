# Databricks notebook source
# MAGIC %md
# MAGIC #Convert .nc (netCDF4 binary file) BoM file into csv file

# COMMAND ----------

from pyspark.sql.functions import mean, min, max, desc, abs, coalesce, when, expr
from pyspark.sql.functions import date_add, to_utc_timestamp, from_utc_timestamp, datediff
from pyspark.sql.functions import regexp_replace, concat, col, lit, substring, greatest
from pyspark.sql.functions import countDistinct, count

from pyspark.sql import functions as F
from pyspark.sql import SparkSession, SQLContext, Window

from pyspark.sql.types import *

from datetime import datetime

import math

from pyspark.context import SparkContext

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# DBTITLE 1,Install packages
# pip install xarray dask netCDF4 bottleneck

# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Define widgets (parameters) at the start
#Initialize the Entity Object to be passed to the Notebook
dbutils.widgets.text("file_object", "", "01:File-Path")
dbutils.widgets.text("source_param", "", "02:Parameters")
dbutils.widgets.text("source_container", "", "03:Source-Container")


# COMMAND ----------

# DBTITLE 1,Get values from widget
file_object = dbutils.widgets.get("file_object")
source_param = dbutils.widgets.get("source_param")
source_container = dbutils.widgets.get("source_container")

print(file_object)
print(source_param)
print(source_container)

# COMMAND ----------

# import json
# Params = json.loads(source_param)
# print(json.dumps(Params, indent=4, sort_keys=True))

# COMMAND ----------

file_object = file_object.replace("//", "/")
print(file_object)

# COMMAND ----------

# MAGIC %run ../includes/include-all-util

# COMMAND ----------

# DBTITLE 1,Get file type
file_type = file_object.strip('.gz').split('.')[-1]
print (file_type)

# COMMAND ----------

# dbutils.fs.mount(
#   source = "wasbs://test@sablobdafdev01.blob.core.windows.net",
#   mount_point = "/mnt/test",
#   extra_configs = {"fs.azure.account.key.sablobdafdev01.blob.core.windows.net":dbutils.secrets.get(scope = ADS_KV_ACCOUNT_SCOPE, key = "daf-sa-blob-key1")})

# dbutils.fs.mount(
#   source = "wasbs://bom715@sablobdafdev01.blob.core.windows.net",
#   mount_point = "/mnt/blob-bom715",
#   extra_configs = {"fs.azure.sas.bom715.sablobdafdev01.blob.core.windows.net":dbutils.secrets.get(scope = ADS_KV_ACCOUNT_SCOPE, key = "daf-sa-blob-sastoken")})


# dbutils.fs.mount(
# source = "wasbs://cleansed@sadafdev01.blob.core.windows.net",
# mount_point = "/mnt/datalake-cleansed",
# extra_configs = {"fs.azure.account.key.sadafdev01.blob.core.windows.net":dbutils.secrets.get(scope = "ADS_KV_ACCOUNT_SCOPE", key = "daf-sa-lake-key1")})

# dbutils.fs.mount(
# source = "wasbs://raw@sadafdev01.blob.core.windows.net",
# mount_point = "/mnt/datalake-raw",
# extra_configs = {"fs.azure.account.key.sadafdev01.blob.core.windows.net":dbutils.secrets.get(scope = "ADS_KV_ACCOUNT_SCOPE", key = "daf-sa-lake-key1")})

# COMMAND ----------

# DBTITLE 1,Connect and mount azure blob storage 
DATA_LAKE_MOUNT_POINT = BlobGetMountPoint(source_container)

# COMMAND ----------

# DBTITLE 1,Get source file path
source_file_path = "dbfs:{mount}/{sourcefile}".format(mount=DATA_LAKE_MOUNT_POINT, sourcefile = file_object)
print ("source_file_path: " + source_file_path)

source_file_path_file_format = "/dbfs{mount}/{sourcefile}".format(mount=DATA_LAKE_MOUNT_POINT, sourcefile = file_object)
print ("source_file_path_file_format: " + source_file_path_file_format)

# COMMAND ----------

# DBTITLE 1,Extract and convert BoM .nc file 
import xarray as xr
import os

# Set variables names for the input file.nc (netcdf_file_in) and the output file.csv (`csv_file_out`)
netcdf_file_in = source_file_path_file_format
csv_file_out = netcdf_file_in + '.csv'
ds = xr.open_dataset(netcdf_file_in)
df = ds.to_dataframe()
# display(df)
df.to_csv(csv_file_out, sep='|')

# COMMAND ----------

# DBTITLE 1,Remove it - for test
# import netCDF4 as nc
# # fn = '/dbfs/mnt/datalake-raw/test/1_BoMFeed1_GridData-BoM715-IDR311A1.RF3.20201213221000.nc'
# fn = source_file_path_file_format
# ds = nc.Dataset(fn)
# print(ds)
# for dim in ds.dimensions.values():
#     print(dim)
# #Access valiables
# for var in ds.variables.values():
#     print(var)


# import xarray as xr
# import os
# netcdf_file_name = file_object
# # Replace 'local_storage_directory', 'netcdf_dir' and 'csv_dir' by respectively
# # the directory path to Copernicus Marine data, the directory path to netcdf files
# # and the directory path to csv files
# local_storage_directory = '/dbfs/mnt/datalake-raw/test/'
# # netcdf_dir = local_storage_directory + 'netcdf/'
# csv_dir = local_storage_directory
# # Set variables names for the input file.nc (netcdf_file_in) and the output file.csv (`csv_file_out`)
# netcdf_file_in = local_storage_directory + netcdf_file_name
# csv_file_out = netcdf_file_in + '.csv'
# ds = xr.open_dataset(netcdf_file_in)
# df = ds.to_dataframe()
# # display(df)
# df.to_csv(csv_file_out)

# COMMAND ----------

dbutils.notebook.exit("1")
