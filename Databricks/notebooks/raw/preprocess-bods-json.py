# Databricks notebook source
# MAGIC %md
# MAGIC #Convert single line json file to multiline json file

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

import json
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

# DBTITLE 1,Connect and mount azure blob storage 
# SourceContainer = 'test'
DATA_LAKE_MOUNT_POINT = BlobGetMountPoint(source_container)
print(DATA_LAKE_MOUNT_POINT)

# COMMAND ----------

# DBTITLE 1,Get source file path
source_file_path = "dbfs:{mount}/{sourcefile}".format(mount=DATA_LAKE_MOUNT_POINT, sourcefile = file_object)
print ("source_file_path: " + source_file_path)

source_file_path_file_format = "/dbfs{mount}/{sourcefile}".format(mount=DATA_LAKE_MOUNT_POINT, sourcefile = file_object)
print ("source_file_path_file_format: " + source_file_path_file_format)


# COMMAND ----------

# DBTITLE 1,Convert single line JSON file into multi-line
#this script modifies a JSON file to be a set of JSON records, each separated by a line feed.
#any carriage returns and line feeds in the original file will be escaped
import io
import os
import sys

os.environ['originalFile'] = f'{source_file_path_file_format}'
os.environ['modifiedFile'] = f'{source_file_path_file_format}_modified'

chunks = 0
bufferSize = 4096*5
carryOver = ''
            
with open(f'{source_file_path_file_format}_modified', 'w') as outFile:
    with open(f'{source_file_path_file_format}', 'rb', buffering=bufferSize) as inFile:
        line = inFile.read(bufferSize).decode("utf-8")
        #The first line may start with a [, which we don't want to include in the output
        if line[0] == '[':
            line = line[1:]

        while len(line) > 0:
            chunks += 1
            line = carryOver + line
            carryOver = ''
            #escape line feeds and carriage returns. Add line feed in place of comma between records.
            line = line.replace('\n','\\n').replace('\r','\\r').replace('},{','}\n{')
            #if the file already had line feeds at the end of records then they got escaped by the previous statement, so revert those back
            #not sure if there can be crlf endings in files but let's cater for them. And if a file was processed twice then we’d have double escaped linefeeds
            line = line.replace('}\\n{','}\n{').replace('}\\r\\n{','}\n{').replace('\\\n','\\n').replace('\\\r','\\r')
            #cater for lines ending part way through a },{ set, whilst allowing for embedded characters of the same 
            if line.endswith('}') and not line.endswith('\}'):
                carryOver = '}'
                line = line[:-1]
            elif line.endswith('},') and not line.endswith('\},'):
                carryOver = '},'
                line = line[:-2]
            elif line.endswith('}]') and not line.endswith('\}]'):
                line = line[:-1]
                
            outFile.write(line)
            line = inFile.read(bufferSize).decode("utf-8")
        #write the last curly bracket                                                
        outFile.write(carryOver)                                                
    print(f'{chunks:,} blocks of {bufferSize:,} bytes processed')

# COMMAND ----------

# DBTITLE 1,Rename the modified file to the original name
# MAGIC %sh
# MAGIC #rename the modified file to the original name
# MAGIC cp $modifiedFile $originalFile

# COMMAND ----------

# DBTITLE 1,Delete the temp modified file
# MAGIC %sh
# MAGIC #delete the _modified file
# MAGIC rm -f -- $modifiedFile

# COMMAND ----------

# DBTITLE 1,Remove it - for test
# df = spark.read \
#       .format(file_type) \
#       .option("header", True) \
#       .option("inferSchema", False) \
#       .option("delimiter", "|") \
#       .load(source_file_path) 

# current_record_count = df.count()
# print("Records read from file : " + str(current_record_count))
# display(df.limit(10))

# COMMAND ----------

dbutils.notebook.exit("1")
