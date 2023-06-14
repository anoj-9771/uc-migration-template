# Databricks notebook source
# MAGIC %md
# MAGIC # data migration for Unity Catalog
# MAGIC The logging part of the migration script requires a single user cluster since writing json files isn't supported in shared cluster. Technically, we could take the data, convert to spark dataframe and then let spark to write the json files but this has been deprioritized since we are using purpose built clusters during migration anyways 

# COMMAND ----------

pip install openpyxl

# COMMAND ----------

import time, math, json, random
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import pyspark.sql.functions as F
import pandas as pd

# COMMAND ----------

# MAGIC %run ./helper_functions

# COMMAND ----------

# MAGIC %run ./test_functions

# COMMAND ----------

# DBTITLE 1,Change the env for each of the environment during runtime. 
env = '' if dbutils.secrets.get('ADS', 'databricks-env') == '~~' else dbutils.secrets.get('ADS', 'databricks-env')
dbs_to_migrate = ['raw', 'cleansed', 'curated', 'datalab']
# dbs_to_migrate = ['datalab', 'cleansed']

# COMMAND ----------

# DBTITLE 1,Update below variables for logging.
catalog = "uc_migration"
schema = "data_migration"
table = f"{env}logs"
datalake_mount = "datalake-raw"
migration_logs_path = f"/dbfs/mnt/{datalake_mount}/uc_migration/uc_migration_logs"

# COMMAND ----------

# DBTITLE 1,Clear the log storage location.
# dbutils.fs.rm(f"/mnt/{datalake_mount}/uc_migration/uc_migration_logs/", True)
# dbutils.fs.mkdirs(f'/mnt/{datalake_mount}/uc_migration/uc_migration_logs/')

# COMMAND ----------

# DBTITLE 1,Iterate through target dbs and clean any data in them.
# for db in dbs_to_migrate:
#     if db in ['raw', 'cleansed', 'curated', 'curated_v2', 'curated_v3', 'semantic']:
#         clean_up_catalog(f'{env}{db}')

# COMMAND ----------

# run only while in development/testing stage
# drop_datalab_tables()


# COMMAND ----------

# DBTITLE 1,Iterate through target dbs and deep clone data to Unity Catalog tables.
# for db in dbs_to_migrate:
#     create_managed_table_parallel(db)

# COMMAND ----------

# DBTITLE 1,Special treatment for non-delta tables in raw layer
df_tables = spark.catalog.listTables('raw')

for table in df_tables:
    try:    
        df_table_data = spark.sql(f'describe extended raw.{table.name}')
        location = df_table_data.filter("col_name = 'Location'").select('data_type').collect()[0]['data_type']    
        provider = df_table_data.filter("col_name = 'Provider'").select('data_type').collect()[0]['data_type'] 
        if (provider) != 'delta':          
            create_external_table(env, 'raw', table.name, location, provider)
    except Exception as e:        
        pass

# COMMAND ----------

# DBTITLE 1,Create database and table to store migration logs table.
spark.sql(f"create database if not exists {catalog}.{schema}")
spark.sql(f'drop table if exists {catalog}.{schema}.{table}')

# COMMAND ----------

# DBTITLE 1,Read the log JSON files into a dataframe.
#code to read the json files 
df = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.includeExistingFiles", "true")
      .option("cloudFiles.inferSchema", "true")
      .option("cloudFiles.schemaLocation", f"/mnt/{datalake_mount}/uc_migration/uc_migration_logs_schema/")
      .load(f"/mnt/{datalake_mount}/uc_migration/uc_migration_logs/")
      .select(['source_table_name', 'target_table_name', 'start_time', 'end_time', 'clone_stats', 'error'])
      .writeStream
      .format("delta")
      .trigger(once=True)
      .option("checkpointLocation", f"/mnt/{datalake_mount}/uc_migration/uc_migration_checkpoint/")
      .table(f'{catalog}.{schema}.{table}')
     )

# COMMAND ----------

# DBTITLE 1,Display the migration logs table filtering on the ones that errored.
#when this cell runs back to back to above cell, the table may not have eventuated (since the above is a streaming action), hence the time.sleep
time.sleep(30)

(spark
 .table(f'{catalog}.{schema}.{table}')
 .filter(F.col('error').isNotNull())
 .display()
)
