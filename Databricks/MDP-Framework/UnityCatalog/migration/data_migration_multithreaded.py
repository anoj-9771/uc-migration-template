# Databricks notebook source
# MAGIC %md
# MAGIC The logging part of the migration script requires a single user cluster since writing json files isn't supported in shared cluster. Technically, we could take the data, convert to spark dataframe and then let spark to write the json files but this has been deprioritized since we are using purpose built clusters during migration anyways 

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

excel_path = '/dbfs/FileStore/uc/uc_scope.xlsx'
excel_path_blob = 'abfss://ucmigration@sadafprod03.dfs.core.windows.net/files/uc_scope.xlsx'
dbutils.fs.cp(excel_path_blob, excel_path)

# COMMAND ----------

dbutils.fs.ls(excel_path)

# COMMAND ----------

# DBTITLE 1,Change the env for each of the environment during runtime. 
env = "dev_"
p_df = read_run_sheet(excel_path, f'{env}schemas')
dbs_to_migrate = p_df[p_df['in_scope'] == 'Y']['database_name'].tolist()
dbs_to_migrate.remove('curated_v2')
dbs_to_migrate.remove('curated_v3')
#add curated_v2 and curated_v3 resources as necessary
# dbs_to_migrate.remove('cleansed')
# dbs_to_migrate = ["raw"]

# COMMAND ----------

# DBTITLE 1,Update below variables for logging.
catalog = "uc_migration"
schema = "data_migration"
table = f"{env}logs"
datalake_mount = "datalake-raw"
migration_logs_path = f"/dbfs/mnt/{datalake_mount}/uc_migration/uc_migration_logs"

# COMMAND ----------

# DBTITLE 1,Clear the log storage location.
dbutils.fs.rm(f"/mnt/{datalake_mount}/uc_migration/", True)
dbutils.fs.mkdirs(f'/mnt/{datalake_mount}/uc_migration/uc_migration_logs/')

# COMMAND ----------

# DBTITLE 1,Iterate through target dbs and clean any data in them.
for db in dbs_to_migrate:
    if db in ['raw', 'cleansed', 'curated', 'curated_v2', 'curated_v3', 'semantic']:
        clean_up_catalog(f'{env}{db}')

# COMMAND ----------

# run only while in development/testing stage
# drop_datalab_tables()


# COMMAND ----------

# DBTITLE 1,Iterate through target dbs and deep clone data to Unity Catalog tables.
for db in dbs_to_migrate:
    create_managed_table_parallel(db)

# COMMAND ----------

# DBTITLE 1,Check table count in hive metastore vs Unity Catalog. Views are not migrated!
for db in dbs_to_migrate:
    assert_table_counts_post_migration(env, db)

# COMMAND ----------

# DBTITLE 1,Check row counts in hive metastore tables vs those in Unity Catalog.
for db in dbs_to_migrate:
    assert_row_counts_random_tables('raw')
    assert_row_counts_random_tables('cleansed')   
    assert_row_counts_random_tables('curated')

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

# MAGIC %sql
# MAGIC show tables in uc_migration.data_migration

# COMMAND ----------

# DBTITLE 1,Display the migration logs table filtering on the ones that errored.
#when this cell runs back to back to above cell, the table may not have eventuated (since the above is a streaming action), hence the time.sleep
time.sleep(30)

(spark
 .table(f'{catalog}.{schema}.{table}')
 .filter(F.col('error').isNotNull())
 .display()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from uc_migration.data_migration.dev_logs
# MAGIC where error is not null
