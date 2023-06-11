# Databricks notebook source
# MAGIC %md
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
env = '' if dbutils.secrets.get('ADS', 'databricks-env') == '_' else dbutils.secrets.get('ADS', 'databricks-env')
dbs_to_migrate = ['raw', 'cleansed', 'curated', 'datalab']
# dbs_to_migrate = ["raw"]

# COMMAND ----------

# DBTITLE 1,Update below variables for logging.
catalog = "uc_migration"
schema = "data_migration"
table = f"{env}logs"
datalake_mount = "datalake-raw"
migration_logs_path = f"/dbfs/mnt/{datalake_mount}/uc_migration/uc_migration_logs"

# COMMAND ----------

# DBTITLE 1,Check table count in hive metastore vs Unity Catalog. Views are not migrated!
for db in dbs_to_migrate:
    assert_table_counts_post_migration(env, db)

# COMMAND ----------

# DBTITLE 1,Check row counts in hive metastore tables vs those in Unity Catalog.
assert_row_counts_random_tables('raw')

# COMMAND ----------


assert_row_counts_random_tables('cleansed')   

# COMMAND ----------

assert_row_counts_random_tables('curated')


# COMMAND ----------

# DBTITLE 1,Retry the migration for failed tables
tables_to_upgrade = spark.table(f'uc_migration.data_migration.{table}').filter(F.col('error').isNotNull()).select('source_table_name').collect()
tables_to_upgrade = [table.asDict()['source_table_name'] for table in tables_to_upgrade]
# for table in tables_to_upgrade:
#     layer = table.split(".")[0]
#     table = table.split(".")[1]
#     create_managed_table(env, layer, table)
