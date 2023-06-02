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

# MAGIC %sql
# MAGIC select * from uc_migration.data_migration.dev_logs
# MAGIC where error not like "%Unsupported DEEP clone%"
# MAGIC
# MAGIC -- where error is not null
# MAGIC -- and
# MAGIC -- where error not like '%`dev_curated`.`nan`.`nan`%'
# MAGIC -- and error not like "%doesn't exist%"
# MAGIC -- where error is not null
# MAGIC -- and error not like "%Unsupported DEEP clone%"

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended curated.view_installation

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in semantic

# COMMAND ----------

spark.table('cleansed.maximo_a_asset')

# COMMAND ----------

spark.table('semantic.dimwaternetwork')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from stage.ocr_extract

# COMMAND ----------

spark.table('curated.view_installation').columns

# COMMAND ----------

create_managed_table(env, 'cleansed', 'vwbeachpollutionweatherforecast')

# COMMAND ----------

for db in ['raw', 'cleansed']:
    print (db)
    spark.sql(f"show tables in {db}").filter("tableName not like '%\_%'").display()

# COMMAND ----------

# DBTITLE 1,Retry the migration for failed tables
tables_to_upgrade = spark.table(f'uc_migration.data_migration.{table}').filter(F.col('error').isNotNull()).select('source_table_name').collect()
# tables_to_upgrade = [table.asDict()['source_table_name'] for table in tables_to_upgrade]
tables_to_upgrade = ['curated.f_scdcomplex']
for table in tables_to_upgrade:
    layer = table.split(".")[0]
    table = table.split(".")[1]
    create_managed_table(env, layer, table)

# COMMAND ----------

lookup_curated_namespace('dev_', 'curated', 'f_scdcomplex', excel_path='/mnt/datalake-raw/cleansed_csv/curated_mapping.csv')['table_name']

# COMMAND ----------

get_target_namespace('dev_', 'curated', 'f_scdcomplex')

# COMMAND ----------

# lookup_curated_namespace('dev_', 'curated', 'f_scdcomplex', excel_path='/mnt/datalake-raw/cleansed_csv/curated_mapping.csv')

# COMMAND ----------


