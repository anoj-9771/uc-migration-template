# Databricks notebook source
# MAGIC %md # Helper Functions

# COMMAND ----------

import re
import pandas as pd
import numpy as np
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md ## read_run_sheet

# COMMAND ----------

def read_run_sheet(excel_path, sheet_name):
    p_df = pd.read_excel(io = excel_path, engine='openpyxl', sheet_name = sheet_name, header=0)
    return p_df

# COMMAND ----------

# MAGIC %md ## log_to_json

# COMMAND ----------

def log_to_json(source_table_name:str, target_table_name:str, start_time:str, end_time:str, clone_stats:str=None, error:str=None) -> None:
    log = {
        "source_table_name": "",
        "target_table_name": "",
        "start_time": "",
        "end_time": "",
        "clone_stats": "",
        "error": ""
    }
    
    log['source_table_name'] = source_table_name
    log['target_table_name'] = target_table_name
    log['start_time'] = start_time
    log['end_time'] = end_time
    log['clone_stats'] = clone_stats
    log['error'] = error

    with open(f"{migration_logs_path}/log_{source_table_name}_{datetime.now().strftime('%Y%m%d_%H%M')}.json", "w") as f:
        json.dump(log, f)

# COMMAND ----------

# MAGIC %md ## lookup_curated_namespace

# COMMAND ----------

def lookup_curated_namespace(env:str, current_database_name: str, current_table_name: str, excel_path:str) -> str:
    """looks up the target table namespace based on the current_table_name provided. note that this function assumes that there are no duplicate 'current_table_name' entries in the excel sheet."""
    future_namespace = {}
    #convert the given database_name and table_name to lower so they can be compared with the migration spreadsheet
    current_database_name = current_database_name.lower()
    current_table_name = current_table_name.lower()
    try:
        p_df = read_run_sheet(excel_path, f'{env}curated_mapping')
        future_database_name = p_df[(p_df['current_table_name'] == current_table_name) & (p_df['current_database_name'].str.contains('curated'))]['future_database_name'].replace(np.nan, None).tolist()[0]
        future_table_name = p_df[(p_df['current_table_name'] == current_table_name) & (p_df['current_database_name'].str.contains('curated'))]['future_table_name'].replace(np.nan, None).tolist()[0]
        future_namespace['database_name'] = future_database_name
        future_namespace['table_name'] = future_table_name
    except Exception as e:
        future_namespace['database_name'] = None
        future_namespace['table_name'] = None
        # future_namespace['database_name'] = 'dim' if 'dim' in current_table_name else 'fact' if 'fact' in current_table_name else 'brg' if 'brg' in current_table_name else 'uncategorized'
        # future_namespace['table_name'] = current_table_name.replace('dim', '') if 'dim' in current_table_name else current_table_name.replace('fact', '') if 'fact' in current_table_name else current_table_name
        print (f'Warning! Issue occurred while looking up the future namespace for table: {current_database_name}.{current_table_name}')
    return future_namespace
    
# lookup_curated_namespace('ppd_', 'semantic', 'vw_maximo_workorder', excel_path=excel_path)

# COMMAND ----------

# MAGIC %md ## get_target_catalog

# COMMAND ----------

def get_target_catalog(env:str, layer:str):
    return get_target_namespace(env, layer, 'vw_maximo_workorder')['catalog_name']

# COMMAND ----------

# MAGIC %md ## get_target_namespace

# COMMAND ----------

def get_target_namespace(env:str, layer:str, table_name:str, excel_path:str="/dbfs/FileStore/uc/uc_scope.xlsx") -> str:
    """generates target namespace based on the table attributes provided."""
    catalog_name = f'{env}{layer}'
    new_namespace_obj = {}
    # if layer == 'raw' or layer == 'cleansed':
    if layer in ['raw', 'cleansed', 'stage', 'rejected']:
        #use pattern to convert raw.source_tablename to raw.source.table_name
        new_namespace_obj['catalog_name'] = catalog_name
        new_namespace_obj['database_name'] = table_name.split('_')[0]
        new_namespace_obj['table_name'] = '_'.join(table_name.split('_')[1:])
    elif 'curated' in layer or 'semantic' in layer:
        #use lookup_curated_namespace to find the target database and table based on mapping sheet
        new_namespace_obj['catalog_name'] = f'{env}{layer}'
        new_namespace_obj['database_name'] = lookup_curated_namespace(env, layer, table_name, excel_path)['database_name']
        new_namespace_obj['table_name'] = lookup_curated_namespace(env, layer, table_name, excel_path)['table_name']
    elif layer == 'datalab':
        #datalab tables are simply moved to datalab.datalab_env database
        trimmed_env = env.replace('_', '')
        new_namespace_obj['catalog_name'] = layer
        # test this out for prod
        new_namespace_obj['database_name'] = 'swc' if trimmed_env == '' else 'preprod' if trimmed_env == 'ppd' else f'{trimmed_env}' 
        new_namespace_obj['table_name'] = table_name
    else:
        trimmed_env = env.replace('_', '')
        new_namespace_obj['catalog_name'] = layer
        # test this out for prod
        new_namespace_obj['database_name'] = trimmed_env 
        new_namespace_obj['table_name'] = table_name
    new_namespace_obj['new_namespace'] = f"{new_namespace_obj['catalog_name']}.{new_namespace_obj['database_name']}.{new_namespace_obj['table_name']}"
    return new_namespace_obj

# get_target_namespace('ppd_', 'semantic', 'vw_maximo_workorder')

# COMMAND ----------

# MAGIC %md ## create_managed_table

# COMMAND ----------

def create_managed_table(env:str, layer:str, table_name:str) -> None:
    """Converts given external table to a managed table in Unity Catalog catalog."""
    hive_metastore_namespace = f'hive_metastore.{layer}.{table_name}'
    new_namespace_obj = get_target_namespace(env, layer, table_name)
    new_namespace = f"{new_namespace_obj['catalog_name']}.{new_namespace_obj['database_name']}.{new_namespace_obj['table_name']}"
    
    start_time = str(datetime.now())

    if new_namespace_obj['database_name'] is None:
        print (f'Lookup for failed for this table: {hive_metastore_namespace}. So not attempting a migration.')
        log_to_json(f'{layer}.{table_name}', new_namespace, start_time, end_time='9999-12-31 23:59', clone_stats=None, error=f'Lookup failed for this table.')
    else:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {new_namespace_obj['catalog_name']}.{new_namespace_obj['database_name']}")
        try:
            print (f'converting table {hive_metastore_namespace} to {new_namespace}')
            spark.table(hive_metastore_namespace).count()
            # using the sql command here because only the sql command (and not the python equivalent) generates the desired info dataframe
            df = spark.sql(f"""CREATE TABLE {new_namespace} CLONE {hive_metastore_namespace}""")
            clone_stats = df.collect()[0].asDict()
            end_time = str(datetime.now())
            log_to_json(f'{layer}.{table_name}', new_namespace, start_time, end_time, clone_stats=clone_stats, error=None)        
            time.sleep(4)  #putting some sleep in order to avoid clogging the data transfer
        except Exception as e:
            print (f'Error: {e}')
            log_to_json(f'{layer}.{table_name}', new_namespace, start_time, end_time='9999-12-31 23:59', clone_stats=None, error=f'{e}')

# create_managed_table(env, 'datalab', 'storage_test_927')
# create_managed_table('ppd_', 'raw', 'iicats_groups')

# COMMAND ----------

def getXMLExtendedProperties(table_name:str) -> dict:
    import json
    sql = "select concat(lower(DestinationSchema),'_',lower(DestinationTableName)) as TableName,ExtendedProperties from hive_metastore.controldb.dbo_extractloadmanifest where RawHandler='raw-load' and RawPath like '%.xml'"
    df = spark.sql(sql)
    return json.loads(df.filter(f"TableName = '{table_name}'").select("ExtendedProperties").collect()[0]['ExtendedProperties'])


# COMMAND ----------

# MAGIC %md ## create_external_table

# COMMAND ----------

def create_external_table(env:str, layer:str, table_name:str, target_location:str, provider:str) -> None:
    """Converts given hive metastore external table to an external table in Unity Catalog"""  
    hive_metastore_namespace = f'hive_metastore.{layer}.{table_name}'
    new_namespace_obj = get_target_namespace(env, layer, table_name)
    new_namespace = f"{new_namespace_obj['catalog_name']}.{new_namespace_obj['database_name']}.{new_namespace_obj['table_name']}"    
    fileFormat = provider
    fileOptions = ""
    env = env.replace('_','')
    if env == '':
        env = 'prod'
    target_path = target_location.replace(f'dbfs:/mnt/datalake-{layer}',f'abfss://{layer}@sadaf{env}01.dfs.core.windows.net')
    if(fileFormat =="XML"):
        extendedProperties = getXMLExtendedProperties(table_name)
        rowTag = extendedProperties["rowTag"]
        fileOptions = f", ignoreNamespace \"true\", rowTag \"{rowTag}\""
    elif (fileFormat =="CSV"):
        fileOptions = ", header \"true\", inferSchema \"true\", multiline \"true\""
    elif (fileFormat == "JSON"):
        spark.conf.set("spark.sql.caseSensitive", "true")
        fileOptions = ", multiline \"true\", inferSchema \"true\""
    else:
        fileFormat = "PARQUET"     
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {new_namespace_obj['catalog_name']}.{new_namespace_obj['database_name']}")
    start_time = str(datetime.now())
    try:
        print (f'converting table {hive_metastore_namespace} to {new_namespace}')
        spark.table(hive_metastore_namespace).count()
        sql = f"DROP TABLE IF EXISTS {new_namespace};"
        spark.sql(sql)
        sql = f"CREATE TABLE {new_namespace} USING {fileFormat} OPTIONS (path \"{target_path}\" {fileOptions});"
        df = spark.sql(sql)
        try:
            clone_stats = df.collect()[0].asDict()
        except:
            clone_stats = {}
        end_time = str(datetime.now())
        log_to_json(f'{layer}.{table_name}', new_namespace, start_time, end_time, clone_stats=clone_stats, error=None)        
        time.sleep(4)  #putting some sleep in order to avoid clogging the data transfer
    except Exception as e:
        print (f'Error: {e}')
        log_to_json(f'{layer}.{table_name}', new_namespace, start_time, end_time='9999-12-31 23:59', clone_stats=None, error=f'{e}')            

# COMMAND ----------

def getXMLExtendedProperties(table_name:str) -> dict:
    import json
    sql = "select concat(lower(DestinationSchema),'_',lower(DestinationTableName)) as TableName,ExtendedProperties from hive_metastore.controldb.dbo_extractloadmanifest where RawHandler='raw-load' and RawPath like '%.xml'"
    df = spark.sql(sql)
    return json.loads(df.filter(f"TableName = '{table_name}'").select("ExtendedProperties").collect()[0]['ExtendedProperties'])


# COMMAND ----------

# MAGIC %md ## create_external_table

# COMMAND ----------

def create_external_table(env:str, layer:str, table_name:str, target_location:str, provider:str) -> None:
    """Converts given hive metastore external table to an external table in Unity Catalog"""  
    hive_metastore_namespace = f'hive_metastore.{layer}.{table_name}'
    new_namespace_obj = get_target_namespace(env, layer, table_name)
    new_namespace = f"{new_namespace_obj['catalog_name']}.{new_namespace_obj['database_name']}.{new_namespace_obj['table_name']}"    
    fileFormat = provider
    fileOptions = ""
    env = env.replace('_','')
    if env == '':
        env = 'prod'
    target_path = target_location.replace(f'dbfs:/mnt/datalake-{layer}',f'abfss://{layer}@sadaf{env}01.dfs.core.windows.net')
    if(fileFormat =="XML"):
        extendedProperties = getXMLExtendedProperties(table_name)
        rowTag = extendedProperties["rowTag"]
        fileOptions = f", ignoreNamespace \"true\", rowTag \"{rowTag}\""
    elif (fileFormat =="CSV"):
        fileOptions = ", header \"true\", inferSchema \"true\", multiline \"true\""
    elif (fileFormat == "JSON"):
        spark.conf.set("spark.sql.caseSensitive", "true")
        fileOptions = ", multiline \"true\", inferSchema \"true\""
    else:
        fileFormat = "PARQUET"     
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {new_namespace_obj['catalog_name']}.{new_namespace_obj['database_name']}")
    start_time = str(datetime.now())
    try:
        print (f'converting table {hive_metastore_namespace} to {new_namespace}')
        spark.table(hive_metastore_namespace).count()
        sql = f"DROP TABLE IF EXISTS {new_namespace};"
        spark.sql(sql)
        sql = f"CREATE TABLE {new_namespace} USING {fileFormat} OPTIONS (path \"{target_path}\" {fileOptions});"
        df = spark.sql(sql)
        try:
            clone_stats = df.collect()[0].asDict()
        except:
            clone_stats = {}
        end_time = str(datetime.now())
        log_to_json(f'{layer}.{table_name}', new_namespace, start_time, end_time, clone_stats=clone_stats, error=None)        
        time.sleep(4)  #putting some sleep in order to avoid clogging the data transfer
    except Exception as e:
        print (f'Error: {e}')
        log_to_json(f'{layer}.{table_name}', new_namespace, start_time, end_time='9999-12-31 23:59', clone_stats=None, error=f'{e}')            

# COMMAND ----------

#debug
# log_to_json('test', 'test', 'test', 'test')

# COMMAND ----------

# MAGIC %md ## parallel_run

# COMMAND ----------

def parallel_run(mapper, env_list, layer_list, table_list, apply_flat_map=False):
    """Invoke the parallel execution of a function."""
    with ThreadPoolExecutor() as executor:
        results = executor.map(mapper, env_list, layer_list, table_list)
        return results

# COMMAND ----------

# MAGIC %md ## create_managed_table_parallel

# COMMAND ----------

def create_managed_table_parallel(layer:str):
      """Converts all hive metastore tables in a a parallel fashion."""
      try:
        tables = spark.sql(f'SHOW TABLES IN hive_metastore.{layer}').select('tableName').collect()
        table_list = [table.asDict()['tableName'] for table in tables ]
        layer_list = [f'{layer}' for x in table_list]
      #how does it work for prod
        env_list = [f'{env}' for x in table_list]
        parallel_run(create_managed_table, env_list, layer_list, table_list)
      except Exception as e:
        print (f'Error: Something went wrong when trying to migrate database {layer}')

# mock failure scenarios
# spark.sql('drop table if exists trial_cleansed.maximo.a_asset')
# create_managed_table('trial_', 'cleansed', 'maximo_a_asset')
# # simulate a failure because of a table that's already existing
# create_managed_table('trial_', 'cleansed', 'maximo_a_asset')
# # simulate a failure because of a table that doesn't exist
# create_managed_table('trial_', 'cleansed', 'maximo_a_asset_stuff')

# COMMAND ----------

# MAGIC %md ## clean_up_catalog

# COMMAND ----------

def clean_up_catalog(catalog):
    """Clean up all databases so you can start fresh migration."""
    print (f'dropping all databases in {catalog}')
    db_row_list = spark.sql(f'show databases in {catalog}').collect()
    db_list =[db_row.asDict()['databaseName'] for db_row in db_row_list]
    db_list.remove('information_schema')
    for db in db_list:
        spark.sql(f"drop database if exists {catalog}.{db} cascade")

# COMMAND ----------

# MAGIC %md ## drop_datalab_tables

# COMMAND ----------

def drop_datalab_tables():
    datalab_tables = (
        spark.table("datalab.information_schema.tables")
        .filter("table_schema != 'information_schema'")
        .select("table_catalog", "table_schema", "table_name")
        .collect()
    )
    datalab_tables = [f"{datalab_table.asDict()['table_catalog']}.{datalab_table.asDict()['table_schema']}.{datalab_table.asDict()['table_name']}" for datalab_table in datalab_tables]
    for table in datalab_tables:
        spark.sql(f"drop table {table}")

# COMMAND ----------

# MAGIC %md ## get_diff_tables

# COMMAND ----------

def get_diff_tables(env, layer):
    hive_table_list = spark.sql(f"show tables in {layer}").select('tableName').collect()
    hive_table_list = [table.asDict()['tableName'] for table in hive_table_list]
    uc_table_list = (
        spark.table(f'{get_target_catalog(env, layer)}.information_schema.tables')
        .withColumn('conv_table_name', F.concat(F.col('table_schema'),F.lit('_'), F.col('table_name')))
        .filter(F.col('table_schema') != 'information_schema')
        .select('conv_table_name').collect()
    )
    uc_table_list = [table.asDict()['conv_table_name'] for table in uc_table_list] if len(uc_table_list) > 0 else []

    return (set(hive_table_list) - set(uc_table_list))
