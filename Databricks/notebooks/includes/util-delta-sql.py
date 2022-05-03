# Databricks notebook source
def DeltaTableExists(table_name):
  
  sql = "DESCRIBE " + table_name
  try:
    LogExtended(sql)
    spark.sql(sql)
  except Exception as e:
    if "not found" in str(e):
      return False
    else:
      #Else raise a error. Something's gone wrong
      raise e
      
  LogEtl (table_name + " exists")
      
  return True

# COMMAND ----------

def DeltaTablePartitioned(table_name):
  
  try:
    spark.sql("SHOW PARTITIONS " + table_name)
  except Exception as e:
    if "not partitioned" in str(e):
      return False
    elif "not found" in str(e):
      return False
    else:
      #Else raise a error. Something's gone wrong
      raise e
      
  return True

# COMMAND ----------

def DeltaDatabaseExists(database_name):
    
  try:
    spark.sql("DESCRIBE " + database_name)
  except Exception as e:
    if "not found" in str(e):
      return False
    else:
      #Else raise a error. Something's gone wrong
      raise e
      
  return True

# COMMAND ----------

def DeltaGetDataLakePath(data_lake_zone, data_lake_folder, object):
  
  mount_point = DataLakeGetMountPoint(data_lake_zone)
  
  if data_lake_zone == ADS_DATALAKE_ZONE_CURATED:
    data_lake_path = f"dbfs:{mount_point}/{object.lower()}/delta"
  else:
    data_lake_path = f"dbfs:{mount_point}/{data_lake_folder.lower()}/{object.lower()}/delta"
  LogEtl(data_lake_path)
  
  return data_lake_path

# COMMAND ----------

def DeltaSaveDataframeDirect(dataframe, source_group, table_name, database_name, container, write_mode, schema, partition_keys = ""):

  #Mount the Data Lake 
  data_lake_mount_point = DataLakeGetMountPoint(container)
  
  print("table_name: "+table_name)
  print(table_name.split("_",1)[-1].lower())

  query = "CREATE DATABASE IF NOT EXISTS {0}".format(database_name)
  spark.sql(query)
  
  #Start of fix to restructure framework folders 
  
  if database_name == ADS_DATABASE_RAW or database_name == ADS_DATABASE_CLEANSED or database_name == ADS_DATABASE_CURATED:
    #Commented the below lines as part of the fix   
    #table_name = "{0}_{1}".format(source_system, table_name)
    delta_path = "dbfs:{mount}/{folder}/{sourceobject}/delta".format(mount=data_lake_mount_point, folder = source_group.lower(), sourceobject = table_name.split("_",1)[-1].lower())
    
  LogEtl ("Saving delta lake file : " + delta_path + " with mode " + write_mode)
  #End of fix to restructure framework folders 
  
  table_name_fq = "{0}.{1}".format(database_name, table_name)
  
  if write_mode == ADS_WRITE_MODE_OVERWRITE:
    if DeltaTableExists(table_name_fq):
      #As we are doing a full load, it is a good idea to do some cleanups
      LogEtl("Vacuuming Delta table : " + table_name_fq)
      spark.sql("VACUUM " + table_name_fq + " ")

  if partition_keys == "":
    LogEtl ("No partition keys")
    dataframe.write \
        .format('delta') \
        .option("mergeSchema", "true") \
        .option("overwriteSchema", "true") \
        .mode(write_mode) \
        .save(delta_path)
  else:
    LogEtl ("Partition keys : " + str(partition_keys))
    dataframe.write \
        .format('delta') \
        .option("mergeSchema", "true")\
        .option("overwriteSchema", "true") \
        .mode(write_mode) \
        .partitionBy(partition_keys) \
        .save(delta_path)
  
  LogEtl ("Creating table : {0} with mode {1} at path : {2}".format(table_name, write_mode, delta_path))
  query = "CREATE TABLE IF NOT EXISTS {0}.{1}  USING DELTA LOCATION \'{2}\'".format(database_name, table_name, delta_path)
  spark.sql(query)
  
  verifyTableSchema(f"{database_name}.{table_name}", schema)
  
  LogEtl ("Finishing : DeltaSaveDataframeToTable")
  

# COMMAND ----------

def DeltaCreateTableIfNotExists(delta_target_table, delta_source_table, data_lake_zone, delta_table_path, is_delta_extract):
  
  database_name = delta_target_table.split(".")[0]
  #Create database if it does not already exists
  query = "CREATE DATABASE IF NOT EXISTS {0}".format(database_name)
  spark.sql(query)

  #Nothing to do if the table already exists
  if DeltaTableExists(delta_target_table): 
    LogEtl('Delta Table ' + delta_target_table + ' already exists')
    return 

  LogEtl('Delta Table ' + delta_target_table + ' do not exist')
  source_query = "SELECT * FROM " + delta_source_table + " LIMIT 0"
  LogEtl(source_query)

  df = spark.sql(source_query)
  
  col_timestamp = COL_DL_CLEANSED_LOAD if data_lake_zone == ADS_DATALAKE_ZONE_CLEANSED else COL_DL_CURATED_LOAD

  #Adding a default column for Cleansed Zone load timestamp type
  df = df.withColumn(col_timestamp, lit(None).cast(TimestampType())) 
  
  #Adding SCD Columns. Have these columns even if the table is not on SCD
  df = df \
    .withColumn(COL_RECORD_START, lit(None).cast(TimestampType())) \
    .withColumn(COL_RECORD_END, lit(None).cast(TimestampType())) \
    .withColumn(COL_RECORD_DELETED, lit(None).cast(IntegerType())) \
    .withColumn(COL_RECORD_CURRENT, lit(None).cast(IntegerType())) 
    
  if not is_delta_extract:
    #If it is not delta extract then we do not need year, month and day
    df = df \
      .drop("year") \
      .drop("month") \
      .drop("day")
      
  df.printSchema()

  partition_table = True if is_delta_extract else False    
  #Temporary forcing the tables to stop partitioning. We want to check the performance impact
  partition_table = False
  
  LogEtl ('Creating an empty delta table at the location : ' + delta_table_path)
  if partition_table:
    df.write \
      .format('delta') \
      .mode("overwrite") \
      .partitionBy("year", "month", "day") \
      .option("mergeSchema", "true") \
     .option("overwriteSchema", "true") \
      .save(delta_table_path) 
  else:
    #If the extract is not on delta, then we do not know the partitioning column, hence the table will be created without partition
    df.write \
      .format('delta') \
      .mode("overwrite") \
      .option("mergeSchema", "true") \
      .option("overwriteSchema", "true") \
      .save(delta_table_path)

  query = "CREATE TABLE IF NOT EXISTS " + delta_target_table + " USING DELTA LOCATION \'" + delta_table_path + "\'"
  LogEtl(query)
  spark.sql(query)
  
  return

# COMMAND ----------

def DeltaTableAsCurrent(tableNameFqn, cache=False):
  df = spark.table(tableNameFqn).where((col(COL_RECORD_CURRENT) == 1) & (col(COL_RECORD_DELETED) == 0))
  if cache:
    df.cache()
  return df

# COMMAND ----------

def DeltaTableAsSnapshot(tableNameFqn, ReportingYear, ReportingDate, cache=False):
  df = spark.table(tableNameFqn).where((col(COL_RECORD_CURRENT) == 1) 
                                       & (col(COL_RECORD_DELETED) == 0) 
                                       & (col("ReportingYear") == ReportingYear)
                                       & (col("ReportingDate") == ReportingDate)
                                      )
  if cache:
    df.cache()
  return df

# COMMAND ----------

def DeltaExcludeSystemColumns(df):
  list = globals().copy()
  for l in list:
    if "COL_" in l:
      col = list[l]
      if col in df.columns:
        df = df.drop(col)
  return df

# COMMAND ----------

def DeltaSyncTableWithSource(source_table, target_table):
  
  #OBSOLTE - This module is not used. The module can add new columns based on the schema file. However in order to get it working with our Merge Code the column should only be added before the system columns rather than at the end. Moreover, we cannot update the data type. Thus, it is recommended to use scripts for rare occassions when a new column is added or updated.
  
  #Check if Target Table Exists
  if DeltaTableExists(target_table):

    #Get the dataframe with columns from source and target
    df_source = spark.sql("describe table {}".format(source_table))
    df_target = spark.sql("describe table {}".format(target_table))

    #Convert the dataframe to list so that we can use it
    lst_source = df_source.toPandas().values.tolist()
    lst_target = df_target.toPandas().values.tolist()
    
    #Convert the target list to dictionary so that it is easier to use
    dct_target = {lst_target[i][0]: lst_target[i][1] for i in range(0, len(lst_target))}

    for i in lst_source:
      #Ignore if the column name is blank of starts with Partitioning information
      if i[0].startswith("# Partition"): break
      if i[0] in dct_target:
        #Column already exists in the target table
        #The following block checks if the data type matches
        if i[1] == dct_target[i[0]]:
          #data type matches
          pass
        else:
          #data type does not match. Ideally automatic data type update should be discourged and the impact should be analyzed
          #print ("ALTER TABLE {} ALTER COLUMN ({} {})".format(target_table, i[0], i[1]))
          pass
      else:
        #Column do not exist in target. Create the new column
        sql = "ALTER TABLE {} ADD COLUMNS ({} {})".format(target_table, i[0], i[1])
        LogEtl (sql)
        spark.sql (sql)
      

# COMMAND ----------

def DeltaSaveToDeltaTableArchive(
  source_table, target_table, target_data_lake_zone, target_database, data_lake_folder, data_load_mode, track_changes = False, is_delta_extract = False, business_key = "", delta_column = "", start_counter = "0", end_counter = "0"):
  
  '''
  This method uses the source table to load data into target Delta Table
  It can do either Truncate-Load or Upsert or Append.
  It can also track history with SCD columns
  '''
  
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)  
  
  target_table_fqn = f"{target_database}.{target_table}" 
  # Start of Fix for Framework Folder Restructure
  if target_data_lake_zone != ADS_DATALAKE_ZONE_CURATED:
    target_table = target_table.split("_",1)[-1] 
  # End of Fix for Framework Folder Restructure
  data_lake_path = DeltaGetDataLakePath(target_data_lake_zone, data_lake_folder, target_table)
  
  #Ensure we have a table before we start the data load.
  #The table is used to generate a dataframe to get the column names and to generate the merge query.
  DeltaCreateTableIfNotExists(
    delta_target_table = target_table_fqn, 
    delta_source_table = source_table, 
    data_lake_zone = target_data_lake_zone, 
    delta_table_path = data_lake_path, 
    is_delta_extract = is_delta_extract)
  
  #Generate the Query to save the data
  LogEtl("Generating delta SQL query")
  delta_query = SQLMerge_DeltaTable_GenerateSQLArchive(
      source_table_name = source_table, 
      target_table_name = target_table_fqn, 
      business_key = business_key, 
      delta_column = delta_column, 
      start_counter = start_counter, 
      end_counter = end_counter, 
      is_delta_extract = is_delta_extract, 
      data_load_mode = data_load_mode,
      track_changes = track_changes, 
      target_data_lake_zone = target_data_lake_zone
  )
  LogExtended(delta_query)
  
  #Execute the SQL Query
  LogEtl("Executing merge query")
  spark.sql(delta_query)
  
#   if is_delta_extract == False and data_load_mode == ADS_WRITE_MODE_MERGE:
#     LogEtl("Generating delete query as it is full load extraction")
#     #Generate the Query to save the data
#     delta_query = SQLMerge_DeltaTable_GenerateSQL(
#         source_table_name = source_table, 
#         target_table_name = target_table_fqn, 
#         business_key = business_key, 
#         delta_column = delta_column, 
#         start_counter = start_counter, 
#         end_counter = end_counter, 
#         is_delta_extract = is_delta_extract, 
#         data_load_mode = data_load_mode,
#         track_changes = track_changes, 
#         target_data_lake_zone = target_data_lake_zone,
#         delete_data = True
#     )
#     LogExtended(delta_query)

#     #Execute the SQL Query
#     LogEtl("Executing delete query")
#     spark.sql(delta_query)
  



# COMMAND ----------

def deriveSurrogateKey(table_name):
    if table_name.startswith('dim'):
      skColumn = f"{table_name.replace('dim', '')}SK"
    else:
      skColumn = f"{table_name}SK"
    
    return skColumn[0].lower() + skColumn[1:]

# COMMAND ----------

def DeltaInjectSurrogateKeyToDataFrame(df, table_name):
  cols = df.columns
  skColumn = deriveSurrogateKey(table_name)
  LogEtl(f"Adding SK column : {skColumn}")
  dfSK = df.withColumn(skColumn, lit(None).cast(LongType()))
  df = dfSK.select(skColumn, *cols)
  #the surrogate key should never be null
  #df.schema[skColumn].nullable = False
  
  return df

# COMMAND ----------

def DeltaUpdateSurrogateKey(target_database, target_table, business_key):
  LogEtl(f"Updating SK values")
  dlTargetTableFqn = f"{target_database}.{target_table}"
  skColumn = deriveSurrogateKey(target_table)
  max = spark.sql(f"SELECT MAX({skColumn}) AS MAX FROM {dlTargetTableFqn}").rdd.collect()[0][0]
  max = 0 if max is None else max

  cols = business_key.split(",")
  updateCondition = " AND ".join(["SRC.`{col}` = TGT.`{col}`".format(col=col) for col in cols]) 

  sqlUpdateSK = f"WITH UpdateSK AS ( SELECT {business_key}, {COL_RECORD_START}, CAST(ROW_NUMBER() OVER (ORDER BY 1) AS BIGINT) + {max} AS {skColumn} FROM {dlTargetTableFqn} WHERE {skColumn} IS NULL ) MERGE INTO {dlTargetTableFqn} TGT USING UpdateSK SRC ON {updateCondition} AND SRC.{COL_RECORD_START} = TGT.{COL_RECORD_START} WHEN MATCHED THEN UPDATE SET {skColumn} = SRC.{skColumn}"
  LogExtended(sqlUpdateSK)
  spark.sql(sqlUpdateSK)

# COMMAND ----------

def DeltaSaveDataFrameToDeltaTableArchive(
  dataframe, target_table, target_data_lake_zone, target_database, data_lake_folder, data_load_mode, track_changes = False, is_delta_extract = False, business_key = "", AddSKColumn = False, delta_column = "", start_counter = "0", end_counter = "0"):
  
  stage_table_name = f"{ADS_DATABASE_STAGE}.{target_table}"
  
  if AddSKColumn:
    dataframe = DeltaInjectSurrogateKeyToDataFrame(dataframe, target_table)
      
  #Drop the stage table if it exists
  spark.sql(f"DROP TABLE IF EXISTS {stage_table_name}")
  #Save the dataframe temporarily to Stage database
  LogEtl(f"write to stg table1")
  dataframe.write.saveAsTable(stage_table_name)
  LogEtl(f"write to stg table2")
  
  #Use our generic method to save the dataframe now to Delta Table
  DeltaSaveToDeltaTableArchive(
    source_table = stage_table_name, 
    target_table = target_table, 
    target_data_lake_zone = target_data_lake_zone, 
    target_database = target_database, 
    data_lake_folder = data_lake_folder,
    data_load_mode = data_load_mode,
    track_changes = track_changes, 
    is_delta_extract =  is_delta_extract, 
    business_key = business_key, 
    delta_column = delta_column, 
    start_counter = start_counter, 
    end_counter = end_counter
    )
  
  if AddSKColumn:
    dlTargetTableFqn = f"{target_database}.{target_table}"
    DeltaUpdateSurrogateKey(target_database, target_table, business_key) 

  verifyTableSchema(f"{target_database}.{target_table}", dataframe.schema)


# COMMAND ----------

def DeltaSaveDataFrameToDeltaTableCleansed(
  dataframe, target_table, target_data_lake_zone, target_database, data_lake_folder, data_load_mode, track_changes = False, is_delta_extract = False, business_key = "", AddSKColumn = False, delta_column = "", start_counter = "0", end_counter = "0"):
  
  stage_table_name = f"{ADS_DATABASE_CLEANSED}.{target_table}"
  
  if AddSKColumn:
    dataframe = DeltaInjectSurrogateKeyToDataFrame(dataframe, target_table)
    
  
  #Drop the stage table if it exists
  spark.sql(f"DROP TABLE IF EXISTS {stage_table_name}")
  #Save the dataframe temporarily to Stage database
  dataframe.write.saveAsTable(stage_table_name)
  
  #Use our generic method to save the dataframe now to Delta Table
  DeltaSaveToDeltaTable(
    source_table = stage_table_name, 
    target_table = target_table, 
    target_data_lake_zone = target_data_lake_zone, 
    target_database = target_database, 
    data_lake_folder = data_lake_folder,
    data_load_mode = data_load_mode,
    track_changes = track_changes, 
    is_delta_extract =  is_delta_extract, 
    business_key = business_key, 
    delta_column = delta_column, 
    start_counter = start_counter, 
    end_counter = end_counter
    )
  
  if AddSKColumn:
    dlTargetTableFqn = f"{target_database}.{target_table}"
    DeltaUpdateSurrogateKey(target_database, target_table, business_key) 
  
  verifyTableSchema(f"{target_database}.{target_table}", dataframe.schema)

# COMMAND ----------

def DeltaSaveDataFrameToCurated(df, database_name, data_lake_folder, table_name, data_load_mode, azure_schema_name = '', save_to_azure = True):
  '''
  The function takes a Data Frame and saves it to the curated zone.
  Also, saves to Azure if save_to_azure flag is True
  '''
  
  DeltaSaveDataFrameToDeltaTable(
    dataframe = df, 
    target_table = table_name, 
    target_data_lake_zone = ADS_DATALAKE_ZONE_CURATED, 
    target_database = database_name,
    data_lake_folder = data_lake_folder,
    data_load_mode = data_load_mode,
    track_changes = False, 
    is_delta_extract = False, 
    business_key = "", 
    AddSKColumn = False,
    delta_column = "", 
    start_counter = "0", 
    end_counter = "0")
  
  #Save the table to Azure SQL DB as well. 
  #As the save to Azure SQL DB function is only available in Scala, we need a workaround to call it from a Python function
  #We call the notebook which has the Scala functions instead of embedding the code in the Python function
  if save_to_azure:
    #variable names
    delta_table_name = database_name + "." + table_name
    azure_table_name = table_name
    
    DeltaSyncToSQLDWOverwrite (delta_table_name, azure_schema_name, azure_table_name)

  

# COMMAND ----------

def DeltaSyncToSQLDW(delta_table, target_schema, target_table, business_key, start_counter, data_load_mode, additional_property = ""):
  '''
  This function sync's Delta Table to SQL DW
  '''
  
  sql_stg_tbl_name = f"{ADS_SQL_SCHEMA_STAGE}.{target_table}"
  sql_main_tbl_name = f"{target_schema}.{target_table}"
  stage_schema = f"{ADS_SQL_SCHEMA_STAGE}"
  
  if data_load_mode != ADS_WRITE_MODE_OVERWRITE:
    LogEtl("Generating SQL Query")
    sql_merge_query = SQLMerge_SQLDW_GenerateSQL(
      source_table_name = sql_stg_tbl_name, 
      target_table_name = sql_main_tbl_name, 
      business_key = business_key, 
      data_load_mode = data_load_mode,
      datalake_source_table =  delta_table)

    LogEtl("Saving the query to a temp table from Python so that it can be used by Scala later.")
    #Store the SQL Merge Query in a temporary table. This can be used later by the Scala code to execute the code on the SQL Server
    qry_table = target_table + "_sql_merge"
    GeneralSaveQueryAsTempTable(sql_merge_query, qry_table)
  else:
    LogEtl("SQL query not needed for OVERWRITE. Will write directly to table")
  
  # Call the Scala notebook to sync the SQL DW from Data Lake
  dbutils.notebook.run("/build/includes/scala-executors/exec-sync-sqldw", 0, {"p_delta_table":delta_table, "p_sql_schema_name":stage_schema, "p_sql_dw_table":target_table, "p_data_load_mode":data_load_mode, "p_start_counter":start_counter, "p_additional_property":additional_property})
  


# COMMAND ----------

def DeltaSyncToSQLDWOverwrite(delta_table, target_schema, target_table):
  '''
  This function sync's Delta Table to SQL DW using the OVERWRITE/TRUNCATE mode
  It calls the same function DeltaSyncToSQLDW but passes the default params to make it TRUNCATE write
  '''
  
  #Defaults
  business_key = ""
  delta_column = ""
  start_counter = ""
  
  DeltaSyncToSQLDW (delta_table, target_schema, target_table, business_key, start_counter, ADS_WRITE_MODE_OVERWRITE)
    

# COMMAND ----------

def DeltaSaveDataFrameToDeltaTable(
  dataframe, target_table, target_data_lake_zone, target_database, data_lake_folder, data_load_mode, schema, track_changes = False, is_delta_extract = False, business_key = "", AddSKColumn = False, delta_column = "", start_counter = "0", end_counter = "0"):
  
  stage_table_name = f"{ADS_DATABASE_STAGE}.{target_table}"
  
  if AddSKColumn:
    dataframe = DeltaInjectSurrogateKeyToDataFrame(dataframe, target_table)
      
  #Drop the stage table if it exists
  spark.sql(f"DROP TABLE IF EXISTS {stage_table_name}")
  #Save the dataframe temporarily to Stage database
  LogEtl(f"write to stg table1")
  dataframe.write.saveAsTable(stage_table_name)
  LogEtl(f"write to stg table2")
  
  #Use our generic method to save the dataframe now to Delta Table
  DeltaSaveToDeltaTable(
    source_table = stage_table_name, 
    target_table = target_table, 
    target_data_lake_zone = target_data_lake_zone, 
    target_database = target_database, 
    data_lake_folder = data_lake_folder,
    data_load_mode = data_load_mode,
    track_changes = track_changes, 
    is_delta_extract =  is_delta_extract, 
    business_key = business_key, 
    delta_column = delta_column, 
    start_counter = start_counter, 
    end_counter = end_counter
    )
  
  if AddSKColumn:
    dlTargetTableFqn = f"{target_database}.{target_table}"
    DeltaUpdateSurrogateKey(target_database, target_table, business_key) 

#   verifyTableSchema(f"{target_database}.{target_table}", dataframe.schema)
  verifyTableSchema(f"{target_database}.{target_table}", schema)

# COMMAND ----------

def DeltaSaveToDeltaTable(
  source_table, target_table, target_data_lake_zone, target_database, data_lake_folder, data_load_mode, track_changes = False, is_delta_extract = False, business_key = "", delta_column = "", start_counter = "0", end_counter = "0"):
  
  '''
  This method uses the source table to load data into target Delta Table
  It can do either Truncate-Load or Upsert or Append.
  It can also track history with SCD columns
  '''
  
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)  
  
  target_table_fqn = f"{target_database}.{target_table}" 
  # Start of Fix for Framework Folder Restructure
  if target_data_lake_zone != ADS_DATALAKE_ZONE_CURATED:
    target_table = target_table.split("_",1)[-1] 
  # End of Fix for Framework Folder Restructure
  data_lake_path = DeltaGetDataLakePath(target_data_lake_zone, data_lake_folder, target_table)
  
  #Ensure we have a table before we start the data load.
  #The table is used to generate a dataframe to get the column names and to generate the merge query.
  DeltaCreateTableIfNotExists(
    delta_target_table = target_table_fqn, 
    delta_source_table = source_table, 
    data_lake_zone = target_data_lake_zone, 
    delta_table_path = data_lake_path, 
    is_delta_extract = is_delta_extract)
  
  #Generate the Query to save the data
  LogEtl("Generating delta SQL query")
  delta_query = SQLMerge_DeltaTable_GenerateSQL(
      source_table_name = source_table, 
      target_table_name = target_table_fqn, 
      business_key = business_key, 
      delta_column = delta_column, 
      start_counter = start_counter, 
      end_counter = end_counter, 
      is_delta_extract = is_delta_extract, 
      data_load_mode = data_load_mode,
      track_changes = track_changes, 
      target_data_lake_zone = target_data_lake_zone
  )
  LogExtended(delta_query)
  
  #Execute the SQL Query
  LogEtl("Executing merge query")
  spark.sql(delta_query)
  
#   if is_delta_extract == False and data_load_mode == ADS_WRITE_MODE_MERGE:
#     LogEtl("Generating delete query as it is full load extraction")
#     #Generate the Query to save the data
#     delta_query = SQLMerge_DeltaTable_GenerateSQL(
#         source_table_name = source_table, 
#         target_table_name = target_table_fqn, 
#         business_key = business_key, 
#         delta_column = delta_column, 
#         start_counter = start_counter, 
#         end_counter = end_counter, 
#         is_delta_extract = is_delta_extract, 
#         data_load_mode = data_load_mode,
#         track_changes = track_changes, 
#         target_data_lake_zone = target_data_lake_zone,
#         delete_data = True
#     )
#     LogExtended(delta_query)

#     #Execute the SQL Query
#     LogEtl("Executing delete query")
#     spark.sql(delta_query)
  


