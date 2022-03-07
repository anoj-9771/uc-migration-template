# Databricks notebook source
# MAGIC %run ./../global-variables-python

# COMMAND ----------

# MAGIC %run ./util-sql-merge-functions

# COMMAND ----------

NEW_LINE = "\r\n"
TAB = "\t"
DELTA_COL_QUALIFER = "`"

# COMMAND ----------

def SQLMerge_DeltaTable_GenerateSQL(source_table_name, target_table_name, business_key, delta_column, start_counter, end_counter, is_delta_extract, data_load_mode, track_changes, target_data_lake_zone, delete_data = False):
  
  curr_time_stamp = GeneralLocalDateTime()
  curr_time_stamp = str(curr_time_stamp.strftime("%Y-%m-%d %H:%M:%S"))
  
  query = f"SELECT * FROM {target_table_name} LIMIT 0"
  df_col_list = spark.sql(query)
  
  sql_query = ""
  if data_load_mode == ADS_WRITE_MODE_MERGE:
    sql_query = _GenerateMergeSQL_DeltaTable(source_table_name, target_table_name, business_key, delta_column, start_counter, end_counter, is_delta_extract, track_changes, target_data_lake_zone, df_col_list, curr_time_stamp, delete_data)
  else:
    delta_column = COL_DL_RAW_LOAD
    sql_query = _SQLInsertSyntax_DeltaTable_Insert(df_col_list, is_delta_extract, delta_column, curr_time_stamp, target_data_lake_zone, source_table_name, target_table_name)
    
  if data_load_mode == ADS_WRITE_MODE_OVERWRITE:
      sql_query = sql_query.replace("INSERT INTO", "INSERT OVERWRITE") 
  elif data_load_mode == ADS_WRITE_MODE_APPEND and target_data_lake_zone == ADS_DATABASE_CLEANSED:
      print(f"Generating append query. Zone : {target_data_lake_zone}")
      sql_query += f" WHERE date_trunc('Second', {COL_DL_RAW_LOAD}) >= to_timestamp('{start_counter}')" 
    
    
  return sql_query

# COMMAND ----------

def _GenerateMergeSQL_DeltaTable(source_table_name, target_table_name, business_key, delta_column, start_counter, end_counter, is_delta_extract, track_changes, target_data_lake_zone, df_col_list, curr_time_stamp, delete_data = False):
  
  ALIAS_TABLE_SOURCE = "SRC"
  ALIAS_TABLE_TARGET = "TGT"
  ALIAS_TABLE_STAGE = "STG"
  ALIAS_TABLE_MAIN = "MN"
  
  #If Delta Column is a combination of Created and Upated Date then use the _transaction_date as delta column
  delta_column = GeneralGetUpdatedDeltaColumn(delta_column)
  

  #################PART 1 SOURCE QUERY ####################################
  #Get the source data from Raw Zone and wrap in a CTE
  #if delete_data:
  #  sql_source_query = _SQLSourceSelect_Delete(source_table_name, target_table_name, business_key, target_data_lake_zone)
  #else:
  #  sql_source_query = _SQLSourceSelect_DeltaTable(source_table_name, business_key, delta_column, start_counter, end_counter, is_delta_extract, target_data_lake_zone)
  #sql = f"WITH {ALIAS_TABLE_SOURCE} AS (" + NEW_LINE + "SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY " + business_key + " ORDER BY _DLRawZoneTimeStamp DESC) AS _RecordVersion FROM " + source_table_name + ") where _RecordVersion = 1)" + NEW_LINE
  sql_source_query = _SQLSourceSelect_DeltaTable(source_table_name, business_key, delta_column, start_counter, end_counter, is_delta_extract, target_data_lake_zone)
  sql = f"WITH {ALIAS_TABLE_SOURCE} AS (" + NEW_LINE + sql_source_query + NEW_LINE
  #################PART 1 SOURCE QUERY ####################################


  
  #################PART 2 MERGE QUERY ####################################
#Start of Fix for Handling Null in Key Coulumns

#   #Create a MERGE SQL for SCD
#   sql += f"MERGE INTO {target_table_name} {ALIAS_TABLE_MAIN} " + NEW_LINE
#   sql += "USING (" + NEW_LINE
  
#   sql += TAB + "-- These rows will either EXPIRE/UPDATE the current existing record (if merge_key matches) or INSERT the new record (if merge_key is not available)"+ NEW_LINE
#   business_key_updated = _GetSQLCollectiveColumnsFromColumnNames(business_key, ALIAS_TABLE_SOURCE, "CONCAT", DELTA_COL_QUALIFER)

#   #Get data from the first CTE above
#   sql += TAB + f"SELECT {business_key_updated} AS merge_key, {ALIAS_TABLE_SOURCE}.* FROM {ALIAS_TABLE_SOURCE}" + NEW_LINE
#   #We need RecordVersion only if it is the Delta Table load from the Raw Zone because we want the last version
#   #For SQL Server, this would have already been resolved

  raw_file_timestamp_exist = False
  di_sequence_number_exist = False
  query = f"SELECT * FROM {source_table_name} LIMIT 0"
  src_col_list = spark.sql(query)
  lst = src_col_list.columns
  col_lst = [col.strip() for col in lst]

  if len(col_lst) > 0:
    raw_file_timestamp_exist = any(COL_DL_RAW_FILE_TIMESTAMP in col for col in col_lst)
    col_dl_raw_load_exist = any(COL_DL_RAW_LOAD in col for col in col_lst)    
    di_sequence_number_exist = any("DI_SEQUENCE_NUMBER" in col for col in col_lst)


#Start of Arman's Fix
#   isSAPISU = False
#   if source_table_name != None and "sapisu" in source_table_name:
#     isSAPISU = True
#End of Arman's Fix
  #Create a MERGE SQL for SCD
  sql += f"MERGE INTO {target_table_name} {ALIAS_TABLE_MAIN} " + NEW_LINE
  sql += "USING (" + NEW_LINE
  sql += TAB + "SELECT * FROM " + NEW_LINE
  sql += TAB + "(" + NEW_LINE
  
  sql += TAB + "-- These rows will either EXPIRE/UPDATE the current existing record (if merge_key matches) or INSERT the new record (if merge_key is not available)"+ NEW_LINE

  #Get data from the first CTE above
  if target_data_lake_zone == ADS_DATABASE_CLEANSED:
    business_key_updated = _GetSQLCollectiveColumnsFromColumnNames(business_key, ALIAS_TABLE_SOURCE, "CONCAT", DELTA_COL_QUALIFER)
    sql += TAB + f"SELECT {ALIAS_TABLE_SOURCE}.*, {business_key_updated} AS merge_key" + NEW_LINE
    if not is_delta_extract and raw_file_timestamp_exist:
      sql += TAB + ",ROW_NUMBER() OVER (PARTITION BY " + business_key_updated + " ORDER BY " + COL_DL_RAW_FILE_TIMESTAMP + " DESC) AS " + COL_RECORD_VERSION + NEW_LINE    

    if not is_delta_extract and not raw_file_timestamp_exist:
      sql += TAB + ",ROW_NUMBER() OVER (PARTITION BY " + business_key_updated + " ORDER BY " + COL_DL_RAW_LOAD + " DESC) AS " + COL_RECORD_VERSION + NEW_LINE    
      
    #Added di_sequence_number to the order by clause  to pickup the right record from SAP delta extracts
    if is_delta_extract:
      if di_sequence_number_exist:
        sql += TAB + ",ROW_NUMBER() OVER (PARTITION BY " + business_key_updated + " ORDER BY " + delta_column + " DESC, DI_SEQUENCE_NUMBER DESC, " + COL_DL_RAW_LOAD + " DESC) AS " + COL_RECORD_VERSION + NEW_LINE
      elif col_dl_raw_load_exist:
        sql += TAB + ",ROW_NUMBER() OVER (PARTITION BY " + business_key_updated + " ORDER BY " + delta_column + " DESC, " + COL_DL_RAW_LOAD + " DESC) AS " + COL_RECORD_VERSION + NEW_LINE
        
      else:
        sql += TAB + ",ROW_NUMBER() OVER (PARTITION BY " + business_key_updated + " ORDER BY " + delta_column + " DESC) AS " + COL_RECORD_VERSION + NEW_LINE
    
#       else:
#         sql += TAB + ",ROW_NUMBER() OVER (PARTITION BY " + business_key_updated + " ORDER BY " + delta_column + " DESC, " + COL_DL_RAW_LOAD + " DESC) AS " + COL_RECORD_VERSION + NEW_LINE

    sql += TAB +  f"FROM {ALIAS_TABLE_SOURCE} ) WHERE {COL_RECORD_VERSION} = 1" + NEW_LINE
  else:
    business_key_updated = _GetSQLCollectiveColumnsFromColumnNames(business_key, ALIAS_TABLE_STAGE, "CONCAT", DELTA_COL_QUALIFER)
    business_key_updated = business_key_updated.replace("STG.", "SRC.").replace(" ", "")
    sql += TAB + f"SELECT {ALIAS_TABLE_SOURCE}.*, {business_key_updated} AS merge_key" + NEW_LINE
    sql += TAB +  f"FROM {ALIAS_TABLE_SOURCE} )" + NEW_LINE
  
  #We need RecordVersion only if it is the Delta Table load from the Raw Zone because we want the last version
  #For SQL Server, this would have already been resolved  
  #if target_data_lake_zone == ADS_DATABASE_CLEANSED: sql += TAB + f"WHERE {COL_RECORD_VERSION} = 1" + NEW_LINE  
  #End of Fix for Handling Null in Key Coulumns
  
  #if track_changes:
    #Union the previous query with the next one
  #  sql += TAB + "UNION ALL" + NEW_LINE
  #  if delete_data:
  #    sql_track_change_union = f"SELECT NULL AS merge_key, {ALIAS_TABLE_SOURCE}.* FROM {ALIAS_TABLE_SOURCE} " + NEW_LINE
  #  else:
  #    sql_track_change_union = _SQLTrackChanges_UnionQuery(ALIAS_TABLE_SOURCE, target_table_name, business_key, target_data_lake_zone, df_col_list, delta_column, is_delta_extract)
  #  sql += TAB + sql_track_change_union + NEW_LINE
  
  #Complete the Merge SQL and join on merge_key
  sql += TAB + ") " + ALIAS_TABLE_STAGE + NEW_LINE
  business_key_updated = _GetSQLCollectiveColumnsFromColumnNames(business_key, ALIAS_TABLE_MAIN, "CONCAT", DELTA_COL_QUALIFER)
  sql += f"ON {business_key_updated} = merge_key " + NEW_LINE
  #Start of Fix for Handling Null in Key Coulumns
  #if target_data_lake_zone == ADS_DATABASE_CLEANSED: sql += TAB + f"AND {COL_RECORD_VERSION} = 1" + NEW_LINE
  #End of Fix for Handling Null in Key Coulumns  
  #################PART 2 MERGE QUERY ####################################


  #################PART 3 UPDATE DATA ####################################
  #If the records match then set the current version of the record as previous.
  #Update thh Row End Date and CurrentRecord = 0
  sql_compare_condition = _SQLUpdateCompareCondition_DeltaTable(df_col_list, business_key, ALIAS_TABLE_STAGE, ALIAS_TABLE_MAIN, delta_column, is_delta_extract, delete_data)
  
  sql += "WHEN MATCHED " + NEW_LINE
  sql += sql_compare_condition
  sql += "THEN UPDATE SET "+ NEW_LINE

  if track_changes:
    sql_update = _SQLUpdateSetValue_SCD_DeltaTable(is_delta_extract, delta_column, ALIAS_TABLE_STAGE, curr_time_stamp)
  else:
    sql_update = _SQLUpdateSetValue_DeltaTable(df_col_list, business_key, ALIAS_TABLE_MAIN, ALIAS_TABLE_STAGE, curr_time_stamp, target_data_lake_zone, delete_data)
  sql += sql_update
  #################PART 3 UPDATE DATA ####################################

  #################PART 4 INSERT DATA ####################################
  #If not matched, then this is the new version of the record. Insert the new row
  #sql_insert = _SQLInsertSyntax_DeltaTable_Merge(df_col_list, is_delta_extract, delta_column, curr_time_stamp, target_data_lake_zone, delete_data)
  sql_insert = _SQLInsertSyntax_DeltaTable_Merge(df_col_list, is_delta_extract, delta_column, curr_time_stamp, target_data_lake_zone,source_table_name,target_table_name, delete_data)

  sql += "WHEN NOT MATCHED THEN " + NEW_LINE
  sql += sql_insert
  #################PART 4 INSERT DATA ####################################
  
  
  return sql

# COMMAND ----------

def _SQLSourceSelect_Delete(source_table, target_table, business_key, target_data_lake_zone):
  #Get the Source Select SQL from RAW
  #Version the rows based on date modified and assign Rank 1 to the latest record
  #Only this record will be historized
  #Ideally when daily data load happens, we will only have 1 version
  
  tbl_src = "SRC"
  tbl_tgt = "TGT"
  
  sql_join_business_keys = _GetSQLJoinConditionFromColumnNames(business_key, tbl_src, tbl_tgt, " = ", " AND ", DELTA_COL_QUALIFER)
  business_key_updated = _GetSQLCollectiveColumnsFromColumnNames(business_key, tbl_src, "CONCAT", DELTA_COL_QUALIFER)
  
  source_sql = TAB + f"SELECT {tbl_tgt}.* FROM {target_table} {tbl_tgt} " + NEW_LINE
  source_sql += TAB + f"LEFT JOIN {source_table} {tbl_src} ON {sql_join_business_keys} " + NEW_LINE
  source_sql += TAB + f"WHERE {business_key_updated} IS NULL " + NEW_LINE
  source_sql += TAB + f"AND {tbl_tgt}.{COL_RECORD_CURRENT} = 1 AND {tbl_tgt}.{COL_RECORD_DELETED} = 0 " + NEW_LINE

  return source_sql


# COMMAND ----------

def _SQLSourceSelect_DeltaTable(source_table, business_key, delta_column, start_counter, end_counter, is_delta_extract, target_data_lake_zone):
  #Get the Source Select SQL from RAW
  #Version the rows based on date modified and assign Rank 1 to the latest record
  #Only this record will be historized
  #Ideally when daily data load happens, we will only have 1 version

  tbl_alias = "SRC"
  business_key_updated = _GetSQLCollectiveColumnsFromColumnNames(business_key, tbl_alias, "CONCAT", DELTA_COL_QUALIFER)
  
  #Start of Fix for Handling Null in Key Coulumns
  if target_data_lake_zone == ADS_DATALAKE_ZONE_CLEANSED:
    #Transform the business_key column string to business_key column string with a suffix of '-TX'
    business_key_tx = _GetSQLCollectiveColumnsFromColumnNames(business_key, tbl_alias, '', DELTA_COL_QUALIFER)
    business_key_tx = business_key_tx.replace("SRC.", "").replace(" ", "")  
    col_list = business_key_tx.split(',') 
    #Generating the sql string for the additional '-TX' coulmns
    col_list =["COALESCE(" + tbl_alias + "." + item.replace("-TX","") + ", 'na') as " + item for item in col_list]
    business_key_tx = ','.join(col_list)
  #End of Fix for Handling Null in Key Coulumns

  #source_sql = TAB + "SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY " + business_key_updated + " ORDER BY " + COL_DL_RAW_LOAD + " DESC) AS _RecordVersion FROM " + source_table + ") WHERE _RecordVersion = 1)"
  #Start of Fix for Handling Null in Key Coulumns
  #Below line commented as part of the fix
  #source_sql = TAB + "SELECT *" + NEW_LINE
  source_sql = TAB + "SELECT " + tbl_alias + ".*" + NEW_LINE
  if target_data_lake_zone == ADS_DATALAKE_ZONE_CLEANSED:
    source_sql += TAB + ", " + business_key_tx + NEW_LINE
  #Start of Fix for Handling Null in Key Coulumns
#   if not is_delta_extract and target_data_lake_zone == ADS_DATABASE_CLEANSED :
#     source_sql += TAB + ",ROW_NUMBER() OVER (PARTITION BY " + business_key_updated + " ORDER BY " + COL_DL_RAW_LOAD + " DESC) AS " + COL_RECORD_VERSION + NEW_LINE
#     source_sql += TAB + "FROM " + source_table + " " + tbl_alias + NEW_LINE
#     source_sql += TAB + ")" + NEW_LINE 
      
#   if is_delta_extract and target_data_lake_zone == ADS_DATABASE_CLEANSED :
#     source_sql += TAB + ",ROW_NUMBER() OVER (PARTITION BY " + business_key_updated + " ORDER BY " + delta_column + " DESC, " + COL_DL_RAW_LOAD + " DESC) AS " + COL_RECORD_VERSION + NEW_LINE
#     source_sql += TAB + "FROM " + source_table + " " + tbl_alias + NEW_LINE
#     source_sql += TAB + ")" + NEW_LINE #this bracket was added by jackson while fixing the if not script above
#   if not target_data_lake_zone == ADS_DATABASE_CLEANSED :
#     source_sql += TAB + "FROM " + source_table + " " + tbl_alias + NEW_LINE
#     source_sql += TAB + ")" + NEW_LINE
   
  source_sql += TAB + "FROM " + source_table + " " + tbl_alias + NEW_LINE
  source_sql += TAB + ")" + NEW_LINE 
  #End of Fix for Handling Null in Key Coulumns


#   if is_delta_extract:
#     #We need the where filter only for the Delta Table Load
#     source_sql += TAB + "WHERE date_trunc('Second', " + COL_DL_RAW_LOAD + ") >= to_timestamp('" + start_counter + "')" + NEW_LINE

  return source_sql


# COMMAND ----------

def _SQLTrackChanges_UnionQuery(source_table_name, target_table_name, business_key, target_data_lake_zone, df_col_list, delta_column, is_delta_extract):
    
    ALIAS_TABLE_TARGET = "TGT"
    ALIAS_TABLE_SOURCE = source_table_name
      
    sql = ""
    sql += NEW_LINE
    sql += TAB + "-- These rows will INSERT new record version of existing records (only applicable for track_changes) "+ NEW_LINE
    sql += TAB + "-- Setting the mergeKey to NULL forces these rows to NOT MATCH and be INSERTed. "+ NEW_LINE

    #Get data from the target able with the Current Record = 1 version 
    #and also checking if the record has changed by comparing columns between Source and Target
    sql += TAB + f"SELECT null as merge_key, {ALIAS_TABLE_SOURCE}.*" + NEW_LINE
    sql += TAB + f"FROM {ALIAS_TABLE_SOURCE} " + NEW_LINE
    sql += TAB + f"LEFT JOIN {target_table_name} {ALIAS_TABLE_TARGET} " + NEW_LINE
    #Join Source and Target table based on Business Key
    sql_join_business_keys = _GetSQLJoinConditionFromColumnNames(business_key, ALIAS_TABLE_SOURCE, ALIAS_TABLE_TARGET, " = ", " AND ", DELTA_COL_QUALIFER)
    sql += TAB + f"ON {sql_join_business_keys}" + NEW_LINE

    sql += TAB + "WHERE 1 = 1 " + NEW_LINE
    
    #If it is delta extract then only use the latest version of the record
    if target_data_lake_zone == ADS_DATABASE_CLEANSED:
      sql += TAB + "-- Additional Joins to ensure we pick only the latest record from source (if delta extract)" + NEW_LINE
      sql += TAB + f"AND {ALIAS_TABLE_SOURCE}.{COL_RECORD_VERSION} = 1" + NEW_LINE

    sql += TAB + "-- The following joins will ensure that when INSERTing new version we only pick records that have changed. " + NEW_LINE
    sql_compare_table_columns = _SQLUpdateCompareCondition_DeltaTable(df_col_list, business_key, ALIAS_TABLE_SOURCE, ALIAS_TABLE_TARGET, delta_column, is_delta_extract, False)
    sql += sql_compare_table_columns
    
    return sql


# COMMAND ----------

def _SQLUpdateCompareCondition_DeltaTable(dataframe, business_key, source_alias, target_alias, delta_column, is_delta_extract, delete_data):
  #Generate SQL for UPDATE column compare
 
  #Exclude the following columns from Update
  #Start of Fix for Handling Null in Key Columns
#   col_exception_list = [business_key, COL_RECORD_VERSION, COL_RECORD_START, COL_RECORD_END, COL_RECORD_CURRENT, COL_RECORD_DELETED, COL_DL_RAW_LOAD, COL_DL_CLEANSED_LOAD, COL_DL_CURATED_LOAD]
  
  buskey_col_list = business_key.split(",")
  col_exception_list = [COL_RECORD_VERSION, COL_RECORD_START, COL_RECORD_END, COL_RECORD_CURRENT, COL_RECORD_DELETED, COL_DL_RAW_LOAD, COL_DL_CLEANSED_LOAD, COL_DL_CURATED_LOAD]
  col_exception_list.extend(buskey_col_list)
  #End of Fix for Handling Null in Key Columns
  #Get the list of columns which does not include the exception list 

  updated_col_list = _GetExclusiveList(dataframe.columns, col_exception_list)
  sql = ""
  #Check current record version even if track_changes is not enabled to ensure we only check only the latest records
  sql += TAB + f"AND {target_alias}.{COL_RECORD_CURRENT} = 1" + NEW_LINE
  
  if delete_data:
    sql = sql
  else:
    if is_delta_extract:
      sql += TAB + "-- If it is delta extract we can find new records checking if the source delta column value is higher than the target table" + NEW_LINE
      sql += TAB + f"AND {source_alias}.{delta_column} > {target_alias}.{delta_column}"
    else:
      # Calling the new Compare function that would handle nulls while comparing source and target columns in delta merge
      #join_condition = _GetSQLJoinConditionFromColumnNames(updated_col_list, source_alias, target_alias, "<>", " OR ", DELTA_COL_QUALIFER)
      join_condition = _GetSQLJoinConditionFromColumnNamesCompare(updated_col_list, source_alias, target_alias, "<>", " OR ", DELTA_COL_QUALIFER)
      sql += TAB + "-- If it full extract we have to do compare on all columns to find records that have changed" + NEW_LINE
      sql += TAB + f"AND ( ({join_condition}) OR ({target_alias}.{COL_RECORD_DELETED} = 1) )"
  
  sql += NEW_LINE
      
  return sql

# COMMAND ----------

def _SQLUpdateSetValue_DeltaTable(dataframe, business_key, source_alias, target_alias, curr_time_stamp, target_data_lake_zone, delete_data):
  #Generate SQL for UPDATE column compare
  
  delete_flag = 1 if delete_data else 0

 #Start of Fix for Handling Null in Key Columns
 #Exclude the following columns from Update
#   col_exception_list = [business_key, COL_RECORD_VERSION, COL_RECORD_START, COL_RECORD_END, COL_RECORD_CURRENT, COL_RECORD_DELETED] 
  buskey_col_list = business_key.split(",")
  col_exception_list = [COL_RECORD_VERSION, COL_RECORD_START, COL_RECORD_END, COL_RECORD_CURRENT, COL_RECORD_DELETED]
  col_exception_list.extend(buskey_col_list) 
  #End of Fix for Handling Null in Key Columns
  
  #Chose the timestamp column based on data lake zone
  if target_data_lake_zone == ADS_DATABASE_CLEANSED:
    col_exception_list.append(COL_DL_CLEANSED_LOAD)
    COL_TIMESTAMP = COL_DL_CLEANSED_LOAD
  elif target_data_lake_zone == ADS_DATABASE_CURATED:
    col_exception_list.append(COL_DL_CURATED_LOAD)
    COL_TIMESTAMP = COL_DL_CURATED_LOAD
    
  #Get the list of columns which does not include the exception list 
  updated_col_list = _GetExclusiveList(dataframe.columns, col_exception_list)

  #Get the SQL Update Join Query part (src.col1 = tgt.col1, src.col2 = tgt.col2)
  sql = _GetSQLJoinConditionFromColumnNames(updated_col_list, source_alias, target_alias, "=", ", ", DELTA_COL_QUALIFER)
      
  #Add the timestamp column for the zone
  sql+= "," + source_alias + "." + COL_TIMESTAMP + " = " + "to_timestamp('" + curr_time_stamp + "')" 
  sql+= f", {COL_RECORD_DELETED} = {delete_flag}" 
  
  sql += NEW_LINE
    
  return sql

# COMMAND ----------

def _SQLUpdateSetValue_SCD_DeltaTable(is_delta_extract, delta_column, table_alias, curr_time_stamp):
  #Generate SQL for UPDATE for SCD Delta Tables
  
  sql = ""
  sql += COL_RECORD_CURRENT + " = 0" 

  if is_delta_extract:
    sql += ", " + COL_RECORD_END + " = " + table_alias + "." + delta_column + " - INTERVAL 1 seconds " + NEW_LINE
    #If we are not doing delta extract, then there is not watermark column from Source, we use the current time to end the records
  else:
    record_end_time_stamp = datetime.strptime(curr_time_stamp, "%Y-%m-%d %H:%M:%S") - timedelta(seconds=1)
    sql += ", " + COL_RECORD_END + " = '" + str(record_end_time_stamp) + "'" + NEW_LINE
    
  return sql

# COMMAND ----------

def _SQLInsertSyntax_DeltaTable_Merge(dataframe, is_delta_extract, delta_column, curr_time_stamp, target_data_lake_zone,source_table_name,target_table_name,delete_data):
  #Generate SQL for INSERT
  
  insert_merge_sql = _SQLInsertSyntax_DeltaTable_Generate(dataframe, is_delta_extract, delta_column, curr_time_stamp, target_data_lake_zone,source_table_name,target_table_name, False, delete_data)

  return insert_merge_sql

# COMMAND ----------

def _SQLInsertSyntax_DeltaTable_Insert(dataframe, is_delta_extract, delta_column, curr_time_stamp, target_data_lake_zone, source_table, target_table):
  #Generate SQL for INSERT
  
  insert_merge_sql = _SQLInsertSyntax_DeltaTable_Generate(dataframe, is_delta_extract, delta_column, curr_time_stamp, target_data_lake_zone, source_table, target_table, True, False)
  
  return insert_merge_sql

# COMMAND ----------

def _SQLInsertSyntax_DeltaTable_Generate(dataframe, is_delta_extract, delta_column, curr_time_stamp, target_data_lake_zone, source_table = "", target_table = "", only_insert = False, delete_data = False):
  #Generate SQL for INSERT
  
  #Exclude the following columns from INSERT. We will add them with new values
  col_exception_list = [COL_RECORD_VERSION, COL_RECORD_START, COL_RECORD_END, COL_RECORD_CURRENT, COL_RECORD_DELETED]
  
  delete_flag = 1 if delete_data else 0

  #Chose the timestamp column based on data lake zone
  if target_data_lake_zone == ADS_DATABASE_CLEANSED:
    col_exception_list.append(COL_DL_CLEANSED_LOAD)
    COL_TIMESTAMP = COL_DL_CLEANSED_LOAD
  elif target_data_lake_zone == ADS_DATABASE_CURATED:
    col_exception_list.append(COL_DL_CURATED_LOAD)
    COL_TIMESTAMP = COL_DL_CURATED_LOAD

#Start of Fix for Handling Null in Key Columns  
  if target_data_lake_zone == ADS_DATABASE_CLEANSED:
    business_key_inp =  Params[PARAMS_BUSINESS_KEY_COLUMN]
    business_key_list = business_key_inp.split(",")
    col_exception_list.extend(business_key_list) 

  #Get the list of columns which does not include the exception list 
  updated_col_list = _GetExclusiveList(dataframe.columns, col_exception_list)
  updated_col_list.sort()
  
#End of Fix for Handling Null in Key Columns  
  
  sql_col = _GetSQLCollectiveColumnsFromColumnNames(updated_col_list, alias = "", func = "", column_qualifer = DELTA_COL_QUALIFER)
#Start of Fix for Handling Null in Key Columns  

  #sql_values = _GetSQLCollectiveColumnsFromColumnNames(updated_col_list, alias = "", func = "", column_qualifer = DELTA_COL_QUALIFER)
  sql_values = sql_col
  if target_data_lake_zone == ADS_DATABASE_CLEANSED:
    business_key = _GetSQLCollectiveColumnsFromColumnNames(business_key_list, alias = "", func = "", column_qualifer = DELTA_COL_QUALIFER)

  
    if is_delta_extract:
      business_key_tx_list = [item + "-TX" for item in business_key_list]
      business_key_tx = _GetSQLCollectiveColumnsFromColumnNames(business_key_tx_list, alias = "", func = "", column_qualifer = DELTA_COL_QUALIFER)

    if not is_delta_extract:
      #Generating the sql string for the additional '-TX' coulmns
      business_key_col = business_key.split(",")
      business_key_col_list =["COALESCE(" + item + ", 'na') " for item in business_key_col]
      business_key_tx = ",".join(business_key_col_list)

    sql_col += f", {business_key}"
    sql_values += f", {business_key_tx}"
#End of Fix for Handling Null in Key Columns
 

  #Add current timestamp column for data load
  sql_col += f", {COL_TIMESTAMP}, "
  sql_values += f", to_timestamp('{curr_time_stamp}'), "

#Start of Fix for Handling Null in Key Columns  
  #SCD columns
  col_record_start = delta_column if is_delta_extract else f"'{curr_time_stamp}'"
  #sql_col += f"{COL_RECORD_START}, {COL_RECORD_END}, {COL_RECORD_DELETED}, {COL_RECORD_CURRENT}"
  #sql_values += f"{col_record_start}, to_timestamp('9999-12-31 00:00:00'), {delete_flag}, 1"
  sql_col += f"{COL_RECORD_START}, {COL_RECORD_END}, {COL_RECORD_DELETED}, {COL_RECORD_CURRENT}"
  if is_delta_extract:
    sql_values += f"to_timestamp(cast ({col_record_start} as string), 'yyyyMMddHHmmss'), "
  else:
    sql_values += f"{col_record_start}, "
  sql_values += f"to_timestamp('2099-12-31 00:00:00'), {delete_flag}, 1"
  #Build the INSERT SQL with column list and values list
  if not is_delta_extract and target_data_lake_zone == ADS_DATABASE_CLEANSED and only_insert:
    sql = f"INSERT INTO {target_table} ({sql_col}) SELECT {sql_values} FROM {source_table}" 
  else:
    sql = f"INSERT ({sql_col}) {NEW_LINE}VALUES ({sql_values})"

#End of Fix for Handling Null in Key Columns  
      
  return sql
