# Databricks notebook source
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime, timedelta

# COMMAND ----------

# MAGIC %run ./../global-variables-python

# COMMAND ----------

# MAGIC %run ./util-sql-merge-functions

# COMMAND ----------

NEW_LINE = "\r\n"
TAB = "\t"
SQL_SERVER_COL_QUALIFER = '"'

# COMMAND ----------

def SQLMerge_SQLEDW_GenerateSQL(source_table_name, target_table_name, business_key, delta_column, data_load_mode, track_changes, datalake_source_table):
  
  curr_time_stamp = GeneralLocalDateTime()
  curr_time_stamp = str(curr_time_stamp.strftime("%Y-%m-%d %H:%M:%S"))
  
  query = f"SELECT * FROM {datalake_source_table} LIMIT 0"
  df_col_list = spark.sql(query)
  
  sql_query = ""
  if data_load_mode == ADS_WRITE_MODE_MERGE:
    sql_query = SQLMerge_SQLEDW_GenerateSQL_Merge(source_table_name, target_table_name, business_key, delta_column, track_changes, datalake_source_table, curr_time_stamp, df_col_list)
  elif data_load_mode == ADS_WRITE_MODE_APPEND:
    delta_column = COL_DL_RAW_LOAD
    sql_query = _SQLInsertSyntax_SQLEDW_APPEND(df_col_list, source_table_name, target_table_name)
    
  return sql_query

# COMMAND ----------

def SQLMerge_SQLEDW_GenerateSQL_Merge(source_table_name, target_table_name, business_key, delta_column, track_changes, datalake_source_table, curr_time_stamp, df_col_list):
  
  ALIAS_TABLE_SOURCE = "tbl_src"
  ALIAS_TABLE_TARGET = "tbl_tgt"
  ALIAS_TABLE_STAGE = "tbl_stg"
  ALIAS_TABLE_MAIN = "tbl_mn"

  #If Delta Column is a combination of Created and Upated Date then use the _transaction_date as delta column
  delta_column_update = GeneralGetUpdatedDeltaColumn(delta_column)
  

  #################PART 1 SOURCE QUERY ####################################
  #Get the source data and wrap in a CTE
  sql = "WITH " + ALIAS_TABLE_SOURCE + " AS (" + NEW_LINE + "SELECT * FROM " + source_table_name + NEW_LINE + ")" + NEW_LINE
  #################PART 1 SOURCE QUERY ####################################


  
  #################PART 2 MERGE QUERY ####################################
  #Create a MERGE SQL for SCD UPSERT
  sql += "MERGE INTO " + target_table_name + " " + ALIAS_TABLE_MAIN + NEW_LINE
  sql += "USING (" + NEW_LINE
  
  business_key_updated = _GetSQLCollectiveColumnsFromColumnNames(business_key, ALIAS_TABLE_SOURCE, "CONCAT", SQL_SERVER_COL_QUALIFER)
    
  #Get data from the first CTE above
  sql += TAB + "SELECT " + business_key_updated + " AS merge_key, " + ALIAS_TABLE_SOURCE + ".*" + NEW_LINE
  sql += TAB + "FROM " + ALIAS_TABLE_SOURCE + NEW_LINE
  
  #Complete the Merge SQL and join on merge_key
  sql += ") " + ALIAS_TABLE_STAGE + NEW_LINE
  business_key_updated = _GetSQLCollectiveColumnsFromColumnNames(business_key, ALIAS_TABLE_MAIN, "CONCAT", SQL_SERVER_COL_QUALIFER)
  sql += "ON " + business_key_updated + " = merge_key " + NEW_LINE
  if track_changes:
    sql += "AND " + ALIAS_TABLE_MAIN + "." + COL_RECORD_START + " = " + ALIAS_TABLE_STAGE + "." + COL_RECORD_START + NEW_LINE
  #################PART 2 MERGE QUERY ####################################


  #################PART 3 UPDATE DATA ####################################
  #If the records match then set the current version of the record as previous.
  #Update thh Row End Date and CurrentRecord = 0
  sql += "WHEN MATCHED " 

  if track_changes:
    sql_update = _SQLUpdateSetValue_SCD_SQLEDW(ALIAS_TABLE_MAIN, ALIAS_TABLE_STAGE)
  else:
    sql_update = _SQLUpdateSetValue_SQLEDW(df_col_list, business_key, ALIAS_TABLE_MAIN, ALIAS_TABLE_STAGE)
  sql += sql_update
#   sql_update = _SQLUpdateSetValue_SQLEDW(df_col_list, business_key, ALIAS_TABLE_MAIN, ALIAS_TABLE_STAGE)
#   sql += sql_update
  #################PART 3 UPDATE DATA ####################################

  #################PART 4 INSERT DATA ####################################
  #If not matched, then this is the new version of the record. Insert the new row
  sql += NEW_LINE
  sql += "WHEN NOT MATCHED THEN " + NEW_LINE
  sql_insert = _SQLInsertSyntax_SQLEDW(df_col_list, ALIAS_TABLE_STAGE)
  sql += sql_insert
  #################PART 4 INSERT DATA ####################################
  sql += ";"
  
  
  return sql

# COMMAND ----------

def _SQLUpdateSetValue_SQLEDW(dataframe, business_key, source_alias, target_alias):
  #Generate SQL for UPDATE column compare
  
  sql = "THEN " + NEW_LINE + "UPDATE SET "+ NEW_LINE

  #Exclude the following columns from Update
  col_exception_list = [business_key]

  #Get the list of columns which does not include the exception list 
  updated_col_list = _GetExclusiveList(dataframe.columns, col_exception_list)

  #Get the SQL Update Join Query part (src.col1 = tgt.col1, src.col2 = tgt.col2)
  sql += _GetSQLJoinConditionFromColumnNames(updated_col_list, source_alias, target_alias, join_type = "=", seperator = ", ", column_qualifer = SQL_SERVER_COL_QUALIFER)

  return sql

# COMMAND ----------

def _SQLUpdateSetValue_SCD_SQLEDW(source_alias, table_alias):
  #Generate SQL for UPDATE column compare
  
  #For SQL Server as the data is already built, we just update from the Stage table when the PK + RecordStart matches
  sql = f"AND {source_alias}.{COL_RECORD_END} <> {table_alias}.{COL_RECORD_END} "+ NEW_LINE
  sql += "THEN " + NEW_LINE + "UPDATE SET "+ NEW_LINE
  sql += f"{COL_RECORD_DELETED} = {table_alias}.{COL_RECORD_DELETED}, " 
  sql += f"{COL_RECORD_CURRENT} = {table_alias}.{COL_RECORD_CURRENT}, " 
  sql += f"{COL_RECORD_END} = {table_alias}.{COL_RECORD_END} " 
  sql += NEW_LINE
    
  return sql

# COMMAND ----------

def _SQLInsertSyntax_SQLEDW(dataframe, table_alias):
  #Generate SQL for INSERT
  
  sql_col = _GetSQLCollectiveColumnsFromColumnNames(dataframe.columns, "", "", SQL_SERVER_COL_QUALIFER)
  sql_values = _GetSQLCollectiveColumnsFromColumnNames(dataframe.columns, table_alias, "", SQL_SERVER_COL_QUALIFER)
  
  sql = "INSERT ({columns}) \nVALUES \n({values})".format(columns = sql_col, values = sql_values)
      
  return sql

# COMMAND ----------

def _SQLInsertSyntax_SQLEDW_APPEND(dataframe, source_table_name, target_table_name):
  #Generate SQL for INSERT
  
  sql_col = _GetSQLCollectiveColumnsFromColumnNames(dataframe.columns, "", "", SQL_SERVER_COL_QUALIFER)
  sql_values = _GetSQLCollectiveColumnsFromColumnNames(dataframe.columns, "", "", SQL_SERVER_COL_QUALIFER)
  
  sql = f"INSERT INTO {target_table_name} ({sql_col}) \nSELECT \n{sql_values} FROM {source_table_name}"
      
  return sql
