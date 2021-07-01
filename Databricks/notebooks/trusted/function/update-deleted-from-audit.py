# Databricks notebook source
# MAGIC %run ../../includes/include-all-util

# COMMAND ----------

dbutils.widgets.text("audit_table", "", "Audit Table")
dbutils.widgets.text("main_table", "", "Main Table")
dbutils.widgets.text("business_key", "", "Business Key Column")
dbutils.widgets.text("delta_column", "", "Delta Column")
dbutils.widgets.text("start_counter", "", "Start Counter")
dbutils.widgets.text("end_counter", "", "End Counter")

# COMMAND ----------

audit_table = dbutils.widgets.get("audit_table")
main_table = dbutils.widgets.get("main_table")
business_key = dbutils.widgets.get("business_key")
delta_column = dbutils.widgets.get("delta_column")
start_counter = dbutils.widgets.get("start_counter")
end_counter = dbutils.widgets.get("end_counter")

start_counter = start_counter.replace("T", " ")
end_counter = end_counter.replace("T", " ")

print(audit_table)
print(main_table)
print(business_key)
print(delta_column)
print(start_counter)
print(end_counter)

# COMMAND ----------

NEW_LINE = "\r\n"
TAB = "\t"

# COMMAND ----------

def GenerateSourceQuery(audit_table, business_key, delta_column, start_counter):

  sql = TAB + "SELECT * " + NEW_LINE
  sql += TAB + ",ROW_NUMBER() OVER (PARTITION BY " + business_key + " ORDER BY " + delta_column + " DESC, " + COL_DL_RAW_LOAD + " DESC) AS " + COL_RECORD_VERSION + NEW_LINE
  sql += TAB + "FROM " + ADS_DATABASE_RAW + "." + audit_table + NEW_LINE
  sql += TAB + "WHERE " + delta_column + " >= '" + start_counter + "'" + NEW_LINE
  
  return sql
  

# COMMAND ----------

def GenerateExpireRecordSQL(audit_table, main_table, business_key, delta_column, start_counter, end_counter):
  
  alias_main = "main"
  alias_deleted = "deleted"
  
  source_query = GenerateSourceQuery(audit_table, business_key, delta_column, start_counter)
  join_condition = _GetSQLJoinConditionFromColumnNames(business_key, alias_main, alias_deleted, "=", " AND ")
  
  sql = "WITH Source AS (" + NEW_LINE
  sql += source_query
  sql += ")" + NEW_LINE
  sql += "MERGE INTO " + ADS_DATABASE_TRUSTED + "." + main_table + " " + alias_main + NEW_LINE
  sql += "USING ("  + NEW_LINE
  sql += TAB + "SELECT * FROM Source " + NEW_LINE
  sql += TAB + "WHERE " + COL_RECORD_VERSION + " = 1" + NEW_LINE
  sql += TAB + "AND " + delta_column + " >= '" + start_counter + "'" + NEW_LINE
  sql += ") " + alias_deleted + NEW_LINE
  sql += TAB + "ON " + join_condition
  sql += TAB + "AND " + alias_main + "." + COL_RECORD_CURRENT + " = 1" + NEW_LINE
  sql += TAB + "AND " + alias_main + "." + COL_RECORD_DELETED + " = 0" + NEW_LINE
  sql += TAB + "AND " + alias_main + "." + COL_RECORD_END + " = '9999-12-31 00:00:00'" + NEW_LINE
  sql += TAB + "AND " + alias_deleted + "." + delta_column + " > " + alias_main + "." + COL_RECORD_START + NEW_LINE
  sql += "WHEN MATCHED THEN" + NEW_LINE
  sql += "UPDATE SET" + NEW_LINE
  sql += TAB + COL_RECORD_CURRENT + " = 0 "  + NEW_LINE
  sql += TAB + "," + COL_RECORD_END + " = " + delta_column + " - INTERVAL 1 seconds "  + NEW_LINE

  
  return sql
  

# COMMAND ----------

def GenerateInsertRecordSQL(audit_table, main_table, business_key, delta_column, start_counter, end_counter):
  
  #Get and empty data frame from the target Delta Table
  #We need this to generate the list of columns for building our dynamic SQL
  query = "SELECT * FROM " + ADS_DATABASE_TRUSTED + "." + main_table + " LIMIT 0"
  df_col_list = spark.sql(query)
  
  alias_main = "main"
  alias_deleted = "deleted"

  source_query = GenerateSourceQuery(audit_table, business_key, delta_column, start_counter)
  join_condition = _GetSQLJoinConditionFromColumnNames(business_key, alias_main, alias_deleted, "=", " AND ")
  
  query = "SELECT * FROM " + ADS_DATABASE_TRUSTED + "." + main_table + " LIMIT 0"
  df_col_list = spark.sql(query)
  col_list = df_col_list.columns
  col_list.remove(COL_DL_TRUSTED_LOAD)
  col_list.remove(COL_RECORD_START)
  col_list.remove(COL_RECORD_END)
  col_list.remove(COL_RECORD_DELETED)
  col_list.remove(COL_RECORD_CURRENT)
  #col_list = ["`" + col + "`" for col in col_list]
  col_list = [col for col in col_list]
  sql_col = ", ".join(col_list)

  col_list = [alias_main + "." + col for col in col_list]
  sql_val = ", ".join(col_list)
  
  curr_time = datetime.now()
  curr_time = str(curr_time.strftime("%Y-%m-%d %H:%M:%S"))

  #COL_DL_TRUSTED_LOAD
  sql_val += ", from_utc_timestamp(to_timestamp('" + curr_time + "'), '" + ADS_TZ_LOCAL + "')" 
  #COL_RECORD_START
  sql_val += ", " + alias_deleted + "." + delta_column 
  #COL_RECORD_END
  sql_val += ", timestamp('" + "9999-12-31 00:00:00" + "')" 
  #COL_RECORD_DELETED
  sql_val += ", 1"
  #COL_RECORD_CURRENT
  sql_val += ", 1"

  sql = "WITH Source AS (" + NEW_LINE
  sql += source_query
  sql += ")" + NEW_LINE
  sql += "INSERT INTO " + ADS_DATABASE_TRUSTED + "." + main_table + NEW_LINE
  sql += "SELECT " + NEW_LINE
  sql += sql_val + NEW_LINE
  sql += "FROM " + ADS_DATABASE_TRUSTED + "." + main_table + " " + alias_main + NEW_LINE
  sql += "INNER JOIN Source " + alias_deleted + NEW_LINE
  sql += "ON " + join_condition
  sql += "AND " + alias_deleted + "." + COL_RECORD_VERSION + " = 1" + NEW_LINE
  sql += "AND " + alias_main + "." + COL_RECORD_CURRENT + " = 1" + NEW_LINE
  sql += "AND " + alias_main + "." + COL_RECORD_DELETED + " = 0" + NEW_LINE
  sql += "AND " + alias_deleted + "." + delta_column + " > " + alias_main + "." + COL_RECORD_START
  
  return sql



# COMMAND ----------

def ExecuteSQL(sql):
  print(sql)
  spark.sql(sql)

# COMMAND ----------

#Insert the deleted record first
sql = GenerateInsertRecordSQL(audit_table, main_table, business_key, delta_column, start_counter, end_counter)
ExecuteSQL(sql)

# COMMAND ----------

#Then Expire the current record
expire_records_sql = GenerateExpireRecordSQL(audit_table, main_table, business_key, delta_column, start_counter, end_counter)
ExecuteSQL(expire_records_sql)


# COMMAND ----------

dbutils.notebook.exit("1")
