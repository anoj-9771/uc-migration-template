// Databricks notebook source
// MAGIC %run ./../include-all-util

// COMMAND ----------

dbutils.widgets.removeAll()
//Define widgets at the beginning
dbutils.widgets.text("p_delta_table", "", "Delta Table")
dbutils.widgets.text("p_sql_schema_name", "", "Schema")
dbutils.widgets.text("p_sql_edw_table", "", "SQLEDW Table")
dbutils.widgets.text("p_data_load_mode", "", "Mode")
dbutils.widgets.text("p_schema_file_url", "", "Schema File URL")
dbutils.widgets.text("p_delta_column", "", "Delta Column")
dbutils.widgets.text("p_start_counter", "", "Start Counter")
dbutils.widgets.text("p_is_delta_extract", "", "DeltaExtract")
dbutils.widgets.text("p_track_changes", "", "TrackChanges")
dbutils.widgets.text("p_additional_property", "", "Additional Propery")


// COMMAND ----------

//Get the value of widgets in the Scala function
val p_delta_table = dbutils.widgets.get("p_delta_table")
val p_sql_schema_name = dbutils.widgets.get("p_sql_schema_name")
val p_sql_edw_table = dbutils.widgets.get("p_sql_edw_table")
val p_data_load_mode = dbutils.widgets.get("p_data_load_mode")
val p_schema_file_url = dbutils.widgets.get("p_schema_file_url")
val p_delta_column = dbutils.widgets.get("p_delta_column")
val p_start_counter = dbutils.widgets.get("p_start_counter")
val p_is_delta_extract = dbutils.widgets.get("p_is_delta_extract")
val p_track_changes = dbutils.widgets.get("p_track_changes")
val p_additional_property = dbutils.widgets.get("p_additional_property")


// COMMAND ----------

var source_object = p_delta_table.split("\\.")(1)

// COMMAND ----------

val is_delta_extract = GeneralGetBoolFromStringScala(p_is_delta_extract)
val track_changes = GeneralGetBoolFromStringScala(p_track_changes)

// COMMAND ----------

var schema_name = ""
if (p_data_load_mode == ADS_WRITE_MODE_OVERWRITE) {
  schema_name = p_sql_schema_name
}
else {
  schema_name = ADS_SQL_SCHEMA_STAGE
}

// COMMAND ----------

ScalaCreateSchema(schema_name)

// COMMAND ----------

//Create an empty table before writing the data to Stage Table
//We do this as DataBricks creates column with default types and sizes. The next step is to alter the table based on the source data type.
//Thus it is easier to have the data type updated before loading any data for performance
var query = s"SELECT * FROM $p_delta_table LIMIT 0"
var df_col_names = spark.sql(query)
ScalaLoadDataToAzSqlDB(df_col_names, schema_name, p_sql_edw_table, ADS_WRITE_MODE_OVERWRITE)

// COMMAND ----------

if (p_schema_file_url != "") {
  var sql_table = s"$schema_name.$p_sql_edw_table"
  ScalaAlterTableDataTypeFromSchema(p_schema_file_url, source_object, sql_table, p_delta_column, p_additional_property)
}

// COMMAND ----------

var query = ""
if (is_delta_extract){
  if (track_changes) {
    println("Tracking Changes with Delta Extract")
    //If it is delta extract get all the records that has not been processed to SQLEDW
    query = s"select * from $p_delta_table where ((_RecordCurrent = 0 and _RecordEnd >= to_timestamp('$p_start_counter')) or (_RecordCurrent = 1 and _RecordStart > to_timestamp('$p_start_counter')))"
  }
  else {
    var delta_column_updated = ""
    //If it is Delta extract and delta column is missing then the extract is from text source. Update the delta column to Raw Zone timestamp
    if (p_delta_column == "") delta_column_updated = COL_DL_RAW_LOAD else delta_column_updated = p_delta_column
    println("Delta Extract without tracking changes")
    query = s"select * from $p_delta_table where $delta_column_updated >= to_timestamp('$p_start_counter')"
  }
  
}
else {
  println("Full Extraction")
  query = s"select * from $p_delta_table"
}

println (query)

// COMMAND ----------

var df = spark.sql(query)
ScalaLoadDataToAzSqlDB (df, schema_name, p_sql_edw_table, ADS_WRITE_MODE_APPEND)

// COMMAND ----------

if (p_data_load_mode != ADS_WRITE_MODE_OVERWRITE) {
  ScalaLoadDataToEDW(p_sql_edw_table, p_sql_schema_name)
}