// Databricks notebook source
// MAGIC %run ./../include-all-util

// COMMAND ----------

dbutils.widgets.removeAll()
//Define widgets at the beginning
dbutils.widgets.text("p_delta_table", "", "Delta Table")
dbutils.widgets.text("p_sql_schema_name", "", "Schema")
dbutils.widgets.text("p_sql_dw_table", "", "SQLDW Table")
dbutils.widgets.text("p_data_load_mode", "", "Mode")
dbutils.widgets.text("p_start_counter", "", "Start Counter")
dbutils.widgets.text("p_additional_property", "", "Additional Propery")


// COMMAND ----------

//Get the value of widgets in the Scala function
val p_delta_table = dbutils.widgets.get("p_delta_table")
val p_sql_schema_name = dbutils.widgets.get("p_sql_schema_name")
val p_sql_dw_table = dbutils.widgets.get("p_sql_dw_table")
val p_data_load_mode = dbutils.widgets.get("p_data_load_mode")
val p_start_counter = dbutils.widgets.get("p_start_counter")
val p_additional_property = dbutils.widgets.get("p_additional_property")


// COMMAND ----------

var source_object = p_delta_table.split("\\.")(1)

// COMMAND ----------

//Set schema name
var schema_name = ""
if (p_data_load_mode == ADS_WRITE_MODE_OVERWRITE) {
  schema_name = p_sql_schema_name
}
else {
  schema_name = ADS_SQL_SCHEMA_STAGE
}

// COMMAND ----------

//Create schema in dw if not exist
ScalaCreateSchema(schema_name)

// COMMAND ----------

//Create an empty table before writing the data to Stage Table
//We do this as DataBricks creates column with default types and sizes. The next step is to alter the table based on the source data type.
//Thus it is easier to have the data type updated before loading any data for performance
var query = s"SELECT * FROM $p_delta_table LIMIT 0"
var df_col_names = spark.sql(query)
ScalaLoadDataToAzSynDB(df_col_names, schema_name, p_sql_dw_table, ADS_WRITE_MODE_OVERWRITE)

// COMMAND ----------

//Generating query for delta extract to dw staging 
var query = ""
println("Delta Extract to staging")
//If it is delta extract get all the records that has not been processed to SQLDW
query = s"select * from $p_delta_table where _RecordStart > to_timestamp('$p_start_counter')"

println (query)

// COMMAND ----------

// Load to Synapse Staging table
var df = spark.sql(query)
ScalaLoadDataToAzSynDB (df, schema_name, p_sql_dw_table, ADS_WRITE_MODE_APPEND)

// COMMAND ----------

// Load to Synapse main table
if (p_data_load_mode != ADS_WRITE_MODE_OVERWRITE) {
  ScalaLoadDataToDW(p_sql_dw_table, p_sql_schema_name)
}

// COMMAND ----------


