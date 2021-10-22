// Databricks notebook source
// MAGIC %run ./global-variables-config

// COMMAND ----------

//Please ensure the name of the scope matches with the scope name created in the environment
val ADS_KV_ACCOUNT_SCOPE = "ADS"

// COMMAND ----------

val ADS_SQL_SCHEMA_RAW = "raw"
val ADS_SQL_SCHEMA_CLEANSED = "cleansed"
val ADS_SQL_SCHEMA_STAGE = "stage"

// COMMAND ----------

val ADS_DATABASE_RAW = "raw"
val ADS_DATABASE_CLEANSED = "cleansed"
val ADS_DATABASE_STAGE = "stage"
val ADS_DATABASE_CURATED = "curated"

// COMMAND ----------

val ADS_DB_SERVER = "sql-swcnonprod01-daf-dev-01.database.windows.net" //ADS_COMPANY_INITIAL + "-sql-dp-" + ADS_ENVIRONMENT + "-" + ADS_SUFFIX + ".database.windows.net"

// COMMAND ----------

val ADS_DATA_LAKE_ACCOUNT = "sadaf"+ ADS_ENVIRONMENT + "01" //f"{ADS_COMPANY_INITIAL.replace('-', '')}dlsdp{ADS_ENVIRONMENT.lower()}{ADS_SUFFIX.replace('-', '')}"

// COMMAND ----------

val ADS_DATABASE_NAME = "syndw"+ ADS_ENVIRONMENT + "01"
val ADS_DATABASE_USERNAME = "svc_synapse1"
val ADS_KV_DB_PWD_SECRET_KEY = "daf-syn-d-sqlpool-password"

// COMMAND ----------

//Synapse settings
val ADS_SYN_DATABASE_NAME = "syndw"+ ADS_ENVIRONMENT + "01"
val ADS_SYN_DATABASE_USERNAME = "svc_synapse1"
val ADS_KV_SYN_DB_PWD_SECRET_KEY = "daf-syn-d-sqlpool-password"

val ADS_SYNAPSE_DB_SERVER = "synws-swcnonprod01-daf-"+ ADS_ENVIRONMENT + "-01" 

// COMMAND ----------

val ADS_COLUMN_TRANSACTION_DT = "_transaction_date"

val COL_RECORD_START = "_RecordStart"
val COL_RECORD_END = "_RecordEnd"
val COL_DL_RAW_LOAD = "_DLRawZoneTimeStamp"
val COL_DL_CLEANSED_LOAD = "_DLCleansedZoneTimeStamp"

// COMMAND ----------

val ADS_WRITE_MODE_OVERWRITE = "overwrite"
val ADS_WRITE_MODE_APPEND = "append"
val ADS_WRITE_MODE_MERGE = "merge"
