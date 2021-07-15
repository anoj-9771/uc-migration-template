// Databricks notebook source
// MAGIC %run ./global-variables-config

// COMMAND ----------

//Please ensure the name of the scope matches with the scope name created in the environment
val ADS_KV_ACCOUNT_SCOPE = "ADS"

// COMMAND ----------

val ADS_SQL_SCHEMA_RAW = "raw"
val ADS_SQL_SCHEMA_TRUSTED = "edw"
val ADS_SQL_SCHEMA_STAGE = "stage"

// COMMAND ----------

val ADS_DATABASE_RAW = "raw"
val ADS_DATABASE_TRUSTED = "trusted"
val ADS_DATABASE_STAGE = "stage"
val ADS_DATABASE_CURATED = "curated"

// COMMAND ----------

val ADS_DB_SERVER = "sql-swcnonprod01-daf-dev-01.database.windows.net" //ADS_COMPANY_INITIAL + "-sql-dp-" + ADS_ENVIRONMENT + "-" + ADS_SUFFIX + ".database.windows.net"

// COMMAND ----------

val ADS_DATABASE_NAME = "EDW"
val ADS_DATABASE_USERNAME = "sqladmin"
val ADS_KV_DB_PWD_SECRET_KEY = "AzureSQLServerPw"

// COMMAND ----------

val ADS_COLUMN_TRANSACTION_DT = "_transaction_date"

val COL_RECORD_START = "_RecordStart"
val COL_RECORD_END = "_RecordEnd"
val COL_DL_RAW_LOAD = "_DLRawZoneTimeStamp"
val COL_DL_TRUSTED_LOAD = "_DLTrustedZoneTimeStamp"

// COMMAND ----------

val ADS_WRITE_MODE_OVERWRITE = "overwrite"
val ADS_WRITE_MODE_APPEND = "append"
val ADS_WRITE_MODE_MERGE = "merge"
