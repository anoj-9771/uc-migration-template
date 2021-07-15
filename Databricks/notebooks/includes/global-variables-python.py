# Databricks notebook source
# MAGIC %run ./global-variables-config

# COMMAND ----------

#Please ensure the name of the scope matches with the scope name created in the environment
ADS_KV_ACCOUNT_SCOPE = "ADS"


# COMMAND ----------

ADS_DATABASE_NAME = "ControlDB"
ADS_DATABASE_USERNAME = "sqladmin"
ADS_KV_DB_PWD_SECRET_KEY = "AzureSQLServerPw"

# COMMAND ----------

ADS_LOAD_SYNAPSE = False
ADS_LOAD_SQLDB = True

# COMMAND ----------

#The resource names are automatically built based on Project Prefix and Environment
ADS_DATA_LAKE_ACCOUNT = "sadafdev01" #f"{ADS_COMPANY_INITIAL.replace('-', '')}dlsdp{ADS_ENVIRONMENT.lower()}{ADS_SUFFIX.replace('-', '')}"
ADS_BLOB_STORAGE_ACCOUNT = "saswcnonprod01landingdev" #ADS_COMPANY_INITIAL + "stor" + ADS_BUSINESS_UNIT + ADS_ENVIRONMENT + ".blob.core.windows.net"

ADS_DB_SERVER = "sql-swcnonprod01-daf-dev-01.database.windows.net" #ADS_COMPANY_INITIAL + "-sql-dp-" + ADS_ENVIRONMENT + "-" + ADS_SUFFIX + ".database.windows.net"

ADS_RESOURCE_GROUP =  "rg-swcnonprod01-daf-dev-01" #f"RG-{ADS_ENVIRONMENT.upper()}-SYD-AMA-PANEL-BI"

# COMMAND ----------

#Data Lake Containers
ADS_CONTAINER_RAW = "raw"
ADS_CONTAINER_CLEANSED = "cleansed"
ADS_CONTAINER_STAGE = "stage"
ADS_CONTAINER_CURATED = "curated"
ADS_CONTAINER_EXTERNAL = "external"
ADS_CONTAINER_STAGE = "stage"


# COMMAND ----------

#Delta Lake Databases
ADS_DATABASE_RAW = "raw"
ADS_DATABASE_CLEANSED = "cleansed"
ADS_DATABASE_STAGE = "stage"
ADS_DATABASE_CURATED = "curated"

ADS_DATALAKE_ZONE_RAW = "raw"
ADS_DATALAKE_ZONE_CLEANSED = "cleansed"
ADS_DATALAKE_ZONE_CURATED = "curated"

#SQL Schema
ADS_SQL_SCHEMA_RAW = "raw"
ADS_SQL_SCHEMA_CLEANSED = "edw"
ADS_SQL_SCHEMA_STAGE = "stage"

ADS_TARGET_DELTA_TABLE = "DELTA"
ADS_TARGET_SQL_SERVER = "SQLSERVER"

# COMMAND ----------

ADS_LOG_VERBOSE = True

# COMMAND ----------

ADS_SECRET_APP_ID = "SERVICE-PRINCIPAL-APP-ID"
ADS_SECRET_APP_SECRET = "SERVICE-PRINCIPAL-SECRET"
ADS_TENANT_ID = "TENANT-ID"

# COMMAND ----------

ADS_WRITE_MODE_OVERWRITE = "overwrite"
ADS_WRITE_MODE_APPEND = "append"
ADS_WRITE_MODE_MERGE = "merge"

# COMMAND ----------

ADS_MYSQL_LMS_DELTA_COL_CREATED = "timecreated"
ADS_MYSQL_LMS_DELTA_COL_UPDATED = "timemodified"

# COMMAND ----------

ADS_COLUMN_CREATED = ["CREATED_DATE", "CREATEDTIME", ADS_MYSQL_LMS_DELTA_COL_CREATED]
ADS_COLUMN_UPDATED = ["UPDATED_DATE", "UPDATEDTIME", ADS_MYSQL_LMS_DELTA_COL_UPDATED]
ADS_COLUMN_TRANSACTION_DT = "_transaction_date"
ADS_TZ_LOCAL = "Australia/Brisbane"

# COMMAND ----------

COL_RECORD_VERSION = "_RecordVersion"
COL_RECORD_START = "_RecordStart"
COL_RECORD_END = "_RecordEnd"
COL_RECORD_CURRENT = "_RecordCurrent"
COL_RECORD_DELETED = "_RecordDeleted"
COL_DL_RAW_LOAD = "_DLRawZoneTimeStamp"
COL_DL_CLEANSED_LOAD = "_DLCleansedZoneTimeStamp"
COL_DL_CURATED_LOAD = "_DLCuratedZoneTimeStamp"
COL_ONEEBS_UPDATED_TIMESTAMP = "_ONEEBS_UPDATED_TRANSACTION_DATE"

# COMMAND ----------

PARAMS_TRACK_CHANGES = "TrackChanges"
PARAMS_DELTA_EXTRACT = "DeltaExtract"
PARAMS_BUSINESS_KEY_COLUMN = "BusinessKeyColumn"
PARAMS_TRUNCATE_TARGET = "TruncateTarget"
PARAMS_UPSERT_TARGET = "UpsertTarget"
PARAMS_APPEND_TARGET = "AppendTarget"
PARAMS_CDC_SOURCE = "CDCSource"
PARAMS_WATERMARK_COLUMN = "WatermarkColumn"
PARAMS_SOURCE_TYPE = "SourceType"
PARAMS_SOURCE_TS_FORMAT = "SourceTimeStampFormat"
PARAMS_ADDITIONAL_PROPERTY = "AdditionalProperty"
PARAMS_UPDATE_METADADTA = "UpdateMetaData"
PARAMS_SOURCE_GROUP = "SourceGroup"
PARAMS_SOURCE_NAME = "SourceName"

