# Databricks notebook source
# MAGIC %run ./global-variables-config

# COMMAND ----------

#Please ensure the name of the scope matches with the scope name created in the environment
ADS_KV_ACCOUNT_SCOPE = "ADS"

# COMMAND ----------

def is_uc():
    """check if the current databricks environemnt is Unity Catalog enabled"""
    try:
        dbutils.secrets.get('ADS', 'databricks-env')
        return True
    except Exception as e:
        return False

# COMMAND ----------

if is_uc():
    ADS_DATABRICKS_ENV = dbutils.secrets.get('ADS', 'databricks-env')
else:
    ADS_DATABRICKS_ENV = ''

# COMMAND ----------

ADS_DATABASE_NAME = "ControlDB"
ADS_DATABASE_USERNAME = "sqladmin"
ADS_KV_DB_PWD_SECRET_KEY = "AzureSQLServerPw"

# COMMAND ----------

ADS_LOAD_SYNAPSE = True
ADS_LOAD_SQLDB = True

# COMMAND ----------

#The resource names are automatically built based on Project Prefix and Environment
ADS_DATA_LAKE_ACCOUNT = "sadaf"+ ADS_ENVIRONMENT + "01"
ADS_BLOB_STORAGE_ACCOUNT = "sablobdaf" + ADS_ENVIRONMENT + "01"
ADS_DB_SERVER = "sql-" + ADS_SUBSCRIPTION + "-daf-" + ADS_ENVIRONMENT + "-01.database.windows.net" 
ADS_RESOURCE_GROUP =  "rg-" + ADS_SUBSCRIPTION + "-daf-" + ADS_ENVIRONMENT + "-01"

# COMMAND ----------

#Synapse settings
ADS_SYN_DATABASE_NAME = "syndw" + ADS_ENVIRONMENT + "01"
ADS_SYN_DATABASE_USERNAME = "svc_synapse1"
ADS_KV_SYN_DB_PWD_SECRET_KEY = "daf-syn-d-sqlpool-password"

ADS_SYNAPSE_DB_SERVER = "synws-" + ADS_SUBSCRIPTION + "-daf-" + ADS_ENVIRONMENT + "-01" 


# COMMAND ----------

#Data Lake Containers
ADS_CONTAINER_RAW = "raw"
ADS_CONTAINER_CLEANSED = "cleansed"
ADS_CONTAINER_STAGE = "stage"
ADS_CONTAINER_CURATED = "curated"
ADS_CONTAINER_CURATED_V2 = "curated-v2"
ADS_CONTAINER_EXTERNAL = "external"
ADS_CONTAINER_REJECTED = "rejected"


# COMMAND ----------

#Delta Lake Databases
ADS_DATABASE_RAW = f"{ADS_DATABRICKS_ENV}raw"
ADS_DATABASE_CLEANSED_STAGE = "cleansed.stg"
ADS_DATABASE_CLEANSED = f"{ADS_DATABRICKS_ENV}cleansed"
ADS_DATABASE_STAGE = f"{ADS_DATABRICKS_ENV}stage"
ADS_DATABASE_CURATED_STAGE = "curated.stg"
ADS_DATABASE_CURATED = f"{ADS_DATABRICKS_ENV}curated"
ADS_DATABASE_CURATED_V2 = "curated_v2"
ADS_DATABASE_REJECTED = f"{ADS_DATABRICKS_ENV}rejected"

ADS_DATALAKE_ZONE_RAW = f"{ADS_DATABRICKS_ENV}raw"
ADS_DATALAKE_ZONE_CLEANSED = f"{ADS_DATABRICKS_ENV}cleansed"
ADS_DATALAKE_ZONE_STAGE = f"{ADS_DATABRICKS_ENV}stage"
ADS_DATALAKE_ZONE_CURATED = f"{ADS_DATABRICKS_ENV}curated"
ADS_DATALAKE_ZONE_CURATED_V2 = "curated-v2"
ADS_DATALAKE_ZONE_REJECTED = f"{ADS_DATABRICKS_ENV}rejected"

#SQL Schema
ADS_SQL_SCHEMA_RAW = "raw"
ADS_SQL_SCHEMA_CLEANSED = "dw"
ADS_SQL_SCHEMA_STAGE = "stage"

ADS_TARGET_DELTA_TABLE = "DELTA"
ADS_TARGET_SQL_SERVER = "SQLSERVER"


# COMMAND ----------

ADS_LOG_VERBOSE = True

# COMMAND ----------

ADS_SECRET_APP_ID = "daf-serviceprincipal-app-id"
ADS_SECRET_APP_SECRET = "daf-serviceprincipal-app-secret"
ADS_TENANT_ID = "daf-tenant-id"

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
ADS_TZ_LOCAL = "Australia/Sydney"

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
COL_DL_RAW_FILE_TIMESTAMP = "_FileDateTimeStamp"
COL_RECORD_BUS_KEY = "_BusinessKey"
COL_DL_REJECTED_LOAD = "_DLRejectedZoneTimeStamp"

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
