# Databricks notebook source
# MAGIC %run /MDP-Framework/ControlDB/common-controldb

# COMMAND ----------

SYSTEM_CODE = "IoT"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType
from pyspark.sql.functions import col, expr, when, desc

fsSchema = StructType([
    StructField('path', StringType()),
    StructField('name', StringType()),
    StructField('size', LongType()),
    StructField('modificationTime', LongType())
])

def DataFrameFromFilePath(path):
    list = dbutils.fs.ls(path)
    df = spark.createDataFrame(list, fsSchema).withColumn("modificationTime", expr("from_unixtime(modificationTime / 1000)"))
    return df

dfContainer1 = DataFrameFromFilePath("/mnt/blob-iotsewertelemetrydata")
dfContainer1.display()

dfContainer2 = DataFrameFromFilePath("/mnt/blob-iotswtelemetryalarmdata")
dfContainer2.display()

# COMMAND ----------

def ConfigureManifest(df):
    # ------------- CONSTRUCT QUERY ----------------- #
    
    # ------------- DISPLAY ----------------- #
    ShowQuery(df)

    # ------------- SAVE ----------------- #
    AddIngestion(df)
    
    # ------------- ShowConfig ----------------- #
    ShowConfig()

# COMMAND ----------

sqlBase = f"""
    with _Base as (SELECT '{SYSTEM_CODE}' SourceSchema, '' SourceKeyVaultSecret, 'skip-load' SourceHandler, 'json' RawFileExtension, 'raw-load-autoloader' RawHandler, '' ExtendedProperties, '' CleansedHandler, '' WatermarkColumn, 1 Enabled) 
    select '' SourceQuery, '' SourceTableName, * from _base where 1 = 0
    """
sqlLines = ""

for i in dfContainer1.rdd.collect():
    fileName = i.name.replace("/","")
    folderPath = i.path.split("dbfs:")[1]
    sqlLines += f"UNION ALL select '{folderPath}' SourceQuery, '{fileName}' SourceTableName, * from _Base "

for i in dfContainer2.rdd.collect():
    fileName = i.name.replace("/","")
    folderPath = i.path.split("dbfs:")[1]
    sqlLines += f"UNION ALL select '{folderPath}' SourceQuery, '{fileName}' SourceTableName, * from _Base "    
    
print(sqlBase + sqlLines)

df = spark.sql(sqlBase + sqlLines)
df.display()

ConfigureManifest(df)

# COMMAND ----------

SYSTEM_CODE = "IoT_DV"

df2 = spark.sql(f"""
    WITH _Base AS 
    (
      SELECT '{SYSTEM_CODE}' SystemCode, '{SYSTEM_CODE}' SourceSchema, '' SourceKeyVaultSecret, 'dataverse-load' SourceHandler, 'json' RawFileExtension, 'raw-load-dataverse' RawHandler, '' ExtendedProperties, 'cleansed-load-delta' CleansedHandler
    )
    SELECT 'new_device' SourceTableName, "modifiedon" WatermarkColumn, '' SourceQuery, * FROM _Base
    UNION ALL
    SELECT 'cr606_devicedesiredproperty' SourceTableName, "modifiedon" WatermarkColumn, '' SourceQuery, * FROM _Base
    UNION ALL
    SELECT 'cr606_devicereportedproperty' SourceTableName, "modifiedon" WatermarkColumn, '' SourceQuery, * FROM _Base
    UNION ALL
    SELECT 'new_devicetype' SourceTableName, "modifiedon" WatermarkColumn, '' SourceQuery, * FROM _Base
    UNION ALL
    SELECT 'new_template' SourceTableName, "modifiedon" WatermarkColumn, '' SourceQuery, * FROM _Base
    UNION ALL
    SELECT 'new_desiredproperty' SourceTableName, "modifiedon" WatermarkColumn, '' SourceQuery, * FROM _Base
    UNION ALL
    SELECT 'cr606_devicefirmware' SourceTableName, "modifiedon" WatermarkColumn, '' SourceQuery, * FROM _Base
    UNION ALL
    SELECT 'cr606_iotjob' SourceTableName, "modifiedon" WatermarkColumn, '' SourceQuery, * FROM _Base
    UNION ALL
    SELECT 'new_templatedesiredproperty' SourceTableName, "modifiedon" WatermarkColumn, '' SourceQuery, * FROM _Base
    UNION ALL
    SELECT 'cr606_jobtemplateproperty' SourceTableName, "modifiedon" WatermarkColumn, '' SourceQuery, * FROM _Base
    UNION ALL
    SELECT 'cr606_point' SourceTableName, "modifiedon" WatermarkColumn, '' SourceQuery, * FROM _Base
    UNION ALL
    SELECT 'cr606_devicegroup' SourceTableName, "modifiedon" WatermarkColumn, '' SourceQuery, * FROM _Base
    UNION ALL
    SELECT 'cr606_installationreport' SourceTableName, "modifiedon" WatermarkColumn, '' SourceQuery, * FROM _Base
    UNION ALL
    SELECT 'cr606_installationreportattachment' SourceTableName, "modifiedon" WatermarkColumn, '' SourceQuery, * FROM _Base
    UNION ALL
    SELECT 'cr606_jobdevice' SourceTableName, "modifiedon" WatermarkColumn, '' SourceQuery, * FROM _Base
    UNION ALL
    SELECT 'cr606_jobfotadevice' SourceTableName, "modifiedon" WatermarkColumn, '' SourceQuery, * FROM _Base
    UNION ALL
    SELECT 'cr606_iotconfiguration' SourceTableName, "modifiedon" WatermarkColumn, '' SourceQuery, * FROM _Base
    UNION ALL
    SELECT 'cr606_wetweatherregionupdate' SourceTableName, "modifiedon" WatermarkColumn, '' SourceQuery, * FROM _Base
    UNION ALL
    SELECT 'cr606_iotevent' SourceTableName, "modifiedon" WatermarkColumn, '' SourceQuery, * FROM _Base
    UNION ALL
    SELECT 'cr606_alarmaudit' SourceTableName, "modifiedon" WatermarkColumn, '' SourceQuery, * FROM _Base
    """)

ConfigureManifest(df2)  

# COMMAND ----------

ExecuteStatement(f"""
update dbo.extractLoadManifest set DestinationTableName = case sourceTableName
    when 'new_device' then 'Device'
    when 'cr606_devicedesiredproperty' then 'DeviceDesiredProperty'
    when 'cr606_devicereportedproperty' then 'DeviceReportedProperty'
    when 'new_devicetype' then 'DeviceType'
    when 'new_template' then 'Template'
    when 'new_desiredproperty' then 'DesiredProperty'
    when 'cr606_devicefirmware' then 'DeviceFirmware'
    when 'cr606_iotjob' then 'IoTJob'
    when 'new_templatedesiredproperty' then 'TemplateDesiredProperty'
    when 'cr606_jobtemplateproperty' then 'JobTemplateProperty'
    when 'cr606_point' then 'Point'
    when 'cr606_devicegroup' then 'DeviceGroup'
    when 'cr606_installationreport' then 'InstallationReport'
    when 'cr606_installationreportattachment' then 'InstallReportAttachment'
    when 'cr606_jobdevice' then 'JobDevice'
    when 'cr606_jobfotadevice' then 'JobFotaDevice'
    when 'cr606_iotconfiguration' then 'IoTConfiguration'
    when 'cr606_wetweatherregionupdate' then 'WetWeatherRegionUpdate'
    when 'cr606_iotevent' then 'IoTEvent'
    when 'cr606_alarmaudit' then 'AlarmAudit'
    end
where systemCode = 'IoT_DV'
""") 
