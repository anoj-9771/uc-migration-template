# Databricks notebook source
# MAGIC %run /MDP-Framework/ControlDB/common-controldb

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

df = DataFrameFromFilePath("/mnt/datalake-landing/hydstra")
df.display()

# COMMAND ----------

sqlBase = """
    with _Base as (SELECT 'hydstra' SourceSchema, '' SourceKeyVaultSecret, 'skip-load' SourceHandler, 'csv' RawFileExtension, 'raw-load-autoloader' RawHandler, '{"separator":"|"}' ExtendedProperties, 'cleansed-load-hydstra' CleansedHandler, '' WatermarkColumn, 1 Enabled) 
    select '' SourceQuery, '' SourceTableName, * from _base where 1 = 0
    """
sqlLines = ""

for i in df.collect():
    fileName = i.name.replace("/","").replace("Hydstra","")
    folderPath = i.path.split("dbfs:")[1]
    sqlLines += f"UNION ALL select '{folderPath}' SourceQuery, '{fileName}' SourceTableName, * from _Base "

print(sqlBase + sqlLines)

df = spark.sql(sqlBase + sqlLines)
df.display()

dfHydstraRef = df.where("SourceTableName = 'MetaData'")

dfHydstraData15Min = df.where("SourceTableName = 'TSV_Provisional'")

dfHydstraData = df.where("SourceTableName NOT IN ('MetaData','TSV_Provisional')")

# COMMAND ----------

def ConfigureManifest(df):
    # ------------- CONSTRUCT QUERY ----------------- #
    
    # ------------- DISPLAY ----------------- #
    ShowQuery(df)

    # ------------- SAVE ----------------- #
    AddIngestion(df)
    
    # ------------- ShowConfig ----------------- #
    ShowConfig()

SYSTEM_CODE = 'hydstraRef'
ConfigureManifest(dfHydstraRef)

SYSTEM_CODE = 'hydstra|15Min'
ConfigureManifest(dfHydstraData15Min)

SYSTEM_CODE = "hydstraData"
ConfigureManifest(dfHydstraData)

# COMMAND ----------

ExecuteStatement("""
update dbo.extractLoadManifest set businessKeyColumn = case sourceTableName
when 'MetaData' then 'metaDataType,referenceCd'
when 'GaugeDetails' then 'gaugeIdentifier'
when 'TSV_Provisional' then 'gaugeId,variableNameUnit,measurementResultDateTime'
when 'TSV_Verified' then 'gaugeId,variableNameUnit,measurementResultDateTime'
end
where systemCode in ('hydstraRef','hydstraData','hydstra|15Min')
""")

# COMMAND ----------

ExecuteStatement("""
update dbo.extractLoadManifest set ExtendedProperties = case sourceTableName
when 'TSV_Provisional' then '{"separator":"|","GroupOrderBy":"_InputFileName ASC"}'
end
where systemCode in ('hydstra|15Min')
""")
