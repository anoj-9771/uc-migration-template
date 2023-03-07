# Databricks notebook source
# MAGIC %run /MDP-Framework/ControlDB/common-controldb

# COMMAND ----------

SYSTEM_CODE = "hydra"

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

df = DataFrameFromFilePath("/mnt/datalake-landing/hydra")
df.display()

# COMMAND ----------

sqlBase = """
    with _Base as (SELECT 'hydra' SourceSchema, '' SourceKeyVaultSecret, 'skip-load' SourceHandler, 'json' RawFileExtension, 'raw-load-autoloader' RawHandler, '' ExtendedProperties, 'cleansed-load-explode-json' CleansedHandler, '' WatermarkColumn, 1 Enabled) 
    select '' SourceQuery, '' SourceTableName, * from _base where 1 = 0
    """
sqlLines = ""

for i in df.rdd.collect():
    fileName = i.name.replace("/","")
    folderPath = i.path.split("dbfs:")[1]
    sqlLines += f"UNION ALL select '{folderPath}' SourceQuery, '{fileName}' SourceTableName, * from _Base "

print(sqlBase + sqlLines)

df = spark.sql(sqlBase + sqlLines)
df.display()

# COMMAND ----------

def ConfigureManifest(df):
    # ------------- CONSTRUCT QUERY ----------------- #
    
    # ------------- DISPLAY ----------------- #
    ShowQuery(df)

    # ------------- SAVE ----------------- #
    AddIngestion(df)
    
    # ------------- ShowConfig ----------------- #
    ShowConfig()

ConfigureManifest(df)   

# COMMAND ----------

ExecuteStatement("""
update dbo.extractLoadManifest set
extendedProperties = '{\"charset\":\"US-ASCII\"}'
where systemCode = 'hydra' and sourceTableName = 'fm_ground'
""")
