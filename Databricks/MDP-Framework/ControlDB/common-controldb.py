# Databricks notebook source
# MAGIC %run ../Common/common-jdbc

# COMMAND ----------

SYSTEM_CODE = None

# COMMAND ----------

def ShowQuery(df):
    df = AddSystemCode(df)
    display(df)

# COMMAND ----------

def AddSystemCode(df):
    if SYSTEM_CODE is None:
        raise Exception(f"SYSTEM_CODE has not been set!")

    return df.selectExpr(f"'{SYSTEM_CODE}' SystemCode", "*")

# COMMAND ----------

def CleanConfig():
    print(f"Cleaning {SYSTEM_CODE} from ExtractLoadManifest...")
    ExecuteStatement(f"""
        DELETE FROM dbo.extractloadmanifest
        WHERE SystemCode = '{SYSTEM_CODE}'
    """)

# COMMAND ----------

def AddIngestion(df, clean = False):
    df = AddSystemCode(df)
    if clean:
        CleanConfig()
        
    for i in df.rdd.collect():
        
        #cleansedHandler = i.CleansedHandler 
        cleansedHandler = 'NULL'
        
        ExecuteStatement(f"""
        DECLARE @RC int
        DECLARE @SystemCode varchar(max) = '{i.SystemCode}'
        DECLARE @Schema varchar(max) = '{i.SystemCode}'
        DECLARE @Table varchar(max) = '{i.SourceTableName}'
        DECLARE @Query varchar(max) = '{i.SourceQuery}'
        DECLARE @WatermarkColumn varchar(max) = NULL
        DECLARE @SourceHandler varchar(max) = '{i.SourceHandler}'
        DECLARE @RawFileExtension varchar(max) = '{i.RawFileExtension}'
        DECLARE @KeyVaultSecret varchar(max) = '{i.SourceKeyVaultSecret}'
        DECLARE @ExtendedProperties varchar(max) = '{i.ExtendedProperties}'
        DECLARE @RawHandler varchar(max) = '{i.RawHandler}'
        DECLARE @CleansedHandler varchar(max) = {cleansedHandler}

        EXECUTE @RC = [dbo].[AddIngestion] 
           @SystemCode
          ,@Schema
          ,@Table
          ,@Query
          ,@WatermarkColumn
          ,@SourceHandler
          ,@RawFileExtension
          ,@KeyVaultSecret
          ,@ExtendedProperties
          ,@RawHandler
          ,@RawHandler
        """)

# COMMAND ----------

def ShowConfig():
    print(f"Showing {SYSTEM_CODE} from ExtractLoadManifest...")
    display(
        spark.table("controldb.dbo_extractloadmanifest")
        .where(f"LOWER(SystemCode) = LOWER('{SYSTEM_CODE}')")
    )
