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
        
    for i in df.collect():
        ExecuteStatement(f"""    
        DECLARE @RC int
        DECLARE @SystemCode varchar(max) = NULLIF('{i.SystemCode}','')
        DECLARE @Schema varchar(max) = NULLIF('{i.SourceSchema}','')
        DECLARE @Table varchar(max) = NULLIF('{i.SourceTableName}','')
        DECLARE @Query varchar(max) = NULLIF('{i.SourceQuery}','')
        DECLARE @WatermarkColumn varchar(max) = NULLIF('{i.WatermarkColumn}','')
        DECLARE @SourceHandler varchar(max) = NULLIF('{i.SourceHandler}','')
        DECLARE @RawFileExtension varchar(max) = NULLIF('{i.RawFileExtension}','')
        DECLARE @KeyVaultSecret varchar(max) = NULLIF('{i.SourceKeyVaultSecret}','')
        DECLARE @ExtendedProperties varchar(max) = NULLIF('{i.ExtendedProperties}','')
        DECLARE @RawHandler varchar(max) = NULLIF('{i.RawHandler}','') 
        DECLARE @CleansedHandler varchar(max) = NULLIF('{i.CleansedHandler}','')    

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
          ,@CleansedHandler
        """)   

# COMMAND ----------

def ShowConfig():
    print(f"Showing {SYSTEM_CODE} from ExtractLoadManifest...")
    display(
        spark.table("controldb.dbo_extractloadmanifest")
        .where(f"LOWER(SystemCode) = LOWER('{SYSTEM_CODE}')")
    )

# COMMAND ----------

def ViewStoredProcedureDefinition(storedProcedureName):
    print(RunQuery(f"SELECT OBJECT_DEFINITION (OBJECT_ID(N'{storedProcedureName}')) Definition ").collect()[0][0])

# COMMAND ----------

def LogMessage(activityType, message):
    ExecuteStatement(f"EXEC LogMessage NULL, 0, '{activityType}', '{message}'")
