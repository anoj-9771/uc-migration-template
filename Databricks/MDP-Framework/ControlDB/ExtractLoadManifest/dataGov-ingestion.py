# Databricks notebook source
# MAGIC %run /MDP-Framework/ControlDB/common-controldb

# COMMAND ----------

SYSTEM_CODE = 'datagov'

# COMMAND ----------

df = spark.sql("""
    SELECT 'datagov' SourceSchema, 'AustraliaPublicHolidays' SourceTableName, 'https://data.gov.au/data/dataset/b1bc6077-dadd-4f61-9f8c-002ab2cdff10/resource/33673aca-0857-42e5-b8f0-9981b4755686/download/australian-public-holidays-combined-2021-2023_33673aca-0857-42e5-b8f0-9981b4755686-updated.csv' SourceQuery, 'http-binary-load' SourceHandler, 'date' BusinessKeyColumn, 'raw-load-delta' RawHandler, 'cleansed-load-delta' CleansedHandler, '' ExtendedProperties, '' WatermarkColumn, 'csv' RawFileExtension, '' SourceKeyVaultSecret
    """)

# COMMAND ----------

display(df)

# COMMAND ----------

def ConfigureManifest(df):
    # ------------- CONSTRUCT QUERY ----------------- #
    
    # ------------- DISPLAY ----------------- #
    #ShowQuery(df)

    # ------------- SAVE ----------------- #
    AddIngestion(df)
    
    ExecuteStatement("""
    update dbo.extractLoadManifest
    set ExtendedProperties = '{"GroupOrderBy" : "Id Desc", "CleansedQuery" : "select * from raw.datagov_australiapublicholidays where upper(jurisdiction) like ''%NSW%''"}',
    BusinessKeyColumn = 'date'
    where SystemCode = 'datagov'
    """)
    
    # ------------- ShowConfig ----------------- #
    ShowConfig()
    

ConfigureManifest(df)       
