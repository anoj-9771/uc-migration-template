# Databricks notebook source
# MAGIC %run ../common-controldb

# COMMAND ----------

df = spark.sql("""
WITH _Base AS 
(
    SELECT 'SharepointList' SystemCode, 'SharepointList' SourceSchema, '' SourceKeyVaultSecret, 'sharepointlist-load' SourceHandler, 'raw-load-delta' RawHandler, 'cleansed-load-delta' CleansedHandler, '' RawFileExtension, '' WatermarkColumn
    ,'' ExtendedProperties
    ,'https://sydneywatercorporation-my.sharepoint.com/personal/63p_sydneywater_com_au' SourceQuery
)
SELECT 'SharePointListDesign' SourceTableName, * FROM _Base
""")
display(df)

# COMMAND ----------

def ConfigureManifest(dataFrameConfig):
    # ------------- CONSTRUCT QUERY ----------------- #
    # ------------- DISPLAY ----------------- #
    ShowQuery(df)

    # ------------- SAVE ----------------- #
    AddIngestion(df, True)
    
    # ------------- ShowConfig ----------------- #
    ShowConfig()

for system_code in ['SharepointList']:
    SYSTEM_CODE = system_code
    print(SYSTEM_CODE)
    ConfigureManifest(df.where(f"SystemCode = '{SYSTEM_CODE}'"))
    
# #ADD BUSINESS KEY
# ExecuteStatement("""
# update dbo.extractLoadManifest set
# businessKeyColumn = ''
# where systemCode in ('SharepointList')
# """)

# #Remove cleansed handler
# ExecuteStatement("""
# update dbo.extractLoadManifest set
# CleansedHandler = null
# where systemCode in ('SharepointList')
# """)      
