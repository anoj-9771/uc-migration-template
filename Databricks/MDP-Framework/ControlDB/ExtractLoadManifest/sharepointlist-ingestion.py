# Databricks notebook source
# MAGIC %run ../common-controldb

# COMMAND ----------

from pyspark.sql.functions import lit, when, lower, expr
df = spark.sql("""
WITH _Base AS 
(
    SELECT 'SharepointListdata' SystemCode, 'SharepointListEDP' SourceSchema, '' SourceKeyVaultSecret, 'sharepointlist-load' SourceHandler, 'raw-load-delta' RawHandler, 'cleansed-load-delta' CleansedHandler, '' RawFileExtension, 'https://sydneywatercorporation.sharepoint.com/sites/EnterpriseDataPlatform-Adjustments' SourceQuery
)
SELECT 'DemandCalculationAdjustment' SourceTableName, "Modified" WatermarkColumn,'{"CleansedQuery" : "select * except(dummy) from {tableFqn} where Id is not null and _DLRawZoneTimeStamp > \\'\\'{lastLoadTimeStamp}\\'\\'"}'  ExtendedProperties, * FROM _Base
UNION ALL
SELECT 'AggregatedComponents' SourceTableName, '' WatermarkColumn, '' ExtendedProperties, * FROM _Base
UNION ALL
(
WITH _Base AS 
(
    SELECT 'SharepointListref' SystemCode, 'SharepointListEDP' SourceSchema, '' SourceKeyVaultSecret, 'sharepointlist-load' SourceHandler, 'raw-load-delta' RawHandler, 'cleansed-load-delta' CleansedHandler, '' RawFileExtension, '' WatermarkColumn
    ,'{"CleansedQuery" : "select * except(dummy) from {tableFqn} where Id is not null and _DLRawZoneTimeStamp > \\'\\'{lastLoadTimeStamp}\\'\\'"}'  ExtendedProperties
    ,'https://sydneywatercorporation.sharepoint.com/sites/EnterpriseDataPlatform-Adjustments' SourceQuery
)
SELECT 'UserInformationList' SourceTableName, * FROM _Base
)
""")
appendTables = ['DemandCalculationAdjustment']
df = (
    df.withColumn('ExtendedProperties', expr('trim("{}" FROM ExtendedProperties)'))
      .withColumn('ExtendedProperties', when(df.SourceTableName.isin(appendTables)
                                        ,expr('ltrim(",",ExtendedProperties ||", "||\'\"LoadType\" : \"Append\"\')')) 
                                        .otherwise(expr('ExtendedProperties')))
      .withColumn('ExtendedProperties', expr('if(ExtendedProperties<>"","{"||ExtendedProperties ||"}","")')) 
)
display(df)

# COMMAND ----------

def ConfigureManifest(df):
    # ------------- CONSTRUCT QUERY ----------------- #
    # ------------- DISPLAY ----------------- #
    ShowQuery(df)

    # ------------- SAVE ----------------- #
    AddIngestion(df)
    
    # ------------- ShowConfig ----------------- #
    ShowConfig()

for system_code in ['SharepointListref','SharepointListdata']:
    SYSTEM_CODE = system_code
    print(SYSTEM_CODE)
    ConfigureManifest(df.where(f"SystemCode = '{SYSTEM_CODE}'"))
    
# #ADD BUSINESS KEY
# ExecuteStatement("""
# update dbo.extractLoadManifest set
# businessKeyColumn = 'ID'
# where systemCode in ('SharepointList')
# """)

# #Remove cleansed handler
# ExecuteStatement("""
# update dbo.extractLoadManifest set
# CleansedHandler = null
# where systemCode in ('SharepointList')
# """)      
