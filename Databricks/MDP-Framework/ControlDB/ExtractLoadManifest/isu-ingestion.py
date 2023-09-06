# Databricks notebook source
# MAGIC %run ../common-controldb

# COMMAND ----------

from pyspark.sql.functions import lit, when, lower, expr
df = spark.sql("""
WITH _Base AS 
(
  SELECT 'isu' SourceSchema, 'daf-sa-blob-sastoken' SourceKeyVaultSecret, 'isu' DestinationSchema, 'bods-load' SourceHandler, --'{ "DeleteSourceFiles" : "True" }' ExtendedProperties,
   '' SystemPath
)
SELECT 'isudata' SystemCode, 'ZBLT_REDRESS' SourceTableName, 'isudata/ZBLT_REDRESS' SourceQuery, '' WatermarkColumn, 'json' RawFileExtension, 'raw-load-bods' RawHandler, 'cleansed-load-bods-slt' CleansedHandler, * FROM _Base
UNION
SELECT 'isudata' SystemCode, '0UC_SALES_SIMU_01' SourceTableName, 'isudata/0UC_SALES_SIMU_01' SourceQuery, '' WatermarkColumn, 'csv' RawFileExtension, 'raw-load-delta' RawHandler, 'cleansed-load-delta' CleansedHandler, * FROM _Base
UNION
SELECT 'isudata' SystemCode, '0UC_SALES_STATS_03' SourceTableName, 'isudata/0UC_SALES_STATS_03' SourceQuery, '' WatermarkColumn, 'csv' RawFileExtension, 'raw-load-delta' RawHandler, 'cleansed-load-delta' CleansedHandler, * FROM _Base
UNION
SELECT 'isuref' SystemCode, 'ZDMT_RATE_TYPE' SourceTableName, 'isuref/ZDMT_RATE_TYPE' SourceQuery, '' WatermarkColumn, 'json' RawFileExtension, 'raw-load-bods' RawHandler, 'cleansed-load-bods-slt' CleansedHandler, * FROM _Base
ORDER BY SourceSchema, SourceTableName
""")

#Delta Tables with delete image. These tables would have di_operation_type column. 
tables = ['']
tablesWithNullableKeys = ['zblt_redress']
#Non-Prod "DeleteSourceFiles" : "False" while testing
# deleteSourceFiles = "False"
#Production
deleteSourceFiles = "True"
df = (
    df.withColumn('ExtendedProperties', lit(f'"DeleteSourceFiles" : "{deleteSourceFiles}"'))
      .withColumn('ExtendedProperties', when(lower(df.SourceTableName).isin(tables),expr('ExtendedProperties ||", "||\'\"SourceRecordDeletion\" : \"True\"\'')) 
                                        .otherwise(expr('ExtendedProperties')))
      .withColumn('ExtendedProperties', when(lower(df.SourceTableName).isin(tablesWithNullableKeys),expr('ExtendedProperties ||", "||\'\"CreateTableConstraints\" : \"False\"\''))
                                        .otherwise(expr('ExtendedProperties')))
      .withColumn('ExtendedProperties', expr('"{"||ExtendedProperties ||"}"'))    
)
display(df)

# COMMAND ----------

def ConfigureManifest(dataFrameConfig, whereClause=None):
    # ------------- CONSTRUCT QUERY ----------------- #
    df = dataFrameConfig.where(whereClause)
    # ------------- DISPLAY ----------------- #
#     ShowQuery(df)

    # ------------- SAVE ----------------- #
    AddIngestion(df)
    
    # ------------- ShowConfig ----------------- #
    ShowConfig()

for system_code in ['isuref','isudata']:
    SYSTEM_CODE = system_code
    ConfigureManifest(df, whereClause=f"SystemCode = '{SYSTEM_CODE}'")

#ADD BUSINESS KEY
ExecuteStatement("""
update dbo.extractLoadManifest set
businessKeyColumn = case sourceTableName
when 'ZBLT_REDRESS' then 'incidentDate,jobNumber,propertyNumber,taskID'
when 'ZDMT_RATE_TYPE' then 'functionClassText,deviceSize,billingClassCode,sopaFlag'
when '0UC_SALES_SIMU_01' then 'sequenceNumber,simulationPeriodId,loadDate'
else businessKeyColumn
end
where systemCode in ('isuref','isudata')
""")

# COMMAND ----------

# MAGIC %run /MDP-Framework/Common/common-spark

# COMMAND ----------

df_c = spark.table("controldb.dbo_extractloadmanifest")
display(df_c.where("SystemCode in ('isuref','isudata')"))
