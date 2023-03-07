# Databricks notebook source
# MAGIC %run ../common-controldb

# COMMAND ----------

from pyspark.sql.functions import lit, when, lower, expr
df = spark.sql("""
--WITH _Base AS 
--(
--  SELECT 'isuref' SystemCode, 'isu' SourceSchema, 'daf-sa-blob-sastoken' SourceKeyVaultSecret, 'isu' DestinationSchema, 'bods-load' SourceHandler, 'json' RawFileExtension, 'raw-load-bods' RawHandler, --'{ "DeleteSourceFiles" : "True" }' ExtendedProperties,
--    'cleansed-load-bods-slt' CleansedHandler, '' SystemPath
--)
--SELECT '' SourceTableName, 'xxx/xxx' SourceQuery, '' WatermarkColumn, * FROM _Base
--UNION 
--(
WITH _Base AS 
(
  SELECT 'isudata' SystemCode, 'isu' SourceSchema, 'daf-sa-blob-sastoken' SourceKeyVaultSecret, 'isu' DestinationSchema, 'bods-load' SourceHandler, 'json' RawFileExtension, 'raw-load-bods' RawHandler, --'{ "DeleteSourceFiles" : "True" }' ExtendedProperties,
  'cleansed-load-bods-slt' CleansedHandler, '' SystemPath
)
SELECT 'ZBLT_REDRESS' SourceTableName, 'isudata/ZBLT_REDRESS' SourceQuery, '' WatermarkColumn, * FROM _Base
--UNION
--Start of tables that may get converted to SLT later
--End of tables that may get converted to SLT later
--)    
ORDER BY SourceSchema, SourceTableName
""")

#Delta Tables with delete image. These tables would have di_operation_type column. 
tables = ['']
tablesWithNullableKeys = ['zblt_redress']
#Non-Prod "DeleteSourceFiles" : "False" while testing
deleteSourceFiles = "False"
#Production
# deleteSourceFiles = "True"
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
else businessKeyColumn
end
where systemCode in ('isuref','isudata')
""")

# COMMAND ----------

#Delete all cells below once this goes to Production

# COMMAND ----------

# MAGIC %run /MDP-Framework/Common/common-spark

# COMMAND ----------

df_c = spark.table("controldb.dbo_extractloadmanifest")
display(df_c.where("SystemCode in ('isuref','isudata')"))

# COMMAND ----------

# for z in ["raw", "cleansed"]:
for z in ["cleansed"]:
    for t in df_c.where("SystemCode in ('isuref','isudata')").rdd.collect():
        tableFqn = f"{z}.{t.DestinationSchema}_{t.SourceTableName}"
        print(tableFqn)
#         CleanTable(tableFqn)

# COMMAND ----------

# CleanTable('cleaned.ppm_0RPM_DECISION_GUID_ID_TEXT')
