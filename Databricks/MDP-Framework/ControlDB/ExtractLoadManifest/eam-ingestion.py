# Databricks notebook source
# MAGIC %run ../common-controldb

# COMMAND ----------

from pyspark.sql.functions import lit, when, lower, expr
df = spark.sql("""
WITH _Base AS 
(
  SELECT 'eamref' SystemCode, 'eam' SourceSchema, 'daf-sa-blob-sastoken' SourceKeyVaultSecret, 'eam' DestinationSchema, 'bods-load' SourceHandler, 'json' RawFileExtension, 'raw-load-bods' RawHandler, --'{ "DeleteSourceFiles" : "True" }' ExtendedProperties,
    'cleansed-load-bods-slt' CleansedHandler, '' SystemPath
)
SELECT '0ASSET_ATTR_TEXT' SourceTableName, 'eamref/0ASSET_ATTR_TEXT' SourceQuery, 'DELTA' WatermarkColumn, * FROM _Base
UNION 
SELECT '0ASSET_MAIN_TEXT' SourceTableName, 'eamref/0ASSET_MAIN_TEXT' SourceQuery, '' WatermarkColumn, * FROM _Base
UNION
(
WITH _Base AS 
(
  SELECT 'eamdata' SystemCode, 'eam' SourceSchema, 'daf-sa-blob-sastoken' SourceKeyVaultSecret, 'eam' DestinationSchema, 'bods-load' SourceHandler, 'json' RawFileExtension, 'raw-load-bods' RawHandler, --'{ "DeleteSourceFiles" : "True" }' ExtendedProperties,
  'cleansed-load-bods-slt' CleansedHandler, '' SystemPath
)
--Start of tables that may get converted to SLT later
SELECT 'ANLA' SourceTableName, 'eamdata/ANLA' SourceQuery, '' WatermarkColumn, * FROM _Base
--End of tables that may get converted to SLT later
)    
ORDER BY SourceSchema, SourceTableName
""")

#Delta Tables with delete image. These tables would have di_operation_type column. 
tables = ['0asset_attr_text']
tablesWithNullableKeys = []
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

for system_code in ['eamref','eamdata']:
    SYSTEM_CODE = system_code
    ConfigureManifest(df, whereClause=f"SystemCode = '{SYSTEM_CODE}'")

#ADD BUSINESS KEY
ExecuteStatement("""
update dbo.extractLoadManifest set
businessKeyColumn = case sourceTableName
when '0ASSET_ATTR_TEXT' then 'assetSubnumber,companyCode,dateValidityEnds,mainAssetNumber'
when '0ASSET_MAIN_TEXT' then 'companyCode,mainAssetNumber'
when 'ANLA' then 'mainAssetNumber,assetSubnumber,companyCode'
else businessKeyColumn
end
where systemCode in ('eamref','eamdata')
""") 

# COMMAND ----------

#Delete all cells below once this goes to Production

# COMMAND ----------

# MAGIC %run /MDP-Framework/Common/common-spark

# COMMAND ----------

df_c = spark.table("controldb.dbo_extractloadmanifest")
display(df_c.where("SystemCode in ('eamref','eamdata')"))

# COMMAND ----------

# for z in ["raw", "cleansed"]:
for z in ["cleansed"]:
    for t in df_c.where("SystemCode in ('eamref','eamdata')").collect():
        tableFqn = f"{z}.{t.DestinationSchema}_{t.SourceTableName}"
        print(tableFqn)
#         CleanTable(tableFqn)

# COMMAND ----------

# CleanTable('cleaned.eam_0RPM_DECISION_GUID_ID_TEXT')
