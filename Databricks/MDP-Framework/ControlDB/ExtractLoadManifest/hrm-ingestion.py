# Databricks notebook source
# MAGIC %run ../common-controldb

# COMMAND ----------

from pyspark.sql.functions import lit, when, lower, expr
df = spark.sql("""
WITH _Base AS 
(
  SELECT 'hrmref' SystemCode, 'hrm' SourceSchema, 'daf-sa-blob-sastoken' SourceKeyVaultSecret, 'hrm' DestinationSchema, 'bods-load' SourceHandler, 'json' RawFileExtension, 'raw-load-bods' RawHandler, --'{ "DeleteSourceFiles" : "True" }' ExtendedProperties,
    'cleansed-load-bods-slt' CleansedHandler, '' SystemPath
)
SELECT '0EMPLOYEE_TEXT' SourceTableName, 'hrmref/0EMPLOYEE_TEXT' SourceQuery, '' WatermarkColumn, * FROM _Base
UNION
(
WITH _Base AS 
(
  SELECT 'hrmdata' SystemCode, 'hrm' SourceSchema, 'daf-sa-blob-sastoken' SourceKeyVaultSecret, 'hrm' DestinationSchema, 'bods-load' SourceHandler, 'json' RawFileExtension, 'raw-load-bods' RawHandler, --'{ "DeleteSourceFiles" : "True" }' ExtendedProperties,
  'cleansed-load-bods-slt' CleansedHandler, '' SystemPath
)
SELECT '0EMPLOYEE_ATTR' SourceTableName, 'hrmdata/0EMPLOYEE_ATTR' SourceQuery, '' WatermarkColumn, * FROM _Base
)    
ORDER BY SourceSchema, SourceTableName
""")

#Non-Prod "DeleteSourceFiles" : "False" while testing
# deleteSourceFiles = "False"
#Production
deleteSourceFiles = "True"
df = (
    df.withColumn('ExtendedProperties', lit(f'"DeleteSourceFiles" : "{deleteSourceFiles}"'))         
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

for system_code in ['hrmref','hrmdata']:
    SYSTEM_CODE = system_code
    ConfigureManifest(df, whereClause=f"SystemCode = '{SYSTEM_CODE}'")

#ADD BUSINESS KEY
ExecuteStatement("""
update dbo.extractLoadManifest set
businessKeyColumn = case sourceTableName
when '0EMPLOYEE_TEXT' then 'validFromDate,validToDate,personnelNumber'
when '0EMPLOYEE_ATTR' then 'startDate,endDate,personnelNumber'
else businessKeyColumn
end
where systemCode in ('hrmref','hrmdata')
""") 
