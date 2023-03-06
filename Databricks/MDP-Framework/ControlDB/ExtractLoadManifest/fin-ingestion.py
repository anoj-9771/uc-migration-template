# Databricks notebook source
# MAGIC %run ../common-controldb

# COMMAND ----------

from pyspark.sql.functions import lit, when, lower, expr
df = spark.sql("""
WITH _Base AS 
(
  SELECT 'finref' SystemCode, 'fin' SourceSchema, 'daf-sa-blob-sastoken' SourceKeyVaultSecret, 'fin' DestinationSchema, 'bods-load' SourceHandler, 'json' RawFileExtension, 'raw-load-bods' RawHandler, --'{ "DeleteSourceFiles" : "True" }' ExtendedProperties,
    'cleansed-load-bods-slt' CleansedHandler, '' SystemPath
)
SELECT '0PROFIT_CTR_TEXT' SourceTableName, 'finref/0PROFIT_CTR_TEXT' SourceQuery, 'DELTA' WatermarkColumn, * FROM _Base
UNION 
SELECT '0CO_AREA_TEXT' SourceTableName, 'finref/0CO_AREA_TEXT' SourceQuery, '' WatermarkColumn, * FROM _Base
UNION 
SELECT '0COSTCENTER_TEXT' SourceTableName, 'finref/0COSTCENTER_TEXT' SourceQuery, 'DELTA' WatermarkColumn, * FROM _Base
UNION 
SELECT '0VERSION_TEXT' SourceTableName, 'finref/0VERSION_TEXT' SourceQuery, '' WatermarkColumn, * FROM _Base
UNION 
SELECT '0AC_DOC_TYP_TEXT' SourceTableName, 'finref/0AC_DOC_TYP_TEXT' SourceQuery, '' WatermarkColumn, * FROM _Base
UNION 
SELECT '0COMP_CODE_TEXT' SourceTableName, 'finref/0COMP_CODE_TEXT' SourceQuery, '' WatermarkColumn, * FROM _Base
UNION 
SELECT '0AC_LEDGER_TEXT' SourceTableName, 'finref/0AC_LEDGER_TEXT' SourceQuery, '' WatermarkColumn, * FROM _Base
UNION 
SELECT '0GL_ACCOUNT_TEXT' SourceTableName, 'finref/0GL_ACCOUNT_TEXT' SourceQuery, '' WatermarkColumn, * FROM _Base
UNION 
SELECT '0COSTELMNT_TEXT' SourceTableName, 'finref/0COSTELMNT_TEXT' SourceQuery, '' WatermarkColumn, * FROM _Base
UNION
(
WITH _Base AS 
(
  SELECT 'findata' SystemCode, 'fin' SourceSchema, 'daf-sa-blob-sastoken' SourceKeyVaultSecret, 'fin' DestinationSchema, 'bods-load' SourceHandler, 'json' RawFileExtension, 'raw-load-bods' RawHandler, --'{ "DeleteSourceFiles" : "True" }' ExtendedProperties,
  'cleansed-load-bods-slt' CleansedHandler, '' SystemPath
)
SELECT '0PROFIT_CTR_ATTR' SourceTableName, 'findata/0PROFIT_CTR_ATTR' SourceQuery, 'DELTA' WatermarkColumn, * FROM _Base
UNION
SELECT '0GL_ACCOUNT_ATTR' SourceTableName, 'findata/0GL_ACCOUNT_ATTR' SourceQuery, 'DELTA' WatermarkColumn, * FROM _Base
UNION
SELECT '0COSTCENTER_ATTR' SourceTableName, 'findata/0COSTCENTER_ATTR' SourceQuery, '' WatermarkColumn, * FROM _Base   
UNION
SELECT '0COMP_CODE_ATTR' SourceTableName, 'findata/0COMP_CODE_ATTR' SourceQuery, '' WatermarkColumn, * FROM _Base   
UNION
--Start of tables that may get converted to SLT later
SELECT 'COBRB' SourceTableName, 'findata/COBRB' SourceQuery, '' WatermarkColumn, * FROM _Base
UNION
SELECT 'COBRA' SourceTableName, 'findata/COBRA' SourceQuery, '' WatermarkColumn, * FROM _Base
UNION
SELECT 'COSS' SourceTableName, 'findata/COSS' SourceQuery, '' WatermarkColumn, * FROM _Base
UNION
SELECT 'COSP' SourceTableName, 'findata/COSP' SourceQuery, '' WatermarkColumn, * FROM _Base
--End of tables that may get converted to SLT later
)    
ORDER BY SourceSchema, SourceTableName
""")

#Delta Tables with delete image. These tables would have di_operation_type column. 
tables = ['0profit_ctr_attr','0gl_account_attr','0profit_ctr_text','0costcenter_text']
tablesWithNullableKeys = ['coss','cosp','cobrb']
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

for system_code in ['finref','findata']:
    SYSTEM_CODE = system_code
    ConfigureManifest(df, whereClause=f"SystemCode = '{SYSTEM_CODE}'")

#ADD BUSINESS KEY
ExecuteStatement("""
update dbo.extractLoadManifest set
businessKeyColumn = case sourceTableName
when '0GL_ACCOUNT_ATTR' then 'chartOfAccounts,glAccountNumber'
when '0GL_ACCOUNT_TEXT' then 'chartOfAccounts,glAccountNumber'
when '0PROFIT_CTR_ATTR' then 'controllingArea,profitCenter,validToDate'
when '0PROFIT_CTR_TEXT' then 'controllingArea,profitCenter,validToDate'
when '0AC_LEDGER_TEXT' then 'ledger'
when '0AC_DOC_TYP_TEXT' then 'documentType'
when '0COSTCENTER_ATTR' then 'controllingArea,costCenter,validToDate'
when '0COMP_CODE_ATTR' then 'companyCode'
when '0CO_AREA_TEXT' then 'controllingArea'
when '0COSTCENTER_TEXT' then 'controllingArea,costCentre,validToDate'
when '0VERSION_TEXT' then 'version'
when '0COMP_CODE_TEXT' then 'companyCode'
when '0COSTELMNT_TEXT' then 'controllingArea,costElement,validFromDate'
when 'COBRB' then 'distributionRuleGroup,ledgerGroup,ledger,sequenceDistributionRule,objectnumber'
when 'COBRA' then 'objectnumber'
when 'COSS' then 'debitCreditIndicator,fiscalYear,coKeySubNumber,costElement,ledgerForControllingObjects,objectnumber,partnerObject,periodBlock,transactionCurrency,sourceObject,version,cobusinessTransaction,valueType'
when 'COSP' then 'debitCreditIndicator,fiscalYear,coKeySubNumber,costElement,ledgerForControllingObjects,objectnumber,tradingPartnerBusinessArea,periodBlock,transactionCurrency,tradingPartnerID,version,cobusinessTransaction,valueType'
else businessKeyColumn
end
where systemCode in ('finref','findata')
""") 

# COMMAND ----------

# MAGIC %sql
# MAGIC select length( 'debitCreditIndicator,fiscalYear,coKeySubNumber,costElement,ledgerForControllingObjects,objectnumber,tradingPartnerBusinessArea,periodBlock,transactionCurrency,tradingPartnerID,version,cobusinessTransaction,valueType')

# COMMAND ----------

#Delete all cells below once this goes to Production

# COMMAND ----------

# MAGIC %run /MDP-Framework/Common/common-spark

# COMMAND ----------

df_c = spark.table("controldb.dbo_extractloadmanifest")
display(df_c.where("SystemCode in ('finref','findata')"))

# COMMAND ----------

# for z in ["raw", "cleansed"]:
for z in ["cleansed"]:
    for t in df_c.where("SystemCode in ('finref','findata')").rdd.collect():
        tableFqn = f"{z}.{t.DestinationSchema}_{t.SourceTableName}"
        print(tableFqn)
#         CleanTable(tableFqn)

# COMMAND ----------

# CleanTable('cleaned.fin_0RPM_DECISION_GUID_ID_TEXT')
