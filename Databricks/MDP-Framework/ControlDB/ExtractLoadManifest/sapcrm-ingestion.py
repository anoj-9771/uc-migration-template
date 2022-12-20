# Databricks notebook source
# MAGIC %run ../common-controldb

# COMMAND ----------

    # ------------- CONSTRUCT QUERY ----------------- #
    df = spark.sql("""
    WITH _Base AS 
    (
      SELECT 'crmref' SystemCode, 'crm' SourceSchema, 'daf-sa-blob-sastoken' SourceKeyVaultSecret, 'bods-load' SourceHandler, 'json' RawFileExtension, 'raw-load-bods' RawHandler, '{ "DeleteSourceFiles" : "True" }' ExtendedProperties, 'cleansed-load-bods-slt' CleansedHandler, '' WatermarkColumn
    )
SELECT  'ZCS_LONG_TEXT_ACT'  SourceTableName, 'crmref/ZCS_LONG_TEXT_ACT'  SourceQuery,  * FROM _Base
UNION
SELECT  'TJ30T'  SourceTableName, 'crmref/TJ30T'  SourceQuery,  * FROM _Base
UNION
SELECT  '0CRM_CATEGORY_TEXT'  SourceTableName, 'crmref/0CRM_CATEGORY_TEXT'  SourceQuery,  * FROM _Base
UNION
SELECT  'ZCST_SOURCE'  SourceTableName, 'crmref/ZCST_SOURCE'  SourceQuery,  * FROM _Base
UNION
SELECT  'CRM_SVY_RE_QUEST'  SourceTableName, 'crmref/CRM_SVY_RE_QUEST'  SourceQuery,  * FROM _Base
UNION
SELECT  'CRMC_PARTNER_FT'  SourceTableName, 'crmref/CRMC_PARTNER_FT'  SourceQuery,  * FROM _Base
UNION
SELECT  '0CRM_PROC_TYPE_TEXT'  SourceTableName, 'crmref/0CRM_PROC_TYPE_TEXT'  SourceQuery,  * FROM _Base
UNION
SELECT  'CRMD_ERMS_STAS_T'  SourceTableName, 'crmref/CRMD_ERMS_STAS_T'  SourceQuery,  * FROM _Base
UNION
SELECT  'CRMD_ERMS_EVENTT'  SourceTableName, 'crmref/CRMD_ERMS_EVENTT'  SourceQuery,  * FROM _Base
UNION
SELECT  'ZCS_LONG_TEXT_F'  SourceTableName, 'crmref/ZCS_LONG_TEXT_F'  SourceQuery,  * FROM _Base
UNION
SELECT  'SCAPTTXT'  SourceTableName, 'crmref/SCAPTTXT'  SourceQuery,  * FROM _Base
    UNION
    (
    WITH _Base AS 
    (
      SELECT 'crmdata' SystemCode, 'crm' SourceSchema, 'daf-sa-blob-sastoken' SourceKeyVaultSecret, 'bods-load' SourceHandler, 'json' RawFileExtension, 'raw-load-bods' RawHandler, '{ "DeleteSourceFiles" : "True" }' ExtendedProperties, 'cleansed-load-bods-slt' CleansedHandler, '' WatermarkColumn
    )
SELECT  '0CRM_SALES_ACT_1'  SourceTableName, 'crmdata/0CRM_SALES_ACT_1'  SourceQuery,  * FROM _Base
UNION
SELECT  'CRMC_ERMS_CAT_HI'  SourceTableName, 'crmdata/CRMC_ERMS_CAT_HI'  SourceQuery,  * FROM _Base
UNION
SELECT  'CRMC_ERMS_CAT_AS'  SourceTableName, 'crmdata/CRMC_ERMS_CAT_AS'  SourceQuery,  * FROM _Base
UNION
SELECT  'CRMC_ERMS_CAT_CT'  SourceTableName, 'crmdata/CRMC_ERMS_CAT_CT'  SourceQuery,  * FROM _Base
UNION
SELECT  'CRMC_ERMS_CAT_CA'  SourceTableName, 'crmdata/CRMC_ERMS_CAT_CA'  SourceQuery,  * FROM _Base
UNION
SELECT  'ZPSTXHWITHCGUID'  SourceTableName, 'crmdata/ZPSTXHWITHCGUID'  SourceQuery,  * FROM _Base
UNION
SELECT  'CRMD_SURVEY'  SourceTableName, 'crmdata/CRMD_SURVEY'  SourceQuery,  * FROM _Base
UNION
SELECT  'CRM_SVY_DB_S'  SourceTableName, 'crmdata/CRM_SVY_DB_S'  SourceQuery,  * FROM _Base
UNION
SELECT  'CRMORDERPHIO'  SourceTableName, 'crmdata/CRMORDERPHIO'  SourceQuery,  * FROM _Base
UNION
SELECT  'CRMC_PROC_TYPE'  SourceTableName, 'crmdata/CRMC_PROC_TYPE'  SourceQuery,  * FROM _Base
UNION
SELECT  'CRMC_PRT_OTYPE'  SourceTableName, 'crmdata/CRMC_PRT_OTYPE'  SourceQuery,  * FROM _Base
UNION
SELECT  'CRMV_ERMS_CAT_CA'  SourceTableName, 'crmdata/CRMV_ERMS_CAT_CA'  SourceQuery,  * FROM _Base
UNION
SELECT  'SKWG_BREL'  SourceTableName, 'crmdata/SKWG_BREL'  SourceQuery,  * FROM _Base
UNION
SELECT  'CRMORDERPHF'  SourceTableName, 'crmdata/CRMORDERPHF'  SourceQuery,  * FROM _Base
    )    
    """)

# COMMAND ----------

def ConfigureManifest(df):
    # ------------- CONSTRUCT QUERY ----------------- #
    
    # ------------- DISPLAY ----------------- #
    ShowQuery(df)

    # ------------- SAVE ----------------- #
    AddIngestion(df)
    
    # ------------- ShowConfig ----------------- #
    ShowConfig()
    
for system_code in ['crmref','crmdata']:
    SYSTEM_CODE = system_code
    ConfigureManifest(df.where(f"SystemCode = '{SYSTEM_CODE}'"))

# COMMAND ----------

 #ADD BUSINESS KEY
ExecuteStatement("""
update dbo.extractLoadManifest set
businessKeyColumn = case sourceTableName
when 'ZCS_LONG_TEXT_ACT' then  'serviceRequestNumber'
when 'TJ30T' then  'statusProfile,statusCode'
when '0CRM_CATEGORY_TEXT' then  'categoryCode'
when 'ZCST_SOURCE' then  'sourceCode'
when 'CRM_SVY_RE_QUEST' then  'application,survey,questionnaire,questionSequenceNumber,question'
when 'CRMC_PARTNER_FT' then  'partnerFunctionCode'
when '0CRM_PROC_TYPE_TEXT' then  'processTypeCode'
when 'CRMD_ERMS_STAS_T' then  'emailStatusID'
when 'CRMD_ERMS_EVENTT' then  'emailEventID'
when 'ZCS_LONG_TEXT_F' then  'serviceRequestID,serviceRequestGUID'
when 'SCAPTTXT' then  'apptType'
when '0CRM_SALES_ACT_1' then  'interactionGUID'
when 'CRMC_ERMS_CAT_HI' then  'categoryTreeGUID,categoryTreeType,childGUID'
when 'CRMC_ERMS_CAT_AS' then  'categoryTreeGUID'
when 'CRMC_ERMS_CAT_CT' then  'categoryGUID'
when 'CRMC_ERMS_CAT_CA' then  'categoryGUID'
when 'ZPSTXHWITHCGUID' then  'applicationObject,noteID,noteGUID,noteTypeCode'
when 'CRMD_SURVEY' then  'surveyGUID'
when 'CRM_SVY_DB_S' then  'surveyId,surveyVersion'
when 'CRMORDERPHIO' then  'documentID'
when 'CRMC_PROC_TYPE' then  'processTypeCode'
when 'CRMC_PRT_OTYPE' then  'objectTypeDescription'
when 'CRMV_ERMS_CAT_CA' then  'categoryTreeGUID,categoryGUID,categoryTreeID,categoryTreeState,categoryTreeDescription,categoryID,categoryLabel'
when 'SKWG_BREL' then  'brelGUID'
when 'CRMORDERPHF' then  'documentID,documentSeqNumber'
else businessKeyColumn
end
where systemCode in ('crmref','crmdata')
""")

# COMMAND ----------

 #updating WatermarkColumn 
ExecuteStatement("""
update dbo.extractLoadManifest set
WatermarkColumn = case sourceTableName
when '0CRM_SALES_ACT_1' then  'DELTA'
when 'ZCS_LONG_TEXT_ACT' then  'DELTA'
when 'ZCS_LONG_TEXT_F' then  'DELTA'
else WatermarkColumn
end
where systemCode in ('crmref','crmdata')
""")
