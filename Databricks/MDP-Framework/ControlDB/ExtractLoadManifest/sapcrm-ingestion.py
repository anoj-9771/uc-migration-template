# Databricks notebook source
# MAGIC %run ../common-controldb

# COMMAND ----------

    # ------------- CONSTRUCT QUERY ----------------- #
    df = spark.sql("""
    WITH _Base AS 
    (
      SELECT 'crmref' SystemCode, 'crm' SourceSchema, 'daf-sa-blob-sastoken' SourceKeyVaultSecret, 'bods-load' SourceHandler, 'json' RawFileExtension, 'raw-load-bods' RawHandler, '{ "DeleteSourceFiles" : "True" }' ExtendedProperties, 'cleansed-load-bods-slt' CleansedHandler, '' WatermarkColumn,'' SystemPath,'crm' DestinationSchema
    )
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
SELECT  'CRMC_ERMS_CAT_CD'  SourceTableName, 'crmref/CRMC_ERMS_CAT_CD'  SourceQuery,  * FROM _Base
UNION
SELECT  'CRMC_ERMS_CAT_LN'  SourceTableName, 'crmref/CRMC_ERMS_CAT_LN'  SourceQuery,  * FROM _Base
UNION
SELECT  'CRMC_ERMS_CAT_OK'  SourceTableName, 'crmref/CRMC_ERMS_CAT_OK'  SourceQuery,  * FROM _Base
UNION
SELECT  'CRMC_QPCT'  SourceTableName, 'crmref/CRMC_QPCT'  SourceQuery,  * FROM _Base
    UNION
    (
    WITH _Base AS 
    (
      SELECT 'crmdata' SystemCode, 'crm' SourceSchema, 'daf-sa-blob-sastoken' SourceKeyVaultSecret, 'bods-load' SourceHandler, 'json' RawFileExtension, 'raw-load-bods' RawHandler, '{ "DeleteSourceFiles" : "True" }' ExtendedProperties, 'cleansed-load-bods-slt' CleansedHandler, '' WatermarkColumn,'' SystemPath,'crm' DestinationSchema
    )
SELECT  '0CRM_SALES_ACT_1'  SourceTableName, 'crmdata/0CRM_SALES_ACT_1'  SourceQuery,  * FROM _Base
UNION
SELECT  '0CRM_SRV_REQ_INCI_H'  SourceTableName, 'crmdata/0CRM_SRV_REQ_INCI_H'  SourceQuery,  * FROM _Base
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
    UNION
    (
    WITH _Base AS 
    (
      SELECT 'sltcrmdata' SystemCode, 'dbo' SourceSchema, 'daf-sql-slt-crm-connectionstring' SourceKeyVaultSecret, 'sql-load' SourceHandler, 'json' RawFileExtension, 'raw-load-delta' RawHandler,'' ExtendedProperties, 'cleansed-load-bods-slt' CleansedHandler,'DELTA_TS' WatermarkColumn,'crmdata' SystemPath,'crm' DestinationSchema
    )
SELECT  'CRM_JCDS'  SourceTableName,''  SourceQuery,  * FROM _Base
UNION
SELECT  'CRM_SVY_DB_SV'  SourceTableName,''  SourceQuery,  * FROM _Base
UNION
SELECT  'CRMD_ERMS_HEADER'  SourceTableName,''  SourceQuery,  * FROM _Base
UNION
SELECT  'CRMD_ERMS_STEP'  SourceTableName,''  SourceQuery,  * FROM _Base
UNION
SELECT  'CRMD_ERMS_CONTNT'  SourceTableName,''  SourceQuery,  * FROM _Base
UNION
SELECT  'CRMD_LINK'  SourceTableName,''  SourceQuery,  * FROM _Base
UNION
SELECT  'CRMD_SRV_SUBJECT'  SourceTableName,''  SourceQuery,  * FROM _Base
UNION
SELECT  'CRMD_SRV_OSSET'  SourceTableName,''  SourceQuery,  * FROM _Base
UNION
SELECT  'SCAPPTSEG'  SourceTableName,''  SourceQuery,  * FROM _Base
UNION
SELECT  'CRMD_BRELVONAE'  SourceTableName,''  SourceQuery,  * FROM _Base
UNION
SELECT  'CRMD_PARTNER'  SourceTableName,''  SourceQuery,  * FROM _Base
UNION
SELECT  'CRMD_DHR_ACTIV'  SourceTableName,''  SourceQuery,  * FROM _Base
UNION
SELECT  'SWWWIHEAD'  SourceTableName,''  SourceQuery,  * FROM _Base
UNION
SELECT  'CDHDR'  SourceTableName,''  SourceQuery, * FROM _Base
UNION
SELECT  'CDPOS'  SourceTableName,''  SourceQuery,  * FROM _Base
UNION
SELECT  'CRMD_ORDERADM_H'  SourceTableName,''  SourceQuery,  * FROM _Base
UNION
SELECT  'CRMD_ORDERADM_I'  SourceTableName,''  SourceQuery,  * FROM _Base
UNION
SELECT  'CRM_JEST'  SourceTableName,''  SourceQuery,  * FROM _Base
UNION
SELECT  'CRMD_ORDER_INDEX'  SourceTableName,''  SourceQuery,  * FROM _Base
    )
    """)
    df.display()


# COMMAND ----------

def ConfigureManifest(df):
    # ------------- CONSTRUCT QUERY ----------------- #
    
    # ------------- DISPLAY ----------------- #
    ShowQuery(df)

    # ------------- SAVE ----------------- #
    AddIngestion(df)
    
    # ------------- ShowConfig ----------------- #
    ShowConfig()
    
for system_code in ['crmref','crmdata','sltcrmdata']:
    SYSTEM_CODE = system_code
    ConfigureManifest(df.where(f"SystemCode = '{SYSTEM_CODE}'"))

# COMMAND ----------

 #ADD BUSINESS KEY
ExecuteStatement("""
update dbo.extractLoadManifest set
businessKeyColumn = case sourceTableName
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
when '0CRM_SRV_REQ_INCI_H' then  'serviceRequestGUID,extract_datetime,di_Sequence_Number'
when 'CRM_JCDS' then  'objectGUID,objectStatus,changeNumber'
when 'CRM_SVY_DB_SV' then  'surveyValuesGuid,surveyValuesVersion,surveyValueKeyAttribute'
when 'CRMD_ERMS_HEADER' then  'emailId'
when 'CRMD_ERMS_STEP' then  'emailID,stepNumber'
when 'CRMD_ERMS_CONTNT' then  'emailID,seqNumber'
when 'CRMD_LINK' then  'hiGUID,setGUID'
when 'CRMD_SRV_SUBJECT' then  'serviceSubjectGUID'
when 'CRMD_SRV_OSSET' then  'objectSubjectGUID'
when 'SCAPPTSEG' then  'apptGUID'
when 'CRMD_BRELVONAE' then  'relationID,segmentDocType'
when 'CRMD_PARTNER' then  'partnerGUID'
when 'CRMD_DHR_ACTIV' then  'activityGUID'
when 'SWWWIHEAD' then  'workItemId'
when 'CDHDR' then  'objectClass,objectId,changeNumber'
when 'CDPOS' then  'objectClass,objectId,changeNumber,tableName,tableKey,fieldName,changeTypeIndicator'
when 'CRMD_ORDERADM_H' then  'objectGUID'
when 'CRMD_ORDERADM_I' then  'objectGUID'
when 'CRM_JEST' then  'objectNumber,statusCode'
when 'CRMC_ERMS_CAT_CD' then  'categroyGUID'
when 'CRMD_ORDER_INDEX' then  'orderGUID'
when 'CRMC_ERMS_CAT_LN' then  'keyGUID'
when 'CRMC_ERMS_CAT_OK' then  'objectGUID'
when 'CRMC_QPCT' then  'catalog,catalogCodeGroup,catalogCode'
else businessKeyColumn
end
where systemCode in ('crmref','crmdata','sltcrmdata')
""")

# COMMAND ----------

 #updating WatermarkColumn 
ExecuteStatement("""
update dbo.extractLoadManifest set
WatermarkColumn = case sourceTableName
when '0CRM_SALES_ACT_1' then  'DELTA'
when 'ZCS_LONG_TEXT_F' then  'DELTA'
when '0CRM_SRV_REQ_INCI_H' then  'DELTA'
else WatermarkColumn
end
where systemCode in ('crmref','crmdata')
""")

# COMMAND ----------

#updating Extendedproperties
ExecuteStatement("""
update dbo.extractLoadManifest set
ExtendedProperties = case sourceTableName
when '0CRM_SALES_ACT_1' then  '{ "DeleteSourceFiles" : "True", "OverrideClusterName":"King_Cluster_SingleNode"}'
when 'ZCS_LONG_TEXT_F' then  '{ "DeleteSourceFiles" : "True", "OverrideClusterName":"King_Cluster_SingleNode"}'
when '0CRM_SRV_REQ_INCI_H' then  '{ "DeleteSourceFiles" : "True", "OverrideClusterName":"King_Cluster_SingleNode"}'
when 'ZPSTXHWITHCGUID' then  '{ "DeleteSourceFiles" : "True", "OverrideClusterName":"King_Cluster_SingleNode"}'
when 'CDPOS' then  '{"OverrideClusterName":"King_Cluster_SingleNode"}'
else ExtendedProperties
end
where systemCode in ('crmref','crmdata','sltcrmdata')
""")
