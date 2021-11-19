# Databricks notebook source
#config parameters
source = 'ISU' #either CRM or ISU
table = '0BP_ID_ATTR'

environment = 'test'
storage_account_name = "sablobdaftest01"
storage_account_access_key = dbutils.secrets.get(scope="TestScope",key="test-sablob-key")
containerName = "archive"


# COMMAND ----------

# MAGIC %run ../../includes/tableEvaluation

# COMMAND ----------

# DBTITLE 1,[Source] with mapping
# MAGIC %sql
# MAGIC select
# MAGIC businessPartnerNumber
# MAGIC ,identificationTypeCode
# MAGIC ,identificationType
# MAGIC ,businessPartnerIdNumber
# MAGIC ,institute
# MAGIC ,entryDate
# MAGIC ,validFromDate
# MAGIC ,validToDate
# MAGIC ,countryShortName
# MAGIC ,stateCode
# MAGIC ,businessPartnerGUID
# MAGIC ,deletedIndicator
# MAGIC from
# MAGIC (select
# MAGIC PARTNER AS businessPartnerNumber
# MAGIC ,TYPE AS identificationTypeCode
# MAGIC ,b.identificationType as identificationType
# MAGIC ,IDNUMBER AS businessPartnerIdNumber
# MAGIC ,INSTITUTE   as institute
# MAGIC ,ENTRY_DATE as entryDate
# MAGIC ,VALID_DATE_FROM as validFromDate
# MAGIC ,VALID_DATE_TO as validToDate
# MAGIC ,COUNTRY as countryShortName
# MAGIC ,REGION as stateCode
# MAGIC ,PARTNER_GUID as businessPartnerGUID
# MAGIC ,FLG_DEL_BW as deletedIndicator
# MAGIC ,row_number() over (partition by PARTNER,TYPE,IDNUMBER order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} 
# MAGIC left join cleansed.isu_0bp_id_type_text  b
# MAGIC on b.identificationTypeCode = TYPE)a  
# MAGIC  where  a.rn = 1

# COMMAND ----------

# DBTITLE 1,[Verification] Count Check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.${vars.table}
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (select
# MAGIC businessPartnerNumber
# MAGIC ,identificationTypeCode
# MAGIC ,identificationType
# MAGIC ,businessPartnerIdNumber
# MAGIC ,institute
# MAGIC ,entryDate
# MAGIC ,validFromDate
# MAGIC ,validToDate
# MAGIC ,countryShortName
# MAGIC ,stateCode
# MAGIC ,businessPartnerGUID
# MAGIC ,deletedIndicator
# MAGIC from
# MAGIC (select
# MAGIC PARTNER AS businessPartnerNumber
# MAGIC ,TYPE AS identificationTypeCode
# MAGIC ,b.identificationType as identificationType
# MAGIC ,IDNUMBER AS businessPartnerIdNumber
# MAGIC ,INSTITUTE   as institute
# MAGIC ,ENTRY_DATE as entryDate
# MAGIC ,VALID_DATE_FROM as validFromDate
# MAGIC ,VALID_DATE_TO as validToDate
# MAGIC ,COUNTRY as countryShortName
# MAGIC ,REGION as stateCode
# MAGIC ,PARTNER_GUID as businessPartnerGUID
# MAGIC ,FLG_DEL_BW as deletedIndicator
# MAGIC ,row_number() over (partition by PARTNER,TYPE,IDNUMBER order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} 
# MAGIC left join cleansed.isu_0bp_id_type_text  b
# MAGIC on b.identificationTypeCode = TYPE)a  
# MAGIC  where  a.rn = 1)

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT 
# MAGIC businessPartnerNumber
# MAGIC ,identificationTypeCode
# MAGIC ,identificationType
# MAGIC ,businessPartnerIdNumber
# MAGIC ,institute
# MAGIC ,entryDate
# MAGIC ,validFromDate
# MAGIC ,validToDate
# MAGIC ,countryShortName
# MAGIC ,stateCode
# MAGIC ,businessPartnerGUID
# MAGIC ,deletedIndicator
# MAGIC , COUNT (*) as count
# MAGIC FROM cleansed.${vars.table}
# MAGIC GROUP BY businessPartnerNumber
# MAGIC ,identificationTypeCode
# MAGIC ,identificationType
# MAGIC ,businessPartnerIdNumber
# MAGIC ,institute
# MAGIC ,entryDate
# MAGIC ,validFromDate
# MAGIC ,validToDate
# MAGIC ,countryShortName
# MAGIC ,stateCode
# MAGIC ,businessPartnerGUID
# MAGIC ,deletedIndicator
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY businessPartnerNumber,identificationTypeCode,businessPartnerIdNumber 
# MAGIC order by businessPartnerNumber,identificationTypeCode,businessPartnerIdNumber) as rn
# MAGIC FROM  cleansed.${vars.table}
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select
# MAGIC businessPartnerNumber
# MAGIC ,identificationTypeCode
# MAGIC ,identificationType
# MAGIC ,businessPartnerIdNumber
# MAGIC ,institute
# MAGIC ,entryDate
# MAGIC ,validFromDate
# MAGIC ,validToDate
# MAGIC ,countryShortName
# MAGIC ,stateCode
# MAGIC ,businessPartnerGUID
# MAGIC ,deletedIndicator
# MAGIC from
# MAGIC (select
# MAGIC PARTNER AS businessPartnerNumber
# MAGIC ,TYPE AS identificationTypeCode
# MAGIC ,b.identificationType as identificationType
# MAGIC ,IDNUMBER AS businessPartnerIdNumber
# MAGIC ,INSTITUTE   as institute
# MAGIC ,ENTRY_DATE as entryDate
# MAGIC ,VALID_DATE_FROM as validFromDate
# MAGIC ,VALID_DATE_TO as validToDate
# MAGIC ,COUNTRY as countryShortName
# MAGIC ,REGION as stateCode
# MAGIC ,PARTNER_GUID as businessPartnerGUID
# MAGIC ,FLG_DEL_BW as deletedIndicator
# MAGIC ,row_number() over (partition by PARTNER,TYPE,IDNUMBER order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} 
# MAGIC left join cleansed.isu_0bp_id_type_text  b
# MAGIC on b.identificationTypeCode = TYPE)a  
# MAGIC except
# MAGIC select
# MAGIC businessPartnerNumber
# MAGIC ,identificationTypeCode
# MAGIC ,identificationType
# MAGIC ,businessPartnerIdNumber
# MAGIC ,institute
# MAGIC ,entryDate
# MAGIC ,validFromDate
# MAGIC ,validToDate
# MAGIC ,countryShortName
# MAGIC ,stateCode
# MAGIC ,businessPartnerGUID
# MAGIC ,deletedIndicator
# MAGIC from
# MAGIC cleansed.${vars.table}

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC businessPartnerNumber
# MAGIC ,identificationTypeCode
# MAGIC ,identificationType
# MAGIC ,businessPartnerIdNumber
# MAGIC ,institute
# MAGIC ,entryDate
# MAGIC ,validFromDate
# MAGIC ,validToDate
# MAGIC ,countryShortName
# MAGIC ,stateCode
# MAGIC ,businessPartnerGUID
# MAGIC ,deletedIndicator
# MAGIC from
# MAGIC cleansed.${vars.table}
# MAGIC except
# MAGIC select
# MAGIC businessPartnerNumber
# MAGIC ,identificationTypeCode
# MAGIC ,identificationType
# MAGIC ,businessPartnerIdNumber
# MAGIC ,institute
# MAGIC ,entryDate
# MAGIC ,validFromDate
# MAGIC ,validToDate
# MAGIC ,countryShortName
# MAGIC ,stateCode
# MAGIC ,businessPartnerGUID
# MAGIC ,deletedIndicator
# MAGIC from
# MAGIC (select
# MAGIC PARTNER AS businessPartnerNumber
# MAGIC ,TYPE AS identificationTypeCode
# MAGIC ,b.identificationType as identificationType
# MAGIC ,IDNUMBER AS businessPartnerIdNumber
# MAGIC ,INSTITUTE   as institute
# MAGIC ,ENTRY_DATE as entryDate
# MAGIC ,VALID_DATE_FROM as validFromDate
# MAGIC ,VALID_DATE_TO as validToDate
# MAGIC ,COUNTRY as countryShortName
# MAGIC ,REGION as stateCode
# MAGIC ,PARTNER_GUID as businessPartnerGUID
# MAGIC ,FLG_DEL_BW as deletedIndicator
# MAGIC ,row_number() over (partition by PARTNER,TYPE,IDNUMBER order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} 
# MAGIC left join cleansed.isu_0bp_id_type_text  b
# MAGIC on b.identificationTypeCode = TYPE)a  
# MAGIC  where  a.rn = 1
