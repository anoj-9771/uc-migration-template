# Databricks notebook source
#config parameters
source = 'ISU' #either CRM or ISU
table = '0UCCONTRACTH_ATTR_2'

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
# MAGIC contractId,
# MAGIC validToDate,
# MAGIC validFromDate,
# MAGIC installationId,
# MAGIC contractHeadGUID,
# MAGIC contractPosGUID,
# MAGIC productId,
# MAGIC productGUID,
# MAGIC marketingCampaign,
# MAGIC deletedIndicator,
# MAGIC productBeginIndicator,
# MAGIC productChangeIndicator,
# MAGIC replicationControls,
# MAGIC createdDate,
# MAGIC createdBy,
# MAGIC lastChangedDate,
# MAGIC individualContractID,
# MAGIC lastChangedBy
# MAGIC from
# MAGIC (select
# MAGIC VERTRAG as contractId,
# MAGIC BIS as validToDate,
# MAGIC AB as validFromDate,
# MAGIC ANLAGE as installationId,
# MAGIC CONTRACTHEAD as contractHeadGUID,
# MAGIC CONTRACTPOS as contractPosGUID,
# MAGIC PRODID as productId,
# MAGIC PRODUCT_GUID as productGUID,
# MAGIC CAMPAIGN as marketingCampaign,
# MAGIC LOEVM as deletedIndicator,
# MAGIC PRODCH_BEG as productBeginIndicator,
# MAGIC PRODCH_END as productChangeIndicator,
# MAGIC XREPLCNTL as replicationControls,
# MAGIC ERDAT as createdDate,
# MAGIC ERNAM as createdBy,
# MAGIC AEDAT as lastChangedDate,
# MAGIC OUCONTRACT as individualContractID,
# MAGIC AENAM as lastChangedBy
# MAGIC ,row_number() over (partition by VERTRAG, BIS order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC )a where  a.rn = 1

# COMMAND ----------

# DBTITLE 1,[Verification] Count Check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.${vars.table}
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (select
# MAGIC contractId,
# MAGIC validToDate,
# MAGIC validFromDate,
# MAGIC installationId,
# MAGIC contractHeadGUID,
# MAGIC contractPosGUID,
# MAGIC productId,
# MAGIC productGUID,
# MAGIC marketingCampaign,
# MAGIC deletedIndicator,
# MAGIC productBeginIndicator,
# MAGIC productChangeIndicator,
# MAGIC replicationControls,
# MAGIC createdDate,
# MAGIC createdBy,
# MAGIC lastChangedDate,
# MAGIC individualContractID,
# MAGIC lastChangedBy
# MAGIC from
# MAGIC (select
# MAGIC VERTRAG as contractId,
# MAGIC BIS as validToDate,
# MAGIC AB as validFromDate,
# MAGIC ANLAGE as installationId,
# MAGIC CONTRACTHEAD as contractHeadGUID,
# MAGIC CONTRACTPOS as contractPosGUID,
# MAGIC PRODID as productId,
# MAGIC PRODUCT_GUID as productGUID,
# MAGIC CAMPAIGN as marketingCampaign,
# MAGIC LOEVM as deletedIndicator,
# MAGIC PRODCH_BEG as productBeginIndicator,
# MAGIC PRODCH_END as productChangeIndicator,
# MAGIC XREPLCNTL as replicationControls,
# MAGIC ERDAT as createdDate,
# MAGIC ERNAM as createdBy,
# MAGIC AEDAT as lastChangedDate,
# MAGIC OUCONTRACT as individualContractID,
# MAGIC AENAM as lastChangedBy
# MAGIC ,row_number() over (partition by VERTRAG, BIS order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC )a where  a.rn = 1)

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT contractId,validToDate,validFromDate,installationId,contractHeadGUID,
# MAGIC contractPosGUID,productId,productGUID,marketingCampaign,deletedIndicator,
# MAGIC productBeginIndicator,productChangeIndicator,replicationControls,createdDate,
# MAGIC createdBy,lastChangedDate,individualContractID,lastChangedBy, COUNT (*) as count
# MAGIC FROM cleansed.${vars.table}
# MAGIC GROUP BY contractId,validToDate,validFromDate,installationId,contractHeadGUID,
# MAGIC contractPosGUID,productId,productGUID,marketingCampaign,deletedIndicator,
# MAGIC productBeginIndicator,productChangeIndicator,replicationControls,createdDate,
# MAGIC createdBy,lastChangedDate,individualContractID,lastChangedBy
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY contractId,validToDate order by contractId,validToDate) as rn
# MAGIC FROM  cleansed.${vars.table}
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC (select
# MAGIC contractId,
# MAGIC validToDate,
# MAGIC validFromDate,
# MAGIC installationId,
# MAGIC contractHeadGUID,
# MAGIC contractPosGUID,
# MAGIC productId,
# MAGIC productGUID,
# MAGIC marketingCampaign,
# MAGIC deletedIndicator,
# MAGIC productBeginIndicator,
# MAGIC productChangeIndicator,
# MAGIC replicationControls,
# MAGIC createdDate,
# MAGIC createdBy,
# MAGIC lastChangedDate,
# MAGIC individualContractID,
# MAGIC lastChangedBy
# MAGIC from
# MAGIC (select
# MAGIC VERTRAG as contractId,
# MAGIC BIS as validToDate,
# MAGIC AB as validFromDate,
# MAGIC ANLAGE as installationId,
# MAGIC CONTRACTHEAD as contractHeadGUID,
# MAGIC CONTRACTPOS as contractPosGUID,
# MAGIC PRODID as productId,
# MAGIC PRODUCT_GUID as productGUID,
# MAGIC CAMPAIGN as marketingCampaign,
# MAGIC LOEVM as deletedIndicator,
# MAGIC PRODCH_BEG as productBeginIndicator,
# MAGIC PRODCH_END as productChangeIndicator,
# MAGIC XREPLCNTL as replicationControls,
# MAGIC ERDAT as createdDate,
# MAGIC ERNAM as createdBy,
# MAGIC AEDAT as lastChangedDate,
# MAGIC OUCONTRACT as individualContractID,
# MAGIC AENAM as lastChangedBy
# MAGIC ,row_number() over (partition by VERTRAG, BIS order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC )a where  a.rn = 1)
# MAGIC except
# MAGIC select 
# MAGIC contractId,
# MAGIC validToDate,
# MAGIC validFromDate,
# MAGIC installationId,
# MAGIC contractHeadGUID,
# MAGIC contractPosGUID,
# MAGIC productId,
# MAGIC productGUID,
# MAGIC marketingCampaign,
# MAGIC deletedIndicator,
# MAGIC productBeginIndicator,
# MAGIC productChangeIndicator,
# MAGIC replicationControls,
# MAGIC createdDate,
# MAGIC createdBy,
# MAGIC lastChangedDate,
# MAGIC individualContractID,
# MAGIC lastChangedBy
# MAGIC from cleansed.${vars.table}

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select 
# MAGIC contractId,
# MAGIC validToDate,
# MAGIC validFromDate,
# MAGIC installationId,
# MAGIC contractHeadGUID,
# MAGIC contractPosGUID,
# MAGIC productId,
# MAGIC productGUID,
# MAGIC marketingCampaign,
# MAGIC deletedIndicator,
# MAGIC productBeginIndicator,
# MAGIC productChangeIndicator,
# MAGIC replicationControls,
# MAGIC createdDate,
# MAGIC createdBy,
# MAGIC lastChangedDate,
# MAGIC individualContractID,
# MAGIC lastChangedBy
# MAGIC from cleansed.${vars.table}
# MAGIC except
# MAGIC (select
# MAGIC contractId,
# MAGIC validToDate,
# MAGIC validFromDate,
# MAGIC installationId,
# MAGIC contractHeadGUID,
# MAGIC contractPosGUID,
# MAGIC productId,
# MAGIC productGUID,
# MAGIC marketingCampaign,
# MAGIC deletedIndicator,
# MAGIC productBeginIndicator,
# MAGIC productChangeIndicator,
# MAGIC replicationControls,
# MAGIC createdDate,
# MAGIC createdBy,
# MAGIC lastChangedDate,
# MAGIC individualContractID,
# MAGIC lastChangedBy
# MAGIC from
# MAGIC (select
# MAGIC VERTRAG as contractId,
# MAGIC BIS as validToDate,
# MAGIC AB as validFromDate,
# MAGIC ANLAGE as installationId,
# MAGIC CONTRACTHEAD as contractHeadGUID,
# MAGIC CONTRACTPOS as contractPosGUID,
# MAGIC PRODID as productId,
# MAGIC PRODUCT_GUID as productGUID,
# MAGIC CAMPAIGN as marketingCampaign,
# MAGIC LOEVM as deletedIndicator,
# MAGIC PRODCH_BEG as productBeginIndicator,
# MAGIC PRODCH_END as productChangeIndicator,
# MAGIC XREPLCNTL as replicationControls,
# MAGIC ERDAT as createdDate,
# MAGIC ERNAM as createdBy,
# MAGIC AEDAT as lastChangedDate,
# MAGIC OUCONTRACT as individualContractID,
# MAGIC AENAM as lastChangedBy
# MAGIC ,row_number() over (partition by VERTRAG, BIS order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC )a where  a.rn = 1)
