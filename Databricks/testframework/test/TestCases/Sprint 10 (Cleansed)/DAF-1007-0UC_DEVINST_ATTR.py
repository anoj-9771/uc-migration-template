# Databricks notebook source
#config parameters
source = 'ISU' #either CRM or ISU
table = '0UC_DEVINST_ATTR'

environment = 'test'
storage_account_name = "sablobdaftest01"
storage_account_access_key = dbutils.secrets.get(scope="TestScope",key="test-sablob-key")
containerName = "archive"


# COMMAND ----------

# MAGIC %run ../../includes/tableEvaluation

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from test.${vars.table}

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed.isu_0UC_PRICCLA_TEXT

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed.isu_0UC_STATTART_TEXT

# COMMAND ----------

# DBTITLE 1,[Source] with mapping
# MAGIC %sql
# MAGIC select
# MAGIC installationId
# MAGIC ,logicalDeviceNumber
# MAGIC ,validToDate
# MAGIC ,validFromDate
# MAGIC ,priceClassCode
# MAGIC ,priceClass
# MAGIC ,payRentalPrice
# MAGIC ,rateTypeCode
# MAGIC ,rateType
# MAGIC ,deletedIndicator
# MAGIC ,bwDeltaProcess
# MAGIC ,operationCode
# MAGIC from
# MAGIC (select
# MAGIC ANLAGE	as	installationId
# MAGIC ,LOGIKNR as	logicalDeviceNumber
# MAGIC ,BIS as	validToDate
# MAGIC ,AB	as	validFromDate
# MAGIC ,PREISKLA as priceClassCode
# MAGIC ,b.priceClass as priceClass
# MAGIC ,GVERRECH as payRentalPrice
# MAGIC ,TARIFART as rateTypeCode
# MAGIC ,c.rateType as	rateType
# MAGIC ,LOEVM	as deletedIndicator
# MAGIC ,UPDMOD	as bwDeltaProcess
# MAGIC ,ZOPCODE as	operationCode
# MAGIC ,row_number() over (partition by ANLAGE,LOGIKNR,BIS order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC left join cleansed.isu_0UC_PRICCLA_TEXT b
# MAGIC on PREISKLA = b.priceClassCode 
# MAGIC left join cleansed.isu_0UC_STATTART_TEXT c
# MAGIC on TARIFART = c.rateTypeCode  
# MAGIC )a where  a.rn = 1

# COMMAND ----------

# DBTITLE 1,[Verification] Count Check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.${vars.table}
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (select
# MAGIC installationId
# MAGIC ,logicalDeviceNumber
# MAGIC ,validToDate
# MAGIC ,validFromDate
# MAGIC ,priceClassCode
# MAGIC ,priceClass
# MAGIC ,payRentalPrice
# MAGIC ,rateTypeCode
# MAGIC ,rateType
# MAGIC ,deletedIndicator
# MAGIC ,bwDeltaProcess
# MAGIC ,operationCode
# MAGIC from
# MAGIC (select
# MAGIC ANLAGE	as	installationId
# MAGIC ,LOGIKNR as	logicalDeviceNumber
# MAGIC ,BIS as	validToDate
# MAGIC ,AB	as	validFromDate
# MAGIC ,PREISKLA as priceClassCode
# MAGIC ,b.priceClass as priceClass
# MAGIC ,GVERRECH as payRentalPrice
# MAGIC ,TARIFART as rateTypeCode
# MAGIC ,c.rateType as	rateType
# MAGIC ,LOEVM	as deletedIndicator
# MAGIC ,UPDMOD	as bwDeltaProcess
# MAGIC ,ZOPCODE as	operationCode
# MAGIC ,row_number() over (partition by ANLAGE,LOGIKNR,BIS order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC left join cleansed.isu_0UC_PRICCLA_TEXT b
# MAGIC on PREISKLA = b.priceClassCode 
# MAGIC left join cleansed.isu_0UC_STATTART_TEXT c
# MAGIC on TARIFART = c.rateTypeCode  
# MAGIC )a where  a.rn = 1
# MAGIC )

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT 
# MAGIC installationId
# MAGIC ,logicalDeviceNumber
# MAGIC ,validToDate
# MAGIC , COUNT (*) as count
# MAGIC FROM cleansed.${vars.table}
# MAGIC GROUP BY 
# MAGIC installationId
# MAGIC ,logicalDeviceNumber
# MAGIC ,validToDate
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY installationId,logicalDeviceNumber,validToDate  order by installationId,logicalDeviceNumber,validToDate) as rn
# MAGIC FROM  cleansed.${vars.table}
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select
# MAGIC installationId
# MAGIC ,logicalDeviceNumber
# MAGIC ,validToDate
# MAGIC ,validFromDate
# MAGIC ,priceClassCode
# MAGIC ,priceClass
# MAGIC ,payRentalPrice
# MAGIC ,rateTypeCode
# MAGIC ,rateType
# MAGIC ,deletedIndicator
# MAGIC ,bwDeltaProcess
# MAGIC ,operationCode
# MAGIC from
# MAGIC (select
# MAGIC ANLAGE	as	installationId
# MAGIC ,LOGIKNR as	logicalDeviceNumber
# MAGIC ,BIS as	validToDate
# MAGIC ,AB	as	validFromDate
# MAGIC ,PREISKLA as priceClassCode
# MAGIC ,b.priceClass as priceClass
# MAGIC ,GVERRECH as payRentalPrice
# MAGIC ,TARIFART as rateTypeCode
# MAGIC ,c.rateType as	rateType
# MAGIC ,LOEVM	as deletedIndicator
# MAGIC ,UPDMOD	as bwDeltaProcess
# MAGIC ,ZOPCODE as	operationCode
# MAGIC ,row_number() over (partition by ANLAGE,LOGIKNR,BIS order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC left join cleansed.isu_0UC_PRICCLA_TEXT b
# MAGIC on PREISKLA = b.priceClassCode 
# MAGIC left join cleansed.isu_0UC_STATTART_TEXT c
# MAGIC on TARIFART = c.rateTypeCode  
# MAGIC )a where  a.rn = 1
# MAGIC except
# MAGIC select
# MAGIC installationId
# MAGIC ,logicalDeviceNumber
# MAGIC ,validToDate
# MAGIC ,validFromDate
# MAGIC ,priceClassCode
# MAGIC ,priceClass
# MAGIC ,payRentalPrice
# MAGIC ,rateTypeCode
# MAGIC ,rateType
# MAGIC ,deletedIndicator
# MAGIC ,bwDeltaProcess
# MAGIC ,operationCode
# MAGIC from
# MAGIC cleansed.${vars.table}

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC installationId
# MAGIC ,logicalDeviceNumber
# MAGIC ,validToDate
# MAGIC ,validFromDate
# MAGIC ,priceClassCode
# MAGIC ,priceClass
# MAGIC ,payRentalPrice
# MAGIC ,rateTypeCode
# MAGIC ,rateType
# MAGIC ,deletedIndicator
# MAGIC ,bwDeltaProcess
# MAGIC ,operationCode
# MAGIC from
# MAGIC cleansed.${vars.table}
# MAGIC except
# MAGIC select
# MAGIC installationId
# MAGIC ,logicalDeviceNumber
# MAGIC ,validToDate
# MAGIC ,validFromDate
# MAGIC ,priceClassCode
# MAGIC ,priceClass
# MAGIC ,payRentalPrice
# MAGIC ,rateTypeCode
# MAGIC ,rateType
# MAGIC ,deletedIndicator
# MAGIC ,bwDeltaProcess
# MAGIC ,operationCode
# MAGIC from
# MAGIC (select
# MAGIC ANLAGE	as	installationId
# MAGIC ,LOGIKNR as	logicalDeviceNumber
# MAGIC ,BIS as	validToDate
# MAGIC ,AB	as	validFromDate
# MAGIC ,PREISKLA as priceClassCode
# MAGIC ,b.priceClass as priceClass
# MAGIC ,GVERRECH as payRentalPrice
# MAGIC ,TARIFART as rateTypeCode
# MAGIC ,c.rateType as	rateType
# MAGIC ,LOEVM	as deletedIndicator
# MAGIC ,UPDMOD	as bwDeltaProcess
# MAGIC ,ZOPCODE as	operationCode
# MAGIC ,row_number() over (partition by ANLAGE,LOGIKNR,BIS order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC left join cleansed.isu_0UC_PRICCLA_TEXT b
# MAGIC on PREISKLA = b.priceClassCode 
# MAGIC left join cleansed.isu_0UC_STATTART_TEXT c
# MAGIC on TARIFART = c.rateTypeCode  
# MAGIC )a where  a.rn = 1
