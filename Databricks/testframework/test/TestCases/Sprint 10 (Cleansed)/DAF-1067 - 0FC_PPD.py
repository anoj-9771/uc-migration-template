# Databricks notebook source
#config parameters
source = 'ISU' #either CRM or ISU
table = '0FC_PPD'

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
# MAGIC promiseToPayId,
# MAGIC paymentDatePromised,
# MAGIC paymentAmountPromised,
# MAGIC promisedAmountOpen,
# MAGIC currency,
# MAGIC amount2,
# MAGIC amount1,
# MAGIC createdDate
# MAGIC from
# MAGIC (select
# MAGIC PPKEY as promiseToPayId,
# MAGIC PRDAT as paymentDatePromised,
# MAGIC PRAMT as paymentAmountPromised,
# MAGIC PRAMO as promisedAmountOpen,
# MAGIC PRCUR as currency,
# MAGIC FDDBT as amount2,
# MAGIC FDDBO as amount1,
# MAGIC ERDAT as createdDate,
# MAGIC row_number() over (partition by PPKEY,PRDAT order by PPKEY,PRDAT desc) as rn
# MAGIC from test.${vars.table} 
# MAGIC )a  
# MAGIC  where  a.rn = 1

# COMMAND ----------

# DBTITLE 1,[Verification] Count Check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.${vars.table}
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (select
# MAGIC promiseToPayId,
# MAGIC paymentDatePromised,
# MAGIC paymentAmountPromised,
# MAGIC promisedAmountOpen,
# MAGIC currency,
# MAGIC amount2,
# MAGIC amount1,
# MAGIC createdDate
# MAGIC from
# MAGIC (select
# MAGIC PPKEY as promiseToPayId,
# MAGIC PRDAT as paymentDatePromised,
# MAGIC PRAMT as paymentAmountPromised,
# MAGIC PRAMO as promisedAmountOpen,
# MAGIC PRCUR as currency,
# MAGIC FDDBT as amount2,
# MAGIC FDDBO as amount1,
# MAGIC ERDAT as createdDate,
# MAGIC row_number() over (partition by PPKEY,PRDAT order by PPKEY,PRDAT desc) as rn
# MAGIC from test.${vars.table} 
# MAGIC )a  
# MAGIC  where  a.rn = 1)

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT 
# MAGIC propmiseToPayId,paymentDatePromised,paymentAmountPromised,
# MAGIC promisedAmountOpen,currency,amount2,amount1,createdDate
# MAGIC , COUNT (*) as count
# MAGIC FROM cleansed.${vars.table}
# MAGIC GROUP BY propmiseToPayId,paymentDatePromised,paymentAmountPromised,
# MAGIC promisedAmountOpen,currency,amount2,amount1,createdDate
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY propmiseToPayId,paymentDatePromised  order by propmiseToPayId,paymentDatePromised) as rn
# MAGIC FROM  cleansed.${vars.table}
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select
# MAGIC promiseToPayId,
# MAGIC paymentDatePromised,
# MAGIC paymentAmountPromised,
# MAGIC promisedAmountOpen,
# MAGIC currency,
# MAGIC amount2,
# MAGIC amount1,
# MAGIC createdDate
# MAGIC from
# MAGIC (select
# MAGIC PPKEY as promiseToPayId,
# MAGIC PRDAT as paymentDatePromised,
# MAGIC PRAMT as paymentAmountPromised,
# MAGIC PRAMO as promisedAmountOpen,
# MAGIC PRCUR as currency,
# MAGIC FDDBT as amount2,
# MAGIC FDDBO as amount1,
# MAGIC ERDAT as createdDate,
# MAGIC row_number() over (partition by PPKEY,PRDAT order by PPKEY,PRDAT desc) as rn
# MAGIC from test.${vars.table} 
# MAGIC )a  
# MAGIC  where  a.rn = 1
# MAGIC except
# MAGIC select
# MAGIC propmiseToPayId,
# MAGIC paymentDatePromised,
# MAGIC paymentAmountPromised,
# MAGIC promisedAmountOpen,
# MAGIC currency,
# MAGIC amount2,
# MAGIC amount1,
# MAGIC createdDate
# MAGIC from
# MAGIC cleansed.${vars.table}

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC propmiseToPayId,
# MAGIC paymentDatePromised,
# MAGIC paymentAmountPromised,
# MAGIC promisedAmountOpen,
# MAGIC currency,
# MAGIC amount2,
# MAGIC amount1,
# MAGIC createdDate
# MAGIC from
# MAGIC cleansed.${vars.table}
# MAGIC except
# MAGIC select
# MAGIC promiseToPayId,
# MAGIC paymentDatePromised,
# MAGIC paymentAmountPromised,
# MAGIC promisedAmountOpen,
# MAGIC currency,
# MAGIC amount2,
# MAGIC amount1,
# MAGIC createdDate
# MAGIC from
# MAGIC (select
# MAGIC PPKEY as promiseToPayId,
# MAGIC PRDAT as paymentDatePromised,
# MAGIC PRAMT as paymentAmountPromised,
# MAGIC PRAMO as promisedAmountOpen,
# MAGIC PRCUR as currency,
# MAGIC FDDBT as amount2,
# MAGIC FDDBO as amount1,
# MAGIC ERDAT as createdDate,
# MAGIC row_number() over (partition by PPKEY,PRDAT order by PPKEY,PRDAT desc) as rn
# MAGIC from test.${vars.table} 
# MAGIC )a  
# MAGIC  where  a.rn = 1
