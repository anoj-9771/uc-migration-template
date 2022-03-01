# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup
storage_account_name = "sadaftest01"
storage_account_access_key = dbutils.secrets.get(scope="Test-Access",key="test-datalake-key")
container_name = "raw"
file_location = "wasbs://raw@sadaftest01.blob.core.windows.net/landing/accessarchive/Z309_TMETERREADING.csv"
file_type = "csv"
print(storage_account_name)

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)


# COMMAND ----------

# DBTITLE 1,[Source] loading to a dataframe
 df = spark.read.format("csv").option('delimiter','|').option('header','true').load("wasbs://raw@sadaftest01.blob.core.windows.net/landing/accessarchive/Z309_TMETEREADING.csv")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.createOrReplaceTempView("Source")

# COMMAND ----------

cleansedf = spark.sql("select * from cleansed.t_access_z309_tdebitreason")

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
cleansedf.printSchema()

# COMMAND ----------

cleansedf.createOrReplaceTempView("Target")

# COMMAND ----------

# DBTITLE 1,[Source] Displaying Records
# MAGIC %sql
# MAGIC select * from Source

# COMMAND ----------

# DBTITLE 1,[Source] After applying mapping
# MAGIC %sql
# MAGIC select
# MAGIC C_DEBI_TYPE as debitTypeCode,
# MAGIC C_DEBI_REAS as debitReasonCode,
# MAGIC T_DEBI_REAS as debitReason,
# MAGIC case when D_DEBI_REAS_EFFE = '00000000' then null
# MAGIC when D_DEBI_REAS_EFFE <> 'null' then CONCAT(LEFT(D_DEBI_REAS_EFFE,4),'-',SUBSTRING(D_DEBI_REAS_EFFE,5,2),'-',RIGHT(D_DEBI_REAS_EFFE,2)) else D_DEBI_REAS_EFFE end as debitReasonEffectiveDate,
# MAGIC case when D_DEBI_REAS_CANC <> 'null' then CONCAT(LEFT(D_DEBI_REAS_CANC,4),'-',SUBSTRING(D_DEBI_REAS_CANC,5,2),'-',RIGHT(D_DEBI_REAS_CANC,2)) else D_DEBI_REAS_CANC end as debitReasonCancelledDate,
# MAGIC C_REVE_TYPE as reversalTypeCode,
# MAGIC C_SURC_CATE as surchargeCategoryCode,
# MAGIC F_ACGL_DEBI_REQD as isGeneralLedgerDebitRequired,
# MAGIC N_ACGL_ACCT_DR as generalLedgerAccountDebitNumber,
# MAGIC N_ACGL_ACCT_CR as generalLedgerAccountCreditNumber,
# MAGIC T_DEBI_REAS_CERT as debitReasonCertificate,
# MAGIC T_DEBI_REAS_ABBR as debitReasonAbbreviation,
# MAGIC F_ACGL_SPLI_REQU as isGeneralLedgerSplitRequired
# MAGIC from source
# MAGIC --2008-05-17T07:00:00.000+0000 2008-05-17 07:00:00 AM
# MAGIC --case when D_RATA_TYPE_EFFE <> 'null' then CONCAT(LEFT(D_RATA_TYPE_EFFE,4),'-',SUBSTRING(D_RATA_TYPE_EFFE,5,2),'-',RIGHT(D_RATA_TYPE_EFFE,2)) else D_RATA_TYPE_EFFE end as rateabilityTypeEffectiveDate,
# MAGIC --,CONCAT(LEFT (H_MODI,10),'T',SUBSTRING(H_MODI,12,8),'.000+0000') as modifiedTimestamp

# COMMAND ----------

# DBTITLE 1,[Target] Displaying records
# MAGIC %sql
# MAGIC select 
# MAGIC debitTypeCode
# MAGIC ,debitReasonCode
# MAGIC ,debitReason
# MAGIC ,debitReasonEffectiveDate
# MAGIC ,debitReasonCancelledDate
# MAGIC ,reversalTypeCode
# MAGIC ,surchargeCategoryCode
# MAGIC ,isGeneralLedgerDebitRequired
# MAGIC ,generalLedgerAccountDebitNumber
# MAGIC ,generalLedgerAccountCreditNumber
# MAGIC ,debitReasonCertificate
# MAGIC ,debitReasonAbbreviation
# MAGIC ,isGeneralLedgerSplitRequired
# MAGIC from cleansed.t_access_z309_tdebitreason

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate checks
# MAGIC %sql
# MAGIC SELECT debittypecode, debitreasoncode, COUNT (*) as count
# MAGIC FROM cleansed.t_access_z309_tdebitreason
# MAGIC GROUP BY debittypecode, debitreasoncode
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Records count check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.t_access_z309_tdebitreason
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from Source

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select
# MAGIC C_DEBI_TYPE as debitTypeCode,
# MAGIC C_DEBI_REAS as debitReasonCode,
# MAGIC T_DEBI_REAS as debitReason,
# MAGIC case when D_DEBI_REAS_EFFE = '00000000' then null
# MAGIC when D_DEBI_REAS_EFFE <> 'null' then CONCAT(LEFT(D_DEBI_REAS_EFFE,4),'-',SUBSTRING(D_DEBI_REAS_EFFE,5,2),'-',RIGHT(D_DEBI_REAS_EFFE,2)) else D_DEBI_REAS_EFFE end as debitReasonEffectiveDate,
# MAGIC case when D_DEBI_REAS_CANC <> 'null' then CONCAT(LEFT(D_DEBI_REAS_CANC,4),'-',SUBSTRING(D_DEBI_REAS_CANC,5,2),'-',RIGHT(D_DEBI_REAS_CANC,2)) else D_DEBI_REAS_CANC end as debitReasonCancelledDate,
# MAGIC C_REVE_TYPE as reversalTypeCode,
# MAGIC C_SURC_CATE as surchargeCategoryCode,
# MAGIC F_ACGL_DEBI_REQD as isGeneralLedgerDebitRequired,
# MAGIC N_ACGL_ACCT_DR as generalLedgerAccountDebitNumber,
# MAGIC N_ACGL_ACCT_CR as generalLedgerAccountCreditNumber,
# MAGIC T_DEBI_REAS_CERT as debitReasonCertificate,
# MAGIC T_DEBI_REAS_ABBR as debitReasonAbbreviation,
# MAGIC F_ACGL_SPLI_REQU as isGeneralLedgerSplitRequired
# MAGIC from source
# MAGIC except
# MAGIC select 
# MAGIC debitTypeCode
# MAGIC ,debitReasonCode
# MAGIC ,upper(debitReason)
# MAGIC ,debitReasonEffectiveDate
# MAGIC ,debitReasonCancelledDate
# MAGIC ,reversalTypeCode
# MAGIC ,surchargeCategoryCode
# MAGIC ,isGeneralLedgerDebitRequired
# MAGIC ,generalLedgerAccountDebitNumber
# MAGIC ,generalLedgerAccountCreditNumber
# MAGIC ,debitReasonCertificate
# MAGIC ,debitReasonAbbreviation
# MAGIC ,isGeneralLedgerSplitRequired
# MAGIC from cleansed.t_access_z309_tdebitreason

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select 
# MAGIC debitTypeCode
# MAGIC ,debitReasonCode
# MAGIC ,upper(debitReason)
# MAGIC ,debitReasonEffectiveDate
# MAGIC ,debitReasonCancelledDate
# MAGIC ,reversalTypeCode
# MAGIC ,surchargeCategoryCode
# MAGIC ,isGeneralLedgerDebitRequired
# MAGIC ,generalLedgerAccountDebitNumber
# MAGIC ,generalLedgerAccountCreditNumber
# MAGIC ,debitReasonCertificate
# MAGIC ,debitReasonAbbreviation
# MAGIC ,isGeneralLedgerSplitRequired
# MAGIC from cleansed.t_access_z309_tdebitreason
# MAGIC except
# MAGIC select
# MAGIC C_DEBI_TYPE as debitTypeCode,
# MAGIC C_DEBI_REAS as debitReasonCode,
# MAGIC T_DEBI_REAS as debitReason,
# MAGIC case when D_DEBI_REAS_EFFE = '00000000' then null
# MAGIC when D_DEBI_REAS_EFFE <> 'null' then CONCAT(LEFT(D_DEBI_REAS_EFFE,4),'-',SUBSTRING(D_DEBI_REAS_EFFE,5,2),'-',RIGHT(D_DEBI_REAS_EFFE,2)) else D_DEBI_REAS_EFFE end as debitReasonEffectiveDate,
# MAGIC case when D_DEBI_REAS_CANC <> 'null' then CONCAT(LEFT(D_DEBI_REAS_CANC,4),'-',SUBSTRING(D_DEBI_REAS_CANC,5,2),'-',RIGHT(D_DEBI_REAS_CANC,2)) else D_DEBI_REAS_CANC end as debitReasonCancelledDate,
# MAGIC C_REVE_TYPE as reversalTypeCode,
# MAGIC C_SURC_CATE as surchargeCategoryCode,
# MAGIC F_ACGL_DEBI_REQD as isGeneralLedgerDebitRequired,
# MAGIC N_ACGL_ACCT_DR as generalLedgerAccountDebitNumber,
# MAGIC N_ACGL_ACCT_CR as generalLedgerAccountCreditNumber,
# MAGIC T_DEBI_REAS_CERT as debitReasonCertificate,
# MAGIC T_DEBI_REAS_ABBR as debitReasonAbbreviation,
# MAGIC F_ACGL_SPLI_REQU as isGeneralLedgerSplitRequired
# MAGIC from source
