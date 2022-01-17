# Databricks notebook source
# DBTITLE 1,Target
lakedftarget = spark.sql("select * from curated.dimContract")
display(lakedftarget)

# COMMAND ----------

lakedftarget.printSchema()

# COMMAND ----------

# DBTITLE 1,[Source] Applying Transformation
# MAGIC %sql
# MAGIC select
# MAGIC c.contractId as contractId
# MAGIC ,case when ch.validFromDate is null then '1900-01-01' else ch.validFromDate end as validFromDate
# MAGIC ,ch.validToDate as validToDate
# MAGIC ,'ISU' as sourceSystemCode
# MAGIC ,case
# MAGIC when c.createdDate < ch.validFromDate then c.createdDate
# MAGIC else ch.validFromDate end as contractStartDate
# MAGIC ,ch.validToDate as contractEndDate
# MAGIC ,case
# MAGIC when c.invoiceContractsJointly = 'X' then 'Y'
# MAGIC else 'N' end as invoiceJointlyFlag
# MAGIC ,c.moveInDate as moveInDate
# MAGIC ,c.moveOutDate as moveOutDate
# MAGIC ,ca.contractAccountNumber as contractAccountNumber
# MAGIC ,ca.contractAccountCategory as contractAccountCategory
# MAGIC ,ca.applicationArea as applicationArea
# MAGIC from cleansed.isu_0UCCONTRACT_ATTR_2 c
# MAGIC Left join cleansed.isu_0UCCONTRACTH_ATTR_2 ch 
# MAGIC on c.contractId = ch.contractId
# MAGIC Left join cleansed.isu_0CACONT_ACC_ATTR_2 ca
# MAGIC on c.contractAccountNumber = ca.contractAccountNumber
# MAGIC where c._RecordDeleted = 0
# MAGIC and c._RecordCurrent = 1
# MAGIC and ch._RecordDeleted = 0
# MAGIC and ch._RecordCurrent = 1
# MAGIC and ca._RecordDeleted = 0
# MAGIC and ca._RecordCurrent = 1

# COMMAND ----------

# DBTITLE 1,[Verification] Auto Generate field check
# MAGIC %sql
# MAGIC select * from curated.dimContract where dimcontractSK is null or dimcontractSK = '' or dimcontractSK = ' '

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Check
# MAGIC %sql
# MAGIC --duplicate check of Surrogate key
# MAGIC SELECT dimContractSK,COUNT (*) as count
# MAGIC FROM curated.dimContract
# MAGIC GROUP BY dimContractSK
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,version 1.1.1
# MAGIC %sql
# MAGIC --duplicate check of Surrogate key
# MAGIC SELECT dimInstallationSK,COUNT (*) as count
# MAGIC FROM curated.dimContract
# MAGIC GROUP BY dimInstallationSK
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT contractId,validFromDate,COUNT (*) as count
# MAGIC FROM curated.dimContract
# MAGIC GROUP BY contractId,validFromDate
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated.dimContract where contractId = -1

# COMMAND ----------

# DBTITLE 1,[Verification] Count Check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from curated.dimContract
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (
# MAGIC select
# MAGIC c.contractId as contractId
# MAGIC --,ch.validFromDate as validFromDate
# MAGIC ,case when ch.validFromDate is null then '1900-01-01' else ch.validFromDate end as validFromDate
# MAGIC ,ch.validToDate as validToDate
# MAGIC ,'ISU' as sourceSystemCode
# MAGIC ,case
# MAGIC when c.createdDate < ch.validFromDate then c.createdDate
# MAGIC else ch.validFromDate end as contractStartDate
# MAGIC ,ch.validToDate as contractEndDate
# MAGIC ,case
# MAGIC when c.invoiceContractsJointly = 'X' then 'Y'
# MAGIC else 'N' end as invoiceJointlyFlag
# MAGIC ,c.moveInDate as moveInDate
# MAGIC ,c.moveOutDate as moveOutDate
# MAGIC ,ca.contractAccountNumber as contractAccountNumber
# MAGIC ,ca.contractAccountCategory as contractAccountCategory
# MAGIC ,ca.applicationArea as applicationArea
# MAGIC from cleansed.isu_0UCCONTRACT_ATTR_2 c
# MAGIC Left join cleansed.isu_0UCCONTRACTH_ATTR_2 ch 
# MAGIC on c.contractId = ch.contractId
# MAGIC Left join cleansed.isu_0CACONT_ACC_ATTR_2 ca
# MAGIC on c.contractAccountNumber = ca.contractAccountNumber
# MAGIC where c._RecordDeleted = 0
# MAGIC and c._RecordCurrent = 1
# MAGIC and ch._RecordDeleted = 0
# MAGIC and ch._RecordCurrent = 1
# MAGIC and ca._RecordDeleted = 0
# MAGIC and ca._RecordCurrent = 1)
# MAGIC --where c.createdDate is not null and ch.validFromDate is not null)--c where c.rn =1

# COMMAND ----------

# DBTITLE 1,[Verification] Source vs Target
# MAGIC %sql
# MAGIC select
# MAGIC c.contractId as contractId
# MAGIC ,case when ch.validFromDate is null then '1900-01-01' else ch.validFromDate end as validFromDate
# MAGIC ,ch.validToDate as validToDate
# MAGIC ,'ISU' as sourceSystemCode
# MAGIC ,case
# MAGIC when (ch.validFromDate < c.createdDate and ch.validFromDate is not null) then ch.validFromDate
# MAGIC else c.createdDate end as contractStartDate
# MAGIC ,ch.validToDate as contractEndDate
# MAGIC ,case
# MAGIC when c.invoiceContractsJointly = 'X' then 'Y'
# MAGIC else 'N' end as invoiceJointlyFlag
# MAGIC ,c.moveInDate as moveInDate
# MAGIC ,c.moveOutDate as moveOutDate
# MAGIC ,ca.contractAccountNumber as contractAccountNumber
# MAGIC ,ca.contractAccountCategory as contractAccountCategory
# MAGIC ,ca.applicationArea as applicationArea
# MAGIC from cleansed.isu_0UCCONTRACT_ATTR_2 c
# MAGIC Left join cleansed.isu_0UCCONTRACTH_ATTR_2 ch 
# MAGIC on c.contractId = ch.contractId
# MAGIC Left join cleansed.isu_0CACONT_ACC_ATTR_2 ca
# MAGIC on c.contractAccountNumber = ca.contractAccountNumber
# MAGIC where c._RecordDeleted = 0
# MAGIC and c._RecordCurrent = 1
# MAGIC and ch._RecordDeleted = 0
# MAGIC and ch._RecordCurrent = 1
# MAGIC and ca._RecordDeleted = 0
# MAGIC and ca._RecordCurrent = 1
# MAGIC except
# MAGIC select
# MAGIC contractId,
# MAGIC validFromDate,
# MAGIC validToDate,
# MAGIC sourceSystemCode,
# MAGIC contractStartDate,
# MAGIC contractEndDate,
# MAGIC invoiceJointlyFlag,
# MAGIC moveInDate,
# MAGIC moveOutDate,
# MAGIC contractAccountNumber,
# MAGIC contractAccountCategory,
# MAGIC applicationArea
# MAGIC from curated.dimContract

# COMMAND ----------

# DBTITLE 1,[Verification] Target vs  Source 
# MAGIC %sql
# MAGIC select
# MAGIC contractId,
# MAGIC validFromDate,
# MAGIC validToDate,
# MAGIC sourceSystemCode,
# MAGIC contractStartDate,
# MAGIC contractEndDate,
# MAGIC invoiceJointlyFlag,
# MAGIC moveInDate,
# MAGIC moveOutDate,
# MAGIC contractAccountNumber,
# MAGIC contractAccountCategory,
# MAGIC applicationArea
# MAGIC from curated.dimContract
# MAGIC except
# MAGIC select
# MAGIC c.contractId as contractId
# MAGIC ,case when ch.validFromDate is null then '1900-01-01' else ch.validFromDate end as validFromDate
# MAGIC ,ch.validToDate as validToDate
# MAGIC ,'ISU' as sourceSystemCode
# MAGIC ,case
# MAGIC when (ch.validFromDate < c.createdDate and ch.validFromDate is not null) then ch.validFromDate
# MAGIC else c.createdDate end as contractStartDate
# MAGIC ,ch.validToDate as contractEndDate
# MAGIC ,case
# MAGIC when c.invoiceContractsJointly = 'X' then 'Y'
# MAGIC else 'N' end as invoiceJointlyFlag
# MAGIC ,c.moveInDate as moveInDate
# MAGIC ,c.moveOutDate as moveOutDate
# MAGIC ,ca.contractAccountNumber as contractAccountNumber
# MAGIC ,ca.contractAccountCategory as contractAccountCategory
# MAGIC ,ca.applicationArea as applicationArea
# MAGIC from cleansed.isu_0UCCONTRACT_ATTR_2 c
# MAGIC Left join cleansed.isu_0UCCONTRACTH_ATTR_2 ch 
# MAGIC on c.contractId = ch.contractId
# MAGIC Left join cleansed.isu_0CACONT_ACC_ATTR_2 ca
# MAGIC on c.contractAccountNumber = ca.contractAccountNumber
# MAGIC where c._RecordDeleted = 0
# MAGIC and c._RecordCurrent = 1
# MAGIC and ch._RecordDeleted = 0
# MAGIC and ch._RecordCurrent = 1
# MAGIC and ca._RecordDeleted = 0
# MAGIC and ca._RecordCurrent = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC contractId,
# MAGIC validFromDate,
# MAGIC validToDate,
# MAGIC sourceSystemCode,
# MAGIC contractStartDate,
# MAGIC contractEndDate,
# MAGIC invoiceJointlyFlag,
# MAGIC moveInDate,
# MAGIC moveOutDate,
# MAGIC contractAccountNumber,
# MAGIC contractAccountCategory,
# MAGIC applicationArea
# MAGIC from curated.dimContract
# MAGIC except
# MAGIC select * from(
# MAGIC select
# MAGIC c.contractId as contractId
# MAGIC ,case when ch.validFromDate is null then '1900-01-01' else ch.validFromDate end as validFromDate
# MAGIC ,ch.validToDate as validToDate
# MAGIC ,'ISU' as sourceSystemCode
# MAGIC ,case
# MAGIC when (ch.validFromDate < c.createdDate and ch.validFromDate is not null) then ch.validFromDate
# MAGIC else c.createdDate end as contractStartDate
# MAGIC ,ch.validToDate as contractEndDate
# MAGIC ,case
# MAGIC when c.invoiceContractsJointly = 'X' then 'Y'
# MAGIC else 'N' end as invoiceJointlyFlag
# MAGIC ,c.moveInDate as moveInDate
# MAGIC ,c.moveOutDate as moveOutDate
# MAGIC ,ca.contractAccountNumber as contractAccountNumber
# MAGIC ,ca.contractAccountCategory as contractAccountCategory
# MAGIC ,ca.applicationArea as applicationArea
# MAGIC from cleansed.isu_0UCCONTRACT_ATTR_2 c
# MAGIC Left join cleansed.isu_0UCCONTRACTH_ATTR_2 ch 
# MAGIC on c.contractId = ch.contractId
# MAGIC Left join cleansed.isu_0CACONT_ACC_ATTR_2 ca
# MAGIC on c.contractAccountNumber = ca.contractAccountNumber
# MAGIC where c._RecordDeleted = 0
# MAGIC and c._RecordCurrent = 1
# MAGIC and ch._RecordDeleted = 0
# MAGIC and ch._RecordCurrent = 1
# MAGIC and ca._RecordDeleted = 0
# MAGIC and ca._RecordCurrent = 1
# MAGIC union all
# MAGIC select * from (
# MAGIC select 
# MAGIC '-1' as contractId,
# MAGIC null as validFromDate,
# MAGIC null as validToDate,
# MAGIC 'ISU' as sourceSystemCode,
# MAGIC null as contractStartDate,
# MAGIC null as contractEndDate,
# MAGIC null as invoiceJointlyFlag,
# MAGIC null as moveInDate,
# MAGIC null as moveOutDate,
# MAGIC null as contractAccountNumber,
# MAGIC null as contractAccountCategory,
# MAGIC null as applicationArea
# MAGIC  from  cleansed.isu_0UCCONTRACT_ATTR_2  limit 1)d)e
