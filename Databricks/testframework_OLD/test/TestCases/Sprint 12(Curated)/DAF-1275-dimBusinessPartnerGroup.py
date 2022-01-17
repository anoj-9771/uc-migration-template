# Databricks notebook source
# DBTITLE 1,Target
# MAGIC %sql
# MAGIC select * from curated.dimBusinessPartnerGroup --where paymentAssistSchemeFlag = 'Y'

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
lakedftarget = spark.sql("select * from curated.dimBusinessPartnerGroup")
lakedftarget.printSchema()

# COMMAND ----------

# DBTITLE 1,[Source] Applying Transformation
# MAGIC %sql
# MAGIC select * from
# MAGIC (select
# MAGIC a.businessPartnerNumber as businessPartnerGroupNumber
# MAGIC ,'ISU' as sourceSystemCode
# MAGIC ,a.validFromDate as validFromDate
# MAGIC ,a.validToDate as validToDate
# MAGIC ,a.businessPartnerGroupCode as businessPartnerGroupCode
# MAGIC ,a.businessPartnerGroup as businessPartnerGroup
# MAGIC ,a.nameGroup1 as businessPartnerGroupName1
# MAGIC ,a.nameGroup2 as businessPartnerGroupName2
# MAGIC ,a.externalBusinessPartnerNumber as externalNumber
# MAGIC ,case
# MAGIC when b.paymentAssistSchemeIndicator = 'X' then 'Y'
# MAGIC else 'N' end as paymentAssistSchemeFlag
# MAGIC ,case
# MAGIC when b.billAssistIndicator = 'X' then 'Y'
# MAGIC else 'N' end as billAssistFlag
# MAGIC ,case
# MAGIC when b.kidneyDialysisIndicator = 'X' then 'Y'
# MAGIC else 'N' end as kidneyDialysisFlag
# MAGIC ,a.createdDateTime as createdDateTime
# MAGIC ,a.createdBy as createdBy
# MAGIC ,a.changedDateTime as lastUpdatedDateTime
# MAGIC ,a.changedBy as lastUpdatedBy
# MAGIC from cleansed.isu_0bpartner_attr a
# MAGIC left join cleansed.crm_0bpartner_attr b
# MAGIC on a.businesspartnernumber = b.businesspartnernumber
# MAGIC where a.businessPartnerCategoryCode = 3
# MAGIC union all
# MAGIC 
# MAGIC select * from(
# MAGIC select 
# MAGIC '-1' as businessPartnerGroupNumber,
# MAGIC 'ISU' as sourceSystemCode,
# MAGIC 'null' as validFromDate,
# MAGIC 'null' as validToDate,
# MAGIC 'null' as businessPartnerGroupCode,
# MAGIC 'null' as businessPartnerGroup,
# MAGIC 'null' as businessPartnerGroupName1,
# MAGIC 'null' as businessPartnerGroupName2,
# MAGIC 'null' as externalNumber,
# MAGIC 'null' as paymentAssistSchemeFlag,
# MAGIC 'null' as billAssistFlag,
# MAGIC 'null' as kidneyDialysisFlag,
# MAGIC 'null' as createdDateTime,
# MAGIC 'null' as createdBy,
# MAGIC 'null' as lastUpdatedDateTime,
# MAGIC 'null' as lastUpdatedBy
# MAGIC from  cleansed.isu_0bpartner_attr limit 1))d

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated.dimbusinesspartnergroup where businesspartnergroupnumber ='-1' --7837792, 7837815

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC businessPartnerNumber,*
# MAGIC from
# MAGIC cleansed.crm_0bpartner_attr

# COMMAND ----------

# DBTITLE 1,[Verification] Auto Generate field check
# MAGIC %sql
# MAGIC select * from curated.dimBusinessPartnerGroup where dimbusinessPartnerGroupSK is null or dimbusinessPartnerGroupSK = '' or dimbusinessPartnerGroupSK = ' '

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Check
# MAGIC %sql
# MAGIC --duplicate check of Surrogate key
# MAGIC SELECT dimbusinessPartnerGroupSK,COUNT (*) as count
# MAGIC FROM curated.dimBusinessPartnerGroup
# MAGIC GROUP BY dimbusinessPartnerGroupSK
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Check
# MAGIC %sql
# MAGIC --duplicate check of unique key
# MAGIC SELECT businessPartnerGroupNumber,COUNT (*) as count
# MAGIC FROM curated.dimBusinessPartnerGroup
# MAGIC GROUP BY businessPartnerGroupNumber
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Count Check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from curated.dimBusinessPartnerGroup
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (
# MAGIC select * from
# MAGIC (select
# MAGIC a.businessPartnerNumber as businessPartnerGroupNumber
# MAGIC ,'ISU' as sourceSystemCode
# MAGIC ,a.validFromDate as validFromDate
# MAGIC ,a.validToDate as validToDate
# MAGIC ,a.businessPartnerGroupCode as businessPartnerGroupCode
# MAGIC ,a.businessPartnerGroup as businessPartnerGroup
# MAGIC ,a.nameGroup1 as businessPartnerGroupName1
# MAGIC ,a.nameGroup2 as businessPartnerGroupName2
# MAGIC ,a.externalBusinessPartnerNumber as externalNumber
# MAGIC ,case
# MAGIC when b.paymentAssistSchemeIndicator = 'X' then 'Y'
# MAGIC else 'N' end as paymentAssistSchemeFlag
# MAGIC ,case
# MAGIC when b.billAssistIndicator = 'X' then 'Y'
# MAGIC else 'N' end as billAssistFlag
# MAGIC ,case
# MAGIC when b.kidneyDialysisIndicator = 'X' then 'Y'
# MAGIC else 'N' end as kidneyDialysisFlag
# MAGIC ,a.createdDateTime as createdDateTime
# MAGIC ,a.createdBy as createdBy
# MAGIC ,a.changedDateTime as lastUpdatedDateTime
# MAGIC ,a.changedBy as lastUpdatedBy
# MAGIC from cleansed.isu_0bpartner_attr a
# MAGIC left join cleansed.crm_0bpartner_attr b
# MAGIC on a.businesspartnernumber = b.businesspartnernumber
# MAGIC where a.businessPartnerCategoryCode = 3
# MAGIC union all
# MAGIC 
# MAGIC select * from(
# MAGIC select 
# MAGIC '-1' as businessPartnerGroupNumber,
# MAGIC 'ISU' as sourceSystemCode,
# MAGIC 'null' as validFromDate,
# MAGIC 'null' as validToDate,
# MAGIC 'null' as businessPartnerGroupCode,
# MAGIC 'null' as businessPartnerGroup,
# MAGIC 'null' as businessPartnerGroupName1,
# MAGIC 'null' as businessPartnerGroupName2,
# MAGIC 'null' as externalNumber,
# MAGIC 'null' as paymentAssistSchemeFlag,
# MAGIC 'null' as billAssistFlag,
# MAGIC 'null' as kidneyDialysisFlag,
# MAGIC 'null' as createdDateTime,
# MAGIC 'null' as createdBy,
# MAGIC 'null' as lastUpdatedDateTime,
# MAGIC 'null' as lastUpdatedBy
# MAGIC from  cleansed.isu_0bpartner_attr limit 1)d)e)--c where c.rn =1)

# COMMAND ----------

# DBTITLE 1,[Verification] Source vs Target
# MAGIC %sql
# MAGIC select * from(
# MAGIC select
# MAGIC a.businessPartnerNumber as businessPartnerGroupNumber
# MAGIC ,'ISU' as sourceSystemCode
# MAGIC ,a.validFromDate as validFromDate
# MAGIC ,a.validToDate as validToDate
# MAGIC ,a.businessPartnerGroupCode as businessPartnerGroupCode
# MAGIC ,a.businessPartnerGroup as businessPartnerGroup
# MAGIC ,a.nameGroup1 as businessPartnerGroupName1
# MAGIC ,a.nameGroup2 as businessPartnerGroupName2
# MAGIC ,a.externalBusinessPartnerNumber as externalNumber
# MAGIC ,case
# MAGIC when b.paymentAssistSchemeIndicator = 'X' then 'Y'
# MAGIC else 'N' end as paymentAssistSchemeFlag
# MAGIC ,case
# MAGIC when b.billAssistIndicator = 'X' then 'Y'
# MAGIC else 'N' end as billAssistFlag
# MAGIC ,case
# MAGIC when b.kidneyDialysisIndicator = 'X' then 'Y'
# MAGIC else 'N' end as kidneyDialysisFlag
# MAGIC ,a.createdDateTime as createdDateTime
# MAGIC ,a.createdBy as createdBy
# MAGIC ,a.changedDateTime as lastUpdatedDateTime
# MAGIC ,a.changedBy as lastUpdatedBy
# MAGIC from cleansed.isu_0bpartner_attr a
# MAGIC left join cleansed.crm_0bpartner_attr b
# MAGIC on a.businesspartnernumber = b.businesspartnernumber
# MAGIC where a.businessPartnerCategoryCode = 3
# MAGIC 
# MAGIC union all
# MAGIC 
# MAGIC select * from(
# MAGIC select 
# MAGIC '-1' as businessPartnerGroupNumber,
# MAGIC 'ISU' as sourceSystemCode,
# MAGIC null as validFromDate,
# MAGIC null as validToDate,
# MAGIC null as businessPartnerGroupCode,
# MAGIC null as businessPartnerGroup,
# MAGIC null as businessPartnerGroupName1,
# MAGIC null as businessPartnerGroupName2,
# MAGIC null as externalNumber,
# MAGIC null as paymentAssistSchemeFlag,
# MAGIC null as billAssistFlag,
# MAGIC null as kidneyDialysisFlag,
# MAGIC null as createdDateTime,
# MAGIC null as createdBy,
# MAGIC null as lastUpdatedDateTime,
# MAGIC null as lastUpdatedBy
# MAGIC from  cleansed.isu_0bpartner_attr limit 1))d
# MAGIC except
# MAGIC select
# MAGIC businessPartnerGroupNumber
# MAGIC ,sourceSystemCode
# MAGIC ,validFromDate
# MAGIC ,validToDate
# MAGIC ,businessPartnerGroupCode
# MAGIC ,businessPartnerGroup
# MAGIC ,businessPartnerGroupName1
# MAGIC ,businessPartnerGroupName2
# MAGIC ,externalNumber
# MAGIC ,paymentAssistSchemeFlag
# MAGIC ,billAssistFlag    
# MAGIC ,kidneyDialysisFlag
# MAGIC ,createdDateTime
# MAGIC ,createdBy
# MAGIC ,lastUpdatedDateTime
# MAGIC ,lastUpdatedBy
# MAGIC from curated.dimBusinessPartnerGroup 

# COMMAND ----------

# DBTITLE 1,[Verification] Target vs  Source 
# MAGIC %sql
# MAGIC select
# MAGIC businessPartnerGroupNumber
# MAGIC ,sourceSystemCode
# MAGIC ,validFromDate
# MAGIC ,validToDate
# MAGIC ,businessPartnerGroupCode
# MAGIC ,businessPartnerGroup
# MAGIC ,businessPartnerGroupName1
# MAGIC ,businessPartnerGroupName2
# MAGIC ,externalNumber
# MAGIC ,paymentAssistSchemeFlag
# MAGIC ,billAssistFlag    
# MAGIC ,kidneyDialysisFlag
# MAGIC ,createdDateTime
# MAGIC ,createdBy
# MAGIC ,lastUpdatedDateTime
# MAGIC ,lastUpdatedBy
# MAGIC from curated.dimBusinessPartnerGroup --where businessPartnerGroupNumber = '0003100669'
# MAGIC except
# MAGIC select * from(
# MAGIC select
# MAGIC a.businessPartnerNumber as businessPartnerGroupNumber
# MAGIC ,'ISU' as sourceSystemCode
# MAGIC ,a.validFromDate as validFromDate
# MAGIC ,a.validToDate as validToDate
# MAGIC ,a.businessPartnerGroupCode as businessPartnerGroupCode
# MAGIC ,a.businessPartnerGroup as businessPartnerGroup
# MAGIC ,a.nameGroup1 as businessPartnerGroupName1
# MAGIC ,a.nameGroup2 as businessPartnerGroupName2
# MAGIC ,a.externalBusinessPartnerNumber as externalNumber
# MAGIC ,case
# MAGIC when b.paymentAssistSchemeIndicator = 'X' then 'Y'
# MAGIC else 'N' end as paymentAssistSchemeFlag
# MAGIC ,case
# MAGIC when b.billAssistIndicator = 'X' then 'Y'
# MAGIC else 'N' end as billAssistFlag
# MAGIC ,case
# MAGIC when b.kidneyDialysisIndicator = 'X' then 'Y'
# MAGIC else 'N' end as kidneyDialysisFlag
# MAGIC ,a.createdDateTime as createdDateTime
# MAGIC ,a.createdBy as createdBy
# MAGIC ,a.changedDateTime as lastUpdatedDateTime
# MAGIC ,a.changedBy as lastUpdatedBy
# MAGIC from cleansed.isu_0bpartner_attr a
# MAGIC left join cleansed.crm_0bpartner_attr b
# MAGIC on a.businesspartnernumber = b.businesspartnernumber
# MAGIC where a.businessPartnerCategoryCode = 3
# MAGIC union all
# MAGIC 
# MAGIC select * from(
# MAGIC select 
# MAGIC '-1' as businessPartnerGroupNumber,
# MAGIC 'ISU' as sourceSystemCode,
# MAGIC null as validFromDate,
# MAGIC null as validToDate,
# MAGIC null as businessPartnerGroupCode,
# MAGIC null as businessPartnerGroup,
# MAGIC null as businessPartnerGroupName1,
# MAGIC null as businessPartnerGroupName2,
# MAGIC null as externalNumber,
# MAGIC null as paymentAssistSchemeFlag,
# MAGIC null as billAssistFlag,
# MAGIC null as kidneyDialysisFlag,
# MAGIC null as createdDateTime,
# MAGIC null as createdBy,
# MAGIC null as lastUpdatedDateTime,
# MAGIC null as lastUpdatedBy
# MAGIC from  cleansed.isu_0bpartner_attr limit 1))d
