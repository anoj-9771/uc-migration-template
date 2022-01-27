# Databricks notebook source
# DBTITLE 1,Target
# MAGIC %sql
# MAGIC select * from curated.dimBusinessPartner --where paymentAssistSchemeFlag = 'Y'

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
lakedftarget = spark.sql("select * from curated.dimBusinessPartner")
lakedftarget.printSchema()

# COMMAND ----------

# DBTITLE 1,[Source] Applying Transformation
# MAGIC %sql
# MAGIC select * from(
# MAGIC select
# MAGIC a.businessPartnerNumber as businessPartnerNumber
# MAGIC ,'ISU' as sourceSystemCode
# MAGIC ,a.validFromDate as validFromDate
# MAGIC ,a.validToDate as validToDate
# MAGIC ,a.businessPartnerCategoryCode as businessPartnerCategoryCode
# MAGIC ,a.businessPartnerCategory as businessPartnerCategory
# MAGIC ,a.businessPartnerTypeCode as businessPartnerTypeCode
# MAGIC ,a.businessPartnerType as businessPartnerType
# MAGIC ,a.externalBusinessPartnerNumber as externalNumber
# MAGIC ,a.businessPartnerGUID as businessPartnerGUID
# MAGIC ,a.firstName as firstName
# MAGIC ,a.lastName as lastName
# MAGIC ,a.middleName as middleName
# MAGIC ,a.nickName as nickName
# MAGIC ,a.titleCode as titleCode
# MAGIC ,a.title as title
# MAGIC ,a.dateOfBirth as dateOfBirth
# MAGIC ,a.dateOfDeath as dateOfDeath
# MAGIC ,b.warWidowIndicator as warWidowFlag      
# MAGIC ,b.deceasedIndicator as deceasedFlag      
# MAGIC ,b.disabilityIndicator as disabilityFlag      
# MAGIC ,b.goldCardHolderIndicator as goldCardHolderFlag
# MAGIC ,case
# MAGIC when
# MAGIC coalesce(a.naturalPersonIndicator, b.naturalPersonIndicator) = 'X' then 'Y' 
# MAGIC else'N' end as naturalPersonFlag
# MAGIC ,b.pensionConcessionCardIndicator as pensionCardFlag
# MAGIC ,b.pensionType as pensionType
# MAGIC ,a.personNumber as personNumber
# MAGIC ,a.personnelNumber as personnelNumber
# MAGIC ,case
# MAGIC when a.businessPartnerCategoryCode = '2' then
# MAGIC concat(coalesce(a.organizationName1, ''), ' ', coalesce(a.organizationName2, ''), ' ',coalesce(a.organizationName3, '')) 
# MAGIC else a.organizationName1 end as organizationName
# MAGIC ,case
# MAGIC when  a.businessPartnerCategoryCode = '2' then a.organizationFoundedDate
# MAGIC else null end as organizationFoundedDate
# MAGIC ,a.createdDateTime as organizationFoundedDate
# MAGIC ,a.createdBy as organizationFoundedDate
# MAGIC ,a.changedDateTime as organizationFoundedDate
# MAGIC ,a.changedBy as organizationFoundedDate
# MAGIC from cleansed.isu_0bpartner_attr a
# MAGIC left join cleansed.crm_0bpartner_attr b
# MAGIC on a.businesspartnernumber = b.businesspartnernumber 
# MAGIC where a.businessPartnerCategoryCode in (1, 2) 
# MAGIC union all
# MAGIC select * from (
# MAGIC select 
# MAGIC '-1' as businessPartnerNumber,
# MAGIC 'ISU' as sourceSystemCode,
# MAGIC 'null' as validFromDate,
# MAGIC 'null' as validToDate,
# MAGIC 'null' as businessPartnerCategoryCode,
# MAGIC 'null' as businessPartnerCategory,
# MAGIC 'null' as businessPartnerTypeCode,
# MAGIC 'null' as businessPartnerType,
# MAGIC 'null' as externalNumber,
# MAGIC 'null' as businessPartnerGUID,
# MAGIC 'null' as firstName,
# MAGIC 'null' as lastName,
# MAGIC 'null' as middleName,
# MAGIC 'null' as nickName,
# MAGIC 'null' as titleCode,
# MAGIC 'null' as title,
# MAGIC 'null' as dateOfBirth,
# MAGIC 'null' as dateOfDeath,
# MAGIC 'null' as warWidowFlag,
# MAGIC 'null' as deceasedFlag,
# MAGIC 'null' as disabilityFlag,
# MAGIC 'null' as goldCardHolderFlag,
# MAGIC 'null' as naturalPersonFlag,
# MAGIC 'null' as pensionCardFlag,
# MAGIC 'null' as pensionType,
# MAGIC 'null' as personNumber,
# MAGIC 'null' as personnelNumber,
# MAGIC 'null' as organizationName,
# MAGIC 'null' as organizationFoundedDate,
# MAGIC 'null' as createdDateTime,
# MAGIC 'null' as createdBy,
# MAGIC 'null' as  changedDateTime,
# MAGIC 'null' as changedBy
# MAGIC  from cleansed.isu_0bpartner_attr limit 1)d)e

# COMMAND ----------

# DBTITLE 1,[Verification] Auto Generate field check
# MAGIC %sql
# MAGIC select * from curated.dimBusinessPartner where dimbusinessPartnerSK is null or dimbusinessPartnerSK = '' or dimbusinessPartnerSK = ' '

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Check
# MAGIC %sql
# MAGIC --duplicate check of Surrogate key
# MAGIC SELECT dimbusinessPartnerSK,COUNT (*) as count
# MAGIC FROM curated.dimBusinessPartner
# MAGIC GROUP BY dimbusinessPartnerSK
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Check
# MAGIC %sql
# MAGIC --duplicate check of unique key
# MAGIC SELECT businessPartnerNumber,COUNT (*) as count
# MAGIC FROM curated.dimBusinessPartner
# MAGIC GROUP BY businessPartnerNumber
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Count Check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from curated.dimBusinessPartner
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (
# MAGIC select * from(
# MAGIC select
# MAGIC a.businessPartnerNumber as businessPartnerNumber
# MAGIC ,'ISU' as sourceSystemCode
# MAGIC ,a.validFromDate as validFromDate
# MAGIC ,a.validToDate as validToDate
# MAGIC ,a.businessPartnerCategoryCode as businessPartnerCategoryCode
# MAGIC ,a.businessPartnerCategory as businessPartnerCategory
# MAGIC ,a.businessPartnerTypeCode as businessPartnerTypeCode
# MAGIC ,a.businessPartnerType as businessPartnerType
# MAGIC ,a.externalBusinessPartnerNumber as externalNumber
# MAGIC ,a.businessPartnerGUID as businessPartnerGUID
# MAGIC ,a.firstName as firstName
# MAGIC ,a.lastName as lastName
# MAGIC ,a.middleName as middleName
# MAGIC ,a.nickName as nickName
# MAGIC ,a.titleCode as titleCode
# MAGIC ,a.title as title
# MAGIC ,a.dateOfBirth as dateOfBirth
# MAGIC ,a.dateOfDeath as dateOfDeath
# MAGIC ,b.warWidowIndicator as warWidowFlag      
# MAGIC ,b.deceasedIndicator as deceasedFlag      
# MAGIC ,b.disabilityIndicator as disabilityFlag      
# MAGIC ,b.goldCardHolderIndicator as goldCardHolderFlag
# MAGIC ,case
# MAGIC when
# MAGIC coalesce(a.naturalPersonIndicator, b.naturalPersonIndicator) = 'X' then 'Y' 
# MAGIC else'N' end as naturalPersonFlag
# MAGIC ,b.pensionConcessionCardIndicator as pensionCardFlag
# MAGIC ,b.pensionType as pensionType
# MAGIC ,a.personNumber as personNumber
# MAGIC ,a.personnelNumber as personnelNumber
# MAGIC ,case
# MAGIC when a.businessPartnerCategoryCode = '2' then
# MAGIC concat(coalesce(a.organizationName1, ''), ' ', coalesce(a.organizationName2, ''), ' ',coalesce(a.organizationName3, '')) 
# MAGIC else a.organizationName1 end as organizationName
# MAGIC ,case
# MAGIC when  a.businessPartnerCategoryCode = '2' then a.organizationFoundedDate
# MAGIC else null end as organizationFoundedDate
# MAGIC ,a.createdDateTime as organizationFoundedDate
# MAGIC ,a.createdBy as organizationFoundedDate
# MAGIC ,a.changedDateTime as organizationFoundedDate
# MAGIC ,a.changedBy as organizationFoundedDate
# MAGIC from cleansed.isu_0bpartner_attr a
# MAGIC left join cleansed.crm_0bpartner_attr b
# MAGIC on a.businesspartnernumber = b.businesspartnernumber 
# MAGIC where a.businessPartnerCategoryCode in (1, 2) 
# MAGIC union all
# MAGIC select * from (
# MAGIC select 
# MAGIC '-1' as businessPartnerNumber,
# MAGIC 'ISU' as sourceSystemCode,
# MAGIC 'null' as validFromDate,
# MAGIC 'null' as validToDate,
# MAGIC 'null' as businessPartnerCategoryCode,
# MAGIC 'null' as businessPartnerCategory,
# MAGIC 'null' as businessPartnerTypeCode,
# MAGIC 'null' as businessPartnerType,
# MAGIC 'null' as externalNumber,
# MAGIC 'null' as businessPartnerGUID,
# MAGIC 'null' as firstName,
# MAGIC 'null' as lastName,
# MAGIC 'null' as middleName,
# MAGIC 'null' as nickName,
# MAGIC 'null' as titleCode,
# MAGIC 'null' as title,
# MAGIC 'null' as dateOfBirth,
# MAGIC 'null' as dateOfDeath,
# MAGIC 'null' as warWidowFlag,
# MAGIC 'null' as deceasedFlag,
# MAGIC 'null' as disabilityFlag,
# MAGIC 'null' as goldCardHolderFlag,
# MAGIC 'null' as naturalPersonFlag,
# MAGIC 'null' as pensionCardFlag,
# MAGIC 'null' as pensionType,
# MAGIC 'null' as personNumber,
# MAGIC 'null' as personnelNumber,
# MAGIC 'null' as organizationName,
# MAGIC 'null' as organizationFoundedDate,
# MAGIC 'null' as createdDateTime,
# MAGIC 'null' as createdBy,
# MAGIC 'null' as  changedDateTime,
# MAGIC 'null' as changedBy
# MAGIC  from cleansed.isu_0bpartner_attr limit 1)d)e )--c where c.rn =1)

# COMMAND ----------

# DBTITLE 1,[Verification] Source vs Target
# MAGIC %sql
# MAGIC select * from(
# MAGIC select
# MAGIC a.businessPartnerNumber as businessPartnerNumber
# MAGIC ,'ISU' as sourceSystemCode
# MAGIC ,a.validFromDate as validFromDate
# MAGIC ,a.validToDate as validToDate
# MAGIC ,a.businessPartnerCategoryCode as businessPartnerCategoryCode
# MAGIC ,a.businessPartnerCategory as businessPartnerCategory
# MAGIC ,a.businessPartnerTypeCode as businessPartnerTypeCode
# MAGIC ,a.businessPartnerType as businessPartnerType
# MAGIC ,a.externalBusinessPartnerNumber as externalNumber
# MAGIC ,a.businessPartnerGUID as businessPartnerGUID
# MAGIC ,a.firstName as firstName
# MAGIC ,a.lastName as lastName
# MAGIC ,a.middleName as middleName
# MAGIC ,a.nickName as nickName
# MAGIC ,a.titleCode as titleCode
# MAGIC ,a.title as title
# MAGIC ,a.dateOfBirth as dateOfBirth
# MAGIC ,a.dateOfDeath as dateOfDeath
# MAGIC ,b.warWidowIndicator as warWidowFlag      
# MAGIC ,b.deceasedIndicator as deceasedFlag      
# MAGIC ,b.disabilityIndicator as disabilityFlag      
# MAGIC ,b.goldCardHolderIndicator as goldCardHolderFlag
# MAGIC ,case
# MAGIC when
# MAGIC coalesce(a.naturalPersonIndicator, b.naturalPersonIndicator) = 'X' then 'Y' 
# MAGIC else'N' end as naturalPersonFlag
# MAGIC ,b.pensionConcessionCardIndicator as pensionCardFlag
# MAGIC ,b.pensionType as pensionType
# MAGIC ,a.personNumber as personNumber
# MAGIC ,a.personnelNumber as personnelNumber
# MAGIC ,case
# MAGIC when a.businessPartnerCategoryCode = '2' then
# MAGIC concat(coalesce(a.organizationName1, ''), ' ', coalesce(a.organizationName2, ''), ' ',coalesce(a.organizationName3, '')) 
# MAGIC else a.organizationName1 end as organizationName
# MAGIC ,case
# MAGIC when  a.businessPartnerCategoryCode = '2' then a.organizationFoundedDate
# MAGIC else null end as organizationFoundedDate
# MAGIC ,a.createdDateTime as organizationFoundedDate
# MAGIC ,a.createdBy as organizationFoundedDate
# MAGIC ,a.changedDateTime as organizationFoundedDate
# MAGIC ,a.changedBy as organizationFoundedDate
# MAGIC from cleansed.isu_0bpartner_attr a
# MAGIC left join cleansed.crm_0bpartner_attr b
# MAGIC on a.businesspartnernumber = b.businesspartnernumber 
# MAGIC where a.businessPartnerCategoryCode in (1, 2) 
# MAGIC union all
# MAGIC select * from (
# MAGIC select 
# MAGIC '-1' as businessPartnerNumber,
# MAGIC 'ISU' as sourceSystemCode,
# MAGIC null as validFromDate,
# MAGIC null as validToDate,
# MAGIC null as businessPartnerCategoryCode,
# MAGIC null as businessPartnerCategory,
# MAGIC null as businessPartnerTypeCode,
# MAGIC null as businessPartnerType,
# MAGIC null as externalNumber,
# MAGIC null as businessPartnerGUID,
# MAGIC null as firstName,
# MAGIC null as lastName,
# MAGIC null as middleName,
# MAGIC null as nickName,
# MAGIC null as titleCode,
# MAGIC null as title,
# MAGIC null as dateOfBirth,
# MAGIC null as dateOfDeath,
# MAGIC null as warWidowFlag,
# MAGIC null as deceasedFlag,
# MAGIC null as disabilityFlag,
# MAGIC null as goldCardHolderFlag,
# MAGIC null as naturalPersonFlag,
# MAGIC null as pensionCardFlag,
# MAGIC null as pensionType,
# MAGIC null as personNumber,
# MAGIC null as personnelNumber,
# MAGIC null as organizationName,
# MAGIC null as organizationFoundedDate,
# MAGIC null as createdDateTime,
# MAGIC null as createdBy,
# MAGIC null as  changedDateTime,
# MAGIC null as changedBy
# MAGIC  from cleansed.isu_0bpartner_attr limit 1)d)e
# MAGIC except
# MAGIC select
# MAGIC businessPartnerNumber
# MAGIC ,sourceSystemCode
# MAGIC ,validFromDate
# MAGIC ,validToDate
# MAGIC ,businessPartnerCategoryCode
# MAGIC ,businessPartnerCategory
# MAGIC ,businessPartnerTypeCode
# MAGIC ,businessPartnerType
# MAGIC ,externalNumber
# MAGIC ,businessPartnerGUID
# MAGIC ,firstName
# MAGIC ,lastName
# MAGIC ,middleName
# MAGIC ,nickName
# MAGIC ,titleCode
# MAGIC ,title
# MAGIC ,dateOfBirth
# MAGIC ,dateOfDeath
# MAGIC ,warWidowFlag      
# MAGIC ,deceasedFlag      
# MAGIC ,disabilityFlag      
# MAGIC ,goldCardHolderFlag
# MAGIC ,naturalPersonFlag
# MAGIC ,pensionCardFlag
# MAGIC ,pensionType
# MAGIC ,personNumber
# MAGIC ,personnelNumber
# MAGIC ,organizationName
# MAGIC ,organizationFoundedDate
# MAGIC ,createdDateTime
# MAGIC ,createdBy
# MAGIC ,changedDateTime
# MAGIC ,changedBy
# MAGIC from curated.dimBusinessPartner

# COMMAND ----------

# DBTITLE 1,[Verification] Target vs  Source 
# MAGIC %sql
# MAGIC 
# MAGIC select
# MAGIC businessPartnerNumber
# MAGIC ,sourceSystemCode
# MAGIC ,validFromDate
# MAGIC ,validToDate
# MAGIC ,businessPartnerCategoryCode
# MAGIC ,businessPartnerCategory
# MAGIC ,businessPartnerTypeCode
# MAGIC ,businessPartnerType
# MAGIC ,externalNumber
# MAGIC ,businessPartnerGUID
# MAGIC ,firstName
# MAGIC ,lastName
# MAGIC ,middleName
# MAGIC ,nickName
# MAGIC ,titleCode
# MAGIC ,title
# MAGIC ,dateOfBirth
# MAGIC ,dateOfDeath
# MAGIC ,warWidowFlag      
# MAGIC ,deceasedFlag      
# MAGIC ,disabilityFlag      
# MAGIC ,goldCardHolderFlag
# MAGIC ,naturalPersonFlag
# MAGIC ,pensionCardFlag
# MAGIC ,pensionType
# MAGIC ,personNumber
# MAGIC ,personnelNumber
# MAGIC ,organizationName
# MAGIC ,organizationFoundedDate
# MAGIC ,createdDateTime
# MAGIC ,createdBy
# MAGIC ,changedDateTime
# MAGIC ,changedBy
# MAGIC from curated.dimBusinessPartner --where businessPartnerGroupNumber = '0003100669'
# MAGIC except
# MAGIC select * from(select
# MAGIC a.businessPartnerNumber as businessPartnerNumber
# MAGIC ,'ISU' as sourceSystemCode
# MAGIC ,a.validFromDate as validFromDate
# MAGIC ,a.validToDate as validToDate
# MAGIC ,a.businessPartnerCategoryCode as businessPartnerCategoryCode
# MAGIC ,a.businessPartnerCategory as businessPartnerCategory
# MAGIC ,a.businessPartnerTypeCode as businessPartnerTypeCode
# MAGIC ,a.businessPartnerType as businessPartnerType
# MAGIC ,a.externalBusinessPartnerNumber as externalNumber
# MAGIC ,a.businessPartnerGUID as businessPartnerGUID
# MAGIC ,a.firstName as firstName
# MAGIC ,a.lastName as lastName
# MAGIC ,a.middleName as middleName
# MAGIC ,a.nickName as nickName
# MAGIC ,a.titleCode as titleCode
# MAGIC ,a.title as title
# MAGIC ,a.dateOfBirth as dateOfBirth
# MAGIC ,a.dateOfDeath as dateOfDeath
# MAGIC ,b.warWidowIndicator as warWidowFlag      
# MAGIC ,b.deceasedIndicator as deceasedFlag      
# MAGIC ,b.disabilityIndicator as disabilityFlag      
# MAGIC ,b.goldCardHolderIndicator as goldCardHolderFlag
# MAGIC ,case
# MAGIC when
# MAGIC coalesce(a.naturalPersonIndicator, b.naturalPersonIndicator) = 'X' then 'Y' 
# MAGIC else'N' end as naturalPersonFlag
# MAGIC ,b.pensionConcessionCardIndicator as pensionCardFlag
# MAGIC ,b.pensionType as pensionType
# MAGIC ,a.personNumber as personNumber
# MAGIC ,a.personnelNumber as personnelNumber
# MAGIC ,case
# MAGIC when a.businessPartnerCategoryCode = '2' then
# MAGIC concat(coalesce(a.organizationName1, ''), ' ', coalesce(a.organizationName2, ''), ' ',coalesce(a.organizationName3, '')) 
# MAGIC else a.organizationName1 end as organizationName
# MAGIC ,case
# MAGIC when  a.businessPartnerCategoryCode = '2' then a.organizationFoundedDate
# MAGIC else null end as organizationFoundedDate
# MAGIC ,a.createdDateTime as organizationFoundedDate
# MAGIC ,a.createdBy as organizationFoundedDate
# MAGIC ,a.changedDateTime as organizationFoundedDate
# MAGIC ,a.changedBy as organizationFoundedDate
# MAGIC from cleansed.isu_0bpartner_attr a
# MAGIC left join cleansed.crm_0bpartner_attr b
# MAGIC on a.businesspartnernumber = b.businesspartnernumber 
# MAGIC where a.businessPartnerCategoryCode in (1, 2) 
# MAGIC union all
# MAGIC 
# MAGIC select * from (
# MAGIC select 
# MAGIC '-1' as businessPartnerNumber,
# MAGIC 'ISU' as sourceSystemCode,
# MAGIC null as validFromDate,
# MAGIC null as validToDate,
# MAGIC null as businessPartnerCategoryCode,
# MAGIC null as businessPartnerCategory,
# MAGIC null as businessPartnerTypeCode,
# MAGIC null as businessPartnerType,
# MAGIC null as externalNumber,
# MAGIC null as businessPartnerGUID,
# MAGIC null as firstName,
# MAGIC null as lastName,
# MAGIC null as middleName,
# MAGIC null as nickName,
# MAGIC null as titleCode,
# MAGIC null as title,
# MAGIC null as dateOfBirth,
# MAGIC null as dateOfDeath,
# MAGIC null as warWidowFlag,
# MAGIC null as deceasedFlag,
# MAGIC null as disabilityFlag,
# MAGIC null as goldCardHolderFlag,
# MAGIC null as naturalPersonFlag,
# MAGIC null as pensionCardFlag,
# MAGIC null as pensionType,
# MAGIC null as personNumber,
# MAGIC null as personnelNumber,
# MAGIC null as organizationName,
# MAGIC null as organizationFoundedDate,
# MAGIC null as createdDateTime,
# MAGIC null as createdBy,
# MAGIC null as  changedDateTime,
# MAGIC null as changedBy
# MAGIC  from cleansed.isu_0bpartner_attr limit 1)d)e
