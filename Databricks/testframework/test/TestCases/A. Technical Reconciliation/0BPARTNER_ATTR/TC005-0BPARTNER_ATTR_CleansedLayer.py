# Databricks notebook source
# MAGIC %sql
# MAGIC select count(*) from raw.isu_0bpartner_attr

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from (
# MAGIC select 
# MAGIC *,
# MAGIC row_number () over (partition by partner order by extract_datetime desc) as rn
# MAGIC from raw.isu_0bpartner_attr) where rn = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select count (*) from cleansed.t_isu_0bpartner_attr

# COMMAND ----------

# MAGIC %sql
# MAGIC select businessPartnerNumber
# MAGIC ,businessPartnerCategoryCode
# MAGIC ,businessPartnerCategory
# MAGIC ,businessPartnerTypeCode
# MAGIC ,businessPartnerType
# MAGIC ,businessPartnerGroupCode
# MAGIC ,businessPartnerGroup
# MAGIC ,externalBusinessPartnerNumber
# MAGIC ,searchTerm1
# MAGIC ,searchTerm2
# MAGIC ,titleCode
# MAGIC ,title
# MAGIC ,deletedIndicator
# MAGIC ,centralBlockBusinessPartner
# MAGIC ,userId
# MAGIC ,paymentAssistSchemeIndicator
# MAGIC ,billAssistIndicator
# MAGIC ,createdOn
# MAGIC ,organizationName1
# MAGIC ,organizationName2
# MAGIC ,organizationName3
# MAGIC ,organizationFoundedDate
# MAGIC ,internationalLocationNumber1
# MAGIC ,internationalLocationNumber2
# MAGIC ,internationalLocationNumber3
# MAGIC ,lastName
# MAGIC ,firstName
# MAGIC ,atBirthName
# MAGIC ,middleName
# MAGIC ,academicTitle
# MAGIC ,nickName
# MAGIC ,nameInitials
# MAGIC ,countryName
# MAGIC ,correspondanceLanguage
# MAGIC ,nationality
# MAGIC ,personNumber
# MAGIC ,unknownGenderIndicator
# MAGIC ,language
# MAGIC ,dateOfBirth
# MAGIC ,dateOfDeath
# MAGIC ,personnelNumber
# MAGIC ,nameGroup1
# MAGIC ,nameGroup2
# MAGIC ,createdBy
# MAGIC ,createdDateTime
# MAGIC ,changedBy
# MAGIC ,changedDateTime
# MAGIC ,businessPartnerGUID
# MAGIC ,addressNumber
# MAGIC ,validFromDate
# MAGIC ,validToDate
# MAGIC ,naturalPersonIndicator
# MAGIC  from (
# MAGIC select *, row_number () over (partition by businessPartnerNumber order by extract_datetime desc) as rn from (
# MAGIC select
# MAGIC extract_datetime
# MAGIC ,PARTNER as businessPartnerNumber
# MAGIC ,TYPE as businessPartnerCategoryCode
# MAGIC ,b.businessPartnerCategory as businessPartnerCategory
# MAGIC ,BPKIND as businessPartnerTypeCode
# MAGIC ,c.businessPartnerType as businessPartnerType
# MAGIC ,BU_GROUP as businessPartnerGroupCode
# MAGIC ,d.businessPartnerGroup as businessPartnerGroup
# MAGIC ,BPEXT as externalBusinessPartnerNumber
# MAGIC ,BU_SORT1 as searchTerm1
# MAGIC ,BU_SORT2 as searchTerm2
# MAGIC ,a.TITLE as titleCode
# MAGIC ,f.title as title
# MAGIC ,XDELE as deletedIndicator
# MAGIC ,XBLCK as centralBlockBusinessPartner
# MAGIC ,ZZUSER as userId
# MAGIC ,ZZPAS_INDICATOR as paymentAssistSchemeIndicator
# MAGIC ,ZZBA_INDICATOR as billAssistIndicator
# MAGIC ,ZZAFLD00001Z as createdOn
# MAGIC ,NAME_ORG1 as organizationName1
# MAGIC ,NAME_ORG2 as organizationName2
# MAGIC ,NAME_ORG3 as organizationName3
# MAGIC ,FOUND_DAT as organizationFoundedDate
# MAGIC ,LOCATION_1 as internationalLocationNumber1
# MAGIC ,LOCATION_2 as internationalLocationNumber2
# MAGIC ,LOCATION_3 as internationalLocationNumber3
# MAGIC ,NAME_LAST as lastName
# MAGIC ,NAME_FIRST as firstName
# MAGIC ,NAME_LAST2 as atBirthName
# MAGIC ,NAMEMIDDLE as middleName
# MAGIC ,TITLE_ACA1 as academicTitle
# MAGIC ,NICKNAME as nickName
# MAGIC ,INITIALS as nameInitials
# MAGIC ,NAMCOUNTRY as countryName
# MAGIC ,LANGU_CORR as correspondanceLanguage
# MAGIC ,NATIO as nationality
# MAGIC ,PERSNUMBER as personNumber
# MAGIC ,XSEXU as unknownGenderIndicator
# MAGIC ,BU_LANGU as language
# MAGIC ,BIRTHDT as dateOfBirth
# MAGIC ,DEATHDT as dateOfDeath
# MAGIC ,PERNO as personnelNumber
# MAGIC ,NAME_GRP1 as nameGroup1
# MAGIC ,NAME_GRP2 as nameGroup2
# MAGIC ,CRUSR as createdBy
# MAGIC ,cast(to_unix_timestamp(CRDAT||' '||CRTIM,'yyyy-MM-dd HH:mm:ss') as timestamp) as createdDateTime
# MAGIC ,CHUSR as changedBy
# MAGIC ,cast(to_unix_timestamp(CHDAT||' '||CHTIM,'yyyy-MM-dd HH:mm:ss') as timestamp) as changedDateTime
# MAGIC ,PARTNER_GUID as businessPartnerGUID
# MAGIC ,ADDRCOMM as addressNumber
# MAGIC ,case when VALID_FROM = '10101000000' then '1900-01-01' else CONCAT(LEFT(VALID_FROM,4),'-',SUBSTRING(VALID_FROM,5,2),'-',SUBSTRING(VALID_FROM,7,2)) end as validFromDate
# MAGIC --,substr(VALID_TO, 1, 8) as validToDate
# MAGIC ,CONCAT(LEFT(VALID_TO,4),'-',SUBSTRING(VALID_TO,5,2),'-',SUBSTRING(VALID_TO,7,2)) as validToDate
# MAGIC --,VALID_FROM as validFromDate
# MAGIC --,VALID_TO as validToDateyes ====!!!! all dates need converting..... :)
# MAGIC 
# MAGIC ,NATPERS as naturalPersonIndicator
# MAGIC from raw.isu_0BPARTNER_ATTR a
# MAGIC left join cleansed.t_sapisu_0BPARTNER_TEXT b
# MAGIC on a.PARTNER = b.businessPartnerNumber and a.TYPE = b.businessPartnerCategoryCode
# MAGIC LEFT JOIN cleansed.t_sapisu_0BPTYPE_TEXT c
# MAGIC ON a.BPKIND = c.businessPartnerTypeCode --and c.SPRAS = 'E'
# MAGIC LEFT JOIN cleansed.t_sapisu_0BP_GROUP_TEXT d
# MAGIC ON a.BU_GROUP = d.businessPartnerGroupCode --and d.SPRAS = 'E'
# MAGIC LEFT JOIN cleansed.t_sapisu_tsad3t f
# MAGIC ON a.TITLE = f.TITLEcode --and f.LANGU = 'E'
# MAGIC )) where rn = 1
# MAGIC 
# MAGIC EXCEPT
# MAGIC select
# MAGIC businessPartnerNumber
# MAGIC ,businessPartnerCategoryCode
# MAGIC ,businessPartnerCategory
# MAGIC ,businessPartnerTypeCode
# MAGIC ,businessPartnerType
# MAGIC ,businessPartnerGroupCode
# MAGIC ,businessPartnerGroup
# MAGIC ,externalBusinessPartnerNumber
# MAGIC ,searchTerm1
# MAGIC ,searchTerm2
# MAGIC ,titleCode
# MAGIC ,title
# MAGIC ,deletedIndicator
# MAGIC ,centralBlockBusinessPartner
# MAGIC ,userId
# MAGIC ,paymentAssistSchemeIndicator
# MAGIC ,billAssistIndicator
# MAGIC ,createdOn
# MAGIC ,organizationName1
# MAGIC ,organizationName2
# MAGIC ,organizationName3
# MAGIC ,organizationFoundedDate
# MAGIC ,internationalLocationNumber1
# MAGIC ,internationalLocationNumber2
# MAGIC ,internationalLocationNumber3
# MAGIC ,lastName
# MAGIC ,firstName
# MAGIC ,atBirthName
# MAGIC ,middleName
# MAGIC ,academicTitle
# MAGIC ,nickName
# MAGIC ,nameInitials
# MAGIC ,countryName
# MAGIC ,correspondanceLanguage
# MAGIC ,nationality
# MAGIC ,personNumber
# MAGIC ,unknownGenderIndicator
# MAGIC ,language
# MAGIC ,dateOfBirth
# MAGIC ,dateOfDeath
# MAGIC ,personnelNumber
# MAGIC ,nameGroup1
# MAGIC ,nameGroup2
# MAGIC ,createdBy
# MAGIC --,createdDate
# MAGIC ,createdDateTime
# MAGIC ,changedBy
# MAGIC --,changedDate
# MAGIC ,changedDateTime
# MAGIC ,businessPartnerGUID
# MAGIC ,addressNumber
# MAGIC ,validFromDate
# MAGIC ,validToDate
# MAGIC ,naturalPersonIndicator
# MAGIC 
# MAGIC FROM
# MAGIC cleansed.t_isu_0bpartner_attr

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC businessPartnerNumber
# MAGIC ,businessPartnerCategoryCode
# MAGIC ,businessPartnerCategory
# MAGIC ,businessPartnerTypeCode
# MAGIC ,businessPartnerType
# MAGIC ,businessPartnerGroupCode
# MAGIC ,businessPartnerGroup
# MAGIC ,externalBusinessPartnerNumber
# MAGIC ,searchTerm1
# MAGIC ,searchTerm2
# MAGIC ,titleCode
# MAGIC ,title
# MAGIC ,deletedIndicator
# MAGIC ,centralBlockBusinessPartner
# MAGIC ,userId
# MAGIC ,paymentAssistSchemeIndicator
# MAGIC ,billAssistIndicator
# MAGIC ,createdOn
# MAGIC ,organizationName1
# MAGIC ,organizationName2
# MAGIC ,organizationName3
# MAGIC ,organizationFoundedDate
# MAGIC ,internationalLocationNumber1
# MAGIC ,internationalLocationNumber2
# MAGIC ,internationalLocationNumber3
# MAGIC ,lastName
# MAGIC ,firstName
# MAGIC ,atBirthName
# MAGIC ,middleName
# MAGIC ,academicTitle
# MAGIC ,nickName
# MAGIC ,nameInitials
# MAGIC ,countryName
# MAGIC ,correspondanceLanguage
# MAGIC ,nationality
# MAGIC ,personNumber
# MAGIC ,unknownGenderIndicator
# MAGIC ,language
# MAGIC ,dateOfBirth
# MAGIC ,dateOfDeath
# MAGIC ,personnelNumber
# MAGIC ,nameGroup1
# MAGIC ,nameGroup2
# MAGIC ,createdBy
# MAGIC --,createdDate
# MAGIC ,createdDateTime
# MAGIC ,changedBy
# MAGIC --,changedDate
# MAGIC ,changedDateTime
# MAGIC ,businessPartnerGUID
# MAGIC ,addressNumber
# MAGIC ,validFromDate
# MAGIC ,validToDate
# MAGIC ,naturalPersonIndicator
# MAGIC 
# MAGIC FROM
# MAGIC cleansed.t_isu_0bpartner_attr
# MAGIC EXCEPT
# MAGIC select businessPartnerNumber
# MAGIC ,businessPartnerCategoryCode
# MAGIC ,businessPartnerCategory
# MAGIC ,businessPartnerTypeCode
# MAGIC ,businessPartnerType
# MAGIC ,businessPartnerGroupCode
# MAGIC ,businessPartnerGroup
# MAGIC ,externalBusinessPartnerNumber
# MAGIC ,searchTerm1
# MAGIC ,searchTerm2
# MAGIC ,titleCode
# MAGIC ,title
# MAGIC ,deletedIndicator
# MAGIC ,centralBlockBusinessPartner
# MAGIC ,userId
# MAGIC ,paymentAssistSchemeIndicator
# MAGIC ,billAssistIndicator
# MAGIC ,createdOn
# MAGIC ,organizationName1
# MAGIC ,organizationName2
# MAGIC ,organizationName3
# MAGIC ,organizationFoundedDate
# MAGIC ,internationalLocationNumber1
# MAGIC ,internationalLocationNumber2
# MAGIC ,internationalLocationNumber3
# MAGIC ,lastName
# MAGIC ,firstName
# MAGIC ,atBirthName
# MAGIC ,middleName
# MAGIC ,academicTitle
# MAGIC ,nickName
# MAGIC ,nameInitials
# MAGIC ,countryName
# MAGIC ,correspondanceLanguage
# MAGIC ,nationality
# MAGIC ,personNumber
# MAGIC ,unknownGenderIndicator
# MAGIC ,language
# MAGIC ,dateOfBirth
# MAGIC ,dateOfDeath
# MAGIC ,personnelNumber
# MAGIC ,nameGroup1
# MAGIC ,nameGroup2
# MAGIC ,createdBy
# MAGIC ,createdDateTime
# MAGIC ,changedBy
# MAGIC ,changedDateTime
# MAGIC ,businessPartnerGUID
# MAGIC ,addressNumber
# MAGIC ,validFromDate
# MAGIC ,validToDate
# MAGIC ,naturalPersonIndicator
# MAGIC  from (
# MAGIC select *, row_number () over (partition by businessPartnerNumber order by extract_datetime desc) as rn from (
# MAGIC select
# MAGIC extract_datetime
# MAGIC ,PARTNER as businessPartnerNumber
# MAGIC ,TYPE as businessPartnerCategoryCode
# MAGIC ,b.businessPartnerCategory as businessPartnerCategory
# MAGIC ,BPKIND as businessPartnerTypeCode
# MAGIC ,c.businessPartnerType as businessPartnerType
# MAGIC ,BU_GROUP as businessPartnerGroupCode
# MAGIC ,d.businessPartnerGroup as businessPartnerGroup
# MAGIC ,BPEXT as externalBusinessPartnerNumber
# MAGIC ,BU_SORT1 as searchTerm1
# MAGIC ,BU_SORT2 as searchTerm2
# MAGIC ,a.TITLE as titleCode
# MAGIC ,f.title as title
# MAGIC ,XDELE as deletedIndicator
# MAGIC ,XBLCK as centralBlockBusinessPartner
# MAGIC ,ZZUSER as userId
# MAGIC ,ZZPAS_INDICATOR as paymentAssistSchemeIndicator
# MAGIC ,ZZBA_INDICATOR as billAssistIndicator
# MAGIC ,ZZAFLD00001Z as createdOn
# MAGIC ,NAME_ORG1 as organizationName1
# MAGIC ,NAME_ORG2 as organizationName2
# MAGIC ,NAME_ORG3 as organizationName3
# MAGIC ,FOUND_DAT as organizationFoundedDate
# MAGIC ,LOCATION_1 as internationalLocationNumber1
# MAGIC ,LOCATION_2 as internationalLocationNumber2
# MAGIC ,LOCATION_3 as internationalLocationNumber3
# MAGIC ,NAME_LAST as lastName
# MAGIC ,NAME_FIRST as firstName
# MAGIC ,NAME_LAST2 as atBirthName
# MAGIC ,NAMEMIDDLE as middleName
# MAGIC ,TITLE_ACA1 as academicTitle
# MAGIC ,NICKNAME as nickName
# MAGIC ,INITIALS as nameInitials
# MAGIC ,NAMCOUNTRY as countryName
# MAGIC ,LANGU_CORR as correspondanceLanguage
# MAGIC ,NATIO as nationality
# MAGIC ,PERSNUMBER as personNumber
# MAGIC ,XSEXU as unknownGenderIndicator
# MAGIC ,BU_LANGU as language
# MAGIC ,BIRTHDT as dateOfBirth
# MAGIC ,DEATHDT as dateOfDeath
# MAGIC ,PERNO as personnelNumber
# MAGIC ,NAME_GRP1 as nameGroup1
# MAGIC ,NAME_GRP2 as nameGroup2
# MAGIC ,CRUSR as createdBy
# MAGIC ,cast(to_unix_timestamp(CRDAT||' '||CRTIM,'yyyy-MM-dd HH:mm:ss') as timestamp) as createdDateTime
# MAGIC ,CHUSR as changedBy
# MAGIC ,cast(to_unix_timestamp(CHDAT||' '||CHTIM,'yyyy-MM-dd HH:mm:ss') as timestamp) as changedDateTime
# MAGIC ,PARTNER_GUID as businessPartnerGUID
# MAGIC ,ADDRCOMM as addressNumber
# MAGIC ,case when VALID_FROM = '10101000000' then '1900-01-01' else CONCAT(LEFT(VALID_FROM,4),'-',SUBSTRING(VALID_FROM,5,2),'-',SUBSTRING(VALID_FROM,7,2)) end as validFromDate
# MAGIC --,substr(VALID_TO, 1, 8) as validToDate
# MAGIC ,CONCAT(LEFT(VALID_TO,4),'-',SUBSTRING(VALID_TO,5,2),'-',SUBSTRING(VALID_TO,7,2)) as validToDate
# MAGIC --,VALID_FROM as validFromDate
# MAGIC --,VALID_TO as validToDateyes ====!!!! all dates need converting..... :)
# MAGIC 
# MAGIC ,NATPERS as naturalPersonIndicator
# MAGIC from raw.isu_0BPARTNER_ATTR a
# MAGIC left join cleansed.t_sapisu_0BPARTNER_TEXT b
# MAGIC on a.PARTNER = b.businessPartnerNumber and a.TYPE = b.businessPartnerCategoryCode
# MAGIC LEFT JOIN cleansed.t_sapisu_0BPTYPE_TEXT c
# MAGIC ON a.BPKIND = c.businessPartnerTypeCode --and c.SPRAS = 'E'
# MAGIC LEFT JOIN cleansed.t_sapisu_0BP_GROUP_TEXT d
# MAGIC ON a.BU_GROUP = d.businessPartnerGroupCode --and d.SPRAS = 'E'
# MAGIC LEFT JOIN cleansed.t_sapisu_tsad3t f
# MAGIC ON a.TITLE = f.TITLEcode --and f.LANGU = 'E'
# MAGIC )) where rn = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from (
# MAGIC select *, row_number () over (partition by businessPartnerNumber order by extract_datetime desc) as rn from (
# MAGIC select
# MAGIC extract_datetime
# MAGIC ,PARTNER as businessPartnerNumber
# MAGIC ,TYPE as businessPartnerCategoryCode
# MAGIC ,b.businessPartnerCategory as businessPartnerCategory
# MAGIC ,BPKIND as businessPartnerTypeCode
# MAGIC ,c.businessPartnerType as businessPartnerType
# MAGIC ,BU_GROUP as businessPartnerGroupCode
# MAGIC ,d.businessPartnerGroup as businessPartnerGroup
# MAGIC ,BPEXT as externalBusinessPartnerNumber
# MAGIC ,BU_SORT1 as searchTerm1
# MAGIC ,BU_SORT2 as searchTerm2
# MAGIC ,a.TITLE as titleCode
# MAGIC ,f.title as title
# MAGIC ,XDELE as deletedIndicator
# MAGIC ,XBLCK as centralBlockBusinessPartner
# MAGIC ,ZZUSER as userId
# MAGIC ,ZZPAS_INDICATOR as paymentAssistSchemeIndicator
# MAGIC ,ZZBA_INDICATOR as billAssistIndicator
# MAGIC ,ZZAFLD00001Z as createdOn
# MAGIC ,NAME_ORG1 as organizationName1
# MAGIC ,NAME_ORG2 as organizationName2
# MAGIC ,NAME_ORG3 as organizationName3
# MAGIC ,FOUND_DAT as organizationFoundedDate
# MAGIC ,LOCATION_1 as internationalLocationNumber1
# MAGIC ,LOCATION_2 as internationalLocationNumber2
# MAGIC ,LOCATION_3 as internationalLocationNumber3
# MAGIC ,NAME_LAST as lastName
# MAGIC ,NAME_FIRST as firstName
# MAGIC ,NAME_LAST2 as atBirthName
# MAGIC ,NAMEMIDDLE as middleName
# MAGIC ,TITLE_ACA1 as academicTitle
# MAGIC ,NICKNAME as nickName
# MAGIC ,INITIALS as nameInitials
# MAGIC ,NAMCOUNTRY as countryName
# MAGIC ,LANGU_CORR as correspondanceLanguage
# MAGIC ,NATIO as nationality
# MAGIC ,PERSNUMBER as personNumber
# MAGIC ,XSEXU as unknownGenderIndicator
# MAGIC ,BU_LANGU as language
# MAGIC ,BIRTHDT as dateOfBirth
# MAGIC ,DEATHDT as dateOfDeath
# MAGIC ,PERNO as personnelNumber
# MAGIC ,NAME_GRP1 as nameGroup1
# MAGIC ,NAME_GRP2 as nameGroup2
# MAGIC ,CRUSR as createdBy
# MAGIC ,cast(to_unix_timestamp(CRDAT||' '||CRTIM,'yyyy-MM-dd HH:mm:ss') as timestamp) as createdDateTime
# MAGIC ,CHUSR as changedBy
# MAGIC ,cast(to_unix_timestamp(CHDAT||' '||CHTIM,'yyyy-MM-dd HH:mm:ss') as timestamp) as changedDateTime
# MAGIC ,PARTNER_GUID as businessPartnerGUID
# MAGIC ,ADDRCOMM as addressNumber
# MAGIC ,case when VALID_FROM = '10101000000' then '1900-01-01' else CONCAT(LEFT(VALID_FROM,4),'-',SUBSTRING(VALID_FROM,5,2),'-',SUBSTRING(VALID_FROM,7,2)) end as validFromDate
# MAGIC --,substr(VALID_TO, 1, 8) as validToDate
# MAGIC ,CONCAT(LEFT(VALID_TO,4),'-',SUBSTRING(VALID_TO,5,2),'-',SUBSTRING(VALID_TO,7,2)) as validToDate
# MAGIC --,VALID_FROM as validFromDate
# MAGIC --,VALID_TO as validToDateyes ====!!!! all dates need converting..... :)
# MAGIC 
# MAGIC ,NATPERS as naturalPersonIndicator
# MAGIC from raw.isu_0BPARTNER_ATTR a
# MAGIC left join cleansed.t_sapisu_0BPARTNER_TEXT b
# MAGIC on a.PARTNER = b.businessPartnerNumber and a.TYPE = b.businessPartnerCategoryCode
# MAGIC LEFT JOIN cleansed.t_sapisu_0BPTYPE_TEXT c
# MAGIC ON a.BPKIND = c.businessPartnerTypeCode --and c.SPRAS = 'E'
# MAGIC LEFT JOIN cleansed.t_sapisu_0BP_GROUP_TEXT d
# MAGIC ON a.BU_GROUP = d.businessPartnerGroupCode --and d.SPRAS = 'E'
# MAGIC LEFT JOIN cleansed.t_sapisu_tsad3t f
# MAGIC ON a.TITLE = f.TITLEcode --and f.LANGU = 'E'
# MAGIC )) where rn = 1

# COMMAND ----------

# DBTITLE 0,[Result] Load Count Result into DataFrame
lakedf = spark.sql("select * from cleansed.t_isu_0bpartner_attr")

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
lakedf.printSchema()
