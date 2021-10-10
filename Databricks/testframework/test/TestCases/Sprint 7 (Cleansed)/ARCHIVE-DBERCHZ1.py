# Databricks notebook source
# DBTITLE 1,[Config] MsSQL Connection String
def AzSqlGetDBConnectionURL():
  
  DB_SERVER_FULL = ADS_DB_SERVER + ".database.windows.net"
  DB_USER_NAME = ADS_DATABASE_USERNAME + "@" + ADS_DB_SERVER

  #SQL Database related settings
  DB_PASSWORD = dbutils.secrets.get(scope = ADS_KV_ACCOUNT_SCOPE, key = ADS_KV_DB_PWD_SECRET_KEY)
  JDBC_PORT =  "1433"
  JDBC_EXTRA_OPTIONS = "encrypt=true;trustServerCertificate=true;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
  SQL_URL = "jdbc:sqlserver://" + DB_SERVER_FULL + ":" + JDBC_PORT + ";database=" + ADS_DATABASE_NAME + ";user=" + DB_USER_NAME+";password=" + DB_PASSWORD + ";" + JDBC_EXTRA_OPTIONS
  SQL_URL_SMALL = "jdbc:sqlserver://" + DB_SERVER_FULL + ":" + JDBC_PORT + ";database=" + ADS_DATABASE_NAME + ";user=" + DB_USER_NAME+";password=" + DB_PASSWORD
  
  return SQL_URL

# COMMAND ----------

# DBTITLE 1,[Config] Connection Setup
storage_account_name = "sadaftest01"
storage_account_access_key = dbutils.secrets.get(scope="Test-Access",key="test-datalake-key")
container_name = "raw"
file_location = "wasbs://raw@sadaftest01.blob.core.windows.net/sapisu/dberchz1/json/year=2021/month=09/day=27/DBO.DBERCHZ1_2021-09-27_165357_260.json.gz"
file_type = "json"
print(storage_account_name)

# COMMAND ----------

# DBTITLE 1,[Config] Connection Setup
storage_account_name = "sadaftest01"
storage_account_access_key = dbutils.secrets.get(scope="Test-Access",key="test-datalake-key")

container_name = "raw"
file_location = "wasbs://raw@sadaftest01.blob.core.windows.net/sapisu/dberchz1/json/year=2021/month=09/day=27/DBO.DBERCHZ1_2021-09-27_165357_260.json.gz"
#file_location = "wasbs://archive@saswcnonprod01landingtst.blob.core.windows.net/sapisu/20210825/20210825_16:14:52/0BPARTNER_ATTR_20210804141617.json"
file_type = "json"
print(storage_account_name)

# COMMAND ----------



# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)


# COMMAND ----------

import gzip

# COMMAND ----------

decompressedFile = gzip.GzipFile(fileobj=file_location, mode='rb')

# COMMAND ----------

# DBTITLE 1,[Source] Loading data into Dataframe
df = spark.read.format(file_type).option("inferSchema", "true").load(file_location)
#df2 = spark.read.format(file_type).option("inferSchema", "true").load(file_location2)

# COMMAND ----------

# DBTITLE 1,[Source] Schema Check - Refer to Raw2Cleansed Mapping
df.printSchema()
df2.printSchema()

# COMMAND ----------

# DBTITLE 0,[Result] Load Count Result into DataFrame
lakedf = spark.sql("select * from cleansed.t_sapisu_0bpartner_attr")

# COMMAND ----------

display(lakedf)

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
lakedf.printSchema()

# COMMAND ----------

# DBTITLE 1,[Source] Creating Temporary Table
df.createOrReplaceTempView("Source1")
df2.createOrReplaceTempView("Source22")

# COMMAND ----------

df = spark.sql("select * from Source1")
df2 = spark.sql("select * from Source22")


# COMMAND ----------

# DBTITLE 1,[Verify] Masking Check
# MAGIC %sql
# MAGIC select
# MAGIC NAME_ORG1
# MAGIC ,NAME_ORG2
# MAGIC ,NAME_ORG3
# MAGIC ,NAME_ORG4
# MAGIC ,NAME_LAST
# MAGIC ,NAME_FIRST
# MAGIC ,NAME_LST2
# MAGIC ,NAME_LAST2
# MAGIC ,NAMEMIDDLE
# MAGIC ,NAME1_TEXT
# MAGIC ,NICKNAME
# MAGIC ,BIRTHPL
# MAGIC ,EMPLO
# MAGIC ,BIRTHDT
# MAGIC ,PERNO
# MAGIC ,NAME_GRP1
# MAGIC ,NAME_GRP2
# MAGIC ,MC_NAME1
# MAGIC ,MC_NAME2
# MAGIC ,CRUSR
# MAGIC ,CHUSR
# MAGIC from Source22

# COMMAND ----------

# MAGIC %sql
# MAGIC select *, '2/09' as filedate from Source22
# MAGIC --except
# MAGIC --SELECT *, '3/09' as filedate from Source;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed.t_sapisu_tsad3t

# COMMAND ----------

# DBTITLE 1,[Source] with mapping
# MAGIC %sql
# MAGIC select
# MAGIC PARTNER as businessPartnerNumber
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
# MAGIC from source22 a
# MAGIC left join cleansed.t_sapisu_0BPARTNER_TEXT b
# MAGIC on a.PARTNER = b.businessPartnerNumber and a.TYPE = b.businessPartnerCategoryCode
# MAGIC LEFT JOIN cleansed.t_sapisu_0BPTYPE_TEXT c
# MAGIC ON a.BPKIND = c.businessPartnerTypeCode --and c.SPRAS = 'E'
# MAGIC LEFT JOIN cleansed.t_sapisu_0BP_GROUP_TEXT d
# MAGIC ON a.BU_GROUP = d.businessPartnerGroupCode --and d.SPRAS = 'E'
# MAGIC LEFT JOIN cleansed.t_sapisu_tsad3t f
# MAGIC ON a.TITLE = f.TITLEcode --and f.LANGU = 'E'
# MAGIC where PARTNER = '0001000363'

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC *
# MAGIC FROM
# MAGIC cleansed.t_sapisu_0BPartner_attr where businesspartnernumber = '0001000363AA'

# COMMAND ----------

lakedf.createOrReplaceTempView("Target")

# COMMAND ----------

# DBTITLE 1,[Verification] Count Checks
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.t_sapisu_0bpartner_attr
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (select
# MAGIC PARTNER as businessPartnerNumber
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
# MAGIC from source22 a
# MAGIC left join cleansed.t_sapisu_0BPARTNER_TEXT b
# MAGIC on a.PARTNER = b.businessPartnerNumber and a.TYPE = b.businessPartnerCategoryCode
# MAGIC LEFT JOIN cleansed.t_sapisu_0BPTYPE_TEXT c
# MAGIC ON a.BPKIND = c.businessPartnerTypeCode --and c.SPRAS = 'E'
# MAGIC LEFT JOIN cleansed.t_sapisu_0BP_GROUP_TEXT d
# MAGIC ON a.BU_GROUP = d.businessPartnerGroupCode --and d.SPRAS = 'E'
# MAGIC LEFT JOIN cleansed.t_sapisu_tsad3t f
# MAGIC ON a.TITLE = f.TITLEcode) --and f.LANGU = 'E'

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT businessPartnerNumber, COUNT (*) as count
# MAGIC FROM cleansed.t_sapisu_0bpartner_attr
# MAGIC GROUP BY businessPartnerNumber
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY businessPartnerNumber order by businessPartnerNumber) as rn
# MAGIC FROM cleansed.t_sapisu_0bpartner_attr
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC 
# MAGIC select
# MAGIC PARTNER as businessPartnerNumber
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
# MAGIC from source22 a
# MAGIC left join cleansed.t_sapisu_0BPARTNER_TEXT b
# MAGIC on a.PARTNER = b.businessPartnerNumber and a.TYPE = b.businessPartnerCategoryCode
# MAGIC LEFT JOIN cleansed.t_sapisu_0BPTYPE_TEXT c
# MAGIC ON a.BPKIND = c.businessPartnerTypeCode --and c.SPRAS = 'E'
# MAGIC LEFT JOIN cleansed.t_sapisu_0BP_GROUP_TEXT d
# MAGIC ON a.BU_GROUP = d.businessPartnerGroupCode --and d.SPRAS = 'E'
# MAGIC LEFT JOIN cleansed.t_sapisu_tsad3t f
# MAGIC ON a.TITLE = f.TITLEcode --and f.LANGU = 'E'
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
# MAGIC cleansed.t_sapisu_0bpartner_attr

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
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
# MAGIC cleansed.t_sapisu_0bpartner_attr
# MAGIC 
# MAGIC EXCEPT 
# MAGIC select
# MAGIC PARTNER as businessPartnerNumber
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
# MAGIC from source22 a
# MAGIC left join cleansed.t_sapisu_0BPARTNER_TEXT b
# MAGIC on a.PARTNER = b.businessPartnerNumber and a.TYPE = b.businessPartnerCategoryCode
# MAGIC LEFT JOIN cleansed.t_sapisu_0BPTYPE_TEXT c
# MAGIC ON a.BPKIND = c.businessPartnerTypeCode --and c.SPRAS = 'E'
# MAGIC LEFT JOIN cleansed.t_sapisu_0BP_GROUP_TEXT d
# MAGIC ON a.BU_GROUP = d.businessPartnerGroupCode --and d.SPRAS = 'E'
# MAGIC LEFT JOIN cleansed.t_sapisu_tsad3t f
# MAGIC ON a.TITLE = f.TITLEcode --and f.LANGU = 'E'
