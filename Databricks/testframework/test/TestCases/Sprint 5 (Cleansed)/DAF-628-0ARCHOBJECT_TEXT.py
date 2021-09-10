# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup
storage_account_name = "saswcnonprod01landingtst"
storage_account_access_key = dbutils.secrets.get(scope="Test-Access",key="test-blob-key")
container_name = "archive"
file_location = "wasbs://archive@saswcnonprod01landingtst.blob.core.windows.net/sapisu/20210902/20210902_00:23:52/0ARCHOBJECT_TEXT_20210831104548.json"
file_location2 = "wasbs://archive@saswcnonprod01landingtst.blob.core.windows.net/sapisu/20210902/20210902_00:23:52/0ARCHOBJECT_TEXT_20210831110701.json"
file_type = "json"
print(storage_account_name)

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)


# COMMAND ----------

# DBTITLE 1,[Source] Loading data into Dataframe
df = spark.read.format(file_type).option("inferSchema", "true").load(file_location)
df2 = spark.read.format(file_type).option("inferSchema", "true").load(file_location2)

# COMMAND ----------

# DBTITLE 1,[Source] Schema Check - Refer to Raw2Cleansed Mapping
df.printSchema()
df2.printSchema()

# COMMAND ----------

lakedf = spark.sql("select * from cleansed.t_sapisu_0ARCHOBJECT_TEXT")
display(lakedf)

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
lakedf.printSchema()

# COMMAND ----------

# DBTITLE 1,[Source] Creating Temporary Table
df.createOrReplaceTempView("Source1")
df2.createOrReplaceTempView("Source2")

# COMMAND ----------

df = spark.sql("select * from Source1")
df2 = spark.sql("select * from Source2")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC AOID,
# MAGIC OBJNR,
# MAGIC LANGU,
# MAGIC DOORPLT
# MAGIC from
# MAGIC (
# MAGIC SELECT
# MAGIC AOID
# MAGIC ,DOORPLT
# MAGIC ,EXTRACT_DATETIME
# MAGIC ,EXTRACT_RUN_ID
# MAGIC ,IDENT
# MAGIC ,LANGU
# MAGIC ,OBJNR
# MAGIC ,ODQ_CHANGEMODE
# MAGIC ,ODQ_ENTITYCNTR
# MAGIC ,XAO
# MAGIC ,ROW_NUMBER () OVER(PARTITION BY AOID ORDER BY FILETIME DESC) AS RN
# MAGIC FROM (
# MAGIC select 
# MAGIC AOID
# MAGIC ,DOORPLT
# MAGIC ,EXTRACT_DATETIME
# MAGIC ,EXTRACT_RUN_ID
# MAGIC ,IDENT
# MAGIC ,LANGU
# MAGIC ,OBJNR
# MAGIC ,ODQ_CHANGEMODE
# MAGIC ,ODQ_ENTITYCNTR
# MAGIC ,XAO
# MAGIC ,'20210831104548' AS FILETIME
# MAGIC from Source1 WHERE LANGU = 'E'
# MAGIC union all
# MAGIC SELECT 
# MAGIC AOID
# MAGIC ,DOORPLT
# MAGIC ,EXTRACT_DATETIME
# MAGIC ,EXTRACT_RUN_ID
# MAGIC ,IDENT
# MAGIC ,LANGU
# MAGIC ,OBJNR
# MAGIC ,ODQ_CHANGEMODE
# MAGIC ,ODQ_ENTITYCNTR
# MAGIC ,XAO
# MAGIC ,'20210831110701' AS FILETIME
# MAGIC from Source2 WHERE LANGU = 'E')A) where rn = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed.t_sapisu_0ARCHOBJECT_TEXT

# COMMAND ----------

# DBTITLE 1,[Source] with mapping
# MAGIC %sql
# MAGIC SELECT
# MAGIC AOID,
# MAGIC OBJNR,
# MAGIC LANGU,
# MAGIC DOORPLT
# MAGIC from
# MAGIC (
# MAGIC SELECT
# MAGIC AOID
# MAGIC ,DOORPLT
# MAGIC ,EXTRACT_DATETIME
# MAGIC ,EXTRACT_RUN_ID
# MAGIC ,IDENT
# MAGIC ,LANGU
# MAGIC ,OBJNR
# MAGIC ,ODQ_CHANGEMODE
# MAGIC ,ODQ_ENTITYCNTR
# MAGIC ,XAO
# MAGIC ,ROW_NUMBER () OVER(PARTITION BY AOID, OBJNR ORDER BY FILETIME DESC) AS RN
# MAGIC FROM (
# MAGIC select 
# MAGIC AOID
# MAGIC ,DOORPLT
# MAGIC ,EXTRACT_DATETIME
# MAGIC ,EXTRACT_RUN_ID
# MAGIC ,IDENT
# MAGIC ,LANGU
# MAGIC ,OBJNR
# MAGIC ,ODQ_CHANGEMODE
# MAGIC ,ODQ_ENTITYCNTR
# MAGIC ,XAO
# MAGIC ,'20210831104548' AS FILETIME
# MAGIC from Source1 WHERE LANGU = 'E'
# MAGIC union all
# MAGIC SELECT 
# MAGIC AOID
# MAGIC ,DOORPLT
# MAGIC ,EXTRACT_DATETIME
# MAGIC ,EXTRACT_RUN_ID
# MAGIC ,IDENT
# MAGIC ,LANGU
# MAGIC ,OBJNR
# MAGIC ,ODQ_CHANGEMODE
# MAGIC ,ODQ_ENTITYCNTR
# MAGIC ,XAO
# MAGIC ,'20210831110701' AS FILETIME
# MAGIC from Source2 WHERE LANGU = 'E')A) where rn = 1

# COMMAND ----------

lakedf.createOrReplaceTempView("Target")

# COMMAND ----------

# MAGIC %sql
# MAGIC --scratch
# MAGIC a.PARTNER as businessPartnerNumber
# MAGIC ,a.TYPE as businessPartnerCategoryCode
# MAGIC ,b.TXTMD as businessPartnerCategory
# MAGIC ,a.BPKIND as businessPartnerTypeCode
# MAGIC ,c.TEXT40 as businessPartnerType
# MAGIC ,a.BU_GROUP as businessPartnerGroupCode
# MAGIC ,d.TXT40 as businessPartnerGroup
# MAGIC ,BPEXT as externalBusinessPartnerNumber
# MAGIC ,BU_SORT1 as searchTerm1
# MAGIC ,BU_SORT2 as searchTerm2
# MAGIC ,TITLE as titleCode
# MAGIC ,'To be mapped' as title
# MAGIC --,f.TITLE_MEDI AS title
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
# MAGIC ,CRDAT as createdDate
# MAGIC ,CRTIM as createdTime
# MAGIC ,CHUSR as changedBy
# MAGIC ,CHDAT as changedDate
# MAGIC ,CHTIM as changedTime
# MAGIC ,a.PARTNER_GUID as businessPartnerGUID
# MAGIC ,ADDRCOMM as addressNumber
# MAGIC ,VALID_FROM as validFromDate
# MAGIC ,VALID_TO as validToDate
# MAGIC ,NATPERS as naturalPersonIndicator
# MAGIC FROM raw.sapisu_0BPARTNER_ATTR a
# MAGIC LEFT JOIN raw.sapisu_0BPARTNER_TEXT b
# MAGIC ON a.PARTNER = b.PARTNER and a.TYPE = b.TYPE
# MAGIC LEFT JOIN raw.sapisu_0BPTYPE_TEXT c
# MAGIC ON a.BPKIND = c.BPKIND --and c.SPRAS = 'E'
# MAGIC LEFT JOIN raw.sapisu_0BP_GROUP_TEXT d
# MAGIC ON a.BU_GROUP = d.BU_GROUP and d.SPRAS = 'E'
# MAGIC --LEFT JOIN ZDSTITLET f
# MAGIC --ON a.TITLE = f.TITLE and f.LANGU = 'E'
# MAGIC where a.PARTNER = '0001000090'

# COMMAND ----------

# DBTITLE 1,[Verification] Count Checks
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.t_sapisu_0ARCHOBJECT_TEXT
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (
# MAGIC 
# MAGIC SELECT
# MAGIC AOID,
# MAGIC OBJNR,
# MAGIC LANGU,
# MAGIC DOORPLT
# MAGIC from
# MAGIC (
# MAGIC SELECT
# MAGIC AOID
# MAGIC ,DOORPLT
# MAGIC ,EXTRACT_DATETIME
# MAGIC ,EXTRACT_RUN_ID
# MAGIC ,IDENT
# MAGIC ,LANGU
# MAGIC ,OBJNR
# MAGIC ,ODQ_CHANGEMODE
# MAGIC ,ODQ_ENTITYCNTR
# MAGIC ,XAO
# MAGIC ,ROW_NUMBER () OVER(PARTITION BY AOID,OBJNR ORDER BY FILETIME DESC) AS RN
# MAGIC FROM (
# MAGIC select 
# MAGIC AOID
# MAGIC ,DOORPLT
# MAGIC ,EXTRACT_DATETIME
# MAGIC ,EXTRACT_RUN_ID
# MAGIC ,IDENT
# MAGIC ,LANGU
# MAGIC ,OBJNR
# MAGIC ,ODQ_CHANGEMODE
# MAGIC ,ODQ_ENTITYCNTR
# MAGIC ,XAO
# MAGIC ,'20210831104548' AS FILETIME
# MAGIC from Source1 WHERE LANGU = 'E'
# MAGIC union all
# MAGIC SELECT 
# MAGIC AOID
# MAGIC ,DOORPLT
# MAGIC ,EXTRACT_DATETIME
# MAGIC ,EXTRACT_RUN_ID
# MAGIC ,IDENT
# MAGIC ,LANGU
# MAGIC ,OBJNR
# MAGIC ,ODQ_CHANGEMODE
# MAGIC ,ODQ_ENTITYCNTR
# MAGIC ,XAO
# MAGIC ,'20210831110701' AS FILETIME
# MAGIC from Source2 WHERE LANGU = 'E')A) where rn = 1
# MAGIC 
# MAGIC )

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT AOID, OBJNR, COUNT (*) as count
# MAGIC FROM cleansed.t_sapisu_0ARCHOBJECT_TEXT
# MAGIC GROUP BY AOID, OBJNR
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY AOID, OBJNR order by AOID desc) as rn
# MAGIC FROM cleansed.t_sapisu_0ARCHOBJECT_text
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC SELECT
# MAGIC AOID,
# MAGIC OBJNR,
# MAGIC LANGU,
# MAGIC DOORPLT
# MAGIC from
# MAGIC (
# MAGIC SELECT
# MAGIC AOID
# MAGIC ,DOORPLT
# MAGIC ,EXTRACT_DATETIME
# MAGIC ,EXTRACT_RUN_ID
# MAGIC ,IDENT
# MAGIC ,LANGU
# MAGIC ,OBJNR
# MAGIC ,ODQ_CHANGEMODE
# MAGIC ,ODQ_ENTITYCNTR
# MAGIC ,XAO
# MAGIC ,ROW_NUMBER () OVER(PARTITION BY AOID,OBJNR ORDER BY FILETIME DESC) AS RN
# MAGIC FROM (
# MAGIC select 
# MAGIC AOID
# MAGIC ,DOORPLT
# MAGIC ,EXTRACT_DATETIME
# MAGIC ,EXTRACT_RUN_ID
# MAGIC ,IDENT
# MAGIC ,LANGU
# MAGIC ,OBJNR
# MAGIC ,ODQ_CHANGEMODE
# MAGIC ,ODQ_ENTITYCNTR
# MAGIC ,XAO
# MAGIC ,'20210831104548' AS FILETIME
# MAGIC from Source1 WHERE LANGU = 'E'
# MAGIC union all
# MAGIC SELECT 
# MAGIC AOID
# MAGIC ,DOORPLT
# MAGIC ,EXTRACT_DATETIME
# MAGIC ,EXTRACT_RUN_ID
# MAGIC ,IDENT
# MAGIC ,LANGU
# MAGIC ,OBJNR
# MAGIC ,ODQ_CHANGEMODE
# MAGIC ,ODQ_ENTITYCNTR
# MAGIC ,XAO
# MAGIC ,'20210831110701' AS FILETIME
# MAGIC from Source2 WHERE LANGU = 'E')A) where rn = 1
# MAGIC 
# MAGIC 
# MAGIC EXCEPT
# MAGIC 
# MAGIC SELECT
# MAGIC AOID,
# MAGIC OBJNR,
# MAGIC LANGU,
# MAGIC DOORPLT
# MAGIC FROM
# MAGIC cleansed.t_sapisu_0ARCHOBJECT_TEXT

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select count (*) from (
# MAGIC SELECT
# MAGIC AOID,
# MAGIC OBJNR,
# MAGIC LANGU,
# MAGIC DOORPLT
# MAGIC FROM
# MAGIC cleansed.t_sapisu_0ARCHOBJECT_TEXT
# MAGIC 
# MAGIC except 
# MAGIC SELECT
# MAGIC AOID,
# MAGIC OBJNR,
# MAGIC LANGU,
# MAGIC DOORPLT
# MAGIC from
# MAGIC (
# MAGIC SELECT
# MAGIC AOID
# MAGIC ,DOORPLT
# MAGIC ,EXTRACT_DATETIME
# MAGIC ,EXTRACT_RUN_ID
# MAGIC ,IDENT
# MAGIC ,LANGU
# MAGIC ,OBJNR
# MAGIC ,ODQ_CHANGEMODE
# MAGIC ,ODQ_ENTITYCNTR
# MAGIC ,XAO
# MAGIC ,ROW_NUMBER () OVER(PARTITION BY AOID,OBJNR ORDER BY FILETIME DESC) AS RN
# MAGIC FROM (
# MAGIC select 
# MAGIC AOID
# MAGIC ,DOORPLT
# MAGIC ,EXTRACT_DATETIME
# MAGIC ,EXTRACT_RUN_ID
# MAGIC ,IDENT
# MAGIC ,LANGU
# MAGIC ,OBJNR
# MAGIC ,ODQ_CHANGEMODE
# MAGIC ,ODQ_ENTITYCNTR
# MAGIC ,XAO
# MAGIC ,'20210831104548' AS FILETIME
# MAGIC from Source1 WHERE LANGU = 'E'
# MAGIC union all
# MAGIC SELECT 
# MAGIC AOID
# MAGIC ,DOORPLT
# MAGIC ,EXTRACT_DATETIME
# MAGIC ,EXTRACT_RUN_ID
# MAGIC ,IDENT
# MAGIC ,LANGU
# MAGIC ,OBJNR
# MAGIC ,ODQ_CHANGEMODE
# MAGIC ,ODQ_ENTITYCNTR
# MAGIC ,XAO
# MAGIC ,'20210831110701' AS FILETIME
# MAGIC from Source2 WHERE LANGU = 'E')A) where rn = 1
# MAGIC )count

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC AOID,
# MAGIC OBJNR,
# MAGIC LANGU,
# MAGIC DOORPLT
# MAGIC from
# MAGIC (
# MAGIC SELECT
# MAGIC AOID
# MAGIC ,DOORPLT
# MAGIC ,EXTRACT_DATETIME
# MAGIC ,EXTRACT_RUN_ID
# MAGIC ,IDENT
# MAGIC ,LANGU
# MAGIC ,OBJNR
# MAGIC ,ODQ_CHANGEMODE
# MAGIC ,ODQ_ENTITYCNTR
# MAGIC ,XAO
# MAGIC ,ROW_NUMBER () OVER(PARTITION BY AOID,OBJNR ORDER BY FILETIME DESC) AS RN
# MAGIC FROM (
# MAGIC select 
# MAGIC AOID
# MAGIC ,DOORPLT
# MAGIC ,EXTRACT_DATETIME
# MAGIC ,EXTRACT_RUN_ID
# MAGIC ,IDENT
# MAGIC ,LANGU
# MAGIC ,OBJNR
# MAGIC ,ODQ_CHANGEMODE
# MAGIC ,ODQ_ENTITYCNTR
# MAGIC ,XAO
# MAGIC ,'20210831104548' AS FILETIME
# MAGIC from Source1 WHERE LANGU = 'E'
# MAGIC union all
# MAGIC SELECT 
# MAGIC AOID
# MAGIC ,DOORPLT
# MAGIC ,EXTRACT_DATETIME
# MAGIC ,EXTRACT_RUN_ID
# MAGIC ,IDENT
# MAGIC ,LANGU
# MAGIC ,OBJNR
# MAGIC ,ODQ_CHANGEMODE
# MAGIC ,ODQ_ENTITYCNTR
# MAGIC ,XAO
# MAGIC ,'20210831110701' AS FILETIME
# MAGIC from Source2 WHERE LANGU = 'E')A) where aoid = '6237188' and objnr = 'I000000000000008731471'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed.t_sapisu_0archobject_text where aoid = '6237188' and objnr = 'I000000000000008731471'
