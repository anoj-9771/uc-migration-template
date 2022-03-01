# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup
storage_account_name = "saswcnonprod01landingtst"
storage_account_access_key = dbutils.secrets.get(scope="Test-Access",key="test-blob-key")
container_name = "archive"
file_location="wasbs://archive@saswcnonprod01landingtst.blob.core.windows.net/sapisu/20210922/20210922_09:57:44/VIBDNODE_20210922092322.json"
file_type = "json"
print(storage_account_name)

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)


# COMMAND ----------

# DBTITLE 1,[Source] Loading data into Dataframe
df = spark.read.format(file_type).option("inferSchema", "true").load(file_location)

# COMMAND ----------

# DBTITLE 1,[Source] Schema Check - Refer to Raw2Cleansed Mapping
df.printSchema()

# COMMAND ----------

# DBTITLE 0,[Result] Load Count Result into DataFrame
lakedf = spark.sql("select * from cleansed.t_isu_vibdnode ")

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
lakedf.printSchema()

# COMMAND ----------

# DBTITLE 1,[Source] Creating Temporary Table
df.createOrReplaceTempView("Source")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from Source

# COMMAND ----------

# DBTITLE 1,[Source] with mapping
# MAGIC %sql
# MAGIC SELECT
# MAGIC INTRENO AS architecturalObjectInternalId,
# MAGIC TREE AS alternativeDisplayStructureId,
# MAGIC AOTYPE_AO AS architecturalObjectTypeCode,
# MAGIC b.XMAOTYPE AS architecturalObjectType,
# MAGIC AONR_AO AS architecturalObjectNumber,
# MAGIC PARENT AS parentArchitecturalObjectInternalId,
# MAGIC AOTYPE_PA AS parentArchitecturalObjectTypeCode,
# MAGIC c.XMAOTYPE AS parentArchitecturalObjectType,
# MAGIC AONR_PA AS parentArchitecturalObjectNumber
# MAGIC FROM Source a
# MAGIC LEFT JOIN cleansed.t_sapisu_tivbdarobjtypet b
# MAGIC ON a.AOTYPE_AO= b.AOTYPE 
# MAGIC LEFT JOIN cleansed.t_sapisu_tivbdarobjtypet c
# MAGIC ON a.AOTYPE_PA = c.AOTYPE 

# COMMAND ----------

lakedf.createOrReplaceTempView("Target")

# COMMAND ----------

# DBTITLE 1,[Verification] Count Checks
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.t_isu_vibdnode  
# MAGIC union all 
# MAGIC select count (*) as RecordCount, 'Source' as TableName from Source 

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT architecturalObjectInternalId ,alternativeDisplayStructureId, COUNT (*) as count
# MAGIC FROM cleansed.t_isu_vibdnode 
# MAGIC GROUP BY architecturalObjectInternalId,alternativeDisplayStructureId
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY architecturalObjectInternalId order by architecturalObjectInternalId) as rn
# MAGIC FROM  cleansed.t_isu_vibdnode 
# MAGIC )a where a.rn > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC distinct architecturalObjectType,parentArchitecturalObjectType from
# MAGIC cleansed.t_isu_vibdnode 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC distinct
# MAGIC b.XMAOTYPE AS architecturalObjectType,
# MAGIC c.XMAOTYPE AS parentArchitecturalObjectType
# MAGIC 
# MAGIC FROM Source a
# MAGIC LEFT JOIN cleansed.t_sapisu_tivbdarobjtypet b
# MAGIC ON a.AOTYPE_AO= b.AOTYPE 
# MAGIC --and b.SPRAS = 'E'
# MAGIC LEFT JOIN cleansed.t_sapisu_tivbdarobjtypet c
# MAGIC ON a.AOTYPE_PA = c.AOTYPE 
# MAGIC --and c.SPRAS = 'E'

# COMMAND ----------

# DBTITLE 1,[Target] display records
# MAGIC %sql
# MAGIC SELECT
# MAGIC * from
# MAGIC cleansed.t_isu_vibdnode 

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC architecturalObjectType,parentArchitecturalObjectType from
# MAGIC cleansed.t_isu_vibdnode  where architecturalObjectInternalId = 'I000100000727'

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC b.XMAOTYPE AS architecturalObjectType,
# MAGIC c.XMAOTYPE AS parentArchitecturalObjectType
# MAGIC FROM Source a
# MAGIC LEFT JOIN cleansed.t_sapisu_tivbdarobjtypet b
# MAGIC ON a.AOTYPE_AO= b.AOTYPE 
# MAGIC LEFT JOIN cleansed.t_sapisu_tivbdarobjtypet c
# MAGIC ON a.AOTYPE_PA = c.AOTYPE 
# MAGIC where INTRENO = 'I000100000727'

# COMMAND ----------

# DBTITLE 1,[Verification] Compare  Source and Target  Data
# MAGIC %sql
# MAGIC SELECT
# MAGIC INTRENO AS architecturalObjectInternalId,
# MAGIC case when TREE is null then 'na' else TREE end as alternativeDisplayStructureId,
# MAGIC AOTYPE_AO AS architecturalObjectTypeCode,
# MAGIC b.XMAOTYPE AS architecturalObjectType,
# MAGIC AONR_AO AS architecturalObjectNumber,
# MAGIC PARENT AS parentArchitecturalObjectInternalId,
# MAGIC AOTYPE_PA AS parentArchitecturalObjectTypeCode,
# MAGIC c.XMAOTYPE AS parentArchitecturalObjectType,
# MAGIC AONR_PA AS parentArchitecturalObjectNumber
# MAGIC FROM Source a
# MAGIC LEFT JOIN cleansed.t_sapisu_tivbdarobjtypet b
# MAGIC ON a.AOTYPE_AO= b.AOTYPE 
# MAGIC LEFT JOIN cleansed.t_sapisu_tivbdarobjtypet c
# MAGIC ON a.AOTYPE_PA = c.AOTYPE  
# MAGIC except
# MAGIC select
# MAGIC architecturalObjectInternalId,
# MAGIC alternativeDisplayStructureId,
# MAGIC architecturalObjectTypeCode,
# MAGIC architecturalObjectType,
# MAGIC architecturalObjectNumber,
# MAGIC parentArchitecturalObjectInternalId,
# MAGIC parentArchitecturalObjectTypeCode,
# MAGIC parentArchitecturalObjectType,
# MAGIC parentArchitecturalObjectNumber
# MAGIC from
# MAGIC cleansed.t_isu_vibdnode 

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC architecturalObjectInternalId,
# MAGIC alternativeDisplayStructureId,
# MAGIC architecturalObjectTypeCode,
# MAGIC architecturalObjectType,
# MAGIC architecturalObjectNumber,
# MAGIC parentArchitecturalObjectInternalId,
# MAGIC parentArchitecturalObjectTypeCode,
# MAGIC parentArchitecturalObjectType,
# MAGIC parentArchitecturalObjectNumber
# MAGIC from
# MAGIC cleansed.t_isu_vibdnode 
# MAGIC except
# MAGIC SELECT
# MAGIC INTRENO AS architecturalObjectInternalId,
# MAGIC case when TREE is null then 'na' else TREE end as alternativeDisplayStructureId,
# MAGIC AOTYPE_AO AS architecturalObjectTypeCode,
# MAGIC b.XMAOTYPE AS architecturalObjectType,
# MAGIC AONR_AO AS architecturalObjectNumber,
# MAGIC PARENT AS parentArchitecturalObjectInternalId,
# MAGIC AOTYPE_PA AS parentArchitecturalObjectTypeCode,
# MAGIC c.XMAOTYPE AS parentArchitecturalObjectType,
# MAGIC AONR_PA AS parentArchitecturalObjectNumber
# MAGIC FROM Source a
# MAGIC LEFT JOIN cleansed.t_sapisu_tivbdarobjtypet b
# MAGIC ON a.AOTYPE_AO= b.AOTYPE 
# MAGIC LEFT JOIN cleansed.t_sapisu_tivbdarobjtypet c
# MAGIC ON a.AOTYPE_PA = c.AOTYPE  

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC INTRENO AS architecturalObjectInternalId,
# MAGIC TREE AS alternativeDisplayStructureId,
# MAGIC c.XMAOTYPE AS parentArchitecturalObjectType
# MAGIC FROM Source a
# MAGIC LEFT JOIN cleansed.t_sapisu_tivbdarobjtypet b
# MAGIC ON a.AOTYPE_AO= b.AOTYPE 
# MAGIC LEFT JOIN cleansed.t_sapisu_tivbdarobjtypet c
# MAGIC ON a.AOTYPE_PA = c.AOTYPE  
# MAGIC except
# MAGIC select
# MAGIC architecturalObjectInternalId,
# MAGIC alternativeDisplayStructureId,
# MAGIC parentArchitecturalObjectType
# MAGIC from
# MAGIC cleansed.t_isu_vibdnode 

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC architecturalObjectInternalId,
# MAGIC alternativeDisplayStructureId,
# MAGIC parentArchitecturalObjectType
# MAGIC from
# MAGIC cleansed.t_isu_vibdnode 
# MAGIC except
# MAGIC SELECT
# MAGIC INTRENO AS architecturalObjectInternalId,
# MAGIC TREE AS alternativeDisplayStructureId,
# MAGIC c.XMAOTYPE AS parentArchitecturalObjectType
# MAGIC FROM Source a
# MAGIC --LEFT JOIN cleansed.t_sapisu_tivbdarobjtypet b
# MAGIC --ON a.AOTYPE_AO= b.AOTYPE 
# MAGIC LEFT JOIN cleansed.t_sapisu_tivbdarobjtypet c
# MAGIC ON a.AOTYPE_PA = c.AOTYPE  

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC INTRENO AS architecturalObjectInternalId,
# MAGIC case when TREE is null then 'na' else TREE end as alternativeDisplayStructureId,
# MAGIC c.XMAOTYPE AS parentArchitecturalObjectType
# MAGIC FROM Source a
# MAGIC LEFT JOIN cleansed.t_sapisu_tivbdarobjtypet c
# MAGIC ON a.AOTYPE_PA = c.AOTYPE  where INTRENO ='I000100000014'

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC architecturalObjectInternalId,
# MAGIC alternativeDisplayStructureId,
# MAGIC parentArchitecturalObjectType
# MAGIC from
# MAGIC cleansed.t_isu_vibdnode  where architecturalObjectInternalId ='I000100000014'

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC distinct
# MAGIC parentArchitecturalObjectType
# MAGIC from
# MAGIC cleansed.t_isu_vibdnode 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC distinct
# MAGIC c.XMAOTYPE AS parentArchitecturalObjectType
# MAGIC FROM Source a
# MAGIC LEFT JOIN cleansed.t_sapisu_tivbdarobjtypet b
# MAGIC ON a.AOTYPE_AO= b.AOTYPE 
# MAGIC LEFT JOIN cleansed.t_sapisu_tivbdarobjtypet c
# MAGIC ON a.AOTYPE_PA = c.AOTYPE 

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC architecturalObjectType,parentArchitecturalObjectType from
# MAGIC cleansed.t_sap_vibdnode where architecturalObjectInternalId = 'I000100000727'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed.t_sap_vibdnode where architecturalObjectInternalId ='I000100000342'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC INTRENO AS architecturalObjectInternalId,
# MAGIC TREE AS alternativeDisplayStructureId,
# MAGIC AOTYPE_AO AS architecturalObjectTypeCode,
# MAGIC b.XMAOTYPE AS architecturalObjectType,
# MAGIC AONR_AO AS architecturalObjectNumber,
# MAGIC PARENT AS parentArchitecturalObjectInternalId,
# MAGIC AOTYPE_PA AS parentArchitecturalObjectTypeCode,
# MAGIC c.XMAOTYPE AS parentArchitecturalObjectType,
# MAGIC AONR_PA AS parentArchitecturalObjectNumber
# MAGIC FROM Source a
# MAGIC LEFT JOIN cleansed.t_sapisu_tivbdarobjtypet b
# MAGIC ON a.AOTYPE_AO= b.AOTYPE 
# MAGIC LEFT JOIN cleansed.t_sapisu_tivbdarobjtypet c
# MAGIC ON a.AOTYPE_PA = c.AOTYPE  where INTRENO ='I000100000000'

# COMMAND ----------

# MAGIC %sql
# MAGIC select TREE AS alternativeDisplayStructureId from Source where INTRENO ='I000100000000'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed.t_sap_vibdnode where architecturalObjectInternalId ='I000100000000'

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct TREE from  source 

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct alternativeDisplayStructureId from cleansed.t_isu_vibdnode 
