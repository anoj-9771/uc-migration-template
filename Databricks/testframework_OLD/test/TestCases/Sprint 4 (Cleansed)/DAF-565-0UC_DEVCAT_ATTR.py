# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup
storage_account_name = "saswcnonprod01landingtst"
storage_account_access_key = dbutils.secrets.get(scope="Test-Access",key="test-blob-key")
container_name = "archive"
file_location = "wasbs://archive@saswcnonprod01landingtst.blob.core.windows.net/sapisu/20210824/20210824_13:47:47/0UC_DEVCAT_ATTR_20210824132259.json"
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
lakedf = spark.sql("select * from cleansed.t_sapisu_0uc_devcat_attr")

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
# MAGIC MATNR	as	materialNumber
# MAGIC ,KOMBINAT	as	deviceCategoryCombination
# MAGIC ,FUNKLAS	as	functionClassCode
# MAGIC ,fk.functionClass	as	functionClass
# MAGIC ,BAUKLAS	as	constructionClassCode
# MAGIC ,bk.constructionClass	as	constructionClass
# MAGIC ,BAUFORM	as	deviceCategoryDescription
# MAGIC ,BAUTXT	as	deviceCategoryName
# MAGIC ,PTBNUM	as	ptiNumber
# MAGIC ,DVGWNUM	as	ggwaNumber
# MAGIC ,BGLKZ	as	certificationRequirementType
# MAGIC ,ZWGRUPPE	as	registerGroupCode
# MAGIC ,b.registerGroup	as	registerGroup
# MAGIC --,UEBERVER	as	transformationRatio
# MAGIC ,CAST(UEBERVER AS DECIMAL(10,3)) AS transformationRatio
# MAGIC ,AENAM	as	changedBy
# MAGIC ,AEDAT	as	lastChangedDate
# MAGIC ,SPARTE	as	division
# MAGIC --,NENNBEL	as	nominalLoad
# MAGIC ,CAST(NENNBEL AS DECIMAL(10,4)) AS nominalLoad
# MAGIC ,STELLPLATZ	as	containerSpaceCount
# MAGIC --,HOEHEBEH	as	containerCategoryHeight
# MAGIC ,CAST(HOEHEBEH AS DECIMAL(5,2)) AS containerCategoryHeight 
# MAGIC --,BREITEBEH	as	containerCategoryWidth
# MAGIC ,CAST(BREITEBEH AS DECIMAL(5,2)) AS containerCategoryWidth
# MAGIC --,TIEFEBEH	as	containerCategoryDepth
# MAGIC ,CAST(TIEFEBEH AS DECIMAL(5,2)) AS containerCategoryDepth
# MAGIC from Source d
# MAGIC left join cleansed.t_sapisu_0uc_funklas_text fk 
# MAGIC on fk.functionClassCode = d.FUNKLAS
# MAGIC left join cleansed.t_sapisu_0uc_bauklas_text bk 
# MAGIC on bk.constructionClassCode = d.BAUKLAS --and SPRAS ='E'
# MAGIC left join cleansed.t_sapisu_0uc_reggrp_text b
# MAGIC on d.ZWGRUPPE = b.registerGroupCode

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC materialNumber,
# MAGIC deviceCategoryCombination,
# MAGIC functionClassCode,
# MAGIC functionClass,
# MAGIC constructionClassCode,
# MAGIC constructionClass,
# MAGIC deviceCategoryDescription,
# MAGIC deviceCategoryName,
# MAGIC ptiNumber,
# MAGIC ggwaNumber,
# MAGIC certificationRequirementType,
# MAGIC registerGroupCode,
# MAGIC registerGroup,
# MAGIC transformationRatio,
# MAGIC changedBy,
# MAGIC lastChangedDate,
# MAGIC division,
# MAGIC nominalLoad,
# MAGIC containerSpaceCount,
# MAGIC containerCategoryHeight,
# MAGIC containerCategoryWidth,
# MAGIC containerCategoryDepth
# MAGIC FROM
# MAGIC cleansed.t_sapisu_0uc_devcat_attr

# COMMAND ----------

lakedf.createOrReplaceTempView("Target")

# COMMAND ----------

# DBTITLE 1,[Verification] Count Checks
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.t_sapisu_0uc_devcat_attr
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from Source

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT materialNumber , COUNT (*) as count
# MAGIC FROM cleansed.t_sapisu_0uc_devcat_attr
# MAGIC GROUP BY materialNumber
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY materialNumber order by materialNumber) as rn
# MAGIC FROM  cleansed.t_sapisu_0uc_devcat_attr
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC SELECT
# MAGIC MATNR	as	materialNumber
# MAGIC ,KOMBINAT	as	deviceCategoryCombination
# MAGIC ,FUNKLAS	as	functionClassCode
# MAGIC ,fk.functionClass	as	functionClass
# MAGIC ,BAUKLAS	as	constructionClassCode
# MAGIC ,bk.constructionClass	as	constructionClass
# MAGIC ,BAUFORM	as	deviceCategoryDescription
# MAGIC ,BAUTXT	as	deviceCategoryName
# MAGIC ,PTBNUM	as	ptiNumber
# MAGIC ,DVGWNUM	as	ggwaNumber
# MAGIC ,BGLKZ	as	certificationRequirementType
# MAGIC ,ZWGRUPPE	as	registerGroupCode
# MAGIC ,b.registerGroup	as	registerGroup
# MAGIC --,UEBERVER	as	transformationRatio
# MAGIC ,CAST(UEBERVER AS DECIMAL(10,3)) AS transformationRatio
# MAGIC ,AENAM	as	changedBy
# MAGIC ,AEDAT	as	lastChangedDate
# MAGIC ,SPARTE	as	division
# MAGIC --,NENNBEL	as	nominalLoad
# MAGIC ,CAST(NENNBEL AS DECIMAL(10,4)) AS nominalLoad
# MAGIC ,STELLPLATZ	as	containerSpaceCount
# MAGIC --,HOEHEBEH	as	containerCategoryHeight
# MAGIC ,CAST(HOEHEBEH AS DECIMAL(5,2)) AS containerCategoryHeight 
# MAGIC --,BREITEBEH	as	containerCategoryWidth
# MAGIC ,CAST(BREITEBEH AS DECIMAL(5,2)) AS containerCategoryWidth
# MAGIC --,TIEFEBEH	as	containerCategoryDepth
# MAGIC ,CAST(TIEFEBEH AS DECIMAL(5,2)) AS containerCategoryDepth
# MAGIC from Source d
# MAGIC left join cleansed.t_sapisu_0uc_funklas_text fk 
# MAGIC on fk.functionClassCode = d.FUNKLAS
# MAGIC left join cleansed.t_sapisu_0uc_bauklas_text bk 
# MAGIC on bk.constructionClassCode = d.BAUKLAS --and SPRAS ='E'
# MAGIC left join cleansed.t_sapisu_0uc_reggrp_text b
# MAGIC on d.ZWGRUPPE = b.registerGroupCode
# MAGIC EXCEPT
# MAGIC select
# MAGIC materialNumber
# MAGIC ,deviceCategoryCombination
# MAGIC ,functionClassCode                             
# MAGIC ,functionClass                                 
# MAGIC ,constructionClassCode                         
# MAGIC ,constructionClass                             
# MAGIC ,deviceCategoryDescription                     
# MAGIC ,deviceCategoryName                  
# MAGIC ,ptiNumber                                     
# MAGIC ,ggwaNumber                                    
# MAGIC ,certificationRequirementType                  
# MAGIC ,registerGroupCode                             
# MAGIC ,registerGroup                                 
# MAGIC ,transformationRatio                           
# MAGIC ,changedBy                                     
# MAGIC ,lastChangedDate                               
# MAGIC ,division                                      
# MAGIC ,nominalLoad                                   
# MAGIC ,containerSpaceCount                           
# MAGIC ,containerCategoryHeight                       
# MAGIC ,containerCategoryWidth                        
# MAGIC ,containerCategoryDepth
# MAGIC FROM
# MAGIC cleansed.t_sapisu_0uc_devcat_attr

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC materialNumber,
# MAGIC deviceCategoryCombination,
# MAGIC functionClassCode,
# MAGIC functionClass,
# MAGIC constructionClassCode,
# MAGIC constructionClass,
# MAGIC deviceCategoryDescription,
# MAGIC deviceCategoryName,
# MAGIC ptiNumber,
# MAGIC ggwaNumber,
# MAGIC certificationRequirementType,
# MAGIC registerGroupCode,
# MAGIC registerGroup,
# MAGIC transformationRatio,
# MAGIC changedBy,
# MAGIC lastChangedDate,
# MAGIC division,
# MAGIC nominalLoad,
# MAGIC containerSpaceCount,
# MAGIC containerCategoryHeight,
# MAGIC containerCategoryWidth,
# MAGIC containerCategoryDepth
# MAGIC FROM
# MAGIC cleansed.t_sapisu_0uc_devcat_attr
# MAGIC 
# MAGIC EXCEPT
# MAGIC 
# MAGIC SELECT
# MAGIC MATNR	as	materialNumber
# MAGIC ,KOMBINAT	as	deviceCategoryCombination
# MAGIC ,FUNKLAS	as	functionClassCode
# MAGIC ,fk.functionClass	as	functionClass
# MAGIC ,BAUKLAS	as	constructionClassCode
# MAGIC ,bk.constructionClass	as	constructionClass
# MAGIC ,BAUFORM	as	deviceCategoryDescription
# MAGIC ,BAUTXT	as	deviceCategoryName
# MAGIC ,PTBNUM	as	ptiNumber
# MAGIC ,DVGWNUM	as	ggwaNumber
# MAGIC ,BGLKZ	as	certificationRequirementType
# MAGIC ,ZWGRUPPE	as	registerGroupCode
# MAGIC ,b.registerGroup	as	registerGroup
# MAGIC --,UEBERVER	as	transformationRatio
# MAGIC ,CAST(UEBERVER AS DECIMAL(10,3)) AS transformationRatio
# MAGIC ,AENAM	as	changedBy
# MAGIC ,AEDAT	as	lastChangedDate
# MAGIC ,SPARTE	as	division
# MAGIC --,NENNBEL	as	nominalLoad
# MAGIC ,CAST(NENNBEL AS DECIMAL(10,4)) AS nominalLoad
# MAGIC ,STELLPLATZ	as	containerSpaceCount
# MAGIC --,HOEHEBEH	as	containerCategoryHeight
# MAGIC ,CAST(HOEHEBEH AS DECIMAL(5,2)) AS containerCategoryHeight 
# MAGIC --,BREITEBEH	as	containerCategoryWidth
# MAGIC ,CAST(BREITEBEH AS DECIMAL(5,2)) AS containerCategoryWidth
# MAGIC --,TIEFEBEH	as	containerCategoryDepth
# MAGIC ,CAST(TIEFEBEH AS DECIMAL(5,2)) AS containerCategoryDepth
# MAGIC from Source d
# MAGIC left join cleansed.t_sapisu_0uc_funklas_text fk 
# MAGIC on fk.functionClassCode = d.FUNKLAS
# MAGIC left join cleansed.t_sapisu_0uc_bauklas_text bk 
# MAGIC on bk.constructionClassCode = d.BAUKLAS --and SPRAS ='E'
# MAGIC left join cleansed.t_sapisu_0uc_reggrp_text b
# MAGIC on d.ZWGRUPPE = b.registerGroupCode
