# Databricks notebook source
# DBTITLE 1,[Config]
Targetdf = spark.sql("select * from cleansed.t_sapisu_0uc_devcat_attr")

# COMMAND ----------

# DBTITLE 1,[Schema Checks] Mapping vs Target
Targetdf.printSchema()

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM raw.sapisu_0uc_devcat_attr

# COMMAND ----------

# DBTITLE 1,[Source] Raw Table with Transformation
# MAGIC %sql
# MAGIC SELECT
# MAGIC MATNR	as	materialNumber
# MAGIC ,KOMBINAT	as	deviceCategoryCombination
# MAGIC ,d.FUNKLAS	as	functionClassCode
# MAGIC ,fk.FUNKTXT	as	functionClass
# MAGIC ,d.BAUKLAS	as	constructionClassCode
# MAGIC ,bk.BAUKLTXT	as	constructionClass
# MAGIC ,BAUFORM	as	deviceCategoryDescription
# MAGIC ,BAUTXT	as	deviceCategoryName
# MAGIC ,PTBNUM	as	ptiNumber
# MAGIC ,DVGWNUM	as	ggwaNumber
# MAGIC ,BGLKZ	as	certificationRequirementType
# MAGIC ,d.ZWGRUPPE	as	registerGroupCode
# MAGIC ,b.EZWG_INFO	as	registerGroup
# MAGIC ,UEBERVER	as	transformationRatio
# MAGIC ,AENAM	as	changedBy
# MAGIC ,AEDAT	as	lastChangedDate
# MAGIC ,SPARTE	as	division
# MAGIC ,NENNBEL	as	nominalLoad
# MAGIC ,STELLPLATZ	as	containerSpaceCount
# MAGIC ,HOEHEBEH	as	containerCategoryHeight
# MAGIC ,BREITEBEH	as	containerCategoryWidth
# MAGIC ,TIEFEBEH	as	containerCategoryDepth
# MAGIC from raw.sapisu_0uc_devcat_attr d
# MAGIC left join raw.sapisu_0UC_FUNKLAS_TEXT fk 
# MAGIC on fk.FUNKLAS = d.FUNKLAS
# MAGIC left join raw.sapisu_0UC_BAUKLAS_TEXT bk 
# MAGIC on bk.BAUKLAS = d.BAUKLAS and bk.SPRAS ='E'
# MAGIC left join raw.sapisu_0UC_REGGRP_TEXT b
# MAGIC on d.ZWGRUPPE = b.ZWGRUPPE

# COMMAND ----------

# DBTITLE 1,[Target] Cleansed Table
# MAGIC %sql
# MAGIC select * from cleansed.t_sapisu_0uc_devcat_attr

# COMMAND ----------

# DBTITLE 1,[Reconciliation] Count Checks Between Source and Target
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'raw.sapisu_0uc_devcat_attr' as TableName from (
# MAGIC SELECT
# MAGIC MATNR	as	materialNumber
# MAGIC ,KOMBINAT	as	deviceCategoryCombination
# MAGIC ,d.FUNKLAS	as	functionClassCode
# MAGIC ,fk.FUNKTXT	as	functionClass
# MAGIC ,d.BAUKLAS	as	constructionClassCode
# MAGIC ,bk.BAUKLTXT	as	constructionClass
# MAGIC ,BAUFORM	as	deviceCategoryDescription
# MAGIC ,BAUTXT	as	deviceCategoryName
# MAGIC ,PTBNUM	as	ptiNumber
# MAGIC ,DVGWNUM	as	ggwaNumber
# MAGIC ,BGLKZ	as	certificationRequirementType
# MAGIC ,d.ZWGRUPPE	as	registerGroupCode
# MAGIC ,b.EZWG_INFO	as	registerGroup
# MAGIC ,UEBERVER	as	transformationRatio
# MAGIC ,AENAM	as	changedBy
# MAGIC ,AEDAT	as	lastChangedDate
# MAGIC ,SPARTE	as	division
# MAGIC ,NENNBEL	as	nominalLoad
# MAGIC ,STELLPLATZ	as	containerSpaceCount
# MAGIC ,HOEHEBEH	as	containerCategoryHeight
# MAGIC ,BREITEBEH	as	containerCategoryWidth
# MAGIC ,TIEFEBEH	as	containerCategoryDepth
# MAGIC from raw.sapisu_0uc_devcat_attr d
# MAGIC left join raw.sapisu_0UC_FUNKLAS_TEXT fk 
# MAGIC on fk.FUNKLAS = d.FUNKLAS
# MAGIC left join raw.sapisu_0UC_BAUKLAS_TEXT bk 
# MAGIC on bk.BAUKLAS = d.BAUKLAS and bk.SPRAS ='E'
# MAGIC left join raw.sapisu_0UC_REGGRP_TEXT b
# MAGIC on d.ZWGRUPPE = b.ZWGRUPPE)a
# MAGIC 
# MAGIC union
# MAGIC 
# MAGIC select count (*) as RecordCount, 'cleansed.t_sapisu_0uc_devcat_attr' as TableName from  cleansed.t_sapisu_0uc_devcat_attr

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT materialNumber, COUNT (*) as count
# MAGIC FROM cleansed.t_sapisu_0uc_devcat_attr
# MAGIC GROUP BY materialNumber
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC MATNR	as	materialNumber
# MAGIC ,KOMBINAT	as	deviceCategoryCombination
# MAGIC ,d.FUNKLAS	as	functionClassCode
# MAGIC ,fk.FUNKTXT	as	functionClass
# MAGIC ,d.BAUKLAS	as	constructionClassCode
# MAGIC ,bk.BAUKLTXT	as	constructionClass
# MAGIC ,BAUFORM	as	deviceCategoryDescription
# MAGIC ,BAUTXT	as	deviceCategoryName
# MAGIC ,PTBNUM	as	ptiNumber
# MAGIC ,DVGWNUM	as	ggwaNumber
# MAGIC ,BGLKZ	as	certificationRequirementType
# MAGIC ,d.ZWGRUPPE	as	registerGroupCode
# MAGIC ,b.EZWG_INFO	as	registerGroup
# MAGIC ,UEBERVER	as	transformationRatio
# MAGIC ,AENAM	as	changedBy
# MAGIC ,AEDAT	as	lastChangedDate
# MAGIC ,SPARTE	as	division
# MAGIC ,NENNBEL	as	nominalLoad
# MAGIC ,STELLPLATZ	as	containerSpaceCount
# MAGIC ,HOEHEBEH	as	containerCategoryHeight
# MAGIC ,BREITEBEH	as	containerCategoryWidth
# MAGIC ,TIEFEBEH	as	containerCategoryDepth,
# MAGIC row_number() OVER(PARTITION BY MATNR order by MATNR) as rn
# MAGIC from raw.sapisu_0uc_devcat_attr d
# MAGIC left join raw.sapisu_0UC_FUNKLAS_TEXT fk 
# MAGIC on fk.FUNKLAS = d.FUNKLAS
# MAGIC left join raw.sapisu_0UC_BAUKLAS_TEXT bk 
# MAGIC on bk.BAUKLAS = d.BAUKLAS and bk.SPRAS ='E'
# MAGIC left join raw.sapisu_0UC_REGGRP_TEXT b
# MAGIC on d.ZWGRUPPE = b.ZWGRUPPE
# MAGIC )a 
# MAGIC where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source to Target
# MAGIC %sql
# MAGIC select * from (
# MAGIC SELECT
# MAGIC MATNR	as	materialNumber
# MAGIC ,KOMBINAT	as	deviceCategoryCombination
# MAGIC ,d.FUNKLAS	as	functionClassCode
# MAGIC ,fk.FUNKTXT	as	functionClass
# MAGIC ,d.BAUKLAS	as	constructionClassCode
# MAGIC ,bk.BAUKLTXT	as	constructionClass
# MAGIC ,BAUFORM	as	deviceCategoryDescription
# MAGIC ,BAUTXT	as	deviceCategoryName
# MAGIC ,PTBNUM	as	ptiNumber
# MAGIC ,DVGWNUM	as	ggwaNumber
# MAGIC ,BGLKZ	as	certificationRequirementType
# MAGIC ,d.ZWGRUPPE	as	registerGroupCode
# MAGIC ,b.EZWG_INFO	as	registerGroup
# MAGIC ,UEBERVER	as	transformationRatio
# MAGIC ,AENAM	as	changedBy
# MAGIC ,AEDAT	as	lastChangedDate
# MAGIC ,SPARTE	as	division
# MAGIC ,NENNBEL	as	nominalLoad
# MAGIC ,STELLPLATZ	as	containerSpaceCount
# MAGIC ,HOEHEBEH	as	containerCategoryHeight
# MAGIC ,BREITEBEH	as	containerCategoryWidth
# MAGIC ,TIEFEBEH	as	containerCategoryDepth
# MAGIC from raw.sapisu_0uc_devcat_attr d
# MAGIC left join raw.sapisu_0UC_FUNKLAS_TEXT fk 
# MAGIC on fk.FUNKLAS = d.FUNKLAS
# MAGIC left join raw.sapisu_0UC_BAUKLAS_TEXT bk 
# MAGIC on bk.BAUKLAS = d.BAUKLAS and bk.SPRAS ='E'
# MAGIC left join raw.sapisu_0UC_REGGRP_TEXT b
# MAGIC on d.ZWGRUPPE = b.ZWGRUPPE
# MAGIC )a
# MAGIC except 
# MAGIC SELECT
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
# MAGIC 
# MAGIC FROM
# MAGIC cleansed.t_sapisu_0uc_devcat_attr

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target to Source
# MAGIC %sql
# MAGIC SELECT
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
# MAGIC except
# MAGIC select * from (
# MAGIC SELECT
# MAGIC MATNR	as	materialNumber
# MAGIC ,KOMBINAT	as	deviceCategoryCombination
# MAGIC ,d.FUNKLAS	as	functionClassCode
# MAGIC ,fk.FUNKTXT	as	functionClass
# MAGIC ,d.BAUKLAS	as	constructionClassCode
# MAGIC ,bk.BAUKLTXT	as	constructionClass
# MAGIC ,BAUFORM	as	deviceCategoryDescription
# MAGIC ,BAUTXT	as	deviceCategoryName
# MAGIC ,PTBNUM	as	ptiNumber
# MAGIC ,DVGWNUM	as	ggwaNumber
# MAGIC ,BGLKZ	as	certificationRequirementType
# MAGIC ,d.ZWGRUPPE	as	registerGroupCode
# MAGIC ,b.EZWG_INFO	as	registerGroup
# MAGIC ,UEBERVER	as	transformationRatio
# MAGIC ,AENAM	as	changedBy
# MAGIC ,AEDAT	as	lastChangedDate
# MAGIC ,SPARTE	as	division
# MAGIC ,NENNBEL	as	nominalLoad
# MAGIC ,STELLPLATZ	as	containerSpaceCount
# MAGIC ,HOEHEBEH	as	containerCategoryHeight
# MAGIC ,BREITEBEH	as	containerCategoryWidth
# MAGIC ,TIEFEBEH	as	containerCategoryDepth
# MAGIC from raw.sapisu_0uc_devcat_attr d
# MAGIC left join raw.sapisu_0UC_FUNKLAS_TEXT fk 
# MAGIC on fk.FUNKLAS = d.FUNKLAS
# MAGIC left join raw.sapisu_0UC_BAUKLAS_TEXT bk 
# MAGIC on bk.BAUKLAS = d.BAUKLAS and bk.SPRAS ='E'
# MAGIC left join raw.sapisu_0UC_REGGRP_TEXT b
# MAGIC on d.ZWGRUPPE = b.ZWGRUPPE
# MAGIC )a
