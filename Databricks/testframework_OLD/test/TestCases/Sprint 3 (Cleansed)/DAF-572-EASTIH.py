# Databricks notebook source
# DBTITLE 1,[Config]
Targetdf = spark.sql("select * from cleansed.t_sapisu_EASTIH")

# COMMAND ----------

# DBTITLE 1,[Schema Checks] Mapping vs Target
Targetdf.printSchema()

# COMMAND ----------

# DBTITLE 1,[Source] Raw Table with Transformation 
# MAGIC %sql 
# MAGIC SELECT
# MAGIC INDEXNR	as	consecutiveNumberOfRegisterRelationship
# MAGIC ,PRUEFGR as	validationGroupForDependentValidations
# MAGIC ,ZWZUART as	registerRelationshipType
# MAGIC ,ERDAT as createdDate
# MAGIC ,ERNAM as createdBy
# MAGIC ,AEDAT as lastChangedDate
# MAGIC ,AENAM as changedBy
# MAGIC FROM raw.sapisu_eastih

# COMMAND ----------

# DBTITLE 1,[Target] Cleansed Table
# MAGIC %sql
# MAGIC select * from cleansed.t_sapisu_eastih

# COMMAND ----------

# DBTITLE 1,[Reconciliation] Count Checks Between Source and Target
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'raw.sapisu_eastih' as TableName from (
# MAGIC SELECT
# MAGIC INDEXNR	as	consecutiveNumberOfRegisterRelationship
# MAGIC ,PRUEFGR as	validationGroupForDependentValidations
# MAGIC ,ZWZUART as	registerRelationshipType
# MAGIC ,ERDAT as createdDate
# MAGIC ,ERNAM as createdBy
# MAGIC ,AEDAT as lastChangedDate
# MAGIC ,AENAM as changedBy
# MAGIC FROM raw.sapisu_eastih
# MAGIC )a
# MAGIC union
# MAGIC select count (*) as RecordCount, 'cleansed.t_sapisu_eastih' as TableName from cleansed.t_sapisu_eastih

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT consecutiveNumberOfRegisterRelationship, COUNT (*) as count
# MAGIC FROM cleansed.t_sapisu_eastih
# MAGIC GROUP BY consecutiveNumberOfRegisterRelationship
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY consecutiveNumberOfRegisterRelationship order by consecutiveNumberOfRegisterRelationship) as rn
# MAGIC FROM  cleansed.t_sapisu_eastih
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source to Target
# MAGIC %sql
# MAGIC select * from (
# MAGIC SELECT
# MAGIC INDEXNR	as	consecutiveNumberOfRegisterRelationship
# MAGIC ,PRUEFGR as	validationGroupForDependentValidations
# MAGIC ,ZWZUART as	registerRelationshipType
# MAGIC ,ERDAT as createdDate
# MAGIC ,ERNAM as createdBy
# MAGIC ,AEDAT as lastChangedDate
# MAGIC ,AENAM as changedBy
# MAGIC FROM raw.sapisu_eastih)a
# MAGIC except
# MAGIC select 
# MAGIC consecutiveNumberOfRegisterRelationship
# MAGIC ,validationGroupForDependentValidations
# MAGIC ,registerRelationshipType
# MAGIC ,createdDate
# MAGIC ,createdBy
# MAGIC ,lastChangedDate
# MAGIC ,changedBy
# MAGIC FROM
# MAGIC cleansed.t_sapisu_eastih

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target to Source
# MAGIC %sql
# MAGIC select 
# MAGIC consecutiveNumberOfRegisterRelationship
# MAGIC ,validationGroupForDependentValidations
# MAGIC ,registerRelationshipType
# MAGIC ,createdDate
# MAGIC ,createdBy
# MAGIC ,lastChangedDate
# MAGIC ,changedBy
# MAGIC FROM
# MAGIC cleansed.t_sapisu_eastih
# MAGIC except
# MAGIC select * from (
# MAGIC SELECT
# MAGIC INDEXNR	as	consecutiveNumberOfRegisterRelationship
# MAGIC ,PRUEFGR as	validationGroupForDependentValidations
# MAGIC ,ZWZUART as	registerRelationshipType
# MAGIC ,ERDAT as createdDate
# MAGIC ,ERNAM as createdBy
# MAGIC ,AEDAT as lastChangedDate
# MAGIC ,AENAM as changedBy
# MAGIC FROM raw.sapisu_eastih)a
