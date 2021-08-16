# Databricks notebook source
# DBTITLE 1,[Config]
Targetdf = spark.sql("select * from cleansed.t_sapisu_0UC_DEVICE_ATTR")

# COMMAND ----------

# DBTITLE 1,[Schema Checks] Mapping vs Target
Targetdf.printSchema()

# COMMAND ----------

# DBTITLE 1,[Source] Raw Table with Transformation 
# MAGIC %sql
# MAGIC SELECT
# MAGIC EQUNR as equipmentNumber
# MAGIC ,MATNR as materialNumber
# MAGIC ,GERAET as deviceNumber
# MAGIC ,BESITZ as inspectionRelevanceIndicator
# MAGIC ,GROES as deviceSize
# MAGIC ,HERST as assetManufacturerName
# MAGIC ,SERGE as manufacturerSerialNumber
# MAGIC ,ZZTYPBZ as manufacturerModelNumber
# MAGIC ,ZZOBJNR as objectNumber
# MAGIC ,ZWNABR as registerNotRelevantToBilling
# MAGIC FROM raw.sapisu_0UC_DEVICE_ATTR

# COMMAND ----------

# DBTITLE 1,[Target] Cleansed Table
# MAGIC %sql
# MAGIC select * from cleansed.t_sapisu_0UC_DEVICE_ATTR 

# COMMAND ----------

# DBTITLE 1,[Reconciliation] Count Checks Between Source and Target
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'raw.sapisu_0UC_DEVICE_ATTR' as TableName from (
# MAGIC SELECT
# MAGIC EQUNR as equipmentNumber
# MAGIC ,MATNR as materialNumber
# MAGIC ,GERAET as deviceNumber
# MAGIC ,BESITZ as inspectionRelevanceIndicator
# MAGIC ,GROES as deviceSize
# MAGIC ,HERST as assetManufacturerName
# MAGIC ,SERGE as manufacturerSerialNumber
# MAGIC ,ZZTYPBZ as manufacturerModelNumber
# MAGIC ,ZZOBJNR as objectNumber
# MAGIC ,ZWNABR as registerNotRelevantToBilling
# MAGIC FROM raw.sapisu_0UC_DEVICE_ATTR
# MAGIC 
# MAGIC )a
# MAGIC 
# MAGIC union
# MAGIC 
# MAGIC select count (*) as RecordCount, 'cleansed.t_sapisu_0UC_DEVICE_ATTR' as TableName from cleansed.t_sapisu_0UC_DEVICE_ATTR 

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT equipmentNumber, COUNT (*) as count
# MAGIC FROM cleansed.t_sapisu_0UC_DEVICE_ATTR
# MAGIC GROUP BY equipmentNumber
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY equipmentNumber order by equipmentNumber) as rn
# MAGIC FROM cleansed.t_sapisu_0UC_DEVICE_ATTR
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source to Target
# MAGIC %sql
# MAGIC SELECT
# MAGIC EQUNR as equipmentNumber
# MAGIC ,MATNR as materialNumber
# MAGIC ,GERAET as deviceNumber
# MAGIC ,BESITZ as inspectionRelevanceIndicator
# MAGIC ,GROES as deviceSize
# MAGIC ,HERST as assetManufacturerName
# MAGIC ,SERGE as manufacturerSerialNumber
# MAGIC ,ZZTYPBZ as manufacturerModelNumber
# MAGIC ,ZZOBJNR as objectNumber
# MAGIC ,ZWNABR as registerNotRelevantToBilling
# MAGIC FROM raw.sapisu_0UC_DEVICE_ATTR
# MAGIC 
# MAGIC EXCEPT
# MAGIC 
# MAGIC select 
# MAGIC equipmentNumber
# MAGIC ,materialNumber
# MAGIC ,deviceNumber
# MAGIC ,inspectionRelevanceIndicator
# MAGIC ,deviceSize
# MAGIC ,assetManufacturerName
# MAGIC ,manufacturerSerialNumber
# MAGIC ,manufacturerModelNumber
# MAGIC ,objectNumber
# MAGIC ,registerNotRelevantToBilling
# MAGIC from cleansed.t_sapisu_0UC_DEVICE_ATTR 

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target to Source
# MAGIC %sql
# MAGIC select 
# MAGIC equipmentNumber
# MAGIC ,materialNumber
# MAGIC ,deviceNumber
# MAGIC ,inspectionRelevanceIndicator
# MAGIC ,deviceSize
# MAGIC ,assetManufacturerName
# MAGIC ,manufacturerSerialNumber
# MAGIC ,manufacturerModelNumber
# MAGIC ,objectNumber
# MAGIC ,registerNotRelevantToBilling
# MAGIC from cleansed.t_sapisu_0UC_DEVICE_ATTR 
# MAGIC 
# MAGIC EXCEPT
# MAGIC 
# MAGIC SELECT
# MAGIC EQUNR as equipmentNumber
# MAGIC ,MATNR as materialNumber
# MAGIC ,GERAET as deviceNumber
# MAGIC ,BESITZ as inspectionRelevanceIndicator
# MAGIC ,GROES as deviceSize
# MAGIC ,HERST as assetManufacturerName
# MAGIC ,SERGE as manufacturerSerialNumber
# MAGIC ,ZZTYPBZ as manufacturerModelNumber
# MAGIC ,ZZOBJNR as objectNumber
# MAGIC ,ZWNABR as registerNotRelevantToBilling
# MAGIC FROM raw.sapisu_0UC_DEVICE_ATTR
