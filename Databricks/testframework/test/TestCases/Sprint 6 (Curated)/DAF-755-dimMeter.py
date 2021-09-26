# Databricks notebook source
# DBTITLE 0,Table
table1 = 't_access_z309_tpropmeter'
table2 = 't_sapisu_0UC_DEVICE_ATTR'
table3 = 't_sapisu_0UC_DEVCAT_ATTR'

# COMMAND ----------

lakedf1 = spark.sql(f"select * from cleansed.{table1}")
display(lakedf1)

# COMMAND ----------

lakedf2 = spark.sql(f"select * from cleansed.{table2}")
display(lakedf2)

# COMMAND ----------

lakedf3 = spark.sql(f"select * from cleansed.{table3}")
display(lakedf3)

# COMMAND ----------

lakedf1.createOrReplaceTempView("Access")
lakedf2.createOrReplaceTempView("SAP")

# COMMAND ----------

lakedftarget = spark.sql("select * from curated.dimmeter")
display(lakedftarget)

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
lakedftarget.printSchema()

# COMMAND ----------

# DBTITLE 1,[Source] Schema Check
lakedf1.printSchema()

# COMMAND ----------

# DBTITLE 1,[Source] Applying Transformation
# MAGIC %sql
# MAGIC select * from (
# MAGIC select sourceSystemCode,
# MAGIC meterId,
# MAGIC meterSize,
# MAGIC waterMeterType
# MAGIC from (
# MAGIC 
# MAGIC select
# MAGIC sourceSystemCode,
# MAGIC meterId,
# MAGIC meterSize,
# MAGIC waterMeterType,
# MAGIC row_number () over (partition by meterMakerNumber order by meterFittedDate desc) as rn
# MAGIC from (
# MAGIC 
# MAGIC select 
# MAGIC 'Access' as sourceSystemCode
# MAGIC ,meterMakerNumber as meterId
# MAGIC ,meterSize
# MAGIC ,waterMeterType
# MAGIC ,meterMakerNumber
# MAGIC ,meterFittedDate
# MAGIC from Access 
# MAGIC where meterfitteddate <> meterremoveddate or meterRemovedDate is null
# MAGIC )a --where metermakernumber ='BDWE1717'
# MAGIC 
# MAGIC )a where rn = 1) 
# MAGIC 
# MAGIC union all
# MAGIC 
# MAGIC select 
# MAGIC 'SAPISU' as sourceSystemCode
# MAGIC ,a.equipmentNumber as meterId
# MAGIC ,b.deviceCategoryDescription as meterSize
# MAGIC ,b.functionClass as waterType
# MAGIC from cleansed.t_sapisu_0UC_DEVICE_ATTR a
# MAGIC left join cleansed.t_sapisu_0UC_DEVCAT_ATTR b
# MAGIC on a.materialNumber = b.materialNumber
# MAGIC 
# MAGIC union all
# MAGIC 
# MAGIC select * from(
# MAGIC select 
# MAGIC 'ACCESS' as sourceSystemCode
# MAGIC ,'-1' as meterId
# MAGIC ,'Unknown' as meterSize
# MAGIC ,'Unknown' as waterMeterType
# MAGIC from Access limit 1)a
# MAGIC 
# MAGIC 
# MAGIC union all
# MAGIC 
# MAGIC select * from (
# MAGIC select 
# MAGIC 'SAPISU' as sourceSystemCode
# MAGIC ,'-1' as meterId
# MAGIC ,'Unknown' as meterSize
# MAGIC ,'Unknown' as waterMeterType
# MAGIC from Access limit 1)b

# COMMAND ----------

# DBTITLE 1,[Verification] Auto Generate field check
# MAGIC %sql
# MAGIC select dimMeterSK from curated.dimmeter where dimmetersk in (null,'',' ')

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Check
# MAGIC %sql
# MAGIC SELECT dimmetersk, COUNT (*) as count
# MAGIC FROM curated.dimmeter
# MAGIC GROUP BY dimmetersk
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT metermakernumber, COUNT (*) as count
# MAGIC FROM cleansed.t_access_z309_tpropmeter
# MAGIC GROUP BY metermakernumber
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select dimmetersk - 1, dimmetersk as missingNumber from curated.dimmeter a where  not exists (select dimmetersk from curated.dimmeter b where b.dimmetersk = a.dimmetersk -1)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from curated.dimmeter where dimmetersk = -1 --meterid = '' --order by meterid asc

# COMMAND ----------

# DBTITLE 1,[Verification] Count Check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from curated.dimmeter
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (
# MAGIC 
# MAGIC 
# MAGIC select * from (
# MAGIC select sourceSystemCode,
# MAGIC meterId,
# MAGIC meterSize,
# MAGIC waterMeterType
# MAGIC from (
# MAGIC 
# MAGIC select
# MAGIC sourceSystemCode,
# MAGIC meterId,
# MAGIC meterSize,
# MAGIC waterMeterType,
# MAGIC row_number () over (partition by meterMakerNumber order by meterFittedDate desc) as rn
# MAGIC from (
# MAGIC 
# MAGIC select 
# MAGIC 'Access' as sourceSystemCode
# MAGIC ,meterMakerNumber as meterId
# MAGIC ,meterSize
# MAGIC ,waterMeterType
# MAGIC ,meterMakerNumber
# MAGIC ,meterFittedDate
# MAGIC from Access 
# MAGIC where meterfitteddate <> meterremoveddate or meterRemovedDate is null
# MAGIC )a --where metermakernumber ='BDWE1717'
# MAGIC 
# MAGIC )a where rn = 1) 
# MAGIC 
# MAGIC union all
# MAGIC 
# MAGIC select 
# MAGIC 'SAPISU' as sourceSystemCode
# MAGIC ,a.equipmentNumber as meterId
# MAGIC ,b.deviceCategoryDescription as meterSize
# MAGIC ,b.functionClass as waterType
# MAGIC from cleansed.t_sapisu_0UC_DEVICE_ATTR a
# MAGIC left join cleansed.t_sapisu_0UC_DEVCAT_ATTR b
# MAGIC on a.materialNumber = b.materialNumber
# MAGIC 
# MAGIC union all
# MAGIC 
# MAGIC select * from(
# MAGIC select 
# MAGIC 'ACCESS' as sourceSystemCode
# MAGIC ,'-1' as meterId
# MAGIC ,'Unknown' as meterSize
# MAGIC ,'Unknown' as waterMeterType
# MAGIC from Access limit 1)a
# MAGIC 
# MAGIC 
# MAGIC union all
# MAGIC 
# MAGIC select * from (
# MAGIC select 
# MAGIC 'SAPISU' as sourceSystemCode
# MAGIC ,'-1' as meterId
# MAGIC ,'Unknown' as meterSize
# MAGIC ,'Unknown' as waterMeterType
# MAGIC from Access limit 1)b
# MAGIC 
# MAGIC 
# MAGIC )

# COMMAND ----------

# DBTITLE 1,[Verify] Source to Target Comparison
# MAGIC %sql
# MAGIC select * from (
# MAGIC select * from (
# MAGIC select sourceSystemCode,
# MAGIC meterId,
# MAGIC meterSize,
# MAGIC waterMeterType
# MAGIC from (
# MAGIC 
# MAGIC select
# MAGIC sourceSystemCode,
# MAGIC meterId,
# MAGIC meterSize,
# MAGIC waterMeterType,
# MAGIC row_number () over (partition by meterMakerNumber order by meterFittedDate desc) as rn
# MAGIC from (
# MAGIC 
# MAGIC select 
# MAGIC 'Access' as sourceSystemCode
# MAGIC ,case when meterMakerNumber is null then '' else meterMakerNumber end as meterId
# MAGIC ,meterSize
# MAGIC ,waterMeterType
# MAGIC ,meterMakerNumber
# MAGIC ,meterFittedDate
# MAGIC from Access 
# MAGIC where meterfitteddate <> meterremoveddate or meterRemovedDate is null
# MAGIC )a --where metermakernumber ='BDWE1717'
# MAGIC 
# MAGIC )a where rn = 1) 
# MAGIC 
# MAGIC union all
# MAGIC 
# MAGIC select 
# MAGIC 'SAPISU' as sourceSystemCode
# MAGIC ,a.equipmentNumber as meterId
# MAGIC ,b.deviceCategoryDescription as meterSize
# MAGIC ,b.functionClass as waterType
# MAGIC from cleansed.t_sapisu_0UC_DEVICE_ATTR a
# MAGIC left join cleansed.t_sapisu_0UC_DEVCAT_ATTR b
# MAGIC on a.materialNumber = b.materialNumber
# MAGIC 
# MAGIC union all
# MAGIC 
# MAGIC select * from(
# MAGIC select 
# MAGIC 'Access' as sourceSystemCode
# MAGIC ,'-1' as meterId
# MAGIC ,'Unknown' as meterSize
# MAGIC ,'Unknown' as waterMeterType
# MAGIC from Access limit 1)a
# MAGIC 
# MAGIC 
# MAGIC union all
# MAGIC 
# MAGIC select * from (
# MAGIC select 
# MAGIC 'SAPISU' as sourceSystemCode
# MAGIC ,'-1' as meterId
# MAGIC ,'Unknown' as meterSize
# MAGIC ,'Unknown' as waterMeterType
# MAGIC from Access limit 1)b
# MAGIC )zz
# MAGIC 
# MAGIC 
# MAGIC except
# MAGIC 
# MAGIC select sourceSystemCode,
# MAGIC meterId,
# MAGIC meterSize,
# MAGIC waterMeterType
# MAGIC from curated.dimmeter

# COMMAND ----------

# DBTITLE 1,[Verify] Target to Source Comparison
# MAGIC %sql
# MAGIC 
# MAGIC select sourceSystemCode,
# MAGIC meterId,
# MAGIC meterSize,
# MAGIC waterMeterType
# MAGIC from curated.dimmeter
# MAGIC 
# MAGIC except 
# MAGIC 
# MAGIC select * from (
# MAGIC select * from (
# MAGIC select sourceSystemCode,
# MAGIC meterId,
# MAGIC meterSize,
# MAGIC waterMeterType
# MAGIC from (
# MAGIC 
# MAGIC select
# MAGIC sourceSystemCode,
# MAGIC meterId,
# MAGIC meterSize,
# MAGIC waterMeterType,
# MAGIC row_number () over (partition by meterMakerNumber order by meterFittedDate desc) as rn
# MAGIC from (
# MAGIC 
# MAGIC select 
# MAGIC 'Access' as sourceSystemCode
# MAGIC ,case when meterMakerNumber is null then '' else meterMakerNumber end as meterId
# MAGIC ,meterSize
# MAGIC ,waterMeterType
# MAGIC ,meterMakerNumber
# MAGIC ,meterFittedDate
# MAGIC from Access 
# MAGIC where meterfitteddate <> meterremoveddate or meterRemovedDate is null
# MAGIC )a --where metermakernumber ='BDWE1717'
# MAGIC 
# MAGIC )a where rn = 1) 
# MAGIC 
# MAGIC union all
# MAGIC 
# MAGIC select 
# MAGIC 'SAPISU' as sourceSystemCode
# MAGIC ,a.equipmentNumber as meterId
# MAGIC ,b.deviceCategoryDescription as meterSize
# MAGIC ,b.functionClass as waterType
# MAGIC from cleansed.t_sapisu_0UC_DEVICE_ATTR a
# MAGIC left join cleansed.t_sapisu_0UC_DEVCAT_ATTR b
# MAGIC on a.materialNumber = b.materialNumber
# MAGIC 
# MAGIC union all
# MAGIC 
# MAGIC select * from(
# MAGIC select 
# MAGIC 'Access' as sourceSystemCode
# MAGIC ,'-1' as meterId
# MAGIC ,'Unknown' as meterSize
# MAGIC ,'Unknown' as waterMeterType
# MAGIC from Access limit 1)a
# MAGIC 
# MAGIC 
# MAGIC union all
# MAGIC 
# MAGIC select * from (
# MAGIC select 
# MAGIC 'SAPISU' as sourceSystemCode
# MAGIC ,'-1' as meterId
# MAGIC ,'Unknown' as meterSize
# MAGIC ,'Unknown' as waterMeterType
# MAGIC from Access limit 1)b
# MAGIC )zz

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from(
# MAGIC select 
# MAGIC 'Access' as sourceSystemCode
# MAGIC ,'-1' as meterId
# MAGIC ,'Unknown' as meterSize
# MAGIC ,'Unknown' as waterMeterType
# MAGIC from Access limit 1)a
# MAGIC 
# MAGIC 
# MAGIC union all
# MAGIC 
# MAGIC select * from (
# MAGIC select 
# MAGIC 'SAPISU' as sourceSystemCode
# MAGIC ,'-1' as meterId
# MAGIC ,'Unknown' as meterSize
# MAGIC ,'Unknown' as waterMeterType
# MAGIC from Access limit 1)b
