# Databricks notebook source
# DBTITLE 0,Table
table1 = 't_sapisu_ERCH'
table2 = 't_sapisu_DBERCHZ1'
table3 = 't_sapisu_DBERCHZ2'

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

lakedf1.createOrReplaceTempView("ERCH")
lakedf2.createOrReplaceTempView("DBERCHZ1")
lakedf3.createOrReplaceTempView("DBERCHZ2")

# COMMAND ----------

lakedftarget = spark.sql("select * from curated.dimBillingDocument")
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
# MAGIC select
# MAGIC 'SAPISU' as sourceSystemCode
# MAGIC ,billingDocumentNumber
# MAGIC ,a.startBillingPeriod as billingPeriodStartDate
# MAGIC ,a.endBillingPeriod as billingPeriodEndDate
# MAGIC ,a.billingDocumentCreateDate as billCreatedDate
# MAGIC ,case when a.DocumentNotReleasedIndicator = 'X' then 'Y' else 'N' end as isOutsortedFlag
# MAGIC ,case when reversalDate is null then 'N' else 'Y' end as isReversedFlag
# MAGIC ,reversalDate
# MAGIC 
# MAGIC FROM ERCH a
# MAGIC join DBERCHZ1 bl1 
# MAGIC on bl1.billingDocumentNumber = b.billingDocumentNumber and bl1.lineItemTypeCode in ('ZDQUAN', 'ZRQUAN')
# MAGIC join DBERCHZ2 bl2 
# MAGIC on bl1.billingDocumentNumber = bl2.billingDocumentNumber and bl1.billingDocumentLineItemId = bl2.billingDocumentLineItemId and bl2.suppressedMeterReadingDocumentID <> ''

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

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from curated.dimmeter order by meterid asc

# COMMAND ----------

# DBTITLE 1,[Verification] Count Check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from curated.dimmeter
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (
# MAGIC 
# MAGIC select * from (
# MAGIC select 
# MAGIC 'Access' as sourceSystemCode
# MAGIC ,meterMakerNumber as meterId
# MAGIC ,meterSize
# MAGIC ,waterMeterType
# MAGIC from Access 
# MAGIC 
# MAGIC union all
# MAGIC 
# MAGIC select 
# MAGIC 'SAP' as sourceSystemCode
# MAGIC ,a.equipmentNumber as meterId
# MAGIC ,b.deviceCategoryDescription as meterSize
# MAGIC ,b.functionClass as waterType
# MAGIC from cleansed.t_sapisu_0UC_DEVICE_ATTR a
# MAGIC left join cleansed.t_sapisu_0UC_DEVCAT_ATTR b
# MAGIC on a.materialNumber = b.materialNumber
# MAGIC 
# MAGIC 
# MAGIC )
# MAGIC 
# MAGIC )

# COMMAND ----------

# DBTITLE 1,[Verify] Source to Target Comparison
# MAGIC %sql
# MAGIC select * from (
# MAGIC select 
# MAGIC 'Access' as sourceSystemCode
# MAGIC ,meterMakerNumber as meterId
# MAGIC ,meterSize
# MAGIC ,waterMeterType
# MAGIC from Access 
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
# MAGIC )
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
# MAGIC select sourceSystemCode,
# MAGIC meterId,
# MAGIC meterSize,
# MAGIC waterMeterType
# MAGIC from curated.dimmeter
# MAGIC except
# MAGIC select * from (
# MAGIC select 
# MAGIC 'Access' as sourceSystemCode
# MAGIC ,meterMakerNumber as meterId
# MAGIC ,meterSize
# MAGIC ,waterMeterType
# MAGIC from Access 
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
# MAGIC )

# COMMAND ----------


union all
'ACCESS' as sourceSystemCode
,'-1' as meterId
,'Unknown' as meterSize
,'Unknown' as waterMeterType

union all
'SAPISU' as sourceSystemCode
,'-1' as meterId
,'Unknown' as meterSize
,'Unknown' as waterMeterType

)

