# Databricks notebook source
# MAGIC %sql
# MAGIC select count (*) from curated.factDailyApportionedConsumption

# COMMAND ----------

curateddf = spark.sql("select * from curated.factDailyApportionedConsumption")
display(curateddf)

# COMMAND ----------

curateddf.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated.factDailyApportionedConsumption order by factdailyapportionedconsumptionSK limit 10000

# COMMAND ----------

curatedSourcedf = spark.sql("select * from curated.factDailyApportionedConsumption order by factdailyapportionedconsumptionSK limit 10000")
display(curatedSourcedf)

# COMMAND ----------

curatedSourcedf.createOrReplaceTempView("Source")

# COMMAND ----------

storage_account_name = "sablobdaftest01"
storage_account_access_key = dbutils.secrets.get(scope="TestScope",key="test-sablob-key")
containerName = "test"
file_location = "wasbs://test@sablobdaftest01.blob.core.windows.net/FactApportioned.csv"
file_type = "csv"
print(storage_account_name)

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)

# COMMAND ----------

df = spark.read.format(file_type).option("header", "true").load(file_location)
display(df)

# COMMAND ----------

df.count()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.createOrReplaceTempView("Target")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from Target 

# COMMAND ----------

# DBTITLE 1,Curated
# MAGIC %sql
# MAGIC select
# MAGIC factDailyApportionedConsumptionSK
# MAGIC ,sourceSystemCode
# MAGIC ,consumptionDateSK
# MAGIC ,dimBillingDocumentSK
# MAGIC ,dimPropertySK
# MAGIC ,dimMeterSK
# MAGIC ,dimLocationSK
# MAGIC ,dimWaterNetworkSK
# MAGIC ,dailyApportionedConsumption
# MAGIC ,_DLCuratedZoneTimeStamp
# MAGIC ,_RecordStart
# MAGIC ,_RecordEnd
# MAGIC ,_RecordDeleted
# MAGIC ,_RecordCurrent
# MAGIC from Source

# COMMAND ----------

# DBTITLE 1,Synapse
# MAGIC %sql
# MAGIC select
# MAGIC factDailyApportionedConsumptionSK
# MAGIC ,sourceSystemCode
# MAGIC ,consumptionDateSK
# MAGIC ,dimBillingDocumentSK
# MAGIC ,dimPropertySK
# MAGIC ,dimMeterSK
# MAGIC ,dimLocationSK
# MAGIC ,dimWaterNetworkSK
# MAGIC ,dailyApportionedConsumption
# MAGIC ,_DLCuratedZoneTimeStamp
# MAGIC ,_RecordStart
# MAGIC ,_RecordEnd
# MAGIC ,_RecordDeleted
# MAGIC ,_RecordCurrent
# MAGIC from Target

# COMMAND ----------

# DBTITLE 1,Curated vs Synapse
# MAGIC %sql
# MAGIC select
# MAGIC factDailyApportionedConsumptionSK
# MAGIC ,sourceSystemCode
# MAGIC ,consumptionDateSK
# MAGIC ,dimBillingDocumentSK
# MAGIC ,dimPropertySK
# MAGIC ,dimMeterSK
# MAGIC ,dimLocationSK
# MAGIC ,dimWaterNetworkSK
# MAGIC ,dailyApportionedConsumption
# MAGIC ,CONCAT(LEFT (_DLCuratedZoneTimeStamp,10),'T',SUBSTRING(_DLCuratedZoneTimeStamp,12,8),'.0000000') as _DLCuratedZoneTimeStamp
# MAGIC ,CONCAT(LEFT (_RecordStart,10),'T',SUBSTRING(_RecordStart,12,8),'.0000000') as _RecordStart
# MAGIC ,CONCAT(LEFT (_RecordEnd,10),'T',SUBSTRING(_RecordEnd,12,8),'.0000000') as _RecordEnd
# MAGIC ,_RecordDeleted
# MAGIC ,_RecordCurrent
# MAGIC from Source
# MAGIC except
# MAGIC select
# MAGIC factDailyApportionedConsumptionSK
# MAGIC ,sourceSystemCode
# MAGIC ,consumptionDateSK
# MAGIC ,dimBillingDocumentSK
# MAGIC ,dimPropertySK
# MAGIC ,dimMeterSK
# MAGIC ,dimLocationSK
# MAGIC ,dimWaterNetworkSK
# MAGIC ,dailyApportionedConsumption
# MAGIC ,_DLCuratedZoneTimeStamp
# MAGIC ,_RecordStart
# MAGIC ,_RecordEnd
# MAGIC ,_RecordDeleted
# MAGIC ,_RecordCurrent
# MAGIC from Target

# COMMAND ----------

# DBTITLE 1,Synapse vs Curated
# MAGIC %sql
# MAGIC select
# MAGIC factDailyApportionedConsumptionSK
# MAGIC ,sourceSystemCode
# MAGIC ,consumptionDateSK
# MAGIC ,dimBillingDocumentSK
# MAGIC ,dimPropertySK
# MAGIC ,dimMeterSK
# MAGIC ,dimLocationSK
# MAGIC ,dimWaterNetworkSK
# MAGIC ,dailyApportionedConsumption
# MAGIC ,_DLCuratedZoneTimeStamp
# MAGIC ,_RecordStart
# MAGIC ,_RecordEnd
# MAGIC ,_RecordDeleted
# MAGIC ,_RecordCurrent
# MAGIC from Target
# MAGIC except
# MAGIC select
# MAGIC factDailyApportionedConsumptionSK
# MAGIC ,sourceSystemCode
# MAGIC ,consumptionDateSK
# MAGIC ,dimBillingDocumentSK
# MAGIC ,dimPropertySK
# MAGIC ,dimMeterSK
# MAGIC ,dimLocationSK
# MAGIC ,dimWaterNetworkSK
# MAGIC ,dailyApportionedConsumption
# MAGIC ,CONCAT(LEFT (_DLCuratedZoneTimeStamp,10),'T',SUBSTRING(_DLCuratedZoneTimeStamp,12,8),'.0000000') as _DLCuratedZoneTimeStamp
# MAGIC ,CONCAT(LEFT (_RecordStart,10),'T',SUBSTRING(_RecordStart,12,8),'.0000000') as _RecordStart
# MAGIC ,CONCAT(LEFT (_RecordEnd,10),'T',SUBSTRING(_RecordEnd,12,8),'.0000000') as _RecordEnd
# MAGIC ,_RecordDeleted
# MAGIC ,_RecordCurrent
# MAGIC from Source

# COMMAND ----------

# DBTITLE 1,Additional Tests
# MAGIC %sql
# MAGIC select count (*), 'factDailyApportionedConsumptionSK' from 
# MAGIC (select distinct factDailyApportionedConsumptionSK FROM curated.Factdailyapportionedconsumption)a
# MAGIC union all
# MAGIC select count (*), 'sourceSystemCode' from 
# MAGIC (select distinct sourceSystemCode FROM curated.Factdailyapportionedconsumption)a
# MAGIC union all
# MAGIC select count (*), 'consumptionDateSK' from 
# MAGIC (select distinct consumptionDateSK FROM curated.Factdailyapportionedconsumption)a
# MAGIC union all
# MAGIC select count (*), 'dimBillingDocumentSK' from 
# MAGIC (select distinct dimBillingDocumentSK FROM curated.Factdailyapportionedconsumption)a
# MAGIC union all
# MAGIC select count (*), 'dimPropertySK' from 
# MAGIC (select distinct dimPropertySK FROM curated.Factdailyapportionedconsumption)a
# MAGIC union all
# MAGIC select count (*), 'dimMeterSK' from 
# MAGIC (select distinct dimMeterSK FROM curated.Factdailyapportionedconsumption)a
# MAGIC union all
# MAGIC select count (*), 'dimLocationSK' from 
# MAGIC (select distinct dimLocationSK FROM curated.Factdailyapportionedconsumption)a

# COMMAND ----------

# DBTITLE 1,Aggregated Test - SUM
# MAGIC %sql
# MAGIC select sum(dailyApportionedConsumption)
# MAGIC from curated.Factdailyapportionedconsumption

# COMMAND ----------

# DBTITLE 1,Null /Blank checks for unique keys
# MAGIC %sql
# MAGIC select consumptionDateSK from curated.Factdailyapportionedconsumption where consumptionDateSK in (null,'',' ')

# COMMAND ----------

# DBTITLE 1,Null /Blank checks for unique keys
# MAGIC %sql
# MAGIC select dimBillingDocumentSK from curated.Factdailyapportionedconsumption where dimBillingDocumentSK in (null,'',' ')

# COMMAND ----------

# DBTITLE 1,Null /Blank checks for unique keys
# MAGIC %sql
# MAGIC select dimPropertySK from curated.Factdailyapportionedconsumption where dimPropertySK in (null,'',' ')

# COMMAND ----------

# DBTITLE 1,Null /Blank checks for unique keys
# MAGIC %sql
# MAGIC select dimMeterSK from curated.Factdailyapportionedconsumption where dimMeterSK in (null,'',' ')

# COMMAND ----------

# DBTITLE 1,Duplicate checks
# MAGIC %sql
# MAGIC SELECT  
# MAGIC consumptionDateSK
# MAGIC ,dimBillingDocumentSK
# MAGIC ,dimPropertySK
# MAGIC ,dimMeterSK
# MAGIC , COUNT (*) as count
# MAGIC  FROM curated.Factdailyapportionedconsumption
# MAGIC  GROUP BY 
# MAGIC  consumptionDateSK
# MAGIC ,dimBillingDocumentSK
# MAGIC ,dimPropertySK
# MAGIC ,dimMeterSK
# MAGIC HAVING COUNT(*) >1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  
# MAGIC consumptionDateSK
# MAGIC , COUNT (*) as count
# MAGIC  FROM curated.Factdailyapportionedconsumption where consumptionDateSK='44371'
# MAGIC  GROUP BY 
# MAGIC  consumptionDateSK 
# MAGIC HAVING COUNT(*) >1 

# COMMAND ----------

# DBTITLE 1,Duplicate checks for unique key
# MAGIC %sql
# MAGIC SELECT  
# MAGIC consumptionDateSK
# MAGIC , COUNT (*) as count
# MAGIC  FROM curated.Factdailyapportionedconsumption
# MAGIC  GROUP BY 
# MAGIC  consumptionDateSK
# MAGIC HAVING COUNT(*) >1 order by consumptionDateSK

# COMMAND ----------

# DBTITLE 1,Duplicate checks for unique key
# MAGIC %sql
# MAGIC SELECT  
# MAGIC dimBillingDocumentSK
# MAGIC , COUNT (*) as count
# MAGIC  FROM curated.Factdailyapportionedconsumption
# MAGIC  GROUP BY 
# MAGIC  dimBillingDocumentSK
# MAGIC HAVING COUNT(*) >1 order by dimBillingDocumentSK

# COMMAND ----------

# DBTITLE 1,Duplicate checks for unique key
# MAGIC %sql
# MAGIC SELECT  
# MAGIC dimPropertySK
# MAGIC , COUNT (*) as count
# MAGIC  FROM curated.Factdailyapportionedconsumption
# MAGIC  GROUP BY 
# MAGIC  dimPropertySK
# MAGIC HAVING COUNT(*) >1 order by dimPropertySK

# COMMAND ----------

# DBTITLE 1,Duplicate checks for unique key
# MAGIC %sql
# MAGIC SELECT  
# MAGIC dimMeterSK
# MAGIC , COUNT (*) as count
# MAGIC  FROM curated.Factdailyapportionedconsumption
# MAGIC  GROUP BY 
# MAGIC  dimMeterSK
# MAGIC HAVING COUNT(*) >1 order by dimMeterSK
