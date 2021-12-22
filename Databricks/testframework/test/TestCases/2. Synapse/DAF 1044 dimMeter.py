# Databricks notebook source
# MAGIC %sql
# MAGIC select count (*) from curated.dimmeter

# COMMAND ----------

curateddf = spark.sql("select * from curated.dimmeter")
display(curateddf)

# COMMAND ----------

curateddf.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated.dimmeter

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC dimMeterSK
# MAGIC ,sourceSystemCode
# MAGIC ,meterId
# MAGIC ,meterSize
# MAGIC ,waterMeterType
# MAGIC  FROM curated.dimmeter limit 100

# COMMAND ----------

# MAGIC %sql
# MAGIC select count (*) from curated.factdailyapportionedconsumption

# COMMAND ----------

# MAGIC %sql
# MAGIC select count (*) from curated.factbilledwaterconsumption

# COMMAND ----------

# MAGIC %sql
# MAGIC select count (*) from curated.dimdate

# COMMAND ----------

curateddf = spark.sql("select * from curated.dimdate")
display(curateddf)

# COMMAND ----------

curateddf.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated.dimdate

# COMMAND ----------

storage_account_name = "sablobdaftest01"
storage_account_access_key = dbutils.secrets.get(scope="TestScope",key="test-sablob-key")
containerName = "test"
file_location = "wasbs://test@sablobdaftest01.blob.core.windows.net/SQL script 7.csv"
file_type = "csv"
print(storage_account_name)

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)

# COMMAND ----------


df = spark.read.format(file_type).option("header", "true").load(file_location)

# COMMAND ----------

df.printSchema()


# COMMAND ----------

df.createOrReplaceTempView("Target")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Target

# COMMAND ----------

# DBTITLE 1,Curated vs Synapse
# MAGIC %sql
# MAGIC select
# MAGIC dimDateSK
# MAGIC --,calendarDate
# MAGIC ,dayOfWeek
# MAGIC ,dayName
# MAGIC ,dayOfMonth
# MAGIC ,dayOfYear
# MAGIC ,monthOfYear
# MAGIC ,monthName
# MAGIC ,quarterOfYear 
# MAGIC ,halfOfYear
# MAGIC ,monthStartDate
# MAGIC ,monthEndDate
# MAGIC ,yearStartDate
# MAGIC ,yearEndDate
# MAGIC ,financialYear
# MAGIC ,financialYearStartDate
# MAGIC ,financialYearEndDate
# MAGIC ,monthOfFinancialYear
# MAGIC ,quarterOfFinancialYear
# MAGIC ,halfOfFinancialYear
# MAGIC  -- ,_DLCuratedZoneTimeStamp 
# MAGIC --  ,_RecordStart
# MAGIC --  ,_RecordEnd
# MAGIC  -- ,_RecordDeleted
# MAGIC  -- ,_RecordCurrent
# MAGIC  from  curated.dimdate where dimDateSK = '402'
# MAGIC  except
# MAGIC  select
# MAGIC dimDateSK
# MAGIC --,calendarDate
# MAGIC ,dayOfWeek
# MAGIC ,dayName
# MAGIC ,dayOfMonth
# MAGIC ,dayOfYear
# MAGIC ,monthOfYear
# MAGIC ,monthName
# MAGIC ,quarterOfYear 
# MAGIC ,halfOfYear
# MAGIC ,monthStartDate
# MAGIC ,monthEndDate
# MAGIC ,yearStartDate
# MAGIC ,yearEndDate
# MAGIC ,financialYear
# MAGIC ,financialYearStartDate
# MAGIC ,financialYearEndDate
# MAGIC ,monthOfFinancialYear
# MAGIC ,quarterOfFinancialYear
# MAGIC ,halfOfFinancialYear
# MAGIC  -- ,_DLCuratedZoneTimeStamp 
# MAGIC --  ,_RecordStart
# MAGIC --  ,_RecordEnd
# MAGIC  -- ,_RecordDeleted
# MAGIC  -- ,_RecordCurrent
# MAGIC  from  Target where dimDateSK = '402'
# MAGIC  

# COMMAND ----------

# DBTITLE 1,Synapse vs Curated

 

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC dimDateSK
# MAGIC ,calendarDate
# MAGIC ,dayOfWeek
# MAGIC ,dayName
# MAGIC ,dayOfMonth
# MAGIC ,dayOfYear
# MAGIC ,monthOfYear
# MAGIC ,monthName
# MAGIC ,quarterOfYear 
# MAGIC ,halfOfYear
# MAGIC ,monthStartDate
# MAGIC ,monthEndDate
# MAGIC ,yearStartDate
# MAGIC ,yearEndDate
# MAGIC ,financialYear
# MAGIC ,financialYearStartDate
# MAGIC ,financialYearEndDate
# MAGIC ,monthOfFinancialYear
# MAGIC ,quarterOfFinancialYear
# MAGIC ,halfOfFinancialYear
# MAGIC  -- ,_DLCuratedZoneTimeStamp 
# MAGIC --  ,_RecordStart
# MAGIC --  ,_RecordEnd
# MAGIC  -- ,_RecordDeleted
# MAGIC  -- ,_RecordCurrent
# MAGIC  from  curated.dimdate where dimDateSK = '402'

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC dimDateSK
# MAGIC ,calendarDate
# MAGIC ,dayOfWeek
# MAGIC ,dayName
# MAGIC ,dayOfMonth
# MAGIC ,dayOfYear
# MAGIC ,monthOfYear
# MAGIC ,monthName
# MAGIC ,quarterOfYear 
# MAGIC ,halfOfYear
# MAGIC ,monthStartDate
# MAGIC ,monthEndDate
# MAGIC ,yearStartDate
# MAGIC ,yearEndDate
# MAGIC ,financialYear
# MAGIC ,financialYearStartDate
# MAGIC ,financialYearEndDate
# MAGIC ,monthOfFinancialYear
# MAGIC ,quarterOfFinancialYear
# MAGIC ,halfOfFinancialYear
# MAGIC  -- ,_DLCuratedZoneTimeStamp 
# MAGIC --  ,_RecordStart
# MAGIC --  ,_RecordEnd
# MAGIC  -- ,_RecordDeleted
# MAGIC  -- ,_RecordCurrent
# MAGIC  from  Target where dimDateSK = '402'

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC cast(to_unix_timestamp(monthStartDate, 'yyyy-MM-dd HH:mm:ss') as timestamp) as monthStartDate
# MAGIC from curated.dimdate where dimDateSK = '402'
# MAGIC except
# MAGIC select
# MAGIC monthStartDate from Target where dimDateSK = '402'

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC CONCAT(LEFT (monthStartDate,10),'T',SUBSTRING(monthStartDate,12,8),'00:00:00.0000000') as monthStartDate
# MAGIC from curated.dimdate where dimDateSK = '402'
# MAGIC except
# MAGIC select
# MAGIC monthStartDate from Target where dimDateSK = '402'

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC monthStartDate from Target where dimDateSK = '402'
