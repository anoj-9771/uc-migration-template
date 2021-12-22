# Databricks notebook source
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
file_location = "wasbs://test@sablobdaftest01.blob.core.windows.net/SQL script 5.json"
file_type = "json"
print(storage_account_name)

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)

# COMMAND ----------

df = spark.read.format(file_type).option("inferSchema", "true").load(file_location)
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



# COMMAND ----------

# DBTITLE 1,Curated vs Synapse
# MAGIC %sql
# MAGIC select
# MAGIC dimDateSK
# MAGIC ,CONCAT(LEFT (calendarDate,10),'T',SUBSTRING(calendarDate,12,8),'00:00:00.0000000') as calendarDate
# MAGIC ,dayOfWeek
# MAGIC ,dayName
# MAGIC ,dayOfMonth
# MAGIC ,dayOfYear
# MAGIC ,monthOfYear
# MAGIC ,monthName
# MAGIC ,quarterOfYear 
# MAGIC ,halfOfYear
# MAGIC ,CONCAT(LEFT (monthStartDate,10),'T',SUBSTRING(monthStartDate,12,8),'00:00:00.0000000') as monthStartDate
# MAGIC ,CONCAT(LEFT (monthEndDate,10),'T',SUBSTRING(monthEndDate,12,8),'00:00:00.0000000') as monthEndDate
# MAGIC ,CONCAT(LEFT (yearStartDate,10),'T',SUBSTRING(yearStartDate,12,8),'00:00:00.0000000') as yearStartDate
# MAGIC ,CONCAT(LEFT (yearEndDate,10),'T',SUBSTRING(yearEndDate,12,8),'00:00:00.0000000') as yearEndDate
# MAGIC ,financialYear
# MAGIC ,CONCAT(LEFT (financialYearStartDate,10),'T',SUBSTRING(financialYearStartDate,12,8),'00:00:00.0000000') as financialYearStartDate
# MAGIC ,CONCAT(LEFT (financialYearEndDate,10),'T',SUBSTRING(financialYearEndDate,12,8),'00:00:00.0000000') as financialYearEndDate
# MAGIC ,monthOfFinancialYear
# MAGIC ,quarterOfFinancialYear
# MAGIC ,halfOfFinancialYear
# MAGIC --,_DLCuratedZoneTimeStamp 
# MAGIC --,_RecordStart
# MAGIC --,_RecordEnd
# MAGIC --,_RecordDeleted
# MAGIC --,_RecordCurrent
# MAGIC  from  curated.dimdate 
# MAGIC  where dimDateSK = '6'
# MAGIC  except
# MAGIC  select
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
# MAGIC --,_DLCuratedZoneTimeStamp 
# MAGIC --,_RecordStart
# MAGIC --,_RecordEnd
# MAGIC --,_RecordDeleted
# MAGIC --,_RecordCurrent
# MAGIC from  Target
# MAGIC where dimDateSK = '6'
# MAGIC  

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC dimDateSK
# MAGIC ,CONCAT(LEFT (calendarDate,10),'T',SUBSTRING(calendarDate,12,8),'00:00:00.0000000') as calendarDate
# MAGIC ,dayOfWeek
# MAGIC ,dayName
# MAGIC ,dayOfMonth
# MAGIC ,dayOfYear
# MAGIC ,monthOfYear
# MAGIC ,monthName
# MAGIC ,quarterOfYear 
# MAGIC ,halfOfYear
# MAGIC ,CONCAT(LEFT (monthStartDate,10),'T',SUBSTRING(monthStartDate,12,8),'00:00:00.0000000') as monthStartDate
# MAGIC ,CONCAT(LEFT (monthEndDate,10),'T',SUBSTRING(monthEndDate,12,8),'00:00:00.0000000') as monthEndDate
# MAGIC ,CONCAT(LEFT (yearStartDate,10),'T',SUBSTRING(yearStartDate,12,8),'00:00:00.0000000') as yearStartDate
# MAGIC ,CONCAT(LEFT (yearEndDate,10),'T',SUBSTRING(yearEndDate,12,8),'00:00:00.0000000') as yearEndDate
# MAGIC ,financialYear
# MAGIC ,CONCAT(LEFT (financialYearStartDate,10),'T',SUBSTRING(financialYearStartDate,12,8),'00:00:00.0000000') as financialYearStartDate
# MAGIC ,CONCAT(LEFT (financialYearEndDate,10),'T',SUBSTRING(financialYearEndDate,12,8),'00:00:00.0000000') as financialYearEndDate
# MAGIC --,financialYearStartDate
# MAGIC --,financialYearEndDate
# MAGIC ,monthOfFinancialYear
# MAGIC ,quarterOfFinancialYear
# MAGIC ,halfOfFinancialYear
# MAGIC ,_DLCuratedZoneTimeStamp 
# MAGIC ,_RecordStart
# MAGIC ,_RecordEnd
# MAGIC ,_RecordDeleted
# MAGIC ,_RecordCurrent
# MAGIC  from  curated.dimdate where dimDateSK = '12056'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated.dimdate where dimDateSK = '12056'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Target where dimDateSK = '12056'

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
# MAGIC ,_DLCuratedZoneTimeStamp 
# MAGIC ,_RecordStart
# MAGIC ,_RecordEnd
# MAGIC ,_RecordDeleted
# MAGIC ,_RecordCurrent
# MAGIC  from  Target where dimDateSK = '12056'
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

# COMMAND ----------


