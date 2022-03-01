# Databricks notebook source
# DBTITLE 1,[Config] Connection Setup
storage_account_name = "saswcnonprod01landingtst"
storage_account_access_key = dbutils.secrets.get(scope="Test-Access",key="test-blob-key")
container_name = "archive"
file_location = "wasbs://archive@saswcnonprod01landingtst.blob.core.windows.net/sapisu/20210901/20210901_17:24:57/SCAL_TT_DATE_20210831124243.json"
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
lakedf = spark.sql("select * from cleansed.t_sapisu_scal_tt_date")

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

# MAGIC %sql
# MAGIC SELECT * from cleansed.t_sapisu_scal_tt_date

# COMMAND ----------

# DBTITLE 1,[Source with mapping]
# MAGIC %sql
# MAGIC SELECT
# MAGIC to_date(CALENDARDATE,'yyyy-MM-dd') as calendarDate,
# MAGIC CALENDARDAY as dayOfMonth,
# MAGIC CALENDARDAYOFYEAR as dayOfYear,
# MAGIC CALENDARWEEK as weekOfYear,
# MAGIC CALENDARMONTH as monthOfYear,
# MAGIC CALENDARQUARTER as quarterOfYear,
# MAGIC HALFYEAR as halfOfYear,
# MAGIC CALENDARYEAR as calendarYear,
# MAGIC WEEKDAY as dayOfWeek,
# MAGIC YEARWEEK as calendarYearWeek,
# MAGIC YEARMONTH as calendarYearMonth,
# MAGIC YEARQUARTER as calendarYearQuarter,
# MAGIC to_date(FIRSTDAYOFMONTHDATE,'yyyy-MM-dd')  as monthStartDate,
# MAGIC to_date(LASTDAYOFMONTHDATE,'yyyy-MM-dd') as monthEndDate
# MAGIC from
# MAGIC Source

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed.t_sapisu_scal_tt_date where calendardate ='1976-06-07'

# COMMAND ----------

lakedf.createOrReplaceTempView("Target")

# COMMAND ----------

# DBTITLE 1,[Verification] Count Checks
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.t_sapisu_scal_tt_date
# MAGIC union 
# MAGIC select count (*) as RecordCount, 'Source' as TableName from Source
# MAGIC where CALENDARDATE <>'null'

# COMMAND ----------

# MAGIC %sql
# MAGIC select calendarDate from cleansed.t_sapisu_scal_tt_date
# MAGIC except
# MAGIC select CALENDARDATE as calendarDate from Source

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT calendarDate, COUNT (*) as count
# MAGIC FROM cleansed.t_sapisu_scal_tt_date
# MAGIC GROUP BY calendarDate 
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY calendarDate order by calendarDate) as rn
# MAGIC FROM  cleansed.t_sapisu_scal_tt_date
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Source vs Target]
# MAGIC %sql
# MAGIC SELECT
# MAGIC to_date(CALENDARDATE,'yyyy-MM-dd') as calendarDate,
# MAGIC CALENDARDAY as dayOfMonth,
# MAGIC CALENDARDAYOFYEAR as dayOfYear,
# MAGIC CALENDARWEEK as weekOfYear,
# MAGIC CALENDARMONTH as monthOfYear,
# MAGIC CALENDARQUARTER as quarterOfYear,
# MAGIC HALFYEAR as halfOfYear,
# MAGIC CALENDARYEAR as calendarYear,
# MAGIC WEEKDAY as dayOfWeek,
# MAGIC YEARWEEK as calendarYearWeek,
# MAGIC YEARMONTH as calendarYearMonth,
# MAGIC YEARQUARTER as calendarYearQuarter,
# MAGIC to_date(FIRSTDAYOFMONTHDATE,'yyyy-MM-dd')  as monthStartDate,
# MAGIC to_date(LASTDAYOFMONTHDATE,'yyyy-MM-dd') as monthEndDate
# MAGIC from
# MAGIC Source where CALENDARDATE <>'null'
# MAGIC except
# MAGIC select
# MAGIC calendarDate,
# MAGIC dayOfMonth,
# MAGIC dayOfYear,
# MAGIC weekOfYear,
# MAGIC monthOfYear,
# MAGIC quarterOfYear,
# MAGIC halfOfYear,
# MAGIC calendarYear,
# MAGIC dayOfweek,
# MAGIC calendarYearWeek,
# MAGIC calendarYearMonth,
# MAGIC calendarYearQuarter,
# MAGIC monthStartDate,
# MAGIC monthEndDate
# MAGIC from
# MAGIC cleansed.t_sapisu_scal_tt_date 

# COMMAND ----------

# DBTITLE 1,Target vs Source]
# MAGIC %sql
# MAGIC select
# MAGIC calendarDate,
# MAGIC dayOfMonth,
# MAGIC dayOfYear,
# MAGIC weekOfYear,
# MAGIC monthOfYear,
# MAGIC quarterOfYear,
# MAGIC halfOfYear,
# MAGIC calendarYear,
# MAGIC dayOfweek,
# MAGIC calendarYearWeek,
# MAGIC calendarYearMonth,
# MAGIC calendarYearQuarter,
# MAGIC monthStartDate,
# MAGIC monthEndDate
# MAGIC from
# MAGIC cleansed.t_sapisu_scal_tt_date
# MAGIC except
# MAGIC SELECT
# MAGIC to_date(CALENDARDATE,'yyyy-MM-dd') as calendarDate,
# MAGIC CALENDARDAY as dayOfMonth,
# MAGIC CALENDARDAYOFYEAR as dayOfYear,
# MAGIC CALENDARWEEK as weekOfYear,
# MAGIC CALENDARMONTH as monthOfYear,
# MAGIC CALENDARQUARTER as quarterOfYear,
# MAGIC HALFYEAR as halfOfYear,
# MAGIC CALENDARYEAR as calendarYear,
# MAGIC WEEKDAY as dayOfWeek,
# MAGIC YEARWEEK as calendarYearWeek,
# MAGIC YEARMONTH as calendarYearMonth,
# MAGIC YEARQUARTER as calendarYearQuarter,
# MAGIC to_date(FIRSTDAYOFMONTHDATE,'yyyy-MM-dd')  as monthStartDate,
# MAGIC to_date(LASTDAYOFMONTHDATE,'yyyy-MM-dd') as monthEndDate
# MAGIC from
# MAGIC Source where CALENDARDATE <>'null'
