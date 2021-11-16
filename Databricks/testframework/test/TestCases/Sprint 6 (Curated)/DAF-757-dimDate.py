# Databricks notebook source
# DBTITLE 0,Table
table1 = 't_sapisu_scal_tt_date'
table2 = 'dimDate'

# COMMAND ----------

import datetime
now = datetime.datetime.now()
print(now.strftime("%A"))
print(now.strftime("%B"))

# COMMAND ----------

lakedf1 = spark.sql(f"select * from cleansed.{table1}")
display(lakedf1)

# COMMAND ----------

# DBTITLE 1,[Source] Schema Check
lakedf1.printSchema()

# COMMAND ----------

lakedftarget = spark.sql("select * from curated.dimdate")
display(lakedftarget)

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
lakedftarget.printSchema()

# COMMAND ----------

lakedf1.createOrReplaceTempView("Source")

# COMMAND ----------

# DBTITLE 1,[Source] Applying Transformation
# MAGIC %sql
# MAGIC select
# MAGIC to_date(CALENDARDATE,'yyyy-MM-dd') as calendarDate
# MAGIC ,monthOfYear
# MAGIC ,monthName
# MAGIC ,dayOfMonth
# MAGIC ,dayName
# MAGIC ,quarterOfYear
# MAGIC ,monthStartDate
# MAGIC ,monthEndDate
# MAGIC ,CONCAT((yearStartDate),'-01-','01') as yearStartDate
# MAGIC ,yearEndDate
# MAGIC ,left(financialYearEndDate,4) as financialYear
# MAGIC ,financialYearStartDate
# MAGIC ,financialYearEndDate
# MAGIC ,case when monthplussix > 12 then monthplussix - 12 else monthplussix end as monthOfFinancialYear
# MAGIC ,quarterOfFinancialYear
# MAGIC ,case when substr(calendardate,6,2) = 01 then 1
# MAGIC when substr(calendardate,6,2) = 02 then 1 
# MAGIC when substr(calendardate,6,2) = 03 then 1 
# MAGIC when substr(calendardate,6,2) = 04 then 1 
# MAGIC when substr(calendardate,6,2) = 05 then 1 
# MAGIC when substr(calendardate,6,2) = 06 then 1 
# MAGIC when substr(calendardate,6,2) = 07 then 2
# MAGIC when substr(calendardate,6,2) = 08 then 2
# MAGIC when substr(calendardate,6,2) = 09 then 2
# MAGIC when substr(calendardate,6,2) = 10 then 2
# MAGIC when substr(calendardate,6,2) = 11 then 2
# MAGIC when substr(calendardate,6,2) = 12 then 2
# MAGIC end as halfOfYear
# MAGIC ,case when substr(calendardate,6,2) = 01 then 2
# MAGIC when substr(calendardate,6,2) = 02 then 2 
# MAGIC when substr(calendardate,6,2) = 03 then 2 
# MAGIC when substr(calendardate,6,2) = 04 then 2 
# MAGIC when substr(calendardate,6,2) = 05 then 2 
# MAGIC when substr(calendardate,6,2) = 06 then 2 
# MAGIC when substr(calendardate,6,2) = 07 then 1
# MAGIC when substr(calendardate,6,2) = 08 then 1
# MAGIC when substr(calendardate,6,2) = 09 then 1
# MAGIC when substr(calendardate,6,2) = 10 then 1
# MAGIC when substr(calendardate,6,2) = 11 then 1
# MAGIC when substr(calendardate,6,2) = 12 then 1
# MAGIC end as halfOfFinancialYear
# MAGIC ,dayOfWeek
# MAGIC ,dayOfYear
# MAGIC from (
# MAGIC select
# MAGIC calendarDate
# MAGIC ,monthOfYear
# MAGIC ,monthName
# MAGIC ,dayOfMonth
# MAGIC ,dayName
# MAGIC ,quarterOfYear
# MAGIC ,monthStartDate
# MAGIC ,monthEndDate
# MAGIC ,yearStartDate
# MAGIC ,yearEndDate
# MAGIC ,financialYear
# MAGIC ,financialYearStartDate
# MAGIC ,financialYearEndDate
# MAGIC ,substr(calendardate,6,2)+6 as monthplussix
# MAGIC ,case when substr(calendardate,6,2) = 01 then 03
# MAGIC when substr(calendardate,6,2) = 02 then 03
# MAGIC when substr(calendardate,6,2) = 03 then 03
# MAGIC when substr(calendardate,6,2) = 04 then 04
# MAGIC when substr(calendardate,6,2) = 05 then 04
# MAGIC when substr(calendardate,6,2) = 06 then 04
# MAGIC when substr(calendardate,6,2) = 07 then 01
# MAGIC when substr(calendardate,6,2) = 08 then 01
# MAGIC when substr(calendardate,6,2) = 09 then 01
# MAGIC when substr(calendardate,6,2) = 10 then 02
# MAGIC when substr(calendardate,6,2) = 11 then 02
# MAGIC when substr(calendardate,6,2) = 12 then 02
# MAGIC end as quarterOfFinancialYear
# MAGIC ,dayOfWeek
# MAGIC ,dayOfYear
# MAGIC from (
# MAGIC select
# MAGIC calendarDate
# MAGIC ,monthOfYear
# MAGIC ,monthName
# MAGIC ,dayOfMonth
# MAGIC ,dayName
# MAGIC ,quarterOfYear
# MAGIC ,monthStartDate
# MAGIC ,monthEndDate
# MAGIC ,yearStartDate
# MAGIC ,yearEndDate
# MAGIC ,left(financialYear,4) as financialYear
# MAGIC ,CONCAT(left(FinancialYearStartDate,4),'-07-','01') as financialYearStartDate
# MAGIC ,CONCAT(left(financialYearEndDate,4),'-06-','30') as financialYearEndDate
# MAGIC ,dayOfWeek
# MAGIC ,dayOfYear
# MAGIC --,monthOfFinancialYear
# MAGIC --,quarterOfFinancialYear
# MAGIC from (
# MAGIC select 
# MAGIC a.calendarDate
# MAGIC ,monthOfYear
# MAGIC ,date_format(date (a.calendarDate), "MMMM") as monthName
# MAGIC ,dayOfMonth
# MAGIC ,case when dayofweek(a.calendarDate) = 1 then 'Sunday'
# MAGIC when dayofweek(a.calendarDate) = 2 then 'Monday'
# MAGIC when dayofweek(a.calendarDate) = 3 then 'Tuesday'
# MAGIC when dayofweek(a.calendarDate) = 4 then 'Wednesday'
# MAGIC when dayofweek(a.calendarDate) = 5 then 'Thursday'
# MAGIC when dayofweek(a.calendarDate) = 6 then 'Friday'
# MAGIC when dayofweek(a.calendarDate) = 7 then 'Saturday'
# MAGIC end as dayName
# MAGIC ,quarterOfYear
# MAGIC ,monthStartDate
# MAGIC ,monthEndDate
# MAGIC ,LEFT(a.CALENDARDATE,4) as yearStartDate
# MAGIC ,b.calendardate as yearEndDate
# MAGIC ,case when substr(a.calendardate, 6,2) >= 7 then ADD_MONTHS(a.calendardate, +12) 
# MAGIC when substr(a.calendardate, 6,2) <= 6 then ADD_MONTHS(a.calendardate, -12) else a.calendardate end as financialYear
# MAGIC ,case when substr(a.calendardate, 6,2) <= 6 then ADD_MONTHS(a.calendardate, -12) else a.calendardate end as financialYearStartDate 
# MAGIC ,case when substr(a.calendardate, 6,2) >= 7 then ADD_MONTHS(a.calendardate, +12) else a.calendardate end as financialYearEndDate
# MAGIC ,dayOfWeek
# MAGIC ,dayOfYear
# MAGIC --,monthOfFinancialYear
# MAGIC --,quarterOfFinancialYear
# MAGIC from Source a --where calendarDate = '1900-01-25'
# MAGIC 
# MAGIC left join 
# MAGIC (
# MAGIC select * from(
# MAGIC select calendardate, dads,
# MAGIC row_number () over (partition by dads order by calendardate desc) as rn
# MAGIC from(
# MAGIC select calendardate, 
# MAGIC left(calendardate,4) as dads
# MAGIC from Source)a 
# MAGIC ) where rn = 1
# MAGIC ) b
# MAGIC on left(a.calendardate,4) = b.dads
# MAGIC )endofselect1)endofselect2)endofselect3 where calendardate = '2001-11-25'

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC calendarDate
# MAGIC ,monthOfYear
# MAGIC ,monthName
# MAGIC ,dayOfMonth
# MAGIC ,dayName
# MAGIC ,quarterOfYear
# MAGIC ,monthStartDate
# MAGIC ,monthEndDate
# MAGIC ,yearStartDate
# MAGIC ,yearEndDate
# MAGIC ,financialYear
# MAGIC ,financialYearStartDate
# MAGIC ,financialYearEndDate
# MAGIC ,monthOfFinancialYear
# MAGIC ,quarterOfFinancialYear
# MAGIC ,halfOfYear
# MAGIC ,halfOfFinancialYear
# MAGIC ,dayOfWeek
# MAGIC ,dayOfYear
# MAGIC from curated.dimdate where calendardate = '2001-11-25'

# COMMAND ----------

# DBTITLE 1,[Checks] Gaps on calendarDate
# MAGIC %sql
# MAGIC select calendardate - 1, calendardate as gapDate from curated.dimdate a where  not exists (select calendardate from curated.dimdate b where b.calendardate = a.calendardate -1)
# MAGIC --1900-01-01 is acceptable

# COMMAND ----------

# DBTITLE 1,[Verification] Blank and Dupes Check
# MAGIC %sql
# MAGIC select dimdatesk from curated.dimdate where dimdatesk in (null,'',' ')

# COMMAND ----------

# MAGIC %sql
# MAGIC select dimdatesk, count(*) as count
# MAGIC from curated.dimdate
# MAGIC group by dimdatesk
# MAGIC having count(*) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select calendardate, count(*) as count
# MAGIC from curated.dimdate
# MAGIC group by calendardate
# MAGIC having count(*) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated.dimdate

# COMMAND ----------

# DBTITLE 1,[Verification] Count Check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from curated.dimdate
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (
# MAGIC 
# MAGIC select
# MAGIC to_date(CALENDARDATE,'yyyy-MM-dd') as calendarDate
# MAGIC ,monthOfYear
# MAGIC ,monthName
# MAGIC ,dayOfMonth
# MAGIC ,dayName
# MAGIC ,quarterOfYear
# MAGIC ,monthStartDate
# MAGIC ,monthEndDate
# MAGIC ,CONCAT((yearStartDate),'-01-','01') as yearStartDate
# MAGIC ,yearEndDate
# MAGIC ,left(financialYearEndDate,4) as financialYear
# MAGIC ,financialYearStartDate
# MAGIC ,financialYearEndDate
# MAGIC ,case when monthplussix > 12 then monthplussix - 12 else monthplussix end as monthOfFinancialYear
# MAGIC ,quarterOfFinancialYear
# MAGIC ,case when substr(calendardate,6,2) = 01 then 1
# MAGIC when substr(calendardate,6,2) = 02 then 1 
# MAGIC when substr(calendardate,6,2) = 03 then 1 
# MAGIC when substr(calendardate,6,2) = 04 then 1 
# MAGIC when substr(calendardate,6,2) = 05 then 1 
# MAGIC when substr(calendardate,6,2) = 06 then 1 
# MAGIC when substr(calendardate,6,2) = 07 then 2
# MAGIC when substr(calendardate,6,2) = 08 then 2
# MAGIC when substr(calendardate,6,2) = 09 then 2
# MAGIC when substr(calendardate,6,2) = 10 then 2
# MAGIC when substr(calendardate,6,2) = 11 then 2
# MAGIC when substr(calendardate,6,2) = 12 then 2
# MAGIC end as halfOfYear
# MAGIC ,case when substr(calendardate,6,2) = 01 then 2
# MAGIC when substr(calendardate,6,2) = 02 then 2 
# MAGIC when substr(calendardate,6,2) = 03 then 2 
# MAGIC when substr(calendardate,6,2) = 04 then 2 
# MAGIC when substr(calendardate,6,2) = 05 then 2 
# MAGIC when substr(calendardate,6,2) = 06 then 2 
# MAGIC when substr(calendardate,6,2) = 07 then 1
# MAGIC when substr(calendardate,6,2) = 08 then 1
# MAGIC when substr(calendardate,6,2) = 09 then 1
# MAGIC when substr(calendardate,6,2) = 10 then 1
# MAGIC when substr(calendardate,6,2) = 11 then 1
# MAGIC when substr(calendardate,6,2) = 12 then 1
# MAGIC end as halfOfFinancialYear
# MAGIC ,dayOfWeek
# MAGIC ,dayOfYear
# MAGIC from (
# MAGIC select
# MAGIC calendarDate
# MAGIC ,monthOfYear
# MAGIC ,monthName
# MAGIC ,dayOfMonth
# MAGIC ,dayName
# MAGIC ,quarterOfYear
# MAGIC ,monthStartDate
# MAGIC ,monthEndDate
# MAGIC ,yearStartDate
# MAGIC ,yearEndDate
# MAGIC ,financialYear
# MAGIC ,financialYearStartDate
# MAGIC ,financialYearEndDate
# MAGIC ,substr(calendardate,6,2)+6 as monthplussix
# MAGIC ,case when substr(calendardate,6,2) = 01 then 03
# MAGIC when substr(calendardate,6,2) = 02 then 03
# MAGIC when substr(calendardate,6,2) = 03 then 03
# MAGIC when substr(calendardate,6,2) = 04 then 04
# MAGIC when substr(calendardate,6,2) = 05 then 04
# MAGIC when substr(calendardate,6,2) = 06 then 04
# MAGIC when substr(calendardate,6,2) = 07 then 01
# MAGIC when substr(calendardate,6,2) = 08 then 01
# MAGIC when substr(calendardate,6,2) = 09 then 01
# MAGIC when substr(calendardate,6,2) = 10 then 02
# MAGIC when substr(calendardate,6,2) = 11 then 02
# MAGIC when substr(calendardate,6,2) = 12 then 02
# MAGIC end as quarterOfFinancialYear
# MAGIC ,dayOfWeek
# MAGIC ,dayOfYear
# MAGIC from (
# MAGIC select
# MAGIC calendarDate
# MAGIC ,monthOfYear
# MAGIC ,monthName
# MAGIC ,dayOfMonth
# MAGIC ,dayName
# MAGIC ,quarterOfYear
# MAGIC ,monthStartDate
# MAGIC ,monthEndDate
# MAGIC ,yearStartDate
# MAGIC ,yearEndDate
# MAGIC ,left(financialYear,4) as financialYear
# MAGIC ,CONCAT(left(FinancialYearStartDate,4),'-07-','01') as financialYearStartDate
# MAGIC ,CONCAT(left(financialYearEndDate,4),'-06-','30') as financialYearEndDate
# MAGIC ,dayOfWeek
# MAGIC ,dayOfYear
# MAGIC --,monthOfFinancialYear
# MAGIC --,quarterOfFinancialYear
# MAGIC from (
# MAGIC select 
# MAGIC a.calendarDate
# MAGIC ,monthOfYear
# MAGIC ,date_format(date (a.calendarDate), "MMMM") as monthName
# MAGIC ,dayOfMonth
# MAGIC ,case when dayofweek(a.calendarDate) = 1 then 'Sunday'
# MAGIC when dayofweek(a.calendarDate) = 2 then 'Monday'
# MAGIC when dayofweek(a.calendarDate) = 3 then 'Tuesday'
# MAGIC when dayofweek(a.calendarDate) = 4 then 'Wednesday'
# MAGIC when dayofweek(a.calendarDate) = 5 then 'Thursday'
# MAGIC when dayofweek(a.calendarDate) = 6 then 'Friday'
# MAGIC when dayofweek(a.calendarDate) = 7 then 'Saturday'
# MAGIC end as dayName
# MAGIC ,quarterOfYear
# MAGIC ,monthStartDate
# MAGIC ,monthEndDate
# MAGIC ,LEFT(a.CALENDARDATE,4) as yearStartDate
# MAGIC ,b.calendardate as yearEndDate
# MAGIC ,case when substr(a.calendardate, 6,2) >= 7 then ADD_MONTHS(a.calendardate, +12) 
# MAGIC when substr(a.calendardate, 6,2) <= 6 then ADD_MONTHS(a.calendardate, -12) else a.calendardate end as financialYear
# MAGIC ,case when substr(a.calendardate, 6,2) <= 6 then ADD_MONTHS(a.calendardate, -12) else a.calendardate end as financialYearStartDate 
# MAGIC ,case when substr(a.calendardate, 6,2) >= 7 then ADD_MONTHS(a.calendardate, +12) else a.calendardate end as financialYearEndDate
# MAGIC ,dayOfWeek
# MAGIC ,dayOfYear
# MAGIC --,monthOfFinancialYear
# MAGIC --,quarterOfFinancialYear
# MAGIC from Source a --where calendarDate = '1900-01-25'
# MAGIC 
# MAGIC left join 
# MAGIC (
# MAGIC select * from(
# MAGIC select calendardate, dads,
# MAGIC row_number () over (partition by dads order by calendardate desc) as rn
# MAGIC from(
# MAGIC select calendardate, 
# MAGIC left(calendardate,4) as dads
# MAGIC from Source)a 
# MAGIC ) where rn = 1
# MAGIC ) b
# MAGIC on left(a.calendardate,4) = b.dads
# MAGIC )endofselect1)endofselect2)endofselect3
# MAGIC )

# COMMAND ----------

# DBTITLE 1,[Verify] Source to Target Comparison
# MAGIC %sql
# MAGIC select
# MAGIC to_date(CALENDARDATE,'yyyy-MM-dd') as calendarDate
# MAGIC ,monthOfYear
# MAGIC ,monthName
# MAGIC ,dayOfMonth
# MAGIC ,dayName
# MAGIC ,quarterOfYear
# MAGIC ,monthStartDate
# MAGIC ,monthEndDate
# MAGIC ,CONCAT((yearStartDate),'-01-','01') as yearStartDate
# MAGIC ,yearEndDate
# MAGIC ,left(financialYearEndDate,4) as financialYear
# MAGIC ,financialYearStartDate
# MAGIC ,financialYearEndDate
# MAGIC ,case when monthplussix > 12 then monthplussix - 12 else monthplussix end as monthOfFinancialYear
# MAGIC ,quarterOfFinancialYear
# MAGIC ,case when substr(calendardate,6,2) = 01 then 1
# MAGIC when substr(calendardate,6,2) = 02 then 1 
# MAGIC when substr(calendardate,6,2) = 03 then 1 
# MAGIC when substr(calendardate,6,2) = 04 then 1 
# MAGIC when substr(calendardate,6,2) = 05 then 1 
# MAGIC when substr(calendardate,6,2) = 06 then 1 
# MAGIC when substr(calendardate,6,2) = 07 then 2
# MAGIC when substr(calendardate,6,2) = 08 then 2
# MAGIC when substr(calendardate,6,2) = 09 then 2
# MAGIC when substr(calendardate,6,2) = 10 then 2
# MAGIC when substr(calendardate,6,2) = 11 then 2
# MAGIC when substr(calendardate,6,2) = 12 then 2
# MAGIC end as halfOfYear
# MAGIC ,case when substr(calendardate,6,2) = 01 then 2
# MAGIC when substr(calendardate,6,2) = 02 then 2 
# MAGIC when substr(calendardate,6,2) = 03 then 2 
# MAGIC when substr(calendardate,6,2) = 04 then 2 
# MAGIC when substr(calendardate,6,2) = 05 then 2 
# MAGIC when substr(calendardate,6,2) = 06 then 2 
# MAGIC when substr(calendardate,6,2) = 07 then 1
# MAGIC when substr(calendardate,6,2) = 08 then 1
# MAGIC when substr(calendardate,6,2) = 09 then 1
# MAGIC when substr(calendardate,6,2) = 10 then 1
# MAGIC when substr(calendardate,6,2) = 11 then 1
# MAGIC when substr(calendardate,6,2) = 12 then 1
# MAGIC end as halfOfFinancialYear
# MAGIC ,dayOfWeek
# MAGIC ,dayOfYear
# MAGIC from (
# MAGIC select
# MAGIC calendarDate
# MAGIC ,monthOfYear
# MAGIC ,monthName
# MAGIC ,dayOfMonth
# MAGIC ,dayName
# MAGIC ,quarterOfYear
# MAGIC ,monthStartDate
# MAGIC ,monthEndDate
# MAGIC ,yearStartDate
# MAGIC ,yearEndDate
# MAGIC ,financialYear
# MAGIC ,financialYearStartDate
# MAGIC ,financialYearEndDate
# MAGIC ,substr(calendardate,6,2)+6 as monthplussix
# MAGIC ,case when substr(calendardate,6,2) = 01 then 03
# MAGIC when substr(calendardate,6,2) = 02 then 03
# MAGIC when substr(calendardate,6,2) = 03 then 03
# MAGIC when substr(calendardate,6,2) = 04 then 04
# MAGIC when substr(calendardate,6,2) = 05 then 04
# MAGIC when substr(calendardate,6,2) = 06 then 04
# MAGIC when substr(calendardate,6,2) = 07 then 01
# MAGIC when substr(calendardate,6,2) = 08 then 01
# MAGIC when substr(calendardate,6,2) = 09 then 01
# MAGIC when substr(calendardate,6,2) = 10 then 02
# MAGIC when substr(calendardate,6,2) = 11 then 02
# MAGIC when substr(calendardate,6,2) = 12 then 02
# MAGIC end as quarterOfFinancialYear
# MAGIC ,dayOfWeek
# MAGIC ,dayOfYear
# MAGIC from (
# MAGIC select
# MAGIC calendarDate
# MAGIC ,monthOfYear
# MAGIC ,monthName
# MAGIC ,dayOfMonth
# MAGIC ,dayName
# MAGIC ,quarterOfYear
# MAGIC ,monthStartDate
# MAGIC ,monthEndDate
# MAGIC ,yearStartDate
# MAGIC ,yearEndDate
# MAGIC ,left(financialYear,4) as financialYear
# MAGIC ,CONCAT(left(FinancialYearStartDate,4),'-07-','01') as financialYearStartDate
# MAGIC ,CONCAT(left(financialYearEndDate,4),'-06-','30') as financialYearEndDate
# MAGIC ,dayOfWeek
# MAGIC ,dayOfYear
# MAGIC --,monthOfFinancialYear
# MAGIC --,quarterOfFinancialYear
# MAGIC from (
# MAGIC select 
# MAGIC a.calendarDate
# MAGIC ,monthOfYear
# MAGIC ,date_format(date (a.calendarDate), "MMMM") as monthName
# MAGIC ,dayOfMonth
# MAGIC ,case when dayofweek(a.calendarDate) = 1 then 'Sunday'
# MAGIC when dayofweek(a.calendarDate) = 2 then 'Monday'
# MAGIC when dayofweek(a.calendarDate) = 3 then 'Tuesday'
# MAGIC when dayofweek(a.calendarDate) = 4 then 'Wednesday'
# MAGIC when dayofweek(a.calendarDate) = 5 then 'Thursday'
# MAGIC when dayofweek(a.calendarDate) = 6 then 'Friday'
# MAGIC when dayofweek(a.calendarDate) = 7 then 'Saturday'
# MAGIC end as dayName
# MAGIC ,quarterOfYear
# MAGIC ,monthStartDate
# MAGIC ,monthEndDate
# MAGIC ,LEFT(a.CALENDARDATE,4) as yearStartDate
# MAGIC ,b.calendardate as yearEndDate
# MAGIC ,case when substr(a.calendardate, 6,2) >= 7 then ADD_MONTHS(a.calendardate, +12) 
# MAGIC when substr(a.calendardate, 6,2) <= 6 then ADD_MONTHS(a.calendardate, -12) else a.calendardate end as financialYear
# MAGIC ,case when substr(a.calendardate, 6,2) <= 6 then ADD_MONTHS(a.calendardate, -12) else a.calendardate end as financialYearStartDate 
# MAGIC ,case when substr(a.calendardate, 6,2) >= 7 then ADD_MONTHS(a.calendardate, +12) else a.calendardate end as financialYearEndDate
# MAGIC ,dayOfWeek
# MAGIC ,dayOfYear
# MAGIC --,monthOfFinancialYear
# MAGIC --,quarterOfFinancialYear
# MAGIC from Source a --where calendarDate = '1900-01-25'
# MAGIC 
# MAGIC left join 
# MAGIC (
# MAGIC select * from(
# MAGIC select calendardate, dads,
# MAGIC row_number () over (partition by dads order by calendardate desc) as rn
# MAGIC from(
# MAGIC select calendardate, 
# MAGIC left(calendardate,4) as dads
# MAGIC from Source)a 
# MAGIC ) where rn = 1
# MAGIC ) b
# MAGIC on left(a.calendardate,4) = b.dads
# MAGIC )endofselect1)endofselect2)endofselect3
# MAGIC 
# MAGIC except
# MAGIC 
# MAGIC select
# MAGIC calendarDate
# MAGIC ,monthOfYear
# MAGIC ,monthName
# MAGIC ,dayOfMonth
# MAGIC ,dayName
# MAGIC ,quarterOfYear
# MAGIC ,monthStartDate
# MAGIC ,monthEndDate
# MAGIC ,yearStartDate
# MAGIC ,yearEndDate
# MAGIC ,financialYear
# MAGIC ,financialYearStartDate
# MAGIC ,financialYearEndDate
# MAGIC ,monthOfFinancialYear
# MAGIC ,quarterOfFinancialYear
# MAGIC ,halfOfYear
# MAGIC ,halfOfFinancialYear
# MAGIC ,dayOfWeek
# MAGIC ,dayOfYear
# MAGIC from curated.dimdate

# COMMAND ----------

# DBTITLE 1,[Verify] Target to Source Comparison
# MAGIC %sql
# MAGIC 
# MAGIC select
# MAGIC calendarDate
# MAGIC ,monthOfYear
# MAGIC ,monthName
# MAGIC ,dayOfMonth
# MAGIC ,dayName
# MAGIC ,quarterOfYear
# MAGIC ,monthStartDate
# MAGIC ,monthEndDate
# MAGIC ,yearStartDate
# MAGIC ,yearEndDate
# MAGIC ,financialYear
# MAGIC ,financialYearStartDate
# MAGIC ,financialYearEndDate
# MAGIC ,monthOfFinancialYear
# MAGIC ,quarterOfFinancialYear
# MAGIC ,halfOfYear
# MAGIC ,halfOfFinancialYear
# MAGIC ,dayOfWeek
# MAGIC ,dayOfYear
# MAGIC from curated.dimdate
# MAGIC 
# MAGIC except
# MAGIC 
# MAGIC select
# MAGIC to_date(CALENDARDATE,'yyyy-MM-dd') as calendarDate
# MAGIC ,monthOfYear
# MAGIC ,monthName
# MAGIC ,dayOfMonth
# MAGIC ,dayName
# MAGIC ,quarterOfYear
# MAGIC ,monthStartDate
# MAGIC ,monthEndDate
# MAGIC ,CONCAT((yearStartDate),'-01-','01') as yearStartDate
# MAGIC ,yearEndDate
# MAGIC ,left(financialYearEndDate,4) as financialYear
# MAGIC ,financialYearStartDate
# MAGIC ,financialYearEndDate
# MAGIC ,case when monthplussix > 12 then monthplussix - 12 else monthplussix end as monthOfFinancialYear
# MAGIC ,quarterOfFinancialYear
# MAGIC ,case when substr(calendardate,6,2) = 01 then 1
# MAGIC when substr(calendardate,6,2) = 02 then 1 
# MAGIC when substr(calendardate,6,2) = 03 then 1 
# MAGIC when substr(calendardate,6,2) = 04 then 1 
# MAGIC when substr(calendardate,6,2) = 05 then 1 
# MAGIC when substr(calendardate,6,2) = 06 then 1 
# MAGIC when substr(calendardate,6,2) = 07 then 2
# MAGIC when substr(calendardate,6,2) = 08 then 2
# MAGIC when substr(calendardate,6,2) = 09 then 2
# MAGIC when substr(calendardate,6,2) = 10 then 2
# MAGIC when substr(calendardate,6,2) = 11 then 2
# MAGIC when substr(calendardate,6,2) = 12 then 2
# MAGIC end as halfOfYear
# MAGIC ,case when substr(calendardate,6,2) = 01 then 2
# MAGIC when substr(calendardate,6,2) = 02 then 2 
# MAGIC when substr(calendardate,6,2) = 03 then 2 
# MAGIC when substr(calendardate,6,2) = 04 then 2 
# MAGIC when substr(calendardate,6,2) = 05 then 2 
# MAGIC when substr(calendardate,6,2) = 06 then 2 
# MAGIC when substr(calendardate,6,2) = 07 then 1
# MAGIC when substr(calendardate,6,2) = 08 then 1
# MAGIC when substr(calendardate,6,2) = 09 then 1
# MAGIC when substr(calendardate,6,2) = 10 then 1
# MAGIC when substr(calendardate,6,2) = 11 then 1
# MAGIC when substr(calendardate,6,2) = 12 then 1
# MAGIC end as halfOfFinancialYear
# MAGIC ,dayOfWeek
# MAGIC ,dayOfYear
# MAGIC from (
# MAGIC select
# MAGIC calendarDate
# MAGIC ,monthOfYear
# MAGIC ,monthName
# MAGIC ,dayOfMonth
# MAGIC ,dayName
# MAGIC ,quarterOfYear
# MAGIC ,monthStartDate
# MAGIC ,monthEndDate
# MAGIC ,yearStartDate
# MAGIC ,yearEndDate
# MAGIC ,financialYear
# MAGIC ,financialYearStartDate
# MAGIC ,financialYearEndDate
# MAGIC ,substr(calendardate,6,2)+6 as monthplussix
# MAGIC ,case when substr(calendardate,6,2) = 01 then 03
# MAGIC when substr(calendardate,6,2) = 02 then 03
# MAGIC when substr(calendardate,6,2) = 03 then 03
# MAGIC when substr(calendardate,6,2) = 04 then 04
# MAGIC when substr(calendardate,6,2) = 05 then 04
# MAGIC when substr(calendardate,6,2) = 06 then 04
# MAGIC when substr(calendardate,6,2) = 07 then 01
# MAGIC when substr(calendardate,6,2) = 08 then 01
# MAGIC when substr(calendardate,6,2) = 09 then 01
# MAGIC when substr(calendardate,6,2) = 10 then 02
# MAGIC when substr(calendardate,6,2) = 11 then 02
# MAGIC when substr(calendardate,6,2) = 12 then 02
# MAGIC end as quarterOfFinancialYear
# MAGIC ,dayOfWeek
# MAGIC ,dayOfYear
# MAGIC from (
# MAGIC select
# MAGIC calendarDate
# MAGIC ,monthOfYear
# MAGIC ,monthName
# MAGIC ,dayOfMonth
# MAGIC ,dayName
# MAGIC ,quarterOfYear
# MAGIC ,monthStartDate
# MAGIC ,monthEndDate
# MAGIC ,yearStartDate
# MAGIC ,yearEndDate
# MAGIC ,left(financialYear,4) as financialYear
# MAGIC ,CONCAT(left(FinancialYearStartDate,4),'-07-','01') as financialYearStartDate
# MAGIC ,CONCAT(left(financialYearEndDate,4),'-06-','30') as financialYearEndDate
# MAGIC ,dayOfWeek
# MAGIC ,dayOfYear
# MAGIC --,monthOfFinancialYear
# MAGIC --,quarterOfFinancialYear
# MAGIC from (
# MAGIC select 
# MAGIC a.calendarDate
# MAGIC ,monthOfYear
# MAGIC ,date_format(date (a.calendarDate), "MMMM") as monthName
# MAGIC ,dayOfMonth
# MAGIC ,case when dayofweek(a.calendarDate) = 1 then 'Sunday'
# MAGIC when dayofweek(a.calendarDate) = 2 then 'Monday'
# MAGIC when dayofweek(a.calendarDate) = 3 then 'Tuesday'
# MAGIC when dayofweek(a.calendarDate) = 4 then 'Wednesday'
# MAGIC when dayofweek(a.calendarDate) = 5 then 'Thursday'
# MAGIC when dayofweek(a.calendarDate) = 6 then 'Friday'
# MAGIC when dayofweek(a.calendarDate) = 7 then 'Saturday'
# MAGIC end as dayName
# MAGIC ,quarterOfYear
# MAGIC ,monthStartDate
# MAGIC ,monthEndDate
# MAGIC ,LEFT(a.CALENDARDATE,4) as yearStartDate
# MAGIC ,b.calendardate as yearEndDate
# MAGIC ,case when substr(a.calendardate, 6,2) >= 7 then ADD_MONTHS(a.calendardate, +12) 
# MAGIC when substr(a.calendardate, 6,2) <= 6 then ADD_MONTHS(a.calendardate, -12) else a.calendardate end as financialYear
# MAGIC ,case when substr(a.calendardate, 6,2) <= 6 then ADD_MONTHS(a.calendardate, -12) else a.calendardate end as financialYearStartDate 
# MAGIC ,case when substr(a.calendardate, 6,2) >= 7 then ADD_MONTHS(a.calendardate, +12) else a.calendardate end as financialYearEndDate
# MAGIC ,dayOfWeek
# MAGIC ,dayOfYear
# MAGIC --,monthOfFinancialYear
# MAGIC --,quarterOfFinancialYear
# MAGIC from Source a --where calendarDate = '1900-01-25'
# MAGIC 
# MAGIC left join 
# MAGIC (
# MAGIC select * from(
# MAGIC select calendardate, dads,
# MAGIC row_number () over (partition by dads order by calendardate desc) as rn
# MAGIC from(
# MAGIC select calendardate, 
# MAGIC left(calendardate,4) as dads
# MAGIC from Source)a 
# MAGIC ) where rn = 1
# MAGIC ) b
# MAGIC on left(a.calendardate,4) = b.dads
# MAGIC )endofselect1)endofselect2)endofselect3

# COMMAND ----------

# DBTITLE 1,[Archived] Code from scal_tt_date for reference
# MAGIC %sql
# MAGIC /**select
# MAGIC to_date(CALENDARDATE,'yyyy-MM-dd') as calendarDate,
# MAGIC dayOfMonth,
# MAGIC dayOfYear,
# MAGIC weekOfYear,
# MAGIC monthOfYear,
# MAGIC quarterOfYear,
# MAGIC halfOfYear,
# MAGIC calendarYear,
# MAGIC dayOfweek,
# MAGIC calendarYearMonth,
# MAGIC calendarYearQuarter,
# MAGIC monthStartDate,
# MAGIC monthEndDate,
# MAGIC CONCAT((yearStartDate),'-01-','01') as yearStartDate,
# MAGIC yearEndDate,
# MAGIC left(financialYearEndDate,4) as financialYear,
# MAGIC financialYearStartDate,
# MAGIC financialYearEndDate,
# MAGIC weekOfFinancialYear + 1 as weekOfFinancialYear,
# MAGIC --case when weekOfFinancialYear = 0 then 1 else weekOfFinancialYear end as weekOfFinancialYear,
# MAGIC case when monthplussix > 12 then monthplussix - 12 else monthplussix end as monthOfFinancialYear,
# MAGIC quarterOfFinancialYear
# MAGIC from (
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
# MAGIC calendarYearMonth,
# MAGIC calendarYearQuarter,
# MAGIC monthStartDate,
# MAGIC monthEndDate,
# MAGIC yearStartDate,
# MAGIC yearEndDate,
# MAGIC financialYearStartDate,
# MAGIC financialYearEndDate,
# MAGIC cast(datediff( calendarDate,financialYearStartDate )/7 as int) AS weekOfFinancialYear,
# MAGIC substr(calendardate,6,2)+6 as monthplussix,
# MAGIC --cast(MONTHS_BETWEEN (TO_DATE(calendardate),TO_DATE(financialYearStartDate)) as int) as monthOfFinancialYear,
# MAGIC case when substr(calendardate,6,2) = 01 then 03
# MAGIC when substr(calendardate,6,2) = 02 then 03
# MAGIC when substr(calendardate,6,2) = 03 then 03
# MAGIC when substr(calendardate,6,2) = 04 then 04
# MAGIC when substr(calendardate,6,2) = 05 then 04
# MAGIC when substr(calendardate,6,2) = 06 then 04
# MAGIC when substr(calendardate,6,2) = 07 then 01
# MAGIC when substr(calendardate,6,2) = 08 then 01
# MAGIC when substr(calendardate,6,2) = 09 then 01
# MAGIC when substr(calendardate,6,2) = 10 then 02
# MAGIC when substr(calendardate,6,2) = 11 then 02
# MAGIC when substr(calendardate,6,2) = 12 then 02
# MAGIC end as quarterOfFinancialYear
# MAGIC --cast(datediff(mm, calendarDate,financialYearStartDate ) as int) AS weekOfFinancialYear  
# MAGIC --case when weekOfFinancialYear = 0 then 1 else weekOfFinancialYear end as weekOfFinancialYear
# MAGIC 
# MAGIC from(
# MAGIC SELECT
# MAGIC calendarDate,
# MAGIC dayOfMonth,
# MAGIC dayOfYear,
# MAGIC weekOfYear,
# MAGIC monthOfYear,
# MAGIC quarterOfYear,
# MAGIC halfOfYear,
# MAGIC calendarYear,
# MAGIC dayOfweek,
# MAGIC calendarYearMonth,
# MAGIC calendarYearQuarter,
# MAGIC monthStartDate,
# MAGIC monthEndDate,
# MAGIC yearStartDate,
# MAGIC yearEndDate,
# MAGIC left(financialYear,4) as financialYear,
# MAGIC CONCAT(left(FinancialYearStartDate,4),'-07-','01') as financialYearStartDate,
# MAGIC CONCAT(left(financialYearEndDate,4),'-06-','30') as financialYearEndDate
# MAGIC 
# MAGIC FROM(
# MAGIC SELECT
# MAGIC a.CALENDARDATE as calendarDate,
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
# MAGIC to_date(LASTDAYOFMONTHDATE,'yyyy-MM-dd') as monthEndDate,
# MAGIC LEFT(a.CALENDARDATE,4) as yearStartDate,
# MAGIC b.calendardate as yearEndDate,
# MAGIC case when substr(a.calendardate, 6,2) >= 7 then ADD_MONTHS(a.calendardate, +12) 
# MAGIC when substr(a.calendardate, 6,2) <= 6 then ADD_MONTHS(a.calendardate, -12) else a.calendardate end as financialYear, 
# MAGIC case when substr(a.calendardate, 6,2) <= 6 then ADD_MONTHS(a.calendardate, -12) else a.calendardate end as financialYearStartDate, 
# MAGIC case when substr(a.calendardate, 6,2) >= 7 then ADD_MONTHS(a.calendardate, +12) else a.calendardate end as financialYearEndDate
# MAGIC --datediff( '1900-07-01','1900-01-23')/7 AS weekOfFinancialYear--need to hardcode 06-31
# MAGIC from 
# MAGIC source a
# MAGIC left join 
# MAGIC (
# MAGIC select * from(
# MAGIC select calendardate, dads,
# MAGIC row_number () over (partition by dads order by calendardate desc) as rn
# MAGIC from(
# MAGIC select calendardate, 
# MAGIC left(calendardate,4) as dads
# MAGIC from Source )a 
# MAGIC ) where rn = 1
# MAGIC ) b
# MAGIC on left(a.calendardate,4) = b.dads
# MAGIC )endofselect)endofselect2)endofselect3 --where calendardate = '2000-06-08'**/
