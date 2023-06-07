# Databricks notebook source

# MAGIC %run /build/includes/global-variables-python

# COMMAND ----------

spark.conf.set("c.catalog_name", ADS_DATABASE_CLEANSED)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view ${c.catalog_name}.beachwatch.pollution_weather_forecast as
# MAGIC 
# MAGIC select distinct
# MAGIC   locationId,
# MAGIC   description as siteName,
# MAGIC   data.oceanTemp,
# MAGIC   data.airTemp,
# MAGIC   case 
# MAGIC   when item.data.advice like '%unlikely%' then 'Unlikely'
# MAGIC   when item.data.advice like '%likely%' then 'Likely'  
# MAGIC   when item.data.advice like '%possible%' then 'Possible'
# MAGIC   end as waterQuality,
# MAGIC   updated,
# MAGIC   cast(left(data.hightide,charindex("metres", data.hightide) -2) as float) as highTideMeters,
# MAGIC   cast(left(data.lowtide,charindex("metres", data.lowtide) -2) as float) as lowTideMeters,
# MAGIC   substring(data.hightide, charindex("at", data.hightide) + 3, length(data.hightide)) as highTideTime,
# MAGIC   substring(data.lowtide, charindex("at", data.lowtide) + 3, length(data.lowtide)) as lowTideTime
# MAGIC from ${c.catalog_name}.beachwatch.cabarita_beach_pollution_weatherforecast
# MAGIC 
# MAGIC union all
# MAGIC select distinct
# MAGIC   locationId,
# MAGIC   description as siteName,
# MAGIC   data.oceanTemp,
# MAGIC   data.airTemp,
# MAGIC   case 
# MAGIC   when item.data.advice like '%unlikely%' then 'Unlikely'
# MAGIC   when item.data.advice like '%likely%' then 'Likely'  
# MAGIC   when item.data.advice like '%possible%' then 'Possible'
# MAGIC   end as waterQuality,
# MAGIC   updated,
# MAGIC   cast(left(data.hightide,charindex("metres", data.hightide) -2) as float) as highTideMeters,
# MAGIC   cast(left(data.lowtide,charindex("metres", data.lowtide) -2) as float) as lowTideMeters,
# MAGIC   substring(data.hightide, charindex("at", data.hightide) + 3, length(data.hightide)) as highTideTime,
# MAGIC   substring(data.lowtide, charindex("at", data.lowtide) + 3, length(data.lowtide)) as lowTideTime
# MAGIC from ${c.catalog_name}.beachwatch.chiswick_baths_pollution_weatherforecast
# MAGIC 
# MAGIC union all
# MAGIC select distinct
# MAGIC   locationId,
# MAGIC   description as siteName,
# MAGIC   data.oceanTemp,
# MAGIC   data.airTemp,
# MAGIC   case 
# MAGIC   when item.data.advice like '%unlikely%' then 'Unlikely'
# MAGIC   when item.data.advice like '%likely%' then 'Likely'  
# MAGIC   when item.data.advice like '%possible%' then 'Possible'
# MAGIC   end as waterQuality,
# MAGIC   updated,
# MAGIC   cast(left(data.hightide,charindex("metres", data.hightide) -2) as float) as highTideMeters,
# MAGIC   cast(left(data.lowtide,charindex("metres", data.lowtide) -2) as float) as lowTideMeters,
# MAGIC   substring(data.hightide, charindex("at", data.hightide) + 3, length(data.hightide)) as highTideTime,
# MAGIC   substring(data.lowtide, charindex("at", data.lowtide) + 3, length(data.lowtide)) as lowTideTime
# MAGIC from ${c.catalog_name}.beachwatch.dawn_fraser_pool_pollution_weatherforecast
