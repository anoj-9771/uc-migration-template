# Databricks notebook source
# MAGIC %sql
# MAGIC create or replace view cleansed.vw_beachwatch_pollution_weather_forecast as
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
# MAGIC from cleansed.beachwatch_cabarita_beach_pollution_weatherforecast
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
# MAGIC from cleansed.beachwatch_chiswick_baths_pollution_weatherforecast
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
# MAGIC from cleansed.beachwatch_dawn_fraser_pool_pollution_weatherforecast
