# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE stage.access_property_hist 
# MAGIC LOCATION 'dbfs:/mnt/datalake-curated/stage'
# MAGIC as with histRaw as(
# MAGIC                 select a.propertyNumber, propertyTypeCode, propertyType, superiorPropertyTypeCode, superiorPropertyType, propertyTypeEffectiveFrom as validFrom, rowSupersededTimestamp as updateTS
# MAGIC                 from cleansed.access_z309_thproperty a),
# MAGIC       histOrdered as(
# MAGIC                 select propertyNumber, propertyTypeCode, propertyType, superiorPropertyTypeCode, superiorPropertyType, validFrom, updateTS, row_number() over (partition by propertyNumber, validFrom order by updateTS) as rnk
# MAGIC                 from histRaw),
# MAGIC       allRows as(
# MAGIC                 select a.propertyNumber, propertyTypeCode, propertyType, superiorPropertyTypeCode, superiorPropertyType, propertyTypeEffectiveFrom as validFrom, to_timestamp('99991231235959','yyyyMMddHHmmss') as updateTS
# MAGIC                 from cleansed.access_z309_tproperty a
# MAGIC                 union all
# MAGIC                 select propertyNumber, propertyTypeCode, propertyType, superiorPropertyTypeCode, superiorPropertyType, validFrom, updateTS
# MAGIC                 from histOrdered
# MAGIC                 where rnk = 1),
# MAGIC       clean1 as(
# MAGIC                 select propertyNumber, propertyTypeCode, propertyType, superiorPropertyTypeCode, superiorPropertyType, validFrom,
# MAGIC                         coalesce(lead(validFrom,1) over (partition by propertyNumber order by validFrom),to_date('99991231','yyyyMMdd')) as validTo
# MAGIC                 from allRows),
# MAGIC       clean2 as(
# MAGIC                 select cast(propertyNumber as string), propertyTypeCode as inferiorPropertyTypeCode, propertyType as inferiorPropertyType, superiorPropertyTypeCode, superiorPropertyType, validFrom as validFromDate, case when validTo = to_date('99991231','yyyyMMdd') then validTo else validto - interval '1' day end as validToDate
# MAGIC                 from clean1
# MAGIC                 where validFrom <> validTo)
# MAGIC select * from clean2;
