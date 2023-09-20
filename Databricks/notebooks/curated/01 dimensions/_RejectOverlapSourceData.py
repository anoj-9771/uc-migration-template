# Databricks notebook source
# MAGIC %run ../../includes/include-all-util

# COMMAND ----------

spark.conf.set("c.catalog_name", ADS_DATABASE_CLEANSED)

# COMMAND ----------
# DBTITLE 1,Property Type History
# MAGIC %sql
# MAGIC with overlap_result_temp as (
# MAGIC select
# MAGIC  *,
# MAGIC  case when
# MAGIC   (validFromDate between (lag(validFromDate,1) over (partition by propertyNumber order by validToDate,validFromDate)) and (lag(validToDate,1) over (partition by propertyNumber order by validToDate,validFromDate))) or
# MAGIC   (validToDate between (lag(validFromDate,1) over (partition by propertyNumber order by validToDate,validFromDate)) and (lag(validToDate,1) over (partition by propertyNumber order by validToDate,validFromDate))) or
# MAGIC   (validFromDate < (lag(validFromDate,1) over (partition by propertyNumber order by validToDate,validFromDate)) and validToDate > (lag(validToDate,1) over (partition by propertyNumber order by validToDate,validFromDate))) or
# MAGIC   (validFromDate >(lag(validFromDate,1) over (partition by propertyNumber order by validToDate,validFromDate)) and validToDate < (lag(validToDate,1) over (partition by propertyNumber order by validToDate,validFromDate)))
# MAGIC  then 'yes'
# MAGIC  when (lag(validFromDate,1) over (partition by propertyNumber order by validToDate,validFromDate)) is null
# MAGIC  then NULL
# MAGIC  else 'no'
# MAGIC  end as overlap
# MAGIC from ${c.catalog_name}.isu.zcd_tpropty_hist where _RecordDeleted <> 1
# MAGIC ),
# MAGIC overlap_result as (
# MAGIC select propertyNumber, validFromDate, validToDate from ${c.catalog_name}.isu.zcd_tpropty_hist o where exists(select 1 from overlap_result_temp i where o.propertyNumber=i.propertyNumber and i.overlap='yes')
# MAGIC )
# MAGIC MERGE INTO ${c.catalog_name}.isu.zcd_tpropty_hist isu_zcd_tpropty_hist
# MAGIC     using overlap_result
# MAGIC     on isu_zcd_tpropty_hist.propertyNumber = overlap_result.propertyNumber
# MAGIC     and isu_zcd_tpropty_hist.validFromDate = overlap_result.validFromDate
# MAGIC     and isu_zcd_tpropty_hist.validToDate = overlap_result.validToDate
# MAGIC     WHEN MATCHED THEN UPDATE SET
# MAGIC     _RecordDeleted=-1

# COMMAND ----------

# DBTITLE 1,Device History
# MAGIC %sql
# MAGIC with overlap_result_temp as (
# MAGIC select
# MAGIC  *,
# MAGIC  case when
# MAGIC   (validFromDate between (lag(validFromDate,1) over (partition by equipmentNumber order by validToDate,validFromDate)) and (lag(validToDate,1) over (partition by equipmentNumber order by validToDate,validFromDate))) or
# MAGIC   (validToDate between (lag(validFromDate,1) over (partition by equipmentNumber order by validToDate,validFromDate)) and (lag(validToDate,1) over (partition by equipmentNumber order by validToDate,validFromDate))) or
# MAGIC   (validFromDate < (lag(validFromDate,1) over (partition by equipmentNumber order by validToDate,validFromDate)) and validToDate > (lag(validToDate,1) over (partition by equipmentNumber order by validToDate,validFromDate))) or
# MAGIC   (validFromDate >(lag(validFromDate,1) over (partition by equipmentNumber order by validToDate,validFromDate)) and validToDate < (lag(validToDate,1) over (partition by equipmentNumber order by validToDate,validFromDate)))
# MAGIC  then 'yes'
# MAGIC  when (lag(validFromDate,1) over (partition by equipmentNumber order by validToDate,validFromDate)) is null
# MAGIC  then NULL
# MAGIC  else 'no'
# MAGIC  end as overlap
# MAGIC from ${c.catalog_name}.isu.0UC_DEVICEH_ATTR where _RecordDeleted <> 1
# MAGIC ),
# MAGIC overlap_result as (
# MAGIC select equipmentNumber, validFromDate, validToDate from ${c.catalog_name}.isu.0UC_DEVICEH_ATTR o where exists(select 1 from overlap_result_temp i where o.equipmentNumber=i.equipmentNumber and i.overlap='yes')
# MAGIC )
# MAGIC MERGE INTO ${c.catalog_name}.isu.0UC_DEVICEH_ATTR isu_0UC_DEVICEH_ATTR
# MAGIC     using overlap_result
# MAGIC     on isu_0UC_DEVICEH_ATTR.equipmentNumber = overlap_result.equipmentNumber
# MAGIC     and isu_0UC_DEVICEH_ATTR.validFromDate = overlap_result.validFromDate
# MAGIC     and isu_0UC_DEVICEH_ATTR.validToDate = overlap_result.validToDate
# MAGIC     WHEN MATCHED THEN UPDATE SET
# MAGIC     _RecordDeleted=-1

# COMMAND ----------

# DBTITLE 1,Installation History
# MAGIC %sql
# MAGIC with overlap_result_temp as (
# MAGIC select
# MAGIC  *,
# MAGIC  case when
# MAGIC   (validFromDate between (lag(validFromDate,1) over (partition by installationNumber order by validToDate,validFromDate)) and (lag(validToDate,1) over (partition by installationNumber order by validToDate,validFromDate))) or
# MAGIC   (validToDate between (lag(validFromDate,1) over (partition by installationNumber order by validToDate,validFromDate)) and (lag(validToDate,1) over (partition by installationNumber order by validToDate,validFromDate))) or
# MAGIC   (validFromDate < (lag(validFromDate,1) over (partition by installationNumber order by validToDate,validFromDate)) and validToDate > (lag(validToDate,1) over (partition by installationNumber order by validToDate,validFromDate))) or
# MAGIC   (validFromDate >(lag(validFromDate,1) over (partition by installationNumber order by validToDate,validFromDate)) and validToDate < (lag(validToDate,1) over (partition by installationNumber order by validToDate,validFromDate)))
# MAGIC  then 'yes'
# MAGIC  when (lag(validFromDate,1) over (partition by installationNumber order by validToDate,validFromDate)) is null
# MAGIC  then NULL
# MAGIC  else 'no'
# MAGIC  end as overlap
# MAGIC from ${c.catalog_name}.isu.0ucinstallah_attr_2 where _RecordDeleted <> 1
# MAGIC ),
# MAGIC overlap_result as (
# MAGIC select installationNumber, validFromDate, validToDate from ${c.catalog_name}.isu.0ucinstallah_attr_2 o where exists(select 1 from overlap_result_temp i where o.installationNumber=i.installationNumber and i.overlap='yes')
# MAGIC )
# MAGIC MERGE INTO ${c.catalog_name}.isu.0ucinstallah_attr_2 isu_0ucinstallah_attr_2
# MAGIC     using overlap_result
# MAGIC     on isu_0ucinstallah_attr_2.installationNumber = overlap_result.installationNumber
# MAGIC     and isu_0ucinstallah_attr_2.validFromDate = overlap_result.validFromDate
# MAGIC     and isu_0ucinstallah_attr_2.validToDate = overlap_result.validToDate
# MAGIC     WHEN MATCHED THEN UPDATE SET
# MAGIC     _RecordDeleted=-1

# COMMAND ----------

# DBTITLE 1,Contract History
# MAGIC %sql
# MAGIC with overlap_result_temp as (
# MAGIC select
# MAGIC  *,
# MAGIC  case when
# MAGIC   (validFromDate between (lag(validFromDate,1) over (partition by contractId order by validToDate,validFromDate)) and (lag(validToDate,1) over (partition by contractId order by validToDate,validFromDate))) or
# MAGIC   (validToDate between (lag(validFromDate,1) over (partition by contractId order by validToDate,validFromDate)) and (lag(validToDate,1) over (partition by contractId order by validToDate,validFromDate))) or
# MAGIC   (validFromDate < (lag(validFromDate,1) over (partition by contractId order by validToDate,validFromDate)) and validToDate > (lag(validToDate,1) over (partition by contractId order by validToDate,validFromDate))) or
# MAGIC   (validFromDate >(lag(validFromDate,1) over (partition by contractId order by validToDate,validFromDate)) and validToDate < (lag(validToDate,1) over (partition by contractId order by validToDate,validFromDate)))
# MAGIC  then 'yes'
# MAGIC  when (lag(validFromDate,1) over (partition by contractId order by validToDate,validFromDate)) is null
# MAGIC  then NULL
# MAGIC  else 'no'
# MAGIC  end as overlap
# MAGIC from ${c.catalog_name}.isu.0uccontracth_attr_2 where _RecordDeleted <> 1
# MAGIC ),
# MAGIC overlap_result as (
# MAGIC select contractId, validFromDate, validToDate from ${c.catalog_name}.isu.0uccontracth_attr_2 o where exists(select 1 from overlap_result_temp i where o.contractId=i.contractId and i.overlap='yes')
# MAGIC )
# MAGIC MERGE INTO ${c.catalog_name}.isu.0uccontracth_attr_2 isu_0uccontracth_attr_2
# MAGIC     using overlap_result
# MAGIC     on isu_0uccontracth_attr_2.contractId = overlap_result.contractId
# MAGIC     and isu_0uccontracth_attr_2.validFromDate = overlap_result.validFromDate
# MAGIC     and isu_0uccontracth_attr_2.validToDate = overlap_result.validToDate
# MAGIC     WHEN MATCHED THEN UPDATE SET
# MAGIC     _RecordDeleted=-1
