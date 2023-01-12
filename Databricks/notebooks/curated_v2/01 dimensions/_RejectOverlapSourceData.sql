-- Databricks notebook source
-- DBTITLE 1,Property Type History
with overlap_result_temp as (
select
 *,
 case when
  (validFromDate between (lag(validFromDate,1) over (partition by propertyNumber order by validToDate,validFromDate)) and (lag(validToDate,1) over (partition by propertyNumber order by validToDate,validFromDate))) or
  (validToDate between (lag(validFromDate,1) over (partition by propertyNumber order by validToDate,validFromDate)) and (lag(validToDate,1) over (partition by propertyNumber order by validToDate,validFromDate))) or
  (validFromDate < (lag(validFromDate,1) over (partition by propertyNumber order by validToDate,validFromDate)) and validToDate > (lag(validToDate,1) over (partition by propertyNumber order by validToDate,validFromDate))) or
  (validFromDate >(lag(validFromDate,1) over (partition by propertyNumber order by validToDate,validFromDate)) and validToDate < (lag(validToDate,1) over (partition by propertyNumber order by validToDate,validFromDate)))
 then 'yes'
 when (lag(validFromDate,1) over (partition by propertyNumber order by validToDate,validFromDate)) is null
 then NULL
 else 'no'
 end as overlap
from cleansed.isu_zcd_tpropty_hist where _RecordDeleted <> 1
),
overlap_result as (
select propertyNumber, validFromDate, validToDate from cleansed.isu_zcd_tpropty_hist o where exists(select 1 from overlap_result_temp i where o.propertyNumber=i.propertyNumber and i.overlap='yes')
)
MERGE INTO cleansed.isu_zcd_tpropty_hist
    using overlap_result
    on isu_zcd_tpropty_hist.propertyNumber = overlap_result.propertyNumber
    and isu_zcd_tpropty_hist.validFromDate = overlap_result.validFromDate
    and isu_zcd_tpropty_hist.validToDate = overlap_result.validToDate
    WHEN MATCHED THEN UPDATE SET
    _RecordDeleted=-1

-- COMMAND ----------

-- DBTITLE 1,Device History
with overlap_result_temp as (
select
 *,
 case when
  (validFromDate between (lag(validFromDate,1) over (partition by equipmentNumber order by validToDate,validFromDate)) and (lag(validToDate,1) over (partition by equipmentNumber order by validToDate,validFromDate))) or
  (validToDate between (lag(validFromDate,1) over (partition by equipmentNumber order by validToDate,validFromDate)) and (lag(validToDate,1) over (partition by equipmentNumber order by validToDate,validFromDate))) or
  (validFromDate < (lag(validFromDate,1) over (partition by equipmentNumber order by validToDate,validFromDate)) and validToDate > (lag(validToDate,1) over (partition by equipmentNumber order by validToDate,validFromDate))) or
  (validFromDate >(lag(validFromDate,1) over (partition by equipmentNumber order by validToDate,validFromDate)) and validToDate < (lag(validToDate,1) over (partition by equipmentNumber order by validToDate,validFromDate)))
 then 'yes'
 when (lag(validFromDate,1) over (partition by equipmentNumber order by validToDate,validFromDate)) is null
 then NULL
 else 'no'
 end as overlap
from cleansed.isu_0UC_DEVICEH_ATTR where _RecordDeleted <> 1
),
overlap_result as (
select equipmentNumber, validFromDate, validToDate from cleansed.isu_0UC_DEVICEH_ATTR o where exists(select 1 from overlap_result_temp i where o.equipmentNumber=i.equipmentNumber and i.overlap='yes')
)
MERGE INTO cleansed.isu_0UC_DEVICEH_ATTR
    using overlap_result
    on isu_0UC_DEVICEH_ATTR.equipmentNumber = overlap_result.equipmentNumber
    and isu_0UC_DEVICEH_ATTR.validFromDate = overlap_result.validFromDate
    and isu_0UC_DEVICEH_ATTR.validToDate = overlap_result.validToDate
    WHEN MATCHED THEN UPDATE SET
    _RecordDeleted=-1

-- COMMAND ----------

-- DBTITLE 1,Installation History
with overlap_result_temp as (
select
 *,
 case when
  (validFromDate between (lag(validFromDate,1) over (partition by installationNumber order by validToDate,validFromDate)) and (lag(validToDate,1) over (partition by installationNumber order by validToDate,validFromDate))) or
  (validToDate between (lag(validFromDate,1) over (partition by installationNumber order by validToDate,validFromDate)) and (lag(validToDate,1) over (partition by installationNumber order by validToDate,validFromDate))) or
  (validFromDate < (lag(validFromDate,1) over (partition by installationNumber order by validToDate,validFromDate)) and validToDate > (lag(validToDate,1) over (partition by installationNumber order by validToDate,validFromDate))) or
  (validFromDate >(lag(validFromDate,1) over (partition by installationNumber order by validToDate,validFromDate)) and validToDate < (lag(validToDate,1) over (partition by installationNumber order by validToDate,validFromDate)))
 then 'yes'
 when (lag(validFromDate,1) over (partition by installationNumber order by validToDate,validFromDate)) is null
 then NULL
 else 'no'
 end as overlap
from cleansed.isu_0ucinstallah_attr_2 where _RecordDeleted <> 1
),
overlap_result as (
select installationNumber, validFromDate, validToDate from cleansed.isu_0ucinstallah_attr_2 o where exists(select 1 from overlap_result_temp i where o.installationNumber=i.installationNumber and i.overlap='yes')
)
MERGE INTO cleansed.isu_0ucinstallah_attr_2
    using overlap_result
    on isu_0ucinstallah_attr_2.installationNumber = overlap_result.installationNumber
    and isu_0ucinstallah_attr_2.validFromDate = overlap_result.validFromDate
    and isu_0ucinstallah_attr_2.validToDate = overlap_result.validToDate
    WHEN MATCHED THEN UPDATE SET
    _RecordDeleted=-1

-- COMMAND ----------

-- DBTITLE 1,Contract History
with overlap_result_temp as (
select
 *,
 case when
  (validFromDate between (lag(validFromDate,1) over (partition by contractId order by validToDate,validFromDate)) and (lag(validToDate,1) over (partition by contractId order by validToDate,validFromDate))) or
  (validToDate between (lag(validFromDate,1) over (partition by contractId order by validToDate,validFromDate)) and (lag(validToDate,1) over (partition by contractId order by validToDate,validFromDate))) or
  (validFromDate < (lag(validFromDate,1) over (partition by contractId order by validToDate,validFromDate)) and validToDate > (lag(validToDate,1) over (partition by contractId order by validToDate,validFromDate))) or
  (validFromDate >(lag(validFromDate,1) over (partition by contractId order by validToDate,validFromDate)) and validToDate < (lag(validToDate,1) over (partition by contractId order by validToDate,validFromDate)))
 then 'yes'
 when (lag(validFromDate,1) over (partition by contractId order by validToDate,validFromDate)) is null
 then NULL
 else 'no'
 end as overlap
from cleansed.isu_0uccontracth_attr_2 where _RecordDeleted <> 1
),
overlap_result as (
select contractId, validFromDate, validToDate from cleansed.isu_0uccontracth_attr_2 o where exists(select 1 from overlap_result_temp i where o.contractId=i.contractId and i.overlap='yes')
)
MERGE INTO cleansed.isu_0uccontracth_attr_2
    using overlap_result
    on isu_0uccontracth_attr_2.contractId = overlap_result.contractId
    and isu_0uccontracth_attr_2.validFromDate = overlap_result.validFromDate
    and isu_0uccontracth_attr_2.validToDate = overlap_result.validToDate
    WHEN MATCHED THEN UPDATE SET
    _RecordDeleted=-1
