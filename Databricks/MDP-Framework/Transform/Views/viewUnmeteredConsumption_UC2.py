# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

DEFAULT_TARGET = 'curated'

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {get_table_namespace(f'{DEFAULT_TARGET}', 'viewUnmeteredConsumptionPropertyType')} (
  unmeteredConsumptionType,
  superiorPropertyTypeCode,
  superiorPropertyType,
  inferiorPropertyTypeCode,
  inferiorPropertyType,
  consumptionQuantity,
  propertyCount,
  reportDate)
AS select 'Unmetered Connected' as unmeteredConsumptionType, dw.superiorPropertyTypeCode, dw.superiorPropertyType, dw.inferiorPropertyTypeCode, dw.inferiorPropertyType, sum(fc.consumptionQuantity) as consumptionQuantity, count(fc.propertyNumber) as propertyCount, fc.reportDate
from {get_table_namespace(f'{DEFAULT_TARGET}', 'factunmeteredconsumption')} fc, {get_table_namespace(f'{DEFAULT_TARGET}', 'dimpropertytypehistory')} dw
where fc.propertyTypeHistorySK = dw.propertyTypeHistorySK
and fc.unmeteredConnectedFlag ='Y'
and fc.reportDate = (select max(reportDate) from {get_table_namespace(f'{DEFAULT_TARGET}', 'factunmeteredconsumption')} where unmeteredConnectedFlag ='Y')
group by fc.unmeteredConnectedFlag, dw.superiorPropertyTypeCode, dw.superiorPropertyType, dw.inferiorPropertyTypeCode, dw.inferiorPropertyType, fc.reportDate
union all
select 'Unmetered Construction' as unmeteredConsumptionType, dw.superiorPropertyTypeCode, dw.superiorPropertyType, dw.inferiorPropertyTypeCode, dw.inferiorPropertyType, sum(fc.consumptionQuantity) as consumptionQuantity, count(fc.propertyNumber) as propertyCount, fc.reportDate
from {get_table_namespace(f'{DEFAULT_TARGET}', 'factunmeteredconsumption')} fc, {get_table_namespace(f'{DEFAULT_TARGET}', 'dimpropertytypehistory')} dw
where fc.propertyTypeHistorySK = dw.propertyTypeHistorySK
and fc.unmeteredconstructionflag ='Y'
and fc.reportDate = (select max(reportDate) from {get_table_namespace(f'{DEFAULT_TARGET}', 'factunmeteredconsumption')} where unmeteredconstructionflag ='Y')
group by fc.unmeteredconstructionflag, dw.superiorPropertyTypeCode, dw.superiorPropertyType, dw.inferiorPropertyTypeCode, dw.inferiorPropertyType, fc.reportDate
order by 1 asc, dw.superiorPropertyTypeCode, dw.superiorPropertyType, dw.inferiorPropertyTypeCode, dw.inferiorPropertyType, fc.reportDate
""")


# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {get_table_namespace(f'{DEFAULT_TARGET}', 'viewUnmeteredConsumptionWaterNetwork')} (
  unmeteredConsumptionType,
  deliverySystem,
  distributionSystem,
  supplyZone,
  pressureArea,
  propertyCount,
  consumptionQuantity,
  reportDate)
AS select 'Unmetered Connected' as unmeteredConsumptionType, dw.deliverySystem, dw.distributionSystem, dw.supplyZone, dw.pressureArea, fc.propertyCount, fc.consumptionQuantity, fc.reportDate
from {get_table_namespace(f'{DEFAULT_TARGET}', 'factconsumptionaggregate')} fc, {get_table_namespace(f'{DEFAULT_TARGET}', 'dimwaterNetwork')} dw
where fc.waterNetworkSK = dw.waterNetworkSK
and fc.unmeteredConnectedFlag ='Y'
and fc.reportDate = (select max(reportDate) from {get_table_namespace(f'{DEFAULT_TARGET}', 'factconsumptionaggregate')} where unmeteredConnectedFlag ='Y')
union all
select 'Unmetered Construction' as unmeteredConsumptionType, dw.deliverySystem, dw.distributionSystem, dw.supplyZone, dw.pressureArea, fc.propertyCount, fc.consumptionQuantity, fc.reportDate
from {get_table_namespace(f'{DEFAULT_TARGET}', 'factconsumptionaggregate')} fc, {get_table_namespace(f'{DEFAULT_TARGET}', 'dimwaterNetwork')} dw
where fc.waterNetworkSK = dw.waterNetworkSK
and fc.unmeteredConstructionFlag ='Y'
and fc.reportDate = (select max(reportDate) from {get_table_namespace(f'{DEFAULT_TARGET}', 'factconsumptionaggregate')} where unmeteredConstructionFlag ='Y')
order by 1 asc, dw.deliverySystem, dw.distributionSystem, dw.supplyZone, dw.pressureArea, fc.reportDate
""")


# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {get_table_namespace(f'{DEFAULT_TARGET}', 'viewUnmeteredConsumptionSystemLevel')} (
  unmeteredConsumptionType,
  consumptionQuantity,
  propertyCount,
  reportDate)
AS select unmeteredConsumptionType, case when unmeteredConsumptionType = 'Unmetered Connected' then sum(consumptionQuantity)+1000 else sum(consumptionQuantity) end as consumptionQuantity, sum(propertyCount) as propertyCount, reportDate
from {get_table_namespace(f'{DEFAULT_TARGET}', 'viewUnmeteredConsumptionWaterNetwork')}
group by UnmeteredConsumptionType, reportDate
""")

