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
from {get_table_namespace(f'{DEFAULT_TARGET}', 'factunmeteredconsumption')} fc left outer join {get_table_namespace(f'{DEFAULT_TARGET}', 'dimpropertytypehistory')} dw on fc.propertyTypeHistorySK = dw.propertyTypeHistorySK
where fc.unmeteredConnectedFlag ='Y'
and fc.reportDate = (select max(reportDate) from {get_table_namespace(f'{DEFAULT_TARGET}', 'factunmeteredconsumption')} where unmeteredConnectedFlag ='Y')
group by fc.unmeteredConnectedFlag, dw.superiorPropertyTypeCode, dw.superiorPropertyType, dw.inferiorPropertyTypeCode, dw.inferiorPropertyType, fc.reportDate
union all
select 'Unmetered Construction' as unmeteredConsumptionType, dw.superiorPropertyTypeCode, dw.superiorPropertyType, dw.inferiorPropertyTypeCode, dw.inferiorPropertyType, sum(fc.consumptionQuantity) as consumptionQuantity, count(fc.propertyNumber) as propertyCount, fc.reportDate
from {get_table_namespace(f'{DEFAULT_TARGET}', 'factunmeteredconsumption')} fc left outer join {get_table_namespace(f'{DEFAULT_TARGET}', 'dimpropertytypehistory')} dw on fc.propertyTypeHistorySK = dw.propertyTypeHistorySK
where fc.unmeteredconstructionflag ='Y'
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
from {get_table_namespace(f'{DEFAULT_TARGET}', 'factconsumptionaggregate')} fc left outer join {get_table_namespace(f'{DEFAULT_TARGET}', 'dimwaterNetwork')} dw on fc.waterNetworkSK = dw.waterNetworkSK
where fc.unmeteredConnectedFlag ='Y'
and fc.reportDate = (select max(reportDate) from {get_table_namespace(f'{DEFAULT_TARGET}', 'factconsumptionaggregate')} where unmeteredConnectedFlag ='Y')
union all
select 'Unmetered Construction' as unmeteredConsumptionType, dw.deliverySystem, dw.distributionSystem, dw.supplyZone, dw.pressureArea, fc.propertyCount, fc.consumptionQuantity, fc.reportDate
from {get_table_namespace(f'{DEFAULT_TARGET}', 'factconsumptionaggregate')} fc left outer join {get_table_namespace(f'{DEFAULT_TARGET}', 'dimwaterNetwork')} dw on fc.waterNetworkSK = dw.waterNetworkSK
where fc.unmeteredConstructionFlag ='Y'
and fc.reportDate = (select max(reportDate) from {get_table_namespace(f'{DEFAULT_TARGET}', 'factconsumptionaggregate')} where unmeteredConstructionFlag ='Y')
order by 1 asc, dw.deliverySystem, dw.distributionSystem, dw.supplyZone, dw.pressureArea, fc.reportDate
""")


# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {get_table_namespace(f'{DEFAULT_TARGET}', 'viewUnmeteredConsumptionSystemLevel')} (
  unmeteredConsumptionType,
  consumptionQuantity,
  publicReservesAllocation,
  propertyCount,
  reportDate)
AS select unmeteredConsumptionType, sum(consumptionQuantity), case when unmeteredConsumptionType = 'Unmetered Connected' then 1000 else NULL end as publicReservesAllocation, sum(propertyCount) as propertyCount, reportDate
from {get_table_namespace(f'{DEFAULT_TARGET}', 'viewUnmeteredConsumptionWaterNetwork')}
group by UnmeteredConsumptionType, reportDate
""")

