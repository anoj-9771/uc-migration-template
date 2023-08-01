# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

DEFAULT_TARGET = 'curated'

# COMMAND ----------

runflag = spark.sql(f"""
          select * from (select first_value(calendarDate) over (order by calendarDate asc) as businessDay from {get_env()}curated.dim.date where month(calendarDate) = month(current_date()) and year(calendarDate) = year(current_date()) and isWeekDayFlag = 'Y' and coalesce(isHolidayFlag,'N') = 'N' limit 1) where businessDay = current_date()""")
 
if runflag.count() == 0:
    # print("Skipping - Runs only on first business day of the month")
    dbutils.notebook.exit('{"Counts": {"SpotCount": 0}}')

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {get_table_namespace(f'{DEFAULT_TARGET}', 'viewUnmeteredConsumptionPropertyType')} 
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
CREATE OR REPLACE TABLE {get_table_namespace(f'{DEFAULT_TARGET}', 'viewUnmeteredConsumptionWaterNetwork')} 
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
CREATE OR REPLACE TABLE {get_table_namespace(f'{DEFAULT_TARGET}', 'viewUnmeteredConsumptionSystemLevel')} 
AS select unmeteredConsumptionType, sum(consumptionQuantity) as consumptionQuantity, case when unmeteredConsumptionType = 'Unmetered Connected' then 1000 else NULL end as publicReservesAllocation, sum(propertyCount) as propertyCount, reportDate
from {get_table_namespace(f'{DEFAULT_TARGET}', 'viewUnmeteredConsumptionWaterNetwork')}
group by UnmeteredConsumptionType, reportDate
""")

