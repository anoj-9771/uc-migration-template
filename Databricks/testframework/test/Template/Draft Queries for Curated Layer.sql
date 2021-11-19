-- Databricks notebook source
-- DBTITLE 1,DimProperty


-- COMMAND ----------

-- DBTITLE 1,FactBilledWaterConsumption
--ACCESS

--SAP
select 
	b.billingDocumentSK,
	b.propertySK,
	db2.meterSK,
	b.billingPeriodStartDateSK,
	b.billingPeriodEndDateSK,
	b.locationSK,
	-1 as waterNetworkSK,
	sum(db1.billingQuantityPlaceBeforeDecimalPoint) as meteredWaterConsumption
  	
from cleansed.t_sapisu_erch b
join cleansed.t_sapisu_dberchz1 db1 
  on db1.billingDocumentNumber = b.billingDocumentNumber
join cleansed.t_sapisu_dberchz2 db2 
  on db2.billingDocumentNumber = b.billingDocumentNumber and db2.billingDocumentLineItemId = db1.billingDocumentLineItemId
join dimBillingDocument db on db.billingDocumentNumber = b.billingDocumentNumber
join dimProperty dp on dp.propertyNumber=  trim(leading '0', businesspartnerNumber)
join dimMeter dm on dm.meterId = db2.equipmentNumber
join dimDate dd on dd.calendarDate = b.startBillingPeriod
join dimDate dd on dd.calendarDate = b.endBillingPeriod
join dimLocation dl on dl.locationId = b.businesspartnerNumber
and  billingSimulationIndicator='' 
and db1.lineItemTypeCode in ('ZDQUAN', 'ZRQUAN')
and db2.suppressedMeterReadingDocumentID <> ''
and db2.billingLineItemBudgetBillingIndicator is NULL
and b.endbillingPeriod between dp.propertyStartDate and dp.propertyEndDate
