-- Databricks notebook source
-- DBTITLE 1,DimProperty


-- COMMAND ----------

-- DBTITLE 1,FactBilledWaterConsumption
--ACCESS
select
  -1 as billingDocumentNumber,
  e.propertySK,
  c.meterSK,
  a.billingPeriodStartDateSK,
  a.billingPeriodEndDateSK,
  e.locationSK,
  -1 as waterNetworkSK,
  sum(a.consumption) as billedWaterConsumption
from
  cleansed.t_access_z309_tmetereading a
  JOIN cleansed.t_access_z309_tpropmeter c ON a.propertyNumber = c.propertyNumber
  and a.propertyMeterNumber = c.propertyMeterNumber
  JOIN cleansed.t_access_z309_tproperty e ON a.propertyNumber = e.propertyNumber
  AND A.meterReadingStatusCode IN ('A', 'B', 'P', 'V') -- BILLED, PROCESSED & AMENDED METER READINGS
  c.meterClassCode IN ('01','04','05','06','07','08','17','19','20','21') -- POTABLE WATER ONLY
  AND not c.isCheckMeter -- EXCLUDE CHECK METERS
  a.meterReadingDays > 0 -- 9 out of ten times initial reads, else manual error
  AND a.meterReadingNumber = (
    SELECT
      max(b.meterReadingNumber) -- MAXIMUM METER READING
    FROM
      cleansed.t_access_z309_tmetereading b
    WHERE
      a.propertyNumber = b.propertyNumber
      AND a.propertyMeterNumber = b.propertyMeterNumber
      AND b.meterreadingStatusCode IN ('A', 'B', 'P', 'V') -- BILLED, PROCESSED & AMENDED METER READINGS
      AND a.readingFromDate = b.readingFromDate
  )
  AND a.meterReadingNumber = (
    SELECT
      max(b.meterReadingNumber) -- MAXIMUM METER READING
    FROM
      cleansed.t_access_z309_tmetereading b
    WHERE
      a.propertyNumber = b.propertyNumber
      AND a.propertyMeterNumber = b.propertyMeterNumber
      AND b.meterReadingStatusCode IN ('A', 'B', 'P', 'V') -- BILLED, PROCESSED & AMENDED METER READINGS
      AND a.readingToDate = b.readingToDate
  )
  AND NOT EXISTS (
    SELECT 1 -- EXCLUDE DEBIT TYPE AND
    FROM
      cleansed.t_access_z309_tdebit H --REASONS FOR RE-USE WATER
    WHERE
      A.propertyNumber = H.propertyNumber -- AND SYDNEY CATCHMENT AUTHORITY PROPERTIES
      AND H.debitTypeCode = '10'
      AND H.debitReasonCode IN ('360', '367')
  ) 
  --JOIN dimBillingDocument db on db.billingDocumentNumber = b.billingDocumentNumber ---No source attribute mentioned in the mapping doc. do we need this????
  join dimProperty dp on dp.propertyNumber = e.propertyNumber 
  --AND b.endbillingPeriod between dp.propertyStartDate and dp.propertyEndDate --shouldn't we include this condition as mentioned by Gulsen????
  JOIN dimMeter dm ON dm.meterId = c.meterMakerNumber
  JOIN dimDate dd ON on dd.calendarDate = b.readingFromDate
  and dd.calendarDate = b.readingToDate
  join dimLocation dl on dl.locationId = e.propertyNumber 
  --AND   c.meterMakerNumber NOT IN ('00256','94015676','00071650','6HD07195')   -- EXCLUDE UNFILTERED METERS WITH  A POTABLE METER CLASS
  -- and   a.meterReadingTimestamp >= to_timestamp(to_date('20171001','yyyymmdd'))
  --group by a.readingFromDate,ca.readingToDate,cc.meterMakerNumber,e.propertyNumber
UNION ALL
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
from
  cleansed.t_sapisu_erch b
  join cleansed.t_sapisu_dberchz1 db1 on db1.billingDocumentNumber = b.billingDocumentNumber
  and db1.lineItemTypeCode in ('ZDQUAN', 'ZRQUAN')
  join cleansed.t_sapisu_dberchz2 db2 on db2.billingDocumentNumber = b.billingDocumentNumber
  and db2.billingDocumentLineItemId = db1.billingDocumentLineItemId
  and db2.suppressedMeterReadingDocumentID <> ''
  and db2.billingLineItemBudgetBillingIndicator is NULL
  join dimBillingDocument db on db.billingDocumentNumber = b.billingDocumentNumber
  join dimProperty dp on dp.propertyNumber = trim(leading '0', b.businesspartnerNumber)
  and b.endbillingPeriod between dp.propertyStartDate
  and dp.propertyEndDate
  join dimMeter dm on dm.meterId = db2.equipmentNumber
  join dimDate dd on dd.calendarDate = b.startBillingPeriod
  and dd.calendarDate = b.endBillingPeriod
  join dimLocation dl on dl.locationId = b.businesspartnerNumber
  and b.billingSimulationIndicator = ''
