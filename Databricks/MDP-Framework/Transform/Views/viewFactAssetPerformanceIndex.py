# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW curated_v3.viewFactAssetPerformanceIndex AS
(
    select 	assetPerformanceIndexSK
,	assetNumber
,	reportingYear
,	reportingMonth
,	calculationTypeCode
,	orderID
,	TO_DATE(selectedPeriodStartDate) AS selectedPeriodStartDate
,	TO_DATE(selectedPeriodEndDate) AS selectedPeriodEndDate
,	TO_DATE(comparisonPeriodStartDate) AS comparisonPeriodStartDate
,	TO_DATE(comparisonPeriodEndDate) AS comparisonPeriodEndDate
,	selectedPeriodBreakdownMaintenanceCount
,	comparisonPeriodBreakdownMaintenanceCount
,	yearlyAverageBreakdownMaintenanceCount
,	selectedPeriodFailedAssetsCount
,	selectedPeriodRepeatedlyFailedAssetsCount
,	CLASSTYPE
,	_recordStart
,	_BusinessKey
,	_DLCuratedZoneTimeStamp
,	_recordEnd
,	_recordCurrent
,	_recordDeleted
 from {DEFAULT_TARGET}.factAssetPerformanceIndex where _recordCurrent=1)
""")


# COMMAND ----------


