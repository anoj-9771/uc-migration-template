# Databricks notebook source
# MAGIC %run ../../Common/common-helpers

# COMMAND ----------

# MAGIC %run ../../Common/common-transform

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {get_table_namespace('curated', 'viewFactAssetPerformanceIndex')} AS
(
    select 	assetPerformanceIndexSK
,	assetNumber
,   assetFK
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
,	assetClassType
,	_recordStart
,	_BusinessKey
,	_DLCuratedZoneTimeStamp
,	_recordEnd
,	_recordCurrent
,	_recordDeleted
 from {get_table_namespace(f'{DEFAULT_TARGET}', 'factAssetPerformanceIndex')} where _recordCurrent=1)
""")


# COMMAND ----------


