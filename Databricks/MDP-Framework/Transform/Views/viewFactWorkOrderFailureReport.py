# Databricks notebook source
# MAGIC %run ../../Common/common-helpers

# COMMAND ----------

# MAGIC %run ../../Common/common-transform

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {get_table_namespace(f'{DEFAULT_TARGET}', 'viewFactWorkOrderFailureReport')} AS
(
    select 	workOrderFailureReportSK
,	workOrderFailureReportId
,	TO_DATE(workOrderFailureReportChangeTimestamp) as workOrderFailureReportChangeDate
,	assetFK
,	workOrderFK
,	workOrderFailureReportTicketIdentifier
,	workOrderFailureReportTicketClass
,	workOrderFailureReportCode
,	workOrderFailureReportDescription
,	workOrderFailureReportListId
,	workOrderFailureReportType
,	workOrderFailureReportRowIdentifer
,   snapshotDate
,	_recordStart
,	_BusinessKey
,	_DLCuratedZoneTimeStamp
,	_recordEnd
,	_recordCurrent
,	_recordDeleted
 from {get_table_namespace(f'{DEFAULT_TARGET}', 'factWorkOrderFailureReport')} where _recordCurrent=1)
""")

