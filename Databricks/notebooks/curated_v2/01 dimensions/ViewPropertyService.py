# Databricks notebook source
# MAGIC %sql
# MAGIC 
# MAGIC -- View: viewPropertyService
# MAGIC -- Description: viewProprtyService
# MAGIC create or replace view curated_v2.viewPropertyService
# MAGIC as
# MAGIC 
# MAGIC SELECT * FROM 
# MAGIC (
# MAGIC SELECT
# MAGIC 		dimpropertyservice.propertyServiceSK
# MAGIC 		,dimpropertyservice.sourceSystemCode
# MAGIC 		,dimpropertyservice.propertyNumber
# MAGIC 		,dimpropertyservice.architecturalObjectInternalId
# MAGIC 		,dimpropertyservice.validToDate
# MAGIC 		,dimpropertyservice.validFromDate
# MAGIC 		,dimpropertyservice.fixtureAndFittingCharacteristicCode
# MAGIC 		,dimpropertyservice.fixtureAndFittingCharacteristic
# MAGIC         ,dimpropertyservice.supplementInfo
# MAGIC         ,dimpropertyservice._RecordStart as _effectiveFrom
# MAGIC         ,dimpropertyservice._RecordEnd as _effectiveTo
# MAGIC     , CASE
# MAGIC       WHEN CURRENT_TIMESTAMP() BETWEEN dimpropertyservice._RecordStart AND dimpropertyservice._RecordEnd then 'Y'
# MAGIC       ELSE 'N'
# MAGIC       END AS currentFlag,
# MAGIC       'Y' AS currentRecordFlag 
# MAGIC FROM curated_v2.dimpropertyservice
# MAGIC         where dimpropertyservice._recordDeleted = 0
# MAGIC         and dimpropertyservice.fixtureAndFittingCharacteristicCode NOT IN ('Unknown','ZDW1','ZDW2','ZPW1','ZPW2','ZPW3','ZPW4','ZRW1','ZRW2','ZRW3','ZWW1','ZWW2','ZWW3')
# MAGIC )
# MAGIC ORDER BY _effectiveFrom
