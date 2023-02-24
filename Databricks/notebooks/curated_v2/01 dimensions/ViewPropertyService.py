# Databricks notebook source
# MAGIC %sql
# MAGIC 
# MAGIC -- View: viewPropertyService
# MAGIC -- Description: viewProprtyService
# MAGIC create or replace view curated_v2.viewPropertyService
# MAGIC as
# MAGIC 
# MAGIC With dimPropertyServiceRanges AS
# MAGIC (
# MAGIC                 SELECT
# MAGIC                 propertyNumber, 
# MAGIC                 validToDate,
# MAGIC                 fixtureAndFittingCharacteristicCode,
# MAGIC                 _recordStart, 
# MAGIC                 _recordEnd,
# MAGIC                 COALESCE( LEAD( _recordStart, 1 ) OVER( PARTITION BY 
# MAGIC                                                   propertyNumber, 
# MAGIC                                                   validToDate,
# MAGIC                                                   fixtureAndFittingCharacteristicCode 
# MAGIC                                                   ORDER BY _recordStart ), 
# MAGIC                   CASE WHEN _recordEnd < cast('9999-12-31T23:59:59' as timestamp) then _recordEnd + INTERVAL 1 SECOND else _recordEnd end) AS _newRecordEnd
# MAGIC                 FROM curated_v2.dimPropertyService
# MAGIC                 WHERE _recordDeleted = 0
# MAGIC ),
# MAGIC 
# MAGIC dateDriver AS
# MAGIC (
# MAGIC                 SELECT 
# MAGIC                 propertyNumber, 
# MAGIC                 validToDate,
# MAGIC                 fixtureAndFittingCharacteristicCode,
# MAGIC                 _recordStart from dimPropertyServiceRanges
# MAGIC                 UNION
# MAGIC                 SELECT 
# MAGIC                 propertyNumber, 
# MAGIC                 validToDate,
# MAGIC                 fixtureAndFittingCharacteristicCode, 
# MAGIC                 _newRecordEnd as _recordStart from dimPropertyServiceRanges
# MAGIC ),
# MAGIC 
# MAGIC effectiveDateRanges AS
# MAGIC (
# MAGIC                 SELECT 
# MAGIC                 propertyNumber, 
# MAGIC                 validToDate,
# MAGIC                 fixtureAndFittingCharacteristicCode,
# MAGIC                 _recordStart AS _effectiveFrom,
# MAGIC                 COALESCE( LEAD( _recordStart, 1 ) OVER( PARTITION BY 
# MAGIC                                           propertyNumber, 
# MAGIC                                           validToDate,
# MAGIC                                           fixtureAndFittingCharacteristicCode ORDER BY _recordStart ) - INTERVAL 1 SECOND, 
# MAGIC                   cast( '9999-12-31T23:59:59' as timestamp ) ) AS _effectiveTo                           
# MAGIC                 FROM dateDriver where _recordStart < cast('9999-12-31T23:59:59' as timestamp)                            
# MAGIC )              
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
# MAGIC         ,effectiveDateRanges._effectiveFrom
# MAGIC         ,effectiveDateRanges._effectiveTo
# MAGIC     , CASE
# MAGIC       WHEN CURRENT_TIMESTAMP() BETWEEN effectiveDateRanges._effectiveFrom AND effectiveDateRanges._effectiveTo then 'Y'
# MAGIC       ELSE 'N'
# MAGIC       END AS currentFlag
# MAGIC FROM effectiveDateRanges as effectiveDateRanges
# MAGIC LEFT OUTER JOIN curated_v2.dimpropertyservice
# MAGIC     ON dimpropertyservice.propertyNumber = effectiveDateRanges.propertyNumber
# MAGIC     	  AND dimpropertyservice.validToDate= effectiveDateRanges.validToDate
# MAGIC     	  AND dimpropertyservice.fixtureAndFittingCharacteristicCode = effectiveDateRanges.fixtureAndFittingCharacteristicCode
# MAGIC         AND dimpropertyservice._recordEnd >= effectiveDateRanges._effectiveFrom 
# MAGIC         AND dimpropertyservice._recordStart <= effectiveDateRanges._effectiveTo
# MAGIC         AND dimpropertyservice._recordDeleted = 0
# MAGIC         where dimpropertyservice.fixtureAndFittingCharacteristicCode NOT IN ('Unknown','ZDW1','ZDW2','ZPW1','ZPW2','ZPW3','ZPW4','ZRW1','ZRW2','ZRW3','ZWW1','ZWW2','ZWW3')
# MAGIC )
# MAGIC ORDER BY _effectiveFrom
