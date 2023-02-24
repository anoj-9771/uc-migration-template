# Databricks notebook source
# MAGIC %sql
# MAGIC 
# MAGIC -- View: viewPropertyRelation
# MAGIC -- Description: viewProprtyRelation
# MAGIC CREATE OR REPLACE VIEW curated_v2.viewPropertyRelation AS
# MAGIC 
# MAGIC With dimpropertyrelationRanges AS
# MAGIC (
# MAGIC                 SELECT
# MAGIC                 property1Number, 
# MAGIC                 property2Number,
# MAGIC                 relationshipTypeCode1,
# MAGIC                 validFromDate,
# MAGIC                 _recordStart, 
# MAGIC                 _recordEnd,
# MAGIC                 COALESCE( LEAD( _recordStart, 1 ) OVER( PARTITION BY 
# MAGIC                                                   property1Number, 
# MAGIC                                                   property2Number,
# MAGIC                                                   relationshipTypeCode1,
# MAGIC                                                   validFromDate ORDER BY _recordStart ), 
# MAGIC                   CASE WHEN _recordEnd < cast('9999-12-31T23:59:59' as timestamp) then _recordEnd + INTERVAL 1 SECOND else _recordEnd end) AS _newRecordEnd
# MAGIC                 FROM curated_v2.dimpropertyrelation
# MAGIC                 WHERE _recordDeleted = 0
# MAGIC ),
# MAGIC 
# MAGIC dateDriver AS
# MAGIC (
# MAGIC                 SELECT 
# MAGIC                 property1Number, 
# MAGIC                 property2Number,
# MAGIC                 relationshipTypeCode1,
# MAGIC                 validFromDate,
# MAGIC                 _recordStart from dimpropertyrelationRanges
# MAGIC                 UNION
# MAGIC                 SELECT 
# MAGIC                 property1Number, 
# MAGIC                 property2Number,
# MAGIC                 relationshipTypeCode1,
# MAGIC                 validFromDate, 
# MAGIC                 _newRecordEnd as _recordStart from dimpropertyrelationRanges
# MAGIC ),
# MAGIC 
# MAGIC effectiveDateRanges AS
# MAGIC (
# MAGIC                 SELECT 
# MAGIC                 property1Number, 
# MAGIC                 property2Number,
# MAGIC                 relationshipTypeCode1,
# MAGIC                 validFromDate, 
# MAGIC                 _recordStart AS _effectiveFrom,
# MAGIC                 COALESCE( LEAD( _recordStart, 1 ) OVER( PARTITION BY 
# MAGIC                                                           property1Number, 
# MAGIC                                                           property2Number,
# MAGIC                                                           relationshipTypeCode1,
# MAGIC                                                           validFromDate ORDER BY _recordStart ) - INTERVAL 1 SECOND, 
# MAGIC                   cast( '9999-12-31T23:59:59' as timestamp ) ) AS _effectiveTo                           
# MAGIC                 FROM dateDriver where _recordStart < cast('9999-12-31T23:59:59' as timestamp)                            
# MAGIC )
# MAGIC 
# MAGIC SELECT * FROM 
# MAGIC (
# MAGIC SELECT
# MAGIC 		dimpropertyrelation.propertyRelationSK
# MAGIC 		,dimpropertyrelation.sourceSystemCode
# MAGIC 		,dimpropertyrelation.property1Number
# MAGIC 		,dimpropertyrelation.property2Number
# MAGIC 		,dimpropertyrelation.validFromDate
# MAGIC 		,dimpropertyrelation.validToDate
# MAGIC 		,dimpropertyrelation.relationshipTypeCode1
# MAGIC 		,dimpropertyrelation.relationshipType1
# MAGIC 		,dimpropertyrelation.relationshipTypeCode2
# MAGIC 		,dimpropertyrelation.relationshipType2
# MAGIC         ,effectiveDateRanges._effectiveFrom
# MAGIC         ,effectiveDateRanges._effectiveTo
# MAGIC     , CASE
# MAGIC       WHEN CURRENT_TIMESTAMP() BETWEEN effectiveDateRanges._effectiveFrom AND effectiveDateRanges._effectiveTo then 'Y'
# MAGIC       ELSE 'N'
# MAGIC       END AS currentFlag
# MAGIC FROM effectiveDateRanges as effectiveDateRanges
# MAGIC LEFT OUTER JOIN curated_v2.dimpropertyrelation
# MAGIC     ON dimpropertyrelation.property1Number = effectiveDateRanges.property1Number 
# MAGIC     	  AND dimpropertyrelation.property2Number = effectiveDateRanges.property2Number
# MAGIC     	  AND dimpropertyrelation.relationshipTypeCode1= effectiveDateRanges.relationshipTypeCode1
# MAGIC     	  AND dimpropertyrelation.validFromDate = effectiveDateRanges.validFromDate
# MAGIC         AND dimpropertyrelation._recordEnd >= effectiveDateRanges._effectiveFrom 
# MAGIC         AND dimpropertyrelation._recordStart <= effectiveDateRanges._effectiveTo
# MAGIC         AND dimpropertyrelation._recordDeleted = 0
# MAGIC )
# MAGIC ORDER BY _effectiveFrom
