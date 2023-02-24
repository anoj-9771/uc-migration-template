# Databricks notebook source
# MAGIC %sql
# MAGIC 
# MAGIC create or replace view curated_v2.viewProperty AS
# MAGIC 
# MAGIC With dimPropertyDateRanges AS
# MAGIC (
# MAGIC                 SELECT
# MAGIC                 propertyNumber, 
# MAGIC                 _recordStart, 
# MAGIC                 _recordEnd,
# MAGIC                 COALESCE( LEAD( _recordStart, 1 ) OVER( PARTITION BY propertyNumber ORDER BY _recordStart ), 
# MAGIC                   CASE WHEN _recordEnd < cast('9999-12-31T23:59:59' as timestamp) then _recordEnd + INTERVAL 1 SECOND else _recordEnd end) AS _newRecordEnd
# MAGIC                 FROM curated_v2.dimProperty
# MAGIC                 --WHERE _recordDeleted = 0
# MAGIC ),
# MAGIC 
# MAGIC dimPropertyTypeHistoryDateRanges AS
# MAGIC (
# MAGIC                 SELECT
# MAGIC                 propertyNumber, 
# MAGIC                 _recordStart, 
# MAGIC                 _recordEnd,
# MAGIC                 COALESCE( LEAD( _recordStart, 1 ) OVER( PARTITION BY propertyNumber ORDER BY _recordStart ), 
# MAGIC                   CASE WHEN _recordEnd < cast('9999-12-31T23:59:59' as timestamp) then _recordEnd + INTERVAL 1 SECOND else _recordEnd end) AS _newRecordEnd
# MAGIC                 FROM curated_v2.dimPropertyTypeHistory
# MAGIC                 WHERE _recordDeleted = 0
# MAGIC ),
# MAGIC 
# MAGIC dimPropertyLotDateRanges AS
# MAGIC (
# MAGIC                 SELECT
# MAGIC                 propertyNumber, 
# MAGIC                 _recordStart, 
# MAGIC                 _recordEnd,
# MAGIC                 COALESCE( LEAD( _recordStart, 1 ) OVER( PARTITION BY propertyNumber ORDER BY _recordStart ), 
# MAGIC                   CASE WHEN _recordEnd < cast('9999-12-31T23:59:59' as timestamp) then _recordEnd + INTERVAL 1 SECOND else _recordEnd end) AS _newRecordEnd
# MAGIC                 FROM curated_v2.dimPropertyLot
# MAGIC                 --WHERE _recordDeleted = 0
# MAGIC ),
# MAGIC 
# MAGIC dimLocationDateRanges AS
# MAGIC (
# MAGIC                 SELECT
# MAGIC                 locationID as propertyNumber, 
# MAGIC                 _recordStart, 
# MAGIC                 _recordEnd,
# MAGIC                 COALESCE( LEAD( _recordStart, 1 ) OVER( PARTITION BY locationID ORDER BY _recordStart ), 
# MAGIC                   CASE WHEN _recordEnd < cast('9999-12-31T23:59:59' as timestamp) then _recordEnd + INTERVAL 1 SECOND else _recordEnd end) AS _newRecordEnd
# MAGIC                 FROM curated_v2.dimLocation
# MAGIC                 --WHERE _recordDeleted = 0
# MAGIC ),
# MAGIC 
# MAGIC dateDriver AS
# MAGIC (
# MAGIC                 SELECT propertyNumber, _recordStart from dimPropertyDateRanges
# MAGIC                 UNION
# MAGIC                 SELECT propertyNumber, _newRecordEnd as _recordStart from dimPropertyDateRanges
# MAGIC                 UNION
# MAGIC                 SELECT propertyNumber, _recordStart from dimPropertyTypeHistoryDateRanges
# MAGIC                 UNION
# MAGIC                 SELECT propertyNumber, _newRecordEnd as _recordStart from dimPropertyTypeHistoryDateRanges
# MAGIC                 UNION
# MAGIC                 SELECT propertyNumber, _recordStart from dimPropertyLotDateRanges
# MAGIC                 UNION
# MAGIC                 SELECT propertyNumber, _newRecordEnd as _recordStart from dimPropertyLotDateRanges
# MAGIC                 UNION
# MAGIC                 SELECT propertyNumber, _recordStart from dimLocationDateRanges
# MAGIC                 UNION
# MAGIC                 SELECT propertyNumber, _newRecordEnd as _recordStart from dimLocationDateRanges
# MAGIC ),
# MAGIC 
# MAGIC effectiveDateRanges AS
# MAGIC (
# MAGIC                 SELECT 
# MAGIC                 propertyNumber, 
# MAGIC                 _recordStart AS _effectiveFrom,
# MAGIC                 COALESCE( LEAD( _recordStart, 1 ) OVER( PARTITION BY propertyNumber ORDER BY _recordStart ) - INTERVAL 1 SECOND, 
# MAGIC                   cast( '9999-12-31T23:59:59' as timestamp ) ) AS _effectiveTo                           
# MAGIC                 FROM dateDriver where _recordStart < cast('9999-12-31T23:59:59' as timestamp)                            
# MAGIC )           
# MAGIC 
# MAGIC 
# MAGIC select * from (
# MAGIC select
# MAGIC     dimProperty.propertySK,
# MAGIC     dimPropertyTypeHistory.propertyTypeHistorySK,
# MAGIC     coalesce(dimProperty.sourceSystemCode, dimPropertyTypeHistory.sourceSystemCode, dimPropertyLot.sourceSystemCode, dimLocation.sourceSystemCode) as sourceSystemCode,
# MAGIC     coalesce(dimProperty.propertyNumber, dimPropertyTypeHistory.propertyNumber, dimPropertyLot.propertyNumber, dimLocation.locationID, -1) as propertyNumber,
# MAGIC     dimProperty.premise,
# MAGIC     dimProperty.waterNetworkSK_drinkingWater,
# MAGIC     dimProperty.waterNetworkSK_recycledWater,
# MAGIC     dimProperty.sewerNetworkSK,
# MAGIC     dimProperty.stormWaterNetworkSK,
# MAGIC     dimProperty.objectNumber,
# MAGIC     dimProperty.propertyInfo,
# MAGIC     dimProperty.buildingFeeDate,
# MAGIC     dimProperty.connectionValidFromDate,
# MAGIC     dimProperty.CRMConnectionObjectGUID,
# MAGIC     dimProperty.architecturalObjectNumber,
# MAGIC     dimProperty.architecturalObjectTypeCode,
# MAGIC     dimProperty.architecturalObjectType,
# MAGIC     dimProperty.parentArchitecturalObjectNumber,
# MAGIC     dimProperty.parentArchitecturalObjectTypeCode,
# MAGIC     dimProperty.parentArchitecturalObjectType,
# MAGIC     dimProperty.hydraBand,
# MAGIC     dimProperty.hydraCalculatedArea,
# MAGIC     dimProperty.hydraAreaUnit,
# MAGIC     dimProperty.overrideArea,
# MAGIC     dimProperty.overrideAreaUnit,
# MAGIC     dimProperty.stormWaterAssessmentFlag,
# MAGIC     dimProperty.hydraAreaFlag,
# MAGIC     dimProperty.comments,
# MAGIC     dimPropertyLot.propertyLotSK,
# MAGIC     dimPropertyLot.planTypeCode,
# MAGIC     dimPropertyLot.planType,
# MAGIC     dimPropertyLot.planNumber,
# MAGIC     dimPropertyLot.lotTypeCode,
# MAGIC     dimPropertyLot.lotType,
# MAGIC     dimPropertyLot.lotNumber,
# MAGIC     dimPropertyLot.sectionNumber,
# MAGIC     dimPropertyLot.latitude,
# MAGIC     dimPropertyLot.longitude,
# MAGIC     dimPropertyTypeHistory.superiorPropertyTypeCode,
# MAGIC     dimPropertyTypeHistory.superiorPropertyType,
# MAGIC     dimPropertyTypeHistory.inferiorPropertyTypeCode,
# MAGIC     dimPropertyTypeHistory.inferiorPropertyType,
# MAGIC     dimLocation.locationSK,
# MAGIC     dimLocation.formattedAddress,
# MAGIC     dimLocation.addressNumber,
# MAGIC     dimLocation.buildingName1,
# MAGIC     dimLocation.buildingName2,
# MAGIC     dimLocation.unitDetails,
# MAGIC     dimLocation.floorNumber,
# MAGIC     dimLocation.houseNumber,
# MAGIC     dimLocation.lotDetails,
# MAGIC     dimLocation.streetName,
# MAGIC     dimLocation.streetLine1,
# MAGIC     dimLocation.streetLine2,
# MAGIC     dimLocation.suburb,
# MAGIC     dimLocation.streetCode,
# MAGIC     dimLocation.cityCode,
# MAGIC     dimLocation.postCode,
# MAGIC     dimLocation.stateCode,
# MAGIC     dimLocation.LGA,
# MAGIC 	dimPropertyTypeHistory.ValidFromDate as propertyTypeValidFromDate,
# MAGIC 	dimPropertyTypeHistory.ValidToDate as propertyTypeValidToDate,
# MAGIC 	effectiveDateRanges._effectiveFrom,
# MAGIC 	effectiveDateRanges._effectiveTo,
# MAGIC     CASE
# MAGIC       WHEN CURRENT_TIMESTAMP() BETWEEN effectiveDateRanges._effectiveFrom AND effectiveDateRanges._effectiveTo then 'Y'
# MAGIC       ELSE 'N'
# MAGIC       END AS currentFlag
# MAGIC from effectiveDateRanges as effectiveDateRanges
# MAGIC left outer join curated_v2.dimProperty dimProperty
# MAGIC         on dimproperty.propertynumber = effectiveDateRanges.propertyNumber 
# MAGIC 		and dimProperty._recordEnd >= effectiveDateRanges._effectiveFrom 
# MAGIC 		and dimProperty._recordStart <= effectiveDateRanges._effectiveTo
# MAGIC         --AND dimProperty._recordDeleted = 0
# MAGIC left outer join curated_v2.dimPropertyTypeHistory dimPropertyTypeHistory 
# MAGIC         on dimPropertyTypeHistory.propertynumber = effectiveDateRanges.propertyNumber 
# MAGIC 		and dimPropertyTypeHistory.validToDate >= effectiveDateRanges._effectiveFrom 
# MAGIC 		and dimPropertyTypeHistory.validFromDate <= effectiveDateRanges._effectiveTo
# MAGIC         AND dimPropertyTypeHistory._recordDeleted = 0
# MAGIC left outer join curated_v2.dimPropertyLot dimPropertyLot
# MAGIC         on dimPropertyLot.propertynumber = effectiveDateRanges.propertyNumber 
# MAGIC 		and dimPropertyLot._recordEnd >= effectiveDateRanges._effectiveFrom 
# MAGIC 		and dimPropertyLot._recordStart <= effectiveDateRanges._effectiveTo
# MAGIC         --AND dimPropertyLot._recordDeleted = 0
# MAGIC left outer join curated_v2.dimLocation dimLocation
# MAGIC         on dimLocation.locationID = effectiveDateRanges.propertyNumber 
# MAGIC 		and dimLocation._recordEnd >= effectiveDateRanges._effectiveFrom 
# MAGIC 		and dimLocation._recordStart <= effectiveDateRanges._effectiveTo 
# MAGIC         --AND dimLocation._recordDeleted = 0 
# MAGIC ) ORDER BY _effectiveFrom;
