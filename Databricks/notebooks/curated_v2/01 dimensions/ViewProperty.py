# Databricks notebook source
# MAGIC %sql
# MAGIC 
# MAGIC create or replace view curated_v2.viewProperty
# MAGIC as (
# MAGIC with dateDriverNonDelete as
# MAGIC (
# MAGIC 	select distinct propertyNumber, to_date(_recordStart) as _effectiveFrom from curated_v2.dimProperty where isnotnull(propertyNumber) and _RecordDeleted = 0 
# MAGIC 	union
# MAGIC 	select distinct propertyNumber,to_date(validFromDate) as _effectiveFrom from curated_v2.dimPropertyTypeHistory  where isnotnull(propertyNumber) and  _RecordDeleted = 0
# MAGIC     union
# MAGIC     select distinct propertyNumber, to_date(_recordStart) as _effectiveFrom from curated_v2.dimPropertyLot where isnotnull(propertyNumber) and _RecordDeleted = 0
# MAGIC     union
# MAGIC     select distinct locationID as propertyNumber, to_date(_recordStart) as _effectiveFrom from curated_v2.dimLocation where isnotnull(locationID) and  _RecordDeleted = 0
# MAGIC ),
# MAGIC effectiveDaterangesNonDelete as 
# MAGIC (
# MAGIC 	select propertyNumber, _effectiveFrom, coalesce(timestamp(date_add(lead(_effectiveFrom,1) over(partition by propertyNumber order by _effectiveFrom), -1)), '9999-12-31') as _effectiveTo
# MAGIC 	from dateDriverNonDelete 
# MAGIC ),
# MAGIC dateDriverDelete as
# MAGIC (
# MAGIC 	select distinct propertyNumber, to_date(_recordStart) as _effectiveFrom from curated_v2.dimProperty where isnotnull(propertyNumber) and _RecordDeleted = 1 
# MAGIC 	union
# MAGIC 	select distinct propertyNumber,to_date(validFromDate) as _effectiveFrom from curated_v2.dimPropertyTypeHistory  where isnotnull(propertyNumber) and  _RecordDeleted = 1
# MAGIC     union
# MAGIC     select distinct propertyNumber, to_date(_recordStart) as _effectiveFrom from curated_v2.dimPropertyLot where isnotnull(propertyNumber) and _RecordDeleted = 1
# MAGIC     union
# MAGIC     select distinct locationID as propertyNumber, to_date(_recordStart) as _effectiveFrom from curated_v2.dimLocation where isnotnull(locationID) and  _RecordDeleted = 1
# MAGIC ),
# MAGIC effectiveDaterangesDelete as 
# MAGIC (
# MAGIC 	select propertyNumber, _effectiveFrom, coalesce(timestamp(date_add(lead(_effectiveFrom,1) over(partition by propertyNumber order by _effectiveFrom), -1)), '9999-12-31') as _effectiveTo
# MAGIC 	from dateDriverDelete 
# MAGIC )
# MAGIC select * from (
# MAGIC select
# MAGIC     dimProperty.propertySK,
# MAGIC     dimPropertyTypeHistory.propertyTypeHistorySK,
# MAGIC     dimProperty.sourceSystemCode,
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
# MAGIC     dimPropertyLot.planTypeCode,
# MAGIC     dimPropertyLot.planType,
# MAGIC     dimPropertyLot.planNumber,
# MAGIC     dimPropertyLot.lotTypeCode,
# MAGIC     dimPropertyLot.lotType,
# MAGIC     dimPropertyLot.lotNumber,
# MAGIC     dimPropertyLot.sectionNumber,
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
# MAGIC     dimLocation.LGACode,
# MAGIC     dimLocation.LGA,
# MAGIC     dimLocation.latitude,
# MAGIC     dimLocation.longitude,
# MAGIC 	dimPropertyTypeHistory.ValidFromDate as propertyTypeValidFromDate,
# MAGIC 	dimPropertyTypeHistory.ValidToDate as propertyTypeValidToDate,
# MAGIC 	cast(effectiveDateRanges._effectiveFrom as date),
# MAGIC 	cast(effectiveDateRanges._effectiveTo as date),
# MAGIC     dimProperty._recordDeleted as _dimPropertyRecordDeleted,
# MAGIC     dimPropertyTypeHistory._recordDeleted as _dimPropertyTypeHistoryRecordDeleted,
# MAGIC     dimPropertyLot._recordDeleted as _dimPropertyLotRecordDeleted,
# MAGIC     dimLocation._recordDeleted as _dimLocationRecordDeleted,
# MAGIC     dimProperty._recordCurrent as _dimPropertyRecordCurrent,
# MAGIC     dimPropertyTypeHistory._recordCurrent as _dimPropertyTypeHistoryRecordCurrent,
# MAGIC     dimPropertyLot._recordCurrent as _dimPropertyLotRecordCurrent,
# MAGIC     dimLocation._recordCurrent as _dimLocationRecordCurrent
# MAGIC     ,CASE
# MAGIC       WHEN CURRENT_DATE() BETWEEN effectiveDateRanges._effectiveFrom AND effectiveDateRanges._effectiveTo then 'Y'
# MAGIC       ELSE 'N'
# MAGIC       END AS currentRecordFlag
# MAGIC from effectiveDateRangesNonDelete as effectiveDateRanges
# MAGIC left outer join curated_v2.dimProperty dimProperty
# MAGIC         on dimproperty.propertynumber = effectiveDateRanges.propertyNumber 
# MAGIC 		and dimProperty._recordEnd >= effectiveDateRanges._effectiveFrom 
# MAGIC 		and dimProperty._recordStart <= effectiveDateRanges._effectiveTo
# MAGIC         AND dimProperty._recordDeleted = 0
# MAGIC left outer join curated_v2.dimPropertyTypeHistory dimPropertyTypeHistory 
# MAGIC         on dimPropertyTypeHistory.propertynumber = effectiveDateRanges.propertyNumber 
# MAGIC 		and dimPropertyTypeHistory.validToDate >= effectiveDateRanges._effectiveFrom 
# MAGIC 		and dimPropertyTypeHistory.validFromDate <= effectiveDateRanges._effectiveTo
# MAGIC         AND dimPropertyTypeHistory._recordDeleted = 0
# MAGIC left outer join curated_v2.dimPropertyLot dimPropertyLot
# MAGIC         on dimPropertyLot.propertynumber = effectiveDateRanges.propertyNumber 
# MAGIC 		and dimPropertyLot._recordEnd >= effectiveDateRanges._effectiveFrom 
# MAGIC 		and dimPropertyLot._recordStart <= effectiveDateRanges._effectiveTo
# MAGIC         AND dimPropertyLot._recordDeleted = 0
# MAGIC left outer join curated_v2.dimLocation dimLocation
# MAGIC         on dimLocation.locationID = effectiveDateRanges.propertyNumber 
# MAGIC 		and dimLocation._recordEnd >= effectiveDateRanges._effectiveFrom 
# MAGIC 		and dimLocation._recordStart <= effectiveDateRanges._effectiveTo 
# MAGIC         AND dimLocation._recordDeleted = 0
# MAGIC )
# MAGIC union
# MAGIC select
# MAGIC     dimProperty.propertySK,
# MAGIC     dimPropertyTypeHistory.propertyTypeHistorySK,
# MAGIC     dimProperty.sourceSystemCode,
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
# MAGIC     dimPropertyLot.planTypeCode,
# MAGIC     dimPropertyLot.planType,
# MAGIC     dimPropertyLot.planNumber,
# MAGIC     dimPropertyLot.lotTypeCode,
# MAGIC     dimPropertyLot.lotType,
# MAGIC     dimPropertyLot.lotNumber,
# MAGIC     dimPropertyLot.sectionNumber,
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
# MAGIC     dimLocation.LGACode,
# MAGIC     dimLocation.LGA,
# MAGIC     dimLocation.latitude,
# MAGIC     dimLocation.longitude,
# MAGIC 	dimPropertyTypeHistory.ValidFromDate as propertyTypeValidFromDate,
# MAGIC 	dimPropertyTypeHistory.ValidToDate as propertyTypeValidToDate,
# MAGIC 	cast(effectiveDateRanges._effectiveFrom as date),
# MAGIC 	cast(effectiveDateRanges._effectiveTo as date),
# MAGIC     dimProperty._recordDeleted as _dimPropertyRecordDeleted,
# MAGIC     dimPropertyTypeHistory._recordDeleted as _dimPropertyTypeHistoryRecordDeleted,
# MAGIC     dimPropertyLot._recordDeleted as _dimPropertyLotRecordDeleted,
# MAGIC     dimLocation._recordDeleted as _dimLocationRecordDeleted,
# MAGIC     dimProperty._recordCurrent as _dimPropertyRecordCurrent,
# MAGIC     dimPropertyTypeHistory._recordCurrent as _dimPropertyTypeHistoryRecordCurrent,
# MAGIC     dimPropertyLot._recordCurrent as _dimPropertyLotRecordCurrent,
# MAGIC     dimLocation._recordCurrent as _dimLocationRecordCurrent
# MAGIC     ,CASE
# MAGIC       WHEN CURRENT_DATE() BETWEEN effectiveDateRanges._effectiveFrom AND effectiveDateRanges._effectiveTo then 'Y'
# MAGIC       ELSE 'N'
# MAGIC       END AS currentRecordFlag
# MAGIC from effectiveDateRangesDelete as effectiveDateRanges
# MAGIC left outer join curated_v2.dimProperty dimProperty
# MAGIC         on dimproperty.propertynumber = effectiveDateRanges.propertyNumber 
# MAGIC 		and dimProperty._recordEnd >= effectiveDateRanges._effectiveFrom 
# MAGIC 		and dimProperty._recordStart <= effectiveDateRanges._effectiveTo
# MAGIC         AND dimProperty._recordDeleted = 1
# MAGIC left outer join curated_v2.dimPropertyTypeHistory dimPropertyTypeHistory 
# MAGIC         on dimPropertyTypeHistory.propertynumber = effectiveDateRanges.propertyNumber 
# MAGIC 		and dimPropertyTypeHistory.validToDate >= effectiveDateRanges._effectiveFrom 
# MAGIC 		and dimPropertyTypeHistory.validFromDate <= effectiveDateRanges._effectiveTo
# MAGIC         AND dimPropertyTypeHistory._recordDeleted = 1
# MAGIC left outer join curated_v2.dimPropertyLot dimPropertyLot
# MAGIC         on dimPropertyLot.propertynumber = effectiveDateRanges.propertyNumber 
# MAGIC 		and dimPropertyLot._recordEnd >= effectiveDateRanges._effectiveFrom 
# MAGIC 		and dimPropertyLot._recordStart <= effectiveDateRanges._effectiveTo
# MAGIC         AND dimPropertyLot._recordDeleted = 1
# MAGIC left outer join curated_v2.dimLocation dimLocation
# MAGIC         on dimLocation.locationID = effectiveDateRanges.propertyNumber 
# MAGIC 		and dimLocation._recordEnd >= effectiveDateRanges._effectiveFrom 
# MAGIC 		and dimLocation._recordStart <= effectiveDateRanges._effectiveTo 
# MAGIC         AND dimLocation._recordDeleted = 1
# MAGIC ) ORDER BY _effectiveFrom;
