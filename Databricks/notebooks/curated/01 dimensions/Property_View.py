# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC create or replace view curated_v2.scd_view_property
# MAGIC as (
# MAGIC with dateDriver as
# MAGIC (
# MAGIC 	select distinct propertyNumber, to_date(_recordStart) as _effectiveFrom from curated_v2.dimProperty where isnotnull(propertyNumber)
# MAGIC 	union
# MAGIC 	select distinct propertyNumber,to_date(validFromDate) as _effectiveFrom from curated_v2.dimPropertyTypeHistory  where isnotnull(propertyNumber)
# MAGIC     union
# MAGIC     select distinct propertyNumber, to_date(_recordStart) as _effectiveFrom from curated_v2.dimPropertyLot where isnotnull(propertyNumber)
# MAGIC     union
# MAGIC     select distinct locationID as propertyNumber, to_date(_recordStart) as _effectiveFrom from curated_v2.dimLocation where isnotnull(locationID)
# MAGIC ),
# MAGIC effectiveDateranges as 
# MAGIC (
# MAGIC 	select propertyNumber, _effectiveFrom, coalesce(timestamp(date_add(lead(_effectiveFrom,1) over(partition by propertyNumber order by _effectiveFrom), -1)), '9999-12-31') as _effectiveTo
# MAGIC 	from dateDriver 
# MAGIC )
# MAGIC select
# MAGIC     dimProperty.propertySK,
# MAGIC     dimPropertyTypeHistory.propertyTypeHistorySK,
# MAGIC     dimProperty.sourceSystemCode,
# MAGIC     dimProperty.propertyNumber,
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
# MAGIC     dimProperty.stormWaterAssessmentIndicator,
# MAGIC     dimProperty.hydraAreaIndicator,
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
# MAGIC 	effectiveDateRanges._effectiveFrom,
# MAGIC 	effectiveDateRanges._effectiveTo
# MAGIC from effectiveDateRanges
# MAGIC left outer join curated_v2.dimProperty dimProperty
# MAGIC         on dimproperty.propertynumber = effectiveDateRanges.propertyNumber 
# MAGIC 		and dimProperty._recordEnd >= effectiveDateRanges._effectiveFrom 
# MAGIC 		and dimProperty._recordStart <= effectiveDateRanges._effectiveTo
# MAGIC left outer join curated_v2.dimPropertyTypeHistory dimPropertyTypeHistory 
# MAGIC         on dimPropertyTypeHistory.propertynumber = effectiveDateRanges.propertyNumber 
# MAGIC 		and dimPropertyTypeHistory.validToDate >= effectiveDateRanges._effectiveFrom 
# MAGIC 		and dimPropertyTypeHistory.validFromDate <= effectiveDateRanges._effectiveTo
# MAGIC left outer join curated_v2.dimPropertyLot dimPropertyLot
# MAGIC         on dimPropertyLot.propertynumber = effectiveDateRanges.propertyNumber 
# MAGIC 		and dimPropertyLot._recordEnd >= effectiveDateRanges._effectiveFrom 
# MAGIC 		and dimPropertyLot._recordStart <= effectiveDateRanges._effectiveTo
# MAGIC left outer join curated_v2.dimLocation dimLocation
# MAGIC         on dimLocation.locationID = effectiveDateRanges.propertyNumber 
# MAGIC 		and dimLocation._recordEnd >= effectiveDateRanges._effectiveFrom 
# MAGIC 		and dimLocation._recordStart <= effectiveDateRanges._effectiveTo 
# MAGIC where isnotnull(dimProperty.propertyNumber)
# MAGIC );
