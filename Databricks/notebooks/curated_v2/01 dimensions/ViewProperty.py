# Databricks notebook source
notebookPath = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split("/")
view = notebookPath[-1:][0]
db = notebookPath[-3:][0]

spark.sql("""
CREATE OR REPLACE VIEW curated_v2.viewProperty AS

With dimPropertyDateRanges AS
(
                SELECT
                propertyNumber, 
                _recordStart, 
                _recordEnd,
                COALESCE( LEAD( _recordStart, 1 ) OVER( PARTITION BY propertyNumber ORDER BY _recordStart ), 
                  CASE WHEN _recordEnd < cast('9999-12-31T23:59:59' as timestamp) then _recordEnd + INTERVAL 1 SECOND else _recordEnd end) AS _newRecordEnd
                FROM curated_v2.dimProperty
                --WHERE _recordDeleted = 0
),

dimPropertyTypeHistoryDateRanges AS
(
                SELECT
                propertyNumber, 
                _recordStart, 
                _recordEnd,
                COALESCE( LEAD( _recordStart, 1 ) OVER( PARTITION BY propertyNumber ORDER BY _recordStart ), 
                  CASE WHEN _recordEnd < cast('9999-12-31T23:59:59' as timestamp) then _recordEnd + INTERVAL 1 SECOND else _recordEnd end) AS _newRecordEnd
                FROM curated_v2.dimPropertyTypeHistory
                WHERE _recordDeleted = 0
),

dimPropertyLotDateRanges AS
(
                SELECT
                propertyNumber, 
                _recordStart, 
                _recordEnd,
                COALESCE( LEAD( _recordStart, 1 ) OVER( PARTITION BY propertyNumber ORDER BY _recordStart ), 
                  CASE WHEN _recordEnd < cast('9999-12-31T23:59:59' as timestamp) then _recordEnd + INTERVAL 1 SECOND else _recordEnd end) AS _newRecordEnd
                FROM curated_v2.dimPropertyLot
                --WHERE _recordDeleted = 0
),

dimLocationDateRanges AS
(
                SELECT
                locationID as propertyNumber, 
                _recordStart, 
                _recordEnd,
                COALESCE( LEAD( _recordStart, 1 ) OVER( PARTITION BY locationID ORDER BY _recordStart ), 
                  CASE WHEN _recordEnd < cast('9999-12-31T23:59:59' as timestamp) then _recordEnd + INTERVAL 1 SECOND else _recordEnd end) AS _newRecordEnd
                FROM curated_v2.dimLocation
                --WHERE _recordDeleted = 0
),

dateDriver AS
(
                SELECT propertyNumber, _recordStart from dimPropertyDateRanges
                UNION
                SELECT propertyNumber, _newRecordEnd as _recordStart from dimPropertyDateRanges
                UNION
                SELECT propertyNumber, _recordStart from dimPropertyTypeHistoryDateRanges
                UNION
                SELECT propertyNumber, _newRecordEnd as _recordStart from dimPropertyTypeHistoryDateRanges
                UNION
                SELECT propertyNumber, _recordStart from dimPropertyLotDateRanges
                UNION
                SELECT propertyNumber, _newRecordEnd as _recordStart from dimPropertyLotDateRanges
                UNION
                SELECT propertyNumber, _recordStart from dimLocationDateRanges
                UNION
                SELECT propertyNumber, _newRecordEnd as _recordStart from dimLocationDateRanges
),

effectiveDateRanges AS
(
                SELECT 
                propertyNumber, 
                _recordStart AS _effectiveFrom,
                COALESCE( LEAD( _recordStart, 1 ) OVER( PARTITION BY propertyNumber ORDER BY _recordStart ) - INTERVAL 1 SECOND, 
                  cast( '9999-12-31T23:59:59' as timestamp ) ) AS _effectiveTo                           
                FROM dateDriver where _recordStart < cast('9999-12-31T23:59:59' as timestamp)                            
)           


select * from (
select
    --dimProperty.propertySK,
    --dimPropertyTypeHistory.propertyTypeHistorySK,
    coalesce(dimProperty.sourceSystemCode, dimPropertyTypeHistory.sourceSystemCode, dimPropertyLot.sourceSystemCode, dimLocation.sourceSystemCode) as sourceSystemCode,
    coalesce(dimProperty.propertyNumber, dimPropertyTypeHistory.propertyNumber, dimPropertyLot.propertyNumber, dimLocation.locationID, -1) as propertyNumber,
    dimProperty.premise,
    --dimProperty.waterNetworkSK_drinkingWater,
    --dimProperty.waterNetworkSK_recycledWater,
    --dimProperty.sewerNetworkSK,
    --dimProperty.stormWaterNetworkSK,
    dimProperty.objectNumber,
    dimProperty.propertyInfo,
    dimProperty.buildingFeeDate,
    dimProperty.connectionValidFromDate,
    dimProperty.CRMConnectionObjectGUID,
    dimProperty.architecturalObjectNumber,
    dimProperty.architecturalObjectTypeCode,
    dimProperty.architecturalObjectType,
    dimProperty.parentArchitecturalObjectNumber,
    dimProperty.parentArchitecturalObjectTypeCode,
    dimProperty.parentArchitecturalObjectType,
    dimProperty.hydraBand,
    dimProperty.hydraCalculatedArea,
    dimProperty.hydraAreaUnit,
    dimProperty.overrideArea,
    dimProperty.overrideAreaUnit,
    dimProperty.stormWaterAssessmentFlag,
    dimProperty.hydraAreaFlag,
    dimProperty.comments,
    --dimPropertyLot.propertyLotSK,
    dimPropertyLot.planTypeCode,
    dimPropertyLot.planType,
    dimPropertyLot.planNumber,
    dimPropertyLot.lotTypeCode,
    dimPropertyLot.lotType,
    dimPropertyLot.lotNumber,
    dimPropertyLot.sectionNumber,
    dimPropertyLot.latitude,
    dimPropertyLot.longitude,
    dimPropertyTypeHistory.superiorPropertyTypeCode,
    dimPropertyTypeHistory.superiorPropertyType,
    dimPropertyTypeHistory.inferiorPropertyTypeCode,
    dimPropertyTypeHistory.inferiorPropertyType,
    --dimLocation.locationSK,
    dimLocation.formattedAddress,
    dimLocation.addressNumber,
    dimLocation.buildingName1,
    dimLocation.buildingName2,
    dimLocation.unitDetails,
    dimLocation.floorNumber,
    dimLocation.houseNumber,
    dimLocation.lotDetails,
    dimLocation.streetName,
    dimLocation.streetLine1,
    dimLocation.streetLine2,
    dimLocation.suburb,
    dimLocation.streetCode,
    dimLocation.cityCode,
    dimLocation.postCode,
    dimLocation.stateCode,
    dimLocation.LGA,
	dimPropertyTypeHistory.ValidFromDate as propertyTypeValidFromDate,
	dimPropertyTypeHistory.ValidToDate as propertyTypeValidToDate,
	effectiveDateRanges._effectiveFrom,
	effectiveDateRanges._effectiveTo,
    CASE
      WHEN CURRENT_TIMESTAMP() BETWEEN effectiveDateRanges._effectiveFrom AND effectiveDateRanges._effectiveTo then 'Y'
      ELSE 'N'
      END AS currentFlag,
    if(dimProperty._RecordDeleted = 0,'Y','N') AS currentRecordFlag 
from effectiveDateRanges as effectiveDateRanges
left outer join curated_v2.dimProperty dimProperty
        on dimproperty.propertynumber = effectiveDateRanges.propertyNumber 
		and dimProperty._recordEnd >= effectiveDateRanges._effectiveFrom 
		and dimProperty._recordStart <= effectiveDateRanges._effectiveTo
        --AND dimProperty._recordDeleted = 0
left outer join curated_v2.dimPropertyTypeHistory dimPropertyTypeHistory 
        on dimPropertyTypeHistory.propertynumber = effectiveDateRanges.propertyNumber 
		and dimPropertyTypeHistory.validToDate >= effectiveDateRanges._effectiveFrom 
		and dimPropertyTypeHistory.validFromDate <= effectiveDateRanges._effectiveTo
        AND dimPropertyTypeHistory._recordDeleted = 0
left outer join curated_v2.dimPropertyLot dimPropertyLot
        on dimPropertyLot.propertynumber = effectiveDateRanges.propertyNumber 
		and dimPropertyLot._recordEnd >= effectiveDateRanges._effectiveFrom 
		and dimPropertyLot._recordStart <= effectiveDateRanges._effectiveTo
        --AND dimPropertyLot._recordDeleted = 0
left outer join curated_v2.dimLocation dimLocation
        on dimLocation.locationID = effectiveDateRanges.propertyNumber 
		and dimLocation._recordEnd >= effectiveDateRanges._effectiveFrom 
		and dimLocation._recordStart <= effectiveDateRanges._effectiveTo 
        --AND dimLocation._recordDeleted = 0 
) ORDER BY _effectiveFrom;
""".replace("CREATE OR REPLACE VIEW", "ALTER VIEW" if spark.sql(f"SHOW VIEWS FROM {db} LIKE '{view}'").count() == 1 else "CREATE OR REPLACE VIEW"))
