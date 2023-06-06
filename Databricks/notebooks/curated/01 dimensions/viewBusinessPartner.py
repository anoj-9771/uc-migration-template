# Databricks notebook source
# MAGIC %run ../common/common-curated-includeMain

# COMMAND ----------

# notebookPath = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split("/")
# view = notebookPath[-1:][0]
# db = notebookPath[-3:][0]
schema_name = 'consumption'
view_name = 'viewbusinesspartner'
view_fqn = f"{ADS_DATABASE_CURATED}.{schema_name}.{view_name}"

spark.sql(f"""
CREATE OR REPLACE VIEW {view_fqn} AS 
 
With dimBusinessPartnerRanges AS
(
                SELECT
                businessPartnerNumber, 
                _recordStart, 
                _recordEnd,
                COALESCE( LEAD( _recordStart, 1 ) OVER( PARTITION BY businessPartnerNumber ORDER BY _recordStart ), 
                  CASE WHEN _recordEnd < cast('9999-12-31T23:59:59' as timestamp) then _recordEnd + INTERVAL 1 SECOND else _recordEnd end) AS _newRecordEnd
                FROM {ADS_DATABASE_CURATED}.dim.BusinessPartner
                where businessPartnerCategoryCode in ('1','2')
),
dimBusinessPartnerAddressRanges AS
(
                SELECT
                ba.businessPartnerNumber, 
                ba._recordStart, 
                ba._recordEnd,
                COALESCE( LEAD( ba._recordStart, 1 ) OVER( PARTITION BY ba.businessPartnerNumber ORDER BY ba._recordStart ), 
                  CASE WHEN ba._recordEnd < cast('9999-12-31T23:59:59' as timestamp) then ba._recordEnd + INTERVAL 1 SECOND else ba._recordEnd end) AS _newRecordEnd
                FROM {ADS_DATABASE_CURATED}.dim.businessPartnerAddress ba
                inner join {ADS_DATABASE_CURATED}.dim.businesspartner bp on bp.businesspartnernumber = ba.businesspartnernumber
                where bp.businessPartnerCategoryCode in ('1','2')
),
dimBusinessPartnerRelationRanges AS
(
                SELECT
                br.businessPartnerNumber, 
                _recordStart, 
                _recordEnd,
                COALESCE( LEAD( _recordStart, 1 ) OVER( PARTITION BY br.businessPartnerNumber ORDER BY _recordStart ), 
                  CASE WHEN _recordEnd < cast('9999-12-31T23:59:59' as timestamp) then _recordEnd + INTERVAL 1 SECOND else _recordEnd end) AS _newRecordEnd
                FROM {ADS_DATABASE_CURATED}.dim.businessPartnerRelation br
                inner join (select businesspartnernumber from {ADS_DATABASE_CURATED}.dim.businesspartner bp where businessPartnerCategoryCode in ('1','2')) bp on bp.businesspartnernumber = br.businesspartnernumber
                WHERE _recordDeleted = 0
),
dateDriver AS
(
                SELECT businessPartnerNumber, _recordStart from dimBusinessPartnerRanges
                UNION
                SELECT businessPartnerNumber, _newRecordEnd as _recordStart from dimBusinessPartnerRanges
                UNION
                SELECT businessPartnerNumber, _recordStart from dimBusinessPartnerAddressRanges
                UNION
                SELECT businessPartnerNumber, _newRecordEnd as _recordStart from dimBusinessPartnerAddressRanges
                UNION
                SELECT businessPartnerNumber, _recordStart from dimBusinessPartnerRelationRanges
                UNION
                SELECT businessPartnerNumber, _newRecordEnd as _recordStart from dimBusinessPartnerRelationRanges
),
effectiveDateRanges AS
(
                SELECT 
                businessPartnerNumber, 
                _recordStart AS _effectiveFrom,
                COALESCE( LEAD( _recordStart, 1 ) OVER( PARTITION BY businessPartnerNumber ORDER BY _recordStart ) - INTERVAL 1 SECOND, 
                  cast( '9999-12-31T23:59:59' as timestamp ) ) AS _effectiveTo                           
                FROM dateDriver where _recordStart < cast('9999-12-31T23:59:59' as timestamp)                            
)

 /*============================
    viewBusinessPartner
==============================*/
SELECT * FROM
(
SELECT
    /* Business Partner Columns */
    --BP.businessPartnerSK,
    --BR.businessPartnerGroupSK,
    coalesce(BP.sourceSystemCode, ADDR.sourceSystemCode,BR.sourceSystemCode) as sourceSystemCode,
    coalesce(DR.businessPartnerNumber,BP.businessPartnerNumber, ADDR.businessPartnerNumber, '-1') as businessPartnerNumber,
    BR.businessPartnerGroupNumber,
    BR.relationshipNumber,
    BR.relationshipTypeCode,
    BR.relationshipType,
    BR.validFromDate as businessPartnerRelationValidFromDate,
    BR.validToDate as businessPartnerRelationValidToDate,
    BP.businessPartnerCategoryCode,
    BP.businessPartnerCategory,
    BP.businessPartnerTypeCode,
    BP.businessPartnerType,
    BP.businessPartnerGroupCode,
    BP.businessPartnerGroup,
    BP.externalNumber,
    BP.businessPartnerGUID,
    BP.firstName,
    BP.lastName,
    BP.middleName,
    BP.nickName,
    BP.titleCode,
    BP.title,
    BP.dateOfBirth,
    BP.dateOfDeath,
    BP.validFromDate as businessPartnerValidFromDate,
    BP.validToDate as businessPartnerValidToDate,
    BP.warWidowFlag,
    BP.deceasedFlag,
    BP.disabilityFlag,
    BP.goldCardHolderFlag,
    BP.naturalPersonFlag,
    BP.consent1Indicator,
    BP.consent2Indicator,
    BP.eligibilityFlag,
    BP.plannedChangeDocument,
    BP.paymentStartDate,
    BP.dateOfCheck,
    BP.pensionConcessionCardFlag,
    BP.pensionType,
    BP.personNumber,
    BP.personnelNumber,
    BP.organizationName,
    BP.organizationFoundedDate,
    BP.createdDateTime as businessPartnerCreatedDateTime,
    BP.createdBy as businessPartnerCreatedBy,
    BP.lastUpdatedDateTime as businessPartnerLastUpdatedDateTime,
    BP.lastUpdatedBy as businessPartnerLastUpdatedBy,
    /* Address Columns */
    --ADDR.businesspartnerAddressSK,
    ADDR.businessPartnerAddressNumber,
    ADDR.addressValidFromDate,
    ADDR.addressValidToDate,
    ADDR.phoneNumber,
    ADDR.phoneExtension,
    ADDR.faxNumber,
    ADDR.faxExtension,
    ADDR.emailAddress,
    ADDR.personalAddressFlag,
    ADDR.coName,
    ADDR.shortFormattedAddress2,
    ADDR.streetLine5 AS locationName,
    ADDR.building,
    ADDR.floorNumber,
    ADDR.apartmentNumber,
    ADDR.housePrimaryNumber,
    ADDR.houseSupplementNumber,
    ADDR.streetPrimaryName,
    ADDR.streetSupplementName1 AS supplementStreetType,
    ADDR.streetSupplementName2 as streetSupplementName,
    ADDR.otherLocationName,
    ADDR.houseNumber,
    ADDR.streetName,
    ADDR.streetCode,
    ADDR.cityName,
    ADDR.cityCode,
    ADDR.stateCode,
    ADDR.stateName,
    ADDR.postalCode,
    ADDR.countryCode,
    ADDR.countryName,
    ADDR.addressFullText,
    ADDR.poBoxCode,
    ADDR.poBoxCity,
    ADDR.postalCodeExtension AS addressDPID,
    ADDR.poBoxExtension AS bspNumber,
    ADDR.deliveryServiceTypeCode,
    ADDR.deliveryServiceType,
    ADDR.deliveryServiceNumber,
    ADDR.addressTimeZone,
    ADDR.communicationAddressNumber,
    DR._effectiveFrom, 
    DR._effectiveTo,
    CASE WHEN coalesce(BR.validToDate, '1900-01-01') = '9999-12-31' and _effectiveto = '9999-12-31 23:59:59.000'  then 'Y' ELSE 'N' END AS currentFlag,
    if(BP._RecordDeleted = 0,'Y','N') AS currentRecordFlag 
FROM effectiveDateRanges DR
LEFT JOIN  {ADS_DATABASE_CURATED}.dim.businesspartner BP ON 
    DR.businessPartnerNumber = BP.businessPartnerNumber AND
    DR._effectiveFrom <= BP._RecordEnd AND
    DR._effectiveTo >= BP._RecordStart  
LEFT JOIN  {ADS_DATABASE_CURATED}.dim.businesspartnerrelation BR ON 
    DR.businessPartnerNumber = BR.businessPartnerNumber AND
    DR._effectiveFrom <= BR._RecordEnd AND
    DR._effectiveTo >= BR._RecordStart AND
    BR._recordDeleted = 0
LEFT JOIN {ADS_DATABASE_CURATED}.dim.businesspartneraddress ADDR ON 
    DR.businessPartnerNumber = ADDR.businessPartnerNumber AND
    DR._effectiveFrom <= ADDR._RecordEnd AND
    DR._effectiveTo >= ADDR._RecordStart 
)
""".replace("CREATE OR REPLACE VIEW", "ALTER VIEW" if viewsExists(view_fqn) else "CREATE OR REPLACE VIEW"))

# COMMAND ----------

# MAGIC %run ./viewBusinessPartnerGroup
