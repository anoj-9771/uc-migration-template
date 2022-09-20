-- Databricks notebook source
-- View: view_device
-- Description: view_device
CREATE OR REPLACE VIEW curated_v2.view_device AS
WITH dateDriver AS
(
    SELECT DISTINCT deviceNumber, _recordStart AS _effectiveFrom FROM curated_v2.dimDevice
    WHERE _recordDeleted <> 1
    union
    SELECT DISTINCT deviceNumber, _recordStart AS _effectiveFrom FROM curated_v2.dimDeviceHistory
    WHERE _recordDeleted <> 1
),
effectiveDateranges AS 
(
    SELECT deviceNumber, _effectiveFrom, COALESCE(TIMESTAMP(DATE_ADD(LEAD(_effectiveFrom,1) OVER(PARTITION BY deviceNumber ORDER BY _effectiveFrom), -1)), '9999-12-31') AS _effectiveTo
    FROM dateDriver
)
SELECT
     effectiveDateRanges._effectiveFrom
    ,effectiveDateRanges._effectiveTo
    ,effectiveDateRanges.deviceNumber
    ,dimdevice.deviceSK
    ,dimdevice.sourceSystemCode
--     ,dimdevice.deviceNumber
    ,dimdevice.materialNumber
    ,dimdevice.deviceID
    ,dimdevice.inspectionRelevanceIndicator
    ,dimdevice.deviceSize
    ,dimdevice.assetManufacturerName
    ,dimdevice.manufacturerSerialNumber
    ,dimdevice.manufacturerModelNumber
    ,dimdevice.objectNumber
    ,dimdevice.functionClassCode
    ,dimdevice.functionClass
    ,dimdevice.constructionClassCode
    ,dimdevice.constructionClass
    ,dimdevice.deviceCategoryName
    ,dimdevice.deviceCategoryDescription
    ,dimdevice.ptiNumber
    ,dimdevice.ggwaNumber
    ,dimdevice.certificationRequirementType
    --,dimdevice._recordStart AS dev_recordStart
    --,dimdevice._recordEnd AS device_recordEnd
    ,dimdevicehistory.deviceHistorySK
--     ,dimdevicehistory.deviceNumber
    ,dimdevicehistory.validToDate AS deviceHistoryValidToDate
    ,dimdevicehistory.validFromDate AS deviceHistoryValidFromDate
    ,dimdevicehistory.logicalDeviceNumber
    ,dimdevicehistory.deviceLocation
    ,dimdevicehistory.deviceCategoryCombination
    ,dimdevicehistory.registerGroupCode
    ,dimdevicehistory.registerGroup
    ,dimdevicehistory.installationDate
    ,dimdevicehistory.deviceRemovalDate
    ,dimdevicehistory.activityReasonCode
    ,dimdevicehistory.activityReason
    ,dimdevicehistory.firstInstallationDate
    ,dimdevicehistory.lastDeviceRemovalDate
    ,dimdeviceinstallationhistory.deviceInstallationHistorySK
    ,dimdeviceinstallationhistory.installationNumber
    ,dimdeviceinstallationhistory.validToDate AS deviceInstallationHistoryValidToDate
    ,dimdeviceinstallationhistory.validFromDate AS deviceInstallationHistoryValidFromDate
    ,dimdeviceinstallationhistory.priceClassCode
    ,dimdeviceinstallationhistory.priceClass
    ,dimdeviceinstallationhistory.rateTypeCode AS deviceInstallationHistoryRateTypeCode
    ,dimdeviceinstallationhistory.rateType AS deviceInstallationHistoryRateType
    ,dimregisterhistory.registerHistorySK
    ,dimregisterhistory.registerNumber
    ,dimregisterhistory.validToDate AS registerHistoryValidToDate
    ,dimregisterhistory.validFromDate AS registerHistoryvalidFromDate
    ,dimregisterhistory.logicalRegisterNumber
    ,dimregisterhistory.divisionCategoryCode
    ,dimregisterhistory.divisionCategory
    ,dimregisterhistory.registerIdCode
    ,dimregisterhistory.registerId
    ,dimregisterhistory.registerTypeCode
    ,dimregisterhistory.registerType
    ,dimregisterhistory.registerCategoryCode
    ,dimregisterhistory.registerCategory
    ,dimregisterhistory.reactiveApparentOrActiveRegisterCode
    ,dimregisterhistory.reactiveApparentOrActiveRegister
    ,dimregisterhistory.unitOfMeasurementMeterReading
    ,dimregisterhistory.doNotReadIndicator	
    ,dimregisterinstallationhistory.registerInstallationHistorySK
    ,dimregisterinstallationhistory.validToDate AS registerInstallationHistoryValidToDate
    ,dimregisterinstallationhistory.validFromDate AS registerInstallationHistoryValidFromDate
    ,dimregisterinstallationhistory.operationCode
    ,dimregisterinstallationhistory.operationDescription
    ,dimregisterinstallationhistory.rateTypeCode AS registerInstallationHistoryRateTypeCode
    ,dimregisterinstallationhistory.rateType AS registerInstallationHistoryRateType
    ,dimregisterinstallationhistory.registerNotRelevantToBilling
    ,dimregisterinstallationhistory.rateFactGroupCode
    ,installAttr.divisionCode
    ,installAttr.division
    , CASE 
        WHEN (dimdeviceHistory.validFromDate <= CURRENT_DATE() AND dimdeviceHistory.validToDate >= CURRENT_DATE()
        AND (dimdeviceInstallationHistory.validToDate IS NULL OR (dimdeviceInstallationHistory.validFromDate <= CURRENT_DATE() AND dimdeviceInstallationHistory.validToDate >= CURRENT_DATE()))
        AND (dimRegisterHistory.validToDate IS NULL OR (dimRegisterHistory.validFromDate <= CURRENT_DATE() AND dimRegisterHistory.validToDate >= CURRENT_DATE()))
        AND (dimRegisterInstallationHistory.validToDate IS NULL OR (dimRegisterInstallationHistory.validFromDate <= CURRENT_DATE() AND dimRegisterInstallationHistory.validToDate >= CURRENT_DATE()))
        )  THEN 'Y' 
           ELSE 'N' END AS currentIndicator
FROM effectiveDateRanges
LEFT OUTER JOIN curated_v2.dimDevice dimdevice
    ON dimdevice.deviceNumber = effectiveDateRanges.deviceNumber 
        AND dimdevice._recordEnd >= effectiveDateRanges._effectiveFrom 
        AND dimdevice._recordStart <= effectiveDateRanges._effectiveTo
LEFT OUTER JOIN curated_v2.dimDeviceHistory dimdevicehistory
    ON dimdevicehistory.deviceNumber = effectiveDateRanges.deviceNumber 
        AND dimdevicehistory._recordEnd >= effectiveDateRanges._effectiveFrom 
        AND dimdevicehistory._recordStart <= effectiveDateRanges._effectiveTo
LEFT OUTER JOIN curated_v2.dimDeviceInstallationHistory dimdeviceinstallationhistory
    ON dimdeviceinstallationhistory.logicalDeviceNumber = dimdevicehistory.logicalDeviceNumber
        AND dimdevicehistory._recordStart >= dimdeviceinstallationhistory._recordStart
        AND dimdevicehistory._recordStart <= dimdeviceinstallationhistory._recordEnd
LEFT OUTER JOIN curated_v2.dimRegisterHistory dimregisterhistory
    ON dimregisterhistory.deviceNumber = dimdevice.deviceNumber
      AND dimDeviceHistory._recordStart >= dimregisterhistory._recordStart
      AND dimDeviceHistory._recordStart <= dimregisterhistory._recordEnd
LEFT OUTER JOIN curated_v2.dimRegisterInstallationHistory dimregisterinstallationhistory
    ON dimregisterinstallationhistory.installationNumber = dimdeviceinstallationhistory.installationNumber
      AND dimregisterhistory.logicalRegisterNumber = dimregisterinstallationhistory.logicalRegisterNumber
      AND dimDeviceHistory._recordStart >= dimregisterinstallationhistory._recordStart
      AND dimDeviceHistory._recordStart <= dimregisterhistory._recordEnd
LEFT OUTER JOIN cleansed.isu_0UCINSTALLA_ATTR_2 installAttr
    ON installAttr.installationId = dimdeviceinstallationhistory.installationNumber
ORDER BY effectiveDateRanges._effectiveFrom

-- COMMAND ----------

-- View: view_businesspartner
-- Description: view_businesspartner
CREATE OR REPLACE VIEW curated_v2.view_businesspartner AS 
 WITH 
     /**************************************
     Build 'Effective From and To' table
     **************************************/
     dateDriver AS (
         SELECT DISTINCT
             sourceSystemCode,
             businessPartnerNumber,
             _recordStart AS _effectiveFrom
         FROM curated_v2.dimBusinessPartner
     ),
 
     effectiveDateRanges AS (
         SELECT 
             sourceSystemCode,
             businessPartnerNumber, 
             _effectiveFrom, 
             COALESCE(
                 TIMESTAMP(
                     DATE_ADD(
                         LEAD(_effectiveFrom,1) OVER (PARTITION BY sourceSystemCode, businessPartnerNumber ORDER BY _effectiveFrom),-1)
                 ), 
             TIMESTAMP('9999-12-31')) AS _effectiveTo
         from dateDriver
     ),   
 
     /**************************************
     -- Get BP Identification Tables --
         Right now, if there are multiple IDs for one ID Type, then the rank 
         function will pick only one (based on sorting the businessPartnerIdNumber)
     **************************************/
     id_driverLicense AS (
         SELECT
             *,
             DENSE_RANK() OVER (PARTITION BY businessPartnerNumber ORDER BY _RecordStart DESC, businessPartnerIdNumber) AS _rank
         FROM curated_v2.dimBusinessPartnerIdentification BPID
         WHERE BPID.identificationType = 'Drivers License Number' -- 'ZLICID'
         AND CURRENT_DATE() BETWEEN BPID._RecordStart AND BPID._RecordEnd -- Current State of BP IDs
     ),
 
     id_pensionNumber AS (
         SELECT
             *,
             DENSE_RANK() OVER (PARTITION BY businessPartnerNumber ORDER BY _RecordStart DESC, businessPartnerIdNumber) AS _rank
         FROM curated_v2.dimBusinessPartnerIdentification BPID
         WHERE BPID.identificationType = 'Pensioner#' -- ZSWPEN'
         AND CURRENT_DATE() BETWEEN BPID._RecordStart AND BPID._RecordEnd -- Current State of BP IDs
     ),
 
     id_australianBusinessNumber AS (
         SELECT
             *,
             DENSE_RANK() OVER (PARTITION BY businessPartnerNumber ORDER BY _RecordStart DESC, businessPartnerIdNumber) AS _rank
         FROM curated_v2.dimBusinessPartnerIdentification BPID
         WHERE BPID.identificationType = 'Australian Business Number' -- 'ZABN'
         AND CURRENT_DATE() BETWEEN BPID._RecordStart AND BPID._RecordEnd -- Current State of BP IDs
     ),
 
     id_australianCompanyNumber AS (
         SELECT
             *,
             DENSE_RANK() OVER (PARTITION BY businessPartnerNumber ORDER BY _RecordStart DESC, businessPartnerIdNumber) AS _rank
         FROM curated_v2.dimBusinessPartnerIdentification BPID
         WHERE BPID.identificationType = 'Australian Company Number' -- 'ZACN'
         AND CURRENT_DATE() BETWEEN BPID._RecordStart AND BPID._RecordEnd -- Current State of BP IDs
     ),
 
     id_dvaNumber AS (
         SELECT
             *,
             DENSE_RANK() OVER (PARTITION BY businessPartnerNumber ORDER BY _RecordStart DESC, businessPartnerIdNumber) AS _rank
         FROM curated_v2.dimBusinessPartnerIdentification BPID
         WHERE BPID.identificationType = 'DVA#' -- ZSWDVA#' 
         AND CURRENT_DATE() BETWEEN BPID._RecordStart AND BPID._RecordEnd -- Current State of BP IDs
     )
 
 /**************************************
 CREATE THE VIEW TABLE
 **************************************/
 SELECT
    /* Business Partner Columns */
    BP.businessPartnerSK,
    BP.sourceSystemCode,
    BP.businessPartnerNumber,
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
    BP.validFromDate,
    BP.validToDate,
    BP.warWidowFlag,
    BP.deceasedFlag,
    BP.disabilityFlag,
    BP.goldCardHolderFlag,
    BP.naturalPersonFlag,
    BP.consent1Indicator,
    BP.consent2Indicator,
    BP.eligibilityFlag,
    BP.paymentAssistSchemeFlag,
    BP.plannedChangeDocument,
    BP.paymentStartDate,
    BP.dateOfCheck,
    BP.pensionConcessionCardFlag,
    BP.pensionType,
    BP.personNumber,
    BP.personnelNumber,
    BP.organizationName,
    BP.organizationFoundedDate,
    BP.createdDateTime,
    BP.createdBy,
    BP.lastUpdatedBy,
    BP.lastUpdatedDateTime,
    /* Address Columns */
    ADDR.businesspartnerAddressSK,
    ADDR.AddressNumber,
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
    ADDR.streetLine5,
    ADDR.building,
    ADDR.floorNumber,
    ADDR.apartmentNumber,
    ADDR.housePrimaryNumber,
    ADDR.houseSupplementNumber,
    ADDR.streetPrimaryName,
    ADDR.streetSupplementName1,
    ADDR.streetSupplementName2,
    ADDR.otherLocationName,
    ADDR.houseNumber,
    ADDR.streetName,
    ADDR.streetCode,
    ADDR.cityName,
    ADDR.cityCode,
    ADDR.stateCode,
    ADDR.postalCode,
    ADDR.countryShortName,
    ADDR.countryName,
    ADDR.addressFullText,
    ADDR.poBoxCode,
    ADDR.poBoxCity,
    ADDR.postalCodeExtension,
    ADDR.poBoxExtension,
    ADDR.deliveryServiceTypeCode,
    ADDR.deliveryServiceType,
    ADDR.deliveryServiceNumber,
    ADDR.addressTimeZone,
    ADDR.communicationAddressNumber,
    /* Identification Columns */
    DL.businessPartnerIdNumber AS driverLicenseNumber,
    DL.ValidFromDate AS driverLicenseNumberValidFrom,
    DL.ValidToDate AS driverLicenseNumberValidTo,
    DL.EntryDate AS driverLicenseNumberEntryDate,
    PEN.businessPartnerIdNumber AS pensionNumber,
    PEN.ValidFromDate AS pensionNumberValidFrom,
    PEN.ValidToDate AS pensionNumberValidTo,
    PEN.EntryDate AS pensionNumberEntryDate,
    ABN.businessPartnerIdNumber AS australianBusinessNumber,
    ABN.ValidFromDate AS australianBusinessNumberValidFrom,
    ABN.ValidToDate AS australianBusinessNumberValidTo,
    ABN.EntryDate AS australianBusinessNumberEntryDate,
    ACN.businessPartnerIdNumber AS australianCompanyNumber,
    ACN.ValidFromDate AS australianCompanyNumberValidFrom,
    ACN.ValidToDate AS australianCompanyNumberValidTo,
    ACN.EntryDate AS australianCompanyNumberEntryDate,
    DVA.businessPartnerIdNumber AS dvaNumber,
    DVA.ValidFromDate AS dvaNumberValidFrom,
    DVA.ValidToDate AS dvaNumberValidTo,
    DVA.EntryDate AS dvaNumberEntryDate,
    DR._effectiveFrom, 
    DR._effectiveTo
FROM effectiveDateRanges DR
LEFT JOIN curated_v2.dimbusinesspartner BP ON 
    DR.businessPartnerNumber = BP.businessPartnerNumber AND
    DR.sourceSystemCode = BP.sourceSystemCode AND
    DR._effectiveFrom <= BP._RecordEnd AND
    DR._effectiveTo >= BP._RecordStart 
LEFT JOIN curated_v2.dimbusinesspartneraddress ADDR ON 
    DR.businessPartnerNumber = ADDR.businessPartnerNumber AND
    DR.sourceSystemCode = ADDR.sourceSystemCode AND
    DR._effectiveFrom <= ADDR._RecordEnd AND
    DR._effectiveTo >= ADDR._RecordStart 
LEFT JOIN id_driverLicense DL ON
    DL.sourceSystemCode = BP.sourceSystemCode AND
    DL.businessPartnerNumber = BP.businessPartnerNumber AND
    DL._rank = 1
LEFT JOIN id_pensionNumber PEN ON
    PEN.sourceSystemCode = BP.sourceSystemCode AND
    PEN.businessPartnerNumber = BP.businessPartnerNumber AND
    PEN._rank = 1
LEFT JOIN id_australianBusinessNumber ABN ON
    ABN.sourceSystemCode = BP.sourceSystemCode AND
    ABN.businessPartnerNumber = BP.businessPartnerNumber AND
    ABN._rank = 1
LEFT JOIN id_australianCompanyNumber ACN ON
    ACN.sourceSystemCode = BP.sourceSystemCode AND
    ACN.businessPartnerNumber = BP.businessPartnerNumber AND
    ACN._rank = 1
LEFT JOIN id_dvaNumber DVA ON
    DVA.sourceSystemCode = BP.sourceSystemCode AND
    DVA.businessPartnerNumber = BP.businessPartnerNumber AND
    DVA._rank = 1

-- COMMAND ----------

-- View: view_businesspartneridentification
-- Description: view_businesspartneridentification

CREATE OR REPLACE VIEW curated_v2.view_businesspartneridentification AS 

WITH 
    businessPartnerNumber AS (
        SELECT DISTINCT
            sourceSystemCode,
            businessPartnerNumber
        FROM curated_v2.dimBusinessPartnerIdentification BP
    )

    SELECT
        BP.sourceSystemCode,
        BP.businessPartnerNumber,
        DL.businessPartnerIdNumber AS driverLicenseNumber,
        DL.ValidFromDate AS driverLicenseNumberValidFrom,
        DL.ValidToDate AS driverLicenseNumberValidTo,
        DL.EntryDate AS driverLicenseNumberEntryDate,
        PN.businessPartnerIdNumber AS passportNumber,
        PN.ValidFromDate AS passportNumberValidFrom,
        PN.ValidToDate AS passportNumberValidTo,
        PN.EntryDate AS passportNumberEntryDate,
        P.businessPartnerIdNumber AS pensionNumber,
        P.ValidFromDate AS pensionNumberValidFrom,
        P.ValidToDate AS pensionNumberValidTo,
        P.EntryDate AS pensionNumberEntryDate,
        ABN.businessPartnerIdNumber AS australianBusinessNumber,
        ABN.ValidFromDate AS australianBusinessNumberValidFrom,
        ABN.ValidToDate AS australianBusinessNumberValidTo,
        ABN.EntryDate AS australianBusinessNumberEntryDate,
        ACN.businessPartnerIdNumber AS australianCompanyNumber,
        ACN.ValidFromDate AS australianCompanyNumberValidFrom,
        ACN.ValidToDate AS australianCompanyNumberValidTo,
        ACN.EntryDate AS australianCompanyNumberEntryDate,
        DVA.businessPartnerIdNumber AS dvaNumber,
        DVA.ValidFromDate AS dvaNumberValidFrom,
        DVA.ValidToDate AS dvaNumberValidTo,
        DVA.EntryDate AS dvaNumberEntryDate,
        CRN.businessPartnerIdNumber AS commercialRegisterNumber,
        CRN.ValidFromDate AS commercialRegisterNumberValidFrom,
        CRN.ValidToDate AS commercialRegisterNumberValidTo,
        CRN.EntryDate AS commercialRegisterNumberEntryDate,
        TLN.businessPartnerIdNumber AS tradeLicenseNumber,
        TLN.ValidFromDate AS tradeLicenseNumberValidFrom,
        TLN.ValidToDate AS tradeLicenseNumberValidTo,
        TLN.EntryDate AS tradeLicenseNumberEntryDate,
        DBT.businessPartnerIdNumber AS directDebitTelephoneNumber,
        DBT.EntryDate AS directDebitTelephoneNumberEntryDate,
        DBE.businessPartnerIdNumber AS directDebitEmail,
        DBE.EntryDate AS directDebitEmailEntryDate,
        ERP.businessPartnerIdNumber AS ebillRegistrationPartyType,
        ERP.EntryDate AS ebillRegistrationPartyTypeEntryDate,
        ERT.businessPartnerIdNumber AS ebillRegistrationTelephoneNumber,
        ERT.EntryDate AS ebillRegistrationTelephoneNumberEntryDate,
        ERE.businessPartnerIdNumber AS ebillRegistrationEmail,
        ERE.EntryDate AS ebillRegistrationEmailEntryDate,
        DN.businessPartnerIdNumber AS dealingNumber,
        DN.EntryDate AS dealingNumberEntryDate,
        DD.businessPartnerIdNumber AS dealingDate,
        DD.EntryDate AS dealingDateEntryDate,
        DT.businessPartnerIdNumber AS dealingType,
        DT.EntryDate AS dealingTypeEntryDate,
        DA.businessPartnerIdNumber AS dealingAmount,
        DA.EntryDate AS dealingAmontEntryDate,
        OID.businessPartnerIdNumber AS onlineID,
        OID.EntryDate AS onlineIDEntryDate,
        UP.businessPartnerIdNumber AS userPassword,
        UP.EntryDate AS userPasswordEntryDate,
        PB.businessPartnerIdNumber AS placeofBirth,
        PB.EntryDate AS placeofBirthEntryDate,
        PET.businessPartnerIdNumber AS petsName,
        PET.EntryDate AS petsNameEntryDate,
        MFN.businessPartnerIdNumber AS mothersFirstName,
        MFN.EntryDate AS mothersFirstNameEntryDate,
        MMN.businessPartnerIdNumber AS mothersMaidenName,
        MMN.EntryDate AS mothersMaidenNameEntryDate,
        FFN.businessPartnerIdNumber AS fathersFirstName,
        FFN.EntryDate AS fathersFirstNameEntryDate,
        LUID.businessPartnerIdNumber AS labwareUserId,
        LUID.ValidFromDate AS labwareUserIdValidFrom,
        LUID.ValidToDate AS labwareUserIdValidTo,
        LUID.EntryDate AS labwareUserIdEntryDate,
        RPSID.businessPartnerIdNumber AS rasPortalServiceId,
        RPSID.ValidFromDate AS rasPortalServiceIdValidFrom,
        RPSID.ValidToDate AS rasPortalServiceIdValidTo,
        RPSID.EntryDate AS rasPortalServiceIdEntryDate,
        ID.businessPartnerIdNumber AS identityCard,
        ID.ValidFromDate AS identityCardValidFrom,
        ID.ValidToDate AS identityCardValidTo,
        ID.EntryDate AS identityCardEntryDate,
        IMS.businessPartnerIdNumber AS imsNumber,
        IMS.EntryDate AS imsNumberEntryDate,
        ESICM.businessPartnerIdNumber AS externalSystemIndicatorForIcm,
        ESICM.EntryDate AS externalSystemIndicatorForIcmEntryDate,
        ESI.businessPartnerIdNumber AS externalSystemIdentifier,
        ESI.EntryDate AS externalSystemIdentifierEntryDate
    FROM businessPartnerNumber BP
    LEFT OUTER JOIN curated_v2.dimBusinessPartnerIdentification DL 
        ON DL.sourceSystemCode = BP.sourceSystemCode AND DL.businessPartnerNumber = BP.businessPartnerNumber AND DL.identificationType = 'Drivers License Number'
    LEFT OUTER JOIN curated_v2.dimBusinessPartnerIdentification P 
        ON P.sourceSystemCode = BP.sourceSystemCode AND P.businessPartnerNumber = BP.businessPartnerNumber AND P.identificationType = 'Passport'
    LEFT OUTER JOIN curated_v2.dimBusinessPartnerIdentification PN 
        ON PN.sourceSystemCode = BP.sourceSystemCode AND PN.businessPartnerNumber = BP.businessPartnerNumber AND PN.identificationType =  'Pensioner#' 
    LEFT OUTER JOIN curated_v2.dimBusinessPartnerIdentification ABN 
        ON ABN.sourceSystemCode = BP.sourceSystemCode AND ABN.businessPartnerNumber = BP.businessPartnerNumber AND ABN.identificationType = 'Australian Business Number'
    LEFT OUTER JOIN curated_v2.dimBusinessPartnerIdentification ACN 
        ON ACN.sourceSystemCode = BP.sourceSystemCode AND ACN.businessPartnerNumber = BP.businessPartnerNumber AND ACN.identificationType = 'Australian Company Number'
    LEFT OUTER JOIN curated_v2.dimBusinessPartnerIdentification DVA 
        ON DVA.sourceSystemCode = BP.sourceSystemCode AND DVA.businessPartnerNumber = BP.businessPartnerNumber AND DVA.identificationType = 'DVA#'
    LEFT OUTER JOIN curated_v2.dimBusinessPartnerIdentification CRN 
        ON CRN.sourceSystemCode = BP.sourceSystemCode AND CRN.businessPartnerNumber = BP.businessPartnerNumber AND CRN.identificationType = 'Commercial Register Number' 
    LEFT OUTER JOIN curated_v2.dimBusinessPartnerIdentification TLN 
        ON TLN.sourceSystemCode = BP.sourceSystemCode AND TLN.businessPartnerNumber = BP.businessPartnerNumber AND TLN.identificationType = 'Trade License Number' 
    LEFT OUTER JOIN curated_v2.dimBusinessPartnerIdentification DBT 
        ON DBT.sourceSystemCode = BP.sourceSystemCode AND DBT.businessPartnerNumber = BP.businessPartnerNumber AND DBT.identificationType = 'Direct Debit Telephone Number' 
    LEFT OUTER JOIN curated_v2.dimBusinessPartnerIdentification DBE 
        ON DBE.sourceSystemCode = BP.sourceSystemCode AND DBE.businessPartnerNumber = BP.businessPartnerNumber AND DBE.identificationType = 'Direct Debit Email ID' 
    LEFT OUTER JOIN curated_v2.dimBusinessPartnerIdentification ERP 
        ON ERP.sourceSystemCode = BP.sourceSystemCode AND ERP.businessPartnerNumber = BP.businessPartnerNumber AND ERP.identificationType = 'Ebill registration Party Type' 
    LEFT OUTER JOIN curated_v2.dimBusinessPartnerIdentification ERT 
        ON ERT.sourceSystemCode = BP.sourceSystemCode AND ERT.businessPartnerNumber = BP.businessPartnerNumber AND ERT.identificationType =  'Ebilll registration Telephone Number' 
    LEFT OUTER JOIN curated_v2.dimBusinessPartnerIdentification ERE 
        ON ERE.sourceSystemCode = BP.sourceSystemCode AND ERE.businessPartnerNumber = BP.businessPartnerNumber AND ERE.identificationType = 'Ebill registration Email ID' 
    LEFT OUTER JOIN curated_v2.dimBusinessPartnerIdentification DN 
        ON DN.sourceSystemCode = BP.sourceSystemCode AND DN.businessPartnerNumber = BP.businessPartnerNumber AND DN.identificationType = 'Dealing Number' 
    LEFT OUTER JOIN curated_v2.dimBusinessPartnerIdentification DD 
        ON DD.sourceSystemCode = BP.sourceSystemCode AND DD.businessPartnerNumber = BP.businessPartnerNumber AND DD.identificationType = 'Dealing Date' 
    LEFT OUTER JOIN curated_v2.dimBusinessPartnerIdentification DT 
        ON DT.sourceSystemCode = BP.sourceSystemCode AND DT.businessPartnerNumber = BP.businessPartnerNumber AND DT.identificationType = 'Dealing Type' 
    LEFT OUTER JOIN curated_v2.dimBusinessPartnerIdentification DA 
        ON dA.sourceSystemCode = BP.sourceSystemCode AND DA.businessPartnerNumber = BP.businessPartnerNumber AND DA.identificationType = 'Dealing Amount' 
    LEFT OUTER JOIN curated_v2.dimBusinessPartnerIdentification OID 
        ON OID.sourceSystemCode = BP.sourceSystemCode AND OID.businessPartnerNumber = BP.businessPartnerNumber AND OID.identificationType =  'Online ID' 
    LEFT OUTER JOIN curated_v2.dimBusinessPartnerIdentification UP 
        ON UP.sourceSystemCode = BP.sourceSystemCode AND UP.businessPartnerNumber = BP.businessPartnerNumber AND UP.identificationType = 'Password' 
    LEFT OUTER JOIN curated_v2.dimBusinessPartnerIdentification PB 
        ON PB.sourceSystemCode = BP.sourceSystemCode AND PB.businessPartnerNumber = BP.businessPartnerNumber AND PB.identificationType =  'Place of Birth' 
    LEFT OUTER JOIN curated_v2.dimBusinessPartnerIdentification PET 
        ON PET.sourceSystemCode = BP.sourceSystemCode AND PET.businessPartnerNumber = BP.businessPartnerNumber AND PET.identificationType = "Pet's Name"
    LEFT OUTER JOIN curated_v2.dimBusinessPartnerIdentification MFN 
        ON MFN.sourceSystemCode = BP.sourceSystemCode AND MFN.businessPartnerNumber = BP.businessPartnerNumber AND MFN.identificationType = "Mother's First Name"
    LEFT OUTER JOIN curated_v2.dimBusinessPartnerIdentification MMN 
        ON MMN.sourceSystemCode = BP.sourceSystemCode AND MMN.businessPartnerNumber = BP.businessPartnerNumber AND MMN.identificationType = "Mother's Maiden Name"
    LEFT OUTER JOIN curated_v2.dimBusinessPartnerIdentification FFN 
        ON FFN.sourceSystemCode = BP.sourceSystemCode AND FFN.businessPartnerNumber = BP.businessPartnerNumber AND FFN.identificationType = "Father's First Name"
    LEFT OUTER JOIN curated_v2.dimBusinessPartnerIdentification LUID 
        ON LUID.sourceSystemCode = BP.sourceSystemCode AND LUID.businessPartnerNumber = BP.businessPartnerNumber AND LUID.identificationType = 'Labware User ID' 
    LEFT OUTER JOIN curated_v2.dimBusinessPartnerIdentification RPSID 
        ON RPSID.sourceSystemCode = BP.sourceSystemCode AND RPSID.businessPartnerNumber = BP.businessPartnerNumber AND RPSID.identificationType = 'RAS Portal Service ID' 
    LEFT OUTER JOIN curated_v2.dimBusinessPartnerIdentification ID 
        ON ID.sourceSystemCode = BP.sourceSystemCode AND ID.businessPartnerNumber = BP.businessPartnerNumber AND ID.identificationType = 'Identity card' 
    LEFT OUTER JOIN curated_v2.dimBusinessPartnerIdentification IMS 
        ON IMS.sourceSystemCode = BP.sourceSystemCode AND IMS.businessPartnerNumber = BP.businessPartnerNumber AND IMS.identificationType = 'IMS Number' 
    LEFT OUTER JOIN curated_v2.dimBusinessPartnerIdentification ESICM 
        ON ESICM.sourceSystemCode = BP.sourceSystemCode AND ESICM.businessPartnerNumber = BP.businessPartnerNumber AND ESICM.identificationType = 'External System Indicator for ICM' 
    LEFT OUTER JOIN curated_v2.dimBusinessPartnerIdentification ESI 
        ON ESI.sourceSystemCode = BP.sourceSystemCode AND ESI.businessPartnerNumber = BP.businessPartnerNumber AND ESI.identificationType = 'External System Identifier' 

-- COMMAND ----------

-- View: view_businesspartnergroup
-- Description: view_businesspartnergroup

CREATE OR REPLACE VIEW curated_v2.view_businesspartnergroup AS 
WITH 
    businessPartnerNumber AS (
        SELECT DISTINCT
            sourceSystemCode,
            businessPartnerNumber
        FROM curated_v2.dimBusinessPartnerIdentification BP
    ),

    ID AS (
        SELECT
            BP.sourceSystemCode             AS sourceSystemCode,
            BP.businessPartnerNumber        AS businessPartnerNumber, 
            ERP.businessPartnerIdNumber     AS ebillRegistrationPartyType,
            ERT.businessPartnerIdNumber     AS ebillRegistrationTelephoneNumber,
            ERE.businessPartnerIdNumber     AS ebillRegistrationEmail,
            ERE.EntryDate                   AS ebillRegistrationEmailEntryDate,
            DN.businessPartnerIdNumber      AS dealingNumber,
            DT.businessPartnerIdNumber      AS dealingType,
            DA.businessPartnerIdNumber      AS dealingAmount,
            DD.businessPartnerIdNumber      AS dealingDate,
            DBT.businessPartnerIdNumber     AS directDebitTelephoneNumber,
            DBE.businessPartnerIdNumber     AS directDebitEmail,
            OID.businessPartnerIdNumber     AS onlineID,
            UP.businessPartnerIdNumber      AS userPassword
        FROM businessPartnerNumber BP
        LEFT OUTER JOIN curated_v2.dimBusinessPartnerIdentification DBT 
            ON DBT.sourceSystemCode = BP.sourceSystemCode AND DBT.businessPartnerNumber = BP.businessPartnerNumber AND DBT.identificationType = 'Direct Debit Telephone Number' 
        LEFT OUTER JOIN curated_v2.dimBusinessPartnerIdentification DBE 
            ON DBE.sourceSystemCode = BP.sourceSystemCode AND DBE.businessPartnerNumber = BP.businessPartnerNumber AND DBE.identificationType = 'Direct Debit Email ID' 
        LEFT OUTER JOIN curated_v2.dimBusinessPartnerIdentification ERP 
            ON ERP.sourceSystemCode = BP.sourceSystemCode AND ERP.businessPartnerNumber = BP.businessPartnerNumber AND ERP.identificationType = 'Ebill registration Party Type' 
        LEFT OUTER JOIN curated_v2.dimBusinessPartnerIdentification ERT 
            ON ERT.sourceSystemCode = BP.sourceSystemCode AND ERT.businessPartnerNumber = BP.businessPartnerNumber AND ERT.identificationType =  'Ebilll registration Telephone Number' 
        LEFT OUTER JOIN curated_v2.dimBusinessPartnerIdentification ERE 
            ON ERE.sourceSystemCode = BP.sourceSystemCode AND ERE.businessPartnerNumber = BP.businessPartnerNumber AND ERE.identificationType = 'Ebill registration Email ID' 
        LEFT OUTER JOIN curated_v2.dimBusinessPartnerIdentification DN 
            ON DN.sourceSystemCode = BP.sourceSystemCode AND DN.businessPartnerNumber = BP.businessPartnerNumber AND DN.identificationType = 'Dealing Number' 
        LEFT OUTER JOIN curated_v2.dimBusinessPartnerIdentification DD 
            ON DD.sourceSystemCode = BP.sourceSystemCode AND DD.businessPartnerNumber = BP.businessPartnerNumber AND DD.identificationType = 'Dealing Date' 
        LEFT OUTER JOIN curated_v2.dimBusinessPartnerIdentification DT 
            ON DT.sourceSystemCode = BP.sourceSystemCode AND DT.businessPartnerNumber = BP.businessPartnerNumber AND DT.identificationType = 'Dealing Type' 
        LEFT OUTER JOIN curated_v2.dimBusinessPartnerIdentification DA 
            ON dA.sourceSystemCode = BP.sourceSystemCode AND DA.businessPartnerNumber = BP.businessPartnerNumber AND DA.identificationType = 'Dealing Amount' 
        LEFT OUTER JOIN curated_v2.dimBusinessPartnerIdentification OID 
            ON OID.sourceSystemCode = BP.sourceSystemCode AND OID.businessPartnerNumber = BP.businessPartnerNumber AND OID.identificationType =  'Online ID' 
        LEFT OUTER JOIN curated_v2.dimBusinessPartnerIdentification UP 
            ON UP.sourceSystemCode = BP.sourceSystemCode AND UP.businessPartnerNumber = BP.businessPartnerNumber AND UP.identificationType = 'Password' 
    )
    
SELECT 
    BPG.businessPartnerGroupSK,
    BPG.sourceSystemCode,
    BPG.businessPartnerGroupNumber,
    BPG.businessPartnerGroupCode,
    BPG.businessPartnerGroup,
    BPG.businessPartnerCategoryCode,
    BPG.businessPartnerCategory,
    BPG.businessPartnerTypeCode,
    BPG.businessPartnerType,
    BPG.externalNumber,
    BPG.businessPartnerGUID,
    BPG.businessPartnerGroupName1,
    BPG.businessPartnerGroupName2,
    ADDR.addressNumber,
    ADDR.addressValidFromDate,
    ADDR.addressValidToDate,
    ADDR.coName,
    ADDR.streetLine5,
    ADDR.building,
    ADDR.floorNumber,
    ADDR.apartmentNumber,
    ADDR.houseNumber,
    ADDR.houseSupplementNumber, -- houseNumber2
    ADDR.streetName,
    ADDR.streetSupplementName1, -- streetType
    ADDR.streetSupplementName2, -- streetLine3
    ADDR.otherLocationName, -- streetLine4
    ADDR.streetCode,
    ADDR.cityName,
    ADDR.cityCode,
    ADDR.postalCode,
    ADDR.stateCode,
    ADDR.countryShortName,
    ADDR.countryName,
    ADDR.poBoxCode,
    ADDR.poBoxCity,
    ADDR.postalCodeExtension,
    ADDR.poBoxExtension,
    ADDR.deliveryServiceTypeCode,
    ADDR.deliveryServiceType, -- deliveryServiceDescription
    ADDR.deliveryServiceNumber,
    ADDR.addressTimeZone,
    ADDR.communicationAddressNumber,
    ADDR.phoneNumber,
    ADDR.faxNumber,
    ADDR.emailAddress,
    BPG.paymentAssistSchemeFlag,
    BPG.billAssistFlag,
    BPG.consent1Indicator,
    BPG.warWidowFlag,
    BPG.indicatorCreatedUserId,
    BPG.indicatorCreatedDate,
    BPG.kidneyDialysisFlag,
    BPG.patientUnit,
    BPG.patientTitleCode,
    BPG.patientTitle,
    BPG.patientFirstName,
    BPG.patientSurname,
    BPG.patientAreaCode,
    BPG.patientPhoneNumber,
    BPG.hospitalCode,
    BPG.hospitalName,
    BPG.patientMachineTypeCode,
    BPG.patientMachineType,
    BPG.machineTypeValidFromDate,
    BPG.machineTypeValidToDate,
    BPG.machineOffReasonCode,
    BPG.machineOffReason,
    BPG.createdBy,
    BPG.createdDateTime,
    BPG.lastUpdatedBy,
    BPG.lastUpdatedDateTime,
    BPG.validFromDate,
    BPG.validToDate,
    ID.ebillRegistrationPartyType,
    ID.ebillRegistrationTelephoneNumber,
    ID.ebillRegistrationEmail,
    ID.ebillRegistrationEmailEntryDate,
    ID.dealingNumber,
    ID.dealingType,
    ID.dealingAmount,
    ID.dealingDate,
    ID.directDebitTelephoneNumber,
    ID.directDebitEmail,
    ID.onlineId,
    ID.userPassword
FROM curated_v2.dimbusinesspartnergroup BPG
LEFT JOIN curated_v2.dimbusinesspartneraddress ADDR ON 
    ADDR.businessPartnerNumber = BPG.businessPartnerGroupNumber AND
    ADDR.sourceSystemCode = BPG.sourceSystemCode 
LEFT JOIN ID ON 
    ID.businessPartnerNumber = BPG.businessPartnerGroupNumber AND
    ID.sourceSystemCode = BPG.sourceSystemCode 
WHERE 
    BPG._recordCurrent = 1 AND
    ADDR._recordCurrent = 1

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("1")
