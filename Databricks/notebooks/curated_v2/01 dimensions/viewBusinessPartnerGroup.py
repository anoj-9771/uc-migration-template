# Databricks notebook source
notebookPath = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split("/")
view = notebookPath[-1:][0]
db = notebookPath[-3:][0]

spark.sql("""
-- View: viewBusinessPartnerGroup
-- Description: viewBusinessPartnerGroup
CREATE OR REPLACE VIEW curated_v2.viewBusinessPartnerGroup AS
/*** 
/*================================================================================================
			all_ID
				-> _rank: used to only bring 1 businesspartner ID per identification type
				-> _currentIndicator: Used to flag whether ID validFrom and To dates are between the current date
                -> Filter CURRENT_DATE() BETWEEN _RecordStart AND _RecordEnd
		 ================================================================================================*/
with all_ID AS (
  SELECT
    *,
    RANK() OVER (
      PARTITION BY businessPartnerNumber,
      identificationType
      ORDER BY
        ifnull(validToDate, '9999-01-01') DESC,
        ifnull(entryDate, '1900-01-01') DESC
    ) AS _rank,
    CASE
      WHEN CURRENT_DATE() BETWEEN ifnull(ID.validFromDate, '1900-01-01')
      AND ifnull(ID.validToDate, '9999-01-01') THEN 'Y'
      ELSE 'N'
    END AS _currentIndicator
  FROM
    curated_v2.dimBusinessPartnerIdentification ID 
),
/*=====================================================================================
		valid_ID
			-> Apply Filters: _rank = 1, _currentIndicator = 1
			-> filter to identificationType's in scope for the view
	 ===================================================================================*/
valid_ID AS (
  SELECT
    businessPartnerNumber,
    businessPartnerIdNumber,
    identificationType,
    validFromDate,
    validToDate,
    entryDate,
    institute,
    _recordStart,
    _recordEnd
  FROM
    all_ID
  WHERE
    all_ID._rank = 1
    AND all_ID._currentIndicator = 'Y'
),
/*========================================
	BusinessPartnerIdentification
		-> applying a pivot to transpose
==========================================*/
businessPartnerIdentification AS (
  SELECT
    businessPartnerNumber AS businessPartnerNumber,
    Drivers_License_Number_bpId AS driversLicenseNumber,
    Drivers_License_Number_ValidFrom AS driversLicenseNumberValidFrom,
    Drivers_License_Number_ValidTo AS driversLicenseNumberValidTo,
    Drivers_License_Number_entryDate AS driversLicenseNumberEntryDate,
    Passport_bpId AS passportNumber,
    Passport_ValidFrom AS passportNumberValidFrom,
    Passport_ValidTo AS passportNumberValidTo,
    Passport_entryDate AS passportNumberEntryDate,
    Pensioner_no_bpId AS pensionNumber,
    Pensioner_no_ValidFrom AS pensionNumberValidFrom,
    Pensioner_no_ValidTo AS pensionNumberValidTo,
    Pensioner_no_entryDate AS pensionNumberEntryDate,
    Australian_Business_Number_bpId AS australianBusinessNumber,
    Australian_Business_Number_ValidFrom AS australianBusinessNumberValidFrom,
    Australian_Business_Number_ValidTo AS australianBusinessNumberValidTo,
    Australian_Business_Number_entryDate AS australianBusinessNumberEntryDate,
    Australian_Company_Number_bpId AS australianCompanyNumber,
    Australian_Company_Number_ValidFrom AS australianCompanyNumberValidFrom,
    Australian_Company_Number_ValidTo AS australianCompanyNumberValidTo,
    Australian_Company_Number_entryDate AS australianCompanyNumberEntryDate,
    DVA_no_bpId AS dvaNumber,
    DVA_no_ValidFrom AS dvaNumberValidFrom,
    DVA_no_ValidTo AS dvaNumberValidTo,
    DVA_no_entryDate AS dvaNumberEntryDate,
    Commercial_Register_Number_bpId AS commercialRegisterNumber,
    Commercial_Register_Number_ValidFrom AS commercialRegisterNumberValidFrom,
    Commercial_Register_Number_ValidTo AS commercialRegisterNumberValidTo,
    Commercial_Register_Number_entryDate AS commercialRegisterNumberEntryDate,
    Trade_License_Number_bpId AS tradeLicenseNumber,
    Trade_License_Number_ValidFrom AS tradeLicenseNumberValidFrom,
    Trade_License_Number_ValidTo AS tradeLicenseNumberValidTo,
    Trade_License_Number_entryDate AS tradeLicenseNumberEntryDate,
    Direct_Debit_Telephone_Number_bpId AS directDebitTelephoneNumber,
    Direct_Debit_Telephone_Number_entryDate AS directDebitTelephoneNumberEntryDate,
    Direct_Debit_Email_ID_bpId AS directDebitEmail,
    Direct_Debit_Email_ID_entryDate AS directDebitEmailEntryDate,
    Ebill_registration_Party_Type_bpId AS ebillRegistrationPartyType,
    Ebill_registration_Party_Type_entryDate AS ebillRegistrationPartyTypeEntryDate,
    Ebill_registration_Telephone_Number_bpId AS ebillRegistrationTelephoneNumber,
    Ebill_registration_Telephone_Number_entryDate AS ebillRegistrationTelephoneNumberEntryDate,
    Ebill_registration_Email_ID_bpId AS ebillRegistrationEmail,
    Ebill_registration_Email_ID_entryDate AS ebillRegistrationEmailEntryDate,
    Dealing_Number_bpId AS dealingNumber,
    Dealing_Number_entryDate AS dealingNumberEntryDate,
    Dealing_Date_bpId AS dealingDate,
    Dealing_Date_entryDate AS dealingDateEntryDate,
    Dealing_Type_bpId AS dealingType,
    Dealing_Type_entryDate AS dealingTypeEntryDate,
    Dealing_Amount_bpId AS dealingAmount,
    Dealing_Amount_entryDate AS dealingAmountEntryDate,
    Online_ID_bpId AS onlineID,
    Online_ID_entryDate AS onlineIDEntryDate,
    Password_bpId AS userPassword,
    Password_entryDate AS userPasswordEntryDate,
    Place_of_Birth_bpId AS placeofBirth,
    Place_of_Birth_entryDate AS placeofBirthEntryDate,
    Pets_Name_bpId AS petsName,
    Pets_Name_entryDate AS petsNameEntryDate,
    Mothers_First_Name_bpId AS mothersFirstName,
    Mothers_First_Name_entryDate AS mothersFirstNameEntryDate,
    Mothers_Maiden_Name_bpId AS mothersMaidenName,
    Mothers_Maiden_Name_entryDate AS mothersMaidenNameEntryDate,
    Fathers_First_Name_bpId AS fathersFirstName,
    Fathers_First_Name_entryDate AS fathersFirstNameEntryDate,
    Labware_User_ID_bpId AS labwareUserId,
    Labware_User_ID_ValidFrom AS labwareUserIdValidFrom,
    Labware_User_ID_ValidTo AS labwareUserIdValidTo,
    Labware_User_ID_entryDate AS labwareUserIdEntryDate,
    RAS_Portal_Service_ID_bpId AS rasPortalServiceId,
    RAS_Portal_Service_ID_ValidFrom AS rasPortalServiceIdValidFrom,
    RAS_Portal_Service_ID_ValidTo AS rasPortalServiceIdValidTo,
    RAS_Portal_Service_ID_entryDate AS rasPortalServiceIdEntryDate,
    Identity_card_bpId AS identityCard,
    Identity_card_ValidFrom AS identityCardValidFrom,
    Identity_card_ValidTo AS identityCardValidTo,
    Identity_card_entryDate AS identityCardEntryDate,
    IMS_Number_bpId AS imsNumber,
    IMS_Number_entryDate AS imsNumberEntryDate,
    External_System_Indicator_for_ICM_bpId AS externalSystemIndicatorForIcm,
    External_System_Indicator_for_ICM_entryDate AS externalSystemIndicatorForIcmEntryDate,
    External_System_Identifier_bpId AS externalSystemIdentifier,
    External_System_Identifier_entryDate AS externalSystemIdentifierEntryDate
  FROM
    valid_id PIVOT (
      MIN(businessPartnerIdNumber) AS bpId,
      MIN(validFromDate) AS validFrom,
      MIN(validToDate) AS validTo,
      MIN(entryDate) AS entryDate,
      MIN(institute) AS institute,
      MIN(_recordStart) AS _recordStart,
      MIN(_recordEnd) AS _recordEnd FOR identificationType IN (
        /* IDs in Scope */
        'Drivers License Number' AS Drivers_License_Number,
        'Passport' AS Passport,
        'Pensioner_no' AS Pensioner_no,
        'Australian Business Number' AS Australian_Business_Number,
        'Australian Company Number' AS Australian_Company_Number,
        'DVA_no' AS DVA_no,
        'Commercial Register Number' AS Commercial_Register_Number,
        'Trade License Number' AS Trade_License_Number,
        'Direct Debit Telephone Number' AS Direct_Debit_Telephone_Number,
        'Direct Debit Email ID' AS Direct_Debit_Email_ID,
        'Ebill registration Party Type' AS Ebill_registration_Party_Type,
        'Ebill registration Telephone Number' AS Ebill_registration_Telephone_Number,
        'Ebill registration Email ID' AS Ebill_registration_Email_ID,
        'Dealing Number' AS Dealing_Number,
        'Dealing Date' AS Dealing_Date,
        'Dealing Type' AS Dealing_Type,
        'Dealing Amount' AS Dealing_Amount,
        'Online ID' AS Online_ID,
        'Password' AS Password,
        'Place of Birth' AS Place_of_Birth,
        "Pet's Name" AS Pets_Name,
        "Mother's First Name" AS Mothers_First_Name,
        "Mother's Maiden Name" AS Mothers_Maiden_Name,
        "Father's First Name" AS Fathers_First_Name,
        "Labware User ID" AS Labware_User_ID,
        "RAS Portal Service ID" AS RAS_Portal_Service_ID,
        "Identity card" AS Identity_card,
        "IMS Number" AS IMS_Number,
        "External System Indicator for ICM" AS External_System_Indicator_for_ICM,
        "External System Identifier" AS External_System_Identifier
      )
    )
),

***/
/*==============================
          Effective From and To Dates
      ================================*/

with dimBusinessPartnerGroupRanges AS
(
                SELECT
                businessPartnerGroupNumber, 
                _recordStart, 
                _recordEnd,
                COALESCE( LEAD( _recordStart, 1 ) OVER( PARTITION BY businessPartnerGroupNumber ORDER BY _recordStart ), 
                  CASE WHEN _recordEnd < cast('9999-12-31T23:59:59' as timestamp) then _recordEnd + INTERVAL 1 SECOND else _recordEnd end) AS _newRecordEnd
                FROM curated_v2.dimBusinessPartnerGroup
                --WHERE _recordDeleted = 0
),

dimBusinessPartnerAddressRanges AS
(
                SELECT
                businessPartnerNumber as businessPartnerGroupNumber, 
                _recordStart, 
                _recordEnd,
                COALESCE( LEAD( _recordStart, 1 ) OVER( PARTITION BY businessPartnerNumber ORDER BY _recordStart ), 
                  CASE WHEN _recordEnd < cast('9999-12-31T23:59:59' as timestamp) then _recordEnd + INTERVAL 1 SECOND else _recordEnd end) AS _newRecordEnd
                FROM curated_v2.dimBusinessPartnerAddress
                --WHERE _recordDeleted = 0
),

dateDriver AS
(
                SELECT businessPartnerGroupNumber, _recordStart from dimBusinessPartnerGroupRanges
                UNION
                SELECT businessPartnerGroupNumber, _newRecordEnd as _recordStart from dimBusinessPartnerGroupRanges
                UNION
                SELECT businessPartnerGroupNumber, _recordStart from dimBusinessPartnerAddressRanges
                UNION
                SELECT businessPartnerGroupNumber, _newRecordEnd as _recordStart from dimBusinessPartnerAddressRanges
),

effectiveDateRanges AS
(
                SELECT 
                businessPartnerGroupNumber, 
                _recordStart AS _effectiveFrom,
                COALESCE( LEAD( _recordStart, 1 ) OVER( PARTITION BY businessPartnerGroupNumber ORDER BY _recordStart ) - INTERVAL 1 SECOND, 
                  cast( '9999-12-31T23:59:59' as timestamp ) ) AS _effectiveTo                           
                FROM dateDriver where _recordStart < cast('9999-12-31T23:59:59' as timestamp)                            
)              

/*============================
      viewBusinessPartnerGroup
  ==============================*/
SELECT
  *
FROM
  (
    SELECT
      /* Business Partner Group Columns */
      --BPG.businessPartnerGroupSK,
      --ADDR.businessPartnerAddressSK,
      BPG.sourceSystemCode,
      coalesce(
        BPG.businessPartnerGroupNumber,
        ADDR.businessPartnerNumber,
        --ID.businessPartnerNumber,
        -1
      ) as businessPartnerGroupNumber,
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
      BPG.validFromDate AS businessPartnerGroupValidFromDate,
      BPG.validToDate AS businessPartnerGroupValidToDate,
      /* Address Columns */
      ADDR.businessPartnerAddressNumber,
      ADDR.addressValidFromDate,
      ADDR.addressValidToDate,
      ADDR.coName,
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
      ADDR.streetCode,
      ADDR.cityName,
      ADDR.cityCode,
      ADDR.postalCode,
      ADDR.stateCode,
      ADDR.countryCode,
      ADDR.countryName,
      ADDR.poBoxCode,
      ADDR.poBoxCity,
      ADDR.postalCodeExtension,
      ADDR.poBoxExtension,
      ADDR.deliveryServiceTypeCode,
      ADDR.deliveryServiceType,
      ADDR.deliveryServiceNumber,
      ADDR.addressTimeZone,
      ADDR.communicationAddressNumber,
      ADDR.phoneNumber,
      ADDR.faxNumber,
      ADDR.emailAddress,
      /* BP ID columns */
      --ID.ebillRegistrationPartyType,
      --ID.ebillRegistrationTelephoneNumber,
      --ID.ebillRegistrationEmail,
      --ID.ebillRegistrationEmailEntryDate,
      --ID.dealingNumber,
      --ID.dealingType,
      --ID.dealingAmount,
      --ID.dealingDate,
      --ID.directDebitTelephoneNumber,
      --ID.directDebitEmail,
      DR._effectiveFrom,
      DR._effectiveTo, 
      CASE
        WHEN CURRENT_TIMESTAMP() BETWEEN DR._effectiveFrom
        AND DR._effectiveTo then 'Y'
        ELSE 'N'
      END AS currentFlag,
      if(BPG._RecordDeleted = 0,'Y','N') AS currentRecordFlag 
    FROM
      effectiveDateRanges DR
      LEFT JOIN curated_v2.dimBusinessPartnerGroup BPG ON DR.businessPartnerGroupNumber = BPG.businessPartnerGroupNumber
      AND DR._effectiveFrom <= BPG._RecordEnd
      AND DR._effectiveTo >= BPG._RecordStart
      --AND BPG._recordDeleted = 0
      LEFT JOIN curated_v2.dimbusinesspartneraddress ADDR ON DR.businessPartnerGroupNumber = ADDR.businessPartnerNumber
      AND DR._effectiveFrom <= ADDR._RecordEnd
      AND DR._effectiveTo >= ADDR._RecordStart
      --AND ADDR._recordDeleted = 0
      --LEFT JOIN businessPartnerIdentification ID ON DR.businessPartnerGroupNumber = ID.businessPartnerNumber
    WHERE
      businessPartnerGroupSK IS NOT NULL
  )
  """.replace("CREATE OR REPLACE VIEW", "ALTER VIEW" if spark.sql(f"SHOW VIEWS FROM {db} LIKE '{view}'").count() == 1 else "CREATE OR REPLACE VIEW"))
