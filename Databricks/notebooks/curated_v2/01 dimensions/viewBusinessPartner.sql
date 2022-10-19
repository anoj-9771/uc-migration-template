-- Databricks notebook source
-- MAGIC %md 
-- MAGIC # Business Partner Identification

-- COMMAND ----------

-- View: viewBusinessPartnerIdentification
-- Description: viewBusinessPartnerIdentification

CREATE OR REPLACE VIEW curated_v2.viewBusinessPartnerIdentification AS 

WITH all_ID AS (
		/*================================================================================================
			All IDs
				-> _rank: used to only bring 1 businesspartner ID per identification type
				-> _currentIndicator: Used to flag whether ID validFrom and To dates are between the current date
                -> Filter to CURRENT_DATE() BETWEEN _RecordStart AND _RecordEnd
		 ================================================================================================*/
		SELECT
        *,
        RANK() OVER (PARTITION BY sourceSystemCode, businessPartnerNumber, identificationType ORDER BY ifnull(validToDate, '9999-01-01') DESC, ifnull(entryDate, '1900-01-01') DESC) AS _rank,
        CASE 
            WHEN CURRENT_DATE() BETWEEN ifnull(ID.validFromDate, '1900-01-01') AND ifnull(ID.validToDate, '9999-01-01') 
            THEN 'Y'
            ELSE 'N'
        END AS _currentIndicator
     FROM curated_v2.dimBusinessPartnerIdentification ID
     WHERE _recordDeleted <> 1

),
	/*=====================================================================================
		valid_ID
			-> Apply Filters: _rank = 1, _currentIndicator = 1
			-> filter to identificationType's in scope for the view
	 ===================================================================================*/
	valid_ID AS (
	SELECT * FROM all_ID 
	WHERE all_ID._rank = 1 AND all_ID._currentIndicator = 'Y' AND
	/* IDs in Scope */
	all_ID.identificationType IN (
		'Drivers License Number',
		'Passport',
		'Pensioner_no',
		'Australian Business Number',
		'Australian Company Number',
		'DVA_no',
		'Commercial Register Number',
		'Trade License Number',
		'Direct Debit Telephone Number',
		'Direct Debit Email ID',
		'Ebill registration Party Type',
		'Ebill registration Telephone Number',
		'Ebill registration Email ID',
		'Dealing Number',
		'Dealing Date',
		'Dealing Type',
		'Dealing Amount',
		'Online ID',
		'Password',
		'Place of Birth',
		"Pet's Name",
		"Mother's First Name",
		"Mother's Maiden Name",
		"Father's First Name",
		"Labware User ID",
		'RAS Portal Service ID',
		'Identity card',
		'IMS Number',
		'External System Indicator for ICM',
		'External System Identifier'
	)
)

/*========================================
	view_businessPartnerIdentification
		-> applying a pivot to transpose
==========================================*/
SELECT * FROM (
	SELECT	
		sourceSystemCode                                                                                 AS sourceSystemCode,
		businessPartnerNumber                                                                            AS businessPartnerNumber,
		--
		Drivers_License_Number                                                                           AS driverLicenseNumber,
		CASE WHEN Drivers_License_Number IS NULL THEN NULL ELSE ValidFromDate END                        AS driverLicenseNumberValidFrom,
		CASE WHEN Drivers_License_Number IS NULL THEN NULL ELSE ValidToDate END                          AS driverLicenseNumberValidTo,
		CASE WHEN Drivers_License_Number IS NULL THEN NULL ELSE EntryDate END                            AS driverLicenseNumberEntryDate,
		--
		Passport                                                                                         AS passportNumber,
		CASE WHEN Passport IS NULL THEN NULL ELSE ValidFromDate END                                      AS passportNumberValidFrom,
		CASE WHEN Passport IS NULL THEN NULL ELSE ValidToDate END                                        AS passportNumberValidTo,
		CASE WHEN Passport IS NULL THEN NULL ELSE EntryDate END                                          AS passportNumberEntryDate,
		--
		Pensioner_no                                                                                     AS pensionNumber,
		CASE WHEN Pensioner_no IS NULL THEN NULL ELSE ValidFromDate END                                  AS pensionNumberValidFrom,
		CASE WHEN Pensioner_no IS NULL THEN NULL ELSE ValidToDate END                                    AS pensionNumberValidTo,
		CASE WHEN Pensioner_no IS NULL THEN NULL ELSE EntryDate END                                      AS pensionNumberEntryDate,
		--
		Australian_Business_Number                                                                       AS australianBusinessNumber,
		CASE WHEN Australian_Business_Number IS NULL THEN NULL ELSE ValidFromDate END                    AS australianBusinessNumberValidFrom,
		CASE WHEN Australian_Business_Number IS NULL THEN NULL ELSE ValidToDate END                      AS australianBusinessNumberValidTo,
		CASE WHEN Australian_Business_Number IS NULL THEN NULL ELSE EntryDate END                        AS australianBusinessNumberEntryDate,
		--
		Australian_Company_Number                                                                        AS australianCompanyNumber,
		CASE WHEN Australian_Company_Number IS NULL THEN NULL ELSE ValidFromDate END                     AS australianCompanyNumberValidFrom,
		CASE WHEN Australian_Company_Number IS NULL THEN NULL ELSE ValidToDate END                       AS australianCompanyNumberValidTo,
		CASE WHEN Australian_Company_Number IS NULL THEN NULL ELSE  EntryDate END                        AS australianCompanyNumberEntryDate,
		--
		DVA_no                                                                                           AS dvaNumber,
		CASE WHEN DVA_no IS NULL THEN NULL ELSE ValidFromDate END                                        AS dvaNumberValidFrom,
		CASE WHEN DVA_no IS NULL THEN NULL ELSE ValidToDate END                                          AS dvaNumberValidTo,
		CASE WHEN DVA_no IS NULL THEN NULL ELSE EntryDate END                                            AS dvaNumberEntryDate,
		--
		Commercial_Register_Number                                                                       AS commercialRegisterNumber,
		CASE WHEN Commercial_Register_Number IS NULL THEN NULL ELSE ValidFromDate END                    AS commercialRegisterNumberValidFrom,
		CASE WHEN Commercial_Register_Number IS NULL THEN NULL ELSE ValidToDate END                      AS commercialRegisterNumberValidTo,
		CASE WHEN Commercial_Register_Number IS NULL THEN NULL ELSE EntryDate END                        AS commercialRegisterNumberEntryDate,
		--
		Trade_License_Number                                                                             AS tradeLicenseNumber,
		CASE WHEN Trade_License_Number IS NULL THEN NULL ELSE ValidFromDate END                          AS tradeLicenseNumberValidFrom,
		CASE WHEN Trade_License_Number IS NULL THEN NULL ELSE ValidToDate END                            AS tradeLicenseNumberValidTo,
		CASE WHEN Trade_License_Number IS NULL THEN NULL ELSE EntryDate END                              AS tradeLicenseNumberEntryDate,
		--
		Direct_Debit_Telephone_Number                                                                    AS directDebitTelephoneNumber,
		CASE WHEN Direct_Debit_Telephone_Number IS NULL THEN NULL ELSE EntryDate END                     AS directDebitTelephoneNumberEntryDate,
		--
		Direct_Debit_Email_ID                                                                            AS directDebitEmail,
		CASE WHEN Direct_Debit_Email_ID IS NULL THEN NULL ELSE EntryDate END                             AS directDebitEmailEntryDate,
		--
		Ebill_registration_Party_Type                                                                    AS ebillRegistrationPartyType,
		CASE WHEN Ebill_registration_Party_Type IS NULL THEN NULL ELSE EntryDate END                     AS ebillRegistrationPartyTypeEntryDate,
		--
		Ebill_registration_Telephone_Number                                                              AS ebillRegistrationTelephoneNumber,
		CASE WHEN Ebill_registration_Telephone_Number IS NULL THEN NULL ELSE EntryDate END               AS ebillRegistrationTelephoneNumberEntryDate,
		--
		Ebill_registration_Email_ID                                                                      AS ebillRegistrationEmail,
		CASE WHEN Ebill_registration_Email_ID IS NULL THEN NULL ELSE EntryDate END                       AS ebillRegistrationEmailEntryDate,
		--
		Dealing_Number                                                                                   AS dealingNumber,
		CASE WHEN Dealing_Number IS NULL THEN NULL ELSE EntryDate END                                    AS dealingNumberEntryDate,
		--
		Dealing_Date                                                                                     AS dealingDate,
		CASE WHEN Dealing_Date IS NULL THEN NULL ELSE EntryDate END                                      AS dealingDateEntryDate,
		--
		Dealing_Type                                                                                     AS dealingType,
		CASE WHEN Dealing_Type IS NULL THEN NULL ELSE EntryDate END                                      AS dealingTypeEntryDate,
		--
		Dealing_Amount                                                                                   AS dealingAmount,
		CASE WHEN Dealing_Amount IS NULL THEN NULL ELSE EntryDate END                                    AS dealingAmountEntryDate,
		--
		Online_ID                                                                                        AS onlineID,
		CASE WHEN Online_ID IS NULL THEN NULL ELSE EntryDate END                                         AS onlineIDEntryDate,
		--
		Password                                                                                         AS userPassword,
		CASE WHEN Password IS NULL THEN NULL ELSE EntryDate END                                          AS userPasswordEntryDate,
		--
		Place_of_Birth                                                                                   AS placeofBirth,
		CASE WHEN Place_of_Birth IS NULL THEN NULL ELSE EntryDate END                                    AS placeofBirthEntryDate,
		--
		Pets_Name                                                                                        AS petsName,
		CASE WHEN Pets_Name IS NULL THEN NULL ELSE EntryDate END                                         AS petsNameEntryDate,
		--
		Mothers_First_Name                                                                               AS mothersFirstName,
		CASE WHEN Mothers_First_Name IS NULL THEN NULL ELSE EntryDate END                                AS mothersFirstNameEntryDate,
		--
		Mothers_Maiden_Name                                                                              AS mothersMaidenName,
		CASE WHEN Mothers_Maiden_Name IS NULL THEN NULL ELSE EntryDate END                               AS mothersMaidenNameEntryDate,
		--
		Fathers_First_Name                                                                               AS fathersFirstName,
		CASE WHEN Fathers_First_Name IS NULL THEN NULL ELSE EntryDate END                                AS fathersFirstNameEntryDate,
		--
		Labware_User_ID                                                                                  AS labwareUserId,
		CASE WHEN Labware_User_ID IS NULL THEN NULL ELSE ValidFromDate END                               AS labwareUserIdValidFrom,
		CASE WHEN Labware_User_ID IS NULL THEN NULL ELSE ValidToDate END                                 AS labwareUserIdValidTo,
		CASE WHEN Labware_User_ID IS NULL THEN NULL ELSE EntryDate END                                   AS labwareUserIdEntryDate,
		--
		RAS_Portal_Service_ID                                                                            AS rasPortalServiceId,
		CASE WHEN RAS_Portal_Service_ID IS NULL THEN NULL ELSE ValidFromDate END                         AS rasPortalServiceIdValidFrom,
		CASE WHEN RAS_Portal_Service_ID IS NULL THEN NULL ELSE ValidToDate END                           AS rasPortalServiceIdValidTo,
		CASE WHEN RAS_Portal_Service_ID IS NULL THEN NULL ELSE EntryDate END                             AS rasPortalServiceIdEntryDate,
		--
		Identity_card                                                                                    AS identityCard,
		CASE WHEN Identity_card IS NULL THEN NULL ELSE ValidFromDate END                                 AS identityCardValidFrom,
		CASE WHEN Identity_card IS NULL THEN NULL ELSE ValidToDate END                                   AS identityCardValidTo,
		CASE WHEN Identity_card IS NULL THEN NULL ELSE EntryDate END                                     AS identityCardEntryDate,
		--
		IMS_Number                                                                                       AS imsNumber,
		CASE WHEN IMS_Number IS NULL THEN NULL ELSE EntryDate END                                        AS imsNumberEntryDate,
		--
		External_System_Indicator_for_ICM                                                                AS externalSystemIndicatorForIcm,
		CASE WHEN External_System_Indicator_for_ICM IS NULL THEN NULL ELSE EntryDate END                 AS externalSystemIndicatorForIcmEntryDate,
		--
		External_System_Identifier                                                                       AS externalSystemIdentifier,
		CASE WHEN External_System_Identifier IS NULL THEN NULL ELSE EntryDate END                        AS externalSystemIdentifierEntryDate,
        _currentIndicator                                                                                AS currentIndicator
	FROM valid_id
	/* Pivot Table */
	PIVOT (
		MIN(businessPartnerIdNumber)
		FOR identificationType IN (
			'Drivers License Number'               AS Drivers_License_Number,
			'Passport'                             AS Passport,
			'Pensioner_no'                         AS Pensioner_no,
			'Australian Business Number'           AS Australian_Business_Number,
			'Australian Company Number'            AS Australian_Company_Number,
			'DVA_no'                               AS DVA_no,
			'Commercial Register Number'           AS Commercial_Register_Number,
			'Trade License Number'                 AS Trade_License_Number,
			'Direct Debit Telephone Number'        AS Direct_Debit_Telephone_Number,
			'Direct Debit Email ID'                AS Direct_Debit_Email_ID,
			'Ebill registration Party Type'        AS Ebill_registration_Party_Type,
			'Ebill registration Telephone Number'  AS Ebill_registration_Telephone_Number,
			'Ebill registration Email ID'          AS Ebill_registration_Email_ID,
			'Dealing Number'                       AS Dealing_Number,
			'Dealing Date'                         AS Dealing_Date,
			'Dealing Type'                         AS Dealing_Type,
			'Dealing Amount'                       AS Dealing_Amount,
			'Online ID'                            AS Online_ID,
			'Password'                             AS Password,
			'Place of Birth'                       AS Place_of_Birth,
			"Pet's Name"                           AS Pets_Name,
			"Mother's First Name"                  AS Mothers_First_Name,
			"Mother's Maiden Name"                 AS Mothers_Maiden_Name,
			"Father's First Name"                  AS Fathers_First_Name,
			"Labware User ID"                      AS Labware_User_ID,
			"RAS Portal Service ID"                AS RAS_Portal_Service_ID,
			"Identity card"                        AS Identity_card,
			"IMS Number"                           AS IMS_Number,
			"External System Indicator for ICM"    AS External_System_Indicator_for_ICM,
			"External System Identifier"           AS External_System_Identifier
		)
	) 
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Business Partner

-- COMMAND ----------

-- View: viewBusinessPartner
-- Description: viewBusinessPartner
CREATE OR REPLACE VIEW curated_v2.viewBusinessPartner AS 
 WITH 
     /*=====================================
        Build 'Effective From and To table
     =======================================*/
     dateDriver AS (
         SELECT DISTINCT
             sourceSystemCode,
             businessPartnerNumber,
             _recordStart AS _effectiveFrom
         FROM curated_v2.dimBusinessPartner
         WHERE _recordDeleted <> 1
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
     )  
 
 /*============================
    viewBusinessPartner
 ==============================*/
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
    BP.lastUpdatedDateTime,
    BP.lastUpdatedBy,
    /* Address Columns */
    ADDR.businesspartnerAddressSK,
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
    ADDR.stateName,
    ADDR.postalCode,
    ADDR.countryCode,
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
    ID.driverLicenseNumber,
    ID.driverLicenseNumberValidFrom,
    ID.driverLicenseNumberValidTo,
    ID.driverLicenseNumberEntryDate,
    ID.pensionNumber,
    ID.pensionNumberValidFrom,
    ID.pensionNumberValidTo,
    ID.pensionNumberEntryDate,
    ID.australianBusinessNumber,
    ID.australianBusinessNumberValidFrom,
    ID.australianBusinessNumberValidTo,
    ID.australianBusinessNumberEntryDate,
    ID.australianCompanyNumber,
    ID.australianCompanyNumberValidFrom,
    ID.australianCompanyNumberValidTo,
    ID.australianCompanyNumberEntryDate,
    ID.dvaNumber,
    ID.dvaNumberValidFrom,
    ID.dvaNumberValidTo,
    ID.dvaNumberEntryDate,
    ID.userPassword,
    ID.userPasswordEntryDate,
    ID.placeofBirth,
    ID.placeofBirthEntryDate,
    ID.petsName,
    ID.petsNameEntryDate,
    ID.mothersFirstName,
    ID.mothersFirstNameEntryDate,
    ID.mothersMaidenName,
    ID.mothersMaidenNameEntryDate,
    ID.fathersFirstName,
    ID.fathersFirstNameEntryDate,
    DR._effectiveFrom, 
    DR._effectiveTo,
    case
      when current_date() between DR._effectiveFrom
      and DR._effectiveTo then 'Y'
      else 'N'
    end as currentIndicator 
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
LEFT JOIN curated_v2.view_businesspartneridentification ID ON 
    DR.businessPartnerNumber = ID.businessPartnerNumber AND
    DR.sourceSystemCode = ID.sourceSystemCode
WHERE businessPartnerSK IS NOT NULL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Business Partner Group

-- COMMAND ----------

-- View: view_businesspartnergroup
-- Description: viewBusinessPartnerGroup

CREATE OR REPLACE VIEW curated_v2.viewBusinessPartnerGroup AS 
WITH 
    /*==============================
        Effective From and To Dates
    ================================*/
     dateDriver AS (
         SELECT DISTINCT
             sourceSystemCode,
             businessPartnerGroupNumber,
             _recordStart AS _effectiveFrom
         FROM curated_v2.dimBusinessPartnerGroup
         WHERE _recordDeleted <> 1
     ),
 
     effectiveDateRanges AS (
         SELECT 
             sourceSystemCode,
             businessPartnerGroupNumber, 
             _effectiveFrom, 
             COALESCE(
                 TIMESTAMP(
                     DATE_ADD(
                         LEAD(_effectiveFrom,1) OVER (PARTITION BY sourceSystemCode, businessPartnerGroupNumber ORDER BY _effectiveFrom),-1)
                 ), 
             TIMESTAMP('9999-12-31')) AS _effectiveTo
         from dateDriver
     )
    
/*============================
    view_businessPartnerGroup
==============================*/    
SELECT 
    /* Business Partner Group Columns */
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
    ID.userPassword,
    DR._effectiveFrom,
    DR._effectiveTo,
    case
      when current_date() between DR._effectiveFrom
      and DR._effectiveTo then 'Y'
      else 'N'
    end as currentIndicator 
FROM effectiveDateRanges DR
LEFT JOIN curated_v2.dimBusinessPartnerGroup BPG ON 
    DR.businessPartnerGroupNumber = BPG.businessPartnerGroupNumber AND
    DR.sourceSystemCode = BPG.sourceSystemCode AND
    DR._effectiveFrom <= BPG._RecordEnd AND
    DR._effectiveTo >= BPG._RecordStart 
LEFT JOIN curated_v2.dimbusinesspartneraddress ADDR ON 
    DR.businessPartnerGroupNumber = ADDR.businessPartnerNumber AND
    DR.sourceSystemCode = ADDR.sourceSystemCode AND
    DR._effectiveFrom <= ADDR._RecordEnd AND
    DR._effectiveTo >= ADDR._RecordStart 
LEFT JOIN curated_v2.view_businesspartneridentification ID ON 
    DR.businessPartnerGroupNumber = ID.businessPartnerNumber AND
    DR.sourceSystemCode = ID.sourceSystemCode
WHERE businessPartnerGroupSK IS NOT NULL

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("1")
