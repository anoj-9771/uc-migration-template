# Databricks notebook source
# DBTITLE 1,0BP_DEF_ADDRESS_ATTR
# MAGIC %sql
# MAGIC select
# MAGIC PARTNER as businessPartnerNumber
# MAGIC ,PARTNER_GUID as businessPartnerGUID
# MAGIC ,ADDRNUMBER as addressNumber
# MAGIC ,DATE_FROM as validFromDate
# MAGIC ,DATE_TO as validToDate
# MAGIC ,TITLE as titleCode
# MAGIC ,NAME1 as businessPartnerName1
# MAGIC ,NAME2 as additionalName
# MAGIC ,NAME3 as businessPartnerName3
# MAGIC ,NAME_CO as coName
# MAGIC ,CITY1 as cityName
# MAGIC ,CITY_CODE as cityCode
# MAGIC ,POST_CODE1 as postalCode
# MAGIC ,POST_CODE2 as poBoxPostalCode
# MAGIC ,POST_CODE3 as companyPostalCode
# MAGIC ,PO_BOX as poBoxCode
# MAGIC ,PO_BOX_NUM as poBoxWithoutNumberIndicator
# MAGIC ,PO_BOX_LOC as poBoxCity
# MAGIC ,CITY_CODE2 as poBoxCode
# MAGIC ,STREET as streetName
# MAGIC ,STREETCODE as streetCode
# MAGIC ,HOUSE_NUM1 as houseNumber
# MAGIC ,HOUSE_NUM2 as houseNumber2
# MAGIC ,STR_SUPPL1 as streetType
# MAGIC ,STR_SUPPL2 as streetLine3
# MAGIC ,STR_SUPPL3 as streetLine4
# MAGIC ,LOCATION as streetLine5
# MAGIC ,BUILDING as building
# MAGIC ,FLOOR as floorNumber
# MAGIC ,ROOMNUMBER as appartmentNumber
# MAGIC ,COUNTRY as countryShortName
# MAGIC ,LANGU as language
# MAGIC ,REGION as stateCode
# MAGIC ,PERS_ADDR as personalAddressIndicator
# MAGIC ,SORT1 as searchTerm1
# MAGIC ,SORT2 as searchTerm2
# MAGIC ,TEL_NUMBER as phoneNumber
# MAGIC ,FAX_NUMBER as faxNumber
# MAGIC ,TIME_ZONE as addressTimeZone
# MAGIC ,SMTP_ADDR as emailAddress
# MAGIC ,URI_ADDR as uriAddress
# MAGIC ,TELDISPLAY as phoneNumberDisplayFormat
# MAGIC ,FAXDISPLAY as faxDisplayFormat
# MAGIC ,LONGITUDE as longitude
# MAGIC ,LATITUDE as latitude
# MAGIC ,ALTITUDE as altitude
# MAGIC ,PRECISID as precision
# MAGIC ,ADDRCOMM as communicationAddressNumber
# MAGIC ,ADDR_SHORT as shortFormattedAddress
# MAGIC ,ADDR_SHORT_S as shortFormattedAddress2
# MAGIC ,LINE0 as addressLine0
# MAGIC ,LINE1 as addressLine1
# MAGIC ,LINE2 as addressLine2
# MAGIC ,LINE3 as addressLine3
# MAGIC ,LINE4 as addressLine4
# MAGIC ,LINE5 as addressLine5
# MAGIC ,LINE6 as addressLine6
# MAGIC ,LINE7 as addressLine7
# MAGIC ,LINE8 as addressLine8
# MAGIC from
# MAGIC Source

# COMMAND ----------

# DBTITLE 1,0BP_ID_ATTR
# MAGIC %sql
# MAGIC select
# MAGIC PARTNER as businessPartnerNumber
# MAGIC ,TYPE as identificationTypeCode
# MAGIC ,b.TYPE as identificationType
# MAGIC ,IDNUMBER as businessPartnerIdNumber
# MAGIC ,INSTITUTE as institute
# MAGIC ,ENTRY_DATE as entryDate
# MAGIC ,VALID_DATE_FROM as validFromDate
# MAGIC ,VALID_DATE_TO as validToDate
# MAGIC ,COUNTRY as countryShortName
# MAGIC ,REGION as stateCode
# MAGIC ,PARTNER_GUID as businessPartnerGUID
# MAGIC ,FLG_DEL_BW as deletedIndicator
# MAGIC from
# MAGIC source a
# MAGIC left join 0BP_ID_TYPE_TEXT b
# MAGIC on a.TYPE = b.TYPE where b.SPRAS='E'

# COMMAND ----------

# DBTITLE 1,0BP_RELATIONS_ATTR
# MAGIC %sql
# MAGIC select
# MAGIC RELNR as businessPartnerRelationshipNumber
# MAGIC ,PARTNER1 as businessPartnerNumber1
# MAGIC ,PARTNER2 as businessPartnerNumber2
# MAGIC ,PARTNER1_GUID as businessPartnerGUID1
# MAGIC ,PARTNER2_GUID as businessPartnerGUID2
# MAGIC ,RELDIR as relationshipDirection
# MAGIC ,RELTYP as relationshipTypeCode
# MAGIC ,Derived as relationshipType
# MAGIC ,DATE_TO as validToDate
# MAGIC ,DATE_FROM as validFromDate
# MAGIC ,COUNTRY as countryShortName
# MAGIC ,POST_CODE1 as postalCode
# MAGIC ,CITY1 as cityName
# MAGIC ,STREET as streetName
# MAGIC ,HOUSE_NUM1 as houseNumber
# MAGIC ,TEL_NUMBER as phoneNumber
# MAGIC ,SMTP_ADDR as emailAddress
# MAGIC ,CMPY_PART_PER as capitalInterestPercentage
# MAGIC ,CMPY_PART_AMO as capitalInterestAmount
# MAGIC ,ADDR_SHORT as shortFormattedAddress
# MAGIC ,ADDR_SHORT_S as shortFormattedAddress2
# MAGIC ,LINE0 as addressLine0
# MAGIC ,LINE1 as addressLine1
# MAGIC ,LINE2 as addressLine2
# MAGIC ,LINE3 as addressLine3
# MAGIC ,LINE4 as addressLine4
# MAGIC ,LINE5 as addressLine5
# MAGIC ,LINE6 as addressLine6
# MAGIC ,FLG_DELETED as deletedIndicator
# MAGIC from source a
# MAGIC left join 0BP_RELATIONS_TEXT b
# MAGIC on a.RELDIR = b.RELDIR and a.RELTYP = b.RELTYP where b.LANGU = 'E'

# COMMAND ----------

# DBTITLE 1,0BPARTNER_ATTR
# MAGIC %sql
# MAGIC select
# MAGIC PARTNER as businessPartnerNumber
# MAGIC ,TYPE as businessPartnerCategoryCode
# MAGIC ,b.TXTMD as businessPartnerCategory
# MAGIC ,BPKIND as businessPartnerTypeCode
# MAGIC ,c.BPKIND as businessPartnerType
# MAGIC ,BU_GROUP as businessPartnerGroupCode
# MAGIC ,d.TEXT40 as businessPartnerGroup
# MAGIC ,BPEXT as externalBusinessPartnerNumber
# MAGIC ,BU_SORT1 as searchTerm1
# MAGIC ,BU_SORT2 as searchTerm2
# MAGIC ,TITLE as titleCode
# MAGIC ,e.TITLE_MEDI as title
# MAGIC ,XDELE as deletedIndicator
# MAGIC ,XBLCK as centralBlockBusinessPartner
# MAGIC ,ZZUSER as userId
# MAGIC ,ZZPAS_INDICATOR as paymentAssistSchemeIndicator
# MAGIC ,ZZBA_INDICATOR as billAssistIndicator
# MAGIC ,ZZAFLD00001Z as createdDate
# MAGIC ,ZZ_CONSENT1 as consent1Indicator
# MAGIC ,ZZWAR_WID as warWidowIndicator
# MAGIC ,ZZDISABILITY as disabilityIndicator
# MAGIC ,ZZGCH as goldCardHolderIndicator
# MAGIC ,ZZDECEASED as deceasedIndicator
# MAGIC ,ZZPCC as pensionConcessionCardIndicator
# MAGIC ,ZZELIGIBILITY as eligibilityIndicator
# MAGIC ,ZZDT_CHK as dateOfCheck
# MAGIC ,ZZPAY_ST_DT as paymentStartDate
# MAGIC ,ZZPEN_TY as pensionType
# MAGIC ,ZZ_CONSENT2 as consent2Indicator
# MAGIC ,NAME_ORG1 as organizationName1
# MAGIC ,NAME_ORG2 as organizationName2
# MAGIC ,NAME_ORG3 as organizationName3
# MAGIC ,FOUND_DAT as organizationFoundedDate
# MAGIC ,LOCATION_1 as internationalLocationNumber1
# MAGIC ,LOCATION_2 as internationalLocationNumber2
# MAGIC ,LOCATION_3 as internationalLocationNumber3
# MAGIC ,NAME_LAST as lastName
# MAGIC ,NAME_FIRST as firstName
# MAGIC ,NAMEMIDDLE as middleName
# MAGIC ,TITLE_ACA1 as academicTitleCode
# MAGIC ,f.TITLE_MEDI as academicTitle
# MAGIC ,NICKNAME as nickName
# MAGIC ,INITIALS as nameInitials
# MAGIC ,NAMCOUNTRY as countryName
# MAGIC ,LANGU_CORR as correspondanceLanguage
# MAGIC ,NATIO as nationality
# MAGIC ,PERSNUMBER as personNumber
# MAGIC ,XSEXU as unknownGenderIndicator
# MAGIC ,BU_LANGU as language
# MAGIC ,BIRTHDT as dateOfBirth
# MAGIC ,DEATHDT as dateOfDeath
# MAGIC ,PERNO as personnelNumber
# MAGIC ,NAME_GRP1 as nameGroup1
# MAGIC ,NAME_GRP2 as nameGroup2
# MAGIC ,MC_NAME1 as searchHelpLastName
# MAGIC ,MC_NAME2 as searchHelpFirstName
# MAGIC ,CRUSR as createdBy
# MAGIC ,CRDAT as createdDate
# MAGIC ,CRTIM as createdTime
# MAGIC ,CHUSR as changedBy
# MAGIC ,CHDAT as changedDate
# MAGIC ,CHTIM as changedTime
# MAGIC ,PARTNER_GUID as businessPartnerGUID
# MAGIC ,ADDRCOMM as communicationAddressNumber
# MAGIC ,TD_SWITCH as plannedChangeDocument
# MAGIC ,VALID_FROM as validFromDate
# MAGIC ,VALID_TO as validToDate
# MAGIC ,NATPERS as naturalPersonIndicator
# MAGIC ,ZZAFLD00000M as kidneyDialysisIndicator
# MAGIC ,ZZUNIT as patientUnit
# MAGIC ,ZZTITLE as patientTitleCode
# MAGIC ,g.TITLE_MEDI as patientTitle
# MAGIC ,ZZF_NAME as patientFirstName
# MAGIC ,ZZSURNAME as patientSurname
# MAGIC ,ZZAREACODE as patientAreaCode
# MAGIC ,ZZPHONE as patientPhoneNumber
# MAGIC ,ZZHOSP_NAME as hospitalName
# MAGIC ,ZZMACH_TYPE as patientMachineType
# MAGIC ,ZZON_DATE as machineTypeValidFromDate
# MAGIC ,ZZOFF_REAS as offReason
# MAGIC ,ZZOFF_DATE as machineTypeValidToDate
# MAGIC from source a
# MAGIC left join 0BPARTNER_TEXT b
# MAGIC on a.PARTNER = b.PARTNER and b.TYPE = a.TYPE
# MAGIC left join 0BPTYPE_TEXT c
# MAGIC on a.BPKIND = c.BPKIND and c.SPRAS = 'E'
# MAGIC left join 0BP_GROUP_TEXT d
# MAGIC on a.BPKIND = d.BPKIND and d.SPRAS = 'E'
# MAGIC left join ZDSTITLET e
# MAGIC on a.TITLE = e.TITLE and e.LANGU = 'E'
# MAGIC left join ZDSTITLET f
# MAGIC a.TITLE = f.TITLE_ACA1 and f.LANGU = 'E'
# MAGIC left join ZDSTITLET g
# MAGIC and a.TITLE = g.ZZTITLE and g.LANGU = 'E' 

# COMMAND ----------

# DBTITLE 1,0CAM_STREETCODE_TEXT
# MAGIC %sql
# MAGIC select
# MAGIC COUNTRY as countryShortName
# MAGIC ,STRT_CODE as streetCode
# MAGIC ,STREET as streetName
# MAGIC from source

# COMMAND ----------

# DBTITLE 1,0FC_PPSTA_TEXT
# MAGIC %sql
# MAGIC select
# MAGIC LANGU as language
# MAGIC ,KEY1 as promiseToPayStatusCode
# MAGIC ,TXTLG as promiseToPayStatus
# MAGIC from source

# COMMAND ----------

# DBTITLE 1,0FC_PYMET_TEXT
# MAGIC %sql
# MAGIC select
# MAGIC SPRAS as language
# MAGIC ,LAND1 as countryCode
# MAGIC ,ZLSCH as paymentMethodCode
# MAGIC ,TEXT1 as paymentMethod
# MAGIC from source

# COMMAND ----------

# DBTITLE 1,0FC_STEP_TEXT
# MAGIC %sql
# MAGIC select
# MAGIC SPRAS as language
# MAGIC ,STEP as collectionStepCode
# MAGIC ,STEPT as collectionStep
# MAGIC from source

# COMMAND ----------

# DBTITLE 1,0FCACTDETID_TEXT
# MAGIC %sql
# MAGIC select
# MAGIC SPRAS as language
# MAGIC ,KOFIZ as accountDeterminationCode
# MAGIC ,TEXT50 as accountDetermination
# MAGIC from source

# COMMAND ----------

# DBTITLE 1,0FC_PPRSW_TEXT
# MAGIC %sql
# MAGIC select
# MAGIC SPRAS as language
# MAGIC ,PPRSW as withdrawalReasonCode
# MAGIC ,TXT50 as withdrawalReason
# MAGIC from source

# COMMAND ----------

# DBTITLE 1,0FC_PPRS_TEXT
# MAGIC %sql
# MAGIC select
# MAGIC SPRAS as language
# MAGIC ,PPRSC as promiseToPayReasonCode
# MAGIC ,TXT50 as promiseToPayReason
# MAGIC from source

# COMMAND ----------

# DBTITLE 1,0IND_NUMSYS_TEXT
# MAGIC %sql
# MAGIC select
# MAGIC LANGU as language
# MAGIC ,ISTYPE as industrySystem
# MAGIC ,BEZ30 as industry
# MAGIC from source

# COMMAND ----------

# DBTITLE 1,0UC_ABRVORG_TEXT
# MAGIC %sql
# MAGIC select
# MAGIC LANGU as language
# MAGIC ,KEY1 as billingTransactionCode
# MAGIC ,TXTLG as billingTransaction
# MAGIC from source

# COMMAND ----------

# DBTITLE 1,0UC_DISCPRV_TEXT
# MAGIC %sql
# MAGIC select
# MAGIC SPRAS as language
# MAGIC ,DISCPROCV as processingVariantCode
# MAGIC ,DESCRIPT as applicationFormDescription
# MAGIC from source

# COMMAND ----------

# DBTITLE 1,0UC_TVORG_TEXT
# MAGIC %sql
# MAGIC select
# MAGIC ,APPLK as applicationArea
# MAGIC ,HVORG as mainTransactionLineItemCode
# MAGIC ,TVORG as subtransactionLineItemCode
# MAGIC ,TXT30 as subtransaction
# MAGIC from source

# COMMAND ----------

# DBTITLE 1,0UC_ICHCKCD_TEXT
# MAGIC %sql
# MAGIC select
# MAGIC SPRAS as language
# MAGIC ,PRUEFPKT as independentValidationCode
# MAGIC ,TEXT60 as independentValidation
# MAGIC from source

# COMMAND ----------

# DBTITLE 1,0UC_IND_SEC_TEXT
# MAGIC %sql
# MAGIC select
# MAGIC SPRAS as language
# MAGIC ,ISTYPE as industrySystem
# MAGIC ,IND_SECTOR as industryCode
# MAGIC ,TEXT as industry
# MAGIC ,TEXT_SHORT as industryShort
# MAGIC from source

# COMMAND ----------

# DBTITLE 1,0UC_INSTFACTS
# MAGIC %sql
# MAGIC select
# MAGIC ANLAGE as installationId
# MAGIC ,OPERAND as operandCode
# MAGIC ,AB as validFromDate
# MAGIC ,BIS as validToDate
# MAGIC ,WERT1 as entryValue
# MAGIC ,WERT2 as valueToBeBilled
# MAGIC ,STRING3 as operandValue
# MAGIC ,BETRAG as amount
# MAGIC ,UPDMOD as bwDeltaProcess
# MAGIC ,MASS as measurementUnit
# MAGIC from source

# COMMAND ----------

# DBTITLE 1,0UC_MRCATEG_TEXT
# MAGIC %sql
# MAGIC select
# MAGIC SPRAS as language
# MAGIC ,ABLESART as meterReadingControlCode
# MAGIC ,TEXT40 as meterReadingCategory
# MAGIC from source

# COMMAND ----------

# DBTITLE 1,0UC_MRTYPE_TEXT
# MAGIC %sql
# MAGIC select
# MAGIC ISTABLART as meterReadingTypeCode
# MAGIC ,TEXT40 as meterReadingType
# MAGIC from source

# COMMAND ----------

# DBTITLE 1,0UC_OPERAND_TEXT
# MAGIC %sql
# MAGIC select
# MAGIC SPRAS as language 
# MAGIC ,OPERAND as operandCode
# MAGIC ,TEXT30 as operand
# MAGIC from source

# COMMAND ----------

# DBTITLE 1,ICAWRITEOFFRSNT
# MAGIC %sql
# MAGIC select
# MAGIC CAWRITEOFFREASON as writeOffReasonCode
# MAGIC ,LANGUAGE as language
# MAGIC ,CAWRITEOFFREASONNAME as writeOffReason
# MAGIC from source

# COMMAND ----------

# DBTITLE 1,IFORMOFADDRTEXT
# MAGIC %sql
# MAGIC select
# MAGIC LANGUAGE as language
# MAGIC ,FORMOFADDRESS as formOfAddressCode
# MAGIC ,FORMOFADDRESSNAME as formOfAddress
# MAGIC from source

# COMMAND ----------

# DBTITLE 1,TE192T
# MAGIC %sql
# MAGIC select
# MAGIC AUSGRUP_IN as outsortingCheckGroupCode
# MAGIC ,TEXT30 as outsortingCheckGroup
# MAGIC from source

# COMMAND ----------

# DBTITLE 1,ZDM_DS_0UC_MRREAS_TEXT
# MAGIC %sql
# MAGIC select
# MAGIC SPRAS as language
# MAGIC ,ABLESGR as meterReadingReasonCode
# MAGIC ,TEXT40 as meterReadingReason
# MAGIC from source

# COMMAND ----------

# DBTITLE 1,ZDM_DS_0UC_MRTYPE_TEXT
# MAGIC %sql
# MAGIC select
# MAGIC SPRAS as language
# MAGIC ,ISTABLART as meterReadingTypeCode
# MAGIC ,TEXT40 as meterReadingType
# MAGIC from source

# COMMAND ----------

# DBTITLE 1,ZDM_DS_ABLSTAT_TXT
# MAGIC %sql
# MAGIC select
# MAGIC LANGU as language
# MAGIC ,KEY1 as meterReadingStatusCode
# MAGIC ,TXTSH as meterReadingStatusShort
# MAGIC ,TXTMD as meterReadingStatusMedium
# MAGIC ,TXTLG as meterReadingStatusLong
# MAGIC from source

# COMMAND ----------

# DBTITLE 1,0UC_BBPROC_TEXT
# MAGIC %sql
# MAGIC select
# MAGIC LANGU as language
# MAGIC ,KEY1 as billingProcedureCode
# MAGIC ,TXTLG as billingProcedure
# MAGIC from source

# COMMAND ----------

# DBTITLE 1,0PM_MEASPOINT_ATTR
# MAGIC %sql
# MAGIC select
# MAGIC POINT as measuringPointId
# MAGIC ,MPOBJ as measuringPointObjectNumber
# MAGIC ,PSORT as positionNumber
# MAGIC ,MPTYP as measuringPointCategory
# MAGIC ,ERDAT as createdDate
# MAGIC ,INACT as inactiveIndicator
# MAGIC ,ATINN as internalcharacteristic
# MAGIC ,MRNGU as measurementRangeUnit
# MAGIC ,CODCT as measurementReadingCatalogCode
# MAGIC ,CODGR as measurementReadingGroupCode
# MAGIC ,AEDAT as lastChangedDate
# MAGIC ,DELTADATE as deltaDate
# MAGIC from source

# COMMAND ----------

# DBTITLE 1,0UC_ABRFREIG_TEXT
# MAGIC %sql
# MAGIC SELECT
# MAGIC SPRAS as language
# MAGIC ,ABRFREIG as billReleasingReasonCode
# MAGIC ,TEXT30 as billReleasingReason
# MAGIC from source

# COMMAND ----------

# DBTITLE 1,0UC_ABRSPERR_TEXT
# MAGIC %sql
# MAGIC SELECT
# MAGIC SPRAS as language
# MAGIC ,ABRSPERR as billBlockingReasonCode
# MAGIC ,TEXT30 as billBlockingReason
# MAGIC from source

# COMMAND ----------

# DBTITLE 1,0FC_BP_ITEMS
# MAGIC %sql
# MAGIC SELECT
# MAGIC OPBEL as contractAccountNumber
# MAGIC ,OPUPW as repetitionItem
# MAGIC ,OPUPK as itemNumber
# MAGIC ,OPUPZ as partialClearingSubitem
# MAGIC ,BUKRS as companyCode
# MAGIC ,b.TXTMD as company
# MAGIC ,AUGST as clearingStatus
# MAGIC ,GPART as businessPartnerGroupNumber
# MAGIC ,VTREF as contractReferenceSpecification
# MAGIC ,VKONT as contractAccountNumber
# MAGIC ,ABWBL as ficaDocumentNumber
# MAGIC ,ABWTP as ficaDocumentCategory
# MAGIC ,APPLK as applicationArea
# MAGIC ,HVORG as mainTransactionLineItemCode
# MAGIC ,c.TXT30 as mainTransactionLineItem
# MAGIC ,TVORG as subtransactionLineItemCode
# MAGIC ,d.TXT30 as subtransactionLineItem
# MAGIC ,KOFIZ as accountDeterminationCode
# MAGIC ,e.TXT50 as accountDetermination
# MAGIC ,SPART as divisionCode
# MAGIC ,HKONT as accountGeneralLedger
# MAGIC ,MWSKZ as taxSalesCode
# MAGIC ,XANZA as downPaymentIndicator
# MAGIC ,STAKZ as statisticalItemType
# MAGIC ,BLDAT as documentDate
# MAGIC ,BUDAT as documentEnteredDate
# MAGIC ,WAERS as currencyKey
# MAGIC ,FAEDN as paymentDueDate
# MAGIC ,FAEDS as cashDiscountDueDate
# MAGIC ,STUDT as deferralToDate
# MAGIC ,SKTPZ as cashDiscountPercentageRate
# MAGIC ,BETRH as amountLocalCurrency
# MAGIC ,BETRW as documentTypeCode
# MAGIC ,SKFBT as amountEligibleCashDiscount
# MAGIC ,SBETH as taxAmountLocalCurrency
# MAGIC ,SBETW as taxAmount
# MAGIC ,AUGDT as clearingDate
# MAGIC ,AUGBL as clearingDocument
# MAGIC ,AUGBD as clearingDocumentPostingDate
# MAGIC ,AUGRD as clearingReason
# MAGIC ,AUGWA as clearingCurrency
# MAGIC ,AUGBT as clearingAmount
# MAGIC ,AUGBS as taxAmount
# MAGIC ,AUGSK as cashDiscount
# MAGIC ,AUGVD as clearingValueDate
# MAGIC ,ABRZU as settlementPeriodLowerLimit
# MAGIC ,ABRZO as billingPeriodUpperLimit
# MAGIC ,AUGRS as clearingRestriction
# MAGIC ,INFOZ as valueAdjustment
# MAGIC ,BLART as documentTypeCode
# MAGIC ,f.LTEXT as documentType
# MAGIC ,XBLNR as referenceDocumentNumber
# MAGIC ,INKPS as collectionItem
# MAGIC ,C4EYE as checkReason
# MAGIC ,SCTAX as taxPortion
# MAGIC ,STTAX as taxAmountDocument
# MAGIC ,ABGRD as writeOffReasonCode
# MAGIC ,HERKF as documentOriginCode
# MAGIC ,CPUDT as documentEnteredDate
# MAGIC ,AWTYP as referenceProcedure
# MAGIC ,AWKEY as objectKey
# MAGIC ,STORB as reversalDocumentNumber
# MAGIC ,FIKEY as reconciliationKeyForGeneralLedger
# MAGIC ,XCLOS as furtherPostingIndicator
# MAGIC from source a
# MAGIC left join 0COMP_CODE_TEXT b
# MAGIC on a.BUKRS = b.BUKRS and b.LANGU = 'E'  --if ref table has been verified with filter on LANGU = E then it can be excluded in this script
# MAGIC left join 0UC_HVORG_TEXT c
# MAGIC on a.HVORG = c.HVORG and a.APPLK = c.APPLK and c.LANGU='E' --if ref table has been verified with filter on LANGU = E then it can be excluded in this script
# MAGIC left join 0UC_TVORG_TEXT d
# MAGIC on a.HVORG = d.HVORG and a.APPLK = d.APPLK and a.TVORG = d.TVORG and SPRAS ='E' --if ref table has been verified with filter on SPRAS = E then it can be excluded in this script
# MAGIC left join 0FCACTDETID_TEXT e
# MAGIC on a.KOFIZ = e.KOFIZ and SPRAS = 'E' --if ref table has been verified with filter on SPRAS = E then it can be excluded in this script
# MAGIC left join 0FC_BLART_TEXT f
# MAGIC on a.APPLK = f.APPLK and a.BLART = f.BLART and SPRAS = 'E' --if ref table has been verified with filter on SPRAS = E then it can be excluded in this script

# COMMAND ----------

# DBTITLE 1,0FC_DUN_HEADER
# MAGIC %sql
# MAGIC SELECT
# MAGIC LAUFD as dateId
# MAGIC ,LAUFI as additionalIdentificationCharacteristic
# MAGIC ,GPART as businessPartnerGroupNumber
# MAGIC ,VKONT as contractAccountNumber
# MAGIC ,MAZAE as dunningNoticeCounter
# MAGIC ,AUSDT as dateOfIssue
# MAGIC ,MDRKD as noticeExecutionDate
# MAGIC ,VKONTGRP as contractAccountGroup
# MAGIC ,ITEMGRP as dunningClosedItemGroup
# MAGIC ,STRAT as collectionStrategyCode
# MAGIC ,STEP as collectionStepCode
# MAGIC ,STEP_LAST as collectionStepLastDunning
# MAGIC ,OPBUK as companyCodeGroup
# MAGIC ,STDBK as standardCompanyCode
# MAGIC ,SPART as divisionCode
# MAGIC ,VTREF as contractReferenceSpecification
# MAGIC ,VKNT1 as leadingContractAccount
# MAGIC ,ABWMA as alternativeDunningRecipient
# MAGIC ,MAHNS as dunningLevel
# MAGIC ,WAERS as currencyKey
# MAGIC ,MSALM as dunningBalance
# MAGIC ,RSALM as totalDuningReductions
# MAGIC ,CHGID as chargesSchedule
# MAGIC ,MGE1M as dunningCharge1
# MAGIC ,MG1BL as documentNumber
# MAGIC ,MG1TY as chargeType
# MAGIC ,MGE2M as dunningCharge2
# MAGIC ,MGE3M as dunningCharge3
# MAGIC ,MINTM as dunningInterest
# MAGIC ,MIBEL as interestPostingDocument
# MAGIC ,BONIT as creditWorthiness
# MAGIC ,XMSTO as noticeReversedIndicator
# MAGIC ,NRZAS as paymentFormNumber
# MAGIC ,XCOLL as submittedIndicator
# MAGIC ,STAKZ as statisticalItemType
# MAGIC ,SUCPC as successRate
# MAGIC FROM Source

# COMMAND ----------

# DBTITLE 1,0FC_PP
# MAGIC %sql
# MAGIC SELECT
# MAGIC PPKEY as propmiseToPayId
# MAGIC ,GPART as businessPartnerGroupNumber
# MAGIC ,VKONT as contractAccountNumber
# MAGIC ,BUKRS as companyCode
# MAGIC ,PPRSC as promiseToPayReasonCode
# MAGIC ,PPRSW as withdrawalReasonCode
# MAGIC ,PPCAT as propmiseToPayCategoryCode
# MAGIC ,C4LEV as numberOfChecks
# MAGIC ,PRCUR as currency
# MAGIC ,PRAMT as paymentAmountPromised
# MAGIC ,PRAMT_CHR as promiseToPayCharges
# MAGIC ,PRAMT_INT as promiseToPayInterest
# MAGIC ,RDAMT as amountCleared
# MAGIC ,ERNAM as createdBy
# MAGIC ,CONCAT(LEFT (ERDAT,10),'T',SUBSTRING(ERTIM,12,8),'.000+0000') as createdDateTime
# MAGIC ,CHDAT as changedDate
# MAGIC ,PPSTA as propmiseToPayStatus
# MAGIC ,XSTCH as statusChangedIndicator
# MAGIC ,PPKEY_NEW as replacementPropmiseToPayId
# MAGIC ,XINDR as installmentsAgreed
# MAGIC ,FTDAT as firstDueDate
# MAGIC ,LTDAT as finalDueDate
# MAGIC ,NRRTS as numberOfPayments
# MAGIC ,PPDUE as paymentPromised
# MAGIC ,PPPAY as amountPaidByToday
# MAGIC ,DEGFA as currentLevelOfFulfillment
# MAGIC FROM Source

# COMMAND ----------

# DBTITLE 1,0FC_PPCAT_TEXT
# MAGIC %sql
# MAGIC SELECT
# MAGIC SPRAS as language --must be nullable = false
# MAGIC ,PPCAT as propmiseToPayCategoryCode --must be nullable = false
# MAGIC ,TXT50 as propmiseToPayCategory
# MAGIC FROM Source

# COMMAND ----------

# DBTITLE 1,0FC_PPD
# MAGIC %sql
# MAGIC SELECT
# MAGIC PPKEY as propmiseToPayId
# MAGIC ,PRDAT as paymentDatePromised
# MAGIC ,PRAMT as paymentAmountPromised
# MAGIC ,PRAMO as promisedAmountOpen
# MAGIC ,PRCUR as currency
# MAGIC ,FDDBT as amount2
# MAGIC ,FDDBO as amount1
# MAGIC ,ERDAT as createdDate
# MAGIC from Source

# COMMAND ----------

# DBTITLE 1,0PM_MEASPOINT_TEXT
# MAGIC %sql
# MAGIC SELECT
# MAGIC POINT as measuringPointId
# MAGIC ,MLANG as language
# MAGIC ,PTTXT as measuringPoint
# MAGIC ,ERDAT as createdDate
# MAGIC ,AEDAT as lastChangedDate
# MAGIC ,DELTADATE as deltaDate
# MAGIC FROM Source

# COMMAND ----------

# DBTITLE 1,0SRV_REQ_INCI_H
# MAGIC %sql
# MAGIC SELECT
# MAGIC GUID as headerUUID
# MAGIC ,OBJECT_ID as utilitiesStructuredContract
# MAGIC ,PROCESS_TYPE as headerType
# MAGIC ,POSTING_DATE as postingDate
# MAGIC ,DESCRIPTION_UC as transactionDescription
# MAGIC ,LOGICAL_SYSTEM as logicalSystem
# MAGIC ,OBJECT_TYPE as headerCategory
# MAGIC ,CREATED_AT as creationDate
# MAGIC ,CREATED_BY as createdBy
# MAGIC ,CHANGED_AT as lastChangedDate
# MAGIC ,CHANGED_BY as changedBy
# MAGIC ,NUM_OF_HEAD as requestHeaderNumber
# MAGIC ,SCENARIO as scenarioId
# MAGIC ,TEMPLATE_TYPE as templateType
# MAGIC ,CREATED_TS as MERGE INTO DATE??
# MAGIC ,CHANGED_TS as MERGE INTO DATE??
# MAGIC ,REC_PRIORITY as recommendedPriority
# MAGIC ,URGENCY as urgency
# MAGIC ,IMPACT as impact
# MAGIC ,ESCALATION as escalation
# MAGIC ,RISK as risk
# MAGIC ,LAST_UPDATED_AT as lastUpdatedAt
# MAGIC ,CATEGORY as activityCategory
# MAGIC ,PRIORITY as activityPriority
# MAGIC ,DIRECTION as activityDirection
# MAGIC ,SOLD_TO_PARTY as soldToParty
# MAGIC ,SALES_EMPLOYEE as salesEmployee
# MAGIC ,PERSON_RESP as responsibleEmployee
# MAGIC ,CONTACT_PERSON as contactPerson
# MAGIC ,SALES_ORG_RESP as salesOrgResponsible
# MAGIC ,SALES_ORG as salesOrg
# MAGIC ,SALES_OFFICE as salesOffice
# MAGIC ,SALES_GROUP as salesGroup
# MAGIC ,SERVICE_ORG_RESP as serviceOrgResponsible
# MAGIC ,SERVICE_ORG as serviceOrg
# MAGIC ,SERVICE_TEAM as serviceTeam
# MAGIC ,CALDAY as calendarDay
# MAGIC ,CALDAY_TS as calendarDatetime
# MAGIC ,PREDEC_OBJKEY as precedingTransactionGUID
# MAGIC ,PREDEC_OBJTYPE as precedingDocumentObjectType
# MAGIC ,PRED_ACT_GUID as precedingActivityGUID
# MAGIC ,PROCESS_CATEGORY as processCategory
# MAGIC ,PROCESS_CATALOG as processCatalog
# MAGIC ,PROCESS_CODEGR as processCodeGroup
# MAGIC ,PROCESS_CODE as processCode
# MAGIC ,PROCESS_OBJTYPE as precedingObjectType
# MAGIC ,QUOT_VALID_TS as validTimestamp
# MAGIC ,CATALOG_TYPE_C as catalogCategoryC
# MAGIC ,KATALOGART_C as catalogC
# MAGIC ,CODEGRUPPE_C as codeGroupC
# MAGIC ,CODE_C as codeC
# MAGIC ,DEFQUANTITY_C as defectCountC
# MAGIC ,COUNTER_C as numberOfActivitiesC
# MAGIC ,ASP_ID_C as coherentAspectIdC
# MAGIC ,CAT_ID_C as coherentCategoryIdC
# MAGIC ,CC_CAT_SUBJECT_C as dataElementGUIDC
# MAGIC ,CATALOG_TYPE_D as catalogCategoryD
# MAGIC ,KATALOGART_D as catalogD
# MAGIC ,CODEGRUPPE_D as codeGroupD
# MAGIC ,CODE_D as codeD
# MAGIC ,DEFQUANTITY_D as defectCountD
# MAGIC ,COUNTER_D as numberOfActivitiesD
# MAGIC ,ASP_ID_D as coherentAspectIdD
# MAGIC ,CAT_ID_D as coherentCategoryIdD
# MAGIC ,CC_CAT_SUBJECT_D as dataElementGUIDD
# MAGIC ,DEFQUANTITY_E as defectCountE
# MAGIC ,COUNTER_E as numberOfActivitiesE
# MAGIC ,CC_CAT_SUBJECT_E as dataElementGUIDE
# MAGIC ,DEFQUANTITY_T as defectCountT
# MAGIC ,COUNTER_T as numberOfActivitiesT
# MAGIC ,CC_CAT_SUBJECT_T as dataElementGUIDT
# MAGIC ,DEFQUANTITY_W as defectCountW
# MAGIC ,COUNTER_W as numberOfActivitiesW
# MAGIC ,CC_CAT_SUBJECT_W as dataElementGUIDW
# MAGIC ,PROFILE_TYPE as subjectProfileCategory
# MAGIC ,CC_CAT_SUBJECT as dataElementGUID
# MAGIC ,LC_SRV_DURATION as serviceLifeCycle
# MAGIC ,LC_SRV_DUR_UNIT as serviceLifeCycleUnit
# MAGIC ,WORK_DURATION as workDuration
# MAGIC ,WORK_DURA_UNIT as workDurationUnit
# MAGIC ,TOTAL_DURATION as totalDuration
# MAGIC ,TOTAL_DURA_UNIT as totalDurationUnit
# MAGIC ,REQ_START_DATE as requestStartDate
# MAGIC ,REQ_END_DATE as requestEndDate
# MAGIC ,DUE_DATE as dueDateTime
# MAGIC ,COMPLETION_TS as completionDateTime
# MAGIC ,ESCALATE_1_TS as firstEscalateDateTime
# MAGIC ,ESCALATE_2_TS as secondEscalateDateTime
# MAGIC ,CC_CAT_ACTREASON as activityReasonCode
# MAGIC ,NO_OF_IR as numberOfInteractionRecords
# MAGIC ,IN_COMPL_BEFORE as completedBeforeIndicator
# MAGIC ,PROBLEM_GUID as problemGUID
# MAGIC ,NOTIFICATION_NO as notificationNumber
# MAGIC ,CRM_ISU_CONTRACT as contractiId
# MAGIC ,STATUS as podStatus
# MAGIC ,USER_STAT_PROC as statusProfile
# MAGIC ,ZZ_MAX_REQ_NO as maximoWorkOrderNumber
# MAGIC ,ZZAFLD000026 as source
# MAGIC ,ZZAFLD000027 as projectId
# MAGIC ,ZZAFLD000028 as issueResponsibility
# MAGIC ,ZZREPORTED_BY as businessPartnerNumber
# MAGIC ,ZZAGREEMENT_N as agreementNumber
# MAGIC ,ZZ_PROPERTY_NO as propertyNumber
# MAGIC ,ZZ_SR_AREA as serviceArea
# MAGIC ,ZZ_SR_SUB_AREA as serviceSubArea
# MAGIC ,ZZ_RESOLUTION_CD as resolutionCode
# MAGIC ,ZZ_SR_CATEGORY_CD as serviceCategoryCode
# MAGIC ,ZZ_ROOT_CAUSE_CD as rootCauseCode
# MAGIC ,ZZ_X_FACILITY_NAME_CD as facilityNameCode
# MAGIC ,ZZ_X_SECONDARY_ANALYSIS_CD as secondaryAnalysisCode
# MAGIC FROM Source

# COMMAND ----------

# DBTITLE 1,0SVY_ANSWER_TEXT
# MAGIC %sql
# MAGIC select
# MAGIC LANGUAGE as language
# MAGIC ,APPLICATION as bwApplication
# MAGIC ,QSTNNR as questionnaireId
# MAGIC ,QUESTION_ID as questionId
# MAGIC ,ANSWER as answerOption
# MAGIC ,TXTSH as answerShort
# MAGIC ,TXTMD as answerMedium
# MAGIC ,TXTLG as answerLong
# MAGIC ,MAIN_ANSWER as answerId
# MAGIC from source

# COMMAND ----------

# DBTITLE 1,0SVY_QSTNNR_TEXT
# MAGIC %sql
# MAGIC select
# MAGIC LANGUAGE as language
# MAGIC ,APPLICATION as bwApplication
# MAGIC ,QSTNNR as questionnaireId
# MAGIC ,TXTSH as questionnaireShort
# MAGIC ,TXTMD as questionnaireMedium
# MAGIC ,TXTLG as questionnaireLong
# MAGIC from
# MAGIC source

# COMMAND ----------

# DBTITLE 1,0SVY_QUEST_TEXT
# MAGIC %sql
# MAGIC SELECT
# MAGIC LANGUAGE as language
# MAGIC ,APPLICATION as bwApplication
# MAGIC ,QSTNNR as questionnaireId
# MAGIC ,QUEST as questionId
# MAGIC ,TXTSH as questionShort
# MAGIC ,TXTMD as questionMedium
# MAGIC ,TXTLG as questionLong
# MAGIC ,QUEST_ID as surveyQuestionId
# MAGIC FROM Source

# COMMAND ----------

# DBTITLE 1,0UC_PRICCLA_TEXT
# MAGIC %sql
# MAGIC select
# MAGIC SPRAS as language
# MAGIC ,PREISKLA as priceClassCode
# MAGIC ,PREISKLBEZ as priceClass
# MAGIC from source

# COMMAND ----------

# DBTITLE 1,0UC_PROFILE_TEXT
# MAGIC %sql
# MAGIC select
# MAGIC PROFILE as profileCode
# MAGIC SPRAS as language
# MAGIC PROFTEXT as profile
# MAGIC from source

# COMMAND ----------

# DBTITLE 1,0UC_PROLE_TEXT
# MAGIC %sql
# MAGIC SELECT
# MAGIC PROFROLE as profileRoleCode
# MAGIC ,SPRAS as language
# MAGIC ,PROFROLETXT as profileRole
# MAGIC from 

# COMMAND ----------

# DBTITLE 1,0UC_REGINST_STR_ATTR
# MAGIC %sql
# MAGIC select
# MAGIC LOGIKZW as logicalRegisterNumber
# MAGIC ,ZWNABR as registerNotRelevantToBilling
# MAGIC ,ANLAGE as installationId
# MAGIC ,BIS as validToDate
# MAGIC ,AB as validFromDate
# MAGIC ,GVERRECH as payRentalPrice
# MAGIC ,TARIFART as rateTypeCode
# MAGIC ,KONDIGR as rateFactGroupCode
# MAGIC ,PREISKLA as priceClassCode
# MAGIC ,LOEVM as deletedIndicator
# MAGIC ,UPDMOD as bwDeltaProcess
# MAGIC ,ZOPCODE as registerNotRelevantToBilling
# MAGIC ,PARTNER as businessPartnerNumber
# MAGIC ,TYPE as businessPartnerCategoryCode
# MAGIC ,Derived as businessPartnerCategory
# MAGIC ,BPKIND as businessPartnerTypeCode
# MAGIC ,Derived as businessPartnerType
# MAGIC ,BU_GROUP as businessPartnerGroupCode
# MAGIC ,Derived as businessPartnerGroup
# MAGIC ,BPEXT as externalBusinessPartnerNumber
# MAGIC ,BU_SORT1 as searchTerm1
# MAGIC ,BU_SORT2 as searchTerm2
# MAGIC ,TITLE as titleCode
# MAGIC ,Derived as title
# MAGIC ,XDELE as deletedIndicator
# MAGIC ,XBLCK as centralBlockBusinessPartner
# MAGIC ,ZZUSER as userId
# MAGIC ,ZZPAS_INDICATOR as paymentAssistSchemeIndicator
# MAGIC ,ZZBA_INDICATOR as billAssistIndicator
# MAGIC ,ZZAFLD00001Z as createdDate
# MAGIC ,ZZ_CONSENT1 as consent1Indicator
# MAGIC ,ZZWAR_WID as warWidowIndicator
# MAGIC ,ZZDISABILITY as disabilityIndicator
# MAGIC ,ZZGCH as goldCardHolderIndicator
# MAGIC ,ZZDECEASED as deceasedIndicator
# MAGIC ,ZZPCC as pensionConcessionCardIndicator
# MAGIC ,ZZELIGIBILITY as eligibilityIndicator
# MAGIC ,ZZDT_CHK as dateOfCheck
# MAGIC ,ZZPAY_ST_DT as paymentStartDate
# MAGIC ,ZZPEN_TY as pensionType
# MAGIC ,ZZ_CONSENT2 as consent2Indicator
# MAGIC ,NAME_ORG1 as organizationName1
# MAGIC ,NAME_ORG2 as organizationName2
# MAGIC ,NAME_ORG3 as organizationName3
# MAGIC ,FOUND_DAT as organizationFoundedDate
# MAGIC ,LOCATION_1 as internationalLocationNumber1
# MAGIC ,LOCATION_2 as internationalLocationNumber2
# MAGIC ,LOCATION_3 as internationalLocationNumber3
# MAGIC ,NAME_LAST as lastName
# MAGIC ,NAME_FIRST as firstName
# MAGIC ,NAMEMIDDLE as middleName
# MAGIC ,TITLE_ACA1 as academicTitleCode
# MAGIC ,Derived as academicTitle
# MAGIC ,NICKNAME as nickName
# MAGIC ,INITIALS as nameInitials
# MAGIC ,NAMCOUNTRY as countryName
# MAGIC ,LANGU_CORR as correspondanceLanguage
# MAGIC ,NATIO as nationality
# MAGIC ,PERSNUMBER as personNumber
# MAGIC ,XSEXU as unknownGenderIndicator
# MAGIC ,BU_LANGU as language
# MAGIC ,BIRTHDT as dateOfBirth
# MAGIC ,DEATHDT as dateOfDeath
# MAGIC ,PERNO as personnelNumber
# MAGIC ,NAME_GRP1 as nameGroup1
# MAGIC ,NAME_GRP2 as nameGroup2
# MAGIC ,MC_NAME1 as searchHelpLastName
# MAGIC ,MC_NAME2 as searchHelpFirstName
# MAGIC ,CRUSR as createdBy
# MAGIC ,CRDAT as createdDate
# MAGIC ,CRTIM as createdTime
# MAGIC ,CHUSR as changedBy
# MAGIC ,CHDAT as changedDate
# MAGIC ,CHTIM as changedTime
# MAGIC ,PARTNER_GUID as businessPartnerGUID
# MAGIC ,ADDRCOMM as communicationAddressNumber
# MAGIC ,TD_SWITCH as plannedChangeDocument
# MAGIC ,VALID_FROM as validFromDate
# MAGIC ,VALID_TO as validToDate
# MAGIC ,NATPERS as naturalPersonIndicator
# MAGIC ,ZZAFLD00000M as kidneyDialysisIndicator
# MAGIC ,ZZUNIT as patientUnit
# MAGIC ,ZZTITLE as patientTitleCode
# MAGIC ,Derived as patientTitle
# MAGIC ,ZZF_NAME as patientFirstName
# MAGIC ,ZZSURNAME as patientSurname
# MAGIC ,ZZAREACODE as patientAreaCode
# MAGIC ,ZZPHONE as patientPhoneNumber
# MAGIC ,ZZHOSP_NAME as hospitalName
# MAGIC ,ZZMACH_TYPE as patientMachineType
# MAGIC ,ZZON_DATE as machineTypeValidFromDate
# MAGIC ,ZZOFF_REAS as offReason
# MAGIC ,ZZOFF_DATE as machineTypeValidToDate
# MAGIC from source

# COMMAND ----------

# DBTITLE 1,0UC_TVORG_TEXT
# MAGIC %sql
# MAGIC select
# MAGIC SPRAS as language
# MAGIC ,APPLK as applicationArea
# MAGIC ,HVORG as mainTransactionLineItemCode
# MAGIC ,TVORG as subtransactionLineItemCode
# MAGIC ,TXT30 as subtransaction
# MAGIC from source

# COMMAND ----------

# DBTITLE 1,0UC_ZAHLKOND_TEXT
# MAGIC %sql
# MAGIC select
# MAGIC SPRAS as language
# MAGIC ,ZAHLKOND as paymentConditionCode
# MAGIC ,TEXT as paymentCondition
# MAGIC from source

# COMMAND ----------

# DBTITLE 1,0UCMTRDUNIT_ATTR
# MAGIC %sql
# MAGIC select
# MAGIC TERMSCHL as portion
# MAGIC ,EPER_ABL as intervalBetweenReadingAndEnd
# MAGIC ,AUF_ABL as intervalBetweenOrderAndReading
# MAGIC ,DOWNL_ABL as intervalBetweenDownloadAndReading
# MAGIC ,DRUCK_ABL as intervalBetweenPrintoutAndReading
# MAGIC ,ANSCH_AUF as intervalBetweenAnnouncementAndOrder
# MAGIC ,AUKSA_AUF as intervalBetweenPrintoutAndOrder
# MAGIC ,PORTION as portionNumber
# MAGIC ,ABLESER as meterReaderNumber 
# MAGIC ,ABLZEIT as meterReadingTime
# MAGIC ,AZVORABL as numberOfPreviousReadings
# MAGIC ,MDENR as numberOFMobileDataEntry
# MAGIC ,ABLKAR as meterReadingInterval
# MAGIC ,STANDKAR as entryInterval
# MAGIC ,EROEDAT as createdDate
# MAGIC ,ERNAM as createdBy
# MAGIC ,AENDDATE as lastChangedDate
# MAGIC ,AENDNAM as lastChangedBy
# MAGIC ,SPARTENTY1 as divisionCategory1
# MAGIC ,SPARTENTY2 as divisionCategory2
# MAGIC ,SPARTENTY3 as divisionCategory3
# MAGIC ,SPARTENTY4 as divisionCategory4
# MAGIC ,SPARTENTY5 as divisionCategory5
# MAGIC ,IDENT as factoryCalendar
# MAGIC ,SAPKAL as correctHolidayToWorkDay
# MAGIC ,STICHTAG as billingKeyDate
# MAGIC ,TAGE as numberOfDays
# MAGIC ,AUF_KAL as intervalBetweenOrderAndPlanned
# MAGIC ,ABL_Z as meterReadingCenter
# MAGIC from Source

# COMMAND ----------

# DBTITLE 1,ICACLEARINGRSNT
# MAGIC %sql
# MAGIC select
# MAGIC CACLEARINGREASON as clearingReasonCode
# MAGIC ,LANGUAGE as language
# MAGIC ,CACLEARINGREASONNAME as clearingReason
# MAGIC from source

# COMMAND ----------

# DBTITLE 1,ICADOCORIGCODET
# MAGIC %sql
# MAGIC select
# MAGIC CADOCUMENTORIGINCODE as documentOriginCode
# MAGIC ,LANGUAGE as language
# MAGIC ,CADOCUMENTORIGINCODENAME as documentOrigin
# MAGIC from source
