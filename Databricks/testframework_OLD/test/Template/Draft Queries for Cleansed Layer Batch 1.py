# Databricks notebook source
# DBTITLE 1,OBP_CAT_TEXT
# MAGIC %sql
# MAGIC select
# MAGIC KEY1 as businessPartnerCategoyCode
# MAGIC ,TXTLG as businessPartnerCategoy
# MAGIC from source WHERE LANGU = 'E'

# COMMAND ----------

# DBTITLE 1,0CAM_STREETCODE_TEXT
# MAGIC %sql
# MAGIC select
# MAGIC COUNTRY as countryShortName
# MAGIC ,STRT_CODE as streetCode
# MAGIC ,STREET as streetName
# MAGIC from source WHERE LANGU = 'E'

# COMMAND ----------

# DBTITLE 1,[0DF_REFIXFI_ATTR]
# MAGIC %sql
# MAGIC select
# MAGIC INTRENO as architecturalObjectInternalId
# MAGIC ,FIXFITCHARACT as fixtureAndFittingCharacteristicCode
# MAGIC ,VALIDTO as validToDate
# MAGIC ,VALIDFROM as validFromDate
# MAGIC ,WEIGHT as weightingValue
# MAGIC ,RESULTVAL as resultValue
# MAGIC ,ADDITIONALINFO as characteristicAdditionalValue
# MAGIC ,AMOUNTPERAREA as amountPerAreaUnit
# MAGIC ,FFCTACCURATE as applicableIndicator
# MAGIC ,CHARACTAMTAREA as characteristicAmountArea
# MAGIC ,CHARACTPERCENT as characteristicPercentage
# MAGIC ,CHARACTAMTABS as characteristicPriceAmount
# MAGIC from source

# COMMAND ----------

# DBTITLE 1,[0EQUIPMENT_TEXT]
# MAGIC %sql
# MAGIC select
# MAGIC EQUNR as equipmentNumber
# MAGIC ,DATETO as validToDate
# MAGIC ,DATEFROM as validFromDate
# MAGIC ,TXTMD as equipmentDescription
# MAGIC ,AEDAT as lastChangedDate
# MAGIC from source  WHERE LANGU = 'E'

# COMMAND ----------

# DBTITLE 1,[0UC_APPLK_TEXT]
# MAGIC %sql
# MAGIC select
# MAGIC LANGU as language
# MAGIC ,KEY1 as applicationAreaCode
# MAGIC ,TXTLG as  applicationArea
# MAGIC from 
# MAGIC source WHERE LANGU = 'E'

# COMMAND ----------

# DBTITLE 1,[0UC_DEVICEH_ATTR]
# MAGIC %sql
# MAGIC select
# MAGIC EQUNR as equipmentNumber
# MAGIC ,BIS as validToDate
# MAGIC ,AB as validFromDate
# MAGIC ,KOMBINAT as deviceCategoryCombination
# MAGIC ,LOGIKNR as logicalDeviceNumber
# MAGIC ,ZWGRUPPE as registerGroupCode
# MAGIC ,EINBDAT  as installationDate
# MAGIC ,AUSBDAT as deviceRemovalDate
# MAGIC ,GERWECHS as activityReasonCode
# MAGIC ,DEVLOC as deviceLocation
# MAGIC ,WGRUPPE as windingGroup
# MAGIC ,LOEVM as deletedIndicator
# MAGIC ,UPDMOD as bwDeltaProcess 
# MAGIC ,AMCG_CAP_GRP as advancedMeterCapabilityGroup
# MAGIC ,MSG_ATTR_ID as messageAttributeId
# MAGIC ,ZZMATNR as materialNumber
# MAGIC ,ZANLAGE as installationId
# MAGIC ,ZADDRNUMBER as addressNumber
# MAGIC ,ZCITY1 as cityName
# MAGIC ,ZHOUSE_NUM1 as houseNumber
# MAGIC ,ZSTREET as streetName
# MAGIC ,ZPOST_CODE1 as postalCode
# MAGIC ,ZTPLMA as superiorFunctionalLocationNumber
# MAGIC ,ZZ_POLICE_EVENT as policeEventNumber
# MAGIC ,ZAUFNR as orderNumber
# MAGIC ,ZERNAM as createdBy
# MAGIC from source a

# COMMAND ----------

# DBTITLE 1,0UC_SALES_SIMU_01
# MAGIC %sql
# MAGIC select
# MAGIC SIMRUNID as simulationPeriodID
# MAGIC ,BELNR as billingDocumentNumber
# MAGIC ,BUKRS as companyCode
# MAGIC ,SPARTE as divisonCode
# MAGIC ,VKONT as contractAccountNumber
# MAGIC ,VERTRAG as contractId
# MAGIC ,ABRVORG as billingTransactionCode
# MAGIC ,HVORG as mainTransactionLineItemCode
# MAGIC ,KOFIZ as contractAccountDeterminationID
# MAGIC ,PORTION as portionNumber
# MAGIC ,ANZTAGE as numberOfContractDaysBilled
# MAGIC ,ANZVERTR as numberOfBilledContracts
# MAGIC ,CNTBILLDOC as numberOfBillingDocuments
# MAGIC ,BELZEILE as billingDocumentLineItemID
# MAGIC ,BELZART as lineItemTypeCode
# MAGIC ,AKLASSE as billingClassCode
# MAGIC ,TVORG as subtransactionForDocumentItem
# MAGIC ,TARIFTYP as rateCategoryCode
# MAGIC ,STATTART as statisticalAnalysisRateType
# MAGIC ,STTARIF as statisticalRate
# MAGIC ,VBRMONAT as consumptionMonth
# MAGIC ,AB as validFromDate
# MAGIC ,BIS as validToDate
# MAGIC ,BUCHREL as billingLineItemReleventPostingIndicator
# MAGIC ,STGRQNT as quantityStatisticsGroupCode
# MAGIC ,STGRAMT as amountStatisticsGroupCode
# MAGIC ,ARTMENGE as billedQuantityStatisticsCode
# MAGIC ,KOKRS as controllingArea
# MAGIC ,PRCTR as profitCenter
# MAGIC ,PS_PSP_PNR as wbsElement
# MAGIC ,WAERS as currencyKey
# MAGIC ,MASSBILL as billingMeasurementUnitCode
# MAGIC ,BETRAG as billingLineItemNetAmount
# MAGIC ,MENGE as billingQuantity
# MAGIC ,ZZAGREEMENT_NUM as agreementNumber
# MAGIC ,PREISBTR as priceAmount
# MAGIC ,CRM_PRODUCT as crmProduct
# MAGIC ,BELZART_NAME as lineItemType
# MAGIC ,PRINTDOCLINE as printDocumentLineItemId
# MAGIC ,ANLAGE as installationId
# MAGIC ,CITY_CODE as cityCode
# MAGIC ,COUNTRY as countryShortName
# MAGIC ,REGION as stateCode
# MAGIC ,REGPOLIT as politicalRegionCode
# MAGIC ,SALESEMPLOYEE as salesEmployee
# MAGIC from Source

# COMMAND ----------

# DBTITLE 1,[0UC_SALES_STATS_03]
# MAGIC %sql
# MAGIC SELECT
# MAGIC BUDAT as postingDate
# MAGIC ,CPUDT as documentEnteredDate
# MAGIC ,FIKEY as reconciliationKeyForGeneralLedger
# MAGIC ,XCANC as isReversalTransaction
# MAGIC ,BELNR as billingDocumentNumber
# MAGIC ,BUKRS as companyCode
# MAGIC ,SPARTE as divisonCode
# MAGIC ,GPARTNER as businessPartnerNumber
# MAGIC ,VKONT as contractAccountNumber
# MAGIC ,VERTRAG as contractId
# MAGIC ,ABWVK as alternativeContractAccountForCollectiveBills
# MAGIC ,ABRVORG as billingTransactionCode
# MAGIC ,APPLK as applicationArea
# MAGIC ,HVORG as mainTransactionLineItemCode
# MAGIC ,KOFIZ as contractAccountDeterminationID
# MAGIC ,PORTION as portionNumber
# MAGIC ,ABLEINH as meterReadingUnit
# MAGIC ,ZUORDDAA as billingAllocationDate
# MAGIC ,ANZTAGE as numberOfContractDaysBilled
# MAGIC ,ANZVERTR as numberOfBilledContracts
# MAGIC ,CNTINVDOC as numberofInvoicingDocuments
# MAGIC ,CNTBILLDOC as numberOfBillingDocuments
# MAGIC ,BELZEILE as billingDocumentLineItemID
# MAGIC ,BELZART as lineItemTypeCode
# MAGIC ,AKLASSE as billingClassCode
# MAGIC ,BRANCHE as industryText
# MAGIC ,TVORG as subtransactionForDocumentItem
# MAGIC ,TARIFTYP as rateCategoryCode
# MAGIC ,STATTART as statisticalAnalysisRateType
# MAGIC ,TARIFNR as rateId
# MAGIC ,KONDIGR as rateFactGroupCode
# MAGIC ,STTARIF as statisticalRate
# MAGIC ,VBRMONAT as consumptionMonth
# MAGIC ,AB as validFromDate
# MAGIC ,BIS as validToDate
# MAGIC ,BUCHREL as billingLineItemReleventPostingIndicator
# MAGIC ,STGRQNT as quantityStatisticsGroupCode
# MAGIC ,STGRAMT as amountStatisticsGroupCode
# MAGIC ,ARTMENGE as billedQuantityStatisticsCode
# MAGIC ,KOKRS as controllingArea
# MAGIC ,PRCTR as profitCenter
# MAGIC ,PS_PSP_PNR as wbsElement
# MAGIC ,WAERS as currencyKey
# MAGIC ,MASSBILL as billingMeasurementUnitCode
# MAGIC ,BETRAG as billingLineItemNetAmount
# MAGIC ,MENGE as billingQuantity
# MAGIC ,PREISBTR as priceAmount
# MAGIC ,BELZART_NAME as lineItemType
# MAGIC ,PRINTDOC as printDocumentId
# MAGIC ,PRINTDOCLINE as printDocumentLineItemId
# MAGIC ,BELEGDAT as billingDocumentCreateDate
# MAGIC ,ANLAGE as installationId
# MAGIC ,CITY_CODE as cityCode
# MAGIC ,STREETCODE as streetCode
# MAGIC ,COUNTRY as countryShortName
# MAGIC ,REGION as stateCode
# MAGIC ,REGPOLIT as politicalRegionCode
# MAGIC ,SERVICEID as serviceProvider
# MAGIC ,SALESEMPLOYEE as salesEmployee
# MAGIC ,INT_UI_BW as internalPointDeliveryKeyBW
# MAGIC ,MWSKZ as taxSalesCode
# MAGIC ,SBASW as taxBaseAmount
# MAGIC ,SBETW as taxAmount
# MAGIC ,PERIOD as fiscalYear
# MAGIC ,PERIV as fiscalYearVariant
# MAGIC from Source

# COMMAND ----------

# DBTITLE 1,ZDM_DS_EDW_PORTION_TEXT (TE420)
# MAGIC %sql
# MAGIC SELECT
# MAGIC TERMSCHL as portion
# MAGIC ,TERMTEXT as scheduleMasterRecord
# MAGIC ,TERMERST as billingPeriodEndDate
# MAGIC ,PERIODEW as periodLengthMonths
# MAGIC ,PERIODET as periodCategory
# MAGIC ,ZUORDDAT as meterReadingAllocationDate
# MAGIC ,ABSZYK as allowableBudgetBillingCycles
# MAGIC ,EROEDAT as createdDate
# MAGIC ,ERNAM as createdBy
# MAGIC ,AENDDATE as lastChangedDate
# MAGIC ,AENDNAM as lastChangedBy
# MAGIC ,SPARTENTY1 as divisionCategory1
# MAGIC ,SPARTENTY2 as divisionCategory2
# MAGIC ,SPARTENTY3 as divisionCategory3
# MAGIC ,SPARTENTY4 as divisionCategory4
# MAGIC ,SPARTENTY5 as divisionCategory5
# MAGIC ,ABSZYKTER1 as budgetBillingCycle1
# MAGIC ,ABSZYKTER2 as budgetBillingCycle2
# MAGIC ,ABSZYKTER3 as budgetBillingCycle3
# MAGIC ,ABSZYKTER4 as budgetBillingCycle4
# MAGIC ,ABSZYKTER5 as budgetBillingCycle5
# MAGIC ,PARASATZ as parameterRecord
# MAGIC ,IDENT as factoryCalendar
# MAGIC ,SAPKAL as correctHolidayToWorkDay
# MAGIC ,PTOLERFROM as lowerLimitBillingPeriod
# MAGIC ,PTOLERTO as upperLimitBillingPeriod
# MAGIC ,PERIODED as periodLengthDays
# MAGIC ,WORK_DAY as isWorkDay
# MAGIC ,EXTRAPOLWASTE as extrapolationCategory
# MAGIC FROM Source

# COMMAND ----------

# DBTITLE 1,ZDSBILLFORMT (EFRM)
# MAGIC %sql
# MAGIC SELECT
# MAGIC FORMKEY as applicationForm
# MAGIC ,FORMCLASS as formClass
# MAGIC ,TDFORM as formName
# MAGIC ,EXIT_BIBL as userExitInclude
# MAGIC ,USER_TOP as userTopInclude
# MAGIC ,ORIG_SYST as originalSystem
# MAGIC ,ERDAT as createdDate
# MAGIC ,ERNAM as createdBy
# MAGIC ,ERSAP as createdBySAPAction
# MAGIC ,AEDAT as lastChangedDate
# MAGIC ,AENAM as lastChangedBy
# MAGIC ,AEUZEIT as lastChangedTime
# MAGIC ,AESAP as lastChangedBySAPAction
# MAGIC ,DESCRIPT as applicationFormDescription
# MAGIC ,EXIT_INIT as userExitBeforeHierarchyInterpretation
# MAGIC ,EXIT_CLOSE as userExitAfterHierarchyInterpretation
# MAGIC ,EXIT_DISPATCH as userExitForDataDispatch
# MAGIC ,FORMTYPE as formType
# MAGIC ,SMARTFORM as smartForm
# MAGIC ,GENGUID as genFormGUID
# MAGIC ,FORMGUID as applicationFormGUID
# MAGIC ,FUNC_NAME as functionName
# MAGIC ,FUNC_POOL as functionGroup
# MAGIC ,CROSSFCLASS as crossFormClassCollection
# MAGIC ,ADFORM as PDFFormName
# MAGIC ,DATADISPATCH_MOD as dataDispatchMode
# MAGIC ,PDF_DYNAMIC as dynamicPDFForm
# MAGIC FROM
# MAGIC Source

# COMMAND ----------

# DBTITLE 1,ZDSCONNSTAT (EDISCORDSTATET)
# MAGIC %sql
# MAGIC SELECT
# MAGIC ORDSTATE as confirmationStatusCode
# MAGIC ,DESCRIPT as confirmationStatus
# MAGIC FROM
# MAGIC Source
# MAGIC WHERE SPRAS = 'E'

# COMMAND ----------

# DBTITLE 1,ZDSCOUTRYT (T005T)
# MAGIC %sql
# MAGIC SELECT
# MAGIC LAND1 as countryCode
# MAGIC ,LANDX as countryShortName
# MAGIC ,NATIO as nationality
# MAGIC ,LANDX50 as countryName
# MAGIC ,NATIO50 as nationalityLong
# MAGIC FROM
# MAGIC Source
# MAGIC WHERE SPRAS = 'E'

# COMMAND ----------

# DBTITLE 1,ZDSDELTYPET (ADDRC_DELI_SERVT)
# MAGIC %sql
# MAGIC SELECT
# MAGIC DELI_SERV_TYPE as deliveryServiceTypeCode
# MAGIC ,DELI_SERV_DSCR as deliveryServiceDescription
# MAGIC FROM Source
# MAGIC WHERE LANGU = 'E'

# COMMAND ----------

# DBTITLE 1,ZDSDISPCTRLT (ESENDCONTROLT)
# MAGIC %sql
# MAGIC SELECT
# MAGIC SENDCONTROL as dispatchControlCode
# MAGIC DESCRIPTION as dispatchControlDescription
# MAGIC FROM
# MAGIC Source
# MAGIC WHERE SPRAS = 'E'

# COMMAND ----------

# DBTITLE 1,ZDSDOMAINTXT (DD07T)
# MAGIC %sql
# MAGIC SELECT
# MAGIC DOMNAME as domainName
# MAGIC DOMVALUE_L as domainValueSingleUpperLimit
# MAGIC DDTEXT as domainValueText
# MAGIC FROM Source
# MAGIC WHERE DDLANGUAGE = 'E'

# COMMAND ----------

# DBTITLE 1,ZDSMRCTRL (TE438T)
# MAGIC %sql
# MAGIC SELECT
# MAGIC SPRAS as language
# MAGIC ,TEXT30 as meterReadingControlDescription
# MAGIC FROM SOURCE
# MAGIC WHERE SPRAS = 'E'

# COMMAND ----------

# DBTITLE 1,ZDSMRCTRL (TE438)
# MAGIC %sql
# MAGIC SELECT
# MAGIC ABLESARTST as meterReadingControlCode
# MAGIC ,b.meterReadingControlDescription as meterReadingControlDescription -- make sure reference tables are tested
# MAGIC ,MAXANZKSA as numberOfCustomerReadings
# MAGIC ,MAXANZSCH as numberOfAutomaticEstimations
# MAGIC ,MAXANZABL as numberOfEstimationsAndCustomerReadings
# MAGIC FROM Source a
# MAGIC LEFT JOIN CLEANSED.TE438T b
# MAGIC WHERE b.language = 'E' --optional if TE438T is tested separately

# COMMAND ----------

# DBTITLE 1,ZDSOPCODE (TE405T)
# MAGIC %sql
# MAGIC SELECT
# MAGIC OPCODE as operationCode
# MAGIC ,OPCODETXT as operationDescription
# MAGIC FROM SOURCE
# MAGIC WHERE SPRAS = 'E'

# COMMAND ----------

# DBTITLE 1,ZDSOSINVT
# MAGIC %sql
# MAGIC SELECT
# MAGIC MANOUTS_IN as manualOutsortingReasonCode
# MAGIC ,TEXT30 as manualOutsortingReasonDescription
# MAGIC FROM
# MAGIC SOURCE
# MAGIC WHERE SPRAS = 'E'

# COMMAND ----------

# DBTITLE 1,ZDSOUTPAYMETH (CRMC_BUAG_PAYM_T)
# MAGIC %sql
# MAGIC SELECT
# MAGIC PAYM_METH as paymentMethodCode
# MAGIC COUNTRY as countryShortName
# MAGIC TEXT as paymentMethodDescription
# MAGIC FROM SOURCE
# MAGIC WHERE LANGU = 'E'

# COMMAND ----------

# DBTITLE 1,ZDSREGIDT (TE065T)
# MAGIC %sql
# MAGIC SELECT
# MAGIC SPARTYP as divisionCategory -- must be nullable false
# MAGIC ,ZWKENN as registerId -- must be nullable false
# MAGIC ,ZWKTXT as registerIdDescription
# MAGIC FROM
# MAGIC Source

# COMMAND ----------

# DBTITLE 1,ZDSREGTYPET (TE523T)
# MAGIC %sql
# MAGIC SELECT
# MAGIC ZWART as registerTypeCode
# MAGIC ZWARTTXT as regiisterTypeDescription
# MAGIC FROM
# MAGIC Source
# MAGIC WHERE SPRAS = 'E' 

# COMMAND ----------

# DBTITLE 1,ZDSTOLGRPT (TFK043T)
# MAGIC %sql
# MAGIC SELECT
# MAGIC TOGRU as toleranceGroupCode -- must be nullable = false
# MAGIC TXT40 as toleranceGroupDescription
# MAGIC FROM
# MAGIC Source
# MAGIC WHERE SPRAS = 'E' 

# COMMAND ----------

# DBTITLE 1,ZPDMMTRGRIDCHAR
# MAGIC %sql
# MAGIC SELECT
# MAGIC INTERNALCHAR as internalcharacteristic
# MAGIC ,INTERNALCOUNTER as internalCounterforArchivingObjectsbyECM
# MAGIC ,ATNAM as characteristicName
# MAGIC from Source

# COMMAND ----------

# DBTITLE 1,ZRE_DS_EDW_ARCOBJT (EHAUISU)
# MAGIC %sql
# MAGIC select
# MAGIC INTRENO as architecturalObjectInternalId -- must be nullable = false
# MAGIC ,PROPERTY as propertyNumber
# MAGIC ,AOTYPE_AO as architecturalObjectTypeCode
# MAGIC ,AONR_AO as architecturalObjectNumber
# MAGIC ,PARENT as parentArchitecturalObjectInternalId
# MAGIC ,AOTYPE_PA as parentArchitecturalObjectTypeCode
# MAGIC ,AONR_PA as parentArchitecturalObjectNumber
# MAGIC ,PARENT_PROPERTY as parentPropertyNumber
# MAGIC from Source

# COMMAND ----------

# DBTITLE 1,ZRE_DS_EDW_PROP_REL (ZCD_TPROP_REL)
# MAGIC %sql
# MAGIC SELECT
# MAGIC PROPERTY1 as property1Number
# MAGIC ,PROPERTY2 as property2Number
# MAGIC ,REL_TYPE1 as relationshipTypeCode1
# MAGIC ,DATE_FROM as validFromDate
# MAGIC ,REL_TYPE2 as relationshipTypeCode2
# MAGIC ,DATE_TO as validToDate
# MAGIC FROM
# MAGIC Source

# COMMAND ----------

# DBTITLE 1,ZRE_DS_EDW_PROPHIST (ZCD_TPROPTY_HIST)
# MAGIC %sql
# MAGIC SELECT
# MAGIC PROPERTY_NO as propertyNumber
# MAGIC ,SUP_PROP_TYPE as superiorPropertyTypeCode
# MAGIC ,b.superiorPropertyType as superiorPropertyType
# MAGIC ,INF_PROP_TYPE as inferiorPropertyTypeCode
# MAGIC ,c.inferiorPropertyType as inferiorPropertyType
# MAGIC ,DATE_FROM as validFromDate
# MAGIC ,DATE_TO as validToDate
# MAGIC ,CREATED_ON as createdDate
# MAGIC ,CREATED_BY as createdBy
# MAGIC ,CHANGED_ON as changedDate
# MAGIC ,CHANGED_BY as changedBy
# MAGIC ,DELTADATE as deltaDateTime
# MAGIC FROM Source a
# MAGIC left join ZCS_DS_SPROP_TEXT b
# MAGIC on a.SUP_PROP_TYPE = b.SUP_PROP_TYPE and SPRAS = 'E'
# MAGIC left join a.INF_PROP_TYPE = b.INF_PROP_TYPE where SPRAS= 'E'

# COMMAND ----------

# DBTITLE 1,ZRE_DS_EDW_TIVBDCHARAC_TXT
# MAGIC %sql
# MAGIC select
# MAGIC FIXFITCHARACT as fixtureAndFittingCharacteristicCode
# MAGIC ,XFIXFITCHARACT as fixtureAndFittingCharacteristicName
# MAGIC from source

# COMMAND ----------

# DBTITLE 1,UTILITIESCONTRACT -- mapping rule to be confirmed by Gulsen
# MAGIC %sql
# MAGIC SELECT
# MAGIC SAPClient as sapClient
# MAGIC ,ItemUUID as headerUUID
# MAGIC ,PodUUID as podUUID
# MAGIC ,HeaderUUID as headerUUID
# MAGIC ,UtilitiesContract as utilitiesContract
# MAGIC ,BusinessPartner as businessPartnerGroupNumber
# MAGIC ,BusinessPartnerFullName as businessPartnerGroupName
# MAGIC ,BusinessAgreement as businessAgreement
# MAGIC ,BusinessAgreementUUID as businessAgreementUUID
# MAGIC ,IncomingPaymentMethod as incomingPaymentMethod
# MAGIC ,IncomingPaymentMethodName as incomingPaymentMethodName
# MAGIC ,PaymentTerms as paymentTerms
# MAGIC ,PaymentTermsName as paymentTermsName
# MAGIC ,SoldToParty as soldToParty
# MAGIC ,SoldToPartyName as businessPartnerFullName
# MAGIC ,Division as division
# MAGIC ,DivisionName as division
# MAGIC ,ContractStartDate as contractStartDate
# MAGIC ,ContractEndDate as contractEndDate
# MAGIC ,CreationDate as creationDate
# MAGIC ,CreatedByUser as changedBy
# MAGIC ,LastChangeDate as lastChangedDate
# MAGIC ,LastChangedByUser as changedBy
# MAGIC ,ItemCategory as headerCategory
# MAGIC ,ItemCategoryName as headerCategoryName
# MAGIC ,Product as product
# MAGIC ,ProductDescription as productDescription
# MAGIC ,ItemType as itemType
# MAGIC ,ItemTypeName as itemTypeName
# MAGIC ,ProductType as productType
# MAGIC ,HeaderType as headerType
# MAGIC ,HeaderTypeName as headerTypeName
# MAGIC ,HeaderCategory as headerCategory
# MAGIC ,HeaderCategoryName as headerCategoryName
# MAGIC ,HeaderDescription as headerDescription
# MAGIC ,IsDeregulationPod as isDeregulationPod
# MAGIC ,UtilitiesPremise as premise
# MAGIC ,NumberOfPersons as numberOfPersons
# MAGIC ,CityName as cityName
# MAGIC ,StreetName as streetName
# MAGIC ,HouseNumber as houseNumber
# MAGIC ,PostalCode as postalCode
# MAGIC ,Building as building
# MAGIC ,AddressTimeZone as addressTimeZone
# MAGIC ,CountryName as countryName
# MAGIC ,RegionName as regionName
# MAGIC ,NumberOfContractChanges as numberOfContractChanges
# MAGIC ,IsOpen as isOpen
# MAGIC ,IsDistributed as isDistributed
# MAGIC ,HasError as hasError
# MAGIC ,IsToBeDistributed as isToBeDistributed
# MAGIC ,IsIncomplete as isIncomplete
# MAGIC ,IsStartedDueToProductChange as isStartedDueToProductChange
# MAGIC ,IsInActivation as isInActivation
# MAGIC ,IsInDeactivation as isInDeactivation
# MAGIC ,IsCancelled as isCancelled
# MAGIC ,CancellationMessageIsCreated as cancellationMessageIsCreated
# MAGIC ,SupplyEndCanclnMsgIsCreated as supplyEndCanclInMsgIsCreated
# MAGIC ,ActivationIsRejected as activationIsRejected
# MAGIC ,DeactivationIsRejected as deactivationIsRejected
# MAGIC ,NumberOfContracts as numberOfContracts
# MAGIC ,NumberOfActiveContracts as numberOfActiveContracts
# MAGIC ,NumberOfIncompleteContracts as numberOfIncompleteContracts
# MAGIC ,NumberOfDistributedContracts as numberOfDistributedContracts
# MAGIC ,NmbrOfContractsToBeDistributed as numberOfContractsToBeDistributed
# MAGIC ,NumberOfBlockedContracts as numberOfBlockedContracts
# MAGIC ,NumberOfCancelledContracts as numberOfCancelledContracts
# MAGIC ,NumberOfProductChanges as numberOfProductChanges
# MAGIC ,NmbrOfContractsWProductChanges as numberOfContractsWProductChanges
# MAGIC ,NmbrOfContrWthEndOfSupRjctd as numberOfContrWthEndOfSupRjctd
# MAGIC ,NmbrOfContrWthStartOfSupRjctd as numberOfContrWthStartOfSupRjctd
# MAGIC ,NmbrOfContrWaitingForEndOfSup as numberOfContrWaitingForEndOfSup
# MAGIC ,NmbrOfContrWaitingForStrtOfSup as numberOfContrWaitingForStrtOfSup
# MAGIC ,CreationDate_E as creationDateE
# MAGIC ,LastChangeDate_E as lastChangedDateE
# MAGIC ,ContractStartDate_E as contractStartDateE
# MAGIC ,ContractEndDate_E as contractEndDateE
# MAGIC FROM Source
# MAGIC where language = 'E'

# COMMAND ----------

# DBTITLE 1,0UCPREMISE_ATTR_2
# MAGIC %sql
# MAGIC select
# MAGIC VSTELLE as premise
# MAGIC ,HAUS as propertyNumber
# MAGIC ,VBSART as typeOfPremise
# MAGIC ,EIGENT as owner
# MAGIC ,OBJNR as objectNumber
# MAGIC ,TPLNUMMER as functionalLocationNumber
# MAGIC ,ERDAT as createdDate
# MAGIC ,ERNAM as createdBy
# MAGIC ,AEDAT as lastChangedDate
# MAGIC ,AENAM as lastChangedBy
# MAGIC ,LOEVM as deletedIndicator
# MAGIC ,ANZPERS as numberOfPersons
# MAGIC ,FLOOR as floorNumber
# MAGIC ,ROOMNUMBER as appartmentNumber
# MAGIC ,HPTWOHNSITZ as mainResidence
# MAGIC ,STR_ERG4 as street5
# MAGIC ,UPDMOD as bwDeltaProcess
# MAGIC from source
# MAGIC where language = 'E'

# COMMAND ----------

# DBTITLE 1,0UCCONTRACT_ATTR_2
# MAGIC %sql
# MAGIC SELECT
# MAGIC BUKRS as companyCode
# MAGIC SPARTE as divisonCode
# MAGIC KOFIZ as contractAccountDeterminationID
# MAGIC ABSZYK as allowableBudgetBillingCycles
# MAGIC GEMFAKT as invoiceContractsJointly
# MAGIC ABRSPERR as billBlockingReasonCode
# MAGIC ABRFREIG as billReleasingReasonCode
# MAGIC VBEZ as contractText
# MAGIC EINZDAT_ALT as legacyMoveInDate
# MAGIC KFRIST as numberOfCancellations
# MAGIC VERLAENG as numberOfRenewals
# MAGIC PERSNR as personnelNumber
# MAGIC VREFER as contractNumberLegacy
# MAGIC ERDAT as createdDate
# MAGIC ERNAM as createdBy
# MAGIC AEDAT as lastChangedDate
# MAGIC AENAM as lastChangedBy
# MAGIC LOEVM as deletedIndicator
# MAGIC FAKTURIERT as isContractInvoiced
# MAGIC PS_PSP_PNR as wbsElement
# MAGIC AUSGRUP as outsortingCheckGroupForBilling
# MAGIC OUTCOUNT as manualOutsortingCount
# MAGIC PYPLS as paymentPlanStartMonth
# MAGIC SERVICEID as serviceProvider
# MAGIC PYPLA as alternativePaymentStartMonth
# MAGIC BILLFINIT as contractTerminatedForBilling
# MAGIC SALESEMPLOYEE as salesEmployee
# MAGIC INVOICING_PARTY as invoicingParty
# MAGIC CANCREASON_NEW as cancellationReasonCRM
# MAGIC ANLAGE as installationId
# MAGIC VKONTO as contractAccountNumber
# MAGIC KZSONDAUSZ as specialMoveOutCase
# MAGIC EINZDAT as moveInDate
# MAGIC AUSZDAT as moveOutDate
# MAGIC ABSSTOPDAT as budgetBillingStopDate
# MAGIC XVERA as isContractTransferred
# MAGIC ZGPART as businessPartnerGroupNumber
# MAGIC ZDATE_FROM as validFromDate
# MAGIC ZZAGREEMENT_NUM as agreementNumber
# MAGIC VSTELLE as premise
# MAGIC HAUS as propertyNumber
# MAGIC ZZZ_ADRMA as alternativeAddressNumber
# MAGIC ZZZ_IDNUMBER as identificationNumber
# MAGIC ZZ_ADRNR as addressNumber
# MAGIC ZZ_OWNER as objectReferenceId
# MAGIC ZZ_OBJNR as objectNumber
# MAGIC CPERS as collectionsContactPerson
# MAGIC VERTRAG  as contractId
# MAGIC from Source

# COMMAND ----------

# DBTITLE 1,0UC_GERWECHS_TEXT
# MAGIC %sql
# MAGIC select
# MAGIC GERWECHS as activityReasonCode
# MAGIC ,GERWETXT as activityReason
# MAGIC ,SPRAS as language
# MAGIC from source

# COMMAND ----------

# DBTITLE 1,0FC_BLART_TEXT
# MAGIC %sql
# MAGIC select
# MAGIC APPLK as applicationArea
# MAGIC ,BLART as documentTypeCode
# MAGIC ,LTEXT as documentType
# MAGIC ,SPRAS as language
# MAGIC from source
