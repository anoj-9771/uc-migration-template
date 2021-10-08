# Databricks notebook source
# DBTITLE 1,OBP_CAT_TEXT
select
KEY1 as businessPartnerCategoyCode
,TXTLG as businessPartnerCategoy
,LANGU as language
,TXTLG as businessPartnerCategoy
,TXTSH as 
from source

# COMMAND ----------

# DBTITLE 1,0CAM_STREETCODE_TEXT
select
LANGU  as language           
,COUNTRY as countryShortName
,STRT_CODE as streetCode
,STREET as streetName
,CITY_CODE as cityCode
,MC_STREET 
,STREET_S15 as streetNameShort
,STREET_SHR as streetNameLong
from source

# COMMAND ----------

# DBTITLE 1,[0DF_REFIXFI_ATTR]
select
INTRENO as architecturalObjectInternalId
,FIXFITCHARACT as fixtureAndFittingCharacteristicCode
,VALIDTO as validToDate
,VALIDFROM as validFromDate
,WEIGHT as weightingValue
,RESULTVAL as resultValue
,ADDITIONALINFO as characteristicAdditionalValue
,MODERNMEASURE as modernizationMeasure
,AMOUNTPERAREA as amountPerAreaUnit
,CURRENCY as currencyKey
,FFCTACCURATE as applicableIndicator
,CHARACTAMTAREA as characteristicAmountArea
,CHARACTPERCENT as characteristicPercentage
,CHARACTAMTABS as characteristicPriceAmount
,MANDT as clientId
from source

# COMMAND ----------

# DBTITLE 1,[0EQUIPMENT_TEXT]
# MAGIC %sql
# MAGIC select
# MAGIC LANGU as language
# MAGIC ,EQUNR as equipmentNumber
# MAGIC ,DATETO as validToDate
# MAGIC ,DATEFROM as validFromDate
# MAGIC ,TXTMD as equipmentDescription
# MAGIC ,AEDAT as lastChangedDate
# MAGIC from source

# COMMAND ----------

# DBTITLE 1,[0UC_APPLK_TEXT]
# MAGIC %sql
# MAGIC select
# MAGIC LANGU as language
# MAGIC ,KEY1 as applicationAreaCode
# MAGIC ,TXTLG as  applicationArea
# MAGIC ,DATEFROM as validFromDate
# MAGIC ,DATETO as validToDate
# MAGIC ,TXTMD 
# MAGIC ,TXTSH 
# MAGIC from 
# MAGIC source

# COMMAND ----------

# DBTITLE 1,[0UC_DEVICEH_ATTR]
# MAGIC %sql
# MAGIC select
# MAGIC EQUNR as equipmentNumber
# MAGIC ,b.TXTMD as equipmentDescription
# MAGIC ,BIS as validToDate
# MAGIC ,AB as validFromDate
# MAGIC ,KOMBINAT as deviceCategoryCombination
# MAGIC ,LOGIKNR as logicalDeviceNumber
# MAGIC ,ZWGRUPPE as registerGroupCode
# MAGIC ,c.EZWG_INFO as registerGroup
# MAGIC ,EINBDAT  as installationDate
# MAGIC ,AUSBDAT as deviceRemovalDate
# MAGIC ,GERWECHS as activityReasonCode
# MAGIC ,d.GERWETXT as activityReason
# MAGIC ,DEVLOC as deviceLocation
# MAGIC ,WGRUPPE as windingGroup
# MAGIC ,LOEVM as deletedIndicator
# MAGIC ,UPDMOD as bwDeltaProcess
# MAGIC ,LOSSDTGROUP as lossDeterminationGroup
# MAGIC ,RATING as powerRating
# MAGIC ,P_VOLTAGE as primaryVoltage
# MAGIC ,S_VOLTAGE as secondaryVoltage
# MAGIC ,AMS as advancedMeteringSystem 
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
# MAGIC left join EQUNR b
# MAGIC on a.TXTMD = b.TXTMD and b.SPRAS = 'E'
# MAGIC left join ZWGRUPPE c
# MAGIC ON a.EZWG_INFO = c.EZWG_INFO
# MAGIC left join GERWECHS d
# MAGIC ON a.GERWETXT = d.GERWETXT and d.SPRAS = 'E'

# COMMAND ----------

# DBTITLE 1,0UC_SALES_SIMU_01
# MAGIC %sql
# MAGIC select
# MAGIC BILLINGRUNNO as billingRunNumber
# MAGIC ,SIMRUNID as simulationPeriodID
# MAGIC ,UPDMOD as bwDeltaProcess
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
# MAGIC ,KONZVER as franchiseContractCode
# MAGIC ,ABLEINH as meterReadingUnit
# MAGIC ,KTOKLASSE as accountClassCode
# MAGIC ,ZUORDDAA as billingAllocationDate
# MAGIC ,THGDAT as gasAllocationDate
# MAGIC ,ANZTAGE as numberOfContractDaysBilled
# MAGIC ,ANZVERTR as numberOfBilledContracts
# MAGIC ,CNTBILLDOC as numberOfBillingDocuments
# MAGIC ,BUPLA as businessPlace
# MAGIC ,BELZEILE as billingDocumentLineItemID
# MAGIC ,BELZART as lineItemTypeCode
# MAGIC ,AKLASSE as billingClassCode
# MAGIC ,BRANCHE as industryText
# MAGIC ,TVORG as subtransactionForDocumentItem
# MAGIC ,SAISON as seasonNumber
# MAGIC ,FRAN_TYPE as franchiseFeeTypeCode
# MAGIC ,KONZIGR as franchiseFeeGroupNumber
# MAGIC ,OUCONTRACT as individualContractID
# MAGIC ,TARIFTYP as rateCategoryCode
# MAGIC ,STATTART as statisticalAnalysisRateType
# MAGIC ,TARIFNR as rateId
# MAGIC ,KONDIGR as rateFactGroupCode
# MAGIC ,STTARIF as statisticalRate
# MAGIC ,TEMP_AREA as tempratureArea
# MAGIC ,DRCKSTUF as gasPressureLevel
# MAGIC ,VBRMONAT as consumptionMonth
# MAGIC ,INTLENGTH as updateTimePeriodBW
# MAGIC ,AB as validFromDate
# MAGIC ,BIS as validToDate
# MAGIC ,DIFFKZ as lineItemDiscountStatisticsIndicator
# MAGIC ,BUCHREL as billingLineItemReleventPostingIndicator
# MAGIC ,STGRQNT as quantityStatisticsGroupCode
# MAGIC ,STGRAMT as amountStatisticsGroupCode
# MAGIC ,ARTMENGE as billedQuantityStatisticsCode
# MAGIC ,GSBER as businessArea
# MAGIC ,KOSTL as costCenter
# MAGIC ,KOKRS as controllingArea
# MAGIC ,PRCTR as profitCenter
# MAGIC ,PS_PSP_PNR as wbsElement
# MAGIC ,AUFNR as orderNumber
# MAGIC ,WAERS as currencyKey
# MAGIC ,MASSBILL as billingMeasurementUnitCode
# MAGIC ,BETRAG as billingLineItemNetAmount
# MAGIC ,MENGE as billingQuantity
# MAGIC ,ZZAGREEMENT_NUM as agreementNumber
# MAGIC ,PREISBTR as priceAmount
# MAGIC ,CRM_PRODUCT as crmProduct
# MAGIC ,BELZART_NAME as lineItemType
# MAGIC ,PRINTDOC as printDocumentId
# MAGIC ,PRINTDOCLINE as printDocumentLineItemId
# MAGIC ,ANLAGE as installationId
# MAGIC ,ANLART as installationType
# MAGIC ,SPEBENE as voltageLevel
# MAGIC ,EIGENVERBR as plantConsumptionAmount
# MAGIC ,STAGRUVER as statisticsGroup
# MAGIC ,STAFO as statististicsUpdateGroupCode
# MAGIC ,CITY_CODE as cityCode
# MAGIC ,CITYP_CODE as districtCode
# MAGIC ,STREETCODE as streetCode
# MAGIC ,COUNTRY as countryShortName
# MAGIC ,REGION as stateCode
# MAGIC ,REGPOLIT as politicalRegionCode
# MAGIC ,REGIOGROUP as regionalStructureGrouping
# MAGIC ,SERVICEID as serviceProvider
# MAGIC ,SALESEMPLOYEE as salesEmployee
# MAGIC ,SALESPARTNER as salesPartner
# MAGIC ,SALESDOCUMENT as salesDocumentNumber
# MAGIC ,CONTRACTCLASS as contractClass
# MAGIC ,MWSKZ as taxSalesCode
# MAGIC ,BELEGDAT as billingDocumentCreateDate
# MAGIC ,BUDAT as postingDate
# MAGIC ,CNTINVDOC as numberofInvoicingDocuments
# MAGIC ,CPUDT as documentEnteredDate
# MAGIC ,FIKEY as reconciliationKeyForGeneralLedger
# MAGIC ,INT_UI_BW as internalPointDeliveryKeyBW
# MAGIC ,ITEMTYPE as invoiceItemType
# MAGIC ,PERIOD as fiscalYear
# MAGIC ,PERIV as fiscalYearVariant
# MAGIC ,RULEGR as ruleGroup
# MAGIC ,SBASW as taxBaseAmount
# MAGIC ,SBETW as taxAmount
# MAGIC ,SRCDOCCAT as sourceDocumentCategory
# MAGIC ,SRCDOCNO as numberOfSourceDocuments
# MAGIC ,STPRZ as taxRate
# MAGIC ,TXDAT as taxCalculationDate
# MAGIC ,TXJCD as taxJurisdictionDescription
