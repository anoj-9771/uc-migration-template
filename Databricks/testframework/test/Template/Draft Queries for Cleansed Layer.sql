-- Databricks notebook source
-- DBTITLE 1,1-VIBDCHARACT
SELECT
INTRENO as architecturalObjectInternalId
,FIXFITCHARACT	as	fixtureAndFittingCharacteristicCode
,b.XFIXFITCHARACT	as	fixtureAndFittingCharacteristic
,VALIDTO	as	validToDate
,VALIDFROM	as	validFromDate
,AMOUNTPERAREA	as	amountPerAreaUnit
,FFCTACCURATE	as	applicableIndicator
,CHARACTAMTAREA	as	characteristicAmountArea
,CHARACTPERCENT	as	characteristicPercentage
,CHARACTAMTABS	as	characteristicPriceAmount
,CHARACTCOUNT	as	characteristicCount
,SUPPLEMENTINFO	as	supplementInfo
FROM VIBDCHARACT a
LEFT JOIN 0DF_REFIXFI_TEXT b
on a.FIXFITCHARACT = b.FIXFITCHARACT and b.SPRAS = ‘E’


-- COMMAND ----------

-- DBTITLE 1,1-VIBDCHARACT_DELTA [Sample]
select *, partition from
(SELECT
FIXFITCHARACT	as	fixtureAndFittingCharacteristicCode
,b.XFIXFITCHARACT	as	fixtureAndFittingCharacteristic
,VALIDTO	as	validToDate
,VALIDFROM	as	validFromDate
,AMOUNTPERAREA	as	amountPerAreaUnit
,FFCTACCURATE	as	applicableIndicator
,CHARACTAMTAREA	as	characteristicAmountArea
,CHARACTPERCENT	as	characteristicPercentage
,CHARACTAMTABS	as	characteristicPriceAmount
,CHARACTCOUNT	as	characteristicCount
,SUPPLEMENTINFO	as	supplementInfo
FROM VIBDCHARACT a
LEFT JOIN 0DF_REFIXFI_TEXT b
on a.FIXFITCHARACT = b.FIXFITCHARACT) delta
LEFT JOIN VIBDCHARACTControltable
where delta.id = VIBDCHARACTControltable.id
and  rn = 1
and status in ('insert','update')

-- COMMAND ----------

-- DBTITLE 1,2-0UC_MTR_DOC - Not yet ready
SELECT
ABLBELNR 	as	meterReadingId
,EQUNR 	as	equipmentNumber
,ZWNUMMER 	as	registerNumber
,ADAT 	as	meterReadingDate
,MRESULT 	as	meterReadingTaken 
,MR_BILL 	as	duplicate
,AKTIV 	as	meterReadingActive 
,ADATSOLL 	as	scheduledMeterReadingDate 
,ABLSTAT 	as	meterReadingStatus 
,ABLHINW 	as	notefromMeterReader 
,ABLESART 	as	scheduledMeterReadingCategory 
,ABLESER 	as	meterReaderNumber 
,MDEUPL 	as	orderHasBeenOutput 
,ISTABLART 	as	meterReadingType 
,ABLESTYP 	as	meterReadingCategory 
,MASSREAD 	as	unitOfMeasurementMeterReading 
,UPDMOD 	as	bwDeltaProcess
,LOEVM 	as	deletionIndicator 
,PRUEFPKT 	as	independentValidation 
,POPCODE 	as	dependentValidation 
,AMS 	as	advancedMeteringSystem 
,TRANSSTAT 	as	transferStatusCode 
,TRANSTSTAMP 	as	timeStamp
,SOURCESYST 	as	sourceSystemOrigin
,ZPREV_ADT 	as	actualmeterReadingDate 
,ZPREV_MRESULT 	as	meterReadingTaken 
,ZZ_PHOTO_IND 	as	meterPhotoIndicator 
,ZZ_FREE_TEXT 	as	freeText 
,ZZ_COMM_CODE 	as	meterReadingCommentCode 
,ZZ_NO_READ_CODE 	as	noReadCode 
,ZGERNR 	as	DeviceNumber
,ZADATTATS 	as	actualMeterReadingDate 
,ZWNABR 	as	registerNotRelevantToBilling 
,AEDAT 	as	lastChangedDate
FROM EABL

-- COMMAND ----------

-- DBTITLE 1,3-VIBDAO
SELECT
INTRENO	as	architecturalObjectInternalId
,AOID	as	architecturalObjectId
,AOTYPE	as	architecturalObjectTypeCode
,b.XMAOTYPE	as	architecturalObjectType
,AONR	as	architecturalObjectNumber
,VALIDFROM	as	validFromDate
,VALIDTO	as	validToDate
,PARTAOID	as	partArchitecturalObjectId
,OBJNR	as	objectNumber
,RERF	as	firstEnteredBy
,DERF	as	firstEnteredOnDate
,TERF	as	firstEnteredTime
,REHER	as	firstEnteredSource
,RBEAR	as	employeeId
,DBEAR	as	lastEdittedOnDate
,TBEAR	as	lastEdittedTime
,RBHER	as	lastEdittedSource
,RESPONSIBLE	as	responsiblePerson
,USEREXCLUSIVE	as	exclusiveUser
,LASTRENO	as	lastRelocationDate
,MEASSTRC	as	measurementStructure
,DOORPLT	as	shortDescription
,RSAREA	as	reservationArea
,SINSTBEZ	as	maintenanceDistrict
,SVERKEHR	as	businessEntityTransportConnectionsIndicator
,ZCD_PROPERTY_NO	as	propertyNumber
,ZCD_PROP_CR_DATE	as	propertyCreatedDate
,ZCD_PROP_LOT_NO	as	propertyLotNumber
,ZCD_REQUEST_NO	as	propertyRequestNumber
,ZCD_PLAN_TYPE	as	planTypeCode
,e.DESCRIPTION	as	planType
,ZCD_PLAN_NUMBER	as	planNumber
,ZCD_PROCESS_TYPE	as	processTypeCode
,f.DESCRIPTION	as	processType
,ZCD_ADDR_LOT_NO	as	addressLotNumber
,ZCD_LOT_TYPE	as	lotTypeCode
,ZCD_UNIT_ENTITLEMENT	as	unitEntitlement
,ZCD_NO_OF_FLATS	as	flatCount
,ZCD_SUP_PROP_TYPE	as	superiorPropertyTypeCode
,ref1.DESCRIPTION	as	superiorPropertyType
,ZCD_INF_PROP_TYPE	as	inferiorPropertyTypeCode
,ref2.DESCRIPTION	as	inferiorPropertyType
,ZCD_STORM_WATER_ASSESS	as	stormWaterAssesmentIndicator
,ZCD_IND_MLIM	as	mlimIndicator
,ZCD_IND_WICA	as	wicaIndicator
,ZCD_IND_SOPA	as	sopaIndicator
,ZCD_IND_COMMUNITY_TITLE	as	communityTitleIndicator
,ZCD_SECTION_NUMBER	as	sectionNumber
,ZCD_HYDRA_CALC_AREA	as	hydraCalculatedArea
,ZCD_HYDRA_AREA_UNIT	as	hydraAreaUnit
,ZCD_HYDRA_AREA_FLAG	as	hydraAreaIndicator
,ZCD_CASENO_FLAG	as	caseNumberIndicator
,ZCD_OVERRIDE_AREA	as	overrideArea
,ZCD_OVERRIDE_AREA_UNIT	as	overrideAreaUnit
,ZCD_CANCELLATION_DATE	as	cancellationDate
,ZCD_CANC_REASON	as	cancellationReasonCode
,ZCD_COMMENTS	as	comments
,ZCD_PROPERTY_INFO	as	propertyInfo
,OBJNRTRG	as	targetObjectNumber
,DIAGRAM_NO	as	diagramNumber
,FIXFITCHARACT	as	fixtureAndFittingCharacteristicCode
,g.XFIXFITCHARACT	as	fixtureAndFittingCharacteristic
FROM 0UC_CONNOBJ_ATTR_2 a
left join superiorPropertyType1 b 
on b.ZCD_SUP_PROP_TYPE = a.ZCD_SUP_PROP_TYPE
left join inferiorPropertyType1 c
on c.ZCD_INF_PROP_TYPE = a.ZCD_INF_PROP_TYPE
left join ZEDWAOTYPTXT d
on a.AOTYPE = d.AOTYPE and d.SPRAS = 'E'
left join ZDSPLANTYPET e
on a.ZCD_PLAN_TYPE = e.PLAN_TYPE and e.LANGU ='E'
left join ZDSPROCTYPET f
on a.ZCD_PROCESS_TYPE = f.PROCESS_TYPE and f.LANGUE='E'
left join 0DF_REFIXFI_TEXT g
on a.FIXFITCHARACT = g.FIXFITCHARACT and g.SPRAS='E'
left join ZCS_DS_SPROP_TEXT ref1 
ON ref1.SUPERIOR_PROP_TYPE = a.ZCD_SUP_PROP_TYPE and ref1.LANGU='E'
left join ZCS_DS_IPROP_TEXT ref2 
ON ref2.INFERIOR_PROP_TYPE = a.ZCD_INF_PROP_TYPE and ref2.LANGU='E'


-- COMMAND ----------

-- DBTITLE 1,4-EASTIH
SELECT
INDEXNR	as	consecutiveNumberOfRegisterRelationship
,LOGIKZW	as	logicalRegisterNumber
,BIS	as	validToDate
,AB	as	validFromDate
,PRUEFGR	as	validationGroupForDependentValidations
,ZWZUART	as	registerRelationshipType
,ERDAT	as	createdDate
,ERNAM	as	createdBy
,AEDAT	as	lastChangedDate
,AENAM	as	changedBy
,OPCODE	as	operationCode
,ONLY_BILLING_REL	as	onlyBillingIndicator
FROM EASTIH

-- COMMAND ----------

-- DBTITLE 1,5-ERCH
SELECT
BELNR	as	billingDocumentNumber
,BUKRS	as	companyCode
,b.TXTMD	as	companyName
,SPARTE	as	divisonCode
,GPARTNER	as	businessPartnerNumber
,VKONT	as	contractAccountNumber
,VERTRAG	as	contractID
,BEGABRPE	as	startBillingPeriod
,ENDABRPE	as	endBillingPeriod
,ABRDATS	as	billingScheduleDate
,ADATSOLL	as	meterReadingScheduleDate
,PTERMTDAT	as	billingPeriodEndDate
,BELEGDAT	as	billingDocumentCreateDate
,ABWVK	as	alternativeContractAccountForCollectiveBills
,BELNRALT	as	previousDocumentNumber
,STORNODAT	as	reversalDate
,ABRVORG	as	billingTransactionCode
,HVORG	as	mainTransactionLineItemCode
,KOFIZ	as	contractAccountDeterminationID
,PORTION	as	portionNumber
,FORMULAR	as	formName
,SIMULATION	as	billingSimulationIndicator
,BELEGART	as	documentTypeCode
,BERGRUND	as	backbillingCreditReasonCode
,BEGNACH	as	backbillingStartPeriod
,TOBRELEASD	as	DocumentNotReleasedIndicator
,TXJCD	as	taxJurisdictionDescription
,KONZVER	as	franchiseContractCode
,EROETIM	as	billingDocumentCreateTime
,ERCHP_V	as	ERCHP_Exist_IND
,ABRVORG2	as	periodEndBillingTransactionCode
,ABLEINH	as	meterReadingUnit
,ENDPRIO	as	billingEndingPriorityCodfe
,ERDAT	as	createdDate
,ERNAM	as	createdBy
,AEDAT	as	lastChangedDate
,AENAM	as	changedBy
,BEGRU	as	authorizationGroupCode
,LOEVM	as	deletedIndicator
,ABRDATSU	as	suppressedBillingOrderScheduleDate
,ABRVORGU	as	suppressedBillingOrderTransactionCode
,N_INVSEP	as	jointInvoiceAutomaticDocumentIndicator
,ABPOPBEL	as	BudgetBillingPlanCode
,MANBILLREL	as	manualDocumentReleasedInvoicingIndicator
,BACKBI	as	backbillingTypeCode
,PERENDBI	as	billingPeriodEndType
,NUMPERBB	as	backbillingPeriodNumber
,BEGEND	as	periodEndBillingStartDate
,ENDOFBB	as	backbillingPeriodEndIndicator
,ENDOFPEB	as	billingPeriodEndIndicator
,NUMPERPEB	as	billingPeriodEndCount
,SC_BELNR_H	as	billingDoumentAdjustmentReversalCount
,SC_BELNR_N	as	billingDocumentNumberForAdjustmentReverssal
,ZUORDDAA	as	billingAllocationDate
,BILLINGRUNNO	as	billingRunNumber
,SIMRUNID	as	simulationPeriodID
,KTOKLASSE	as	accountClassCode
,ORIGDOC	as	billingDocumentOriginCode
,NOCANC	as	billingDonotExecuteIndicator
,ABSCHLPAN	as	billingPlanAdjustIndicator
,MEM_OPBEL	as	newBillingDocumentNumberForReversedInvoicing
,MEM_BUDAT	as	billingPostingDateInDocument
,EXBILLDOCNO	as	externalDocumentNumber
,BCREASON	as	reversalReasonCode
,NINVOICE	as	billingDocumentWithoutInvoicingCode
,NBILLREL	as	billingRelavancyIndicator
,CORRECTION_DATE	as	errorDetectedDate
,BASDYPER	as	basicCategoryDynamicPeriodControlCode
,ESTINBILL	as	meterReadingResultEstimatedBillingIndicator
,ESTINBILLU	as	SuppressedOrderEstimateBillingIndicator
,ESTINBILL_SAV	as	originalValueEstimateBillingIndicator
,ESTINBILL_USAV	as	suppressedOrderBillingIndicator
,ACTPERIOD	as	currentBillingPeriodCategoryCode
,ACTPERORG	as	toBeBilledPeriodOriginalCategoryCode
,EZAWE	as	incomingPaymentMethodCode
,DAUBUCH	as	standingOrderIndicator
,FDGRP	as	planningGroupNumber
,BILLING_PERIOD	as	billingKeyDate
,OSB_GROUP	as	onsiteBillingGroupCode
,BP_BILL	as	resultingBillingPeriodIndicator
,MAINDOCNO	as	billingDocumentPrimaryInstallationNumber
,INSTGRTYPE	as	instalGroupTypeCode
,INSTROLE	as	instalGroupRoleCode
FROM ERCH a
LEFT JOIN 0COMP_CODE_TEXT b
ON a.BUKRS = b.BUKRS AND b.LANGU='E'


-- COMMAND ----------

-- DBTITLE 1,6-DBERCHZ2
SELECT
BELNR	as	billingDocumentNumber
,BELZEILE	as	billingDocumentLineItemID
,EQUNR	as	equipmentNumber
,GERAET	as	deviceNumber
,MATNR	as	materialNumber
,ZWNUMMER	as	registerNumber
,INDEXNR	as	registerRelationshipConsecutiveNumber
,ABLESGR	as	meterReadingReasonCode
,ABLESGRV	as	previousMeterReadingReasonCode
,ATIM	as	billingMeterReadingTime
,ATIMVA	as	previousMeterReadingTime
,ADATMAX	as	maxMeterReadingDate
,ATIMMAX	as	maxMeterReadingTime
,THGDATUM	as	serviceAllocationDate
,ZUORDDAT	as	meterReadingAllocationDate
,ABLBELNR	as	suppressedMeterReadingDocumentID
,LOGIKNR	as	logicalDeviceNumber
,LOGIKZW	as	logicalRegisterNumber
,ISTABLART	as	meterReadingTypeCode
,ISTABLARTVA	as	previousMeterReadingTypeCode
,EXTPKZ	as	meterReadingResultsSimulationIndicator
,BEGPROG	as	forecastPeriodStartDate
,ENDEPROG	as	forecastPeriodEndDate
,ABLHINW	as	meterReaderNoteText
,V_ZWSTAND	as	meterReadingBeforeDecimalPoint
,N_ZWSTAND	as	meterReadingAfterDecimalPoint
,V_ZWSTNDAB	as	billedMeterReadingBeforeDecimalPlaces
,N_ZWSTNDAB	as	billedMeterReadingAfterDecimalPlaces
,V_ZWSTVOR	as	billedMeterReadingBeforeDecimalPlaces
,N_ZWSTVOR	as	previousMeterReadingAfterDecimalPlaces
,V_ZWSTDIFF	as	meterReadingDifferenceBeforeDecimalPlaces
,N_ZWSTDIFF	as	meterReadingDifferenceAfterDecimalPlaces
FROM DBERCHZ2

-- COMMAND ----------

-- DBTITLE 1,7-DBERCHZ1
SELECT
BELNR	as	billingDocumentNumber
,BELZEILE	as	billingDocumentLineItemId
,CSNO	as	B]billingSequenceNumber
,BELZART	as	lineItemTypeCode
,ABSLKZ	as	billingLineItemBudgetBillingIndicator
,DIFFKZ	as	lineItemDiscountStatisticsIndicator
,BUCHREL	as	billingLineItemReleventPostingIndicator
,MENGESTREL	as	billedValueStatisticallyReleventIndicator
,BETRSTREL	as	billingLineItemStatisticallyReleventAmount
,STGRQNT	as	quantityStatisticsGroupCode
,STGRAMT	as	amountStatisticsGroupCoide
,PRINTREL	as	billingLinePrintReleventIndicator
,AKLASSE	as	billingClassCode
,b.TEXT	as	billingClass
,BRANCHE	as	industryText
,TVORG	as	subtransactionForDocumentItem
,GEGEN_TVORG	as	offsettingTransactionSubtransactionForDocumentItem
,LINESORT	as	poresortingBillingLineItems
,AB	as	validFromDate
,BIS	as	validToDate
,TIMTYPZA	as	billingLineItemTimeCategoryCode
,SCHEMANR	as	billingSchemaNumber
,SNO	as	billingSchemaStepSequenceNumber
,PROGRAMM	as	variantProgramNumber
,MASSBILL	as	billingMeasurementUnitCode
,SAISON	as	seasonNumber
,TIMBASIS	as	timeBasisCode
,TIMTYP	as	timeCategoryCode
,FRAN_TYPE	as	franchiseFeeTypeCode
,KONZIGR	as	franchiseFeeGroupNumber
,TARIFTYP	as	rateTypeCode
,TARIFNR	as	rateId
,KONDIGR	as	rateFactGroupNumber
,STTARIF	as	statisticalRate
,GEWKEY	as	weightingKeyId
,WDHFAKT	as	referenceValuesForRepetitionFactor
,TEMP_AREA	as	tempratureArea
,DYNCANC01	as	reversalDynamicPeriodControl1
,DYNCANC02	as	reversalDynamicPeriodControl2
,DYNCANC03	as	reversalDynamicPeriodControl3
,DYNCANC04	as	reversalDynamicPeriodControl4
,DYNCANC05	as	reversalDynamicPeriodControl5
,DYNCANC	as	reverseBackbillingIndicator
,DYNEXEC	as	allocateBackbillingIndicator
,LRATESTEP	as	eateStepLogicalNumber
,PEB	as	periodEndBillingIndicator
,STAFO	as	statististicsUpdateGroupCode
,ARTMENGE	as	billedQuantityStatisticsCode
,STATTART	as	statisticalAnalysisRateType
,TIMECONTRL	as	periodControlCode
,TCNUMTOR	as	timesliceNumeratorTimePortion
,TCDENOMTOR	as	timesliceDenominatorTimePortion
,TIMTYPQUOT	as	timesliceTimeCatogoryTimePortion
,AKTIV	as	meterReadingActiveIndicator
,KONZVER	as	franchiseContractIndicator
,PERTYP	as	billingPeriodInternalCategoryCode
,OUCONTRACT	as	individualContractID
,V_ABRMENGE	as	billingQuantityPlaceBeforeDecimalPoint
,N_ABRMENGE	as	billingQuantityPlaceAfterDecimalPoint
from DBERCHZ1 a
left join 0UC_AKLASSE_TEXT b
on a.AKLASSE = b.AKLASSE and b.SPRAS ='E'

-- COMMAND ----------

-- DBTITLE 1,8-ZCD_TPROPTY_HIST
SELECT
PROPERTY_NO	as	propertyNumber
SUP_PROP_TYPE	as	superiorPropertyTypeCode
Derived	as	superiorPropertyType
INF_PROP_TYPE	as	inferiorPropertyTypeCode
Derived	as	inferiorPropertyType
DATE_FROM	as	validFromDate
DATE_TO	as	validToDate
from ZCD_TPROPTY_HIST a
join (SELECT  ZCD_SUP_PROP_TYPE,  ref1.DESCRIPTION as SUPERIOR_PROP_TYPE
from 0uc_connobj_attr_2 a
join ZCS_DS_SPROP_TEXT ref2 ON ref1.SUPERIOR_PROP_TYPE = a.ZCD_SUP_PROP_TYPE) b

on a.SUPERIOR_PROP_TYPE = b.ZCD_SUP_PROP_TYPE

LEFT JOIN (SELECT  ZCD_INF_PROP_TYPE,  ref2.DESCRIPTION as INFERIOR_PROP_TYPE
from 0uc_connobj_attr_2 a
LEFT JOIN ZCS_DS_IPROP_TEXT ref2 ON ref2.INFERIOR_PROP_TYPE = a.ZCD_INF_PROP_TYPE) c

on a.INFERIOR_PROP_TYPE = c.INFERIOR_PROP_TYPE


-- COMMAND ----------

-- DBTITLE 1,9-0EQUIPMENT_ATTR
SELECT
EQUNR	as	equipmentNumber
,DATETO	as	validToDate
,DATEFROM	as	validFromDate
,EQART	as	technicalObjjectTypeCode
,INVNR	as	inventoryNumber
,IWERK 	as	maintenancePlanningPlant
,KOKRS 	as	controllingArea
,TPLNR 	as	functionalLocationNumber
,SWERK 	as	maintenancePlant
,ADRNR 	as	addressNumber
,BUKRS 	as	companyCode
,Derived	as	companyName
,MATNR	as	materialNumber
,ANSWT	as	acquisitionDate
,ANSDT	as	acquisitionValue
,ERDAT 	as	createdDate
,AEDAT 	as	lastChangedDate
,INBDT	as	startUpDate
,PROID 	as	workBreakdownStructureElement
,EQTYP	as	equipmentCategoryCode
FROM 0EQUIPMENT_ATTR a
LEFT JOIN 0COMP_CODE_TEXT b
ON a.BUKRS = b.BUKRS and b.LANGU ='E'


-- COMMAND ----------

-- DBTITLE 1,10-0UCINSTALLAH_ATTR_2
SELECT
MANDT	as	clientId
,ANLAGE 	as	installationId
,BIS 	as	validToDate
,AB 	as	validFromDate
,TARIFTYP 	as	rateCategoryCode
,b.TTYPBEZ	as	rateCategory
,BRANCHE 	as	industry
,AKLASSE 	as	billingClassCode
,c.TEXT	as	billingClass
,ABLEINH 	as	meterReadingUnit
,UPDMOD 	as	deltaProcessRecordMode
,ZLOGIKNR 	as	logicalDeviceNumber
FROM 0UCINSTALLAH_ATTR_2 a 
LEFT JOIN 0UC_TARIFTYP_TEXT b
ON a.TARIFTYP = b.TARIFTYP
LEFT JOIN 0UC_AKLASSE_TEXT c
ON a.AKLASSE = c.AKLASSE


-- COMMAND ----------

-- DBTITLE 1,11-0UC_CONNOBJ_ATTR_2
SELECT
HAUS 	as	propertyNumber
,COUNTRY 	as	countryShortName
,CITY_CODE 	as	cityCode
,STREETCODE 	as	streetCode
,Derived	as	streetName --DERIVED
,POSTALCODE 	as	postCode
,REGION 	as	stateCode
,REGPOLIT 	as	politicalRegionCode
,Derived	as	politicalRegion --DERIVED
,wwTP 	as	connectionObjectGUID
,ZCD_PLAN_TYPE 	as	planTypeCode
,D.DESCRIPTION	as	planType
,ZCD_PROCESS_TYPE 	as	processTypeCode
,E.DESCRIPTION	as	processType
,ZCD_PLAN_NUMBER 	as	planNumber
,ZCD_LOT_TYPE 	as	lotTypeCode
,ZCD_LOT_NUMBER 	as	lotNumber
,ZCD_SECTION_NUMBER 	as	sectionNumber
,ZCD_IND_SRV_AGR 	as	serviceAgreementIndicator
,ZCD_IND_MLIM 	as	mlimIndicator
,ZCD_IND_WICA 	as	wicaIndicator
,ZCD_IND_SOPA 	as	sopaIndicator
,ZCD_AONR 	as	architecturalObjectNumber
,ZCD_AOTYPE 	as	architecturalObjectTypeCode
,F.XMAOTYPE	as	architecturalObjectType
,ZCD_BLD_FEE_DATE 	as	buildingFeeDate
,ZZPUMP_WW 	as	pumpWateWaterIndicator
,ZZFIRE_SERVICE 	as	fireServiceIndicator
,ZINTRENO 	as	architecturalObjectInternalId
,ZAOID 	as	architecturalObjectId
,ZFIXFITCHARACT 	as	fixtureAndFittingCharacteristic
,WWTP 	as	WWTP
,SCAMP 	as	SCAMP
,HAUS_STREET 	as	streetName
,HAUS_NUM1 	as	houseNumber
,HAUS_LGA_NAME 	as	LGA
,HOUS_CITY1 	as	cityName
,WATER_DELIVERY_SYSTEM 	as	waterDeliverySystem
,WATER_DISTRIBUTION_SYSTEM_WATE 	as	waterDistributionSystem
,WATER_SUPPLY_ZONE 	as	waterSupplyZone
,RECYCLE_WATER_DELIVERY_SYSTEM 	as	receycleWaterDeliverySystem
,RECYCLE_WATER_DISTRIBUTION_SYS 	as	receycleWaterDistributionSystem
,RECYCLE_WATER_SUPPLY_ZONE 	as	recycleWaterSupplyZone
,ZCD_SUP_PROP_TYPE 	as	superiorPropertyTypeCode
,G.SUPERIOR_PROP_TYPE	as	superiorPropertyType
,G.ZCD_INF_PROP_TYPE 	as	inferiorPropertyTypeCode
,H.INFERIOR_PROP_TYPE	as	inferiorPropertyType
,Z_OWNER 	as	objectReferenceIndicator
,Z_OBJNR 	as	objectNumber
,ZCD_NO_OF_FLATS 	as	flatCount
FROM 0UC_CONNOBJ_ATTR_2 a
LEFT JOIN CRM_0CAM_STREETCODE_TEXT B
LEFT JOIN ZBL_DS_REGPOLIT_TEXT C
LEFT JOIN ZDSPLANTYPET D
ON d.PLAN_TYPE = A.ZCD_PLAN_TYPE 
LEFT JOIN ZDSPROCTYPET E
ON A.ZCD_PROCESS_TYPE = E.PROCESS_TYPE
LEFT JOIN ZEDWAOTYPTXT F
ON A.ZCD_AOTYPE = F.AOTYPE
LEFT JOIN (SELECT  ZCD_SUP_PROP_TYPE,  ref1.DESCRIPTION as SUPERIOR_PROP_TYPE
    from 0uc_connobj_attr_2 a
    join ZCS_DS_SPROP_TEXT ref2 ON ref1.SUPERIOR_PROP_TYPE = a.ZCD_SUP_PROP_TYPE) G
ON A.ZCD_SUP_PROP_TYPE = G.SUPERIOR_PROP_TYPE
LEFT JOIN (SELECT  ZCD_INF_PROP_TYPE,  ref2.DESCRIPTION as INFERIOR_PROP_TYPE
    from 0uc_connobj_attr_2 a
    join ZCS_DS_IPROP_TEXT ref2 ON ref2.INFERIOR_PROP_TYPE = a.ZCD_INF_PROP_TYPE) H
ON A.ZCD_INF_PROP_TYPE  = H.INFERIOR_PROP_TYPE

-- COMMAND ----------

-- DBTITLE 1,12-0FUNCT_LOC_ATTR
SELECT
TPLNR 	as	functionalLocationNumber
,FLTYP 	as	functionalLocationCategory
,IWERK 	as	maintenancePlanningPlant
,SWERK 	as	maintenancePlant
,ADRNR 	as	addressNumber
,KOKRS 	as	controllingArea
,BUKRS 	as	companyCode
,B.TXTMD	as	companyName
,PROID 	as	workBreakdownStructureElement
,ERDAT 	as	createdDate
,AEDAT 	as	lastChangedDate
,LGWID 	as	workCenterObjectId
,PPSID 	as	ppWorkCenterObjectId
,ALKEY 	as	labelingSystem
,STRNO 	as	functionalLocationLabel
,LAM_START 	as	startPoint
,LAM_END 	as	endPoint
,LINEAR_LENGTH 	as	linearLength
,LINEAR_UNIT 	as	unitOfMeasurement
,ZZ_ZCD_AONR 	as	architecturalObjectCount
,ZZ_ADRNR 	as	addressNumber
,ZZ_OWNER 	as	objectReferenceIndicator
,ZZ_VSTELLE 	as	premiseId
,ZZ_ANLAGE 	as	installationId
,ZZ_VKONTO 	as	contractAccountNumber
,ZZADRMA 	as	alternativeAddressNumber
,ZZ_OBJNR 	as	objectNumber
,ZZ_IDNUMBER 	as	identificationNumber
,ZZ_GPART 	as	businessPartnerNumber
,ZZ_HAUS 	as	connectionObjectId
,ZZ_LOCATION 	as	locationDescription
,ZZ_BUILDING 	as	buildingNumber
,ZZ_FLOOR 	as	floorNumber
,ZZ_HOUSE_NUM2 	as	houseNumber2
,ZZ_HOUSE_NUM3 	as	houseNumber3
,ZZ_HOUSE_NUM1 	as	houseNumber1
,ZZ_STREET 	as	streetName
,ZZ_STR_SUPPL1 	as	streetLine1
,ZZ_STR_SUPPL2 	as	streetLine2
,ZZ_CITY1 	as	cityName
,ZZ_REGION 	as	stateCode
,ZZ_POST_CODE1 	as	postCode
,ZZZ_LOCATION 	as	locationDescriptionSecondary
,ZZZ_BUILDING 	as	buildingNumberSecondary
,ZZZ_FLOOR 	as	floorNumberSecondary
,ZZZ_HOUSE_NUM2 	as	houseNumber2Secondary
,ZZZ_HOUSE_NUM3 	as	houseNumber3Secondary
,ZZZ_HOUSE_NUM1 	as	houseNumber1Secondary
,ZZZ_STREET 	as	streetNameSecondary
,ZZZ_STR_SUPPL1 	as	streetLine1Secondary
,ZZZ_STR_SUPPL2 	as	streetLine2Secondary
,ZZZ_CITY1 	as	cityNameSecondary
,ZZZ_REGION 	as	stateCodeSecondary
,ZZZ_POST_CODE1 	as	postCodeSecondary
,ZCD_BLD_FEE_DATE 	as	buildingFeeDate
FROM 0FUNCT_LOC_ATTR A
LEFT JOIN 0COMP_CODE_TEXT B
ON A.BUKRS = B.BUKRS

-- COMMAND ----------

-- DBTITLE 1,13-0UC_DEVCAT_ATTR
SELECT
MATNR	as	materialNumber
,KOMBINAT	as	deviceCategoryCombination
,FUNKLAS	as	functionClassCode
,fk.FUNKTXT	as	functionClass
,BAUKLAS	as	constructionClassCode
,bk.BAUKLTXT	as	constructionClass
,BAUFORM	as	deviceCategoryDescription
,BAUTXT	as	deviceCategoryName
,PREISKLA	as	Price_Class_NM
,PTBNUM	as	ptiNumber
,DVGWNUM	as	ggwaNumber
,BGLKZ	as	certificationRequirementType
,VLZEITT	as	calibrationValidityYears
,VLZEITTI	as	internalCertificationYears
,VLZEITN	as	VLZEITN
,VLKZBAU	as	VLKZBAU
,ZWGRUPPE	as	registerGroupCode
,EAGRUPPE	as	Input_Output_Group_CD
,MESSART	as	Measurement_Type_CD
,b.EZWG_INFO	as	registerGroup
,UEBERVER	as	transformationRatio
,BGLNETZ	as	Install_Device_Certify_IND
,WGRUPPE	as	Winding_Group_CD
,PRIMWNR1	as	Active_Primary_Winding_NUM
,SEKWNR1	as	Active_Secondary_Winding_NUM
,PRIMWNR2	as	PRIMWNR2
,SEKWNR2	as	SEKWNR2
,AENAM	as	changedBy
,AEDAT	as	lastChangedDate
,ZSPANNS	as	ZSPANNS
,ZSTROMS	as	ZSTROMS
,ZSPANNP	as	ZSPANNP
,ZSTROMP	as	ZSTROMP
,GRPMATNR	as	GRPMATNR
,ORDER_CODE	as	ORDER_CODE
,SPARTE	as	division
,NENNBEL	as	nominalLoad
,KENNZTYP	as	Vehicle_Category_CD
,KENNZTYP_TXT	as	KENNZTYP_TXT
,STELLPLATZ	as	containerSpaceCount
,HOEHEBEH	as	containerCategoryHeight
,BREITEBEH	as	containerCategoryWidth
,TIEFEBEH	as	containerCategoryDepth
,ABMEIH	as	ABMEIH
,LADEZEIT	as	LADEZEIT
,LADZ_EIH	as	LADZ_EIH
,LADEVOL	as	LADEVOL
,LADV_EIH	as	LADV_EIH
,GEW_ZUL	as	GEW_ZUL
,GEWEIH	as	GEWEIH
,EIGENTUM	as	EIGENTUM
,EIGENTUM_TXT	as	EIGENTUM_TXT
,LADV_TXT	as	LADV_TXT
,LADZ_TXT	as	LADZ_TXT
,GEW_TXT	as	GEW_TXT
,ABM_TXT	as	ABM_TXT
,PRODUCT_AREA	as	PRODUCT_AREA
,NOTIF_CODE	as	NOTIF_CODE
,G_INFOSATZ	as	G_INFOSATZ
,PPM_METER	as	PPM_METER
from CUSTOMER_BILLING.S4H_0UC_DEVCAT_ATTR d
left join CUSTOMER_BILLING.S4H_0UC_FUNKLAS_TEXT fk 
on fk.FUNKLAS = d.FUNKLAS
left join CUSTOMER_BILLING.S4H_0UC_BAUKLAS_TEXT bk 
on bk.BAUKLAS = d.BAUKLAS and SPRAS ='E'
left join 0UC_REGGRP_TEXT b
on d.ZWGRUPPE = b.ZWGRUPPE



-- COMMAND ----------

-- DBTITLE 1,14-0BPARTNER_ATTR
SELECT
PARTNER  AS businessPartnerNumber,
TYPE  AS businessPartnerCategoryCode,
Derived AS businessPartnerCategory,
BPKIND  AS businessPartnerTypeCode,
Derived AS businessPartnerType,
BU_GROUP  AS businessPartnerGroupCode,
Derived AS businessPartnerGroup,
BPEXT  AS externalBusinessPartnerNumber,
BU_SORT1  AS searchTerm1,
BU_SORT2  AS searchTerm2,
SOURCE  AS SOURCE ,
TITLE  AS titleCode,
Derived AS title,
XDELE  AS deletedIndicator,
XBLCK  AS centralBlockBusinessPartner,
AUGRP  AS Authorization Group ,
TITLE_LET  AS TITLE_LET ,
BU_LOGSYS  AS logicalSystem,
CONTACT  AS CONTACT ,
NOT_RELEASED  AS notReleasedIndicator,
NOT_LG_COMPETENT  AS notLEgallyCompetentIndicator,
PRINT_MODE  AS PRINT_MODE ,
BP_EEW_DUMMY  AS dummyFunction,
ZZUSER  AS userId,
ZZPAS_INDICATOR  AS paymentAssistSchemeIndicator,
ZZBA_INDICATOR  AS billAssistIndicator,
ZZAFLD00001Z  AS createdOn,
ZZ_CONSENT1  AS consent1Indicator,
ZZWAR_WID  AS warWidow,
ZZDISABILITY  AS disabilityIndicator,
ZZGCH  AS goldCardHolderIndicator,
ZZDECEASED  AS deceasedIndicator,
ZZPCC  AS pccIndicator,
ZZELIGIBILITY  AS eligibilityIndicator,
ZZDT_CHK  AS checkDate,
ZZPAY_ST_DT  AS paymentStartDate,
ZZPEN_TY  AS pensionType,
ZZ_CONSENT2  AS consent2Indicator,
RATE  AS costRate,
NAME_ORG1  AS organizationName1,
NAME_ORG2  AS organizationName2,
NAME_ORG3  AS organizationName3,
NAME_ORG4  AS NAME_ORG4 ,
LEGAL_ENTY  AS LEGAL_ENTY ,
IND_SECTOR  AS IND_SECTOR ,
LEGAL_ORG  AS LEGAL_ORG ,
FOUND_DAT  AS organizationFoundedDate,
LIQUID_DAT  AS LIQUID_DAT ,
LOCATION_1  AS internationalLocationNumber1,
LOCATION_2  AS internationalLocationNumber2,
LOCATION_3  AS internationalLocationNumber3,
NAME_LAST  AS lastName,
NAME_FIRST  AS firstName,
NAME_LST2  AS NAME_LST2 ,
NAME_LAST2  AS NAME_LAST2 ,
NAME_LAST2  AS atBirthName,
NAMEMIDDLE  AS middleName,
TITLE_ACA1  AS academicTitle,
TITLE_ACA2  AS TITLE_ACA2 ,
TITLE_ROYL  AS TITLE_ROYL ,
PREFIX1  AS PREFIX1 ,
PREFIX2  AS PREFIX2 ,
NAME1_TEXT  AS NAME1_TEXT ,
NICKNAME  AS nickName,
INITIALS  AS nameInitials,
NAMEFORMAT  AS NAMEFORMAT ,
NAMCOUNTRY  AS countryName,
LANGU_CORR  AS correspondanceLanguage,
XSEXM  AS XSEXM ,
XSEXF  AS XSEXF ,
BIRTHPL  AS BIRTHPL ,
MARST  AS MARST ,
EMPLO  AS EMPLO ,
JOBGR  AS JOBGR ,
NATIO  AS nationality,
CNTAX  AS CNTAX ,
CNDSC  AS CNDSC ,
PERSNUMBER  AS personNumber,
XSEXU  AS unknownGenderIndicator,
XUBNAME  AS XUBNAME ,
BU_LANGU  AS language,
BIRTHDT  AS dateOfBirth,
DEATHDT  AS dateOfDeath,
PERNO  AS personnelNumber,
CHILDREN  AS CHILDREN ,
MEM_HOUSE  AS MEM_HOUSE ,
PARTGRPTYP  AS PARTGRPTYP ,
NAME_GRP1  AS nameGroup1,
NAME_GRP2  AS nameGroup2,
MC_NAME1  AS MC_NAME1 ,
MC_NAME2  AS MC_NAME2 ,
CRUSR  AS createdBy,
CRDAT  AS createdDate,
CRTIM  AS createdTime,
CHUSR  AS changedBy,
CHDAT  AS changedDate,
CHTIM  AS changedTime,
PARTNER_GUID  AS businessPartnerGUID,
ADDRCOMM  AS addressNumber,
TD_SWITCH  AS TD_SWITCH ,
IS_ORG_CENTRE  AS organizationalCenterIndicator,
DB_KEY  AS DB_KEY ,
VALID_FROM  AS validFromDate,
VALID_TO  AS validToDate,
XPCPT  AS businessPurposeCompletedIndicator,
NATPERS  AS naturalPersonIndicator,
MILVE  AS MILVE ,
NUC_SEC  AS NUC_SEC ,
PAR_REL  AS PAR_REL ,
BP_SORT  AS BP_SORT ,
KBANKS  AS KBANKS ,
KBANKL  AS KBANKL ,
DQ_ONLINE_UPDATE  AS DQ_ONLINE_UPDATE ,
DQ_OFFLINE_UPDATE  AS DQ_OFFLINE_UPDATE ,
DQ_TIMESTAMP  AS deltaQueueUpdateTimestamp 
FROM 0BPARTNER_ATTR a
LEFT JOIN (select TXTMD from 0BPARTNER_TEXT ref where ref.PARTNER = BUT000.PARTNER and ref.TYPE = but000.TYPE) B
ON A.OBPARTNER_TEXT = B.businessPartnerCategory
LEFT JOIN (select TEXT40 from 0BPTYPE_TEXT ref where ref.BPKIND = BUT000.BPKIND) C
ON A.OBPTYPE_TEXT = C.businessPartnerType
LEFT JOIN (select TEXT40 from 0BP_GROUP_TEXT ref where ref.BU_GROUP = BUT000.BU_GROUP) D
ON A.0BP_GROUP_TEXT = D.businessPartnerGroup
LEFT JOIN (select TITLE_MEDI from ZDSTITLET ref where ref.TITLE = BUT000.TITLE) E
0N A.ZDSTITLET = E.title


-- COMMAND ----------

-- DBTITLE 1,15-0UC_DEVICE_ATTR
SELECT 
EQUNR AS equipmentNumber,
MATNR AS materialNumber,
GERAET AS deviceNumber,
BESITZ AS inspectionRelevanceIndicator,
GROES AS deviceSize,
HERST AS assetManufacturerName,
SERGE AS manufacturerSerialNumber,
ZZTYPBZ AS manufacturerModelNumber,
ZZOBJNR AS objectNumber,
ZWNABR AS registerNotRelevantToBilling
FROM 0UCDEVICE_ATTR

-- COMMAND ----------


