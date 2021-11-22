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
,ZCD_NO_OF_FLATS 	as	flatCount,
DATE_FROM as 
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
-- MAGIC %sql
-- MAGIC SELECT
-- MAGIC MATNR	as	materialNumber
-- MAGIC ,KOMBINAT	as	deviceCategoryCombination
-- MAGIC ,FUNKLAS	as	functionClassCode
-- MAGIC ,fk.functionClass	as	functionClass
-- MAGIC ,BAUKLAS	as	constructionClassCode
-- MAGIC ,bk.constructionClass	as	constructionClass
-- MAGIC ,BAUFORM	as	deviceCategoryDescription
-- MAGIC ,BAUTXT	as	deviceCategoryName
-- MAGIC ,PTBNUM	as	ptiNumber
-- MAGIC ,DVGWNUM	as	ggwaNumber
-- MAGIC ,BGLKZ	as	certificationRequirementType
-- MAGIC ,ZWGRUPPE	as	registerGroupCode
-- MAGIC ,b.registerGroup	as	registerGroup
-- MAGIC --,UEBERVER	as	transformationRatio
-- MAGIC ,CAST(UEBERVER AS DECIMAL(10,3)) AS transformationRatio
-- MAGIC ,AENAM	as	changedBy
-- MAGIC ,AEDAT	as	lastChangedDate
-- MAGIC ,SPARTE	as	division
-- MAGIC --,NENNBEL	as	nominalLoad
-- MAGIC ,CAST(NENNBEL AS DECIMAL(10,4)) AS nominalLoad
-- MAGIC ,STELLPLATZ	as	containerSpaceCount
-- MAGIC --,HOEHEBEH	as	containerCategoryHeight
-- MAGIC ,CAST(HOEHEBEH AS DECIMAL(5,2)) AS containerCategoryHeight 
-- MAGIC --,BREITEBEH	as	containerCategoryWidth
-- MAGIC ,CAST(BREITEBEH AS DECIMAL(5,2)) AS containerCategoryWidth
-- MAGIC --,TIEFEBEH	as	containerCategoryDepth
-- MAGIC ,CAST(TIEFEBEH AS DECIMAL(5,2)) AS containerCategoryDepth
-- MAGIC from Source d
-- MAGIC left join cleansed.t_sapisu_0uc_funklas_text fk 
-- MAGIC on fk.functionClassCode = d.FUNKLAS
-- MAGIC left join cleansed.t_sapisu_0uc_bauklas_text bk 
-- MAGIC on bk.constructionClassCode = d.BAUKLAS --and SPRAS ='E'
-- MAGIC left join cleansed.t_sapisu_0uc_reggrp_text b
-- MAGIC on d.ZWGRUPPE = b.registerGroupCode

-- COMMAND ----------

-- DBTITLE 1,14-0BPARTNER_ATTR
SELECT
PARTNER  AS businessPartnerNumber,
TYPE  AS businessPartnerCategoryCode,
b.TXTMD AS businessPartnerCategory,
BPKIND  AS businessPartnerTypeCode,
c.TEXT40 AS businessPartnerType,
BU_GROUP  AS businessPartnerGroupCode,
d.TEXT40 AS businessPartnerGroup,
BPEXT  AS externalBusinessPartnerNumber,
BU_SORT1  AS searchTerm1,
BU_SORT2  AS searchTerm2,
SOURCE  AS SOURCE ,
TITLE  AS titleCode,
f.TITLE_MEDI AS title,
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
LEFT JOIN OBPARTNER_TEXT b
ON a.PARTNER = b.PARTNER and a.TYPE = b.TYPE
LEFT JOIN 0BPTYPE_TEXT c
ON a.BPKIND = c.BPKIND and c.SPRAS = 'E'
LEFT JOIN  0BP_GROUP_TEXT d
ON a.BU_GROUP = d.BU_GROUP and d.SPRAS = 'E'
LEFT JOIN ZDSTITLET f
ON a.TITLE = f.TITLE and f.LANGU = 'E'


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

-- DBTITLE 1,16-DBERCHZ3
SELECT
BELNR	as	billingDocumentNumber
,BELZEILE	as	billingDocumentLineItemId
,MWSKZ	as	taxSalesCode
,ERMWSKZ	as	texDeterminationCode
,NETTOBTR	as	billingLineItemNetAmount
,TWAERS	as	transactionCurrency
,PREISTUF	as	priceLevel
,PREISTYP	as	priceCategory
,PREIS	as	price
,PREISZUS	as	priceSummaryIndicator
,VONZONE	as	fromBlock
,BISZONE	as	toBlock
,ZONENNR	as	numberOfPriceBlock
,PREISBTR	as	priceAmount
,MNGBASIS	as	amountLongQuantityBase
,PREIGKL	as	priceAdjustemntClause
,URPREIS	as	priceAdjustemntClauseBasePrice
,PREIADD	as	PREIADD
,PREIFAKT	as	priceAdjustmentFactor
,OPMULT	as	additionFirst
,TXDAT_KK	as	taxDecisiveDate
,PRCTR	as	profitCenter
,KOSTL	as	costCenter
,PS_PSP_PNR	as	wbsElement
,AUFNR	as	orderNumber
,PAOBJNR	as	profitabilitySegmentNumber
,PAOBJNR_S	as	profitabilitySegmentNumberForPost
,GSBER	as	businessArea
,APERIODIC	as	nonPeriodicPosting
,GROSSGROUP	as	grossGroup
,BRUTTOZEILE	as	grossBillingLineItem
,BUPLA	as	businessPlace
,LINE_CLASS	as	billingLineClassificationIndicator
,PREISART	as	priceType
,V_NETTOBTR_L	as	longNetAmountPredecimalPlaces
,N_NETTOBTR_L	as	longNetAmountDecimalPlaces
FROM DBERCHZ3

-- COMMAND ----------

-- DBTITLE 1,17-VIBDNODE

SELECT
INTRENO AS architecturalObjectInternalId,
TREE AS alternativeDisplayStructureId,
AOTYPE_AO AS architecturalObjectTypeCode,
b.XMAOTYPE AS architecturalObjectType,
AONR_AO AS architecturalObjectNumber,
PARENT AS parentArchitecturalObjectInternalId,
AOTYPE_PA AS parentArchitecturalObjectTypeCode,
c.XMAOTYPE AS parentArchitecturalObjectType,
AONR_PA AS parentArchitecturalObjectNumber
FROM VIBDNODE a
LEFT JOIN ZEDWAOTYPTXT b
ON a.AOTYPE_AO= b.AOTYPE and b.SPRAS = 'E'
LEFT JOIN ZEDWAOTYPTXT c
ON c.AOTYPE_PA = b.AOTYPE and c.SPRAS = 'E'


---------ORRRRRR--------

SELECT
architecturalObjectInternalId,
alternativeDisplayStructureId,
architecturalObjectTypeCode,
architecturalObjectType,
architecturalObjectNumber,
parentArchitecturalObjectInternalId,
parentArchitecturalObjectTypeCode,
set1.XMAOTYPE AS parentArchitecturalObjectType,
parentArchitecturalObjectNumber
(
SELECT
INTRENO AS architecturalObjectInternalId,
TREE AS alternativeDisplayStructureId,
AOTYPE_AO AS architecturalObjectTypeCode,
b.XMAOTYPE AS architecturalObjectType,
AONR_AO AS architecturalObjectNumber,
PARENT AS parentArchitecturalObjectInternalId,
AOTYPE_PA AS parentArchitecturalObjectTypeCode,
--b.XMAOTYPE AS parentArchitecturalObjectType,
AONR_PA AS parentArchitecturalObjectNumber
FROM VIBDNODE a
LEFT JOIN ZEDWAOTYPTXT b
ON a.AOTYPE_AO= b.AOTYPE and b.SPRAS = 'E'
)set1
LEFT JOIN ZEDWAOTYPTXT c
ON set1.parentArchitecturalObjectTypeCode = c.AOTYPE and c.SPRAS = 'E'

-- COMMAND ----------

-- DBTITLE 1,18-ERCHO
SELECT
BELNR AS billingDocumentNumber,
OUTCNSO AS outsortingNumber,
VALIDATION AS billingValidationName,
MANOUTSORT AS manualOutsortingReasonCode,
FREI_AM AS documentReleasedDate,
FREI_VON AS documentReleasedUserName,
DEVIATION AS deviation,
SIMULATION AS billingSimulationIndicator,
OUTCOUNT AS manualOutsortingCount
FROM ERCHO

--------------------------------NULL VERFIFICATION----------------------------------------
select distinct BELNR from ERCHO where BELNR = null
union
select distinct OUTCNSO from ERCHO where OUTCNSO = null


-- COMMAND ----------

-- DBTITLE 1,TMETERREADING
SELECT N_PROP AS propertyNumber ,
N_PROP_METE AS propertyMeterNumber,
N_METE_READ AS meterReadingNumber,
C_METE_READ_TOLE AS meterReadingTolerenceCode,
C_METE_READ_TYPE AS meterReadingTypeCode,
C_METE_READ_CONS AS meterReadingConsumptionTypeCode,
C_METE_READ_STAT AS meterReadingStatusTypeCode,
C_METE_CANT_READ AS meterCantReadReasonCode,
C_PDE_READ_METH AS pdeReadingMethodCode,
Q_METE_READ AS meterReadingQuantity,
D_METE_READ AS meterReadingDate,
T_METE_READ_TIME AS meterReadingTime,
Q_METE_READ_CONS AS meterReadingConsumptionQuantity,
Q_METE_READ_DAYS AS meterReadingDaysNumberberCount,
F_READ_COMM_CODE AS meterReadingCommentCodeIndicatoricator,
F_READ_COMM_FREE AS meterReadingFreeCommentIndicator,
Q_PDE_HIGH_LOW AS pdeHighLowAttemptCount,
Q_PDE_REEN_COUN AS pdeHighReEntryCount,
F_PDE_AUXI_READ AS pdeHighReEntryAuxiliaryReadIndicator,
D_METE_READ_UPDA AS meterReadingUpdateDate
FROM TMETERREADING

--------------------------NULL VERFIFICATION--------------------------
select distinct N_PROP from TMETERREADING where N_PROP = null
union
select distinct N_PROP_METE from TMETERREADING where N_PROP_METE = null
union
select distinct N_METE_READ from TMETERREADING where N_METE_READ = null
union
select distinct Q_METE_READ from TMETERREADING where Q_METE_READ = null
union
select distinct Q_METE_READ_CONS from TMETERREADING where Q_METE_READ_CONS = null
union
select distinct Q_METE_READ_DAYS from TMETERREADING where Q_METE_READ_DAYS = null
union
select distinct Q_PDE_HIGH_LOW from TMETERREADING where Q_PDE_HIGH_LOW = null
union
select distinct Q_PDE_REEN_COUN from TMETERREADING where Q_PDE_REEN_COUN = null