-- Databricks notebook source
-- View: VW_PROPERTY
-- Description: this view provide snapshot of integrated model of property at run time.
-- strucure similar to IM_PROPERTY, but without property services
-- History: 1.0 28/3/2022 LV created
-- 1.1 26/4/2022 filtered with `_RecordCurrent` = 1
-- 1.2 26/05/2022 changed view name, removed C&B alias
CREATE OR REPLACE view curated.view_property AS
SELECT isu_ehauisu.propertyNumber,
isu_0ucpremise_attr_2.premise,
isu_zcd_tpropty_hist.superiorPropertyTypeCode,
isu_zcd_tpropty_hist.superiorPropertyType,
isu_zcd_tpropty_hist.inferiorPropertyTypeCode,
isu_zcd_tpropty_hist.inferiorPropertyType,
isu_zcd_tpropty_hist.validFromDate propertyTypeValidFromDate,
isu_zcd_tpropty_hist.validToDate propertyTypeValidToDate,
isu_vibdnode.architecturalObjectInternalId,
isu_vibdnode.architecturalObjectNumber,
isu_vibdnode.architecturalObjectTypeCode,
isu_vibdnode.architecturalObjectType,
isu_vibdnode.parentArchitecturalObjectTypeCode,
isu_vibdnode.parentArchitecturalObjectType,
isu_vibdnode.parentArchitecturalObjectNumber parentPropertyNumber,
isu_ehauisu.CRMConnectionObjectGUID,
isu_0funct_loc_attr.locationDescription,
isu_0funct_loc_attr.buildingNumber,
isu_0funct_loc_attr.floorNumber,
isu_0funct_loc_attr.houseNumber3,
isu_0funct_loc_attr.houseNumber2,
isu_0funct_loc_attr.houseNumber1,
isu_0funct_loc_attr.streetName,
isu_0funct_loc_attr.streetLine1,
isu_0funct_loc_attr.streetLine2,
isu_0funct_loc_attr.cityName,
isu_0uc_connobj_attr_2.validFromDate connectionObjectValidFromDate,
isu_0uc_connobj_attr_2.streetCode,
isu_0uc_connobj_attr_2.cityCode,
isu_0funct_loc_attr.postcode,
isu_0funct_loc_attr.stateCode,
isu_0uc_connobj_attr_2.politicalRegionCode,
isu_0uc_connobj_attr_2.LGA,
isu_0uc_connobj_attr_2.planTypeCode,
isu_0uc_connobj_attr_2.planType,
isu_0uc_connobj_attr_2.processTypeCode,
isu_0uc_connobj_attr_2.processType,
isu_0uc_connobj_attr_2.planNumber,
isu_0uc_connobj_attr_2.lotTypeCode,
isu_dd07t.domainValueText lotTypeCodeDescription,
isu_0uc_connobj_attr_2.lotNumber,
isu_0uc_connobj_attr_2.sectionNumber,
isu_0uc_connobj_attr_2.serviceAgreementIndicator,
isu_0uc_connobj_attr_2.mlimIndicator,
isu_0uc_connobj_attr_2.wicaIndicator,
isu_0uc_connobj_attr_2.sopaIndicator,
isu_0uc_connobj_attr_2.buildingFeeDate,
isu_0uc_connobj_attr_2.objectNumber,
isu_vibdao.hydraCalculatedArea,
isu_vibdao.hydraAreaUnit,
isu_vibdao.propertyInfo,
isu_vibdao.stormWaterAssessmentIndicator,
isu_vibdao.hydraAreaIndicator,
isu_vibdao.overrideArea hydraOverrideArea,
isu_vibdao.overrideAreaUnit hydraOverrideAreaUnit,
isu_vibdao.comments hydraComments,
CASE WHEN isu_zcd_tpropty_hist.validFromDate <= current_date and isu_zcd_tpropty_hist.validToDate >= current_date THEN 'Y' ELSE 'N' END currentIndicator,
'Y' currentRecordIndicator
FROM cleansed.isu_vibdnode isu_vibdnode
INNER JOIN cleansed.isu_ehauisu
ON isu_vibdnode.architecturalObjectNumber = isu_ehauisu.propertyNumber
INNER JOIN cleansed.isu_0funct_loc_attr isu_0funct_loc_attr
ON isu_vibdnode.architecturalObjectNumber = isu_0funct_loc_attr.functionalLocationNumber
INNER JOIN cleansed.isu_0ucpremise_attr_2 isu_0ucpremise_attr_2
ON isu_vibdnode.architecturalObjectNumber = isu_0ucpremise_attr_2.propertyNumber
INNER JOIN cleansed.isu_zcd_tpropty_hist
ON isu_vibdnode.architecturalObjectNumber = isu_zcd_tpropty_hist.propertyNumber
INNER JOIN cleansed.isu_0uc_connobj_attr_2
ON isu_vibdnode.architecturalObjectNumber = isu_0uc_connobj_attr_2.propertyNumber
INNER JOIN cleansed.isu_vibdao
ON isu_vibdnode.architecturalObjectInternalId = isu_vibdao.architecturalObjectInternalId
left outer join cleansed.isu_dd07t
on isu_dd07t.domainName = 'ZCD_DO_ADDR_LOT_TYPE'
and isu_0uc_connobj_attr_2.lotTypeCode = isu_dd07t.domainValueSingleUpperLimit
and isu_dd07t.`_RecordCurrent` = 1
WHERE
isu_vibdnode.`_RecordCurrent` = 1 and
isu_ehauisu.`_RecordCurrent` = 1 and
isu_0funct_loc_attr.`_RecordCurrent` = 1 and
isu_0funct_loc_attr.`_RecordCurrent` = 1 and
isu_0ucpremise_attr_2.`_RecordCurrent` = 1 and
isu_zcd_tpropty_hist.`_RecordCurrent` = 1 and
isu_0uc_connobj_attr_2.`_RecordCurrent` = 1 and
isu_vibdao.`_RecordCurrent` = 1

-- COMMAND ----------

-- View: VW_PROPERT_SERV
-- Description: this view provide snapshot of integrated model of property at run time.
-- strucure similar to IM_PROPERTY_SERV, but without historical data
-- History: 1.0 1/4/2022 LV created
-- 1.1 26/5/2022 LV changed view name, removed C&B alias column
CREATE OR REPLACE VIEW curated.view_propertyservices As
select
isu_vibdnode.architecturalObjectNumber propertyNumber,
isu_vibdcharact.architecturalObjectInternalId ,
isu_vibdcharact.validToDate,
isu_vibdcharact.validFromDate,
isu_vibdcharact.fixtureAndFittingCharacteristicCode ,
isu_vibdcharact.fixtureAndFittingCharacteristic ,
isu_vibdcharact.supplementInfo ,
-- CHG_DT,
CASE WHEN isu_vibdcharact.validFromDate <= current_date and isu_vibdcharact.validToDate >= current_date THEN 'Y' ELSE 'N' END currentIndicator,
'Y' currentRecordIndicator
from cleansed.isu_vibdcharact isu_vibdcharact
inner join cleansed.isu_vibdnode isu_vibdnode
on isu_vibdcharact.architecturalObjectInternalId = isu_vibdnode.architecturalObjectInternalId

-- COMMAND ----------

-- strucure similar to IM_PROPERTY_REL, but without historical data
-- History: 1.0 4/4/2022 LV created
-- 1.1 26/5/2022 LV changed view name, removed C&B alias
CREATE OR REPLACE VIEW curated.view_propertyrelationship as
select isu_zcd_tprop_rel.property1Number as property1Number,
isu_zcd_tprop_rel.property2Number as property2Number,
isu_zcd_tprop_rel.validFromDate validFromDate,
isu_zcd_tprop_rel.validToDate validToDate,
isu_zcd_tprop_rel.relationshipTypeCode1 ,
isu_zcd_tprop_rel.relationshipType1 ,
isu_zcd_tprop_rel.relationshipTypeCode2 ,
isu_zcd_tprop_rel.relationshipType2 ,
-- CHG_DT -- not available in
CASE WHEN isu_zcd_tprop_rel.validFromDate <= current_date and isu_zcd_tprop_rel.validToDate >= current_date THEN 'Y' ELSE 'N' END currentIndicator,
'Y' currentRecordIndicator
from cleansed.isu_zcd_tprop_rel
where isu_zcd_tprop_rel.`_RecordCurrent` = 1

-- COMMAND ----------

-- View: view_installation
-- Description: this view provide DAF data similar to C&B VW_INSTALLATION
-- History: 1.0 11/5/2022 LV created
-- 1.1 27/5/2022 LV renamed view
CREATE OR REPLACE VIEW curated.view_installation as
select isu_0ucinstalla_attr_2.installationId,
isu_0ucinstallah_attr_2.validFromDate,
isu_0ucinstallah_attr_2.validToDate,
isu_0ucinstalla_attr_2.divisionCode,
isu_0ucinstalla_attr_2.division,
isu_0ucinstalla_attr_2.propertyNumber,
isu_0ucinstalla_attr_2.premise,
isu_0ucinstalla_attr_2.reference,
isu_0ucinstalla_attr_2.serviceTypeCode,
isu_0ucinstalla_attr_2.serviceType,
isu_0ucinstallah_attr_2.rateCategoryCode,
isu_0ucinstallah_attr_2.rateCategory,
isu_0ucinstallah_attr_2.industryCode,
isu_0ucinstallah_attr_2.industry,
isu_0ucinstallah_attr_2.billingClassCode,
isu_0ucinstallah_attr_2.billingClass,
isu_0ucinstallah_attr_2.meterReadingUnit,
isu_0ucmtrdunit_attr.portionNumber,
isu_0ucinstallah_attr_2.industrySystemCode,
isu_0ucinstallah_attr_2.industrySystem,
isu_0ucinstalla_attr_2.authorizationGroupCode,
isu_0ucinstalla_attr_2.meterReadingControlCode,
isu_0ucinstalla_attr_2.meterReadingControl,
isu_0uc_isu_32.disconnectionDocumentNumber,
isu_0uc_isu_32.disconnectionActivityPeriod,
isu_0uc_isu_32.disconnectionObjectNumber,
isu_0uc_isu_32.disconnectiondate,
isu_0uc_isu_32.disconnectionActivityTypeCode,
isu_0uc_isu_32.disconnectionActivityType,
isu_0uc_isu_32.disconnectionObjectTypeCode,
isu_0uc_isu_32.disconnectionReasonCode,
isu_0uc_isu_32.disconnectionReason,
isu_0uc_isu_32.processingVariantCode,
isu_0uc_isu_32.processingVariant,
isu_0uc_isu_32.disconnectionReconnectionStatusCode,
isu_0uc_isu_32.disconnectionReconnectionStatus,
isu_0uc_isu_32.disconnectionDocumentStatusCode,
isu_0uc_isu_32.disconnectionDocumentStatus,
isu_0ucinstalla_attr_2.createdBy installationCreatedBy,
isu_0ucinstalla_attr_2.createdDate installationCreatedDate,
isu_0ucinstalla_attr_2.lastChangedBy installationLastChangedBy,
isu_0ucinstalla_attr_2.lastChangedDate installationLastChangedDate,
isu_0ucinstalla_attr_2.deletedIndicator installationDeletedIndicator,
isu_0ucinstallah_attr_2.deltaProcessRecordMode installationHistorydeletedIndicator,
case when (isu_0ucinstallah_attr_2.validFromDate <= current_date and isu_0ucinstallah_attr_2.validToDate >= current_date) then 'Y'
else 'N'
end currentIndicator,
'Y' currentRecordIndicator
from cleansed.isu_0ucinstalla_attr_2
inner join cleansed.isu_0ucinstallah_attr_2
on isu_0ucinstalla_attr_2.installationId = isu_0ucinstallah_attr_2.installationId
left outer join cleansed.isu_0uc_isu_32
on isu_0ucinstallah_attr_2.installationId = isu_0uc_isu_32.installationId
and isu_0ucinstallah_attr_2.validFromDate <= current_date and isu_0ucinstallah_attr_2.validToDate >= current_date
and isu_0uc_isu_32.referenceObjectTypeCode = 'INSTLN'
and isu_0uc_isu_32.validFromDate <= current_date and isu_0uc_isu_32.validToDate >= current_date
and isu_0uc_isu_32.`_RecordCurrent` = 1
left outer join cleansed.isu_0ucmtrdunit_attr
on isu_0ucinstallah_attr_2.meterReadingUnit = isu_0ucmtrdunit_attr.portion
and isu_0ucmtrdunit_attr.`_RecordCurrent` = 1
where
isu_0ucinstalla_attr_2.`_RecordCurrent` = 1
and isu_0ucinstallah_attr_2.`_RecordCurrent` = 1

-- COMMAND ----------

-- View: VW_CONTRACT
-- Description: this view provide DAF data similar to C&B IM_CONTRACT
-- History: 1.0 16/5/2022 LV created
-- 1.1 20/05/2022 LV added lookup text columns from text tables
-- 1.2 27/05/2022 LV changed view name, some column names
CREATE OR REPLACE VIEW curated.view_contract as
SELECT isu_0uccontract_attr_2.contractId,
isu_0uccontracth_attr_2.validFromDate contractHistoryValidFromDate,
isu_0uccontracth_attr_2.validToDate contractHistoryValidToDate,
isu_0uccontract_attr_2.companyCode,
isu_0comp_code_text.companyName,
isu_0uccontract_attr_2.divisionCode,
isu_0division_text.division,
isu_0uccontract_attr_2.installationId,
isu_0uccontract_attr_2.contractAccountNumber,
isu_0uccontract_attr_2.accountDeterminationCode,
isu_0fcactdetid_text.accountDetermination,
isu_0uccontract_attr_2.allowableBudgetBillingCycles,
isu_0uccontract_attr_2.invoiceContractsJointly,
isu_dd07t.domainValueText invoiceContractsJointlyDescription,
isu_0uccontract_attr_2.billBlockingReasonCode,
isu_0uc_abrsperr_text.billBlockingReason,
isu_0uccontract_attr_2.billReleasingReasonCode,
isu_0uccontract_attr_2.contractText,
isu_0uccontract_attr_2.legacyMoveInDate,
isu_0uccontract_attr_2.numberOfCancellations,
isu_0uccontract_attr_2.numberOfRenewals,
isu_0uccontract_attr_2.personnelNumber,
isu_0uccontract_attr_2.contractNumberLegacy,
isu_0uccontract_attr_2.deletedIndicator,
isu_0uccontract_attr_2.isContractInvoiced,
isu_0uccontract_attr_2.outsortingCheckGroupForBilling,
isu_0uccontract_attr_2.manualOutsortingCount,
isu_0uccontract_attr_2.serviceProvider,
isu_0uccontract_attr_2.contractTerminatedForBilling,
isu_0uccontract_attr_2.invoicingParty,
isu_0uccontract_attr_2.cancellationReasonCRM,
isu_0uccontract_attr_2.specialMoveOutCase,
isu_0uccontract_attr_2.moveInDate,
isu_0uccontract_attr_2.moveOutDate,
isu_0uccontract_attr_2.budgetBillingStopDate,
isu_0uccontract_attr_2.isContractTransferred,
isu_0uccontract_attr_2.premise,
isu_0uccontract_attr_2.propertyNumber,
isu_0uccontract_attr_2.validFromDate contractValidityDate,
isu_0uccontract_attr_2.agreementNumber,
isu_0uccontract_attr_2.alternativeAddressNumber,
isu_0uccontract_attr_2.identificationNumber,
isu_0uccontract_attr_2.addressNumber,
isu_0uccontract_attr_2.objectReferenceId,
isu_0uccontracth_attr_2.marketingCampaign,
isu_0uccontracth_attr_2.individualContractId,
isu_0uccontracth_attr_2.productBeginIndicator,
isu_0uccontracth_attr_2.productChangeIndicator,
isu_0uccontracth_attr_2.replicationControls,
isu_dd07t_alias.domainValueText replicationControlsDescription,
isu_0uccontract_attr_2.objectNumber,
crm_utilitiescontract.podUUID,
crm_utilitiescontract.headerType,
crm_utilitiescontract.headerTypeName,
case when crm_utilitiescontract.isCancelled = 'X' then 'Y'
else 'N'
end isCancelled,
isu_0uccontract_attr_2.createdDate,
isu_0uccontract_attr_2.createdBy,
isu_0uccontract_attr_2.lastChangedDate,
isu_0uccontract_attr_2.lastChangedBy,
case when isu_0uccontract_attr_2.deletedIndicator = 'X' then 'Y'
else 'N'
end contractDeletedIndicator,
case when isu_0uccontracth_attr_2.deletedIndicator = 'X' then 'Y'
else 'N'
end contractHistoryDeletedIndicator,
case when (isu_0uccontracth_attr_2.validFromDate <= current_date and isu_0uccontracth_attr_2.validToDate >= current_date) then 'Y'
else 'N'
end currentIndicator,
'Y' currentRecordIndicator
from cleansed.isu_0uccontract_attr_2
inner join cleansed.isu_0uccontracth_attr_2
on isu_0uccontract_attr_2.contractId = isu_0uccontracth_attr_2.contractId
left outer join cleansed.crm_utilitiescontract
on isu_0uccontracth_attr_2.contractId = crm_utilitiescontract.utilitiesContract
and isu_0uccontracth_attr_2.validToDate = crm_utilitiescontract.contractEndDateE
and crm_utilitiescontract.`_RecordCurrent`= 1
left outer join cleansed.isu_0comp_code_text
on isu_0uccontract_attr_2.companyCode = isu_0comp_code_text.companyCode
and isu_0comp_code_text.`_RecordCurrent` = 1
left outer join cleansed.isu_0division_text
on isu_0uccontract_attr_2.divisionCode = isu_0division_text.divisionCode
and isu_0division_text.`_RecordCurrent` = 1
left outer join cleansed.isu_0fcactdetid_text
on isu_0uccontract_attr_2.accountDeterminationCode = isu_0fcactdetid_text.accountDeterminationCode
and isu_0fcactdetid_text.`_RecordCurrent` = 1
left outer join cleansed.isu_dd07t
on isu_dd07t.domainName = 'E_GEMFAKT'
and isu_0uccontract_attr_2.invoiceContractsJointly = isu_dd07t.domainValueSingleUpperLimit
and isu_dd07t.`_RecordCurrent` = 1
left outer join cleansed.isu_0uc_abrsperr_text
on isu_0uccontract_attr_2.billBlockingReasonCode = isu_0uc_abrsperr_text.billBlockingReasonCode
and isu_0uc_abrsperr_text.`_RecordCurrent` = 1
left outer join cleansed.isu_dd07t isu_dd07t_alias
on isu_dd07t_alias.domainName = 'EXREPLCNTL'
and isu_0uccontracth_attr_2.replicationControls = isu_dd07t_alias.domainValueSingleUpperLimit
and isu_dd07t_alias.`_RecordCurrent` = 1
where isu_0uccontracth_attr_2.deletedIndicator is null
and isu_0uccontract_attr_2.`_RecordCurrent` = 1
and isu_0uccontracth_attr_2.`_RecordCurrent` = 1
