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
isu_dd07t.domainValueText lotType,
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
isu_0uccontract_attr_2.invoiceContractsJointly invoiceContractsJointlyCode,
isu_dd07t.domainValueText invoiceContractsJointly,
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
isu_0uccontracth_attr_2.replicationControls replicationControlsCode,
isu_dd07t_alias.domainValueText replicationControls,
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

-- COMMAND ----------

-- View: VW_CONTRACT_ACC
-- Description: this view provide DAF data similar to C&B IM_CONTRACT_ACC
-- History:     1.0 19/5/2022 LV created
--              1.1 30/05/2022 LV changed view name, column name for standard names
CREATE OR REPLACE VIEW curated.view_contractaccount as
select 
isu_0uc_accntbp_attr_2.contractAccountNumber,
isu_0uc_accntbp_attr_2.businessPartnerGroupNumber,
isu_0uc_accntbp_attr_2.accountRelationshipCode,
isu_0uc_accntbp_attr_2.accountRelationship,
isu_0cacont_acc_attr_2.applicationArea applicationAreaCode,
isu_0uc_applk_text.applicationArea, 
isu_0cacont_acc_attr_2.contractAccountCategoryCode,
isu_0cacont_acc_attr_2.contractAccountCategory,
isu_0uc_accntbp_attr_2.businessPartnerReferenceNumber,
isu_0uc_accntbp_attr_2.toleranceGroupCode,
isu_tfk043t.toleranceGroupDescription toleranceGroup,
isu_0uc_accntbp_attr_2.manualOutsortingReasonCode,
isu_te128t.manualOutsortingReasonDescription manualOutsortingReason,
isu_0uc_accntbp_attr_2.outsortingCheckGroupCode,
isu_te192t.outsortingCheckGroup,
isu_0uc_accntbp_attr_2.manualOutsortingCount,
isu_0uc_accntbp_attr_2.participationInYearlyAdvancePayment participationInYearlyAdvancePaymentCode,
isu_dd07t.domainValueText participationInYearlyAdvancePayment,
isu_0uc_accntbp_attr_2.billingProcedureActivationIndicator,
isu_0uc_bbproc_text.billingProcedure,
isu_0uc_accntbp_attr_2.paymentConditionCode,
isu_0uc_zahlkond_text.paymentCondition,
isu_0uc_accntbp_attr_2.accountDeterminationCode,
isu_0fcactdetid_text.accountDetermination,
isu_0uc_accntbp_attr_2.alternativeInvoiceRecipient,
isu_0uc_accntbp_attr_2.addressNumberForAlternativeBillRecipient,
isu_0uc_accntbp_attr_2.alternativeContractAccountForCollectiveBills,
isu_0uc_accntbp_attr_2.dispatchControlForAlternativeBillRecipient,
isu_0uc_accntbp_attr_2.applicationForm applicationFormCode,
isu_efrm.applicationFormDescription applicationForm,
isu_0uc_accntbp_attr_2.sendAdditionalBillIndicator,
isu_0uc_accntbp_attr_2.headerUUID,
isu_0uc_accntbp_attr_2.companyCodeGroup,
isu_0comp_code_text.companyName,
isu_0uc_accntbp_attr_2.standardCompanyCode,
isu_0comp_code_text_2.companyName standardCompanyName,
isu_0uc_accntbp_attr_2.incomingPaymentMethodCode,
isu_0fc_pymet_text.paymentMethod incomingPaymentMethod,
isu_0uc_accntbp_attr_2.bankDetailsId,
isu_0uc_accntbp_attr_2.paymentCardId,
isu_0uc_accntbp_attr_2.noPaymentFormIndicator,
isu_0uc_accntbp_attr_2.alternativeDunningRecipient,
isu_0uc_accntbp_attr_2.collectionStrategyCode,
isu_tfk047xt.collectionStrategyName,
isu_0uc_accntbp_attr_2.collectionManagementMasterDataGroup,
isu_0uc_accntbp_attr_2.shippingControlForAlternativeDunningRecipient,
isu_0uc_accntbp_attr_2.sendAdditionalDunningNoticeIndicator,
isu_0uc_accntbp_attr_2.dispatchControl dispatchControlCode,
isu_esendcontrolt.dispatchControlDescription dispatchControl,
isu_0uc_accntbp_attr_2.budgetBillingRequestForCashPayer budgetBillingRequestForCashPayerCode,
isu_dd07t_alias.domainValueText budgetBillingRequestForCashPayer,
isu_0uc_accntbp_attr_2.budgetBillingRequestForDebtor budgetBillingRequestForDebtorCode,
isu_dd07t_alias2.domainValueText budgetBillingRequestForDebtor,
isu_0uc_accntbp_attr_2.directDebitLimit,
isu_0uc_accntbp_attr_2.addressNumberForAlternativeDunningRecipient,
isu_0uc_accntbp_attr_2.deletedIndicator accountBpDeletedIndicator,
isu_0cacont_acc_attr_2.legacyContractAccountNumber,
isu_0cacont_acc_attr_2.deletedIndicator contractAccountDeletedIndicator,
isu_0cacont_acc_attr_2.createdBy contractAccountCreatedBy,
isu_0cacont_acc_attr_2.createdDate contractAccountCreatedDate,
isu_0cacont_acc_attr_2.lastChangedBy contractAccountLastChangedBy,
isu_0cacont_acc_attr_2.lastChangedDate contractAccountLastChangedDate,
isu_0uc_accntbp_attr_2.createdBy accountBpCreatedBy,
isu_0uc_accntbp_attr_2.createdDate accountBpCreatedDate,
isu_0uc_accntbp_attr_2.changedBy accountBpChangedBy,
isu_0uc_accntbp_attr_2.lastChangedDate accountBpLastChangedDate,
'Y' currentRecordIndicator
from cleansed.isu_0cacont_acc_attr_2
inner join cleansed.isu_0uc_accntbp_attr_2
on  isu_0cacont_acc_attr_2.contractAccountNumber = isu_0uc_accntbp_attr_2.contractAccountNumber
left outer join cleansed.isu_0uc_applk_text
on isu_0uc_applk_text.applicationAreaCode = isu_0cacont_acc_attr_2.applicationArea
and isu_0uc_applk_text.`_RecordCurrent` = 1
left outer join cleansed.isu_tfk043t
on isu_0uc_accntbp_attr_2.toleranceGroupCode = isu_tfk043t.toleranceGroupCode
and isu_tfk043t.`_RecordCurrent` = 1
left outer join cleansed.isu_te128t
on isu_0uc_accntbp_attr_2.manualOutsortingReasonCode = isu_te128t.manualOutsortingReasonCode
left outer join cleansed.isu_te192t
on isu_0uc_accntbp_attr_2.outsortingCheckGroupCode = isu_te192t.outsortingCheckGroupCode
and isu_te192t.`_RecordCurrent` = 1
left outer join cleansed.isu_dd07t
on isu_dd07t.domainName = 'JVLTE'
and isu_0uc_accntbp_attr_2.participationInYearlyAdvancePayment = isu_dd07t.domainValueSingleUpperLimit
and isu_dd07t._RecordCurrent = 1
left outer join cleansed.isu_0uc_bbproc_text
on isu_0uc_accntbp_attr_2.billingProcedureActivationIndicator = isu_0uc_bbproc_text.billingProcedureCode
and isu_0uc_bbproc_text.`_RecordCurrent` = 1
left outer join cleansed.isu_0uc_zahlkond_text
on isu_0uc_accntbp_attr_2.paymentConditionCode = isu_0uc_zahlkond_text.paymentConditionCode
and isu_0uc_zahlkond_text.`_RecordCurrent` = 1
left outer join cleansed.isu_0fcactdetid_text
on isu_0uc_accntbp_attr_2.accountDeterminationCode = isu_0fcactdetid_text.accountDeterminationCode
and isu_0fcactdetid_text.`_RecordCurrent` = 1
left outer join cleansed.isu_efrm
on isu_0uc_accntbp_attr_2.applicationForm = isu_efrm.applicationForm
and isu_efrm.`_RecordCurrent` = 1
left outer join cleansed.isu_0comp_code_text
on isu_0uc_accntbp_attr_2.companyCodeGroup = isu_0comp_code_text.companyCode
and isu_0comp_code_text.`_RecordCurrent` = 1
left outer join cleansed.isu_0comp_code_text isu_0comp_code_text_2
on isu_0uc_accntbp_attr_2.standardCompanyCode = isu_0comp_code_text_2.companyCode
and isu_0comp_code_text_2.`_RecordCurrent` = 1
left outer join cleansed.isu_0fc_pymet_text
on isu_0uc_accntbp_attr_2.incomingPaymentMethodCode = isu_0fc_pymet_text.paymentMethodCode
and isu_0fc_pymet_text.countryCode = 'AU'
and isu_0fc_pymet_text.`_RecordCurrent` = 1
left outer join cleansed.isu_tfk047xt
on isu_0uc_accntbp_attr_2.collectionStrategyCode = isu_tfk047xt.collectionStrategyCode
and isu_tfk047xt.`_RecordCurrent` = 1
left outer join cleansed.isu_esendcontrolt
on isu_0uc_accntbp_attr_2.dispatchControl = isu_esendcontrolt.dispatchControlCode
and isu_esendcontrolt.`_RecordCurrent` = 1
left outer join cleansed.isu_dd07t isu_dd07t_alias
on isu_dd07t_alias.domainName = 'ABSLANFO'
and isu_0uc_accntbp_attr_2.budgetBillingRequestForCashPayer = isu_dd07t_alias.domainValueSingleUpperLimit
and isu_dd07t_alias.`_RecordCurrent` = 1
left outer join cleansed.isu_dd07t isu_dd07t_alias2
on isu_dd07t_alias2.domainName = 'ABSLANFO'
and isu_0uc_accntbp_attr_2.budgetBillingRequestForDebtor = isu_dd07t_alias2.domainValueSingleUpperLimit
and isu_dd07t_alias2.`_RecordCurrent` = 1
where
isu_0cacont_acc_attr_2.`_RecordCurrent` = 1
and isu_0cacont_acc_attr_2.`_RecordCurrent` = 1

-- COMMAND ----------

-- View: view_billedwaterconsumption
-- Description: this view provide DAF data similar to ZBL extractor in C&B
-- History:     1.0 09/5/2022 LV created
--              1.1 26/5/2022 renamed to view_billedwaterconsumption
--                            added property details
CREATE OR REPLACE VIEW curated.view_billedwaterconsumption as
with ercho as
(
select isu_ercho.*,
     rank() over (partition by isu_ercho.billingDocumentNumber order by isu_ercho.outsortingNumber desc) rankNumber
from cleansed.isu_ercho
where isu_ercho.`_RecordCurrent` = 1
),
erchc as
(
select isu_erchc.*,
rank() over (partition by isu_erchc.billingDocumentNumber order by sequenceNumber desc) rankNumber
from cleansed.isu_erchc
where isu_erchc.`_RecordCurrent` = 1
)
select 
isu_erch.billingDocumentNumber,
isu_dberchz2.billingDocumentLineItemId, 
nvl(ercho.outsortingNumber, 0) outsortingNumber,
nvl(erchc.sequenceNumber, 0) sequenceNumber, 
isu_erch.businessPartnerGroupNumber,
isu_zcd_tpropty_hist.propertyNumber,
isu_zcd_tpropty_hist.superiorPropertyTypeCode,
isu_zcd_tpropty_hist.superiorPropertyType,
isu_zcd_tpropty_hist.inferiorPropertyTypeCode,
isu_zcd_tpropty_hist.inferiorPropertyType,
isu_zcd_tpropty_hist.validFromDate propertyTypeValidFromDate,
isu_zcd_tpropty_hist.validToDate propertyTypeValidToDate,
isu_erch.contractId,
isu_erch.portionNumber,
isu_erch.startBillingPeriod billingPeriodStartDate,
isu_erch.endBillingPeriod billingPeriodEndDate,
isu_erch.contractAccountNumber,
isu_erch.reversalDate,
erchc.invoiceReversalPostingDate,
isu_erch.divisionCode,
isu_0division_text.division,
isu_erch.lastChangedDate,
isu_erch.createdDate,
ercho.documentReleasedDate,
isu_erch.documentNotReleasedIndicator,
erchc.postingDate,  
isu_erch.meterReadingUnit,
isu_erch.billingDocumentCreateDate,
isu_erch.erchcExistIndicator,
isu_erch.billingDocumentWithoutInvoicingCode, 
erchc.documentNotReleasedIndicator invoiceNotReleasedIndicator,
isu_dberchz1.billingQuantityPlaceBeforeDecimalPoint meteredWaterConsumption,
isu_dberchz1.lineItemTypeCode,
isu_te835t.lineItemType,
isu_dberchz1.billingLineItemBudgetBillingIndicator,
isu_dberchz1.subtransactionForDocumentItem,
isu_dberchz1.industryCode,
isu_0uc_aklasse_text.billingClass,
isu_dberchz1.validFromDate, 
isu_dberchz1.validToDate,
isu_dberchz1.rateTypeCode,
isu_0uc_tariftyp_text.TTYPBEZ rateType,
isu_dberchz1.rateId,
isu_0uc_tarifnr_text.rateDescription,
isu_dberchz1.statisticalAnalysisRateType statisticalAnalysisRateTypeCode, 
isu_0uc_stattart_text.rateType statisticalAnalysisRateType,
isu_dberchz2.equipmentNumber, 
isu_dberchz2.deviceNumber,
isu_dberchz2.materialNumber,
isu_dberchz2.logicalRegisterNumber,
isu_dberchz2.registerNumber,
isu_dberchz2.suppressedMeterReadingDocumentId,
isu_dberchz2.meterReadingReasonCode,
isu_dberchz2.previousMeterReadingReasonCode,
isu_dberchz2.meterReadingTypeCode,
isu_dberchz2.previousMeterReadingTypeCode,
isu_dberchz2.maxMeterReadingDate,
isu_dberchz2.maxMeterReadingTime,
isu_dberchz2.billingMeterReadingTime,
isu_dberchz2.previousMeterReadingTime,
isu_dberchz2.meterReadingResultsSimulationIndicator,
isu_dberchz2.registerRelationshipConsecutiveNumber,
isu_dberchz2.meterReadingAllocationDate,
isu_dberchz2.registerRelationshipSortHelpCode ,
isu_dberchz2.logicalDeviceNumber,
isu_dberchz2.meterReaderNoteText,
isu_dberchz2.quantityDeterminationProcedureCode,  
isu_dberchz2.meterReadingDocumentId,   
isu_dberchz2.previousMeterReadingBeforeDecimalPlaces,
isu_dberchz2.meterReadingBeforeDecimalPoint,
isu_dberchz2.meterReadingDifferenceBeforeDecimalPlaces
from cleansed.isu_erch
inner join cleansed.isu_dberchz1 
on isu_erch.billingDocumentNumber = isu_dberchz1.billingDocumentNumber
inner join cleansed.isu_dberchz2 
on isu_dberchz1.billingDocumentNumber = isu_dberchz2.billingDocumentNumber
and isu_dberchz1.billingDocumentLineItemId = isu_dberchz2.billingDocumentLineItemId
left outer join ercho
on isu_erch.billingDocumentNumber = ercho.billingDocumentNumber
and ercho.rankNumber = 1
left outer join erchc
on isu_erch.billingDocumentNumber = erchc.billingDocumentNumber
and erchc.rankNumber = 1
inner join cleansed.isu_zcd_tpropty_hist
on isu_zcd_tpropty_hist.propertyNumber = ltrim('0', isu_erch.businessPartnerGroupNumber)
and isu_zcd_tpropty_hist.validFromDate <= current_date and isu_zcd_tpropty_hist.validToDate >= current_date
and isu_zcd_tpropty_hist.`_RecordCurrent`= 1
left outer join cleansed.isu_0division_text
on isu_erch.divisionCode = isu_0division_text.divisionCode
and isu_0division_text.`_RecordCurrent` = 1
left outer join cleansed.isu_te835t
on isu_dberchz1.lineItemTypeCode = isu_te835t.lineItemTypeCode
and isu_te835t.`_RecordCurrent` = 1
left outer join cleansed.isu_0uc_tariftyp_text
on isu_dberchz1.rateTypeCode = isu_0uc_tariftyp_text.TARIFTYP
and isu_0uc_tariftyp_text.`_RecordCurrent` = 1
left outer join cleansed.isu_0uc_tarifnr_text
on isu_dberchz1.rateId = isu_0uc_tarifnr_text.rateId
and isu_0uc_tarifnr_text.`_RecordCurrent` = 1
left outer join cleansed.isu_0uc_aklasse_text
on isu_dberchz1.billingClassCode = isu_0uc_aklasse_text.billingClassCode
and isu_0uc_aklasse_text.`_RecordCurrent` = 1
left outer join cleansed.isu_0uc_stattart_text
on isu_dberchz1.statisticalAnalysisRateType = isu_0uc_stattart_text.rateTypeCode
and isu_0uc_stattart_text.`_RecordCurrent` = 1
where 
isu_erch.billingSimulationIndicator = ' '
and isu_erch.documentNotReleasedIndicator = ' '
and isu_dberchz1.lineItemTypeCode in ('ZDQUAN', 'ZRQUAN')
and isu_dberchz2.suppressedMeterReadingDocumentId <> ' '
and isu_erch.`_RecordCurrent` = 1
and isu_dberchz1.`_RecordCurrent` = 1
and isu_dberchz2.`_RecordCurrent` = 1

-- COMMAND ----------

-- View: view_dailyapportionedconsumption
-- Description: this view provide DAF data similar to QQV extractor in C&B
-- History:     1.0 24/4/2022 LV created
--              1.1 10/05/2022 LV added erchc, updated renamed columns
--              1.2 26/05/2022 LV changed view name, added property details 
CREATE OR REPLACE VIEW curated.view_dailyapportionedconsumption as
with statBilling as
(SELECT factdailyapportionedconsumption.meterConsumptionBillingDocumentSK, 
factdailyapportionedconsumption.PropertySK,
factdailyapportionedconsumption.LocationSK,
factdailyapportionedconsumption.BusinessPartnerGroupSK,
factdailyapportionedconsumption.ContractSK,
factdailyapportionedconsumption.MeterSK,
factdailyapportionedconsumption.sourceSystemCode,
trunc(consumptionDate, 'MM') firstDayOfMonthDate,
last_day(consumptionDate) lastDayOfMonthDate,
min(factdailyapportionedconsumption.consumptionDate) deviceMonthStartDate,
max(factdailyapportionedconsumption.consumptionDate) deviceMonthEndDate,
datediff(max(factdailyapportionedconsumption.consumptionDate), min(factdailyapportionedconsumption.consumptionDate)) + 1 activeDaysPerMonthPerDevice,
sum(factdailyapportionedconsumption.dailyApportionedConsumption) consumptionPerMonthPerDevice
FROM curated.factdailyapportionedconsumption
WHERE 
factdailyapportionedconsumption.`_RecordCurrent` = 1
GROUP BY factdailyapportionedconsumption.meterconsumptionbillingdocumentSK, 
factdailyapportionedconsumption.PropertySK,
factdailyapportionedconsumption.LocationSK,
factdailyapportionedconsumption.BusinessPartnerGroupSK,
factdailyapportionedconsumption.ContractSK,
factdailyapportionedconsumption.MeterSK,
factdailyapportionedconsumption.sourceSystemCode,
trunc(factdailyapportionedconsumption.consumptionDate, 'MM'),
last_day(factdailyapportionedconsumption.consumptionDate)
)
select 
dimmeterconsumptionbillingdocument.meterConsumptionBillingDocumentSK,
dimmeterconsumptionbillingdocument.billingDocumentNumber, 
dimmeterconsumptionbillingdocument.invoiceMaxSequenceNumber,
statBilling.sourceSystemCode,
dimmeter.meterNumber,
dimmeter.meterSerialNumber,
dimmeter.materialNumber,
dimmeter.meterSize,
isu_0uc_devcat_attr.functionClassCode,
isu_0uc_devcat_attr.functionClass,
dimmeterconsumptionbillingdocument.billingPeriodStartDate,
dimmeterconsumptionbillingdocument.billingPeriodEndDate,
dimmeterconsumptionbillingdocument.billCreatedDate,
dimmeterconsumptionbillingdocument.lastChangedDate, 
dimmeterconsumptionbillingdocument.invoicePostingDate,
datediff(dimmeterconsumptionbillingdocument.billingPeriodEndDate, dimmeterconsumptionbillingdocument.billingPeriodStartDate) + 1 totalBilledDays,
isu_zcd_tpropty_hist.propertyNumber,
isu_zcd_tpropty_hist.superiorPropertyTypeCode,
isu_zcd_tpropty_hist.superiorPropertyType,
isu_zcd_tpropty_hist.inferiorPropertyTypeCode,
isu_zcd_tpropty_hist.inferiorPropertyType,
isu_zcd_tpropty_hist.validFromDate propertyTypeValidFromDate,
isu_zcd_tpropty_hist.validToDate propertyTypeValidToDate,
isu_ehauisu.politicalRegionCode,
dimlocation.LGA,
dimbusinesspartnergroup.businessPartnerGroupNumber,
statBilling.firstDayOfMonthDate,
statBilling.lastDayOfMonthDate,
statBilling.deviceMonthStartDate,
statBilling.deviceMonthEndDate,
statBilling.activeDaysPerMonthPerDevice,
statBilling.consumptionPerMonthPerDevice,
dimmeterconsumptionbillingdocument.isOutSortedFlag,
dimmeterconsumptionbillingdocument.invoiceNotReleasedIndicator,
dimmeterconsumptionbillingdocument.isreversedFlag,
dimmeterconsumptionbillingdocument.reversalDate,
dimmeterconsumptionbillingdocument.invoiceReversalPostingDate,
dimmeterconsumptionbillingdocument.erchcExistIndicator,
dimmeterconsumptionbillingdocument.billingDocumentWithoutInvoicingCode,
dimmeterconsumptionbillingdocument.portionNumber,
dimmeterconsumptionbillingdocument.billingReasonCode,
dimContract.contractId,
dimcontract.contractAccountNumber,
diminstallation.installationId,
diminstallation.divisionCode,
diminstallation.division
from statBilling  
inner join curated.dimmeterconsumptionbillingdocument
on statBilling.meterconsumptionbillingdocumentSK = dimmeterconsumptionbillingdocument.meterconsumptionbillingdocumentSK
inner join curated.dimlocation
on dimlocation.LocationSK = statBilling.LocationSK
inner join curated.dimproperty
on dimproperty.PropertySK = statBilling.PropertySK
inner join curated.dimbusinesspartnergroup
on dimbusinesspartnergroup.BusinessPartnerGroupSK = statBilling.BusinessPartnerGroupSK
inner join curated.dimcontract
on dimContract.ContractSK = statBilling.ContractSK
inner join cleansed.isu_ehauisu
on dimproperty.propertyNumber = isu_ehauisu.propertyNumber
inner join curated.dimmeter
on dimmeter.MeterSK = statBilling.MeterSK
inner join cleansed.isu_0uc_devcat_attr
on dimmeter.materialNumber = isu_0uc_devcat_attr.materialNumber
inner join curated.brginstallationpropertymetercontract
on brginstallationpropertymetercontract.MeterSK = statBilling.MeterSK
inner join curated.diminstallation
on diminstallation.InstallationSK = brginstallationpropertymetercontract.InstallationSK
inner join cleansed.isu_zcd_tpropty_hist
on isu_zcd_tpropty_hist.propertyNumber = dimproperty.propertyNumber
and isu_zcd_tpropty_hist.validFromDate <= current_date and isu_zcd_tpropty_hist.validToDate >= current_date
and isu_zcd_tpropty_hist.`_RecordCurrent`= 1
where dimmeterconsumptionbillingdocument.`_RecordCurrent` = 1
and dimproperty.`_RecordCurrent` = 1
and dimlocation.`_RecordCurrent` = 1
and dimbusinesspartnergroup.`_RecordCurrent` = 1
and isu_ehauisu.`_RecordCurrent` = 1
and dimmeter.`_RecordCurrent` = 1
and isu_0uc_devcat_attr.`_RecordCurrent` = 1
and brginstallationpropertymetercontract.`_RecordCurrent` = 1
and diminstallation.`_RecordCurrent` = 1

-- COMMAND ----------

-- View: VW_DEVICE
-- Description: this view provide DAF data similar to C&B IM_DEVICE
-- History:     1.0 06/5/2022 LV created
--              1.1 10/5/2022 Added new columns
--              1.2 27/05/2022 changed view name, column names
CREATE OR REPLACE VIEW curated.view_device as
select isu_0uc_deviceh_attr.equipmentNumber,
isu_0uc_deviceh_attr.validFromDate deviceHistoryValidFromDate,
isu_0uc_deviceh_attr.validToDate deviceHistoryvalidToDate,
isu_0uc_device_attr.deviceNumber,
isu_0uc_deviceh_attr.logicalDeviceNumber,
isu_0uc_devinst_attr.installationId,
isu_0uc_devinst_attr.validFromDate deviceInstallationValidFromDate,
isu_0uc_devinst_attr.validToDate deviceInstallationValidToDate,
isu_0uc_regist_attr.registerNumber,
isu_0uc_regist_attr.validFromDate registerValidFromDate,
isu_0uc_regist_attr.validToDate registerValidToDate,
isu_0uc_regist_attr.logicalRegisterNumber,
isu_0uc_reginst_str_attr.validFromDate registerInstallationValidFromDate,
isu_0uc_reginst_str_attr.validToDate registerInstallationValidToDate,
isu_0ucinstalla_attr_2.divisionCode,
isu_0ucinstalla_attr_2.division,
isu_0uc_deviceh_attr.deviceLocation,
isu_0uc_device_attr.materialNumber,
isu_0uc_devcat_attr.functionClassCode,
isu_0uc_devcat_attr.functionClass,
isu_0uc_devcat_attr.constructionClassCode,
isu_0uc_devcat_attr.constructionClass,
isu_0uc_devcat_attr.deviceCategoryName,
isu_0uc_devcat_attr.deviceCategoryDescription,
isu_0uc_reginst_str_attr.operationCode,
isu_te405t.operationDescription,
isu_0uc_reginst_str_attr.rateTypeCode registerInstallationRateTypeCode,
isu_0uc_stattart_text.rateType registerInstallationRateType,
isu_0uc_reginst_str_attr.registerNotRelevantToBilling,
isu_0uc_reginst_str_attr.rateFactGroupCode,
isu_0uc_devinst_attr.priceClassCode,
isu_0uc_devinst_attr.priceClass,
isu_0uc_deviceh_attr.deviceCategoryCombination,
isu_0uc_deviceh_attr.registerGroupCode,
isu_0uc_deviceh_attr.registerGroup,
isu_0uc_deviceh_attr.installationDate,
min(isu_0uc_deviceh_attr.installationDate) over (partition by isu_0uc_deviceh_attr.equipmentNumber) firstInstallationDate,
isu_0uc_deviceh_attr.deviceRemovalDate,
case
when (isu_0uc_deviceh_attr.validFromDate <= current_date and isu_0uc_deviceh_attr.validToDate >= current_date) then isu_0uc_deviceh_attr.deviceRemovalDate
else null
END lastDeviceRemovalDate,
isu_0uc_deviceh_attr.activityReasonCode,
isu_0uc_deviceh_attr.activityReason,
isu_0uc_devinst_attr.rateTypeCode deviceInstallationRateTypeCode,
isu_0uc_devinst_attr.rateType deviceInstallationRateType,
isu_0uc_device_attr.inspectionRelevanceIndicator, 
isu_0uc_device_attr.deviceSize,
isu_0uc_device_attr.assetManufacturerName,
isu_0uc_device_attr.manufacturerSerialNumber,
isu_0uc_device_attr.manufacturerModelNumber,
isu_0uc_regist_attr.divisionCategoryCode,
isu_0uc_regist_attr.divisionCategory,
isu_0uc_regist_attr.registerIdCode,
isu_0uc_regist_attr.registerId,
isu_0uc_regist_attr.registerTypeCode,
isu_0uc_regist_attr.registerType,
isu_0uc_regist_attr.registerCategoryCode,
isu_0uc_regist_attr.registerCategory,
isu_0uc_regist_attr.reactiveApparentOrActiveRegister,
isu_dd07t.domainValueText reactiveApparentOrActiveRegisterTxt,
isu_0uc_regist_attr.unitOfMeasurementMeterReading,
isu_0uc_devcat_attr.ptiNumber,
isu_0uc_devcat_attr.ggwaNumber,
isu_0uc_devcat_attr.certificationRequirementType,
isu_0uc_regist_attr.doNotReadIndicator,
isu_0uc_device_attr.objectNumber,
case 
when (isu_0uc_deviceh_attr.validFromDate <= current_date and isu_0uc_deviceh_attr.validToDate >= current_date
and (isu_0uc_devinst_attr.validToDate is null or (isu_0uc_devinst_attr.validFromDate <= current_date and isu_0uc_devinst_attr.validToDate >= current_date))
and (isu_0uc_regist_attr.validToDate is null or (isu_0uc_regist_attr.validFromDate <= current_date and isu_0uc_regist_attr.validToDate >= current_date))
and (isu_0uc_reginst_str_attr.validToDate is null or (isu_0uc_reginst_str_attr.validFromDate <= current_date and isu_0uc_reginst_str_attr.validToDate >= current_date))
)  THEN 'Y' 
else 'N' 
END currentIndicator,
'Y' currentRecordIndicator
from cleansed.isu_0uc_deviceh_attr
inner join cleansed.isu_0uc_device_attr
on isu_0uc_deviceh_attr.equipmentNumber = isu_0uc_device_attr.equipmentNumber
left outer join cleansed.isu_0uc_devinst_attr
on isu_0uc_deviceh_attr.logicalDeviceNumber = isu_0uc_devinst_attr.logicalDeviceNumber
and isu_0uc_deviceh_attr.validFromDate >= isu_0uc_devinst_attr.validFromDate
and isu_0uc_deviceh_attr.validFromDate <= isu_0uc_devinst_attr.validToDate
and isu_0uc_devinst_attr.`_RecordCurrent` = 1
and isu_0uc_devinst_attr.deletedIndicator is null
left outer join cleansed.isu_0uc_regist_attr
on isu_0uc_regist_attr.equipmentNumber = isu_0uc_deviceh_attr.equipmentNumber
and isu_0uc_deviceh_attr.validFromDate >= isu_0uc_regist_attr.validFromDate
and isu_0uc_deviceh_attr.validFromDate <= isu_0uc_regist_attr.validToDate
and isu_0uc_regist_attr.`_RecordCurrent` = 1
and isu_0uc_regist_attr.deletedIndicator is null
left outer join cleansed.isu_0uc_reginst_str_attr
on isu_0uc_reginst_str_attr.installationId = isu_0uc_devinst_attr.installationId
and isu_0uc_reginst_str_attr.logicalRegisterNumber = isu_0uc_regist_attr.logicalRegisterNumber
and isu_0uc_deviceh_attr.validFromDate >= isu_0uc_reginst_str_attr.validFromDate
and isu_0uc_deviceh_attr.validFromDate <= isu_0uc_reginst_str_attr.validToDate
and isu_0uc_reginst_str_attr._RecordCurrent = 1
and isu_0uc_reginst_str_attr.deletedIndicator is null
left outer join cleansed.isu_0ucinstalla_attr_2
on isu_0ucinstalla_attr_2.installationId = isu_0uc_devinst_attr.installationId
and isu_0ucinstalla_attr_2.`_RecordCurrent` = 1
and isu_0ucinstalla_attr_2.deletedIndicator is null
left outer join cleansed.isu_0uc_devcat_attr
on isu_0uc_device_attr.materialNumber = isu_0uc_devcat_attr.materialNumber
and isu_0uc_devcat_attr.`_RecordCurrent` = 1
left outer join cleansed.isu_te405t
on isu_0uc_reginst_str_attr.operationCode = isu_te405t.operationCode
and isu_te405t.`_RecordCurrent` = 1
left outer join cleansed.isu_0uc_stattart_text
on isu_0uc_regist_attr.registerTypeCode = isu_0uc_stattart_text.rateTypeCode
and isu_0uc_stattart_text.`_RecordCurrent` = 1
left outer join cleansed.isu_dd07t
on  isu_dd07t.domainName = 'BLIWIRK'
and isu_dd07t.domainValueSingleUpperLimit = isu_0uc_regist_attr.reactiveApparentOrActiveRegister
and isu_dd07t.`_RecordCurrent` = 1
where 
nvl(isu_0uc_reginst_str_attr.registerNotRelevantToBilling, ' ') <> 'X'
and isu_0uc_deviceh_attr.`_RecordCurrent` = 1
and isu_0uc_deviceh_attr.deletedIndicator is null
and isu_0uc_device_attr.`_RecordCurrent` = 1
