# Databricks notebook source
#config parameters
source = 'ISU' #either CRM or ISU
table = '0FC_DUN_HEADER'

environment = 'test'
storage_account_name = "sablobdaftest01"
storage_account_access_key = dbutils.secrets.get(scope="TestScope",key="test-sablob-key")
containerName = "archive"


# COMMAND ----------

# MAGIC %run ../../includes/tableEvaluation

# COMMAND ----------

# DBTITLE 1,[Source] with mapping
# MAGIC %sql
# MAGIC select
# MAGIC ficaDocumentNumber
# MAGIC ,ficaDocumentCategory
# MAGIC ,businessArea
# MAGIC ,dateId
# MAGIC ,additionalIdentificationCharacteristic
# MAGIC ,businessPartnerGroupNumber
# MAGIC ,contractAccountNumber
# MAGIC ,dunningNoticeCounter
# MAGIC ,dateOfIssue
# MAGIC ,noticeExecutionDate
# MAGIC ,contractAccountGroup
# MAGIC ,dunningClosedItemGroup
# MAGIC ,collectionStrategyCode
# MAGIC ,collectionStepCode
# MAGIC ,collectionStepLastDunning
# MAGIC ,companyCodeGroup
# MAGIC ,standardCompanyCode
# MAGIC ,divisionCode
# MAGIC ,contractReferenceSpecification
# MAGIC ,leadingContractAccount
# MAGIC ,alternativeDunningRecipient
# MAGIC ,dunningLevel
# MAGIC ,currencyKey
# MAGIC ,dunningBalance
# MAGIC ,totalDuningReductions
# MAGIC ,chargesSchedule
# MAGIC ,dunningCharge1
# MAGIC ,documentNumber
# MAGIC ,chargeType
# MAGIC ,dunningCharge2
# MAGIC ,dunningCharge3
# MAGIC ,dunningInterest
# MAGIC ,interestPostingDocument
# MAGIC ,creditWorthiness
# MAGIC ,noticeReversedIndicator
# MAGIC ,paymentFormNumber
# MAGIC ,submittedIndicator
# MAGIC ,statisticalItemType
# MAGIC ,successRate
# MAGIC from
# MAGIC (select
# MAGIC ABWBL	as	ficaDocumentNumber
# MAGIC ,ABWTP	as	ficaDocumentCategory
# MAGIC ,GSBER	as	businessArea
# MAGIC ,LAUFD	as	dateId
# MAGIC ,LAUFI	as	additionalIdentificationCharacteristic
# MAGIC ,GPART	as	businessPartnerGroupNumber
# MAGIC ,VKONT	as	contractAccountNumber
# MAGIC ,MAZAE	as	dunningNoticeCounter
# MAGIC ,AUSDT	as	dateOfIssue
# MAGIC ,MDRKD	as	noticeExecutionDate
# MAGIC ,VKONTGRP as	contractAccountGroup
# MAGIC ,ITEMGRP as	dunningClosedItemGroup
# MAGIC ,STRAT	as	collectionStrategyCode
# MAGIC ,STEP	as	collectionStepCode
# MAGIC ,STEP_LAST	as	collectionStepLastDunning
# MAGIC ,OPBUK	as	companyCodeGroup
# MAGIC ,STDBK	as	standardCompanyCode
# MAGIC ,SPART	as	divisionCode
# MAGIC ,VTREF	as	contractReferenceSpecification
# MAGIC ,VKNT1	as	leadingContractAccount
# MAGIC ,ABWMA	as	alternativeDunningRecipient
# MAGIC ,MAHNS	as	dunningLevel
# MAGIC ,WAERS	as	currencyKey
# MAGIC ,MSALM	as	dunningBalance
# MAGIC ,RSALM	as	totalDuningReductions
# MAGIC ,CHGID	as	chargesSchedule
# MAGIC ,MGE1M	as	dunningCharge1
# MAGIC ,MG1BL	as	documentNumber
# MAGIC ,MG1TY	as	chargeType
# MAGIC ,MGE2M	as	dunningCharge2
# MAGIC ,MGE3M	as	dunningCharge3
# MAGIC ,MINTM	as	dunningInterest
# MAGIC ,MIBEL	as	interestPostingDocument
# MAGIC ,BONIT	as	creditWorthiness
# MAGIC ,XMSTO	as	noticeReversedIndicator
# MAGIC ,NRZAS	as	paymentFormNumber
# MAGIC ,XCOLL	as	submittedIndicator
# MAGIC ,STAKZ	as	statisticalItemType
# MAGIC ,SUCPC	as	successRate
# MAGIC ,row_number() over (partition by LAUFD,LAUFI,GPART,VKONT,MAZAE order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} 
# MAGIC )a  
# MAGIC  where  a.rn = 1

# COMMAND ----------

# DBTITLE 1,[Verification] Count Check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.${vars.table}
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (select
# MAGIC ficaDocumentNumber
# MAGIC ,ficaDocumentCategory
# MAGIC ,businessArea
# MAGIC ,dateId
# MAGIC ,additionalIdentificationCharacteristic
# MAGIC ,businessPartnerGroupNumber
# MAGIC ,contractAccountNumber
# MAGIC ,dunningNoticeCounter
# MAGIC ,dateOfIssue
# MAGIC ,noticeExecutionDate
# MAGIC ,contractAccountGroup
# MAGIC ,dunningClosedItemGroup
# MAGIC ,collectionStrategyCode
# MAGIC ,collectionStepCode
# MAGIC ,collectionStepLastDunning
# MAGIC ,companyCodeGroup
# MAGIC ,standardCompanyCode
# MAGIC ,divisionCode
# MAGIC ,contractReferenceSpecification
# MAGIC ,leadingContractAccount
# MAGIC ,alternativeDunningRecipient
# MAGIC ,dunningLevel
# MAGIC ,currencyKey
# MAGIC ,dunningBalance
# MAGIC ,totalDuningReductions
# MAGIC ,chargesSchedule
# MAGIC ,dunningCharge1
# MAGIC ,documentNumber
# MAGIC ,chargeType
# MAGIC ,dunningCharge2
# MAGIC ,dunningCharge3
# MAGIC ,dunningInterest
# MAGIC ,interestPostingDocument
# MAGIC ,creditWorthiness
# MAGIC ,noticeReversedIndicator
# MAGIC ,paymentFormNumber
# MAGIC ,submittedIndicator
# MAGIC ,statisticalItemType
# MAGIC ,successRate
# MAGIC from
# MAGIC (select
# MAGIC ABWBL	as	ficaDocumentNumber
# MAGIC ,ABWTP	as	ficaDocumentCategory
# MAGIC ,GSBER	as	businessArea
# MAGIC ,LAUFD	as	dateId
# MAGIC ,LAUFI	as	additionalIdentificationCharacteristic
# MAGIC ,GPART	as	businessPartnerGroupNumber
# MAGIC ,VKONT	as	contractAccountNumber
# MAGIC ,MAZAE	as	dunningNoticeCounter
# MAGIC ,AUSDT	as	dateOfIssue
# MAGIC ,MDRKD	as	noticeExecutionDate
# MAGIC ,VKONTGRP as	contractAccountGroup
# MAGIC ,ITEMGRP as	dunningClosedItemGroup
# MAGIC ,STRAT	as	collectionStrategyCode
# MAGIC ,STEP	as	collectionStepCode
# MAGIC ,STEP_LAST	as	collectionStepLastDunning
# MAGIC ,OPBUK	as	companyCodeGroup
# MAGIC ,STDBK	as	standardCompanyCode
# MAGIC ,SPART	as	divisionCode
# MAGIC ,VTREF	as	contractReferenceSpecification
# MAGIC ,VKNT1	as	leadingContractAccount
# MAGIC ,ABWMA	as	alternativeDunningRecipient
# MAGIC ,MAHNS	as	dunningLevel
# MAGIC ,WAERS	as	currencyKey
# MAGIC ,MSALM	as	dunningBalance
# MAGIC ,RSALM	as	totalDuningReductions
# MAGIC ,CHGID	as	chargesSchedule
# MAGIC ,MGE1M	as	dunningCharge1
# MAGIC ,MG1BL	as	documentNumber
# MAGIC ,MG1TY	as	chargeType
# MAGIC ,MGE2M	as	dunningCharge2
# MAGIC ,MGE3M	as	dunningCharge3
# MAGIC ,MINTM	as	dunningInterest
# MAGIC ,MIBEL	as	interestPostingDocument
# MAGIC ,BONIT	as	creditWorthiness
# MAGIC ,XMSTO	as	noticeReversedIndicator
# MAGIC ,NRZAS	as	paymentFormNumber
# MAGIC ,XCOLL	as	submittedIndicator
# MAGIC ,STAKZ	as	statisticalItemType
# MAGIC ,SUCPC	as	successRate
# MAGIC ,row_number() over (partition by LAUFD,LAUFI,GPART,VKONT,MAZAE order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} 
# MAGIC )a  
# MAGIC  where  a.rn = 1)

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT 
# MAGIC dateId
# MAGIC ,additionalIdentificationCharacteristic
# MAGIC ,businessPartnerGroupNumber
# MAGIC ,contractAccountNumber
# MAGIC ,dunningNoticeCounter
# MAGIC , COUNT (*) as count
# MAGIC FROM cleansed.${vars.table}
# MAGIC GROUP BY dateId
# MAGIC ,additionalIdentificationCharacteristic
# MAGIC ,businessPartnerGroupNumber
# MAGIC ,contractAccountNumber
# MAGIC ,dunningNoticeCounter
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY dateId,additionalIdentificationCharacteristic,businessPartnerGroupNumber,contractAccountNumber,dunningNoticeCounter  order by dateId,additionalIdentificationCharacteristic,businessPartnerGroupNumber,contractAccountNumber,dunningNoticeCounter) as rn
# MAGIC FROM  cleansed.${vars.table}
# MAGIC )a where a.rn > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed.isu_0fc_dun_header

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select
# MAGIC --ficaDocumentNumber
# MAGIC --,ficaDocumentCategory
# MAGIC --/,businessArea
# MAGIC dateId
# MAGIC ,additionalIdentificationCharacteristic
# MAGIC ,businessPartnerGroupNumber
# MAGIC ,contractAccountNumber
# MAGIC ,dunningNoticeCounter
# MAGIC ,dateOfIssue
# MAGIC ,noticeExecutionDate
# MAGIC ,contractAccountGroup
# MAGIC ,dunningClosedItemGroup
# MAGIC ,collectionStrategyCode
# MAGIC ,collectionStepCode
# MAGIC ,collectionStepLastDunning
# MAGIC ,companyCodeGroup
# MAGIC ,standardCompanyCode
# MAGIC ,divisionCode
# MAGIC ,contractReferenceSpecification
# MAGIC ,leadingContractAccount
# MAGIC ,alternativeDunningRecipient
# MAGIC ,dunningLevel
# MAGIC ,currencyKey
# MAGIC ,dunningBalance
# MAGIC ,totalDuningReductions
# MAGIC ,chargesSchedule
# MAGIC ,dunningCharge1
# MAGIC ,documentNumber
# MAGIC ,chargeType
# MAGIC ,dunningCharge2
# MAGIC ,dunningCharge3
# MAGIC ,dunningInterest
# MAGIC ,interestPostingDocument
# MAGIC ,creditWorthiness
# MAGIC ,noticeReversedIndicator
# MAGIC ,paymentFormNumber
# MAGIC ,submittedIndicator
# MAGIC ,statisticalItemType
# MAGIC ,successRate
# MAGIC from
# MAGIC (select
# MAGIC --ABWBL	as	ficaDocumentNumber
# MAGIC --,ABWTP	as	ficaDocumentCategory
# MAGIC --,GSBER	as	businessArea
# MAGIC LAUFD	as	dateId
# MAGIC ,LAUFI	as	additionalIdentificationCharacteristic
# MAGIC ,GPART	as	businessPartnerGroupNumber
# MAGIC ,VKONT	as	contractAccountNumber
# MAGIC ,MAZAE	as	dunningNoticeCounter
# MAGIC ,AUSDT	as	dateOfIssue
# MAGIC ,MDRKD	as	noticeExecutionDate
# MAGIC ,VKONTGRP as	contractAccountGroup
# MAGIC ,ITEMGRP as	dunningClosedItemGroup
# MAGIC ,STRAT	as	collectionStrategyCode
# MAGIC ,STEP	as	collectionStepCode
# MAGIC ,STEP_LAST	as	collectionStepLastDunning
# MAGIC ,OPBUK	as	companyCodeGroup
# MAGIC ,STDBK	as	standardCompanyCode
# MAGIC ,SPART	as	divisionCode
# MAGIC ,VTREF	as	contractReferenceSpecification
# MAGIC ,VKNT1	as	leadingContractAccount
# MAGIC ,ABWMA	as	alternativeDunningRecipient
# MAGIC ,MAHNS	as	dunningLevel
# MAGIC ,WAERS	as	currencyKey
# MAGIC ,MSALM	as	dunningBalance
# MAGIC ,RSALM	as	totalDuningReductions
# MAGIC ,CHGID	as	chargesSchedule
# MAGIC ,MGE1M	as	dunningCharge1
# MAGIC ,MG1BL	as	documentNumber
# MAGIC ,MG1TY	as	chargeType
# MAGIC ,MGE2M	as	dunningCharge2
# MAGIC ,MGE3M	as	dunningCharge3
# MAGIC ,MINTM	as	dunningInterest
# MAGIC ,MIBEL	as	interestPostingDocument
# MAGIC ,BONIT	as	creditWorthiness
# MAGIC ,XMSTO	as	noticeReversedIndicator
# MAGIC ,NRZAS	as	paymentFormNumber
# MAGIC ,XCOLL	as	submittedIndicator
# MAGIC ,STAKZ	as	statisticalItemType
# MAGIC ,SUCPC	as	successRate
# MAGIC ,row_number() over (partition by LAUFD,LAUFI,GPART,VKONT,MAZAE order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} 
# MAGIC )a  
# MAGIC  where  a.rn = 1
# MAGIC except
# MAGIC select
# MAGIC --ficaDocumentNumber
# MAGIC --,ficaDocumentCategory
# MAGIC --,businessArea
# MAGIC dateId
# MAGIC ,additionalIdentificationCharacteristic
# MAGIC ,businessPartnerGroupNumber
# MAGIC ,contractAccountNumber
# MAGIC ,dunningNoticeCounter
# MAGIC ,dateOfIssue
# MAGIC ,noticeExecutionDate
# MAGIC ,contractAccountGroup
# MAGIC ,dunningClosedItemGroup
# MAGIC ,collectionStrategyCode
# MAGIC ,collectionStepCode
# MAGIC ,collectionStepLastDunning
# MAGIC ,companyCodeGroup
# MAGIC ,standardCompanyCode
# MAGIC ,divisionCode
# MAGIC ,contractReferenceSpecification
# MAGIC ,leadingContractAccount
# MAGIC ,alternativeDunningRecipient
# MAGIC ,dunningLevel
# MAGIC ,currencyKey
# MAGIC ,dunningBalance
# MAGIC ,totalDuningReductions
# MAGIC ,chargesSchedule
# MAGIC ,dunningCharge1
# MAGIC ,documentNumber
# MAGIC ,chargeType
# MAGIC ,dunningCharge2
# MAGIC ,dunningCharge3
# MAGIC ,dunningInterest
# MAGIC ,interestPostingDocument
# MAGIC ,creditWorthiness
# MAGIC ,noticeReversedIndicator
# MAGIC ,paymentFormNumber
# MAGIC ,submittedIndicator
# MAGIC ,statisticalItemType
# MAGIC ,successRate
# MAGIC from
# MAGIC cleansed.${vars.table}

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC ficaDocumentNumber
# MAGIC ,ficaDocumentCategory
# MAGIC ,businessArea
# MAGIC ,dateId
# MAGIC ,additionalIdentificationCharacteristic
# MAGIC ,businessPartnerGroupNumber
# MAGIC ,contractAccountNumber
# MAGIC ,dunningNoticeCounter
# MAGIC ,dateOfIssue
# MAGIC ,noticeExecutionDate
# MAGIC ,contractAccountGroup
# MAGIC ,dunningClosedItemGroup
# MAGIC ,collectionStrategyCode
# MAGIC ,collectionStepCode
# MAGIC ,collectionStepLastDunning
# MAGIC ,companyCodeGroup
# MAGIC ,standardCompanyCode
# MAGIC ,divisionCode
# MAGIC ,contractReferenceSpecification
# MAGIC ,leadingContractAccount
# MAGIC ,alternativeDunningRecipient
# MAGIC ,dunningLevel
# MAGIC ,currencyKey
# MAGIC ,dunningBalance
# MAGIC ,totalDuningReductions
# MAGIC ,chargesSchedule
# MAGIC ,dunningCharge1
# MAGIC ,documentNumber
# MAGIC ,chargeType
# MAGIC ,dunningCharge2
# MAGIC ,dunningCharge3
# MAGIC ,dunningInterest
# MAGIC ,interestPostingDocument
# MAGIC ,creditWorthiness
# MAGIC ,noticeReversedIndicator
# MAGIC ,paymentFormNumber
# MAGIC ,submittedIndicator
# MAGIC ,statisticalItemType
# MAGIC ,successRate
# MAGIC from
# MAGIC cleansed.${vars.table}
# MAGIC except
# MAGIC select
# MAGIC ficaDocumentNumber
# MAGIC ,ficaDocumentCategory
# MAGIC ,businessArea
# MAGIC ,dateId
# MAGIC ,additionalIdentificationCharacteristic
# MAGIC ,businessPartnerGroupNumber
# MAGIC ,contractAccountNumber
# MAGIC ,dunningNoticeCounter
# MAGIC ,dateOfIssue
# MAGIC ,noticeExecutionDate
# MAGIC ,contractAccountGroup
# MAGIC ,dunningClosedItemGroup
# MAGIC ,collectionStrategyCode
# MAGIC ,collectionStepCode
# MAGIC ,collectionStepLastDunning
# MAGIC ,companyCodeGroup
# MAGIC ,standardCompanyCode
# MAGIC ,divisionCode
# MAGIC ,contractReferenceSpecification
# MAGIC ,leadingContractAccount
# MAGIC ,alternativeDunningRecipient
# MAGIC ,dunningLevel
# MAGIC ,currencyKey
# MAGIC ,dunningBalance
# MAGIC ,totalDuningReductions
# MAGIC ,chargesSchedule
# MAGIC ,dunningCharge1
# MAGIC ,documentNumber
# MAGIC ,chargeType
# MAGIC ,dunningCharge2
# MAGIC ,dunningCharge3
# MAGIC ,dunningInterest
# MAGIC ,interestPostingDocument
# MAGIC ,creditWorthiness
# MAGIC ,noticeReversedIndicator
# MAGIC ,paymentFormNumber
# MAGIC ,submittedIndicator
# MAGIC ,statisticalItemType
# MAGIC ,successRate
# MAGIC from
# MAGIC (select
# MAGIC ABWBL	as	ficaDocumentNumber
# MAGIC ,ABWTP	as	ficaDocumentCategory
# MAGIC ,GSBER	as	businessArea
# MAGIC ,LAUFD	as	dateId
# MAGIC ,LAUFI	as	additionalIdentificationCharacteristic
# MAGIC ,GPART	as	businessPartnerGroupNumber
# MAGIC ,VKONT	as	contractAccountNumber
# MAGIC ,MAZAE	as	dunningNoticeCounter
# MAGIC ,AUSDT	as	dateOfIssue
# MAGIC ,MDRKD	as	noticeExecutionDate
# MAGIC ,VKONTGRP as	contractAccountGroup
# MAGIC ,ITEMGRP as	dunningClosedItemGroup
# MAGIC ,STRAT	as	collectionStrategyCode
# MAGIC ,STEP	as	collectionStepCode
# MAGIC ,STEP_LAST	as	collectionStepLastDunning
# MAGIC ,OPBUK	as	companyCodeGroup
# MAGIC ,STDBK	as	standardCompanyCode
# MAGIC ,SPART	as	divisionCode
# MAGIC ,VTREF	as	contractReferenceSpecification
# MAGIC ,VKNT1	as	leadingContractAccount
# MAGIC ,ABWMA	as	alternativeDunningRecipient
# MAGIC ,MAHNS	as	dunningLevel
# MAGIC ,WAERS	as	currencyKey
# MAGIC ,MSALM	as	dunningBalance
# MAGIC ,RSALM	as	totalDuningReductions
# MAGIC ,CHGID	as	chargesSchedule
# MAGIC ,MGE1M	as	dunningCharge1
# MAGIC ,MG1BL	as	documentNumber
# MAGIC ,MG1TY	as	chargeType
# MAGIC ,MGE2M	as	dunningCharge2
# MAGIC ,MGE3M	as	dunningCharge3
# MAGIC ,MINTM	as	dunningInterest
# MAGIC ,MIBEL	as	interestPostingDocument
# MAGIC ,BONIT	as	creditWorthiness
# MAGIC ,XMSTO	as	noticeReversedIndicator
# MAGIC ,NRZAS	as	paymentFormNumber
# MAGIC ,XCOLL	as	submittedIndicator
# MAGIC ,STAKZ	as	statisticalItemType
# MAGIC ,SUCPC	as	successRate
# MAGIC ,row_number() over (partition by LAUFD,LAUFI,GPART,VKONT,MAZAE order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} 
# MAGIC )a  
# MAGIC  where  a.rn = 1
