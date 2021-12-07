# Databricks notebook source
#config parameters
source = 'ISU' #either CRM or ISU
table = '0FC_ACCNTBP_ATTR_2'

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
# MAGIC contractAccountNumber,
# MAGIC businessPartnerGroupNumber,
# MAGIC createdDate,
# MAGIC createdBy,
# MAGIC lastChangedDate,
# MAGIC changedBy,
# MAGIC additionalDaysForCashManagement,
# MAGIC headerUUID,
# MAGIC directDebitLimit,
# MAGIC numberOfMonthsForDirectDebitLimit,
# MAGIC businessPartnerReferenceNumber,
# MAGIC companyCodeGroup,
# MAGIC standardCompanyCode,
# MAGIC alternativeDunningRecipient,
# MAGIC bankDetailsId,
# MAGIC incomingPaymentMethodCode,
# MAGIC deletedIndicator,
# MAGIC alternativeContractAccountForCollectiveBills,
# MAGIC accountRelationshipCode,
# MAGIC addressNumber,
# MAGIC addressNumberForAlternativeDunningRecipient,
# MAGIC alternativeInvoiceRecipient,
# MAGIC addressNumberForAlternativeBillRecipient,
# MAGIC toleranceGroupCode,
# MAGIC paymentCardId,
# MAGIC clearingCategory,
# MAGIC collectionManagementMasterDataGroup,
# MAGIC collectionStrategyCode
# MAGIC from
# MAGIC (
# MAGIC SELECT
# MAGIC VKONT as contractAccountNumber,
# MAGIC GPART as businessPartnerGroupNumber,
# MAGIC ERDAT as createdDate,
# MAGIC ERNAM as createdBy,
# MAGIC AEDATP as lastChangedDate,
# MAGIC AENAMP as changedBy,
# MAGIC FDZTG as additionalDaysForCashManagement,
# MAGIC GUID as headerUUID,
# MAGIC DDLAM as directDebitLimit,
# MAGIC DDLNM as numberOfMonthsForDirectDebitLimit,
# MAGIC EXVKO as businessPartnerReferenceNumber,
# MAGIC OPBUK as companyCodeGroup,
# MAGIC STDBK as standardCompanyCode,
# MAGIC ABWMA as alternativeDunningRecipient,
# MAGIC EBVTY as bankDetailsId,
# MAGIC EZAWE as incomingPaymentMethodCode,
# MAGIC LOEVM as deletedIndicator,
# MAGIC ABWVK as alternativeContractAccountForCollectiveBills,
# MAGIC VKPBZ as accountRelationshipCode,
# MAGIC ADRNB as addressNumber,
# MAGIC ADRMA as addressNumberForAlternativeDunningRecipient,
# MAGIC ABWRH as alternativeInvoiceRecipient,
# MAGIC ADRRH as addressNumberForAlternativeBillRecipient,
# MAGIC TOGRU as toleranceGroupCode,
# MAGIC CCARD_ID as paymentCardId,
# MAGIC VERTYP as clearingCategory,
# MAGIC CMGRP as collectionManagementMasterDataGroup,
# MAGIC STRAT as collectionStrategyCode
# MAGIC ,row_number() over (partition by VKONT,GPART order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} 
# MAGIC )a where  a.rn = 1

# COMMAND ----------

# DBTITLE 1,[Verification] Count Check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.${vars.table}
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (select
# MAGIC contractAccountNumber,
# MAGIC businessPartnerGroupNumber,
# MAGIC createdDate,
# MAGIC createdBy,
# MAGIC lastChangedDate,
# MAGIC changedBy,
# MAGIC additionalDaysForCashManagement,
# MAGIC headerUUID,
# MAGIC directDebitLimit,
# MAGIC numberOfMonthsForDirectDebitLimit,
# MAGIC businessPartnerReferenceNumber,
# MAGIC companyCodeGroup,
# MAGIC standardCompanyCode,
# MAGIC alternativeDunningRecipient,
# MAGIC bankDetailsId,
# MAGIC incomingPaymentMethodCode,
# MAGIC deletedIndicator,
# MAGIC alternativeContractAccountForCollectiveBills,
# MAGIC accountRelationshipCode,
# MAGIC addressNumber,
# MAGIC addressNumberForAlternativeDunningRecipient,
# MAGIC alternativeInvoiceRecipient,
# MAGIC addressNumberForAlternativeBillRecipient,
# MAGIC toleranceGroupCode,
# MAGIC paymentCardId,
# MAGIC clearingCategory,
# MAGIC collectionManagementMasterDataGroup,
# MAGIC collectionStrategyCode
# MAGIC from
# MAGIC (
# MAGIC SELECT
# MAGIC VKONT as contractAccountNumber,
# MAGIC GPART as businessPartnerGroupNumber,
# MAGIC ERDAT as createdDate,
# MAGIC ERNAM as createdBy,
# MAGIC AEDATP as lastChangedDate,
# MAGIC AENAMP as changedBy,
# MAGIC FDZTG as additionalDaysForCashManagement,
# MAGIC GUID as headerUUID,
# MAGIC DDLAM as directDebitLimit,
# MAGIC DDLNM as numberOfMonthsForDirectDebitLimit,
# MAGIC EXVKO as businessPartnerReferenceNumber,
# MAGIC OPBUK as companyCodeGroup,
# MAGIC STDBK as standardCompanyCode,
# MAGIC ABWMA as alternativeDunningRecipient,
# MAGIC EBVTY as bankDetailsId,
# MAGIC EZAWE as incomingPaymentMethodCode,
# MAGIC LOEVM as deletedIndicator,
# MAGIC ABWVK as alternativeContractAccountForCollectiveBills,
# MAGIC VKPBZ as accountRelationshipCode,
# MAGIC ADRNB as addressNumber,
# MAGIC ADRMA as addressNumberForAlternativeDunningRecipient,
# MAGIC ABWRH as alternativeInvoiceRecipient,
# MAGIC ADRRH as addressNumberForAlternativeBillRecipient,
# MAGIC TOGRU as toleranceGroupCode,
# MAGIC CCARD_ID as paymentCardId,
# MAGIC VERTYP as clearingCategory,
# MAGIC CMGRP as collectionManagementMasterDataGroup,
# MAGIC STRAT as collectionStrategyCode
# MAGIC ,row_number() over (partition by VKONT,GPART order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} 
# MAGIC )a where  a.rn = 1)

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT contractAccountNumber,businessPartnerGroupNumber,createdDate,createdBy,lastChangedDate,changedBy,additionalDaysForCashManagement,headerUUID,
# MAGIC directDebitLimit,numberOfMonthsForDirectDebitLimit,businessPartnerReferenceNumber,companyCodeGroup,standardCompanyCode,alternativeDunningRecipient,
# MAGIC bankDetailsId,incomingPaymentMethodCode,deletedIndicator,alternativeContractAccountForCollectiveBills,accountRelationshipCode,addressNumber,
# MAGIC addressNumberForAlternativeDunningRecipient,alternativeInvoiceRecipient,addressNumberForAlternativeBillRecipient,toleranceGroupCode,paymentCardId,
# MAGIC clearingCategory,collectionManagementMasterDataGroup,collectionStrategyCode, COUNT (*) as count
# MAGIC FROM cleansed.${vars.table}
# MAGIC GROUP BY contractAccountNumber,businessPartnerGroupNumber,createdDate,createdBy,lastChangedDate,changedBy,additionalDaysForCashManagement,headerUUID,
# MAGIC directDebitLimit,numberOfMonthsForDirectDebitLimit,businessPartnerReferenceNumber,companyCodeGroup,standardCompanyCode,alternativeDunningRecipient,
# MAGIC bankDetailsId,incomingPaymentMethodCode,deletedIndicator,alternativeContractAccountForCollectiveBills,accountRelationshipCode,addressNumber,
# MAGIC addressNumberForAlternativeDunningRecipient,alternativeInvoiceRecipient,addressNumberForAlternativeBillRecipient,toleranceGroupCode,paymentCardId,
# MAGIC clearingCategory,collectionManagementMasterDataGroup,collectionStrategyCode
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT  * FROM  (
# MAGIC     SELECT
# MAGIC       *,
# MAGIC       row_number() OVER(
# MAGIC         PARTITION BY contractAccountNumber,businessPartnerGroupNumber
# MAGIC         order by
# MAGIC           contractAccountNumber,businessPartnerGroupNumber
# MAGIC       ) as rn
# MAGIC     FROM  cleansed.${vars.table}
# MAGIC   ) a where  a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select
# MAGIC contractAccountNumber,
# MAGIC businessPartnerGroupNumber,
# MAGIC createdDate,
# MAGIC createdBy,
# MAGIC lastChangedDate,
# MAGIC changedBy,
# MAGIC additionalDaysForCashManagement,
# MAGIC headerUUID,
# MAGIC directDebitLimit,
# MAGIC numberOfMonthsForDirectDebitLimit,
# MAGIC businessPartnerReferenceNumber,
# MAGIC companyCodeGroup,
# MAGIC standardCompanyCode,
# MAGIC alternativeDunningRecipient,
# MAGIC bankDetailsId,
# MAGIC incomingPaymentMethodCode,
# MAGIC deletedIndicator,
# MAGIC alternativeContractAccountForCollectiveBills,
# MAGIC accountRelationshipCode,
# MAGIC addressNumber,
# MAGIC addressNumberForAlternativeDunningRecipient,
# MAGIC alternativeInvoiceRecipient,
# MAGIC addressNumberForAlternativeBillRecipient,
# MAGIC toleranceGroupCode,
# MAGIC paymentCardId,
# MAGIC clearingCategory,
# MAGIC collectionManagementMasterDataGroup,
# MAGIC collectionStrategyCode
# MAGIC from
# MAGIC (
# MAGIC SELECT
# MAGIC VKONT as contractAccountNumber,
# MAGIC GPART as businessPartnerGroupNumber,
# MAGIC ERDAT as createdDate,
# MAGIC ERNAM as createdBy,
# MAGIC AEDATP as lastChangedDate,
# MAGIC AENAMP as changedBy,
# MAGIC FDZTG as additionalDaysForCashManagement,
# MAGIC GUID as headerUUID,
# MAGIC DDLAM as directDebitLimit,
# MAGIC DDLNM as numberOfMonthsForDirectDebitLimit,
# MAGIC EXVKO as businessPartnerReferenceNumber,
# MAGIC OPBUK as companyCodeGroup,
# MAGIC STDBK as standardCompanyCode,
# MAGIC ABWMA as alternativeDunningRecipient,
# MAGIC EBVTY as bankDetailsId,
# MAGIC EZAWE as incomingPaymentMethodCode,
# MAGIC LOEVM as deletedIndicator,
# MAGIC ABWVK as alternativeContractAccountForCollectiveBills,
# MAGIC VKPBZ as accountRelationshipCode,
# MAGIC ADRNB as addressNumber,
# MAGIC ADRMA as addressNumberForAlternativeDunningRecipient,
# MAGIC ABWRH as alternativeInvoiceRecipient,
# MAGIC ADRRH as addressNumberForAlternativeBillRecipient,
# MAGIC TOGRU as toleranceGroupCode,
# MAGIC CCARD_ID as paymentCardId,
# MAGIC VERTYP as clearingCategory,
# MAGIC CMGRP as collectionManagementMasterDataGroup,
# MAGIC STRAT as collectionStrategyCode
# MAGIC ,row_number() over (partition by VKONT,GPART order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} 
# MAGIC )a where  a.rn = 1
# MAGIC except
# MAGIC select
# MAGIC contractAccountNumber,
# MAGIC businessPartnerGroupNumber,
# MAGIC createdDate,
# MAGIC createdBy,
# MAGIC lastChangedDate,
# MAGIC changedBy,
# MAGIC additionalDaysForCashManagement,
# MAGIC headerUUID,
# MAGIC directDebitLimit,
# MAGIC numberOfMonthsForDirectDebitLimit,
# MAGIC businessPartnerReferenceNumber,
# MAGIC companyCodeGroup,
# MAGIC standardCompanyCode,
# MAGIC alternativeDunningRecipient,
# MAGIC bankDetailsId,
# MAGIC incomingPaymentMethodCode,
# MAGIC deletedIndicator,
# MAGIC alternativeContractAccountForCollectiveBills,
# MAGIC accountRelationshipCode,
# MAGIC addressNumber,
# MAGIC addressNumberForAlternativeDunningRecipient,
# MAGIC alternativeInvoiceRecipient,
# MAGIC addressNumberForAlternativeBillRecipient,
# MAGIC toleranceGroupCode,
# MAGIC paymentCardId,
# MAGIC clearingCategory,
# MAGIC collectionManagementMasterDataGroup,
# MAGIC collectionStrategyCode
# MAGIC from
# MAGIC cleansed.${vars.table}

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC contractAccountNumber,
# MAGIC businessPartnerGroupNumber,
# MAGIC createdDate,
# MAGIC createdBy,
# MAGIC lastChangedDate,
# MAGIC changedBy,
# MAGIC additionalDaysForCashManagement,
# MAGIC headerUUID,
# MAGIC directDebitLimit,
# MAGIC numberOfMonthsForDirectDebitLimit,
# MAGIC businessPartnerReferenceNumber,
# MAGIC companyCodeGroup,
# MAGIC standardCompanyCode,
# MAGIC alternativeDunningRecipient,
# MAGIC bankDetailsId,
# MAGIC incomingPaymentMethodCode,
# MAGIC deletedIndicator,
# MAGIC alternativeContractAccountForCollectiveBills,
# MAGIC accountRelationshipCode,
# MAGIC addressNumber,
# MAGIC addressNumberForAlternativeDunningRecipient,
# MAGIC alternativeInvoiceRecipient,
# MAGIC addressNumberForAlternativeBillRecipient,
# MAGIC toleranceGroupCode,
# MAGIC paymentCardId,
# MAGIC clearingCategory,
# MAGIC collectionManagementMasterDataGroup,
# MAGIC collectionStrategyCode
# MAGIC from
# MAGIC cleansed.${vars.table}
# MAGIC except
# MAGIC select
# MAGIC contractAccountNumber,
# MAGIC businessPartnerGroupNumber,
# MAGIC createdDate,
# MAGIC createdBy,
# MAGIC lastChangedDate,
# MAGIC changedBy,
# MAGIC additionalDaysForCashManagement,
# MAGIC headerUUID,
# MAGIC directDebitLimit,
# MAGIC numberOfMonthsForDirectDebitLimit,
# MAGIC businessPartnerReferenceNumber,
# MAGIC companyCodeGroup,
# MAGIC standardCompanyCode,
# MAGIC alternativeDunningRecipient,
# MAGIC bankDetailsId,
# MAGIC incomingPaymentMethodCode,
# MAGIC deletedIndicator,
# MAGIC alternativeContractAccountForCollectiveBills,
# MAGIC accountRelationshipCode,
# MAGIC addressNumber,
# MAGIC addressNumberForAlternativeDunningRecipient,
# MAGIC alternativeInvoiceRecipient,
# MAGIC addressNumberForAlternativeBillRecipient,
# MAGIC toleranceGroupCode,
# MAGIC paymentCardId,
# MAGIC clearingCategory,
# MAGIC collectionManagementMasterDataGroup,
# MAGIC collectionStrategyCode
# MAGIC from
# MAGIC (
# MAGIC SELECT
# MAGIC VKONT as contractAccountNumber,
# MAGIC GPART as businessPartnerGroupNumber,
# MAGIC ERDAT as createdDate,
# MAGIC ERNAM as createdBy,
# MAGIC AEDATP as lastChangedDate,
# MAGIC AENAMP as changedBy,
# MAGIC FDZTG as additionalDaysForCashManagement,
# MAGIC GUID as headerUUID,
# MAGIC DDLAM as directDebitLimit,
# MAGIC DDLNM as numberOfMonthsForDirectDebitLimit,
# MAGIC EXVKO as businessPartnerReferenceNumber,
# MAGIC OPBUK as companyCodeGroup,
# MAGIC STDBK as standardCompanyCode,
# MAGIC ABWMA as alternativeDunningRecipient,
# MAGIC EBVTY as bankDetailsId,
# MAGIC EZAWE as incomingPaymentMethodCode,
# MAGIC LOEVM as deletedIndicator,
# MAGIC ABWVK as alternativeContractAccountForCollectiveBills,
# MAGIC VKPBZ as accountRelationshipCode,
# MAGIC ADRNB as addressNumber,
# MAGIC ADRMA as addressNumberForAlternativeDunningRecipient,
# MAGIC ABWRH as alternativeInvoiceRecipient,
# MAGIC ADRRH as addressNumberForAlternativeBillRecipient,
# MAGIC TOGRU as toleranceGroupCode,
# MAGIC CCARD_ID as paymentCardId,
# MAGIC VERTYP as clearingCategory,
# MAGIC CMGRP as collectionManagementMasterDataGroup,
# MAGIC STRAT as collectionStrategyCode
# MAGIC ,row_number() over (partition by VKONT,GPART order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table} 
# MAGIC )a where  a.rn = 1
