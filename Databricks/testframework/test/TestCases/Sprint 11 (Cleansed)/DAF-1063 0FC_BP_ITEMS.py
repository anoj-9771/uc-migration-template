# Databricks notebook source
#config parameters
source = 'ISU' #either CRM or ISU
table = '0FC_BP_ITEMS'

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
# MAGIC contractDocumentNumber
# MAGIC ,repetitionItem
# MAGIC ,itemNumber
# MAGIC ,partialClearingSubitem
# MAGIC ,companyCode
# MAGIC ,company
# MAGIC ,clearingStatus
# MAGIC ,businessPartnerGroupNumber
# MAGIC ,contractReferenceSpecification
# MAGIC ,contractAccountNumber
# MAGIC ,ficaDocumentNumber
# MAGIC ,ficaDocumentCategory
# MAGIC ,applicationArea
# MAGIC ,mainTransactionLineItemCode
# MAGIC ,mainTransactionLineItem
# MAGIC ,subtransactionLineItemCode
# MAGIC ,subtransactionLineItem
# MAGIC ,accountDeterminationCode
# MAGIC ,accountDetermination
# MAGIC ,divisionCode
# MAGIC ,accountGeneralLedger
# MAGIC ,taxSalesCode
# MAGIC ,downPaymentIndicator
# MAGIC ,statisticalItemType
# MAGIC ,documentDate
# MAGIC ,postingDate
# MAGIC ,currencyKey
# MAGIC ,paymentDueDate
# MAGIC ,cashDiscountDueDate
# MAGIC ,deferralToDate
# MAGIC ,cashDiscountPercentageRate
# MAGIC ,amountLocalCurrency
# MAGIC ,amountTransactionCurrency
# MAGIC ,amountEligibleCashDiscount
# MAGIC ,taxAmountLocalCurrency
# MAGIC ,taxAmountTransactionCurrency
# MAGIC ,clearingDate
# MAGIC ,clearingDocument
# MAGIC ,clearingDocumentPostingDate
# MAGIC ,clearingReason
# MAGIC ,clearingCurrency
# MAGIC ,clearingAmount
# MAGIC ,taxAmountClearingCurrency
# MAGIC ,cashDiscount
# MAGIC ,clearingValueDate
# MAGIC ,settlementPeriodLowerLimit
# MAGIC ,billingPeriodUpperLimit
# MAGIC ,clearingRestriction
# MAGIC ,valueAdjustment
# MAGIC ,documentTypeCode
# MAGIC ,documentType
# MAGIC ,referenceDocumentNumber
# MAGIC ,collectionItem
# MAGIC ,checkReason
# MAGIC ,taxPortion
# MAGIC ,taxAmountDocument
# MAGIC ,writeOffReasonCode
# MAGIC ,documentOriginCode
# MAGIC ,documentEnteredDate
# MAGIC ,referenceProcedure
# MAGIC ,objectKey
# MAGIC ,reversalDocumentNumber
# MAGIC ,reconciliationKeyForGeneralLedger
# MAGIC ,furtherPostingIndicator
# MAGIC from
# MAGIC (select
# MAGIC OPBEL as contractDocumentNumber,
# MAGIC OPUPW as repetitionItem,
# MAGIC OPUPK as itemNumber,
# MAGIC OPUPZ as partialClearingSubitem,
# MAGIC BUKRS as companyCode,
# MAGIC b.companyName as company,
# MAGIC AUGST as clearingStatus,
# MAGIC GPART as businessPartnerGroupNumber,
# MAGIC VTREF as contractReferenceSpecification,
# MAGIC VKONT as contractAccountNumber,
# MAGIC ABWBL as ficaDocumentNumber,
# MAGIC ABWTP as ficaDocumentCategory,
# MAGIC APPLK as applicationArea,
# MAGIC HVORG as mainTransactionLineItemCode,
# MAGIC c.mainTransaction as mainTransactionLineItem,
# MAGIC TVORG as subtransactionLineItemCode,
# MAGIC d.subtransaction as subtransactionLineItem,
# MAGIC KOFIZ as accountDeterminationCode,
# MAGIC e.accountDetermination as accountDetermination,
# MAGIC SPART as divisionCode,
# MAGIC HKONT as accountGeneralLedger,
# MAGIC MWSKZ as taxSalesCode,
# MAGIC XANZA as downPaymentIndicator,
# MAGIC STAKZ as statisticalItemType,
# MAGIC BLDAT as documentDate,
# MAGIC BUDAT as postingDate,
# MAGIC WAERS as currencyKey,
# MAGIC FAEDN as paymentDueDate,
# MAGIC FAEDS as cashDiscountDueDate,
# MAGIC STUDT as deferralToDate,
# MAGIC SKTPZ as cashDiscountPercentageRate,
# MAGIC BETRH as amountLocalCurrency,
# MAGIC BETRW as amountTransactionCurrency,
# MAGIC SKFBT as amountEligibleCashDiscount,
# MAGIC SBETH as taxAmountLocalCurrency,
# MAGIC SBETW as taxAmountTransactionCurrency,
# MAGIC AUGDT as clearingDate,
# MAGIC AUGBL as clearingDocument,
# MAGIC AUGBD as clearingDocumentPostingDate,
# MAGIC AUGRD as clearingReason,
# MAGIC AUGWA as clearingCurrency,
# MAGIC AUGBT as clearingAmount,
# MAGIC AUGBS as taxAmountClearingCurrency,
# MAGIC AUGSK as cashDiscount,
# MAGIC AUGVD as clearingValueDate,
# MAGIC ABRZU as settlementPeriodLowerLimit,
# MAGIC ABRZO as billingPeriodUpperLimit,
# MAGIC AUGRS as clearingRestriction,
# MAGIC INFOZ as valueAdjustment,
# MAGIC BLART as documentTypeCode,
# MAGIC f.documentType as documentType,
# MAGIC XBLNR as referenceDocumentNumber,
# MAGIC INKPS as collectionItem,
# MAGIC C4EYE as checkReason,
# MAGIC SCTAX as taxPortion,
# MAGIC STTAX as taxAmountDocument,
# MAGIC ABGRD as writeOffReasonCode,
# MAGIC HERKF as documentOriginCode,
# MAGIC CPUDT as documentEnteredDate,
# MAGIC AWTYP as referenceProcedure,
# MAGIC AWKEY as objectKey,
# MAGIC STORB as reversalDocumentNumber,
# MAGIC FIKEY as reconciliationKeyForGeneralLedger,
# MAGIC XCLOS as furtherPostingIndicator,
# MAGIC row_number() over (partition by OPBEL,OPUPW,OPUPK,OPUPZ order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC LEFT JOIN cleansed.ISU_0COMP_CODE_TEXT b
# MAGIC   ON b.companyCode = BUKRS
# MAGIC LEFT JOIN cleansed.ISU_0UC_HVORG_TEXT c
# MAGIC   ON c.applicationArea = APPLK
# MAGIC   AND c.mainTransactionLineItemCode = HVORG
# MAGIC left join cleansed.ISU_0UC_TVORG_TEXT d
# MAGIC   on d.applicationArea = APPLK
# MAGIC   and d.mainTransactionLineItemCode = HVORG
# MAGIC   and d.subtransactionLineItemCode = TVORG
# MAGIC LEFT JOIN cleansed.ISU_0FCACTDETID_TEXT e
# MAGIC   on e.accountDeterminationCode = KOFIZ
# MAGIC left join cleansed.ISU_0FC_BLART_TEXT f
# MAGIC   on f.applicationArea = APPLK
# MAGIC   and f.documentTypeCode = BLART
# MAGIC )a  where  a.rn = 1

# COMMAND ----------

# DBTITLE 1,[Verification] Count Check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.${vars.table}
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (select
# MAGIC contractDocumentNumber
# MAGIC ,repetitionItem
# MAGIC ,itemNumber
# MAGIC ,partialClearingSubitem
# MAGIC ,companyCode
# MAGIC ,company
# MAGIC ,clearingStatus
# MAGIC ,businessPartnerGroupNumber
# MAGIC ,contractReferenceSpecification
# MAGIC ,contractAccountNumber
# MAGIC ,ficaDocumentNumber
# MAGIC ,ficaDocumentCategory
# MAGIC ,applicationArea
# MAGIC ,mainTransactionLineItemCode
# MAGIC ,mainTransactionLineItem
# MAGIC ,subtransactionLineItemCode
# MAGIC ,subtransactionLineItem
# MAGIC ,accountDeterminationCode
# MAGIC ,accountDetermination
# MAGIC ,divisionCode
# MAGIC ,accountGeneralLedger
# MAGIC ,taxSalesCode
# MAGIC ,downPaymentIndicator
# MAGIC ,statisticalItemType
# MAGIC ,documentDate
# MAGIC ,postingDate
# MAGIC ,currencyKey
# MAGIC ,paymentDueDate
# MAGIC ,cashDiscountDueDate
# MAGIC ,deferralToDate
# MAGIC ,cashDiscountPercentageRate
# MAGIC ,amountLocalCurrency
# MAGIC ,amountTransactionCurrency
# MAGIC ,amountEligibleCashDiscount
# MAGIC ,taxAmountLocalCurrency
# MAGIC ,taxAmountTransactionCurrency
# MAGIC ,clearingDate
# MAGIC ,clearingDocument
# MAGIC ,clearingDocumentPostingDate
# MAGIC ,clearingReason
# MAGIC ,clearingCurrency
# MAGIC ,clearingAmount
# MAGIC ,taxAmountClearingCurrency
# MAGIC ,cashDiscount
# MAGIC ,clearingValueDate
# MAGIC ,settlementPeriodLowerLimit
# MAGIC ,billingPeriodUpperLimit
# MAGIC ,clearingRestriction
# MAGIC ,valueAdjustment
# MAGIC ,documentTypeCode
# MAGIC ,documentType
# MAGIC ,referenceDocumentNumber
# MAGIC ,collectionItem
# MAGIC ,checkReason
# MAGIC ,taxPortion
# MAGIC ,taxAmountDocument
# MAGIC ,writeOffReasonCode
# MAGIC ,documentOriginCode
# MAGIC ,documentEnteredDate
# MAGIC ,referenceProcedure
# MAGIC ,objectKey
# MAGIC ,reversalDocumentNumber
# MAGIC ,reconciliationKeyForGeneralLedger
# MAGIC ,furtherPostingIndicator
# MAGIC from
# MAGIC (select
# MAGIC OPBEL as contractDocumentNumber,
# MAGIC OPUPW as repetitionItem,
# MAGIC OPUPK as itemNumber,
# MAGIC OPUPZ as partialClearingSubitem,
# MAGIC BUKRS as companyCode,
# MAGIC b.companyName as company,
# MAGIC AUGST as clearingStatus,
# MAGIC GPART as businessPartnerGroupNumber,
# MAGIC VTREF as contractReferenceSpecification,
# MAGIC VKONT as contractAccountNumber,
# MAGIC ABWBL as ficaDocumentNumber,
# MAGIC ABWTP as ficaDocumentCategory,
# MAGIC APPLK as applicationArea,
# MAGIC HVORG as mainTransactionLineItemCode,
# MAGIC c.mainTransaction as mainTransactionLineItem,
# MAGIC TVORG as subtransactionLineItemCode,
# MAGIC d.subtransaction as subtransactionLineItem,
# MAGIC KOFIZ as accountDeterminationCode,
# MAGIC e.accountDetermination as accountDetermination,
# MAGIC SPART as divisionCode,
# MAGIC HKONT as accountGeneralLedger,
# MAGIC MWSKZ as taxSalesCode,
# MAGIC XANZA as downPaymentIndicator,
# MAGIC STAKZ as statisticalItemType,
# MAGIC BLDAT as documentDate,
# MAGIC BUDAT as postingDate,
# MAGIC WAERS as currencyKey,
# MAGIC FAEDN as paymentDueDate,
# MAGIC FAEDS as cashDiscountDueDate,
# MAGIC STUDT as deferralToDate,
# MAGIC SKTPZ as cashDiscountPercentageRate,
# MAGIC BETRH as amountLocalCurrency,
# MAGIC BETRW as amountTransactionCurrency,
# MAGIC SKFBT as amountEligibleCashDiscount,
# MAGIC SBETH as taxAmountLocalCurrency,
# MAGIC SBETW as taxAmountTransactionCurrency,
# MAGIC AUGDT as clearingDate,
# MAGIC AUGBL as clearingDocument,
# MAGIC AUGBD as clearingDocumentPostingDate,
# MAGIC AUGRD as clearingReason,
# MAGIC AUGWA as clearingCurrency,
# MAGIC AUGBT as clearingAmount,
# MAGIC AUGBS as taxAmountClearingCurrency,
# MAGIC AUGSK as cashDiscount,
# MAGIC AUGVD as clearingValueDate,
# MAGIC ABRZU as settlementPeriodLowerLimit,
# MAGIC ABRZO as billingPeriodUpperLimit,
# MAGIC AUGRS as clearingRestriction,
# MAGIC INFOZ as valueAdjustment,
# MAGIC BLART as documentTypeCode,
# MAGIC f.documentType as documentType,
# MAGIC XBLNR as referenceDocumentNumber,
# MAGIC INKPS as collectionItem,
# MAGIC C4EYE as checkReason,
# MAGIC SCTAX as taxPortion,
# MAGIC STTAX as taxAmountDocument,
# MAGIC ABGRD as writeOffReasonCode,
# MAGIC HERKF as documentOriginCode,
# MAGIC CPUDT as documentEnteredDate,
# MAGIC AWTYP as referenceProcedure,
# MAGIC AWKEY as objectKey,
# MAGIC STORB as reversalDocumentNumber,
# MAGIC FIKEY as reconciliationKeyForGeneralLedger,
# MAGIC XCLOS as furtherPostingIndicator,
# MAGIC row_number() over (partition by OPBEL,OPUPW,OPUPK,OPUPZ order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC LEFT JOIN cleansed.ISU_0COMP_CODE_TEXT b
# MAGIC   ON b.companyCode = BUKRS
# MAGIC LEFT JOIN cleansed.ISU_0UC_HVORG_TEXT c
# MAGIC   ON c.applicationArea = APPLK
# MAGIC   AND c.mainTransactionLineItemCode = HVORG
# MAGIC left join cleansed.ISU_0UC_TVORG_TEXT d
# MAGIC   on d.applicationArea = APPLK
# MAGIC   and d.mainTransactionLineItemCode = HVORG
# MAGIC   and d.subtransactionLineItemCode = TVORG
# MAGIC LEFT JOIN cleansed.ISU_0FCACTDETID_TEXT e
# MAGIC   on e.accountDeterminationCode = KOFIZ
# MAGIC left join cleansed.ISU_0FC_BLART_TEXT f
# MAGIC   on f.applicationArea = APPLK
# MAGIC   and f.documentTypeCode = BLART
# MAGIC )a  where  a.rn = 1)

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT 
# MAGIC contractDocumentNumber,repetitionItem,itemNumber,partialClearingSubitem,companyCode,company
# MAGIC ,clearingStatus,businessPartnerGroupNumber,contractReferenceSpecification,contractAccountNumber
# MAGIC ,ficaDocumentNumber,ficaDocumentCategory,applicationArea,mainTransactionLineItemCode,mainTransactionLineItem
# MAGIC ,subtransactionLineItemCode,subtransactionLineItem,accountDeterminationCode,accountDetermination,divisionCode
# MAGIC ,accountGeneralLedger,taxSalesCode,downPaymentIndicator,statisticalItemType,documentDate,postingDate,currencyKey
# MAGIC ,paymentDueDate,cashDiscountDueDate,deferralToDate,cashDiscountPercentageRate,amountLocalCurrency,amountTransactionCurrency
# MAGIC ,amountEligibleCashDiscount,taxAmountLocalCurrency,taxAmountTransactionCurrency,clearingDate,clearingDocument
# MAGIC ,clearingDocumentPostingDate,clearingReason,clearingCurrency,clearingAmount,taxAmountClearingCurrency,cashDiscount
# MAGIC ,clearingValueDate,settlementPeriodLowerLimit,billingPeriodUpperLimit,clearingRestriction,valueAdjustment
# MAGIC ,documentTypeCode,documentType,referenceDocumentNumber,collectionItem,checkReason,taxPortion,taxAmountDocument
# MAGIC ,writeOffReasonCode,documentOriginCode,documentEnteredDate,referenceProcedure,objectKey,reversalDocumentNumber
# MAGIC ,reconciliationKeyForGeneralLedger,furtherPostingIndicator, COUNT (*) as count
# MAGIC FROM cleansed.${vars.table}
# MAGIC GROUP BY contractDocumentNumber,repetitionItem,itemNumber,partialClearingSubitem,companyCode,company
# MAGIC ,clearingStatus,businessPartnerGroupNumber,contractReferenceSpecification,contractAccountNumber
# MAGIC ,ficaDocumentNumber,ficaDocumentCategory,applicationArea,mainTransactionLineItemCode,mainTransactionLineItem
# MAGIC ,subtransactionLineItemCode,subtransactionLineItem,accountDeterminationCode,accountDetermination,divisionCode
# MAGIC ,accountGeneralLedger,taxSalesCode,downPaymentIndicator,statisticalItemType,documentDate,postingDate,currencyKey
# MAGIC ,paymentDueDate,cashDiscountDueDate,deferralToDate,cashDiscountPercentageRate,amountLocalCurrency,amountTransactionCurrency
# MAGIC ,amountEligibleCashDiscount,taxAmountLocalCurrency,taxAmountTransactionCurrency,clearingDate,clearingDocument
# MAGIC ,clearingDocumentPostingDate,clearingReason,clearingCurrency,clearingAmount,taxAmountClearingCurrency,cashDiscount
# MAGIC ,clearingValueDate,settlementPeriodLowerLimit,billingPeriodUpperLimit,clearingRestriction,valueAdjustment
# MAGIC ,documentTypeCode,documentType,referenceDocumentNumber,collectionItem,checkReason,taxPortion,taxAmountDocument
# MAGIC ,writeOffReasonCode,documentOriginCode,documentEnteredDate,referenceProcedure,objectKey,reversalDocumentNumber
# MAGIC ,reconciliationKeyForGeneralLedger,furtherPostingIndicator
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY contractDocumentNumber,repetitionItem,itemNumber,partialClearingSubitem  
# MAGIC order by contractDocumentNumber,repetitionItem,itemNumber,partialClearingSubitem) as rn
# MAGIC FROM  cleansed.${vars.table}
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select
# MAGIC contractDocumentNumber
# MAGIC ,repetitionItem
# MAGIC ,itemNumber
# MAGIC ,partialClearingSubitem
# MAGIC ,companyCode
# MAGIC ,company
# MAGIC ,clearingStatus
# MAGIC ,businessPartnerGroupNumber
# MAGIC ,contractReferenceSpecification
# MAGIC ,contractAccountNumber
# MAGIC ,ficaDocumentNumber
# MAGIC ,ficaDocumentCategory
# MAGIC ,applicationArea
# MAGIC ,mainTransactionLineItemCode
# MAGIC ,mainTransactionLineItem
# MAGIC ,subtransactionLineItemCode
# MAGIC ,subtransactionLineItem
# MAGIC ,accountDeterminationCode
# MAGIC ,accountDetermination
# MAGIC ,divisionCode
# MAGIC ,accountGeneralLedger
# MAGIC ,taxSalesCode
# MAGIC ,downPaymentIndicator
# MAGIC ,statisticalItemType
# MAGIC ,documentDate
# MAGIC ,postingDate
# MAGIC ,currencyKey
# MAGIC ,paymentDueDate
# MAGIC ,cashDiscountDueDate
# MAGIC ,deferralToDate
# MAGIC ,cashDiscountPercentageRate
# MAGIC ,amountLocalCurrency
# MAGIC ,amountTransactionCurrency
# MAGIC ,amountEligibleCashDiscount
# MAGIC ,taxAmountLocalCurrency
# MAGIC ,taxAmountTransactionCurrency
# MAGIC ,clearingDate
# MAGIC ,clearingDocument
# MAGIC ,clearingDocumentPostingDate
# MAGIC ,clearingReason
# MAGIC ,clearingCurrency
# MAGIC ,clearingAmount
# MAGIC ,taxAmountClearingCurrency
# MAGIC ,cashDiscount
# MAGIC ,clearingValueDate
# MAGIC ,settlementPeriodLowerLimit
# MAGIC ,billingPeriodUpperLimit
# MAGIC ,clearingRestriction
# MAGIC ,valueAdjustment
# MAGIC ,documentTypeCode
# MAGIC ,documentType
# MAGIC ,referenceDocumentNumber
# MAGIC ,collectionItem
# MAGIC ,checkReason
# MAGIC ,taxPortion
# MAGIC ,taxAmountDocument
# MAGIC ,writeOffReasonCode
# MAGIC ,documentOriginCode
# MAGIC ,documentEnteredDate
# MAGIC ,referenceProcedure
# MAGIC ,objectKey
# MAGIC ,reversalDocumentNumber
# MAGIC ,reconciliationKeyForGeneralLedger
# MAGIC ,furtherPostingIndicator
# MAGIC from
# MAGIC (select
# MAGIC OPBEL as contractDocumentNumber,
# MAGIC OPUPW as repetitionItem,
# MAGIC OPUPK as itemNumber,
# MAGIC OPUPZ as partialClearingSubitem,
# MAGIC BUKRS as companyCode,
# MAGIC b.companyName as company,
# MAGIC AUGST as clearingStatus,
# MAGIC GPART as businessPartnerGroupNumber,
# MAGIC VTREF as contractReferenceSpecification,
# MAGIC VKONT as contractAccountNumber,
# MAGIC ABWBL as ficaDocumentNumber,
# MAGIC ABWTP as ficaDocumentCategory,
# MAGIC APPLK as applicationArea,
# MAGIC HVORG as mainTransactionLineItemCode,
# MAGIC c.mainTransaction as mainTransactionLineItem,
# MAGIC TVORG as subtransactionLineItemCode,
# MAGIC d.subtransaction as subtransactionLineItem,
# MAGIC KOFIZ as accountDeterminationCode,
# MAGIC e.accountDetermination as accountDetermination,
# MAGIC SPART as divisionCode,
# MAGIC HKONT as accountGeneralLedger,
# MAGIC MWSKZ as taxSalesCode,
# MAGIC XANZA as downPaymentIndicator,
# MAGIC STAKZ as statisticalItemType,
# MAGIC BLDAT as documentDate,
# MAGIC BUDAT as postingDate,
# MAGIC WAERS as currencyKey,
# MAGIC FAEDN as paymentDueDate,
# MAGIC FAEDS as cashDiscountDueDate,
# MAGIC STUDT as deferralToDate,
# MAGIC SKTPZ as cashDiscountPercentageRate,
# MAGIC BETRH as amountLocalCurrency,
# MAGIC BETRW as amountTransactionCurrency,
# MAGIC SKFBT as amountEligibleCashDiscount,
# MAGIC SBETH as taxAmountLocalCurrency,
# MAGIC SBETW as taxAmountTransactionCurrency,
# MAGIC AUGDT as clearingDate,
# MAGIC AUGBL as clearingDocument,
# MAGIC AUGBD as clearingDocumentPostingDate,
# MAGIC AUGRD as clearingReason,
# MAGIC AUGWA as clearingCurrency,
# MAGIC AUGBT as clearingAmount,
# MAGIC AUGBS as taxAmountClearingCurrency,
# MAGIC AUGSK as cashDiscount,
# MAGIC AUGVD as clearingValueDate,
# MAGIC ABRZU as settlementPeriodLowerLimit,
# MAGIC ABRZO as billingPeriodUpperLimit,
# MAGIC AUGRS as clearingRestriction,
# MAGIC INFOZ as valueAdjustment,
# MAGIC BLART as documentTypeCode,
# MAGIC f.documentType as documentType,
# MAGIC XBLNR as referenceDocumentNumber,
# MAGIC INKPS as collectionItem,
# MAGIC C4EYE as checkReason,
# MAGIC SCTAX as taxPortion,
# MAGIC STTAX as taxAmountDocument,
# MAGIC ABGRD as writeOffReasonCode,
# MAGIC HERKF as documentOriginCode,
# MAGIC CPUDT as documentEnteredDate,
# MAGIC AWTYP as referenceProcedure,
# MAGIC AWKEY as objectKey,
# MAGIC STORB as reversalDocumentNumber,
# MAGIC FIKEY as reconciliationKeyForGeneralLedger,
# MAGIC XCLOS as furtherPostingIndicator,
# MAGIC row_number() over (partition by OPBEL,OPUPW,OPUPK,OPUPZ order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC LEFT JOIN cleansed.ISU_0COMP_CODE_TEXT b
# MAGIC   ON b.companyCode = BUKRS
# MAGIC LEFT JOIN cleansed.ISU_0UC_HVORG_TEXT c
# MAGIC   ON c.applicationArea = APPLK
# MAGIC   AND c.mainTransactionLineItemCode = HVORG
# MAGIC left join cleansed.ISU_0UC_TVORG_TEXT d
# MAGIC   on d.applicationArea = APPLK
# MAGIC   and d.mainTransactionLineItemCode = HVORG
# MAGIC   and d.subtransactionLineItemCode = TVORG
# MAGIC LEFT JOIN cleansed.ISU_0FCACTDETID_TEXT e
# MAGIC   on e.accountDeterminationCode = KOFIZ
# MAGIC left join cleansed.ISU_0FC_BLART_TEXT f
# MAGIC   on f.applicationArea = APPLK
# MAGIC   and f.documentTypeCode = BLART
# MAGIC )a  where  a.rn = 1
# MAGIC except
# MAGIC select
# MAGIC contractDocumentNumber
# MAGIC ,repetitionItem
# MAGIC ,itemNumber
# MAGIC ,partialClearingSubitem
# MAGIC ,companyCode
# MAGIC ,company
# MAGIC ,clearingStatus
# MAGIC ,businessPartnerGroupNumber
# MAGIC ,contractReferenceSpecification
# MAGIC ,contractAccountNumber
# MAGIC ,ficaDocumentNumber
# MAGIC ,ficaDocumentCategory
# MAGIC ,applicationArea
# MAGIC ,mainTransactionLineItemCode
# MAGIC ,mainTransactionLineItem
# MAGIC ,subtransactionLineItemCode
# MAGIC ,subtransactionLineItem
# MAGIC ,accountDeterminationCode
# MAGIC ,accountDetermination
# MAGIC ,divisionCode
# MAGIC ,accountGeneralLedger
# MAGIC ,taxSalesCode
# MAGIC ,downPaymentIndicator
# MAGIC ,statisticalItemType
# MAGIC ,documentDate
# MAGIC ,postingDate
# MAGIC ,currencyKey
# MAGIC ,paymentDueDate
# MAGIC ,cashDiscountDueDate
# MAGIC ,deferralToDate
# MAGIC ,cashDiscountPercentageRate
# MAGIC ,amountLocalCurrency
# MAGIC ,amountTransactionCurrency
# MAGIC ,amountEligibleCashDiscount
# MAGIC ,taxAmountLocalCurrency
# MAGIC ,taxAmountTransactionCurrency
# MAGIC ,clearingDate
# MAGIC ,clearingDocument
# MAGIC ,clearingDocumentPostingDate
# MAGIC ,clearingReason
# MAGIC ,clearingCurrency
# MAGIC ,clearingAmount
# MAGIC ,taxAmountClearingCurrency
# MAGIC ,cashDiscount
# MAGIC ,clearingValueDate
# MAGIC ,settlementPeriodLowerLimit
# MAGIC ,billingPeriodUpperLimit
# MAGIC ,clearingRestriction
# MAGIC ,valueAdjustment
# MAGIC ,documentTypeCode
# MAGIC ,documentType
# MAGIC ,referenceDocumentNumber
# MAGIC ,collectionItem
# MAGIC ,checkReason
# MAGIC ,taxPortion
# MAGIC ,taxAmountDocument
# MAGIC ,writeOffReasonCode
# MAGIC ,documentOriginCode
# MAGIC ,documentEnteredDate
# MAGIC ,referenceProcedure
# MAGIC ,objectKey
# MAGIC ,reversalDocumentNumber
# MAGIC ,reconciliationKeyForGeneralLedger
# MAGIC ,furtherPostingIndicator
# MAGIC from
# MAGIC cleansed.${vars.table}

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC contractDocumentNumber
# MAGIC ,repetitionItem
# MAGIC ,itemNumber
# MAGIC ,partialClearingSubitem
# MAGIC ,companyCode
# MAGIC ,company
# MAGIC ,clearingStatus
# MAGIC ,businessPartnerGroupNumber
# MAGIC ,contractReferenceSpecification
# MAGIC ,contractAccountNumber
# MAGIC ,ficaDocumentNumber
# MAGIC ,ficaDocumentCategory
# MAGIC ,applicationArea
# MAGIC ,mainTransactionLineItemCode
# MAGIC ,mainTransactionLineItem
# MAGIC ,subtransactionLineItemCode
# MAGIC ,subtransactionLineItem
# MAGIC ,accountDeterminationCode
# MAGIC ,accountDetermination
# MAGIC ,divisionCode
# MAGIC ,accountGeneralLedger
# MAGIC ,taxSalesCode
# MAGIC ,downPaymentIndicator
# MAGIC ,statisticalItemType
# MAGIC ,documentDate
# MAGIC ,postingDate
# MAGIC ,currencyKey
# MAGIC ,paymentDueDate
# MAGIC ,cashDiscountDueDate
# MAGIC ,deferralToDate
# MAGIC ,cashDiscountPercentageRate
# MAGIC ,amountLocalCurrency
# MAGIC ,amountTransactionCurrency
# MAGIC ,amountEligibleCashDiscount
# MAGIC ,taxAmountLocalCurrency
# MAGIC ,taxAmountTransactionCurrency
# MAGIC ,clearingDate
# MAGIC ,clearingDocument
# MAGIC ,clearingDocumentPostingDate
# MAGIC ,clearingReason
# MAGIC ,clearingCurrency
# MAGIC ,clearingAmount
# MAGIC ,taxAmountClearingCurrency
# MAGIC ,cashDiscount
# MAGIC ,clearingValueDate
# MAGIC ,settlementPeriodLowerLimit
# MAGIC ,billingPeriodUpperLimit
# MAGIC ,clearingRestriction
# MAGIC ,valueAdjustment
# MAGIC ,documentTypeCode
# MAGIC ,documentType
# MAGIC ,referenceDocumentNumber
# MAGIC ,collectionItem
# MAGIC ,checkReason
# MAGIC ,taxPortion
# MAGIC ,taxAmountDocument
# MAGIC ,writeOffReasonCode
# MAGIC ,documentOriginCode
# MAGIC ,documentEnteredDate
# MAGIC ,referenceProcedure
# MAGIC ,objectKey
# MAGIC ,reversalDocumentNumber
# MAGIC ,reconciliationKeyForGeneralLedger
# MAGIC ,furtherPostingIndicator
# MAGIC from
# MAGIC cleansed.${vars.table}
# MAGIC except
# MAGIC select
# MAGIC contractDocumentNumber
# MAGIC ,repetitionItem
# MAGIC ,itemNumber
# MAGIC ,partialClearingSubitem
# MAGIC ,companyCode
# MAGIC ,company
# MAGIC ,clearingStatus
# MAGIC ,businessPartnerGroupNumber
# MAGIC ,contractReferenceSpecification
# MAGIC ,contractAccountNumber
# MAGIC ,ficaDocumentNumber
# MAGIC ,ficaDocumentCategory
# MAGIC ,applicationArea
# MAGIC ,mainTransactionLineItemCode
# MAGIC ,mainTransactionLineItem
# MAGIC ,subtransactionLineItemCode
# MAGIC ,subtransactionLineItem
# MAGIC ,accountDeterminationCode
# MAGIC ,accountDetermination
# MAGIC ,divisionCode
# MAGIC ,accountGeneralLedger
# MAGIC ,taxSalesCode
# MAGIC ,downPaymentIndicator
# MAGIC ,statisticalItemType
# MAGIC ,documentDate
# MAGIC ,postingDate
# MAGIC ,currencyKey
# MAGIC ,paymentDueDate
# MAGIC ,cashDiscountDueDate
# MAGIC ,deferralToDate
# MAGIC ,cashDiscountPercentageRate
# MAGIC ,amountLocalCurrency
# MAGIC ,amountTransactionCurrency
# MAGIC ,amountEligibleCashDiscount
# MAGIC ,taxAmountLocalCurrency
# MAGIC ,taxAmountTransactionCurrency
# MAGIC ,clearingDate
# MAGIC ,clearingDocument
# MAGIC ,clearingDocumentPostingDate
# MAGIC ,clearingReason
# MAGIC ,clearingCurrency
# MAGIC ,clearingAmount
# MAGIC ,taxAmountClearingCurrency
# MAGIC ,cashDiscount
# MAGIC ,clearingValueDate
# MAGIC ,settlementPeriodLowerLimit
# MAGIC ,billingPeriodUpperLimit
# MAGIC ,clearingRestriction
# MAGIC ,valueAdjustment
# MAGIC ,documentTypeCode
# MAGIC ,documentType
# MAGIC ,referenceDocumentNumber
# MAGIC ,collectionItem
# MAGIC ,checkReason
# MAGIC ,taxPortion
# MAGIC ,taxAmountDocument
# MAGIC ,writeOffReasonCode
# MAGIC ,documentOriginCode
# MAGIC ,documentEnteredDate
# MAGIC ,referenceProcedure
# MAGIC ,objectKey
# MAGIC ,reversalDocumentNumber
# MAGIC ,reconciliationKeyForGeneralLedger
# MAGIC ,furtherPostingIndicator
# MAGIC from
# MAGIC (select
# MAGIC OPBEL as contractDocumentNumber,
# MAGIC OPUPW as repetitionItem,
# MAGIC OPUPK as itemNumber,
# MAGIC OPUPZ as partialClearingSubitem,
# MAGIC BUKRS as companyCode,
# MAGIC b.companyName as company,
# MAGIC AUGST as clearingStatus,
# MAGIC GPART as businessPartnerGroupNumber,
# MAGIC VTREF as contractReferenceSpecification,
# MAGIC VKONT as contractAccountNumber,
# MAGIC ABWBL as ficaDocumentNumber,
# MAGIC ABWTP as ficaDocumentCategory,
# MAGIC APPLK as applicationArea,
# MAGIC HVORG as mainTransactionLineItemCode,
# MAGIC c.mainTransaction as mainTransactionLineItem,
# MAGIC TVORG as subtransactionLineItemCode,
# MAGIC d.subtransaction as subtransactionLineItem,
# MAGIC KOFIZ as accountDeterminationCode,
# MAGIC e.accountDetermination as accountDetermination,
# MAGIC SPART as divisionCode,
# MAGIC HKONT as accountGeneralLedger,
# MAGIC MWSKZ as taxSalesCode,
# MAGIC XANZA as downPaymentIndicator,
# MAGIC STAKZ as statisticalItemType,
# MAGIC BLDAT as documentDate,
# MAGIC BUDAT as postingDate,
# MAGIC WAERS as currencyKey,
# MAGIC FAEDN as paymentDueDate,
# MAGIC FAEDS as cashDiscountDueDate,
# MAGIC STUDT as deferralToDate,
# MAGIC SKTPZ as cashDiscountPercentageRate,
# MAGIC BETRH as amountLocalCurrency,
# MAGIC BETRW as amountTransactionCurrency,
# MAGIC SKFBT as amountEligibleCashDiscount,
# MAGIC SBETH as taxAmountLocalCurrency,
# MAGIC SBETW as taxAmountTransactionCurrency,
# MAGIC AUGDT as clearingDate,
# MAGIC AUGBL as clearingDocument,
# MAGIC AUGBD as clearingDocumentPostingDate,
# MAGIC AUGRD as clearingReason,
# MAGIC AUGWA as clearingCurrency,
# MAGIC AUGBT as clearingAmount,
# MAGIC AUGBS as taxAmountClearingCurrency,
# MAGIC AUGSK as cashDiscount,
# MAGIC AUGVD as clearingValueDate,
# MAGIC ABRZU as settlementPeriodLowerLimit,
# MAGIC ABRZO as billingPeriodUpperLimit,
# MAGIC AUGRS as clearingRestriction,
# MAGIC INFOZ as valueAdjustment,
# MAGIC BLART as documentTypeCode,
# MAGIC f.documentType as documentType,
# MAGIC XBLNR as referenceDocumentNumber,
# MAGIC INKPS as collectionItem,
# MAGIC C4EYE as checkReason,
# MAGIC SCTAX as taxPortion,
# MAGIC STTAX as taxAmountDocument,
# MAGIC ABGRD as writeOffReasonCode,
# MAGIC HERKF as documentOriginCode,
# MAGIC CPUDT as documentEnteredDate,
# MAGIC AWTYP as referenceProcedure,
# MAGIC AWKEY as objectKey,
# MAGIC STORB as reversalDocumentNumber,
# MAGIC FIKEY as reconciliationKeyForGeneralLedger,
# MAGIC XCLOS as furtherPostingIndicator,
# MAGIC row_number() over (partition by OPBEL,OPUPW,OPUPK,OPUPZ order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC LEFT JOIN cleansed.ISU_0COMP_CODE_TEXT b
# MAGIC   ON b.companyCode = BUKRS
# MAGIC LEFT JOIN cleansed.ISU_0UC_HVORG_TEXT c
# MAGIC   ON c.applicationArea = APPLK
# MAGIC   AND c.mainTransactionLineItemCode = HVORG
# MAGIC left join cleansed.ISU_0UC_TVORG_TEXT d
# MAGIC   on d.applicationArea = APPLK
# MAGIC   and d.mainTransactionLineItemCode = HVORG
# MAGIC   and d.subtransactionLineItemCode = TVORG
# MAGIC LEFT JOIN cleansed.ISU_0FCACTDETID_TEXT e
# MAGIC   on e.accountDeterminationCode = KOFIZ
# MAGIC left join cleansed.ISU_0FC_BLART_TEXT f
# MAGIC   on f.applicationArea = APPLK
# MAGIC   and f.documentTypeCode = BLART
# MAGIC )a  where  a.rn = 1
