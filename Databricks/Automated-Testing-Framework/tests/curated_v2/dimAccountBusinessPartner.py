# Databricks notebook source
# MAGIC %run ../../atf-common

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated_v2.dimAccountBusinessPartner

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct(sendAdditionalBillFlag),distinct(noPaymentFormFlagsendAdditionalDunningNoticeFlag) ,distinct(sendAdditionalDunningNoticeFlag) from curated_v2.dimAccountBusinessPartner

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct(sendAdditionalBillFlag) from curated_v2.dimAccountBusinessPartner

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct(noPaymentFormFlag) from curated_v2.dimAccountBusinessPartner

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct(sendAdditionalDunningNoticeFlag) from curated_v2.dimAccountBusinessPartner

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct(sendAdditionalBillFlag,noPaymentFormFlag,sendAdditionalDunningNoticeFlag) from curated_v2.dimAccountBusinessPartner

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
target_df = spark.sql("select * from curated_v2.dimAccountBusinessPartner")
target_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Source with mapping for active and deleted records
source_isu=spark.sql("""
select
'ISU' as sourceSystemCode,
a.contractAccountNumber as contractAccountNumber,
a.businessPartnerGroupNumber as businessPartnerGroupNumber,
a.accountRelationshipCode as accountRelationshipCode,
a.accountRelationship as accountRelationship,
a.businessPartnerReferenceNumber as businessPartnerReferenceNumber,
a.toleranceGroupCode as toleranceGroupCode,
a.toleranceGroup as toleranceGroup,
a.manualOutsortingReasonCode as manualOutsortingReasonCode,
a.manualOutsortingReason as manualOutsortingReason,
a.outsortingCheckGroupCode as outsortingCheckGroupCode,
a.outsortingCheckGroup as outsortingCheckGroup,
a.manualOutsortingCount as manualOutsortingCount,
a.participationInYearlyAdvancePaymentCode as participationInYearlyAdvancePaymentCode,
a.participationInYearlyAdvancePayment as participationInYearlyAdvancePayment,
a.activatebudgetbillingProcedureCode as activatebudgetbillingProcedureCode,
a.activatebudgetbillingProcedure as activatebudgetbillingProcedure,
a.paymentConditionCode as paymentConditionCode,
a.paymentCondition as paymentCondition,
a.accountDeterminationCode as accountDeterminationCode,
a.accountDetermination as accountDetermination,
a.alternativeInvoiceRecipient as alternativeInvoiceRecipient,
a.addressNumber as addressNumber,
a.addressNumberForAlternativeBillRecipient as addressNumberForAlternativeBillRecipient,
a.alternativeContractAccountForCollectiveBills as alternativeContractAccountForCollectiveBills,
a.dispatchControlForAltBillRecipientCode as dispatchControlForAltBillRecipientCode,
a.dispatchControlForAltBillRecipient as dispatchControlForAltBillRecipient,
a.applicationFormCode as applicationFormCode,
a.applicationForm as applicationForm,
a.sendAdditionalBillFlag as sendAdditionalBillFlag,
a.headerUUID as headerUUID,
a.companyGroupCode as companyGroupCode,
a.companyGroupName as companyGroupName,
a.standardCompanyCode as standardCompanyCode,
a.standardCompanyName as standardCompanyName,
a.incomingPaymentMethodCode as incomingPaymentMethodCode,
a.incomingPaymentMethod as incomingPaymentMethod,
a.bankDetailsId as bankDetailsId,
a.paymentCardId as paymentCardId,
a.noPaymentFormFlag as noPaymentFormFlag,
a.alternativeDunningRecipient as alternativeDunningRecipient,
a.collectionStrategyCode as collectionStrategyCode,
a.collectionStrategyName as collectionStrategyName,
a.collectionManagementMasterDataGroupCode as collectionManagementMasterDataGroupCode,
a.collectionManagementMasterDataGroup as collectionManagementMasterDataGroup,
a.shippingControlForAltDunningRecipientCode as shippingControlForAltDunningRecipientCode,
a.shippingControlForAltDunningRecipient as shippingControlForAltDunningRecipient,
a.sendAdditionalDunningNoticeFlag as sendAdditionalDunningNoticeFlag,
a.dispatchControlForOriginalCustomerCode as dispatchControlForOriginalCustomerCode,
a.dispatchControlForOriginalCustomer as dispatchControlForOriginalCustomer,
a.budgetBillingRequestForCashPayerCode as budgetBillingRequestForCashPayerCode,
a.budgetBillingRequestForCashPayer as budgetBillingRequestForCashPayer,
a.budgetBillingRequestForDebtorCode as budgetBillingRequestForDebtorCode,
a.budgetBillingRequestForDebtor as budgetBillingRequestForDebtor,
a.directDebitLimit as directDebitLimit,
a.addressNumberForAlternativeDunningRecipient as addressNumberForAlternativeDunningRecipient,
a.numberOfSuccessfulDirectDebits as numberOfSuccessfulDirectDebits,
a.numberOfDirectDebitReturns as numberOfDirectDebitReturns,
a.additionalDaysForCashManagement as additionalDaysForCashManagement,
a.numberOfMonthsForDirectDebitLimit as numberOfMonthsForDirectDebitLimit,
a.clearingCategoryCode as clearingCategoryCode,
a.clearingCategory as clearingCategory,
a.createdBy as createdBy,
a.createdDate as createdDate,
a.changedBy as changedBy,
a.lastChangedDate as lastChangedDate
, a._RecordStart as _RecordStart
, a._RecordEnd as _RecordEnd
, a._RecordCurrent as _RecordCurrent
, a._RecordDeleted as _RecordDeleted


from
cleansed.isu_0uc_accntbp_attr_2 a

""")
source_isu.createOrReplaceTempView("source_view")
#display(source_isu)
#source_isu.count()

src_a = spark.sql("Select * except(_RecordCurrent,_recordDeleted) from source_view where _RecordCurrent=1 and _recordDeleted=0 ")
src_d = spark.sql("Select * except(_RecordCurrent,_recordDeleted) from source_view where _RecordCurrent=0 and _recordDeleted=1 ")
src_a.createOrReplaceTempView("src_a")
src_d.createOrReplaceTempView("src_d")


# COMMAND ----------

# DBTITLE 1,Define Variables for ATF
keyColumns = 'contractAccountNumber, businessPartnerGroupNumber'
mandatoryColumns = 'contractAccountNumber, businessPartnerGroupNumber'

columns = ("""
sourceSystemCode,
contractAccountNumber,
businessPartnerGroupNumber,
accountRelationshipCode,
accountRelationship,
businessPartnerReferenceNumber,
toleranceGroupCode,
toleranceGroup,
manualOutsortingReasonCode,
manualOutsortingReason,
outsortingCheckGroupCode,
outsortingCheckGroup,
manualOutsortingCount,
participationInYearlyAdvancePaymentCode,
participationInYearlyAdvancePayment,
activatebudgetbillingProcedureCode,
activatebudgetbillingProcedure,
paymentConditionCode,
paymentCondition,
accountDeterminationCode,
accountDetermination,
alternativeInvoiceRecipient,
addressNumber,
addressNumberForAlternativeBillRecipient,
alternativeContractAccountForCollectiveBills,
dispatchControlForAltBillRecipientCode,
dispatchControlForAltBillRecipient,
applicationFormCode,
applicationForm,
sendAdditionalBillFlag,
headerUUID,
companyGroupCode,
companyGroupName,
standardCompanyCode,
standardCompanyName,
incomingPaymentMethodCode,
incomingPaymentMethod,
bankDetailsId,
paymentCardId,
noPaymentFormFlag,
alternativeDunningRecipient,
collectionStrategyCode,
collectionStrategyName,
collectionManagementMasterDataGroupCode,
collectionManagementMasterDataGroup,
shippingControlForAltDunningRecipientCode,
shippingControlForAltDunningRecipient,
sendAdditionalDunningNoticeFlag,
dispatchControlForOriginalCustomerCode,
dispatchControlForOriginalCustomer,
budgetBillingRequestForCashPayerCode,
budgetBillingRequestForCashPayer,
budgetBillingRequestForDebtorCode,
budgetBillingRequestForDebtor,
directDebitLimit,
addressNumberForAlternativeDunningRecipient,
numberOfSuccessfulDirectDebits,
numberOfDirectDebitReturns,
additionalDaysForCashManagement,
numberOfMonthsForDirectDebitLimit,
clearingCategoryCode,
clearingCategory,
createdBy,
createdDate,
changedBy,
lastChangedDate

""")

source_a = spark.sql(f"""
Select {columns}
From src_a
""")

source_d = spark.sql(f"""
Select {columns}
From src_d
""")

# COMMAND ----------

#ALWAYS RUN THIS AT THE END
RunTests()

# COMMAND ----------

# MAGIC %md
# MAGIC #Investigation for failed tests

# COMMAND ----------

src_c=  spark.sql("Select * except(_RecordCurrent,_recordDeleted) from source_view where _RecordCurrent=0 and _recordDeleted=0 ")
src_c.createOrReplaceTempView("src_c")
display(src_c)
src_c.count()

# COMMAND ----------

tgt_c=  spark.sql("Select * except(_RecordCurrent,_recordDeleted) from curated_v2.dimAccountBusinessPartner where _RecordCurrent=0 and _recordDeleted=0 ")
tgt_c.createOrReplaceTempView("tgt_c")
display(tgt_c)
tgt_c.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT length(contractAccountNumber) from source_view
# MAGIC                       WHERE length(contractAccountNumber)<>2

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC SELECT DISTINCT length(businessPartnerGroupNumber) from source_view
# MAGIC                       WHERE length(businessPartnerGroupNumber)<>2
