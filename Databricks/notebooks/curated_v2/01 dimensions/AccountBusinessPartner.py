# Databricks notebook source
# MAGIC %run ../common/common-curated-includeMain

# COMMAND ----------

def getAccountBusinessPartner():
    
    df_isu = spark.sql(f"""
        select 
          'ISU' as sourceSystemCode,
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
          from {ADS_DATABASE_CLEANSED}.isu_0uc_accntbp_attr_2 
          where _RecordCurrent = 1 and _RecordDeleted = 0 
        """)
    
    dummyDimRecDf = spark.createDataFrame([("-1","-1")], ["contractAccountNumber","businessPartnerGroupNumber"])
    
    df = df_isu.unionByName(dummyDimRecDf, allowMissingColumns = True)
    
    schema = StructType([StructField('accountBusinessPartnerSK', StringType(), False),
                      StructField('sourceSystemCode', StringType(), True),
                      StructField('contractAccountNumber',StringType(), False),
                      StructField('businessPartnerGroupNumber',StringType(), False),
                      StructField('accountRelationshipCode',StringType(), True),
                      StructField('accountRelationship',StringType(), True),
                      StructField('businessPartnerReferenceNumber',StringType(), True),
                      StructField('toleranceGroupCode',StringType(), True),
                      StructField('toleranceGroup',StringType(), True),
                      StructField('manualOutsortingReasonCode',StringType(), True),
                      StructField('manualOutsortingReason',StringType(), True),
                      StructField('outsortingCheckGroupCode',StringType(), True),
                      StructField('outsortingCheckGroup',StringType(), True),
                      StructField('manualOutsortingCount',StringType(), True),
                      StructField('participationInYearlyAdvancePaymentCode',StringType(), True),
                      StructField('participationInYearlyAdvancePayment',StringType(), True),
                      StructField('activatebudgetbillingProcedureCode',StringType(), True),
                      StructField('activatebudgetbillingProcedure',StringType(), True),
                      StructField('paymentConditionCode',StringType(), True),
                      StructField('paymentCondition',StringType(), True),
                      StructField('accountDeterminationCode',StringType(), True),
                      StructField('accountDetermination',StringType(), True),
                      StructField('alternativeInvoiceRecipient',StringType(), True),
                      StructField('addressNumber',StringType(), True),
                      StructField('addressNumberForAlternativeBillRecipient',StringType(), True),
                      StructField('alternativeContractAccountForCollectiveBills',StringType(), True),
                      StructField('dispatchControlForAltBillRecipientCode',StringType(), True),
                      StructField('dispatchControlForAltBillRecipient',StringType(), True),
                      StructField('applicationFormCode',StringType(), True),
                      StructField('applicationForm',StringType(), True),
                      StructField('sendAdditionalBillFlag',StringType(), True),
                      StructField('headerUUID',StringType(), True),
                      StructField('companyGroupCode',StringType(), True),
                      StructField('companyGroupName',StringType(), True),
                      StructField('standardCompanyCode',StringType(), True),
                      StructField('standardCompanyName',StringType(), True),
                      StructField('incomingPaymentMethodCode',StringType(), True),
                      StructField('incomingPaymentMethod',StringType(), True),
                      StructField('bankDetailsId',StringType(), True),
                      StructField('paymentCardId',StringType(), True),
                      StructField('noPaymentFormFlag',StringType(), True),
                      StructField('alternativeDunningRecipient',StringType(), True),
                      StructField('collectionStrategyCode',StringType(), True),
                      StructField('collectionStrategyName',StringType(), True),
                      StructField('collectionManagementMasterDataGroupCode',StringType(), True),
                      StructField('collectionManagementMasterDataGroup',StringType(), True),
                      StructField('shippingControlForAltDunningRecipientCode',StringType(), True),
                      StructField('shippingControlForAltDunningRecipient',StringType(), True),
                      StructField('sendAdditionalDunningNoticeFlag',StringType(), True),
                      StructField('dispatchControlForOriginalCustomerCode',StringType(), True),
                      StructField('dispatchControlForOriginalCustomer',StringType(), True),
                      StructField('budgetBillingRequestForCashPayerCode',StringType(), True),
                      StructField('budgetBillingRequestForCashPayer',StringType(), True),
                      StructField('budgetBillingRequestForDebtorCode',StringType(), True),
                      StructField('budgetBillingRequestForDebtor',StringType(), True),
                      StructField('directDebitLimit',DecimalType(13,0), True),
                      StructField('addressNumberForAlternativeDunningRecipient',StringType(), True),
                      StructField('numberOfSuccessfulDirectDebits',StringType(), True),
                      StructField('numberOfDirectDebitReturns',StringType(), True),
                      StructField('additionalDaysForCashManagement',StringType(), True),
                      StructField('numberOfMonthsForDirectDebitLimit',StringType(), True),
                      StructField('clearingCategoryCode',StringType(), True),
                      StructField('clearingCategory',StringType(), True),
                      StructField('createdBy',StringType(), True),
                      StructField('createdDate',DateType(), True),
                      StructField('changedBy',StringType(), True),
                      StructField('lastChangedDate',DateType(), True)])
    
    return df, schema

# COMMAND ----------

df, schema = getAccountBusinessPartner()
#TemplateEtl(df, entity="dimAccountBusinessPartner", businessKey="contractAccountNumber,businessPartnerGroupNumber", schema=schema, writeMode=ADS_WRITE_MODE_OVERWRITE, AddSK=True)
TemplateEtlSCD(df, entity="dimAccountBusinessPartner", businessKey="contractAccountNumber,businessPartnerGroupNumber", schema=schema)

# COMMAND ----------

dbutils.notebook.exit("1")
