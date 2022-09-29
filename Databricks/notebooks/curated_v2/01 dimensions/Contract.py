# Databricks notebook source
###########################################################################################################################
# Loads CONTRACT dimension 
#############################################################################################################################
# Method
# 1.Load Cleansed layer table data into dataframe and transform
# 2.SCHEMA DEFINITION
#############################################################################################################################

# COMMAND ----------

# MAGIC %run ../common/common-curated-includeMain

# COMMAND ----------

def getContract():

    #Contract Data from SAP ISU
    isuContractDf  = spark.sql(f"""select 'ISU' as sourceSystemCode
                                    ,contractId
                                    ,companyCode
                                    ,companyName
                                    ,divisionCode
                                    ,division
                                    ,installationNumber
                                    ,contractAccountNumber
                                    ,accountDeterminationCode
                                    ,accountDetermination
                                    ,allowableBudgetBillingCyclesCode
                                    ,allowableBudgetBillingCycles
                                    ,invoiceContractsJointlyCode
                                    ,invoiceContractsJointly
                                    ,manualBillContractflag
                                    ,billBlockingReasonCode
                                    ,billBlockingReason
                                    ,specialMoveOutCaseCode
                                    ,specialMoveOutCase
                                    ,contractText
                                    ,legacyMoveInDate
                                    ,numberOfCancellations
                                    ,numberOfRenewals
                                    ,personnelNumber
                                    ,contractNumberLegacy
                                    ,isContractInvoicedFlag
                                    ,isContractTransferredFlag
                                    ,outsortingCheckGroupForBilling
                                    ,manualOutsortingCount
                                    ,serviceProvider
                                    ,contractTerminatedForBillingFlag
                                    ,invoicingParty
                                    ,cancellationReasonCRM
                                    ,moveInDate
                                    ,moveOutDate
                                    ,budgetBillingStopDate
                                    ,premise
                                    ,propertyNumber
                                    ,validFromDate
                                    ,agreementNumber
                                    ,addressNumber
                                    ,alternativeAddressNumber
                                    ,identificationNumber
                                    ,objectReferenceIndicator
                                    ,objectNumber
                                    ,createdDate
                                    ,createdBy
                                    ,lastChangedDate
                                    ,lastChangedBy
                                from {ADS_DATABASE_CLEANSED}.isu_0uccontract_attr_2
                                where _RecordCurrent = 1 and _RecordDeleted=0
                              """)
    
    dummyDimRecDf = spark.createDataFrame([("-1","Unknown","Unknown")], ["contractId","companyName","division"])   
    dfResult = isuContractDf.unionByName(dummyDimRecDf, allowMissingColumns = True)    
    
    #5.Apply schema definition
    schema = StructType([
                            StructField('contractSK', StringType(), False),
                            StructField('sourceSystemCode', StringType(), True),
                            StructField('contractId', StringType(), False),
                            StructField('companyCode', StringType(), True),
                            StructField('companyName', StringType(), True),
                            StructField('divisionCode', StringType(), True),
                            StructField('division', StringType(), True),
                            StructField('installationNumber', StringType(), True),
                            StructField('contractAccountNumber', StringType(), True),
                            StructField('accountDeterminationCode', StringType(), True),
                            StructField('accountDetermination', StringType(), True),
                            StructField('allowableBudgetBillingCyclesCode', StringType(), True),
                            StructField('allowableBudgetBillingCycles', StringType(), True),
                            StructField('invoiceContractsJointlyCode', StringType(), True),
                            StructField('invoiceContractsJointly', StringType(), True),
                            StructField('manualBillContractflag', StringType(), True),
                            StructField('billBlockingReasonCode', StringType(), True),
                            StructField('billBlockingReason', StringType(), True),
                            StructField('specialMoveOutCaseCode', StringType(), True),   
                            StructField('specialMoveOutCase', StringType(), True),
                            StructField('contractText', StringType(), True),
                            StructField('legacyMoveInDate', DateType(), True),
                            StructField('numberOfCancellations', StringType(), True),
                            StructField('numberOfRenewals', StringType(), True),
                            StructField('personnelNumber', StringType(), True),
                            StructField('contractNumberLegacy', StringType(), True),
                            StructField('isContractInvoicedFlag', StringType(), True),
                            StructField('isContractTransferredFlag', StringType(), True),
                            StructField('outsortingCheckGroupForBilling', StringType(), True),
                            StructField('manualOutsortingCount', StringType(), True),
                            StructField('serviceProvider', StringType(), True),
                            StructField('contractTerminatedForBillingFlag', StringType(), True),
                            StructField('invoicingParty', StringType(), True),
                            StructField('cancellationReasonCRM', StringType(), True),
                            StructField('moveInDate', DateType(), True),
                            StructField('moveOutDate', DateType(), True),
                            StructField('budgetBillingStopDate', DateType(), True),
                            StructField('premise', StringType(), True),
                            StructField('propertyNumber', StringType(), True),
                            StructField('validFromDate', DateType(), True),
                            StructField('agreementNumber', StringType(), True),
                            StructField('addressNumber', StringType(), True),
                            StructField('alternativeAddressNumber', StringType(), True),
                            StructField('identificationNumber', StringType(), True),
                            StructField('objectReferenceIndicator', StringType(), True),
                            StructField('objectNumber', StringType(), True),   
                            StructField('createdDate', DateType(), True),
                            StructField('createdBy', StringType(), True),
                            StructField('lastChangedDate', DateType(), True),
                            StructField('lastChangedBy', StringType(), True)
                      ])    
    
    return dfResult, schema

# COMMAND ----------

df, schema = getContract()
TemplateEtlSCD(df, entity="dimContract", businessKey="contractId", schema=schema)

# COMMAND ----------

dbutils.notebook.exit("1")
