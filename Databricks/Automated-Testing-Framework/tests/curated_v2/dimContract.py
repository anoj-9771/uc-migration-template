# Databricks notebook source
# MAGIC %run ../../atf-common

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated_v2.dimContract

# COMMAND ----------

aaa=spark.sql("select objectReferenceIndicator from curated_v2.dimContract where objectReferenceIndicator='X'")
aaa.createOrReplaceTempView("aaa")
display(aaa)

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
target_df = spark.sql("select * from curated_v2.dimContract")
target_df.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct(objectReferenceIndicator) from  curated_v2.dimContract

# COMMAND ----------

# DBTITLE 1,Source with mapping for active and deleted records
source_isu=spark.sql("""
select
'ISU' as sourceSystemCode,    
a.contractId as contractId,
a.companyCode as companyCode,
a.companyName as companyName,
a.divisionCode as divisionCode,
a.division as division,
a.installationNumber as installationNumber,
a.contractAccountNumber as contractAccountNumber,
a.accountDeterminationCode as accountDeterminationCode,
a.accountDetermination as accountDetermination,
a.allowableBudgetBillingCyclesCode as allowableBudgetBillingCyclesCode,
a.allowableBudgetBillingCycles as allowableBudgetBillingCycles,
a.invoiceContractsJointlyCode as invoiceContractsJointlyCode,
a.invoiceContractsJointly as invoiceContractsJointly,
a.manualBillContractflag as manualBillContractflag,
a.billBlockingReasonCode as billBlockingReasonCode,
a.billBlockingReason as billBlockingReason,
a.specialMoveOutCaseCode as specialMoveOutCaseCode,
a.specialMoveOutCase as specialMoveOutCase,
a.contractText as contractText,
a.legacyMoveInDate as legacyMoveInDate,
a.numberOfCancellations as numberOfCancellations,
a.numberOfRenewals as numberOfRenewals,
a.personnelNumber as personnelNumber,
a.contractNumberLegacy as contractNumberLegacy,
a.isContractInvoicedFlag as isContractInvoicedFlag,
a.isContractTransferredFlag as isContractTransferredFlag,
a.outsortingCheckGroupForBilling as outsortingCheckGroupForBilling,
a.manualOutsortingCount as manualOutsortingCount,
a.serviceProvider as serviceProvider,
a.contractTerminatedForBillingFlag as contractTerminatedForBillingFlag,
a.invoicingParty as invoicingParty,
a.cancellationReasonCRM as cancellationReasonCRM,
a.moveInDate as moveInDate,
a.moveOutDate as moveOutDate,
a.budgetBillingStopDate as budgetBillingStopDate,
a.premise as premise,
a.propertyNumber as propertyNumber,
a.validFromDate as validFromDate,
a.agreementNumber as agreementNumber,
a.addressNumber as addressNumber,
a.alternativeAddressNumber as alternativeAddressNumber,
a.identificationNumber as identificationNumber,
a.objectReferenceIndicator as objectReferenceIndicator,
a.objectNumber as objectNumber,
a.createdDate as createdDate,
a.createdBy as createdBy,
a.lastChangedDate as lastChangedDate,
a.lastChangedBy as lastChangedBy
, a._RecordStart as _RecordStart
, a._RecordEnd as _RecordEnd
, a._RecordCurrent as _RecordCurrent
, a._RecordDeleted as _RecordDeleted

from
cleansed.isu_0uccontract_attr_2 a 
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
keyColumns = 'contractId'
mandatoryColumns = 'contractId'
columns = ("""
sourceSystemCode,
contractId,
companyCode,
companyName,
divisionCode,
division,
installationNumber,
contractAccountNumber,
accountDeterminationCode,
accountDetermination,
allowableBudgetBillingCyclesCode,
allowableBudgetBillingCycles,
invoiceContractsJointlyCode,
invoiceContractsJointly,
manualBillContractflag,
billBlockingReasonCode,
billBlockingReason,
specialMoveOutCaseCode,
specialMoveOutCase,
contractText,
legacyMoveInDate,
numberOfCancellations,
numberOfRenewals,
personnelNumber,
contractNumberLegacy,
isContractInvoicedFlag,
isContractTransferredFlag,
outsortingCheckGroupForBilling,
manualOutsortingCount,
serviceProvider,
contractTerminatedForBillingFlag,
invoicingParty,
cancellationReasonCRM,
moveInDate,
moveOutDate,
budgetBillingStopDate,
premise,
propertyNumber,
validFromDate,
agreementNumber,
addressNumber,
alternativeAddressNumber,
identificationNumber,
objectReferenceIndicator,
objectNumber,
createdDate,
createdBy,
lastChangedDate,
lastChangedBy 
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
