# Databricks notebook source
# MAGIC %run ../../common/common-atf

# COMMAND ----------

# DBTITLE 1,Define fields and table names
keyColumns =  'businessPartnerGroupNumber'
mandatoryColumns = 'businessPartnerGroupNumber,sourceSystemCode'

columns = ("""sourceSystemCode,
businessPartnerGroupNumber,
businessPartnerGroupCode,
businessPartnerGroup,
businessPartnerCategoryCode,
businessPartnerCategory,
businessPartnerTypeCode,
businessPartnerType,
externalNumber,
businessPartnerGUID,
businessPartnerGroupName1,
businessPartnerGroupName2,
paymentAssistSchemeFlag,
billAssistFlag,
consent1Indicator,
warWidowFlag,
indicatorCreatedUserId,
indicatorCreatedDate,
kidneyDialysisFlag,
patientUnit,
patientTitleCode,
patientTitle,
patientFirstName,
patientSurname,
patientAreaCode,
patientPhoneNumber,
hospitalCode,
hospitalName,
patientMachineTypeCode,
patientMachineType,
machineTypeValidFromDate,
machineTypeValidToDate,
machineOffReasonCode,
machineOffReason,
createdBy,
createdDateTime,
lastUpdatedBy,
lastUpdatedDateTime,
validFromDate,
validToDate
""")

# COMMAND ----------

#ALWAYS RUN THIS AT THE END
RunTests()
