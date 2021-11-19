# Databricks notebook source
#config parameters
source = 'ISU' #either CRM or ISU
table = 'TE438T'

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
# MAGIC portion
# MAGIC ,scheduleMasterRecord
# MAGIC ,billingPeriodEndDate
# MAGIC ,periodLengthMonths
# MAGIC ,periodCategory
# MAGIC ,meterReadingAllocationDate
# MAGIC ,allowableBudgetBillingCycles
# MAGIC ,createdDate
# MAGIC ,createdBy
# MAGIC ,lastChangedDate
# MAGIC ,lastChangedBy
# MAGIC ,divisionCategory1
# MAGIC ,divisionCategory2
# MAGIC ,divisionCategory3
# MAGIC ,divisionCategory4
# MAGIC ,divisionCategory5
# MAGIC ,budgetBillingCycle1
# MAGIC ,budgetBillingCycle2
# MAGIC ,budgetBillingCycle3
# MAGIC ,budgetBillingCycle4
# MAGIC ,budgetBillingCycle5
# MAGIC ,parameterRecord
# MAGIC ,factoryCalendar
# MAGIC ,correctHolidayToWorkDay
# MAGIC ,lowerLimitBillingPeriod
# MAGIC ,upperLimitBillingPeriod
# MAGIC ,periodLengthDays
# MAGIC ,isWorkDay
# MAGIC ,extrapolationCategory
# MAGIC from
# MAGIC (SELECT
# MAGIC TERMSCHL as portion
# MAGIC ,TERMTEXT as scheduleMasterRecord
# MAGIC ,TERMERST as billingPeriodEndDate
# MAGIC ,PERIODEW as periodLengthMonths
# MAGIC ,PERIODET as periodCategory
# MAGIC ,ZUORDDAT as meterReadingAllocationDate
# MAGIC ,ABSZYK as allowableBudgetBillingCycles
# MAGIC ,EROEDAT as createdDate
# MAGIC ,ERNAM as createdBy
# MAGIC ,AENDDATE as lastChangedDate
# MAGIC ,AENDNAM as lastChangedBy
# MAGIC ,SPARTENTY1 as divisionCategory1
# MAGIC ,SPARTENTY2 as divisionCategory2
# MAGIC ,SPARTENTY3 as divisionCategory3
# MAGIC ,SPARTENTY4 as divisionCategory4
# MAGIC ,SPARTENTY5 as divisionCategory5
# MAGIC ,ABSZYKTER1 as budgetBillingCycle1
# MAGIC ,ABSZYKTER2 as budgetBillingCycle2
# MAGIC ,ABSZYKTER3 as budgetBillingCycle3
# MAGIC ,ABSZYKTER4 as budgetBillingCycle4
# MAGIC ,ABSZYKTER5 as budgetBillingCycle5
# MAGIC ,PARASATZ as parameterRecord
# MAGIC ,IDENT as factoryCalendar
# MAGIC ,SAPKAL as correctHolidayToWorkDay
# MAGIC ,PTOLERFROM as lowerLimitBillingPeriod
# MAGIC ,PTOLERTO as upperLimitBillingPeriod
# MAGIC ,PERIODED as periodLengthDays
# MAGIC ,WORK_DAY as isWorkDay
# MAGIC ,EXTRAPOLWASTE as extrapolationCategory
# MAGIC ,row_number() over (partition by TERMSCHL order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC )a where  a.rn = 1

# COMMAND ----------

# DBTITLE 1,[Verification] Count Check
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.${vars.table}
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from (select
# MAGIC portion
# MAGIC ,scheduleMasterRecord
# MAGIC ,billingPeriodEndDate
# MAGIC ,periodLengthMonths
# MAGIC ,periodCategory
# MAGIC ,meterReadingAllocationDate
# MAGIC ,allowableBudgetBillingCycles
# MAGIC ,createdDate
# MAGIC ,createdBy
# MAGIC ,lastChangedDate
# MAGIC ,lastChangedBy
# MAGIC ,divisionCategory1
# MAGIC ,divisionCategory2
# MAGIC ,divisionCategory3
# MAGIC ,divisionCategory4
# MAGIC ,divisionCategory5
# MAGIC ,budgetBillingCycle1
# MAGIC ,budgetBillingCycle2
# MAGIC ,budgetBillingCycle3
# MAGIC ,budgetBillingCycle4
# MAGIC ,budgetBillingCycle5
# MAGIC ,parameterRecord
# MAGIC ,factoryCalendar
# MAGIC ,correctHolidayToWorkDay
# MAGIC ,lowerLimitBillingPeriod
# MAGIC ,upperLimitBillingPeriod
# MAGIC ,periodLengthDays
# MAGIC ,isWorkDay
# MAGIC ,extrapolationCategory
# MAGIC from
# MAGIC (SELECT
# MAGIC TERMSCHL as portion
# MAGIC ,TERMTEXT as scheduleMasterRecord
# MAGIC ,TERMERST as billingPeriodEndDate
# MAGIC ,PERIODEW as periodLengthMonths
# MAGIC ,PERIODET as periodCategory
# MAGIC ,ZUORDDAT as meterReadingAllocationDate
# MAGIC ,ABSZYK as allowableBudgetBillingCycles
# MAGIC ,EROEDAT as createdDate
# MAGIC ,ERNAM as createdBy
# MAGIC ,AENDDATE as lastChangedDate
# MAGIC ,AENDNAM as lastChangedBy
# MAGIC ,SPARTENTY1 as divisionCategory1
# MAGIC ,SPARTENTY2 as divisionCategory2
# MAGIC ,SPARTENTY3 as divisionCategory3
# MAGIC ,SPARTENTY4 as divisionCategory4
# MAGIC ,SPARTENTY5 as divisionCategory5
# MAGIC ,ABSZYKTER1 as budgetBillingCycle1
# MAGIC ,ABSZYKTER2 as budgetBillingCycle2
# MAGIC ,ABSZYKTER3 as budgetBillingCycle3
# MAGIC ,ABSZYKTER4 as budgetBillingCycle4
# MAGIC ,ABSZYKTER5 as budgetBillingCycle5
# MAGIC ,PARASATZ as parameterRecord
# MAGIC ,IDENT as factoryCalendar
# MAGIC ,SAPKAL as correctHolidayToWorkDay
# MAGIC ,PTOLERFROM as lowerLimitBillingPeriod
# MAGIC ,PTOLERTO as upperLimitBillingPeriod
# MAGIC ,PERIODED as periodLengthDays
# MAGIC ,WORK_DAY as isWorkDay
# MAGIC ,EXTRAPOLWASTE as extrapolationCategory
# MAGIC ,row_number() over (partition by TERMSCHL order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC )a where  a.rn = 1)

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT portion, COUNT (*) as count
# MAGIC FROM cleansed.${vars.table}
# MAGIC GROUP BY portion
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY portion order by portion) as rn
# MAGIC FROM  cleansed.${vars.table}
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select
# MAGIC portion
# MAGIC ,scheduleMasterRecord
# MAGIC ,billingPeriodEndDate
# MAGIC ,periodLengthMonths
# MAGIC ,periodCategory
# MAGIC ,meterReadingAllocationDate
# MAGIC ,allowableBudgetBillingCycles
# MAGIC ,createdDate
# MAGIC ,createdBy
# MAGIC ,lastChangedDate
# MAGIC ,lastChangedBy
# MAGIC ,divisionCategory1
# MAGIC ,divisionCategory2
# MAGIC ,divisionCategory3
# MAGIC ,divisionCategory4
# MAGIC ,divisionCategory5
# MAGIC ,budgetBillingCycle1
# MAGIC ,budgetBillingCycle2
# MAGIC ,budgetBillingCycle3
# MAGIC ,budgetBillingCycle4
# MAGIC ,budgetBillingCycle5
# MAGIC ,parameterRecord
# MAGIC ,factoryCalendar
# MAGIC ,correctHolidayToWorkDay
# MAGIC ,lowerLimitBillingPeriod
# MAGIC ,upperLimitBillingPeriod
# MAGIC ,periodLengthDays
# MAGIC ,isWorkDay
# MAGIC ,extrapolationCategory
# MAGIC from
# MAGIC (SELECT
# MAGIC TERMSCHL as portion
# MAGIC ,TERMTEXT as scheduleMasterRecord
# MAGIC ,TERMERST as billingPeriodEndDate
# MAGIC ,PERIODEW as periodLengthMonths
# MAGIC ,PERIODET as periodCategory
# MAGIC ,ZUORDDAT as meterReadingAllocationDate
# MAGIC ,ABSZYK as allowableBudgetBillingCycles
# MAGIC ,EROEDAT as createdDate
# MAGIC ,ERNAM as createdBy
# MAGIC ,AENDDATE as lastChangedDate
# MAGIC ,AENDNAM as lastChangedBy
# MAGIC ,SPARTENTY1 as divisionCategory1
# MAGIC ,SPARTENTY2 as divisionCategory2
# MAGIC ,SPARTENTY3 as divisionCategory3
# MAGIC ,SPARTENTY4 as divisionCategory4
# MAGIC ,SPARTENTY5 as divisionCategory5
# MAGIC ,ABSZYKTER1 as budgetBillingCycle1
# MAGIC ,ABSZYKTER2 as budgetBillingCycle2
# MAGIC ,ABSZYKTER3 as budgetBillingCycle3
# MAGIC ,ABSZYKTER4 as budgetBillingCycle4
# MAGIC ,ABSZYKTER5 as budgetBillingCycle5
# MAGIC ,PARASATZ as parameterRecord
# MAGIC ,IDENT as factoryCalendar
# MAGIC ,SAPKAL as correctHolidayToWorkDay
# MAGIC ,PTOLERFROM as lowerLimitBillingPeriod
# MAGIC ,PTOLERTO as upperLimitBillingPeriod
# MAGIC ,PERIODED as periodLengthDays
# MAGIC ,WORK_DAY as isWorkDay
# MAGIC ,EXTRAPOLWASTE as extrapolationCategory
# MAGIC ,row_number() over (partition by TERMSCHL order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC )a where  a.rn = 1
# MAGIC except
# MAGIC select
# MAGIC portion
# MAGIC ,scheduleMasterRecord
# MAGIC ,billingPeriodEndDate
# MAGIC ,periodLengthMonths
# MAGIC ,periodCategory
# MAGIC ,meterReadingAllocationDate
# MAGIC ,allowableBudgetBillingCycles
# MAGIC ,createdDate
# MAGIC ,createdBy
# MAGIC ,lastChangedDate
# MAGIC ,lastChangedBy
# MAGIC ,divisionCategory1
# MAGIC ,divisionCategory2
# MAGIC ,divisionCategory3
# MAGIC ,divisionCategory4
# MAGIC ,divisionCategory5
# MAGIC ,budgetBillingCycle1
# MAGIC ,budgetBillingCycle2
# MAGIC ,budgetBillingCycle3
# MAGIC ,budgetBillingCycle4
# MAGIC ,budgetBillingCycle5
# MAGIC ,parameterRecord
# MAGIC ,factoryCalendar
# MAGIC ,correctHolidayToWorkDay
# MAGIC ,lowerLimitBillingPeriod
# MAGIC ,upperLimitBillingPeriod
# MAGIC ,periodLengthDays
# MAGIC ,isWorkDay
# MAGIC ,extrapolationCategory
# MAGIC from
# MAGIC cleansed.${vars.table}

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC portion
# MAGIC ,scheduleMasterRecord
# MAGIC ,billingPeriodEndDate
# MAGIC ,periodLengthMonths
# MAGIC ,periodCategory
# MAGIC ,meterReadingAllocationDate
# MAGIC ,allowableBudgetBillingCycles
# MAGIC ,createdDate
# MAGIC ,createdBy
# MAGIC ,lastChangedDate
# MAGIC ,lastChangedBy
# MAGIC ,divisionCategory1
# MAGIC ,divisionCategory2
# MAGIC ,divisionCategory3
# MAGIC ,divisionCategory4
# MAGIC ,divisionCategory5
# MAGIC ,budgetBillingCycle1
# MAGIC ,budgetBillingCycle2
# MAGIC ,budgetBillingCycle3
# MAGIC ,budgetBillingCycle4
# MAGIC ,budgetBillingCycle5
# MAGIC ,parameterRecord
# MAGIC ,factoryCalendar
# MAGIC ,correctHolidayToWorkDay
# MAGIC ,lowerLimitBillingPeriod
# MAGIC ,upperLimitBillingPeriod
# MAGIC ,periodLengthDays
# MAGIC ,isWorkDay
# MAGIC ,extrapolationCategory
# MAGIC from
# MAGIC cleansed.${vars.table}
# MAGIC except
# MAGIC select
# MAGIC portion
# MAGIC ,scheduleMasterRecord
# MAGIC ,billingPeriodEndDate
# MAGIC ,periodLengthMonths
# MAGIC ,periodCategory
# MAGIC ,meterReadingAllocationDate
# MAGIC ,allowableBudgetBillingCycles
# MAGIC ,createdDate
# MAGIC ,createdBy
# MAGIC ,lastChangedDate
# MAGIC ,lastChangedBy
# MAGIC ,divisionCategory1
# MAGIC ,divisionCategory2
# MAGIC ,divisionCategory3
# MAGIC ,divisionCategory4
# MAGIC ,divisionCategory5
# MAGIC ,budgetBillingCycle1
# MAGIC ,budgetBillingCycle2
# MAGIC ,budgetBillingCycle3
# MAGIC ,budgetBillingCycle4
# MAGIC ,budgetBillingCycle5
# MAGIC ,parameterRecord
# MAGIC ,factoryCalendar
# MAGIC ,correctHolidayToWorkDay
# MAGIC ,lowerLimitBillingPeriod
# MAGIC ,upperLimitBillingPeriod
# MAGIC ,periodLengthDays
# MAGIC ,isWorkDay
# MAGIC ,extrapolationCategory
# MAGIC from
# MAGIC (SELECT
# MAGIC TERMSCHL as portion
# MAGIC ,TERMTEXT as scheduleMasterRecord
# MAGIC ,TERMERST as billingPeriodEndDate
# MAGIC ,PERIODEW as periodLengthMonths
# MAGIC ,PERIODET as periodCategory
# MAGIC ,ZUORDDAT as meterReadingAllocationDate
# MAGIC ,ABSZYK as allowableBudgetBillingCycles
# MAGIC ,EROEDAT as createdDate
# MAGIC ,ERNAM as createdBy
# MAGIC ,AENDDATE as lastChangedDate
# MAGIC ,AENDNAM as lastChangedBy
# MAGIC ,SPARTENTY1 as divisionCategory1
# MAGIC ,SPARTENTY2 as divisionCategory2
# MAGIC ,SPARTENTY3 as divisionCategory3
# MAGIC ,SPARTENTY4 as divisionCategory4
# MAGIC ,SPARTENTY5 as divisionCategory5
# MAGIC ,ABSZYKTER1 as budgetBillingCycle1
# MAGIC ,ABSZYKTER2 as budgetBillingCycle2
# MAGIC ,ABSZYKTER3 as budgetBillingCycle3
# MAGIC ,ABSZYKTER4 as budgetBillingCycle4
# MAGIC ,ABSZYKTER5 as budgetBillingCycle5
# MAGIC ,PARASATZ as parameterRecord
# MAGIC ,IDENT as factoryCalendar
# MAGIC ,SAPKAL as correctHolidayToWorkDay
# MAGIC ,PTOLERFROM as lowerLimitBillingPeriod
# MAGIC ,PTOLERTO as upperLimitBillingPeriod
# MAGIC ,PERIODED as periodLengthDays
# MAGIC ,WORK_DAY as isWorkDay
# MAGIC ,EXTRAPOLWASTE as extrapolationCategory
# MAGIC ,row_number() over (partition by TERMSCHL order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC )a where  a.rn = 1
