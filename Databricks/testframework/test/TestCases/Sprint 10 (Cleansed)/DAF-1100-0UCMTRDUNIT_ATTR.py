# Databricks notebook source
#config parameters
source = 'ISU' #either CRM or ISU
table = '0UCMTRDUNIT_ATTR'

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
# MAGIC ,intervalBetweenReadingAndEnd
# MAGIC ,intervalBetweenOrderAndReading
# MAGIC ,intervalBetweenDownloadAndReading
# MAGIC ,intervalBetweenPrintoutAndReading
# MAGIC ,intervalBetweenAnnouncementAndOrder
# MAGIC ,intervalBetweenPrintoutAndOrder
# MAGIC ,portionNumber
# MAGIC ,meterReaderNumber
# MAGIC ,meterReadingTime
# MAGIC ,numberOfPreviousReadings
# MAGIC ,numberOFMobileDataEntry
# MAGIC ,meterReadingInterval
# MAGIC ,entryInterval
# MAGIC ,createdDate
# MAGIC ,createdBy
# MAGIC ,lastChangedDate
# MAGIC ,lastChangedBy
# MAGIC ,divisionCategory1
# MAGIC ,divisionCategory2
# MAGIC ,divisionCategory3
# MAGIC ,divisionCategory4
# MAGIC ,divisionCategory5
# MAGIC ,factoryCalendar
# MAGIC ,correctHolidayToWorkDay
# MAGIC ,billingKeyDate
# MAGIC ,numberOfDays
# MAGIC ,intervalBetweenOrderAndPlanned
# MAGIC ,meterReadingCenter
# MAGIC from
# MAGIC (select
# MAGIC TERMSCHL as portion
# MAGIC ,EPER_ABL as intervalBetweenReadingAndEnd
# MAGIC ,AUF_ABL as intervalBetweenOrderAndReading
# MAGIC ,DOWNL_ABL as intervalBetweenDownloadAndReading
# MAGIC ,DRUCK_ABL as intervalBetweenPrintoutAndReading
# MAGIC ,ANSCH_AUF as intervalBetweenAnnouncementAndOrder
# MAGIC ,AUKSA_AUF as intervalBetweenPrintoutAndOrder
# MAGIC ,PORTION as portionNumber
# MAGIC ,ABLESER as meterReaderNumber
# MAGIC ,ABLZEIT as meterReadingTime
# MAGIC ,AZVORABL as numberOfPreviousReadings
# MAGIC ,MDENR as numberOFMobileDataEntry
# MAGIC ,ABLKAR as meterReadingInterval
# MAGIC ,STANDKAR as entryInterval
# MAGIC ,EROEDAT as createdDate
# MAGIC ,ERNAM as createdBy
# MAGIC ,AENDDATE as lastChangedDate
# MAGIC ,AENDNAM as lastChangedBy
# MAGIC ,SPARTENTY1 as divisionCategory1
# MAGIC ,SPARTENTY2 as divisionCategory2
# MAGIC ,SPARTENTY3 as divisionCategory3
# MAGIC ,SPARTENTY4 as divisionCategory4
# MAGIC ,SPARTENTY5 as divisionCategory5
# MAGIC ,IDENT as factoryCalendar
# MAGIC ,SAPKAL as correctHolidayToWorkDay
# MAGIC ,STICHTAG as billingKeyDate
# MAGIC ,TAGE as numberOfDays
# MAGIC ,AUF_KAL as intervalBetweenOrderAndPlanned
# MAGIC ,ABL_Z as meterReadingCenter
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
# MAGIC ,intervalBetweenReadingAndEnd
# MAGIC ,intervalBetweenOrderAndReading
# MAGIC ,intervalBetweenDownloadAndReading
# MAGIC ,intervalBetweenPrintoutAndReading
# MAGIC ,intervalBetweenAnnouncementAndOrder
# MAGIC ,intervalBetweenPrintoutAndOrder
# MAGIC ,portionNumber
# MAGIC ,meterReaderNumber
# MAGIC ,meterReadingTime
# MAGIC ,numberOfPreviousReadings
# MAGIC ,numberOFMobileDataEntry
# MAGIC ,meterReadingInterval
# MAGIC ,entryInterval
# MAGIC ,createdDate
# MAGIC ,createdBy
# MAGIC ,lastChangedDate
# MAGIC ,lastChangedBy
# MAGIC ,divisionCategory1
# MAGIC ,divisionCategory2
# MAGIC ,divisionCategory3
# MAGIC ,divisionCategory4
# MAGIC ,divisionCategory5
# MAGIC ,factoryCalendar
# MAGIC ,correctHolidayToWorkDay
# MAGIC ,billingKeyDate
# MAGIC ,numberOfDays
# MAGIC ,intervalBetweenOrderAndPlanned
# MAGIC ,meterReadingCenter
# MAGIC from
# MAGIC (select
# MAGIC TERMSCHL as portion
# MAGIC ,EPER_ABL as intervalBetweenReadingAndEnd
# MAGIC ,AUF_ABL as intervalBetweenOrderAndReading
# MAGIC ,DOWNL_ABL as intervalBetweenDownloadAndReading
# MAGIC ,DRUCK_ABL as intervalBetweenPrintoutAndReading
# MAGIC ,ANSCH_AUF as intervalBetweenAnnouncementAndOrder
# MAGIC ,AUKSA_AUF as intervalBetweenPrintoutAndOrder
# MAGIC ,PORTION as portionNumber
# MAGIC ,ABLESER as meterReaderNumber
# MAGIC ,ABLZEIT as meterReadingTime
# MAGIC ,AZVORABL as numberOfPreviousReadings
# MAGIC ,MDENR as numberOFMobileDataEntry
# MAGIC ,ABLKAR as meterReadingInterval
# MAGIC ,STANDKAR as entryInterval
# MAGIC ,EROEDAT as createdDate
# MAGIC ,ERNAM as createdBy
# MAGIC ,AENDDATE as lastChangedDate
# MAGIC ,AENDNAM as lastChangedBy
# MAGIC ,SPARTENTY1 as divisionCategory1
# MAGIC ,SPARTENTY2 as divisionCategory2
# MAGIC ,SPARTENTY3 as divisionCategory3
# MAGIC ,SPARTENTY4 as divisionCategory4
# MAGIC ,SPARTENTY5 as divisionCategory5
# MAGIC ,IDENT as factoryCalendar
# MAGIC ,SAPKAL as correctHolidayToWorkDay
# MAGIC ,STICHTAG as billingKeyDate
# MAGIC ,TAGE as numberOfDays
# MAGIC ,AUF_KAL as intervalBetweenOrderAndPlanned
# MAGIC ,ABL_Z as meterReadingCenter
# MAGIC ,row_number() over (partition by TERMSCHL order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC )a where  a.rn = 1
# MAGIC )

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT portion
# MAGIC , COUNT (*) as count
# MAGIC FROM cleansed.${vars.table}
# MAGIC GROUP BY portion
# MAGIC HAVING COUNT (*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Checks
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY portion  order by portion) as rn
# MAGIC FROM  cleansed.${vars.table}
# MAGIC )a where a.rn > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Source and Target Data
# MAGIC %sql
# MAGIC select
# MAGIC portion
# MAGIC ,intervalBetweenReadingAndEnd
# MAGIC ,intervalBetweenOrderAndReading
# MAGIC ,intervalBetweenDownloadAndReading
# MAGIC ,intervalBetweenPrintoutAndReading
# MAGIC ,intervalBetweenAnnouncementAndOrder
# MAGIC ,intervalBetweenPrintoutAndOrder
# MAGIC ,portionNumber
# MAGIC ,meterReaderNumber
# MAGIC ,meterReadingTime
# MAGIC ,numberOfPreviousReadings
# MAGIC ,numberOFMobileDataEntry
# MAGIC ,meterReadingInterval
# MAGIC ,entryInterval
# MAGIC ,createdDate
# MAGIC ,createdBy
# MAGIC ,lastChangedDate
# MAGIC ,lastChangedBy
# MAGIC ,divisionCategory1
# MAGIC ,divisionCategory2
# MAGIC ,divisionCategory3
# MAGIC ,divisionCategory4
# MAGIC ,divisionCategory5
# MAGIC ,factoryCalendar
# MAGIC ,correctHolidayToWorkDay
# MAGIC ,to_date(billingKeyDate, 'yyyy-MM-dd')
# MAGIC ,numberOfDays
# MAGIC ,intervalBetweenOrderAndPlanned
# MAGIC ,meterReadingCenter
# MAGIC from
# MAGIC (select
# MAGIC TERMSCHL as portion
# MAGIC ,cast(EPER_ABL as Integer) as intervalBetweenReadingAndEnd
# MAGIC ,cast(AUF_ABL as Integer) as intervalBetweenOrderAndReading
# MAGIC ,cast(DOWNL_ABL as Integer) as intervalBetweenDownloadAndReading
# MAGIC ,cast(DRUCK_ABL as Integer) as intervalBetweenPrintoutAndReading
# MAGIC ,cast(ANSCH_AUF as Integer) as intervalBetweenAnnouncementAndOrder
# MAGIC ,cast(AUKSA_AUF as Integer) as intervalBetweenPrintoutAndOrder
# MAGIC ,PORTION as portionNumber
# MAGIC ,ABLESER as meterReaderNumber
# MAGIC ,cast(ABLZEIT as decimal) as meterReadingTime
# MAGIC ,cast(AZVORABL as Integer) as numberOfPreviousReadings
# MAGIC ,MDENR as numberOFMobileDataEntry
# MAGIC ,cast(ABLKAR as Integer) as meterReadingInterval
# MAGIC ,cast(STANDKAR as Integer) as entryInterval
# MAGIC ,to_date(EROEDAT, 'yyyy-MM-dd') as createdDate
# MAGIC ,ERNAM as createdBy
# MAGIC ,to_date(AENDDATE, 'yyyy-MM-dd') as lastChangedDate
# MAGIC ,AENDNAM as lastChangedBy
# MAGIC ,SPARTENTY1 as divisionCategory1
# MAGIC ,SPARTENTY2 as divisionCategory2
# MAGIC ,SPARTENTY3 as divisionCategory3
# MAGIC ,SPARTENTY4 as divisionCategory4
# MAGIC ,SPARTENTY5 as divisionCategory5
# MAGIC ,IDENT as factoryCalendar
# MAGIC ,SAPKAL as correctHolidayToWorkDay
# MAGIC ,cast(STICHTAG as string) as billingKeyDate
# MAGIC ,TAGE as numberOfDays
# MAGIC ,AUF_KAL as intervalBetweenOrderAndPlanned
# MAGIC ,ABL_Z as meterReadingCenter
# MAGIC ,row_number() over (partition by TERMSCHL order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC )a where  a.rn = 1
# MAGIC except
# MAGIC select
# MAGIC portion
# MAGIC ,intervalBetweenReadingAndEnd
# MAGIC ,intervalBetweenOrderAndReading
# MAGIC ,intervalBetweenDownloadAndReading
# MAGIC ,intervalBetweenPrintoutAndReading
# MAGIC ,intervalBetweenAnnouncementAndOrder
# MAGIC ,intervalBetweenPrintoutAndOrder
# MAGIC ,portionNumber
# MAGIC ,meterReaderNumber
# MAGIC ,meterReadingTime
# MAGIC ,numberOfPreviousReadings
# MAGIC ,numberOFMobileDataEntry
# MAGIC ,meterReadingInterval
# MAGIC ,entryInterval
# MAGIC ,createdDate
# MAGIC ,createdBy
# MAGIC ,lastChangedDate
# MAGIC ,lastChangedBy
# MAGIC ,divisionCategory1
# MAGIC ,divisionCategory2
# MAGIC ,divisionCategory3
# MAGIC ,divisionCategory4
# MAGIC ,divisionCategory5
# MAGIC ,factoryCalendar
# MAGIC ,correctHolidayToWorkDay
# MAGIC ,billingKeyDate
# MAGIC ,numberOfDays
# MAGIC ,intervalBetweenOrderAndPlanned
# MAGIC ,meterReadingCenter
# MAGIC from
# MAGIC cleansed.${vars.table}

# COMMAND ----------

# DBTITLE 1,[Verification] Compare Target and Source Data
# MAGIC %sql
# MAGIC select
# MAGIC portion
# MAGIC ,intervalBetweenReadingAndEnd
# MAGIC ,intervalBetweenOrderAndReading
# MAGIC ,intervalBetweenDownloadAndReading
# MAGIC ,intervalBetweenPrintoutAndReading
# MAGIC ,intervalBetweenAnnouncementAndOrder
# MAGIC ,intervalBetweenPrintoutAndOrder
# MAGIC ,portionNumber
# MAGIC ,meterReaderNumber
# MAGIC ,meterReadingTime
# MAGIC ,numberOfPreviousReadings
# MAGIC ,numberOFMobileDataEntry
# MAGIC ,meterReadingInterval
# MAGIC ,entryInterval
# MAGIC ,createdDate
# MAGIC ,createdBy
# MAGIC ,lastChangedDate
# MAGIC ,lastChangedBy
# MAGIC ,divisionCategory1
# MAGIC ,divisionCategory2
# MAGIC ,divisionCategory3
# MAGIC ,divisionCategory4
# MAGIC ,divisionCategory5
# MAGIC ,factoryCalendar
# MAGIC ,correctHolidayToWorkDay
# MAGIC ,billingKeyDate
# MAGIC ,numberOfDays
# MAGIC ,intervalBetweenOrderAndPlanned
# MAGIC ,meterReadingCenter
# MAGIC from
# MAGIC cleansed.${vars.table}
# MAGIC except
# MAGIC select
# MAGIC portion
# MAGIC ,intervalBetweenReadingAndEnd
# MAGIC ,intervalBetweenOrderAndReading
# MAGIC ,intervalBetweenDownloadAndReading
# MAGIC ,intervalBetweenPrintoutAndReading
# MAGIC ,intervalBetweenAnnouncementAndOrder
# MAGIC ,intervalBetweenPrintoutAndOrder
# MAGIC ,portionNumber
# MAGIC ,meterReaderNumber
# MAGIC ,meterReadingTime
# MAGIC ,numberOfPreviousReadings
# MAGIC ,numberOFMobileDataEntry
# MAGIC ,meterReadingInterval
# MAGIC ,entryInterval
# MAGIC ,createdDate
# MAGIC ,createdBy
# MAGIC ,lastChangedDate
# MAGIC ,lastChangedBy
# MAGIC ,divisionCategory1
# MAGIC ,divisionCategory2
# MAGIC ,divisionCategory3
# MAGIC ,divisionCategory4
# MAGIC ,divisionCategory5
# MAGIC ,factoryCalendar
# MAGIC ,correctHolidayToWorkDay
# MAGIC ,to_date(billingKeyDate, 'yyyy-MM-dd')
# MAGIC ,numberOfDays
# MAGIC ,intervalBetweenOrderAndPlanned
# MAGIC ,meterReadingCenter
# MAGIC from
# MAGIC (select
# MAGIC TERMSCHL as portion
# MAGIC ,cast(EPER_ABL as Integer) as intervalBetweenReadingAndEnd
# MAGIC ,cast(AUF_ABL as Integer) as intervalBetweenOrderAndReading
# MAGIC ,cast(DOWNL_ABL as Integer) as intervalBetweenDownloadAndReading
# MAGIC ,cast(DRUCK_ABL as Integer) as intervalBetweenPrintoutAndReading
# MAGIC ,cast(ANSCH_AUF as Integer) as intervalBetweenAnnouncementAndOrder
# MAGIC ,cast(AUKSA_AUF as Integer) as intervalBetweenPrintoutAndOrder
# MAGIC ,PORTION as portionNumber
# MAGIC ,ABLESER as meterReaderNumber
# MAGIC ,cast(ABLZEIT as decimal) as meterReadingTime
# MAGIC ,cast(AZVORABL as Integer) as numberOfPreviousReadings
# MAGIC ,MDENR as numberOFMobileDataEntry
# MAGIC ,cast(ABLKAR as Integer) as meterReadingInterval
# MAGIC ,cast(STANDKAR as Integer) as entryInterval
# MAGIC ,to_date(EROEDAT, 'yyyy-MM-dd') as createdDate
# MAGIC ,ERNAM as createdBy
# MAGIC ,to_date(AENDDATE, 'yyyy-MM-dd') as lastChangedDate
# MAGIC ,AENDNAM as lastChangedBy
# MAGIC ,SPARTENTY1 as divisionCategory1
# MAGIC ,SPARTENTY2 as divisionCategory2
# MAGIC ,SPARTENTY3 as divisionCategory3
# MAGIC ,SPARTENTY4 as divisionCategory4
# MAGIC ,SPARTENTY5 as divisionCategory5
# MAGIC ,IDENT as factoryCalendar
# MAGIC ,SAPKAL as correctHolidayToWorkDay
# MAGIC ,cast(STICHTAG as string) as billingKeyDate
# MAGIC ,TAGE as numberOfDays
# MAGIC ,AUF_KAL as intervalBetweenOrderAndPlanned
# MAGIC ,ABL_Z as meterReadingCenter
# MAGIC ,row_number() over (partition by TERMSCHL order by EXTRACT_DATETIME desc) as rn
# MAGIC from test.${vars.table}
# MAGIC )a where  a.rn = 1
