# Databricks notebook source
table = 'ZDM_DS_EDW_PORTION_TEXT'
table1 = table.lower()
print(table1)

# COMMAND ----------

# DBTITLE 0,Writing Count Result in Database
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS test

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists test.table1

# COMMAND ----------

# DBTITLE 1,[Config] Connection Setup
from datetime import datetime

global fileCount

storage_account_name = "sablobdaftest01"
storage_account_access_key = dbutils.secrets.get(scope="TestScope",key="test-sablob-key")
container_name = "archive"
fileLocation = "wasbs://archive@saswcnonprod01landingtst.blob.core.windows.net/sapisu/"
fileType = 'json'
print(storage_account_name)
spark.conf.set("fs.azure.account.key."+storage_account_name+".blob.core.windows.net",storage_account_access_key) 

# COMMAND ----------

def listDetails(inFile):
    global fileCount
    dfs[fileCount] = spark.read.format(fileType).option("inferSchema", "true").load(inFile.path)
    print(f'Results for {inFile.name.strip("/")}')
    dfs[fileCount].printSchema()
    tmpTable = f'{inFile.name.split(".")[0]}_file{str(fileCount)}'
    dfs[fileCount].createOrReplaceTempView(tmpTable)
    display(spark.sql(f'select * from {tmpTable}'))
    testdf = spark.sql(f'select * from {tmpTable}')
    testdf.write.format(fileType).mode("append").saveAsTable("test" + "." + table)
   

# COMMAND ----------

# DBTITLE 1,List folders in fileLocation
folders = dbutils.fs.ls(fileLocation)
fileNames = []
fileCount = 0
dfs = {}

for folder in folders:
    try:
        subfolders = dbutils.fs.ls(folder.path)
        prntDate = False
        for subfolder in subfolders:
            files = dbutils.fs.ls(subfolder.path)
            prntTime = False
            for myFile in files:
                if myFile.name[:len(table)] == table:
                    fileCount += 1
                    if not prntDate:
                        print(f'{datetime.strftime(datetime.strptime(folder.name.strip("/"),"%Y%m%d"),"%Y-%m-%d")}')
                        printDate = True
                    
                    if not prntTime:
                        print(f'\t{subfolder.name.split("_")[-1].strip("/")}')
                        prntTime = True
                        
                    print(f'\t\t{myFile.name.strip("/")}\t{myFile.size}')
                    
                    if myFile.size > 0:
                        fileNames.append(myFile)
    except:
        print(f'Invalid folder name: {folder.name.strip("/")}')

for myFile in fileNames:
    listDetails(myFile)

# COMMAND ----------

sourcedf = spark.sql(f"select * from test.{table1}")
display(sourcedf)

# COMMAND ----------

# DBTITLE 1,Source schema check
sourcedf.printSchema()

# COMMAND ----------

sourcedf.createOrReplaceTempView("Source")

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct EXTRACT_DATETIME from Source order by EXTRACT_DATETIME desc

# COMMAND ----------

# DBTITLE 1,[Source with mapping]
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
# MAGIC from(SELECT
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
# MAGIC ,row_number() over (partition by portion order by EXTRACT_DATETIME desc) as rn
# MAGIC from source) a where a.rn = 1

# COMMAND ----------

lakedf = spark.sql("select * from cleansed.t_sapisu_ZDM_DS_EDW_PORTION_TEXT")

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
lakedf.printSchema()

# COMMAND ----------

# DBTITLE 1,[Verification] count
# MAGIC %sql
# MAGIC select count (*) as RecordCount, 'Target' as TableName from cleansed.t_sapisu_ZDM_DS_EDW_PORTION_TEXT
# MAGIC union all
# MAGIC select count (*) as RecordCount, 'Source' as TableName from Source

# COMMAND ----------

# DBTITLE 1,[Duplicate checks]
# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT
# MAGIC *,
# MAGIC row_number() OVER(PARTITION BY Portion order by validFromDate) as rn
# MAGIC FROM  cleansed.t_sapisu_ZDM_DS_EDW_PORTION_TEXT
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
# MAGIC from(SELECT
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
# MAGIC ,row_number() over (partition by portion order by EXTRACT_DATETIME desc) as rn
# MAGIC from source) a where a.rn = 1
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
# MAGIC cleansed.t_sapisu_ZDM_DS_EDW_PORTION_TEXT 

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
# MAGIC cleansed.t_sapisu_ZDM_DS_EDW_PORTION_TEXT
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
# MAGIC from(SELECT
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
# MAGIC ,row_number() over (partition by portion order by EXTRACT_DATETIME desc) as rn
# MAGIC from source) a where a.rn = 1
