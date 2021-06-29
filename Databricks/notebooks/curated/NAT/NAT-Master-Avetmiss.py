# Databricks notebook source
from pyspark.sql.functions import *
from datetime import datetime
from pyspark.sql.window import *
from pyspark.sql.types import *
from dateutil.relativedelta import relativedelta

# COMMAND ----------

# MAGIC %run ../../includes/include-all-util

# COMMAND ----------

# DBTITLE 1,Avetmiss - Functions
# MAGIC %run ./Functions/NAT-Avetmiss

# COMMAND ----------

# DBTITLE 1,Spark Config
# When set to true Spark SQL will automatically select a compression codec for each column based on statistics of the data.
spark.conf.set("spark.sql.inMemoryColumnarStorage.compressed",True)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
# Limit of total size of serialized results of all partitions for each Spark action (e.g. collect) in bytes. Should be at least 1M, or 0 for unlimited. Jobs will be aborted if the total size is above this limit. Having a high limit may cause out-of-memory errors in driver (depends on spark.driver.memory and memory overhead of objects in JVM). Setting a proper limit can protect the driver from out-of-memory errors.
#spark.conf.set("spark.driver.maxResultSize",0)

# COMMAND ----------

# DBTITLE 1,Params for Snapshot
#Get the Current Local Date
date = GeneralLocalDateTime().date()

#Find the last Sunday Date
var_sunday_date_prev = date - timedelta(days = (date.weekday() + 1) %7)
print("Sunday : {0}".format(var_sunday_date_prev))

dfReportingYear = spark.sql("select ReportingYear from trusted.reference_nat_current_period where _RecordCurrent = 1 and _RecordDeleted = 0")
var_reporting_year =  dfReportingYear.select("ReportingYear").collect()[0][0]
print("Reporting Year {0}".format(var_reporting_year))

# COMMAND ----------

# DBTITLE 1,Input Tables
#Compliance Tables #PRADA-1735, PRADA-1739
dfAvetmissCourse = DeltaTableAsCurrent("Compliance.Avetmiss_Course")
dfAvetmissLocation = DeltaTableAsCurrent("Compliance.Avetmiss_Location")
dfAvetmissStudent = DeltaTableAsCurrent("Compliance.Avetmiss_Student")
dfAvetmissDisabilityType = DeltaTableAsCurrent("Compliance.avetmiss_disability_type")
dfAvetmissUnit = DeltaTableAsCurrent("Compliance.Avetmiss_Unit")
dfAvetmissStudentStatusLog = DeltaTableAsCurrent("Compliance.Avetmiss_student_status")


dfAvetmissCourseEnrolment = DeltaTableAsSnapshot("Compliance.Avetmiss_Course_Enrolment", var_reporting_year, var_sunday_date_prev)
dfAvetmissUnitEnrolment = DeltaTableAsSnapshot("Compliance.Avetmiss_Unit_Enrolment", var_reporting_year, var_sunday_date_prev)


#OneEBS Tables
dfAwards = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_awards")
dfConfigurableStatuses = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_configurable_statuses")
dfPeopleASR = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_people_asr")
dfDisabilities = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_disabilities")

#Reference Tables
dfQualificationType = DeltaTableAsCurrent("reference.qualification_type")
dfFundingSource = DeltaTableAsCurrent("reference.funding_source")

#Count Used for Functions
countAvetmissCourseEnrolment = dfAvetmissCourseEnrolment.count()

# COMMAND ----------

# DBTITLE 1,Params
#TODO: CutOffDate IS HARDCODED AND NEEDS TO BE SOURCED FROM TABLE
row = DeltaTableAsCurrent("trusted.reference_nat_current_period").join(DeltaTableAsCurrent("trusted.reference_nat_collection_dates"), expr("trusted.reference_nat_current_period.ReportingYear == trusted.reference_nat_collection_dates.ReportingYear AND trusted.reference_nat_current_period.Quarter == trusted.reference_nat_collection_dates.Quarter")).selectExpr("trusted.reference_nat_current_period.ReportingYear", "CollectionStart", "AvetmissCollectionEnd", "EreportingCollectionEnd", "CAST('2021-02-28' AS DATE) AS CutOffDate").collect()[0]

def FormatNatDate(col):
  return col.strftime("%Y-%m-%d")

#Parameter List
paramsAvt = { "CollectionStartAvt" : FormatNatDate(row.CollectionStart)
             , "CollectionEndAvt" : FormatNatDate(row.AvetmissCollectionEnd)
             , "CutOffDateAvt" : FormatNatDate(row.CutOffDate)
             , "ReportingYearAvt" : row.ReportingYear }

#Print Date Range
print("paramsAvt = {}".format(paramsAvt))


# COMMAND ----------

# DBTITLE 1,NAT 120 - Avetmiss
#Get NAT 120 Avetmiss
print("{} Running NAT 120 Avetmiss".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
countNAT120Avetmiss = 0
if (countAvetmissCourseEnrolment) > 0:
  dfNAT120Avetmiss = GetNAT120Avetmiss(dfAvetmissCourseEnrolment, dfAvetmissLocation, dfAvetmissStudent, dfAvetmissUnit, dfAvetmissUnitEnrolment, dfFundingSource, paramsAvt).cache()
  countNAT120Avetmiss = dfNAT120Avetmiss.count()
  print("{} Completed NAT 120 Avetmiss {} records".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),countNAT120Avetmiss))
else:
  print("{} Skipped NAT 120 Avetmiss {} records".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), countNAT120Avetmiss))

# COMMAND ----------

# DBTITLE 1,NAT 130 - Avetmiss
#Get NAT 130 Avetmiss
print("{} Running NAT 130 Avetmiss".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
countNAT130Avetmiss = 0
if (countAvetmissCourseEnrolment) > 0:
  dfNAT130Avetmiss = GetNAT130Avetmiss(dfAvetmissCourseEnrolment, dfAwards, dfConfigurableStatuses, paramsAvt).cache()
  countNAT130Avetmiss = dfNAT130Avetmiss.count()
  print("{} Completed NAT 130 Avetmiss {} records".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),countNAT130Avetmiss))
else:
  print("{} Skipped NAT 130 Avetmiss {} records".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), countNAT130Avetmiss))

# COMMAND ----------

# DBTITLE 1,NAT 010 - Avetmiss
#Get NAT 010 Avetmiss
print("{} Running NAT 010 Avetmiss".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
countNAT010Avetmiss = 0
if (countNAT120Avetmiss) > 0:
  dfNAT010Avetmiss = GetNAT010Avetmiss(dfNAT120Avetmiss, dfAvetmissLocation)
  countNAT010Avetmiss = dfNAT010Avetmiss.count()
  print("{} Completed NAT 010 Avetmiss {} records".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),countNAT010Avetmiss))
else:
  print("{} Skipped NAT 010 Avetmiss {} records".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), countNAT010Avetmiss))

# COMMAND ----------

# DBTITLE 1,NAT 020 - Avetmiss
#Get NAT 020 Avetmiss
print("{} Running NAT 020 Avetmiss".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
countNAT020Avetmiss = 0
if (countAvetmissCourseEnrolment) > 0:
  dfNAT020Avetmiss = GetNAT020Avetmiss(dfAvetmissLocation, dfNAT120Avetmiss)
  countNAT020Avetmiss = dfNAT020Avetmiss.count()
  print("{} Completed NAT 020 E-Reporting {} records".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),countNAT020Avetmiss))
else:
  print("{} Skipped NAT 020 E-Reporting {} records".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), countNAT020Avetmiss))

# COMMAND ----------

# DBTITLE 1,NAT 030- Avetmiss
#Get NAT 030 Avetmiss
print("{} Running NAT 030 Avetmiss".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
countNAT030Avetmiss = 0
if (countAvetmissCourseEnrolment) > 0:
  dfNAT030Avetmiss = GetNAT030Avetmiss(dfNAT120Avetmiss, dfNAT130Avetmiss, dfAvetmissCourse, dfQualificationType)
  countNAT030Avetmiss = dfNAT030Avetmiss.count()
  print("{} Completed NAT 030 Avetmiss {} records".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),countNAT030Avetmiss))
else:
  print("{} Skipped NAT 030 Avetmiss {} records".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), countNAT030Avetmiss))

# COMMAND ----------

# DBTITLE 1,NAT 060 - Avetmiss
#Get NAT 060 Avetmiss
print("{} Running NAT 060 Avetmiss".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
countNAT060Avetmiss = 0
if (countNAT120Avetmiss) > 0:
  dfNAT060Avetmiss = GetNAT060Avetmiss(dfNAT120Avetmiss,dfAvetmissUnit)
  countNAT060Avetmiss = dfNAT060Avetmiss.count()
  print("{} Completed NAT 060 Avetmiss {} records".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),countNAT060Avetmiss))
else:
  print("{} Skipped NAT 060 Avetmiss {} records".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), countNAT060Avetmiss))

# COMMAND ----------

# DBTITLE 1,NAT 080 - Avetmiss
#Get NAT 080 Avetmiss
print("{} Running NAT 080 Avetmiss".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
countNAT080Avetmiss = 0
if (countNAT120Avetmiss) > 0:
  dfNAT080Avetmiss = GetNAT080Avetmiss(dfNAT120Avetmiss, dfNAT130Avetmiss, dfAvetmissStudent, dfAvetmissDisabilityType, dfPeopleASR,paramsAvt)
  countNAT080Avetmiss = dfNAT080Avetmiss.count()
  print("{} Completed NAT 080 Avetmiss {} records".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),countNAT080Avetmiss))
else:
  print("{} Skipped NAT 080 Avetmiss {} records".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), countNAT080Avetmiss))

# COMMAND ----------

# DBTITLE 1,NAT 085- Avetmiss
#Get NAT 085 Avetmiss
print("{} Running NAT 085 Avetmiss".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
countNAT085Avetmiss = 0
if (countNAT120Avetmiss) > 0:
  dfNAT085Avetmiss = GetNAT085Avetmiss(dfNAT120Avetmiss,dfNAT130Avetmiss,dfAvetmissStudent)
  countNAT085Avetmiss = dfNAT085Avetmiss.count()
  print("{} Completed NAT 085 Avetmiss {} records".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),countNAT085Avetmiss))
else:
  print("{} Skipped NAT 085 Avetmiss {} records".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), countNAT085Avetmiss))

# COMMAND ----------

# DBTITLE 1,NAT 090 - Avetmiss
#Get NAT 090 Avetmiss
print("{} Running NAT 090 Avetmiss".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
countNAT090Avetmiss = 0
if (countNAT120Avetmiss + countNAT130Avetmiss) > 0:
  dfNAT090Avetmiss = GetNAT090Avetmiss(dfNAT120Avetmiss, dfNAT130Avetmiss, dfAvetmissStudent, dfDisabilities, paramsAvt)
  countNAT090Avetmiss = dfNAT090Avetmiss.count()
  print("{} Completed NAT 090 Avetmiss {} records".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),countNAT090Avetmiss))
else:
  print("{} Skipped NAT 090 Avetmiss {} records".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), countNAT090Avetmiss))

# COMMAND ----------

# DBTITLE 1,NAT 100 - Avetmiss
#Get NAT 100 Avetmiss
print("{} Running NAT 100 Avetmiss".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
countNAT100Avetmiss = 0
if (countNAT120Avetmiss + countNAT130Avetmiss) > 0:
  dfNAT100Avetmiss = GetNAT100Avetmiss(dfAvetmissStudent, dfNAT120Avetmiss, dfNAT130Avetmiss)
  countNAT100Avetmiss = dfNAT100Avetmiss.count()
  print("{} Completed NAT 100 Avetmiss {} records".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),countNAT100Avetmiss))
else:
  print("{} Skipped NAT 100 Avetmiss {} records".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), countNAT100Avetmiss))

# COMMAND ----------

# DBTITLE 1,File Params
#Params
date = GeneralLocalDateTime().strftime("%Y%m%d")
container = "curated"
fileFormat = "txt"

# COMMAND ----------

# DBTITLE 1,Write to Datalake - Avetmiss
#Avetmiss
if countNAT120Avetmiss > 0:
  DatalakeWriteFile(dfNAT120Avetmiss.select("Output"), container, "nat/avetmiss/"+date, "nat00120.txt", fileFormat, None, "\t")
  
if countNAT130Avetmiss > 0:
  DatalakeWriteFile(dfNAT130Avetmiss.select("Output"), container, "nat/avetmiss/"+date, "nat00130.txt", fileFormat, None, "\t")
  
if countNAT010Avetmiss > 0:
  DatalakeWriteFile(dfNAT010Avetmiss.select("Output"), container, "nat/avetmiss/"+date, "nat00010.txt", fileFormat, None, "\t")
  
if countNAT020Avetmiss > 0:
  DatalakeWriteFile(dfNAT020Avetmiss.select("Output"), container, "nat/avetmiss/"+date, "nat00020.txt", fileFormat, None, "\t")
  
if countNAT030Avetmiss > 0:
  DatalakeWriteFile(dfNAT030Avetmiss.select("Output"), container, "nat/avetmiss/"+date, "nat00030.txt", fileFormat, None, "\t")
  
if countNAT060Avetmiss > 0:
  DatalakeWriteFile(dfNAT060Avetmiss.select("Output"), container, "nat/avetmiss/"+date, "nat00060.txt", fileFormat, None, "\t")
  
if countNAT080Avetmiss > 0:
  DatalakeWriteFile(dfNAT080Avetmiss.select("Output"), container, "nat/avetmiss/"+date, "nat00080.txt", fileFormat, None, "\t")
  
if countNAT085Avetmiss > 0:
  DatalakeWriteFile(dfNAT085Avetmiss.select("Output"), container, "nat/avetmiss/"+date, "nat00085.txt", fileFormat, None, "\t")
  
if countNAT090Avetmiss > 0:
  DatalakeWriteFile(dfNAT090Avetmiss.select("Output"), container, "nat/avetmiss/"+date, "nat00090.txt", fileFormat, None, "\t")
  
if countNAT100Avetmiss > 0:
  DatalakeWriteFile(dfNAT100Avetmiss.select("Output"), container, "nat/avetmiss/"+date, "nat00100.txt", fileFormat, None, "\t")

# COMMAND ----------

Output = str(date)
print(Output)
dbutils.notebook.exit(Output)