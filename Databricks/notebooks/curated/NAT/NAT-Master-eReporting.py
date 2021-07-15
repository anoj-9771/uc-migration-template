# Databricks notebook source
from pyspark.sql.functions import *
from datetime import datetime
from pyspark.sql.window import *
from pyspark.sql.types import *
from dateutil.relativedelta import relativedelta

# COMMAND ----------

# MAGIC %run ../../includes/include-all-util

# COMMAND ----------

# DBTITLE 1,E-Reporting - Functions
# MAGIC %run ./Functions/NAT-E-Reporting

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



# COMMAND ----------

# DBTITLE 1,Params
#TODO: CutOffDate IS HARDCODED AND NEEDS TO BE SOURCED FROM TABLE
row = DeltaTableAsCurrent("trusted.reference_nat_current_period").join(DeltaTableAsCurrent("trusted.reference_nat_collection_dates"), expr("trusted.reference_nat_current_period.ReportingYear == trusted.reference_nat_collection_dates.ReportingYear AND trusted.reference_nat_current_period.Quarter == trusted.reference_nat_collection_dates.Quarter")).selectExpr("trusted.reference_nat_current_period.ReportingYear", "CollectionStart", "AvetmissCollectionEnd", "EreportingCollectionEnd", "CAST('2021-02-28' AS DATE) AS CutOffDate").collect()[0]

def FormatNatDate(col):
  return col.strftime("%Y-%m-%d")

#Parameter List
#ALWAYS SET E-REPORTING TO 2015 START AND TODAYS END OF YEAR
currentYear = GeneralLocalDateTime().year
paramsERpt = { "CollectionStartERpt" : "2015-01-01" 
              , "CollectionEndERpt" : f"{currentYear}-12-31"
              , "ReportingYearERpt" : f"CY{currentYear}" }

#Print Date Range
print("paramsERpt = {}".format(paramsERpt))

# COMMAND ----------

# DBTITLE 1,NAT Base - E-Reporting
#Get Ereporting Course list
print("{} Running NAT Base E-Reporting".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
countNATBaseERpt = 0
dfNATBase = GetNatBaseErpt(paramsERpt).cache()
countNATBaseERpt = dfNATBase.count()
if (countNATBaseERpt) > 0:
  print("{} Completed NAT Base eReporting {} records".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),countNATBaseERpt))
else:
  print("{} Skipped NAT Base eReporting {} records".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), countNATBaseERpt))

# COMMAND ----------

# DBTITLE 1,NAT 120 - E-Reporting
#Get NAT 120 E-Reporting
print("{} Running NAT 120 E-Reporting".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
countNAT120ERptAll = 0
dfNAT120ERptAll = GetNAT120ERpt(dfNATBase,paramsERpt,None).cache()
dfNAT120ERpt90000 = GetNAT120ERpt(dfNATBase, paramsERpt, 90000).cache()
dfNAT120ERpt90001 = GetNAT120ERpt(dfNATBase, paramsERpt, 90001).cache()
dfNAT120ERpt90002 = GetNAT120ERpt(dfNATBase, paramsERpt, 90002).cache()
dfNAT120ERpt90003 = GetNAT120ERpt(dfNATBase, paramsERpt, 90003).cache()
dfNAT120ERpt90004 = GetNAT120ERpt(dfNATBase, paramsERpt, 90004).cache()
dfNAT120ERpt90005 = GetNAT120ERpt(dfNATBase, paramsERpt, 90005).cache()
dfNAT120ERpt90006 = GetNAT120ERpt(dfNATBase, paramsERpt, 90006).cache()
dfNAT120ERpt90008 = GetNAT120ERpt(dfNATBase, paramsERpt, 90008).cache()
dfNAT120ERpt90009 = GetNAT120ERpt(dfNATBase, paramsERpt, 90009).cache()
dfNAT120ERpt90010 = GetNAT120ERpt(dfNATBase, paramsERpt, 90010).cache()
dfNAT120ERpt90011 = GetNAT120ERpt(dfNATBase, paramsERpt, 90011).cache()

# COMMAND ----------

# DBTITLE 1,NAT 130 - E-Reporting
#Get NAT 130 E-Reporting
print("{} Running NAT 130 E-Reporting".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
countNAT130ERptAll = 0
dfNAT130ERptAll = GetNAT130ERpt(dfNATBase, None, paramsERpt)
dfNAT130ERpt90000 = GetNAT130ERpt(dfNATBase, 90000, paramsERpt)
dfNAT130ERpt90001 = GetNAT130ERpt(dfNATBase, 90001, paramsERpt)
dfNAT130ERpt90002 = GetNAT130ERpt(dfNATBase, 90002, paramsERpt)
dfNAT130ERpt90003 = GetNAT130ERpt(dfNATBase, 90003, paramsERpt)
dfNAT130ERpt90004 = GetNAT130ERpt(dfNATBase, 90004, paramsERpt)
dfNAT130ERpt90005 = GetNAT130ERpt(dfNATBase, 90005, paramsERpt)
dfNAT130ERpt90006 = GetNAT130ERpt(dfNATBase, 90006, paramsERpt)
dfNAT130ERpt90008 = GetNAT130ERpt(dfNATBase, 90008, paramsERpt)
dfNAT130ERpt90009 = GetNAT130ERpt(dfNATBase, 90009, paramsERpt)
dfNAT130ERpt90010 = GetNAT130ERpt(dfNATBase, 90010, paramsERpt)
dfNAT130ERpt90011 = GetNAT130ERpt(dfNATBase, 90011, paramsERpt)

# COMMAND ----------

# DBTITLE 1,NAT 010 - E-Reporting
#Get NAT 010 E-Reporting
print("{} Running NAT 010 E-Reporting".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
countNAT010ERptAll = 0
dfNAT010ERptAll = GetNAT010ERpt(dfAvetmissLocation, dfNAT120ERptAll,None)
dfNAT010ERpt90000 = GetNAT010ERpt(dfAvetmissLocation, dfNAT120ERpt90000,90000)
dfNAT010ERpt90001 = GetNAT010ERpt(dfAvetmissLocation, dfNAT120ERpt90001,90001)
dfNAT010ERpt90002 = GetNAT010ERpt(dfAvetmissLocation, dfNAT120ERpt90002,90002)
dfNAT010ERpt90003 = GetNAT010ERpt(dfAvetmissLocation, dfNAT120ERpt90003,90003)
dfNAT010ERpt90004 = GetNAT010ERpt(dfAvetmissLocation, dfNAT120ERpt90004,90004)
dfNAT010ERpt90005 = GetNAT010ERpt(dfAvetmissLocation, dfNAT120ERpt90005,90005)
dfNAT010ERpt90006 = GetNAT010ERpt(dfAvetmissLocation, dfNAT120ERpt90006,90006)
dfNAT010ERpt90008 = GetNAT010ERpt(dfAvetmissLocation, dfNAT120ERpt90008,90008)
dfNAT010ERpt90009 = GetNAT010ERpt(dfAvetmissLocation, dfNAT120ERpt90009,90009)
dfNAT010ERpt90010 = GetNAT010ERpt(dfAvetmissLocation, dfNAT120ERpt90010,90010)
dfNAT010ERpt90011 = GetNAT010ERpt(dfAvetmissLocation, dfNAT120ERpt90011,90011)

# COMMAND ----------

# DBTITLE 1,NAT 020 - E-Reporting
#Get NAT 020 E-Reporting
print("{} Running NAT 020 E-Reporting".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
countNAT020ERptAll = 0
dfNAT020ERptAll = GetNAT020ERpt(dfAvetmissLocation,  None, dfNAT120ERptAll)
dfNAT020ERpt90000 = GetNAT020ERpt(dfAvetmissLocation, 90000, dfNAT120ERpt90000)
dfNAT020ERpt90001 = GetNAT020ERpt(dfAvetmissLocation, 90001, dfNAT120ERpt90001)
dfNAT020ERpt90002 = GetNAT020ERpt(dfAvetmissLocation, 90002, dfNAT120ERpt90002)
dfNAT020ERpt90003 = GetNAT020ERpt(dfAvetmissLocation, 90003, dfNAT120ERpt90003)
dfNAT020ERpt90004 = GetNAT020ERpt(dfAvetmissLocation, 90004, dfNAT120ERpt90004)
dfNAT020ERpt90005 = GetNAT020ERpt(dfAvetmissLocation, 90005, dfNAT120ERpt90005)
dfNAT020ERpt90006 = GetNAT020ERpt(dfAvetmissLocation, 90006, dfNAT120ERpt90006)
dfNAT020ERpt90008 = GetNAT020ERpt(dfAvetmissLocation, 90008, dfNAT120ERpt90008)
dfNAT020ERpt90009 = GetNAT020ERpt(dfAvetmissLocation, 90009, dfNAT120ERpt90009)
dfNAT020ERpt90010 = GetNAT020ERpt(dfAvetmissLocation, 90010, dfNAT120ERpt90010)
dfNAT020ERpt90011 = GetNAT020ERpt(dfAvetmissLocation, 90011, dfNAT120ERpt90011)

# COMMAND ----------

# DBTITLE 1,NAT 030 - E-Reporting
#Get NAT 030 E-Reporting
print("{} Running NAT 030 E-Reporting".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
countNAT030ERptAll = 0
dfNAT030ERptAll = GetNAT030ERpt(dfAvetmissCourse, None, dfNAT120ERptAll, dfNAT130ERptAll)
dfNAT030ERpt90000 = GetNAT030ERpt(dfAvetmissCourse, 90000, dfNAT120ERpt90000, dfNAT130ERpt90000)
dfNAT030ERpt90001 = GetNAT030ERpt(dfAvetmissCourse, 90001, dfNAT120ERpt90001, dfNAT130ERpt90001)
dfNAT030ERpt90002 = GetNAT030ERpt(dfAvetmissCourse, 90002, dfNAT120ERpt90002, dfNAT130ERpt90002)
dfNAT030ERpt90003 = GetNAT030ERpt(dfAvetmissCourse, 90003, dfNAT120ERpt90003, dfNAT130ERpt90003)
dfNAT030ERpt90004 = GetNAT030ERpt(dfAvetmissCourse, 90004, dfNAT120ERpt90004, dfNAT130ERpt90004)
dfNAT030ERpt90005 = GetNAT030ERpt(dfAvetmissCourse, 90005, dfNAT120ERpt90005, dfNAT130ERpt90005)
dfNAT030ERpt90006 = GetNAT030ERpt(dfAvetmissCourse, 90006, dfNAT120ERpt90006, dfNAT130ERpt90006)
dfNAT030ERpt90008 = GetNAT030ERpt(dfAvetmissCourse, 90008, dfNAT120ERpt90008, dfNAT130ERpt90008)
dfNAT030ERpt90009 = GetNAT030ERpt(dfAvetmissCourse, 90009, dfNAT120ERpt90009, dfNAT130ERpt90009)
dfNAT030ERpt90010 = GetNAT030ERpt(dfAvetmissCourse, 90010, dfNAT120ERpt90010, dfNAT130ERpt90010)
dfNAT030ERpt90011 = GetNAT030ERpt(dfAvetmissCourse, 90011, dfNAT120ERpt90011, dfNAT130ERpt90011)

# COMMAND ----------

# DBTITLE 1,NAT 030A - E-Reporting
#Get NAT 030A E-Reporting
print("{} Running NAT 030A E-Reporting".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
countNAT030AERptAll = 0
dfNAT030AERptAll = GetNAT030AERpt(dfAvetmissCourse, None, dfNAT120ERptAll, dfNAT130ERptAll, dfQualificationType)
dfNAT030AERpt90000 = GetNAT030AERpt(dfAvetmissCourse, 90000, dfNAT120ERpt90000, dfNAT130ERpt90000, dfQualificationType)
dfNAT030AERpt90001 = GetNAT030AERpt(dfAvetmissCourse, 90001, dfNAT120ERpt90001, dfNAT130ERpt90001, dfQualificationType)
dfNAT030AERpt90002 = GetNAT030AERpt(dfAvetmissCourse, 90002, dfNAT120ERpt90002, dfNAT130ERpt90002, dfQualificationType)
dfNAT030AERpt90003 = GetNAT030AERpt(dfAvetmissCourse, 90003, dfNAT120ERpt90003, dfNAT130ERpt90003, dfQualificationType)
dfNAT030AERpt90004 = GetNAT030AERpt(dfAvetmissCourse, 90004, dfNAT120ERpt90004, dfNAT130ERpt90004, dfQualificationType)
dfNAT030AERpt90005 = GetNAT030AERpt(dfAvetmissCourse, 90005, dfNAT120ERpt90005, dfNAT130ERpt90005, dfQualificationType)
dfNAT030AERpt90006 = GetNAT030AERpt(dfAvetmissCourse, 90006, dfNAT120ERpt90006, dfNAT130ERpt90006, dfQualificationType)
dfNAT030AERpt90008 = GetNAT030AERpt(dfAvetmissCourse, 90008, dfNAT120ERpt90008, dfNAT130ERpt90008, dfQualificationType)
dfNAT030AERpt90009 = GetNAT030AERpt(dfAvetmissCourse, 90009, dfNAT120ERpt90009, dfNAT130ERpt90009, dfQualificationType)
dfNAT030AERpt90010 = GetNAT030AERpt(dfAvetmissCourse, 90010, dfNAT120ERpt90010, dfNAT130ERpt90010, dfQualificationType)
dfNAT030AERpt90011 = GetNAT030AERpt(dfAvetmissCourse, 90011, dfNAT120ERpt90011, dfNAT130ERpt90011, dfQualificationType)

# COMMAND ----------

# DBTITLE 1,NAT 060 - E-Reporting
#Get NAT 060 E-Reporting
print("{} Running NAT 060 E-Reporting".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
countNAT060ERptAll = 0
# if (countNAT120ERptAll) > 0:
  #Full Load
dfNAT060ERptAll = GetNAT060ERpt(dfNAT120ERptAll, dfAvetmissUnit)
dfNAT060ERpt90000 = GetNAT060ERpt(dfNAT120ERpt90000, dfAvetmissUnit)
dfNAT060ERpt90001 = GetNAT060ERpt(dfNAT120ERpt90001, dfAvetmissUnit)
dfNAT060ERpt90002 = GetNAT060ERpt(dfNAT120ERpt90002, dfAvetmissUnit)
dfNAT060ERpt90003 = GetNAT060ERpt(dfNAT120ERpt90003, dfAvetmissUnit)
dfNAT060ERpt90004 = GetNAT060ERpt(dfNAT120ERpt90004, dfAvetmissUnit)
dfNAT060ERpt90005 = GetNAT060ERpt(dfNAT120ERpt90005, dfAvetmissUnit)
dfNAT060ERpt90006 = GetNAT060ERpt(dfNAT120ERpt90006, dfAvetmissUnit)
dfNAT060ERpt90008 = GetNAT060ERpt(dfNAT120ERpt90008, dfAvetmissUnit)
dfNAT060ERpt90009 = GetNAT060ERpt(dfNAT120ERpt90009, dfAvetmissUnit)
dfNAT060ERpt90010 = GetNAT060ERpt(dfNAT120ERpt90010, dfAvetmissUnit)
dfNAT060ERpt90011 = GetNAT060ERpt(dfNAT120ERpt90011, dfAvetmissUnit)

# COMMAND ----------

# DBTITLE 1,NAT 080 - E-Reporting
#Get NAT 080 E-Reporting
print("{} Running NAT 080 E-Reporting".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
countNAT080ERptAll = 0
dfNAT080ERptAll = GetNAT080ERpt(dfNAT120ERptAll,dfAvetmissStudent,dfAvetmissStudentStatusLog,dfDisabilities, paramsERpt)
dfNAT080ERpt90000 = GetNAT080ERpt(dfNAT120ERpt90000, dfAvetmissStudent, dfAvetmissStudentStatusLog, dfDisabilities, paramsERpt)
dfNAT080ERpt90001 = GetNAT080ERpt(dfNAT120ERpt90001, dfAvetmissStudent, dfAvetmissStudentStatusLog, dfDisabilities, paramsERpt)
dfNAT080ERpt90002 = GetNAT080ERpt(dfNAT120ERpt90002, dfAvetmissStudent, dfAvetmissStudentStatusLog, dfDisabilities, paramsERpt)
dfNAT080ERpt90003 = GetNAT080ERpt(dfNAT120ERpt90003, dfAvetmissStudent, dfAvetmissStudentStatusLog, dfDisabilities, paramsERpt)
dfNAT080ERpt90004 = GetNAT080ERpt(dfNAT120ERpt90004, dfAvetmissStudent, dfAvetmissStudentStatusLog, dfDisabilities, paramsERpt)
dfNAT080ERpt90005 = GetNAT080ERpt(dfNAT120ERpt90005, dfAvetmissStudent, dfAvetmissStudentStatusLog, dfDisabilities, paramsERpt)
dfNAT080ERpt90006 = GetNAT080ERpt(dfNAT120ERpt90006, dfAvetmissStudent, dfAvetmissStudentStatusLog, dfDisabilities, paramsERpt)
dfNAT080ERpt90008 = GetNAT080ERpt(dfNAT120ERpt90008, dfAvetmissStudent, dfAvetmissStudentStatusLog, dfDisabilities, paramsERpt)
dfNAT080ERpt90009 = GetNAT080ERpt(dfNAT120ERpt90009, dfAvetmissStudent, dfAvetmissStudentStatusLog, dfDisabilities, paramsERpt)
dfNAT080ERpt90010 = GetNAT080ERpt(dfNAT120ERpt90010, dfAvetmissStudent, dfAvetmissStudentStatusLog, dfDisabilities, paramsERpt)
dfNAT080ERpt90011 = GetNAT080ERpt(dfNAT120ERpt90011, dfAvetmissStudent, dfAvetmissStudentStatusLog, dfDisabilities, paramsERpt)

# COMMAND ----------

# DBTITLE 1,NAT 085 - E-Reporting
#Get NAT 085 E-Reporting
print("{} Running NAT 085 E-Reporting".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
countNAT085ERptAll = 0
dfNAT085ERptAll = GetNAT085ERpt(dfNAT120ERptAll,dfAvetmissStudent)
dfNAT085ERpt90000 = GetNAT085ERpt(dfNAT120ERpt90000, dfAvetmissStudent)
dfNAT085ERpt90001 = GetNAT085ERpt(dfNAT120ERpt90001, dfAvetmissStudent)
dfNAT085ERpt90002 = GetNAT085ERpt(dfNAT120ERpt90002, dfAvetmissStudent)
dfNAT085ERpt90003 = GetNAT085ERpt(dfNAT120ERpt90003, dfAvetmissStudent)
dfNAT085ERpt90004 = GetNAT085ERpt(dfNAT120ERpt90004, dfAvetmissStudent)
dfNAT085ERpt90005 = GetNAT085ERpt(dfNAT120ERpt90005, dfAvetmissStudent)
dfNAT085ERpt90006 = GetNAT085ERpt(dfNAT120ERpt90006, dfAvetmissStudent)
dfNAT085ERpt90008 = GetNAT085ERpt(dfNAT120ERpt90008, dfAvetmissStudent)
dfNAT085ERpt90009 = GetNAT085ERpt(dfNAT120ERpt90009, dfAvetmissStudent)
dfNAT085ERpt90010 = GetNAT085ERpt(dfNAT120ERpt90010, dfAvetmissStudent)
dfNAT085ERpt90011 = GetNAT085ERpt(dfNAT120ERpt90011, dfAvetmissStudent)

# COMMAND ----------

# DBTITLE 1,NAT 090 - E-Reporting
#Get NAT 090 E-Reporting
print("{} Running NAT 090 E-Reporting".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
countNAT090ERptAll = 0
dfNAT090ERptAll = GetNAT090ERpt(dfNAT120ERptAll, dfAvetmissStudent, dfAvetmissStudentStatusLog, dfDisabilities, paramsERpt)
dfNAT090ERpt90000 = GetNAT090ERpt(dfNAT120ERpt90000, dfAvetmissStudent, dfAvetmissStudentStatusLog, dfDisabilities, paramsERpt)
dfNAT090ERpt90001 = GetNAT090ERpt(dfNAT120ERpt90001, dfAvetmissStudent, dfAvetmissStudentStatusLog, dfDisabilities, paramsERpt)
dfNAT090ERpt90002 = GetNAT090ERpt(dfNAT120ERpt90002, dfAvetmissStudent, dfAvetmissStudentStatusLog, dfDisabilities, paramsERpt)
dfNAT090ERpt90003 = GetNAT090ERpt(dfNAT120ERpt90003, dfAvetmissStudent, dfAvetmissStudentStatusLog, dfDisabilities, paramsERpt)
dfNAT090ERpt90004 = GetNAT090ERpt(dfNAT120ERpt90004, dfAvetmissStudent, dfAvetmissStudentStatusLog, dfDisabilities, paramsERpt)
dfNAT090ERpt90005 = GetNAT090ERpt(dfNAT120ERpt90005, dfAvetmissStudent, dfAvetmissStudentStatusLog, dfDisabilities, paramsERpt)
dfNAT090ERpt90006 = GetNAT090ERpt(dfNAT120ERpt90006, dfAvetmissStudent, dfAvetmissStudentStatusLog, dfDisabilities, paramsERpt)
dfNAT090ERpt90008 = GetNAT090ERpt(dfNAT120ERpt90008, dfAvetmissStudent, dfAvetmissStudentStatusLog, dfDisabilities, paramsERpt)
dfNAT090ERpt90009 = GetNAT090ERpt(dfNAT120ERpt90009, dfAvetmissStudent, dfAvetmissStudentStatusLog, dfDisabilities, paramsERpt)
dfNAT090ERpt90010 = GetNAT090ERpt(dfNAT120ERpt90010, dfAvetmissStudent, dfAvetmissStudentStatusLog, dfDisabilities, paramsERpt)
dfNAT090ERpt90011 = GetNAT090ERpt(dfNAT120ERpt90011, dfAvetmissStudent, dfAvetmissStudentStatusLog, dfDisabilities, paramsERpt)

# COMMAND ----------

# DBTITLE 1,NAT 100 - E-Reporting
#Get NAT 100 E-Reporting
print("{} Running NAT 100 E-Reporting".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
countNAT100ERptAll = 0
dfNAT100ERptAll = GetNAT100ERpt(dfAvetmissStudent, None, dfNAT120ERptAll)
dfNAT100ERpt90000 = GetNAT100ERpt(dfAvetmissStudent, 90000, dfNAT120ERpt90000)
dfNAT100ERpt90001 = GetNAT100ERpt(dfAvetmissStudent, 90001, dfNAT120ERpt90001)
dfNAT100ERpt90002 = GetNAT100ERpt(dfAvetmissStudent, 90002, dfNAT120ERpt90002)
dfNAT100ERpt90003 = GetNAT100ERpt(dfAvetmissStudent, 90003, dfNAT120ERpt90003)
dfNAT100ERpt90004 = GetNAT100ERpt(dfAvetmissStudent, 90004, dfNAT120ERpt90004)
dfNAT100ERpt90005 = GetNAT100ERpt(dfAvetmissStudent, 90005, dfNAT120ERpt90005)
dfNAT100ERpt90006 = GetNAT100ERpt(dfAvetmissStudent, 90006, dfNAT120ERpt90006)
dfNAT100ERpt90008 = GetNAT100ERpt(dfAvetmissStudent, 90008, dfNAT120ERpt90008)
dfNAT100ERpt90009 = GetNAT100ERpt(dfAvetmissStudent, 90009, dfNAT120ERpt90009)
dfNAT100ERpt90010 = GetNAT100ERpt(dfAvetmissStudent, 90010, dfNAT120ERpt90010)
dfNAT100ERpt90011 = GetNAT100ERpt(dfAvetmissStudent, 90011, dfNAT120ERpt90011)

# COMMAND ----------

# DBTITLE 1,File Params
#Params
date = GeneralLocalDateTime().strftime("%Y%m%d")
container = "curated"
fileFormat = "txt"

# COMMAND ----------

# DBTITLE 1,Write to Datalake - E-Reporting
# if countNAT120ERptAll > 0:
#All
DatalakeWriteFile(dfNAT120ERptAll.select("Output"), container, "nat/e-reporting/"+date+"/all", "nat00120.txt", fileFormat, None, "\t")
DatalakeWriteFile(GetNAT120ERpt(dfNATBase,paramsERpt,None).select("Output"), container, "nat/e-reporting/"+date+"/all", "nat00120a.txt", fileFormat, None, "\t")

#Institutes
DatalakeWriteFile(dfNAT120ERpt90000.select("Output"), container, "nat/e-reporting/"+date+"/wsi" , "nat00120.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT120ERpt90001.select("Output"), container, "nat/e-reporting/"+date+"/nei" , "nat00120.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT120ERpt90002.select("Output"), container, "nat/e-reporting/"+date+"/hi"  , "nat00120.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT120ERpt90003.select("Output"), container, "nat/e-reporting/"+date+"/si"  , "nat00120.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT120ERpt90004.select("Output"), container, "nat/e-reporting/"+date+"/oten", "nat00120.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT120ERpt90005.select("Output"), container, "nat/e-reporting/"+date+"/ri"  , "nat00120.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT120ERpt90006.select("Output"), container, "nat/e-reporting/"+date+"/ii"  , "nat00120.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT120ERpt90008.select("Output"), container, "nat/e-reporting/"+date+"/swsi", "nat00120.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT120ERpt90009.select("Output"), container, "nat/e-reporting/"+date+"/wi"  , "nat00120.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT120ERpt90010.select("Output"), container, "nat/e-reporting/"+date+"/nci" , "nat00120.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT120ERpt90011.select("Output"), container, "nat/e-reporting/"+date+"/nsi" , "nat00120.txt", fileFormat, None, "\t")

# if countNAT130ERptAll > 0:
#All
DatalakeWriteFile(dfNAT130ERptAll.select("Output"), container, "nat/e-reporting/"+date+"/all", "nat00130.txt", fileFormat, None, "\t")

#Institutes
DatalakeWriteFile(dfNAT130ERpt90000.select("Output"), container, "nat/e-reporting/"+date+"/wsi" , "nat00130.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT130ERpt90001.select("Output"), container, "nat/e-reporting/"+date+"/nei" , "nat00130.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT130ERpt90002.select("Output"), container, "nat/e-reporting/"+date+"/hi"  , "nat00130.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT130ERpt90003.select("Output"), container, "nat/e-reporting/"+date+"/si"  , "nat00130.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT130ERpt90004.select("Output"), container, "nat/e-reporting/"+date+"/oten", "nat00130.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT130ERpt90005.select("Output"), container, "nat/e-reporting/"+date+"/ri"  , "nat00130.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT130ERpt90006.select("Output"), container, "nat/e-reporting/"+date+"/ii"  , "nat00130.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT130ERpt90008.select("Output"), container, "nat/e-reporting/"+date+"/swsi", "nat00130.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT130ERpt90009.select("Output"), container, "nat/e-reporting/"+date+"/wi"  , "nat00130.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT130ERpt90010.select("Output"), container, "nat/e-reporting/"+date+"/nci" , "nat00130.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT130ERpt90011.select("Output"), container, "nat/e-reporting/"+date+"/nsi" , "nat00130.txt", fileFormat, None, "\t")

# if countNAT010ERptAll > 0:
#All
DatalakeWriteFile(dfNAT010ERptAll.select("Output"), container, "nat/e-reporting/"+date+"/all", "nat00010.txt", fileFormat, None, "\t")

#Institutes
DatalakeWriteFile(dfNAT010ERpt90000.select("Output"), container, "nat/e-reporting/"+date+"/wsi" , "nat00010.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT010ERpt90001.select("Output"), container, "nat/e-reporting/"+date+"/nei" , "nat00010.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT010ERpt90002.select("Output"), container, "nat/e-reporting/"+date+"/hi"  , "nat00010.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT010ERpt90003.select("Output"), container, "nat/e-reporting/"+date+"/si"  , "nat00010.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT010ERpt90004.select("Output"), container, "nat/e-reporting/"+date+"/oten", "nat00010.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT010ERpt90005.select("Output"), container, "nat/e-reporting/"+date+"/ri"  , "nat00010.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT010ERpt90006.select("Output"), container, "nat/e-reporting/"+date+"/ii"  , "nat00010.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT010ERpt90008.select("Output"), container, "nat/e-reporting/"+date+"/swsi", "nat00010.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT010ERpt90009.select("Output"), container, "nat/e-reporting/"+date+"/wi"  , "nat00010.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT010ERpt90010.select("Output"), container, "nat/e-reporting/"+date+"/nci" , "nat00010.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT010ERpt90011.select("Output"), container, "nat/e-reporting/"+date+"/nsi" , "nat00010.txt", fileFormat, None, "\t")

# if countNAT020ERptAll > 0:
#All
DatalakeWriteFile(dfNAT020ERptAll.select("Output"), container, "nat/e-reporting/"+date+"/all", "nat00020.txt", fileFormat, None, "\t")

#Institutes
DatalakeWriteFile(dfNAT020ERpt90000.select("Output"), container, "nat/e-reporting/"+date+"/wsi" , "nat00020.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT020ERpt90001.select("Output"), container, "nat/e-reporting/"+date+"/nei" , "nat00020.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT020ERpt90002.select("Output"), container, "nat/e-reporting/"+date+"/hi"  , "nat00020.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT020ERpt90003.select("Output"), container, "nat/e-reporting/"+date+"/si"  , "nat00020.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT020ERpt90004.select("Output"), container, "nat/e-reporting/"+date+"/oten", "nat00020.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT020ERpt90005.select("Output"), container, "nat/e-reporting/"+date+"/ri"  , "nat00020.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT020ERpt90006.select("Output"), container, "nat/e-reporting/"+date+"/ii"  , "nat00020.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT020ERpt90008.select("Output"), container, "nat/e-reporting/"+date+"/swsi", "nat00020.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT020ERpt90009.select("Output"), container, "nat/e-reporting/"+date+"/wi"  , "nat00020.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT020ERpt90010.select("Output"), container, "nat/e-reporting/"+date+"/nci" , "nat00020.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT020ERpt90011.select("Output"), container, "nat/e-reporting/"+date+"/nsi" , "nat00020.txt", fileFormat, None, "\t")

# if countNAT030ERptAll > 0:
#All
DatalakeWriteFile(dfNAT030ERptAll.select("Output"), container, "nat/e-reporting/"+date+"/all", "nat00030.txt", fileFormat, None, "\t")

#Institutes
DatalakeWriteFile(dfNAT030ERpt90000.select("Output"), container, "nat/e-reporting/"+date+"/wsi" , "nat00030.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT030ERpt90001.select("Output"), container, "nat/e-reporting/"+date+"/nei" , "nat00030.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT030ERpt90002.select("Output"), container, "nat/e-reporting/"+date+"/hi"  , "nat00030.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT030ERpt90003.select("Output"), container, "nat/e-reporting/"+date+"/si"  , "nat00030.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT030ERpt90004.select("Output"), container, "nat/e-reporting/"+date+"/oten", "nat00030.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT030ERpt90005.select("Output"), container, "nat/e-reporting/"+date+"/ri"  , "nat00030.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT030ERpt90006.select("Output"), container, "nat/e-reporting/"+date+"/ii"  , "nat00030.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT030ERpt90008.select("Output"), container, "nat/e-reporting/"+date+"/swsi", "nat00030.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT030ERpt90009.select("Output"), container, "nat/e-reporting/"+date+"/wi"  , "nat00030.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT030ERpt90010.select("Output"), container, "nat/e-reporting/"+date+"/nci" , "nat00030.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT030ERpt90011.select("Output"), container, "nat/e-reporting/"+date+"/nsi" , "nat00030.txt", fileFormat, None, "\t")

# if countNAT030AERptAll > 0:
#All
DatalakeWriteFile(dfNAT030AERptAll.select("Output"), container, "nat/e-reporting/"+date+"/all", "nat00030a.txt", fileFormat, None, "\t")

#Institutes
DatalakeWriteFile(dfNAT030ERpt90000.select("Output"), container, "nat/e-reporting/"+date+"/wsi" , "nat00030a.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT030ERpt90001.select("Output"), container, "nat/e-reporting/"+date+"/nei" , "nat00030a.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT030ERpt90002.select("Output"), container, "nat/e-reporting/"+date+"/hi"  , "nat00030a.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT030ERpt90003.select("Output"), container, "nat/e-reporting/"+date+"/si"  , "nat00030a.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT030ERpt90004.select("Output"), container, "nat/e-reporting/"+date+"/oten", "nat00030a.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT030ERpt90005.select("Output"), container, "nat/e-reporting/"+date+"/ri"  , "nat00030a.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT030ERpt90006.select("Output"), container, "nat/e-reporting/"+date+"/ii"  , "nat00030a.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT030ERpt90008.select("Output"), container, "nat/e-reporting/"+date+"/swsi", "nat00030a.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT030ERpt90009.select("Output"), container, "nat/e-reporting/"+date+"/wi"  , "nat00030a.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT030ERpt90010.select("Output"), container, "nat/e-reporting/"+date+"/nci" , "nat00030a.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT030ERpt90011.select("Output"), container, "nat/e-reporting/"+date+"/nsi" , "nat00030a.txt", fileFormat, None, "\t")

# if countNAT060ERptAll > 0:
#All
DatalakeWriteFile(dfNAT060ERptAll.select("Output"), container, "nat/e-reporting/"+date+"/all", "nat00060.txt", fileFormat, None, "\t")

#Institutes
DatalakeWriteFile(dfNAT060ERpt90000.select("Output"), container, "nat/e-reporting/"+date+"/wsi" , "nat00060.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT060ERpt90001.select("Output"), container, "nat/e-reporting/"+date+"/nei" , "nat00060.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT060ERpt90002.select("Output"), container, "nat/e-reporting/"+date+"/hi"  , "nat00060.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT060ERpt90003.select("Output"), container, "nat/e-reporting/"+date+"/si"  , "nat00060.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT060ERpt90004.select("Output"), container, "nat/e-reporting/"+date+"/oten", "nat00060.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT060ERpt90005.select("Output"), container, "nat/e-reporting/"+date+"/ri"  , "nat00060.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT060ERpt90006.select("Output"), container, "nat/e-reporting/"+date+"/ii"  , "nat00060.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT060ERpt90008.select("Output"), container, "nat/e-reporting/"+date+"/swsi", "nat00060.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT060ERpt90009.select("Output"), container, "nat/e-reporting/"+date+"/wi"  , "nat00060.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT060ERpt90010.select("Output"), container, "nat/e-reporting/"+date+"/nci" , "nat00060.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT060ERpt90011.select("Output"), container, "nat/e-reporting/"+date+"/nsi" , "nat00060.txt", fileFormat, None, "\t")

# if countNAT080ERptAll > 0:
#All
DatalakeWriteFile(dfNAT080ERptAll.select("Output"), container, "nat/e-reporting/"+date+"/all", "nat00080.txt", fileFormat, None, "\t")

#Institutes
DatalakeWriteFile(dfNAT080ERpt90000.select("Output"), container, "nat/e-reporting/"+date+"/wsi" , "nat00080.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT080ERpt90001.select("Output"), container, "nat/e-reporting/"+date+"/nei" , "nat00080.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT080ERpt90002.select("Output"), container, "nat/e-reporting/"+date+"/hi"  , "nat00080.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT080ERpt90003.select("Output"), container, "nat/e-reporting/"+date+"/si"  , "nat00080.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT080ERpt90004.select("Output"), container, "nat/e-reporting/"+date+"/oten", "nat00080.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT080ERpt90005.select("Output"), container, "nat/e-reporting/"+date+"/ri"  , "nat00080.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT080ERpt90006.select("Output"), container, "nat/e-reporting/"+date+"/ii"  , "nat00080.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT080ERpt90008.select("Output"), container, "nat/e-reporting/"+date+"/swsi", "nat00080.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT080ERpt90009.select("Output"), container, "nat/e-reporting/"+date+"/wi"  , "nat00080.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT080ERpt90010.select("Output"), container, "nat/e-reporting/"+date+"/nci" , "nat00080.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT080ERpt90011.select("Output"), container, "nat/e-reporting/"+date+"/nsi" , "nat00080.txt", fileFormat, None, "\t")

# if countNAT085ERptAll > 0:
#All
DatalakeWriteFile(dfNAT085ERptAll.select("Output"), container, "nat/e-reporting/"+date+"/all", "nat00085.txt", fileFormat, None, "\t")

#Institutes
DatalakeWriteFile(dfNAT085ERpt90000.select("Output"), container, "nat/e-reporting/"+date+"/wsi" , "nat00085.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT085ERpt90001.select("Output"), container, "nat/e-reporting/"+date+"/nei" , "nat00085.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT085ERpt90002.select("Output"), container, "nat/e-reporting/"+date+"/hi"  , "nat00085.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT085ERpt90003.select("Output"), container, "nat/e-reporting/"+date+"/si"  , "nat00085.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT085ERpt90004.select("Output"), container, "nat/e-reporting/"+date+"/oten", "nat00085.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT085ERpt90005.select("Output"), container, "nat/e-reporting/"+date+"/ri"  , "nat00085.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT085ERpt90006.select("Output"), container, "nat/e-reporting/"+date+"/ii"  , "nat00085.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT085ERpt90008.select("Output"), container, "nat/e-reporting/"+date+"/swsi", "nat00085.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT085ERpt90009.select("Output"), container, "nat/e-reporting/"+date+"/wi"  , "nat00085.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT085ERpt90010.select("Output"), container, "nat/e-reporting/"+date+"/nci" , "nat00085.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT085ERpt90011.select("Output"), container, "nat/e-reporting/"+date+"/nsi" , "nat00085.txt", fileFormat, None, "\t")

# if countNAT090ERptAll > 0:
#All
DatalakeWriteFile(dfNAT090ERptAll.select("Output"), container, "nat/e-reporting/"+date+"/all", "nat00090.txt", fileFormat, None, "\t")

#Institutes
DatalakeWriteFile(dfNAT090ERpt90000.select("Output"), container, "nat/e-reporting/"+date+"/wsi" , "nat00090.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT090ERpt90001.select("Output"), container, "nat/e-reporting/"+date+"/nei" , "nat00090.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT090ERpt90002.select("Output"), container, "nat/e-reporting/"+date+"/hi"  , "nat00090.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT090ERpt90003.select("Output"), container, "nat/e-reporting/"+date+"/si"  , "nat00090.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT090ERpt90004.select("Output"), container, "nat/e-reporting/"+date+"/oten", "nat00090.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT090ERpt90005.select("Output"), container, "nat/e-reporting/"+date+"/ri"  , "nat00090.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT090ERpt90006.select("Output"), container, "nat/e-reporting/"+date+"/ii"  , "nat00090.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT090ERpt90008.select("Output"), container, "nat/e-reporting/"+date+"/swsi", "nat00090.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT090ERpt90009.select("Output"), container, "nat/e-reporting/"+date+"/wi"  , "nat00090.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT090ERpt90010.select("Output"), container, "nat/e-reporting/"+date+"/nci" , "nat00090.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT090ERpt90011.select("Output"), container, "nat/e-reporting/"+date+"/nsi" , "nat00090.txt", fileFormat, None, "\t")

# if countNAT100ERptAll > 0:
#All
DatalakeWriteFile(dfNAT100ERptAll.select("Output"), container, "nat/e-reporting/"+date+"/all", "nat00100.txt", fileFormat, None, "\t")

#Institutes
DatalakeWriteFile(dfNAT100ERpt90000.select("Output"), container, "nat/e-reporting/"+date+"/wsi" , "nat00100.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT100ERpt90001.select("Output"), container, "nat/e-reporting/"+date+"/nei" , "nat00100.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT100ERpt90002.select("Output"), container, "nat/e-reporting/"+date+"/hi"  , "nat00100.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT100ERpt90003.select("Output"), container, "nat/e-reporting/"+date+"/si"  , "nat00100.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT100ERpt90004.select("Output"), container, "nat/e-reporting/"+date+"/oten", "nat00100.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT100ERpt90005.select("Output"), container, "nat/e-reporting/"+date+"/ri"  , "nat00100.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT100ERpt90006.select("Output"), container, "nat/e-reporting/"+date+"/ii"  , "nat00100.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT100ERpt90008.select("Output"), container, "nat/e-reporting/"+date+"/swsi", "nat00100.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT100ERpt90009.select("Output"), container, "nat/e-reporting/"+date+"/wi"  , "nat00100.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT100ERpt90010.select("Output"), container, "nat/e-reporting/"+date+"/nci" , "nat00100.txt", fileFormat, None, "\t")
DatalakeWriteFile(dfNAT100ERpt90011.select("Output"), container, "nat/e-reporting/"+date+"/nsi" , "nat00100.txt", fileFormat, None, "\t")

# COMMAND ----------

Output = str(date)
print(Output)
dbutils.notebook.exit(Output)
