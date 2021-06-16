# Databricks notebook source
# DBTITLE 1,Sparks Config
# When set to true Spark SQL will automatically select a compression codec for each column based on statistics of the data.
spark.conf.set("spark.sql.inMemoryColumnarStorage.compressed",True)

# Limit of total size of serialized results of all partitions for each Spark action (e.g. collect) in bytes. Should be at least 1M, or 0 for unlimited. Jobs will be aborted if the total size is above this limit. Having a high limit may cause out-of-memory errors in driver (depends on spark.driver.memory and memory overhead of objects in JVM). Setting a proper limit can protect the driver from out-of-memory errors.
#spark.conf.set("spark.driver.maxResultSize",0)

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

# COMMAND ----------

# MAGIC %run ../../includes/util-general

# COMMAND ----------

def RunQualtricsLoad(EntityName, QualtricsTS, Dictionary):
  
  DEBUG_RECORD_COUNT = False
  DEBUG_DISPLAY_RECORDS = False
  
  dbutils.notebook.run("./functions/QualtricsLoad", 0, {
    "EntityName":EntityName ,
    "QualtricsTimestamp":QualtricsTS ,
    "BusinessRulesDictionary":str(Dictionary), 
    "DebugRecordCount":str(DEBUG_RECORD_COUNT), 
    "DebugDisplayRecords":str(DEBUG_DISPLAY_RECORDS) 
  })

# COMMAND ----------

import datetime
StartTimeStamp = GeneralGetAESTCurrent()
#StartTimeStamp = datetime.datetime(2021, 3, 15)
QualtricsTimeStamp = StartTimeStamp.strftime("%Y-%m-%d %H:%M:%S")
print(QualtricsTimeStamp)


# COMMAND ----------

# DBTITLE 1,Business Rule
LIST_STUDENT_STATUS_EXCLUDE = ['DECD', 'DINC', 'DISK', 'EXCA', 'EXCL', 'LEAV', 'LEXC', 'OHSE', 'OHSI', 'TCVV', 'TSSB', 'TSSE']

LIST_PROGRESS_CODE_EXCLUDE = []
LIST_PROGRESS_CODE_INCLUDE = []
LIST_PROGRESS_REASON_INCLUDE = []
LIST_UNIT_CATEGORY_INCLUDE = []
LIST_AWARD_CATEGORY_CODE_EXCLUDE = []

BusinessRule = {
  "CALOCC_CTYPE" : "COURSE",
  "SURVEY_DESCRIPTION" : "AFS",
  "SURVEY_DESCRIPTION_ALLOW_NULL" : True,
  "STUDENT_STATUS_CODE_EXCLUSION_LIST" : LIST_STUDENT_STATUS_EXCLUDE,
  "STUDENT_STATUS_CODE_ALLOW_NULL" : True,
  "USI_REQUIRED" : True,
  "PERSONAL_EMAIL_REQUIRED" : True,
  "PROGRESS_STATUS" : "",
  "PROGRESS_CODE_EXCLUSION_LIST" : LIST_PROGRESS_CODE_EXCLUDE,
  "PROGRESS_CODE_INCLUSION_LIST" : LIST_PROGRESS_CODE_INCLUDE,
  "PROGRESS_REASON_INCLUSION_LIST" : LIST_PROGRESS_REASON_INCLUDE,
  "UNIT_CATEGORY_INCLUSION_LIST" : LIST_UNIT_CATEGORY_INCLUDE,
  "AWARD_CATEGORY_CODE_EXCLUSION_LIST" : LIST_AWARD_CATEGORY_CODE_EXCLUDE,
  "ATTAINMENTS_EXCLUDE" : "",
  "DATE_AWARDED_REQUIRED" : False,
  "DATE_AWARDED_ALLOW_NULL" : True,
  "DATE_AWARDED_LAST_WEEK" : False,
  "DATE_AWARDED_LAST_YEAR" : False,
  "PROGRESS_DATE_LAST_WEEK" : False,
  "AWARD_STATUS_EXCLUDE" : "",
  "AWARD_STATUS_INCLUDE" : "",
  "AWARD_STATUS_ALLOW_NULL" : True,
  "MONTH_DIFF_STARTDATE_ENDDATE_GT": "",
  "MONTH_DIFF_CURRENT_ENDDATE_GT": "",
  "WEEK_DIFF_STARTDATE_CURRENT_LIST": [],
  "DAY_DIFF_STARTDATE_ENDDATE_GT" : "",
  "DAY_DIFF_CURRENT_ENDDATE_GT" : "",
  "EXCLUDE_ACTIVE_ENROLMENT" : False
}

print(BusinessRule)

# COMMAND ----------

# DBTITLE 1,EnrolmentActivity
LIST_PROGRESS_CODE_EXCLUDE = ['0.0 ROI', '0.1 AR', '0.22 EDU', '0.24 EVI', '0.26 APFI', '0.4 AS', '0.5 AO', '0.7 AA', '0.23 TESTE', '0.24 EVIE', '0.25VSLAEE', '1.10VSLPEN', '1.3 RPLREQ', '1.4 RPLNOG', '1.5 VFHPEN', '1.7FHPEN', '1.91TFCMAN', '1.92TFCERR', '0.2 AP', '0.9 TVH', '0.10 INC', '0.27 APTP', '0.13 ATPS', '0.74TFCERR', '0.23 TEST']

#Copy the original Dictionary and add any specific rules
BusinessRuleDict1 = BusinessRule.copy()
BusinessRuleDict1["PROGRESS_CODE_EXCLUSION_LIST"] = LIST_PROGRESS_CODE_EXCLUDE
BusinessRuleDict1["PROGRESS_STATUS"] = "A"
BusinessRuleDict1["AWARD_STATUS_EXCLUDE"] = "PUBLISHED"
BusinessRuleDict1["DAY_DIFF_STARTDATE_ENDDATE_GT"] = 90
BusinessRuleDict1["DAY_DIFF_CURRENT_ENDDATE_GT"] = 90
BusinessRuleDict1["WEEK_DIFF_STARTDATE_CURRENT_LIST"] = [8 , 20, 32, 44, 56, 68, 80]

print(BusinessRuleDict1)

RunQualtricsLoad ("EnrolmentActivity", QualtricsTimeStamp, BusinessRuleDict1) 

# COMMAND ----------

# DBTITLE 1,New Enrolment Activity
LIST_PROGRESS_CODE_EXCLUDE = ['0.0 ROI', '0.1 AR', '0.22 EDU', '0.24 EVI', '0.26 APFI', '0.4 AS', '0.5 AO', '0.7 AA', '0.23 TESTE', '0.24 EVIE', '0.25VSLAEE', '1.10VSLPEN', '1.3 RPLREQ', '1.4 RPLNOG', '1.5 VFHPEN', '1.7FHPEN', '1.91TFCMAN', '1.92TFCERR', '0.2 AP', '0.9 TVH', '0.10 INC', '0.27 APTP', '0.13 ATPS', '0.74TFCERR', '0.23 TEST']

BusinessRuleDict2 = BusinessRule.copy()
BusinessRuleDict2["PROGRESS_CODE_EXCLUSION_LIST"] = LIST_PROGRESS_CODE_EXCLUDE
BusinessRuleDict2["PROGRESS_STATUS"] = "A"
BusinessRuleDict2["AWARD_STATUS_EXCLUDE"] = "PUBLISHED"
BusinessRuleDict2["WEEK_DIFF_STARTDATE_CURRENT_LIST"] = [3]
BusinessRuleDict2["DAY_DIFF_STARTDATE_ENDDATE_GT"] = 20
BusinessRuleDict2["DAY_DIFF_CURRENT_ENDDATE_GT"] = 0

print(BusinessRuleDict2)

RunQualtricsLoad ("NewEnrolmentActivity", QualtricsTimeStamp, BusinessRuleDict2) 

# COMMAND ----------

# DBTITLE 1,EnrolmentCompletedLW
BusinessRuleDict3 = BusinessRule.copy()

BusinessRuleDict3["PROGRESS_STATUS"] = "F"
BusinessRuleDict3["AWARD_STATUS_INCLUDE"] = "PUBLISHED"
BusinessRuleDict3["DATE_AWARDED_LAST_WEEK"] = True
BusinessRuleDict3["DATE_AWARDED_REQUIRED"] = True
BusinessRuleDict3["ATTAINMENTS_EXCLUDE"] = "Withdrawn Award"

print(BusinessRuleDict3)

RunQualtricsLoad ("EnrolmentProgressCompleteLW", QualtricsTimeStamp, BusinessRuleDict3) 


# COMMAND ----------

# DBTITLE 1,EnrolmentCompletedLY
LIST_PROGRESS_CODE_INCLUDE = ['4.1 COMPL']
LIST_UNIT_CATEGORY_INCLUDE = ['HEC','TPQ','AC']
LIST_AWARD_CATEGORY_CODE_EXCLUDE = [29, 34]

BusinessRuleDict4 = BusinessRule.copy()

BusinessRuleDict4["PROGRESS_STATUS"] = "F"
BusinessRuleDict4["AWARD_STATUS_INCLUDE"] = "PUBLISHED"
BusinessRuleDict4["DATE_AWARDED_LAST_YEAR"] = True
BusinessRuleDict4["DATE_AWARDED_REQUIRED"] = True
BusinessRuleDict4["ATTAINMENTS_EXCLUDE"] = "Withdrawn Award"
BusinessRuleDict4["PROGRESS_CODE_INCLUSION_LIST"] = LIST_PROGRESS_CODE_INCLUDE
BusinessRuleDict4["UNIT_CATEGORY_INCLUSION_LIST"] = LIST_UNIT_CATEGORY_INCLUDE
BusinessRuleDict4["AWARD_CATEGORY_CODE_EXCLUSION_LIST"] = LIST_AWARD_CATEGORY_CODE_EXCLUDE
BusinessRuleDict4["EXCLUDE_ACTIVE_ENROLMENT"] = True


print(BusinessRuleDict4)

RunQualtricsLoad ("EnrolmentProgressCompleteLY", QualtricsTimeStamp, BusinessRuleDict4) 


# COMMAND ----------

# DBTITLE 1,Withdrawn Enrolments
LIST_PROGRESS_CODE_INCLUDE = ['3.1 WD', '3.3WN']
LIST_PROGRESS_REASON_INCLUDE = ['SSDISC', 'WDSAC' , 'WDVFHAC', 'SSDiscCOV', 'WCOV']

BusinessRuleDict5 = BusinessRule.copy()

BusinessRuleDict5["PROGRESS_STATUS"] = "W"
BusinessRuleDict5["PROGRESS_CODE_INCLUSION_LIST"] = LIST_PROGRESS_CODE_INCLUDE
BusinessRuleDict5["PROGRESS_REASON_INCLUSION_LIST"] = LIST_PROGRESS_REASON_INCLUDE
BusinessRuleDict5["PROGRESS_DATE_LAST_WEEK"] = True
BusinessRuleDict5["DATE_AWARDED_LAST_YEAR"] = False
BusinessRuleDict5["DATE_AWARDED_REQUIRED"] = False
BusinessRuleDict5["DATE_AWARDED_ALLOW_NULL"] = False
BusinessRuleDict5["EXCLUDE_ACTIVE_ENROLMENT"] = True

print(BusinessRuleDict5)

RunQualtricsLoad ("WithdrawnEnrolmentLW", QualtricsTimeStamp, BusinessRuleDict5) 


# COMMAND ----------

ReportFolder = StartTimeStamp.strftime("year=%Y/month=%m/day=%d/hour=%H/minute=%M")
print(ReportFolder)
dbutils.notebook.exit(ReportFolder)

# COMMAND ----------


