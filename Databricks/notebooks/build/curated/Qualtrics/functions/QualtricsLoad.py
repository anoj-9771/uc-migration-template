# Databricks notebook source
#Apply Window Functions
from pyspark.sql.window import Window


# COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

# COMMAND ----------

#dbutils.widgets.removeAll()

dbutils.widgets.text("EntityName","")
dbutils.widgets.text("BusinessRulesDictionary","")
dbutils.widgets.text("QualtricsTimestamp","")
dbutils.widgets.text("DebugMode","False")
dbutils.widgets.text("DebugPersonCode","False")


# COMMAND ----------

# DBTITLE 1,Include our Utility Function
# MAGIC %run ./util-Qualtrics

# COMMAND ----------

EntityName = dbutils.widgets.get("EntityName")
business_rules =  dbutils.widgets.get("BusinessRulesDictionary")
QualtricsTimestamp = dbutils.widgets.get("QualtricsTimestamp")
DebugMode = dbutils.widgets.get("DebugMode")
DebugPersonCode = dbutils.widgets.get("DebugPersonCode")

print(EntityName)
print(business_rules)
print(QualtricsTimestamp)
print(DebugMode)
print(DebugPersonCode)

# COMMAND ----------

qualtrics_timestamp = datetime.strptime(QualtricsTimestamp, "%Y-%m-%d %H:%M:%S")
import ast
BusinessRules = ast.literal_eval(business_rules)

print(BusinessRules)
print(type(BusinessRules))
print(qualtrics_timestamp)

# COMMAND ----------

debug_mode = GeneralGetBoolFromString(DebugMode)
print("Debug : {0}".format(debug_mode))

if debug_mode:
  debug_record_count = True
  debug_display_records = True
else:
  debug_record_count = False
  debug_display_records = False
  DebugPersonCode = ""
  
print ("Debug Record Count : {0}".format(debug_record_count))
print ("Debug Display : {0}".format(debug_display_records))
print ("Debug Person Code : {0}".format(DebugPersonCode))

# COMMAND ----------

#Show Business Rules
import json
print(json.dumps(BusinessRules, indent=4, sort_keys=True))

# COMMAND ----------

# DBTITLE 1,Get the basic dataframes
#EBS TABLES
dfAddresses = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_addresses")
dfAttainments = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_attainments")
dfConfigurableStatuses = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_configurable_statuses")
dfDisabilities = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_disabilities")
dfLocations = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_locations")
dfOrganisationUnits = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_organisation_units")
dfPeople = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_people")
dfPeopleAsr = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_people_asr")
dfPeopleUnits = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_people_units")
dfPeopleUnitsSpecial = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_people_units_special")
dfPeopleUsi = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_people_usi")
dfStudentStatusLog = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_student_status_log")
dfUnitInstanceOccurrences = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_unit_instance_occurrences")
dfUnitInstances = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_unit_instances")
dfVerifierProperties = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_verifier_properties")
dfVerifiers = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_verifiers")
dfVisaSubclasses = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_visa_subclasses")
dfVisas = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_visas")


# COMMAND ----------

#filter records to current and not deleted
dfAddresses = dfAddresses.where("_RecordCurrent = 1 and _RecordDeleted = 0")
dfAttainments = dfAttainments.where("_RecordCurrent = 1 and _RecordDeleted = 0")
dfConfigurableStatuses = dfConfigurableStatuses.where("_RecordCurrent = 1 and _RecordDeleted = 0")
dfDisabilities = dfDisabilities.where("_RecordCurrent = 1 and _RecordDeleted = 0")
dfLocations = dfLocations.where("_RecordCurrent = 1 and _RecordDeleted = 0")
dfOrganisationUnits = dfOrganisationUnits.where("_RecordCurrent = 1 and _RecordDeleted = 0")
dfPeople = dfPeople.where("_RecordCurrent = 1 and _RecordDeleted = 0")
dfPeopleAsr = dfPeopleAsr.where("_RecordCurrent = 1 and _RecordDeleted = 0")
dfPeopleUnits = dfPeopleUnits.where("_RecordCurrent = 1 and _RecordDeleted = 0")
dfPeopleUnitsSpecial = dfPeopleUnitsSpecial.where("_RecordCurrent = 1 and _RecordDeleted = 0")
dfPeopleUsi = dfPeopleUsi.where("_RecordCurrent = 1 and _RecordDeleted = 0")
dfStudentStatusLog = dfStudentStatusLog.where("_RecordCurrent = 1 and _RecordDeleted = 0")
dfUnitInstanceOccurrences = dfUnitInstanceOccurrences.where("_RecordCurrent = 1 and _RecordDeleted = 0")
dfUnitInstances = dfUnitInstances.where("_RecordCurrent = 1 and _RecordDeleted = 0")
dfVerifierProperties = dfVerifierProperties.where("_RecordCurrent = 1 and _RecordDeleted = 0")
dfVerifiers = dfVerifiers.where("_RecordCurrent = 1 and _RecordDeleted = 0")
dfVisaSubclasses = dfVisaSubclasses.where("_RecordCurrent = 1 and _RecordDeleted = 0")
dfVisas = dfVisas.where("_RecordCurrent = 1 and _RecordDeleted = 0")

# COMMAND ----------

#filter to Active records only
dfStudentStatusLog = dfStudentStatusLog.filter((col("START_DATE") <= qualtrics_timestamp) & (col("END_DATE") > qualtrics_timestamp))

#Use the national code if available
dfUnitInstances = dfUnitInstances.withColumn("NewNationalCode", coalesce(col("National_Course_Code"), col("FES_UNIT_INSTANCE_CODE")))

# COMMAND ----------

#Compliance Table
dfAvetmissLocation = DeltaTableAsCurrent("compliance.avetmiss_location")
dfCourseSkillsPoint = DeltaTableAsCurrent("reference.course_skills_point")
dfCourseToExclude = DeltaTableAsCurrent("trusted.qualtrics_coursetoexclude")


# COMMAND ----------

# DBTITLE 1,Prefix column names to make it unique
dfQAddresses = dfAddresses.select(*(col(x).alias("a_"+ x) for x in dfAddresses.columns))
dfQAttainments = dfAttainments.select(*(col(x).alias("at_"+ x) for x in dfAttainments.columns))
dfQAwardStatus = dfConfigurableStatuses.select(*(col(x).alias("as_"+ x) for x in dfConfigurableStatuses.columns))
dfQDisabilities = dfDisabilities.select(*(col(x).alias("d_"+ x) for x in dfDisabilities.columns))
dfQLocations = dfLocations.select(*(col(x).alias("l_"+ x) for x in dfLocations.columns))
dfQOrganisationUnits = dfOrganisationUnits.select(*(col(x).alias("ou_"+ x) for x in dfOrganisationUnits.columns))
dfQPeople = dfPeople.select(*(col(x).alias("p_"+ x) for x in dfPeople.columns))
dfQPeopleAsr = dfPeopleAsr.select(*(col(x).alias("pasr_"+ x) for x in dfPeopleAsr.columns))
dfQPeopleUnits = dfPeopleUnits.select(*(col(x).alias("pu_"+ x) for x in dfPeopleUnits.columns))
dfQPeopleUnitsSpecial = dfPeopleUnitsSpecial.select(*(col(x).alias("pus_"+ x) for x in dfPeopleUnitsSpecial.columns))
dfQPeopleUsi = dfPeopleUsi.select(*(col(x).alias("pusi_"+ x) for x in dfPeopleUsi.columns))
dfQStudentStatusLog = dfStudentStatusLog.select(*(col(x).alias("ssl_"+ x) for x in dfStudentStatusLog.columns))
dfQUnitInstanceOccurrences = dfUnitInstanceOccurrences.select(*(col(x).alias("uio_"+ x) for x in dfUnitInstanceOccurrences.columns))
dfQUnitInstances = dfUnitInstances.select(*(col(x).alias("ui_"+ x) for x in dfUnitInstances.columns))
dfQVerifierProperties = dfVerifierProperties.select(*(col(x).alias("vp_"+ x) for x in dfVerifierProperties.columns))
dfQVerifiers = dfVerifiers.select(*(col(x).alias("v_"+ x) for x in dfVerifiers.columns))
dfQVisas = dfVisas.select(*(col(x).alias("v_"+ x) for x in dfVisas.columns))
dfQVisaSubclasses = dfVisaSubclasses.select(*(col(x).alias("vsc_"+ x) for x in dfVisaSubclasses.columns))
dfQTeacher = dfPeople.select(*(col(x).alias("t_"+ x) for x in dfPeople.columns))


# COMMAND ----------

dfQAvetmissLocation = dfAvetmissLocation.select(*(col(x).alias("al_"+ x) for x in dfAvetmissLocation.columns))
dfQCourseSkillsPoint = dfCourseSkillsPoint.select(*(col(x).alias("csp_"+ x) for x in dfCourseSkillsPoint.columns))
dfQCourseToExclude = dfCourseToExclude.select(*(col(x).alias("excl_"+ x) for x in dfCourseToExclude.columns))


# COMMAND ----------

# DBTITLE 1,Chose only the columns needed
dfQAddresses = dfQAddresses.select("a_PERSON_CODE", "a_ADDRESS_TYPE", "a_UK_POST_CODE_PT1", "a_START_DATE", "a_END_DATE")
dfQAttainments = dfQAttainments.select("at_PEOPLE_UNITS_ID", "at_CONFIGURABLE_STATUS_ID", "at_DATE_AWARDED", "at_DESCRIPTION")
dfQAwardStatus = dfQAwardStatus.select("as_STATUS_CODE", "as_ID", "as_ACTIVE", "as_STATUS_TYPE")
dfQLocations = dfQLocations.select("l_LOCATION_CODE", "l_FES_LONG_DESCRIPTION")
dfQOrganisationUnits = dfQOrganisationUnits.select("ou_ORGANISATION_CODE", "ou_FES_FULL_NAME")
dfQPeople = dfQPeople.select("p_PERSON_CODE", "p_HOME_LANGUAGE", "p_FES_COUNTRY_OF_BIRTH", "p_RESIDENTIAL_STATUS", "p_PRIOR_ATTAIN_LEVEL", "p_LEARN_DIFF", "p_HIGHEST_POST_SCHOOL_QUAL", "p_PERSONAL_EMAIL", "p_PRIOR_ATTAIN_LEVEL", "p_IS_AT_SCHOOL", "p_F_LANG", "p_INDIGENOUS_STATUS", "p_DATE_OF_BIRTH", "p_YEAR_LEFT", "p_FORENAME", "p_SURNAME", "p_SEX", "p_MOBILE_PHONE_NUMBER", "p_FES_USER_1", "p_FES_USER_3", "p_FES_USER_5", "p_FES_USER_7", "p_FES_USER_9", "p_FES_USER_11", "p_FES_USER_13", "p_FES_USER_15", "p_FES_USER_17", "p_DIS_ACCESSED", "p_OVERSEAS", "p_IS_RETURNING_STUDENT", "p_REGISTRATION_NO", "p_KNOWN_AS", "p_college_email")
dfQPeopleAsr = dfQPeopleAsr.select("pasr_PERSON_CODE", "pasr_SURVEY_CONTACT_STATUS")
dfQPeopleUnits = dfQPeopleUnits.select("pu_PERSON_CODE", "pu_USER_5", "pu_CALOCC_CODE", "pu_CALOCC_CTYPE", "pu_UNIT_INSTANCE_CODE", "pu_PROGRESS_CODE", "pu_HAS_SOCIAL_HOUSING", "pu_PROGRESS_DATE", "pu_WELFARE_STATUS", "pu_SOURCE_TYPE", "pu_PROGRESS_STATUS", "pu_USER_1", "pu_ID", "pu_STUDY_REASON", "pu_DESTINATION", "pu_UIO_ID")
dfQPeopleUnitsSpecial = dfQPeopleUnitsSpecial.select("pus_PEOPLE_UNITS_ID", "pus_START_DATE", "pus_END_DATE")
dfQPeopleUsi = dfQPeopleUsi.select("pusi_PERSON_CODE", "pusi_USI")
dfQStudentStatusLog = dfQStudentStatusLog.select("ssl_PERSON_CODE", "ssl_STUDENT_STATUS_CODE", "ssl_START_DATE", "ssl_END_DATE")
dfQTeacher = dfQTeacher.select("t_PERSON_CODE", "t_FORENAME", "t_SURNAME")
dfQUnitInstanceOccurrences = dfQUnitInstanceOccurrences.select("uio_UIO_ID", "uio_FES_USER_1", "uio_FES_USER_8", "uio_FES_POSSIBLE_HOURS", "uio_FES_MOA_CODE", "uio_FES_START_DATE", "uio_FES_END_DATE", "uio_OWNING_ORGANISATION", "uio_SLOC_LOCATION_CODE", "uio_FES_COURSE_CONTACT")
dfQUnitInstances = dfQUnitInstances.select("ui_FES_UNIT_INSTANCE_CODE", "ui_PROSP_USER_7", "ui_FES_LONG_DESCRIPTION", "ui_UNIT_CATEGORY", "ui_AWARD_CATEGORY_CODE", "ui_NewNationalCode")
dfQVerifiers = dfQVerifiers.select("v_RV_DOMAIN", "v_FES_SHORT_DESCRIPTION", "v_FES_LONG_DESCRIPTION", "v_LOW_VALUE")


# COMMAND ----------

# DBTITLE 1,Remove any Duplicates from Address
#To fix bug in source records where previous address is not closed off (END_DATE is Null)
dfQAddresses = dfQAddresses.where("a_PERSON_CODE is not null and a_ADDRESS_TYPE = 'RES' and a_END_DATE is null")
dfQAddresses = dfQAddresses.withColumn("Address_Rank", dense_rank().over(Window.partitionBy("a_PERSON_CODE").orderBy(desc("a_START_DATE"), desc("a_END_DATE"))))
dfQAddresses = dfQAddresses.filter(col("Address_Rank") == 1)


# COMMAND ----------

# DBTITLE 1,Reference Tables
#Only select the Active Reference Values
dfQVerifiers = dfQVerifiers.where("v_FES_ACTIVE = 'Y'")
#Select only the needed columns from the Verifiers table
dfQVerifiers = dfQVerifiers.select("v_RV_DOMAIN", "v_FES_SHORT_DESCRIPTION", "v_FES_LONG_DESCRIPTION", "v_LOW_VALUE")

#Copies of Reference Table based on different domain
dfQRefHomeLang = dfQVerifiers.where ("v_RV_DOMAIN = 'HOME_LANGUAGE'")
dfQRefCountry = dfQVerifiers.where ("v_RV_DOMAIN = 'COUNTRY'")
dfQRefResStatus = dfQVerifiers.where ("v_RV_DOMAIN = 'RESIDENTIAL_STATUS'")
dfQRefPriorSchool = dfQVerifiers.where ("v_RV_DOMAIN = 'PRIOR_ATTAIN_LEVEL'")
dfQRefDisability = dfQVerifiers.where ("v_RV_DOMAIN = 'DISABILITY_STATUS'")
dfQRefHighestQual = dfQVerifiers.where ("v_RV_DOMAIN = 'HIGHEST_POST_SCHOOL_QUAL'")
dfQRefStudyReason = dfQVerifiers.where ("v_RV_DOMAIN = 'STUDY_REASON'")
dfQRefProdSubType = dfQVerifiers.where ("v_RV_DOMAIN = 'PRODUCT_SUB_TYPE'")
dfQRefQualAward = dfQVerifiers.where ("v_RV_DOMAIN = 'QUAL_AWARD_CATEGORY_CODE'")
dfQRefSurvey = dfQVerifiers.where ("v_RV_DOMAIN = 'AVETMISS_SURVEY_CONTACT_STATUS'")
dfQRefProgressReason = dfQVerifiers.where ("v_RV_DOMAIN = 'DESTINATION'")
dfQRefDisabilityType = dfQVerifiers.where ("v_RV_DOMAIN = 'DISABILITY_TYPE'")

#Prefix Column Names to make it unique
dfQRefHomeLang = dfQRefHomeLang.select(*(col(x).alias("ref_hl_"+ x) for x in dfQRefHomeLang.columns))
dfQRefCountry = dfQRefCountry.select(*(col(x).alias("ref_c_"+ x) for x in dfQRefCountry.columns))
dfQRefResStatus = dfQRefResStatus.select(*(col(x).alias("ref_rs_"+ x) for x in dfQRefResStatus.columns))
dfQRefPriorSchool = dfQRefPriorSchool.select(*(col(x).alias("ref_ps_"+ x) for x in dfQRefPriorSchool.columns))
dfQRefDisability = dfQRefDisability.select(*(col(x).alias("ref_d_"+ x) for x in dfQRefDisability.columns))
dfQRefHighestQual = dfQRefHighestQual.select(*(col(x).alias("ref_hq_"+ x) for x in dfQRefHighestQual.columns))
dfQRefStudyReason = dfQRefStudyReason.select(*(col(x).alias("ref_sr_"+ x) for x in dfQRefStudyReason.columns))
dfQRefProdSubType = dfQRefProdSubType.select(*(col(x).alias("ref_pst_"+ x) for x in dfQRefProdSubType.columns))
dfQRefQualAward = dfQRefQualAward.select(*(col(x).alias("ref_qa_"+ x) for x in dfQRefQualAward.columns))
dfQRefSurvey = dfQRefSurvey.select(*(col(x).alias("ref_s_"+ x) for x in dfQRefSurvey.columns))
dfQRefProgressReason = dfQRefProgressReason.select(*(col(x).alias("ref_pr_"+ x) for x in dfQRefProgressReason.columns))
dfQRefDisabilityType = dfQRefDisabilityType.select(*(col(x).alias("ref_dt_"+ x) for x in dfQRefDisabilityType.columns))



# COMMAND ----------

if debug_mode:
  if DebugPersonCode != "":
    dfQPeople = dfQPeople.where("p_PERSON_CODE IN ({0})".format(DebugPersonCode)).orderBy("p_PERSON_CODE")
  display(dfQPeople)

# COMMAND ----------

# DBTITLE 1,Join all the Tables in a common dataframe
dfQAll = dfQPeopleUnits \
  .join(dfQUnitInstanceOccurrences, dfQUnitInstanceOccurrences.uio_UIO_ID == dfQPeopleUnits.pu_UIO_ID, how = "inner") \
  .join(dfQPeople, dfQPeople.p_PERSON_CODE == dfQPeopleUnits.pu_PERSON_CODE, how = "inner") \
  .join(dfQLocations, dfQLocations.l_LOCATION_CODE == dfQUnitInstanceOccurrences.uio_SLOC_LOCATION_CODE, how = "inner") \
  .join(dfQUnitInstances, dfQUnitInstances.ui_FES_UNIT_INSTANCE_CODE == dfQPeopleUnits.pu_UNIT_INSTANCE_CODE, how = "inner") \
  .join(dfQStudentStatusLog, (dfQStudentStatusLog.ssl_PERSON_CODE == dfQPeopleUnits.pu_PERSON_CODE), how = "left") \
  .join(dfQPeopleUnitsSpecial, dfQPeopleUnitsSpecial.pus_PEOPLE_UNITS_ID == dfQPeopleUnits.pu_ID, how = "left") \
  .join(dfQAddresses, dfQAddresses.a_PERSON_CODE == dfQPeople.p_PERSON_CODE, how = "left") \
  .join(dfQPeopleUsi, dfQPeopleUsi.pusi_PERSON_CODE == dfQPeople.p_PERSON_CODE, how = "left") \
  .join(dfQTeacher, dfQTeacher.t_PERSON_CODE == dfQUnitInstanceOccurrences.uio_FES_COURSE_CONTACT, how = "left") \
  .join(dfQOrganisationUnits, dfQOrganisationUnits.ou_ORGANISATION_CODE == dfQUnitInstanceOccurrences.uio_OWNING_ORGANISATION, how = "left") \
  .join(dfQPeopleAsr, dfQPeopleAsr.pasr_PERSON_CODE == dfQPeople.p_PERSON_CODE, how = "left") \
  .join(dfQAttainments, dfQAttainments.at_PEOPLE_UNITS_ID == dfQPeopleUnits.pu_ID, how = "left") \
  .join(dfQAwardStatus, ((dfQAwardStatus.as_ID == dfQAttainments.at_CONFIGURABLE_STATUS_ID) \
        & (dfQAwardStatus.as_STATUS_CODE == 'PUBLISHED') \
        & (dfQAwardStatus.as_ACTIVE == 'Y') \
        & (dfQAwardStatus.as_STATUS_TYPE == 'AWARD')) \
        , how = "left") \
  .join(dfQRefHomeLang, dfQRefHomeLang.ref_hl_v_LOW_VALUE == dfQPeople.p_HOME_LANGUAGE, how = "left") \
  .join(dfQRefCountry, dfQRefCountry.ref_c_v_LOW_VALUE == dfQPeople.p_FES_COUNTRY_OF_BIRTH, how = "left") \
  .join(dfQRefResStatus, dfQRefResStatus.ref_rs_v_LOW_VALUE == dfQPeople.p_RESIDENTIAL_STATUS, how = "left") \
  .join(dfQRefPriorSchool, dfQRefPriorSchool.ref_ps_v_LOW_VALUE == dfQPeople.p_PRIOR_ATTAIN_LEVEL, how = "left") \
  .join(dfQRefDisability, dfQRefDisability.ref_d_v_LOW_VALUE == dfQPeople.p_LEARN_DIFF, how = "left") \
  .join(dfQRefHighestQual, dfQRefHighestQual.ref_hq_v_LOW_VALUE == dfQPeople.p_HIGHEST_POST_SCHOOL_QUAL, how = "left") \
  .join(dfQRefStudyReason, dfQRefStudyReason.ref_sr_v_LOW_VALUE == dfQPeopleUnits.pu_STUDY_REASON, how = "left") \
  .join(dfQRefProdSubType, dfQRefProdSubType.ref_pst_v_LOW_VALUE == dfQUnitInstances.ui_UNIT_CATEGORY, how = "left") \
  .join(dfQRefQualAward, dfQRefQualAward.ref_qa_v_LOW_VALUE == dfQUnitInstances.ui_AWARD_CATEGORY_CODE, how = "left") \
  .join(dfQRefSurvey, dfQRefSurvey.ref_s_v_LOW_VALUE == dfQPeopleAsr.pasr_SURVEY_CONTACT_STATUS, how = "left") \
  .join(dfQRefProgressReason, dfQRefProgressReason.ref_pr_v_LOW_VALUE == dfQPeopleUnits.pu_DESTINATION, how = "left") \
  .join(dfQAvetmissLocation, dfQUnitInstanceOccurrences.uio_SLOC_LOCATION_CODE == dfQAvetmissLocation.al_LocationCode, how = "left") \
  .join(dfQCourseSkillsPoint,  dfQUnitInstances.ui_NewNationalCode == dfQCourseSkillsPoint.csp_NationalCourseCode, how = "left") \
  .join(dfQCourseToExclude, dfQUnitInstances.ui_FES_UNIT_INSTANCE_CODE == dfQCourseToExclude.excl_Product_Code, how = "left") 


# COMMAND ----------

# DBTITLE 1,Find the Sunday(s) for the current date
from datetime import *
from dateutil.relativedelta import *
date = GeneralLocalDateTime().date()

days_next_sunday = (8 - (date.weekday() + 2)) %7
sunday_date_next = date + timedelta(days = days_next_sunday)
print("Days {0} : Sunday-Next : {1}".format(days_next_sunday, sunday_date_next))

days_prev_sunday = (date.weekday() + 1) %7
sunday_date_prev = date - timedelta(days = days_prev_sunday)
print("Days {0} : Sunday-Prev : {1}".format(days_prev_sunday, sunday_date_prev))

last_week_start = sunday_date_prev - timedelta(days = 6)
last_week_end = sunday_date_prev 

last_year_start = last_week_start.replace(year = last_week_start.year - 1)
last_year_end = last_week_end.replace(year = last_week_end.year - 1)

print ("Last Week ST : {}".format(last_week_start))
print ("Last Week EN : {}".format(last_week_end))
print ("Last Year ST : {}".format(last_year_start))
print ("Last Year EN : {}".format(last_year_end))


# COMMAND ----------

# DBTITLE 1,Apply Filters on the Recordset to filter records based on Business Rules
# Main Filters
dfQFiltered = dfQAll

dfQFiltered = dfQFiltered.where("excl_PRODUCT_CODE IS NULL")

if BusinessRules["CALOCC_CTYPE"] != "":
  print("Applying filter: CALOCC_CTYPE = {}".format(BusinessRules["CALOCC_CTYPE"]))
  dfQFiltered = dfQFiltered.where("pu_CALOCC_CTYPE = '{}'".format(BusinessRules["CALOCC_CTYPE"]))
  
if BusinessRules["SURVEY_DESCRIPTION"] != "":
  print("Applying filter: SURVEY_DESCRIPTION = {}".format(BusinessRules["SURVEY_DESCRIPTION"]))
  if BusinessRules["SURVEY_DESCRIPTION_ALLOW_NULL"]:
    dfQFiltered = dfQFiltered.where("pasr_SURVEY_CONTACT_STATUS = '{}' OR pasr_SURVEY_CONTACT_STATUS is null".format(BusinessRules["SURVEY_DESCRIPTION"]))
  else:
    dfQFiltered = dfQFiltered.where("pasr_SURVEY_CONTACT_STATUS = '{}'".format(BusinessRules["SURVEY_DESCRIPTION"]))

if BusinessRules["USI_REQUIRED"]:
  print("Applying filter: USI_REQUIRED = {}".format(BusinessRules["USI_REQUIRED"]))
  dfQFiltered = dfQFiltered.where("pusi_USI is not null")
  
if BusinessRules["PERSONAL_EMAIL_REQUIRED"]:
  print("Applying filter: PERSONAL_EMAIL_REQUIRED = {}".format(BusinessRules["PERSONAL_EMAIL_REQUIRED"]))
  dfQFiltered = dfQFiltered.where("COALESCE(P_PERSONAL_EMAIL, p_college_email) is not null")
 

# COMMAND ----------

if debug_record_count: print(dfQFiltered.count())

# COMMAND ----------

if debug_display_records: display(dfQFiltered.orderBy("pu_PERSON_CODE", "pu_CALOCC_CODE", "uio_FES_USER_1"))

# COMMAND ----------

if BusinessRules["PROGRESS_STATUS"] != "":
  print("Applying filter: PROGRESS_STATUS = {}".format(BusinessRules["PROGRESS_STATUS"]))
  dfQFiltered = dfQFiltered.where("pu_PROGRESS_STATUS = '{}'".format(BusinessRules["PROGRESS_STATUS"]))
  
if BusinessRules["DATE_AWARDED_REQUIRED"]:
  print("Applying filter: DATE_AWARDED_REQUIRED = {}".format(BusinessRules["DATE_AWARDED_REQUIRED"]))
  dfQFiltered = dfQFiltered.where("at_DATE_AWARDED is not null")
else:
  if BusinessRules["DATE_AWARDED_ALLOW_NULL"]:
    print("Applying filter: DATE_AWARDED_ALLOW_NULL = {}".format(BusinessRules["DATE_AWARDED_ALLOW_NULL"]))
    dfQFiltered = dfQFiltered.where("ifnull(at_DATE_AWARDED, '') = ''")

if BusinessRules["DATE_AWARDED_LAST_WEEK"]:
  print("Applying filter: DATE_AWARDED_LAST_WEEK = {}".format(BusinessRules["DATE_AWARDED_LAST_WEEK"]))
  dfQFiltered = dfQFiltered.where("at_DATE_AWARDED between '" + str(last_week_start) + "' and '" + str(last_week_end) + "'")
  
if BusinessRules["DATE_AWARDED_LAST_YEAR"]:
  print("Applying filter: DATE_AWARDED_LAST_YEAR = {}".format(BusinessRules["DATE_AWARDED_LAST_YEAR"]))
  dfQFiltered = dfQFiltered.where("at_DATE_AWARDED between '" + str(last_year_start) + "' and '" + str(last_year_end) + "'")

if BusinessRules["AWARD_STATUS_EXCLUDE"] != "":
  print("Applying filter: AWARD_STATUS_EXCLUDE = {}".format(BusinessRules["AWARD_STATUS_EXCLUDE"]))
  dfQFiltered = dfQFiltered.where("ifnull(as_STATUS_CODE, '') <> '{}'".format(BusinessRules["AWARD_STATUS_EXCLUDE"]))

if BusinessRules["AWARD_STATUS_INCLUDE"] != "":
  print("Applying filter: AWARD_STATUS_INCLUDE = {}".format(BusinessRules["AWARD_STATUS_INCLUDE"]))
  dfQFiltered = dfQFiltered.where("as_STATUS_CODE = '{}'".format(BusinessRules["AWARD_STATUS_INCLUDE"]))

if BusinessRules["ATTAINMENTS_EXCLUDE"] != "":
  print("Applying filter: ATTAINMENTS_EXCLUDE = {}".format(BusinessRules["ATTAINMENTS_EXCLUDE"]))
  dfQFiltered = dfQFiltered.where("ifnull(at_DESCRIPTION, '') <> '{}'".format(BusinessRules["ATTAINMENTS_EXCLUDE"]))
  
if BusinessRules["PROGRESS_DATE_LAST_WEEK"]:
  print("Applying filter: PROGRESS_DATE_LAST_WEEK = {}".format(BusinessRules["PROGRESS_DATE_LAST_WEEK"]))
  dfQFiltered = dfQFiltered.where("pu_PROGRESS_DATE between '" + str(last_week_start) + "' and '" + str(last_week_end) + "'")


# COMMAND ----------

if debug_record_count: print(dfQFiltered.count())

# COMMAND ----------

if debug_display_records: display(dfQFiltered.orderBy("pu_PERSON_CODE", "pu_CALOCC_CODE", "uio_FES_USER_1"))

# COMMAND ----------

# All List Filters

if len(BusinessRules["STUDENT_STATUS_CODE_EXCLUSION_LIST"]) > 0:
  print("Applying filter: STUDENT_STATUS_CODE_EXCLUSION_LIST = {}".format(BusinessRules["STUDENT_STATUS_CODE_EXCLUSION_LIST"]))
  if BusinessRules["STUDENT_STATUS_CODE_ALLOW_NULL"]:
    dfQFiltered = dfQFiltered.filter(~col("ssl_STUDENT_STATUS_CODE").isin(BusinessRules["STUDENT_STATUS_CODE_EXCLUSION_LIST"]) | col("ssl_STUDENT_STATUS_CODE").isNull())
  else:
    dfQFiltered = dfQFiltered.filter(~col("ssl_STUDENT_STATUS_CODE").isin(BusinessRules["STUDENT_STATUS_CODE_EXCLUSION_LIST"]))

if len(BusinessRules["PROGRESS_CODE_EXCLUSION_LIST"]) > 0:
  print("Applying filter: PROGRESS_CODE_EXCLUSION_LIST = {}".format(BusinessRules["PROGRESS_CODE_EXCLUSION_LIST"]))
  dfQFiltered = dfQFiltered.filter(~col("pu_PROGRESS_CODE").isin(BusinessRules["PROGRESS_CODE_EXCLUSION_LIST"]))
  
if len(BusinessRules["PROGRESS_CODE_INCLUSION_LIST"]) > 0:
  print("Applying filter: PROGRESS_CODE_INCLUSION_LIST = {}".format(BusinessRules["PROGRESS_CODE_INCLUSION_LIST"]))
  dfQFiltered = dfQFiltered.filter(col("pu_PROGRESS_CODE").isin(BusinessRules["PROGRESS_CODE_INCLUSION_LIST"]))

if len(BusinessRules["UNIT_CATEGORY_INCLUSION_LIST"]) > 0:
  print("Applying filter: UNIT_CATEGORY_INCLUSION_LIST = {}".format(BusinessRules["UNIT_CATEGORY_INCLUSION_LIST"]))
  dfQFiltered = dfQFiltered.filter(col("ui_UNIT_CATEGORY").isin(BusinessRules["UNIT_CATEGORY_INCLUSION_LIST"]))

if len(BusinessRules["AWARD_CATEGORY_CODE_EXCLUSION_LIST"]) > 0:
  print("Applying filter: AWARD_CATEGORY_CODE_EXCLUSION_LIST = {}".format(BusinessRules["AWARD_CATEGORY_CODE_EXCLUSION_LIST"]))
  dfQFiltered = dfQFiltered.filter(~col("ui_AWARD_CATEGORY_CODE").isin(BusinessRules["AWARD_CATEGORY_CODE_EXCLUSION_LIST"]))

if len(BusinessRules["PROGRESS_REASON_INCLUSION_LIST"]) > 0:
  print("Applying filter: PROGRESS_REASON_INCLUSION_LIST = {}".format(BusinessRules["PROGRESS_REASON_INCLUSION_LIST"]))
  dfQFiltered = dfQFiltered.filter(col("PU_DESTINATION").isin(BusinessRules["PROGRESS_REASON_INCLUSION_LIST"]))

# COMMAND ----------

if debug_record_count: print(dfQFiltered.count())

# COMMAND ----------

if debug_display_records: display(dfQFiltered.orderBy("pu_PERSON_CODE", "pu_CALOCC_CODE", "uio_FES_USER_1"))

# COMMAND ----------

# DBTITLE 1,Active Enrolments
if BusinessRules["EXCLUDE_ACTIVE_ENROLMENT"]:
  print("Applying filter: EXCLUDE_ACTIVE_ENROLMENT = {}".format(BusinessRules["EXCLUDE_ACTIVE_ENROLMENT"]))

  #date = date.today()
  date = qualtrics_timestamp
  cut_off_date = date.replace(year = date.year - 3)
  print(cut_off_date)

  dfQActiveEnrolment = dfQPeople \
    .join(dfQPeopleUnits, dfQPeopleUnits.pu_PERSON_CODE == dfQPeople.p_PERSON_CODE, how = "left") \
    .join(dfQCourseToExclude, dfQCourseToExclude.excl_Product_Code == dfQPeopleUnits.pu_UNIT_INSTANCE_CODE, how = "left") \
    .join(dfQPeopleUsi, dfQPeopleUsi.pusi_PERSON_CODE == dfQPeople.p_PERSON_CODE, how = "left") \

  dfQActiveEnrolment = dfQActiveEnrolment.where("pu_CALOCC_CTYPE = 'COURSE'")
  dfQActiveEnrolment = dfQActiveEnrolment.where("excl_PRODUCT_CODE IS NULL")
  dfQActiveEnrolment = dfQActiveEnrolment.where("pu_PROGRESS_CODE IN ('1.1 ACTIVE')")
  dfQActiveEnrolment = dfQActiveEnrolment.where("pu_PROGRESS_DATE > '" + str(cut_off_date) + "'")
  
  dfQFiltered = dfQFiltered.join(dfQActiveEnrolment, dfQFiltered.pusi_USI == dfQActiveEnrolment.pusi_USI, how = "left_anti")
  

# COMMAND ----------

if debug_record_count: print(dfQFiltered.count())

# COMMAND ----------

if debug_display_records: display(dfQFiltered.orderBy("pu_PERSON_CODE", "pu_CALOCC_CODE", "uio_FES_USER_1"))

# COMMAND ----------

# DBTITLE 1,Get the Month and Week Difference values
from pyspark.sql.functions import months_between
dfQReferenceDates = dfQFiltered
dfQReferenceDates = dfQReferenceDates.withColumn("StartDateCol", coalesce(col("pus_START_DATE"), col("uio_FES_START_DATE")))
dfQReferenceDates = dfQReferenceDates.withColumn("EndDateCol", coalesce(col("pus_END_DATE"), col("uio_FES_END_DATE")))

#The Months Between Function is different in SQL and Spark. The Trunc function sets each date to the first date of the month
#Then the Months Between Function calculates the difference in month based on standard (1st of each month)
dfQReferenceDates = dfQReferenceDates.withColumn("month_diff_startdate_enddate", months_between(trunc(col("EndDateCol"), "Month"), trunc(col("StartDateCol"), "Month")))
dfQReferenceDates = dfQReferenceDates.withColumn("month_diff_curent_enddate", months_between(trunc(col("EndDateCol"), "Month"), trunc(current_date(), "Month")))

#There is no proper Week Difference function in Spark
#In order to align with SQL (which cacluates the weeks based on number of weeks from Sunday)
#We first find out what was the previous Sunday date for the Current Date and then calculate the number of weeks (days/7) and apply the ceil function to get the whole numbers
dfQReferenceDates = dfQReferenceDates.withColumn("week_diff_startdate_current", ceil(datediff(lit(sunday_date_prev), col("StartDateCol"))/7))

dfQReferenceDates = dfQReferenceDates.withColumn("day_diff_startdate_enddate", datediff(col("EndDateCol"), col("StartDateCol")))
dfQReferenceDates = dfQReferenceDates.withColumn("day_diff_current_enddate", datediff(col("EndDateCol"), current_date()))


# COMMAND ----------

if debug_display_records:
  display(dfQReferenceDates \
  .select("pu_UIO_ID" 
  , "pu_PERSON_CODE", "StartDateCol", "EndDateCol"
  , "month_diff_startdate_enddate", "month_diff_curent_enddate", "week_diff_startdate_current", "day_diff_startdate_enddate", "day_diff_current_enddate"
  , "pus_START_DATE", "uio_FES_START_DATE", "pus_END_DATE", "uio_FES_END_DATE"
  , "pu_ID", "pu_UNIT_INSTANCE_CODE", "pu_PROGRESS_CODE"
  , "uio_FES_END_DATE", "uio_UIO_ID", "uio_SLOC_LOCATION_CODE", "uio_OWNING_ORGANISATION"
  , "pus_PEOPLE_UNITS_ID"
  , "p_PERSON_CODE"
  , "l_LOCATION_CODE"
  , "ui_FES_UNIT_INSTANCE_CODE"
  , "ssl_PERSON_CODE"
  , "a_PERSON_CODE"
  , "pusi_PERSON_CODE"
  , "t_PERSON_CODE"
  , "ou_ORGANISATION_CODE"
  , "pasr_PERSON_CODE"
  , "at_PEOPLE_UNITS_ID", "at_CONFIGURABLE_STATUS_ID"
  , "as_ID") \
  .orderBy("pu_PERSON_CODE"))

# COMMAND ----------

# DBTITLE 1,Apply Filters for Week and Month 
#Apply filters for Month
dfQFilteredDates = dfQReferenceDates

if BusinessRules["MONTH_DIFF_STARTDATE_ENDDATE_GT"] != "":
  print("Applying filter: MONTH_DIFF_STARTDATE_ENDDATE_GT = {}".format(BusinessRules["MONTH_DIFF_STARTDATE_ENDDATE_GT"]))
  dfQFilteredDates = dfQFilteredDates.filter(dfQFilteredDates.month_diff_startdate_enddate > BusinessRules["MONTH_DIFF_STARTDATE_ENDDATE_GT"])

if BusinessRules["MONTH_DIFF_CURRENT_ENDDATE_GT"] != "":
  print("Applying filter: MONTH_DIFF_CURRENT_ENDDATE_GT = {}".format(BusinessRules["MONTH_DIFF_CURRENT_ENDDATE_GT"]))
  dfQFilteredDates = dfQFilteredDates.filter(dfQFilteredDates.month_diff_curent_enddate > BusinessRules["MONTH_DIFF_CURRENT_ENDDATE_GT"])

#Apply filters for Weeks
if (len(BusinessRules["WEEK_DIFF_STARTDATE_CURRENT_LIST"]) > 0):
  print("Applying filter: WEEK_DIFF_STARTDATE_CURRENT_LIST = {}".format(BusinessRules["WEEK_DIFF_STARTDATE_CURRENT_LIST"]))
  dfQFilteredDates = dfQFilteredDates.filter(col("week_diff_startdate_current").isin(BusinessRules["WEEK_DIFF_STARTDATE_CURRENT_LIST"]))

if BusinessRules["DAY_DIFF_STARTDATE_ENDDATE_GT"] != "":
  print("Applying filter: DAY_DIFF_STARTDATE_ENDDATE_GT = {}".format(BusinessRules["DAY_DIFF_STARTDATE_ENDDATE_GT"]))
  dfQFilteredDates = dfQFilteredDates.filter(col("day_diff_startdate_enddate") > BusinessRules["DAY_DIFF_STARTDATE_ENDDATE_GT"])

if BusinessRules["DAY_DIFF_CURRENT_ENDDATE_GT"] != "":
  print("Applying filter: DAY_DIFF_CURRENT_ENDDATE_GT = {}".format(BusinessRules["DAY_DIFF_CURRENT_ENDDATE_GT"]))
  dfQFilteredDates = dfQFilteredDates.filter(col("day_diff_current_enddate") > BusinessRules["DAY_DIFF_CURRENT_ENDDATE_GT"])


# COMMAND ----------

if debug_record_count: print(dfQFilteredDates.count())

# COMMAND ----------

if debug_display_records: display(dfQFilteredDates.orderBy("pu_PERSON_CODE", "pu_CALOCC_CODE", "uio_FES_USER_1"))

# COMMAND ----------

if debug_display_records:
  display(dfQFilteredDates \
  .select("pu_UIO_ID" 
  , "pu_PERSON_CODE", "StartDateCol", "EndDateCol"
  , "month_diff_startdate_enddate", "month_diff_curent_enddate", "week_diff_startdate_current", "day_diff_startdate_enddate", "day_diff_current_enddate"
  , "pus_START_DATE", "uio_FES_START_DATE", "pus_END_DATE", "uio_FES_END_DATE"
  , "pu_ID", "pu_UNIT_INSTANCE_CODE", "pu_PROGRESS_CODE"
  , "uio_FES_END_DATE", "uio_UIO_ID", "uio_SLOC_LOCATION_CODE", "uio_OWNING_ORGANISATION"
  , "pus_PEOPLE_UNITS_ID"
  , "p_PERSON_CODE"
  , "l_LOCATION_CODE"
  , "ui_FES_UNIT_INSTANCE_CODE"
  , "ssl_PERSON_CODE"
  , "a_PERSON_CODE"
  , "pusi_PERSON_CODE"
  , "t_PERSON_CODE"
  , "ou_ORGANISATION_CODE"
  , "pasr_PERSON_CODE"
  , "at_PEOPLE_UNITS_ID", "at_CONFIGURABLE_STATUS_ID"
  , "as_ID") \
  .orderBy("pu_PERSON_CODE"))

# COMMAND ----------

#Get the Distinct list of Person Code
dfQPersonDistinctList = dfQFilteredDates.select("pu_PERSON_CODE").distinct()

# COMMAND ----------

# DBTITLE 1,Find all Disabilities for every Person Code
#Select only the list of Person filtered till this stage
dfQDisabilityPerson = dfQPersonDistinctList \
  .withColumnRenamed("pu_PERSON_CODE", "disability_PERSON_CODE")

#Join with the Verifiers Reference Table to find the Disability
dfQDisabilityReference = dfQDisabilities \
  .join(dfQRefDisabilityType, dfQDisabilities.d_DISABILITY_TYPE == dfQRefDisabilityType.ref_dt_v_LOW_VALUE, how = "inner") \

#Apply the Ranking function to find all the Disability ranked by Disability Type from the Disabilities table
dfQDisabilityReference = dfQDisabilityReference.withColumn("DisabilityOrder", \
                       row_number() \
                       .over( \
                         Window.partitionBy("d_PER_PERSON_CODE") \
                         .orderBy((col("d_DISABILITY_TYPE"))) \
                       ) 
                      )

#Select the columns needed
dfQDisabilityReference = dfQDisabilityReference \
  .select("d_ID", "d_PER_PERSON_CODE", "d_DISABILITY_TYPE", "ref_dt_v_FES_LONG_DESCRIPTION", "DisabilityOrder") \
  .orderBy(col("d_PER_PERSON_CODE"), col("DisabilityOrder"))

#Find uptill 3 level of disabilities for each Person
dfQDisabilityFinal = dfQDisabilityPerson \
  .join(dfQDisabilityReference, ((dfQDisabilityPerson.disability_PERSON_CODE == dfQDisabilityReference.d_PER_PERSON_CODE) & (dfQDisabilityReference.DisabilityOrder == 1)), how = "left") \
    .withColumnRenamed("ref_dt_v_FES_LONG_DESCRIPTION", "Disability1") \
    .drop("DisabilityOrder", "ref_dt_v_FES_SHORT_DESCRIPTION", "d_DISABILITY_TYPE", "d_PER_PERSON_CODE", "d_ID") \
  .join(dfQDisabilityReference, ((dfQDisabilityPerson.disability_PERSON_CODE == dfQDisabilityReference.d_PER_PERSON_CODE) & (dfQDisabilityReference.DisabilityOrder == 2)), how = "left") \
    .withColumnRenamed("ref_dt_v_FES_LONG_DESCRIPTION", "Disability2") \
    .drop("DisabilityOrder", "ref_dt_v_FES_SHORT_DESCRIPTION", "d_DISABILITY_TYPE", "d_PER_PERSON_CODE", "d_ID") \
  .join(dfQDisabilityReference, ((dfQDisabilityPerson.disability_PERSON_CODE == dfQDisabilityReference.d_PER_PERSON_CODE) & (dfQDisabilityReference.DisabilityOrder == 3)), how = "left") \
    .withColumnRenamed("ref_dt_v_FES_LONG_DESCRIPTION", "Disability3") \
    .drop("DisabilityOrder", "ref_dt_v_FES_SHORT_DESCRIPTION", "d_DISABILITY_TYPE", "d_PER_PERSON_CODE", "d_ID")

dfQDisabilityFinal = dfQDisabilityFinal.where("Disability1 is not null")

# COMMAND ----------

if debug_record_count: print(dfQDisabilityFinal.count())

# COMMAND ----------

if debug_display_records: display(dfQDisabilityFinal.orderBy("disability_PERSON_CODE"))

# COMMAND ----------

# DBTITLE 1,Find all Visa Subclasses Details for every Person Code
#Select only the list of Person filtered till this stage
dfQVisaSCPerson = dfQPersonDistinctList \
  .withColumnRenamed("pu_PERSON_CODE", "visa_PERSON_CODE")

#Join with the Verifiers Reference Table to find the Visa Sub Classes
dfQVisaReference = dfQVisas \
  .join(dfQVisaSubclasses, dfQVisas.v_VISA_SUB_CLASS == dfQVisaSubclasses.vsc_CODE, how = "inner") \

#Apply the Ranking function to find all the Visa SC ranked by Disability Type from the Visa tables
dfQVisaReference = dfQVisaReference.withColumn("VisaOrder", \
                       row_number() \
                       .over( \
                         Window.partitionBy("v_PERSON_CODE") \
                         .orderBy((col("v_PERSON_CODE"))) \
                       ) 
                      )

#Select the columns needed
dfQVisaReference = dfQVisaReference \
  .select("v_ID", "v_PERSON_CODE", "v_VISA_SUB_CLASS", "vsc_CODE", "vsc_DESCRIPTION", "VisaOrder") \
  .orderBy(col("v_PERSON_CODE"), col("VisaOrder"))


#Find uptill 3 level of Visa SubClass for each Person
dfQVisaSCFinal = dfQVisaSCPerson \
  .join(dfQVisaReference, ((dfQVisaSCPerson.visa_PERSON_CODE == dfQVisaReference.v_PERSON_CODE) & (dfQVisaReference.VisaOrder == 1)), how = "left") \
    .withColumnRenamed("vsc_DESCRIPTION", "VisaSubClass1") \
    .withColumnRenamed("v_PERSON_CODE", "v_PERSON_CODE_1") \
    .drop("VisaOrder", "vsc_DESCRIPTION", "v_ID", "v_VISA_SUB_CLASS", "vsc_CODE", "v_PERSON_CODE") \
  .join(dfQVisaReference, ((dfQVisaSCPerson.visa_PERSON_CODE == dfQVisaReference.v_PERSON_CODE) & (dfQVisaReference.VisaOrder == 2)), how = "left") \
    .withColumnRenamed("vsc_DESCRIPTION", "VisaSubClass2") \
    .withColumnRenamed("v_PERSON_CODE", "v_PERSON_CODE_2") \
    .drop("VisaOrder", "vsc_DESCRIPTION", "v_ID", "v_VISA_SUB_CLASS", "vsc_CODE", "v_PERSON_CODE") \
  .join(dfQVisaReference, ((dfQVisaSCPerson.visa_PERSON_CODE == dfQVisaReference.v_PERSON_CODE) & (dfQVisaReference.VisaOrder == 3)), how = "left") \
    .withColumnRenamed("vsc_DESCRIPTION", "VisaSubClass3") \
    .withColumnRenamed("v_PERSON_CODE", "v_PERSON_CODE_3") \
    .drop("VisaOrder", "vsc_DESCRIPTION", "v_ID", "v_VISA_SUB_CLASS", "vsc_CODE", "v_PERSON_CODE") \

dfQVisaSCFinal = dfQVisaSCFinal.where("VisaSubClass1 is not null")

# COMMAND ----------

if debug_record_count: print(dfQVisaSCFinal.count())

# COMMAND ----------

if debug_display_records: display(dfQVisaSCFinal.orderBy("visa_PERSON_CODE"))

# COMMAND ----------

#Join with Disability and Visa dataframes to get the multiple values for each category
dfQPivot = dfQFilteredDates
dfQPivot = dfQPivot \
  .join(dfQDisabilityFinal, dfQPivot.pu_PERSON_CODE == dfQDisabilityFinal.disability_PERSON_CODE, how = "left") \
  .join(dfQVisaSCFinal, dfQPivot.pu_PERSON_CODE == dfQVisaSCFinal.visa_PERSON_CODE, how = "left")

# COMMAND ----------

if debug_record_count: print(dfQPivot.count())

# COMMAND ----------

# DBTITLE 1,Window Function to identify Duplicates
dfQUnique = dfQPivot
dfQUnique = dfQUnique.withColumn("DupCount", \
                       row_number() \
                       .over(Window.partitionBy("pu_PERSON_CODE", "pu_UNIT_INSTANCE_CODE", "pu_PROGRESS_CODE") \
                       .orderBy(col("EndDateCol").desc())))

# COMMAND ----------

if debug_record_count: print(dfQUnique.count())

# COMMAND ----------

# DBTITLE 1,Remove Duplicates
dfQUnique = dfQUnique.filter(dfQUnique.DupCount == 1)

# COMMAND ----------

if debug_record_count: print(dfQUnique.count())

# COMMAND ----------

# DBTITLE 1,Other Business Transformations (No Filters on Records here)
dfQUnique = dfQUnique \
  .withColumn("LeftBeforeYear10", expr("case when p_PRIOR_ATTAIN_LEVEL IN (2, 8, 3) and p_IS_AT_SCHOOL = 'N' then 'Y' else 'N' END")) \
  .withColumn("English_How_Well", expr("case " +
                   "when p_F_LANG = 1 then 'Very Well' " + 
                   "when p_F_LANG = 2 then 'Well' " +
                   "when p_F_LANG = 3 then 'Not Well' " +
                   "when p_F_LANG = 4 then 'Not At All' " +
                   "end")) \
  .withColumn("ApprenticeOrTrainee", expr("case " + 
                   "when pu_USER_1 = 'A' then 'Apprentice' " +
                   "when pu_USER_1 = 'T' then 'Trainee' " +
                   "when pu_USER_1 = 'N' then 'Neither' " +
                   "else 'Neither' " + 
                   "end")) \
  .withColumn("AttendanceMode", \
                  when(col("uio_FES_USER_8") == "FT", "Full Time") \
                  .when(col("uio_FES_USER_8") == "ONL", "Online")
                  .when(col("uio_FES_USER_8") == "PTD", "Part Time Day")
                  .when(col("uio_FES_USER_8") == "PTE", "Part Time Evening")
                  .when(col("uio_FES_USER_8") == "WORK", "Workplace")
                  .when(col("uio_FES_USER_8") == "FLEXIBLE", "Flexible")
                  .otherwise(col("uio_FES_USER_8"))
             ) \
  .withColumn("CurrentStatus", \
                  when(dfQUnique["pu_PROGRESS_STATUS"] == "A", "Active") \
                  .when(col('pu_PROGRESS_STATUS') == "F", "Completed") \
                  .when(col("pu_PROGRESS_STATUS") == "W", "Withdrawn") \
                  .when(col("pu_PROGRESS_STATUS") == "T", "Transferred") \
                  .when(col("pu_PROGRESS_STATUS") == "L", "Expression of Interest") \
                  .when(col("pu_PROGRESS_STATUS") == "N", "Not Yet Started") \
                  .otherwise(col("pu_PROGRESS_STATUS"))
             ) \
  .withColumn("MarketingSource", \
                  when(col("pu_SOURCE_TYPE") == 1, "TV/Cinema/Radio") \
                  .when(col("pu_SOURCE_TYPE") == 2, "Magazine/Newspaper") \
                  .when(col("pu_SOURCE_TYPE") == 3, "Advertising Banner") \
                  .when(col("pu_SOURCE_TYPE") == 4, "Centrelink") \
                  .when(col("pu_SOURCE_TYPE") == 5, "Job Centres") \
                  .when(col("pu_SOURCE_TYPE") == 6, "Google") \
                  .when(col("pu_SOURCE_TYPE") == 7, "Facebook/Twitter") \
                  .when(col("pu_SOURCE_TYPE") == 8, "Friend/Family") \
                  .when(col("pu_SOURCE_TYPE") == 9, "Other") \
             ) \
  .withColumn("WelfareStatus", \
                  when(col("pu_WELFARE_STATUS") == 0, "Not a welfare recipient") \
                  .when(col("pu_WELFARE_STATUS") == 1, "Is a dependent child or spouse of a welfare recipient") \
                  .when(col("pu_WELFARE_STATUS") == 2, "Is a welfare recipient") \
             ) \
  .withColumn("IndigenousStatus", \
                  when(col("p_INDIGENOUS_STATUS") == '1', "Aboriginal") \
                  .when(col("p_INDIGENOUS_STATUS") == '2', "TS Islander") \
                  .when(col("p_INDIGENOUS_STATUS") == '3', "Aboriginal and TS Islander") \
                  .when(col("p_INDIGENOUS_STATUS") == '4', "No") \
                  .otherwise("Not Set") \
             ) \
  .withColumn("ProductName", coalesce(col("ui_PROSP_USER_7"), col("ui_FES_LONG_DESCRIPTION"))) \
  .withColumn("Teacher", concat(col("t_FORENAME"), lit(" "), col("t_SURNAME"))) \
  .withColumn("Region", \
                  when(col("al_InstituteID") == '0357', "TAFE Digital") \
                  .when(col("al_InstituteID") == '0158', "Sydney Region") \
                  .when(col("al_InstituteID") == '0160', "North Region") \
                  .when(col("al_InstituteID") == '0161', "Western Sydney") \
                  .when(col("al_InstituteID") == '0162', "Western Sydney") \
                  .when(col("al_InstituteID") == '0163', "West Region") \
                  .when(col("al_InstituteID") == '0164', "South Region") \
                  .when(col("al_InstituteID") == '0165', "Sydney Region") \
                  .when(col("al_InstituteID") == '0166', "West Region") \
                  .when(col("al_InstituteID") == '0167', "North Region") \
                  .when(col("al_InstituteID") == '0168', "South Region") \
             ) \
  .withColumn("RTO", \
                  when(col("al_InstituteID") == '0357', "OTEN") \
                  .when(col("al_InstituteID") == '0158', "North Sydney") \
                  .when(col("al_InstituteID") == '0160', "North Coast") \
                  .when(col("al_InstituteID") == '0161', "South Western Sydney") \
                  .when(col("al_InstituteID") == '0162', "Western Sydney") \
                  .when(col("al_InstituteID") == '0163', "New England") \
                  .when(col("al_InstituteID") == '0164', "Riverina") \
                  .when(col("al_InstituteID") == '0165', "Sydney") \
                  .when(col("al_InstituteID") == '0166', "Western") \
                  .when(col("al_InstituteID") == '0167', "Hunter") \
                  .when(col("al_InstituteID") == '0168', "Illawarra") \
             ) \
  .withColumn("SourceSystem", lit("RTO")) \

              

# COMMAND ----------

# DBTITLE 1,Column Values formatting
#FormatDates
dfQUnique = dfQUnique \
  .withColumn("DateOfBirth", date_format(col("p_DATE_OF_BIRTH"), "yyyyMMdd")) \
  .withColumn("StartDate", date_format(col("StartDateCol"), "yyyyMMdd")) \
  .withColumn("EndDate", date_format(col("EndDateCol"), "yyyyMMdd")) \
  .withColumn("ProgressDate", date_format(col("pu_PROGRESS_DATE"), "yyyyMMdd")) \
  .withColumn("DateAwarded", date_format(col("at_DATE_AWARDED"), "yyyyMMdd")) \
  .withColumn("ArrivalInAustralia", col("p_YEAR_LEFT").cast(IntegerType()).cast(StringType())) \
  .withColumn("SkillsTeam", col("csp_SkillsPointID").cast(IntegerType()).cast(StringType()))


# COMMAND ----------

# DBTITLE 1,Column Names Rename - to align with Reporting
dfQColRenamed = dfQUnique \
  .withColumnRenamed("p_PERSON_CODE", "PersonCode") \
  .withColumnRenamed("p_PERSONAL_EMAIL", "PersonalEmail") \
  .withColumnRenamed("p_FORENAME", "FirstName") \
  .withColumnRenamed("p_SURNAME", "Surname") \
  .withColumnRenamed("p_SEX", "Gender") \
  .withColumnRenamed("a_UK_POST_CODE_PT1", "PostCode") \
  .withColumnRenamed("p_MOBILE_PHONE_NUMBER", "MobilePhone") \
  .withColumnRenamed("pusi_USI", "USI") \
  .withColumnRenamed("pu_USER_5", "WorksInNSW") \
  .withColumnRenamed("p_IS_AT_SCHOOL", "IsAtSchool") \
  .withColumnRenamed("ref_ps_v_FES_LONG_DESCRIPTION", "HighSchoolYearCompleted") \
  .withColumnRenamed("p_FES_USER_3", "BachelorHigherDegree") \
  .withColumnRenamed("p_FES_USER_5", "AdvDiplomaOrAssocDegree") \
  .withColumnRenamed("p_FES_USER_7", "DiplomaOrAssocDiploma") \
  .withColumnRenamed("p_FES_USER_9", "CertIVOrAdvCertTech") \
  .withColumnRenamed("p_FES_USER_11", "CertIIIOrTradeCert") \
  .withColumnRenamed("p_FES_USER_13", "CertII") \
  .withColumnRenamed("p_FES_USER_15", "CertI") \
  .withColumnRenamed("p_FES_USER_17", "CertOthers") \
  .withColumnRenamed("uio_FES_POSSIBLE_HOURS", "DeliveryHours") \
  .withColumnRenamed("ou_FES_FULL_NAME", "TeachingSection") \
  .withColumnRenamed("pu_CALOCC_CODE", "CaloccCode") \
  .withColumnRenamed("uio_FES_USER_1", "OfferingCode") \
  .withColumnRenamed("pu_UNIT_INSTANCE_CODE", "ProductCode") \
  .withColumnRenamed("pu_PROGRESS_CODE", "ProgressCode") \
  .withColumnRenamed("ref_hq_v_FES_LONG_DESCRIPTION", "HighestPostSchoolQual") \
  .withColumnRenamed("ref_rs_v_FES_LONG_DESCRIPTION", "ResidentialStatus") \
  .withColumnRenamed("ref_d_v_FES_LONG_DESCRIPTION", "DisabilityFlag") \
  .withColumnRenamed("p_DIS_ACCESSED", "DisabilityHelp") \
  .withColumnRenamed("p_FES_USER_1", "EnglishHelp") \
  .withColumnRenamed("ref_c_v_FES_LONG_DESCRIPTION", "CountryOfBirth") \
  .withColumnRenamed("ref_hl_v_FES_LONG_DESCRIPTION", "HomeLanguage") \
  .withColumnRenamed("p_OVERSEAS", "InternationalStudent") \
  .withColumnRenamed("ref_sr_v_FES_LONG_DESCRIPTION", "MainReasonforStudy") \
  .withColumnRenamed("uio_FES_POSSIBLE_HOURS", "DeliveryHours") \
  .withColumnRenamed("uio_FES_MOA_CODE", "DeliveryMode") \
  .withColumnRenamed("l_FES_LONG_DESCRIPTION", "EnrolmnentLocation") \
  .withColumnRenamed("ref_qa_v_FES_LONG_DESCRIPTION", "AwardLevel") \
  .withColumnRenamed("ref_pst_v_FES_LONG_DESCRIPTION", "ProductSubType") \
  .withColumnRenamed("p_IS_RETURNING_STUDENT", "StudiedAtTafeBefore") \
  .withColumnRenamed("ou_FES_FULL_NAME", "TeachingSection") \
  .withColumnRenamed("ssl_STUDENT_STATUS_CODE", "StudentStatusCode") \
  .withColumnRenamed("pu_HAS_SOCIAL_HOUSING", "HasSocialHousing") \
  .withColumnRenamed("ref_s_v_FES_LONG_DESCRIPTION", "SurveyContactStatus") \
  .withColumnRenamed("ref_pr_v_FES_LONG_DESCRIPTION", "ProgressReason") \
  .withColumnRenamed("ui_FES_LONG_DESCRIPTION", "AwardProduct") \
  .withColumnRenamed("p_REGISTRATION_NO", "LearnerId") \
  .withColumnRenamed("as_STATUS_CODE", "AwardStatus") \
  .withColumnRenamed("p_KNOWN_AS", "PreferredGivenName") \
  .withColumnRenamed("p_DATE_OF_BIRTH", "DateOfBirth-Original") \
  .withColumnRenamed("pu_PROGRESS_DATE", "ProgressDate-Original") \
  .withColumnRenamed("at_DATE_AWARDED", "DateAwarded-Original") \



# COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

# COMMAND ----------

# DBTITLE 1,Select the columns needed
dfQFinal = dfQColRenamed.select ("PersonCode"
             , "SourceSystem"
             , "PersonalEmail"
             , "FirstName"
             , "Surname"
             , "Gender"
             , "DateOfBirth"
             , "PostCode"
             , "MobilePhone"
             , "USI"
             , "WorksInNSW"
             , "IsAtSchool"
             , "LeftBeforeYear10"
             , "HighSchoolYearCompleted"
             , "BachelorHigherDegree"
             , "AdvDiplomaOrAssocDegree"
             , "DiplomaOrAssocDiploma"
             , "CertIVOrAdvCertTech"
             , "CertIIIOrTradeCert"
             , "CertII"
             , "CertI"
             , "CertOthers"
             , "HighestPostSchoolQual"
             , "ResidentialStatus"
             , "DisabilityFlag"
             , "DisabilityHelp"
             , "Disability1"
             , "Disability2"
             , "Disability3"
             , "English_How_Well"
             , "EnglishHelp"
             , "CountryOfBirth"
             , "HomeLanguage"
             , "ArrivalInAustralia"
             , "InternationalStudent"
             , "VisaSubClass1"
             , "VisaSubClass2"
             , "VisaSubClass3"
             , "ApprenticeOrTrainee"
             , "MainReasonforStudy"
             , "AttendanceMode"
             , "CurrentStatus"
             , "DeliveryHours"
             , "DeliveryMode"
             , "StartDate"
             , "EndDate"
             , "EnrolmnentLocation"
             , "MarketingSource"
             , "ProductCode"
             , "AwardLevel"
             , "ProductName"
             , "ProductSubType"
             , "ProgressCode"
             , "ProgressDate"
             , "StudiedAtTafeBefore"
             , "Teacher"
             , "TeachingSection"
             , "Region"
             , "RTO"
             , "SkillsTeam"
             , "HasSocialHousing"
             , "WelfareStatus"
             , "SurveyContactStatus"
             , "StudentStatusCode"
             , "ProgressReason"
             , "IndigenousStatus"
             , "AwardProduct"
             , "LearnerId"
             , "DateAwarded"
             , "AwardStatus"
             , "CaloccCode"
             , "OfferingCode"
             , "PreferredGivenName"
#              , "ui_FES_UNIT_INSTANCE_CODE"
#              , "DateOfBirth-Original"
#              , "ProgressDate-Original"
#              , "DateAwarded-Original"
#              , "month_diff_startdate_enddate"
#              , "month_diff_curent_enddate"
#              , "week_diff_startdate_current"
#              , "day_diff_startdate_enddate"
#              , "day_diff_current_enddate"
#              , "DupCount"
            ).orderBy("PersonCode")


# COMMAND ----------

#Do not include the Teaching Section as per Business rule
dfQFinal = dfQFinal.filter(dfQFinal.TeachingSection != 'Open Colleges B2B')


# COMMAND ----------

#Replace NULL values with NULL String
dfQFinal = dfQFinal.fillna("NULL")

# COMMAND ----------

record_count = dfQFinal.count()
print(record_count)

# COMMAND ----------

# DBTITLE 1,Save the Data
if record_count > 0 :
  dfQFinal.cache()
  QualtricsSaveDataFrame(dfQFinal, EntityName, qualtrics_timestamp)
