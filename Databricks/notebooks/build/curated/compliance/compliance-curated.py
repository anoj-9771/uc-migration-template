# Databricks notebook source
# MAGIC %run ./includes/util-compliance-common

# COMMAND ----------

# MAGIC %run ./functions/compliance-functions-dimensions

# COMMAND ----------

# MAGIC %run ./functions/compliance-functions-facts

# COMMAND ----------

# DBTITLE 1,Parameters
#Set Parameters
dbutils.widgets.removeAll()

dbutils.widgets.text("Start_Date","")
dbutils.widgets.text("End_Date","")
dbutils.widgets.text("param_load_dimensions","true")
dbutils.widgets.text("param_load_facts","true")
dbutils.widgets.text("param_refresh_link_sp","true")

# COMMAND ----------

#Get Parameters
start_date = dbutils.widgets.get("Start_Date")
end_date = dbutils.widgets.get("End_Date")
param_load_dimensions = dbutils.widgets.get("param_load_dimensions")
param_load_facts = dbutils.widgets.get("param_load_facts")
param_refresh_link_sp = dbutils.widgets.get("param_refresh_link_sp")

params = {"start_date": start_date, "end_date": end_date}

#DEFAULT IF ITS BLANK
start_date = "2000-01-01" if not start_date else start_date
end_date = "9999-12-31" if not end_date else end_date

#Print Date Range
print(f"Start_Date = {start_date}| End_Date = {end_date}")

# COMMAND ----------

LoadDimensions = GeneralGetBoolFromString(param_load_dimensions)
LoadFacts = GeneralGetBoolFromString(param_load_facts)
RefreshLinkSP = GeneralGetBoolFromString(param_refresh_link_sp)

print(f"Load Dimemsions : {LoadDimensions}")
print(f"Load Facts      : {LoadFacts}")
print(f"Refresh SPs     : {RefreshLinkSP}")


# COMMAND ----------

# DBTITLE 1,Spark Config
# When set to true Spark SQL will automatically select a compression codec for each column based on statistics of the data.
spark.conf.set("spark.sql.inMemoryColumnarStorage.compressed",True)

# Limit of total size of serialized results of all partitions for each Spark action (e.g. collect) in bytes. Should be at least 1M, or 0 for unlimited. Jobs will be aborted if the total size is above this limit. Having a high limit may cause out-of-memory errors in driver (depends on spark.driver.memory and memory overhead of objects in JVM). Setting a proper limit can protect the driver from out-of-memory errors.
#spark.conf.set("spark.driver.maxResultSize",0)

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

# COMMAND ----------

# DBTITLE 1,All DataFrames
#REFERENCE TABLES
refAnimalUseDf = DeltaTableAsCurrent("reference.animal_use")
refAttendanceModeDf = DeltaTableAsCurrent("reference.attendance_mode")
refAvetmissCourseDf = DeltaTableAsCurrent("reference.avetmiss_course")
refAvetmissCourseSkillsPointDf = DeltaTableAsCurrent("reference.avetmiss_course_skills_point")
refAvetmissQualificationDf = DeltaTableAsCurrent("reference.avetmiss_qualification")
refAvetmissReportingCutOffDateDf = DeltaTableAsCurrent("reference.avetmiss_reporting_year_cut_off_date")
refCountryDf = DeltaTableAsCurrent("reference.country")
refCourseReplacementDf = DeltaTableAsCurrent("reference.course_replacement")
refDeliveryModeDf = DeltaTableAsCurrent("reference.delivery_mode")
refDisabilityStatusDf = DeltaTableAsCurrent("reference.disability_status")
refDisabilityTypeDf = DeltaTableAsCurrent("reference.disability_type")
refEmploymentStatusDf = DeltaTableAsCurrent("reference.employment_status")
refEnglishLevelDf = DeltaTableAsCurrent("reference.english_level")
refFieldOfEducationDf = DeltaTableAsCurrent("reference.field_of_education")
refFieldOfEducationIscDf = DeltaTableAsCurrent("reference.field_of_education_isc")
refFundingSourceDf = DeltaTableAsCurrent("reference.funding_source")
refFundingSourceBprDf = DeltaTableAsCurrent("reference.funding_source_bpr")
refFundingSourceCoreFundDf = DeltaTableAsCurrent("reference.funding_source_core_fund")
refFundingSourceSbiRulesDf = DeltaTableAsCurrent("reference.funding_source_sbi_rules")
refFundingSourceVetFeeHelpFundDf = DeltaTableAsCurrent("reference.funding_source_vet_fee_help_fund")
refIndustryDf = DeltaTableAsCurrent("reference.industry")
refIndigenousStatusDf = DeltaTableAsCurrent("reference.indigenous_status")
refIndustrySkillsCouncilDf = DeltaTableAsCurrent("reference.industry_skills_council")
refInstituteDf = DeltaTableAsCurrent("reference.institute")
refLanguageDf = DeltaTableAsCurrent("reference.language")
refLgaDf = DeltaTableAsCurrent("reference.lga_reference_data")
refOccupationDf = DeltaTableAsCurrent("reference.occupation")
refOfferingStatusDf = DeltaTableAsCurrent("reference.offering_status")
refOrganisationTypeDf = DeltaTableAsCurrent("reference.organisation_type")
refPriorEducationLocationDf = DeltaTableAsCurrent("reference.prior_education_location")
refProductSubTypeDf = DeltaTableAsCurrent("reference.product_sub_type")
refQualificationGroupDf = DeltaTableAsCurrent("reference.qualification_group")
refQualificationTypeDf = DeltaTableAsCurrent("reference.qualification_type")
refQualificationTypeMappingDf = DeltaTableAsCurrent("reference.qualification_type_mapping")
refReportingPeriodDf = DeltaTableAsCurrent("reference.reporting_period")
refResidentialStatusDf = DeltaTableAsCurrent("reference.residential_status")
refResourceAllocationModelCategoryDf = DeltaTableAsCurrent("reference.resource_allocation_model_category")
refSbiCategoryDf = DeltaTableAsCurrent("reference.sbi_category")
refSbiGroupDf = DeltaTableAsCurrent("reference.sbi_group")
refSbiSubCategoryDf = DeltaTableAsCurrent("reference.sbi_sub_category")
refSeifaDf = DeltaTableAsCurrent("reference.seifa")
refSkillsListDf = DeltaTableAsCurrent("reference.skills_list")
refStratificationGroupDf = DeltaTableAsCurrent("reference.stratification_group")
refStudentStatusDf = DeltaTableAsCurrent("reference.student_status")
refTrainingPackageIscDf = DeltaTableAsCurrent("reference.training_package_isc")
refTsNswFundDf = DeltaTableAsCurrent("reference.ts_nsw_fund")
refTsNswFundGroupDf = DeltaTableAsCurrent("reference.ts_nsw_fund_group")
refVetFeeHelpFundDf = DeltaTableAsCurrent("reference.vet_fee_help_fund")
refVisaHolderTypeDf = DeltaTableAsCurrent("reference.visa_holder_type")
refWelfareStatusDf = DeltaTableAsCurrent("reference.welfare_status")
refDeliveryModeAvetmissDeliveryModeDf = DeltaTableAsCurrent(" reference.delivery_mode_avetmiss_delivery_mode")

#EBS TABLES
ebsOneebsEbs0165AddressesDf = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_addresses")
ebsOneebsEbs0165AttainmentsDf = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_attainments")
ebsOneebsEbs0165AwardsPrintedDetailsDf = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_awards_printed_details")
ebsOneebsEbs0165ConfigurableStatusesDf = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_configurable_statuses")
ebsOneebsEbs0165DisabilitiesDf = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_disabilities")
ebsOneebsEbs0165FeesListDf = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_fees_list")
ebsOneebsEbs0165FeesListTempDf = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_fees_list_temp")
ebsOneebsEbs0165FeesListWaiversDf = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_fees_list_waivers")
ebsOneebsEbs0165InstalmentPlansDf = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_instalment_plans")
ebsOneebsEbs0165LocationsDf = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_locations")
ebsOneebsEbs0165OrgUnitLinksDf = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_org_unit_links")
ebsOneebsEbs0165OrgUnitPeopleDf = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_org_unit_people")
ebsOneebsEbs0165OrganisationUnitsDf = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_organisation_units")
ebsOneebsEbs0165PeopleDf = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_people")
ebsOneebsEbs0165PeopleAsrDf = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_people_asr")
ebsOneebsEbs0165PeopleUnitsDf = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_people_units")
ebsOneebsEbs0165PeopleUnitsSpecialDf = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_people_units_special")
ebsOneebsEbs0165PeopleUsiDf = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_people_usi")
ebsOneebsEbs0165StudentStatusLogDf = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_student_status_log")
ebsOneebsEbs0165UiLinksDf = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_ui_links")
ebsOneebsEbs0165UnitInstanceOccurrencesDf = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_unit_instance_occurrences")
ebsOneebsEbs0165UnitInstancesDf = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_unit_instances")
ebsOneebsEbs0165VerifierPropertiesDf = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_verifier_properties")
ebsOneebsEbs0165VisaSubclassesDf = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_visa_subclasses")
ebsOneebsEbs0165VisasDf = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_visas")
ebsOneebsEbs0165WaiverValuesDf = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_waiver_values")
ebsOneebsEbs0165WebConfigDf = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_web_config")
ebsOneebsEbs0900UiLinksDf = DeltaTableAsCurrent("trusted.oneebs_ebs_0900_ui_links")
ebsOneebsEbs0900UnitInstancesDf = DeltaTableAsCurrent("trusted.oneebs_ebs_0900_unit_instances")
ebsOneebsEbs0165PeopleUnitLinksDf = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_people_unit_links")
ebsOneebsEbs0165FeeTypesDf = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_fee_types")
ebsOneebsEbs0165WhoToPayDf = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_who_to_pay")
ebsOneebsEbs0165FeeValuesDf = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_fee_values")


# COMMAND ----------

#Chose only the columns needed for CutOff Period
refAvetmissReportingCutOffDateDf = refAvetmissReportingCutOffDateDf.selectExpr('ReportingYear AS CutOffReportingYear', 'AvetmissCutOffReportingDate')

# COMMAND ----------

VALIDATE_ONLY = False

# COMMAND ----------

def TemplateEtl(df : object, entity, businessKey, appendMode=False):
  rawEntity = entity
  entity = GeneralToPascalCase(rawEntity)
  LogEtl(f"Starting {entity}.")
  
  if VALIDATE_ONLY:
    LogEtl(df)
  else:
    AvetmissMergeSCD(df, rawEntity, businessKey) if appendMode==False else AvetmissAppend(df, rawEntity, businessKey)
    
  LogEtl(f"Finished {entity}.")

# COMMAND ----------

def ReportingPeriod():
  TemplateEtl(df=GetAvetmissReportingPeriod(refReportingPeriodDf), 
              entity="avetmiss_reporting_period", 
              businessKey="ReportingPeriodKey")

def Location():
  TemplateEtl(df=GetAvetmissLocation(ebsOneebsEbs0165LocationsDf, ebsOneebsEbs0165OrganisationUnitsDf, ebsOneebsEbs0165OrgUnitLinksDf, ebsOneebsEbs0165AddressesDf, refInstituteDf, start_date, end_date), 
              entity="avetmiss_location", 
              businessKey="LocationCode")
  
def Student():
  TemplateEtl(df=GetAvetmissStudent(ebsOneebsEbs0165PeopleDf, ebsOneebsEbs0165StudentStatusLogDf, refStudentStatusDf, ebsOneebsEbs0165AddressesDf, refCountryDf, ebsOneebsEbs0165VisasDf, refVisaHolderTypeDf, ebsOneebsEbs0165VisaSubclassesDf, refEnglishLevelDf, refLanguageDf, refResidentialStatusDf, refEmploymentStatusDf, refDisabilityStatusDf, ebsOneebsEbs0165PeopleUsiDf, refSeifaDf, refLgaDf, ebsOneebsEbs0165PeopleAsrDf, refPriorEducationLocationDf, refIndigenousStatusDf, start_date, end_date), 
              entity="avetmiss_student", 
              businessKey="PersonCode")
  
def StudentStatus(currentReportingPeriodDf):
  TemplateEtl(df=GetAvetmissStudentStatus(ebsOneebsEbs0165StudentStatusLogDf, refStudentStatusDf, currentReportingPeriodDf, start_date, end_date), 
              entity="avetmiss_student_status", 
              businessKey="PersonCode,ReportingYear")
  
def DisabilityType(currentReportingPeriodDf):
  TemplateEtl(df=GetAvetmissDisabilityType(ebsOneebsEbs0165DisabilitiesDf, currentReportingPeriodDf, start_date, end_date), 
              entity="avetmiss_disability_type", 
              businessKey="PersonCode,ReportingYear")
  
def Course():
  df = GetAvetmissCourse(ebsOneebsEbs0165UnitInstancesDf, ebsOneebsEbs0165UiLinksDf, refAnimalUseDf, refQualificationTypeDf, refOccupationDf, refIndustryDf, refResourceAllocationModelCategoryDf, refFieldOfEducationDf, refProductSubTypeDf, refAvetmissCourseDf, start_date, end_date)
  TemplateEtl(df, entity = "avetmiss_course", businessKey = "CourseCode")
  
def CurrentCourseMapping(courseDf):
  TemplateEtl(df=GetAvetmissCurrentCourseMapping(ebsOneebsEbs0165UnitInstancesDf, refSkillsListDf, refCourseReplacementDf, courseDf, start_date, end_date), 
              entity="avetmiss_current_course_mapping", 
              businessKey="OldCourseCode")
  
def Unit():
  TemplateEtl(df=GetAvetmissUnit(ebsOneebsEbs0165UnitInstancesDf, ebsOneebsEbs0900UnitInstancesDf, refAnimalUseDf, refOccupationDf, refIndustryDf, refResourceAllocationModelCategoryDf, refFieldOfEducationDf, refProductSubTypeDf, start_date, end_date),
              entity="avetmiss_unit", 
              businessKey="UnitId,UnitCode")
  
def CourseOffering(complianceLocationDf):
  TemplateEtl(df=GetAvetmissCourseOffering(ebsOneebsEbs0165UnitInstanceOccurrencesDf, ebsOneebsEbs0165UnitInstancesDf, refDeliveryModeDf, refAttendanceModeDf, complianceLocationDf, ebsOneebsEbs0165OrganisationUnitsDf, refFundingSourceDf, refOfferingStatusDf, start_date, end_date),
              entity="avetmiss_course_offering", 
              businessKey="CourseOfferingID")
  
def UnitOffering(complianceLocationDf):
  TemplateEtl(df=GetAvetmissUnitOffering(ebsOneebsEbs0165UnitInstanceOccurrencesDf, ebsOneebsEbs0165UnitInstancesDf, refDeliveryModeDf, refAttendanceModeDf, complianceLocationDf, ebsOneebsEbs0165OrganisationUnitsDf, refDeliveryModeAvetmissDeliveryModeDf, start_date, end_date),
              entity="avetmiss_unit_offering", 
              businessKey="UnitOfferingID,UnitOfferingCode")

def UnitEnrolment(currentReportingPeriodDf, RefreshLinkSP):
  TemplateEtl(df=GetAvetmissUnitEnrolment(refAvetmissCourseDf, currentReportingPeriodDf, RefreshLinkSP),
              entity="avetmiss_unit_enrolment", 
              businessKey="ReportingDate", appendMode=True)
  

def CourseEnrolment(complianceUnitEnrolmentDf, complianceLocationDf, complianceStudentDf, currentReportingPeriodDf, RefreshLinkSP):
  TemplateEtl(df=GetAvetmissCourseEnrolment(complianceUnitEnrolmentDf, complianceLocationDf, complianceStudentDf, currentReportingPeriodDf, refAvetmissCourseDf, refQualificationTypeMappingDf, refStratificationGroupDf, refFundingSourceCoreFundDf, refFundingSourceDf, refFundingSourceVetFeeHelpFundDf, refVetFeeHelpFundDf, refTrainingPackageIscDf, refFieldOfEducationIscDf, refIndustrySkillsCouncilDf, refDeliveryModeDf, refAvetmissCourseSkillsPointDf, refTsNswFundDf, refTsNswFundGroupDf, refWelfareStatusDf, refOrganisationTypeDf, refQualificationTypeDf, refQualificationGroupDf, refFundingSourceSbiRulesDf, refSbiSubCategoryDf, refSbiCategoryDf, refSbiGroupDf, ebsOneebsEbs0165FeesListDf, ebsOneebsEbs0165FeesListTempDf, ebsOneebsEbs0165WebConfigDf, ebsOneebsEbs0165InstalmentPlansDf, ebsOneebsEbs0165PeopleUnitsDf, ebsOneebsEbs0165UnitInstanceOccurrencesDf, ebsOneebsEbs0165UnitInstancesDf, ebsOneebsEbs0165PeopleDf, ebsOneebsEbs0165AttainmentsDf, ebsOneebsEbs0165PeopleUnitsSpecialDf, ebsOneebsEbs0165ConfigurableStatusesDf, ebsOneebsEbs0165OrganisationUnitsDf, ebsOneebsEbs0165FeesListWaiversDf, ebsOneebsEbs0165WaiverValuesDf, ebsOneebsEbs0165AwardsPrintedDetailsDf, ebsOneebsEbs0165UnitInstancesDf, ebsOneebsEbs0900UnitInstancesDf, ebsOneebsEbs0165UiLinksDf, ebsOneebsEbs0900UiLinksDf, refFieldOfEducationDf, ebsOneebsEbs0165FeeTypesDf, ebsOneebsEbs0165WhoToPayDf, ebsOneebsEbs0165FeeValuesDf, ebsOneebsEbs0165PeopleUnitLinksDf, RefreshLinkSP),
              entity="avetmiss_course_enrolment", 
              businessKey="ReportingDate", appendMode=True)

def PrimaryLearner():
  df = GetAvetmissPrimaryLearner(ebsOneebsEbs0165PeopleDf
                                 , ebsOneebsEbs0165AddressesDf
                                 , ebsOneebsEbs0165PeopleUnitsDf
                                 , ebsOneebsEbs0165PeopleUsiDf
                                 , ebsOneebsEbs0165FeesListDf
                                 , ebsOneebsEbs0165UnitInstanceOccurrencesDf
                                 , ebsOneebsEbs0165UnitInstancesDf
                                 , ebsOneebsEbs0165AttainmentsDf, start_date, end_date)
  TemplateEtl(df, entity = "avetmiss_primary_learner", businessKey = "PersonCode")
   

# COMMAND ----------

#TODO: PLACE THIS INTO A DEVOPS SCRIPT FOR CI/CD
def DatabaseChanges():
  #CREATE stage AND compliance DATABASES IF NOT PRESENT
  spark.sql("CREATE DATABASE IF NOT EXISTS stage")
  spark.sql("CREATE DATABASE IF NOT EXISTS compliance")  



# COMMAND ----------

# DBTITLE 1,Main - ETL
def Main():
  DatabaseChanges()
  ReportingPeriod()

  #determine current reporting periods based on the next reporting date    
  complianceReportingPeriodDf = DeltaTableAsCurrent("compliance.avetmiss_reporting_period", True)
  
  if ADS_ENVIRONMENT == "":
    #If the environment is Prod then get the latest date from the Reporting table
    latestReportingDate = complianceReportingPeriodDf.filter(col("ReportingDate") <= to_date(current_timestamp())).agg(max("ReportingDate")).collect()[0][0]
  elif ADS_ENVIRONMENT.lower() == "p":
    #If the environment is PreProd then take the snapshot as of 2020-12-06. The date the environment was refreshed from OneEBS
    latestReportingDate = lit('2020-12-06')
  elif ADS_ENVIRONMENT.lower() == "t":
    #If the environment is TEST then take the snapshot as of 2020-12-06. The date the environment was refreshed from OneEBS
    latestReportingDate = lit('2020-07-26')
  else:
    latestReportingDate = lit('2021-01-01')
  LogEtl(f"Reporting Date: {latestReportingDate}")
  
  currentReportingPeriodDf = complianceReportingPeriodDf.filter(col("ReportingDate") == latestReportingDate) \
      .select("ReportingDate", "ReportingYear", "ReportingYearStartDate", "ReportingYearEndDate", "ReportingYearNo")
  
  #Exclude Dates outside of CutOff Period Range
  currentReportingPeriodDf = currentReportingPeriodDf.join(refAvetmissReportingCutOffDateDf, ((currentReportingPeriodDf.ReportingYear == refAvetmissReportingCutOffDateDf.CutOffReportingYear) & (currentReportingPeriodDf.ReportingDate <= refAvetmissReportingCutOffDateDf.AvetmissCutOffReportingDate)), how="inner")
  
  #==============
  # DIMENSIONS
  #==============
  #LoadDimensions = True
  #LoadFacts = True
  
  if LoadDimensions:
    LogEtl("Start Dimensions")
    Location()
    Student()
    StudentStatus(currentReportingPeriodDf)
    DisabilityType(currentReportingPeriodDf) 
    Course()
    CurrentCourseMapping(DeltaTableAsCurrent("compliance.avetmiss_course", True))
    Unit()
    PrimaryLearner()
    
    complianceLocationDf = DeltaTableAsCurrent("compliance.avetmiss_location", True)
    CourseOffering(complianceLocationDf)
    UnitOffering(complianceLocationDf)
    
    LogEtl("End Dimensions")

  #==============
  # FACTS
  #==============
  
  if LoadFacts:
    LogEtl("Start Facts")
    UnitEnrolment(currentReportingPeriodDf, RefreshLinkSP) #REFACTOR
    
    complianceUnitEnrolmentDf = DeltaTableAsCurrent("compliance.avetmiss_unit_enrolment", True).join(currentReportingPeriodDf.select('ReportingYear', 'ReportingDate'), on=['ReportingYear', 'ReportingDate'], how="inner")
    complianceLocationDf = DeltaTableAsCurrent("compliance.avetmiss_location", True)
    complianceStudentDf = DeltaTableAsCurrent("compliance.avetmiss_student", True)
    
    CourseEnrolment(complianceUnitEnrolmentDf, complianceLocationDf, complianceStudentDf, currentReportingPeriodDf, RefreshLinkSP)
    
    LogEtl("End Facts")
  


# COMMAND ----------

Main()

# COMMAND ----------

dbutils.notebook.exit("1")
