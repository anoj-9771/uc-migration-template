# Databricks notebook source
# MAGIC %run ./includes/util-cim-common

# COMMAND ----------

# MAGIC %run ./functions/cim-functions-dimensions

# COMMAND ----------

# MAGIC %run ./functions/cim-functions-facts

# COMMAND ----------

# DBTITLE 1,Parameters
#Set Parameters
dbutils.widgets.removeAll()

dbutils.widgets.text("Start_Date","")
dbutils.widgets.text("End_Date","")

#test repo comment

#Get Parameters
start_date = dbutils.widgets.get("Start_Date")
end_date = dbutils.widgets.get("End_Date")

params = {"start_date": start_date, "end_date": end_date}

#DEFAULT IF ITS BLANK
start_date = "2000-01-01" if not start_date else start_date
end_date = "9999-12-31" if not end_date else end_date

#Print Date Range
print(f"Start_Date = {start_date}| End_Date = {end_date}")

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
refAQFLevelDf = DeltaTableAsCurrent("reference.aqf_level")
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
refMasterReferenceDataDf = DeltaTableAsCurrent("reference.master_reference_data")


#EBS TABLES
ebsOneebsEbs0165AccreditationDf = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_ui_accreditation")
ebsOneebsEbs0165AddressesDf = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_addresses")
ebsOneebsEbs0165AwardDf = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_awards")
ebsOneebsEbs0165AwardRuleSetDf = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_award_rulesets")
ebsOneebsEbs0165CourseAssessmentDf = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_course_assessments")
ebsOneebsEbs0165GradingSchemeDf = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_grading_schemes")
ebsOneebsEbs0165RuleSetsDf = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_rulesets")
ebsOneebsEbs0165RuleSetsRulesDf = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_ruleset_rules")
ebsOneebsEbs0165RulesDf = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_rules")
ebsOneebsEbs0165UnitInstanceOccurrencesDf = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_unit_instance_occurrences")
ebsOneebsEbs0165UIOLinksDf = DeltaTableAsCurrent("trusted.Oneebs_ebs_0165_UIO_LINKS")
ebsOneebsEbs0165VerifiersDf = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_verifiers")
ebsOneebsEbs0165UnitInstancesDf = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_unit_instances")
ebsOneebsEbs0165UnitInstanceAwardDf = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_unit_instance_awards")
ebsOneebsEbs0165LinkDf = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_ui_links")
ebsOneebsEbs0165LocationsDf = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_locations")
ebsOneebsEbs0165OrgUnitLinksDf = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_org_unit_links")
ebsOneebsEbs0165OrganisationUnitsDf = DeltaTableAsCurrent("trusted.oneebs_ebs_0165_organisation_units")

# COMMAND ----------

def TemplateEtl(df : object, entity, businessKey, AddSK = True):
  rawEntity = entity
  entity = GeneralToPascalCase(rawEntity)
  LogEtl(f"Starting {entity}.")
  
  v_CIM_SQL_SCHEMA = "CIM"
  v_CIM_CURATED_DATABASE = "CIM"
  v_CIM_DATALAKE_FOLDER = "CIM"
  
  #CIMMergeSCD(df, rawEntity, businessKey) if appendMode==False else CIMAppend(df, rawEntity, businessKey)
  DeltaSaveDataFrameToDeltaTable(
df, rawEntity, ADS_DATALAKE_ZONE_CURATED, v_CIM_CURATED_DATABASE, v_CIM_DATALAKE_FOLDER, ADS_WRITE_MODE_MERGE, track_changes = True, is_delta_extract = False, business_key = businessKey, AddSKColumn = AddSK, delta_column = "", start_counter = "0", end_counter = "0")

  delta_table = f"{v_CIM_SQL_SCHEMA}.{rawEntity}"
  DeltaSyncToSQLEDW(delta_table, v_CIM_SQL_SCHEMA, entity, businessKey, delta_column = "", start_counter = "0", data_load_mode = ADS_WRITE_MODE_MERGE, track_changes = True, is_delta_extract = False, schema_file_url = "", additional_property = "")
    
  LogEtl(f"Finished {entity}.")

# COMMAND ----------

def Award():
  TemplateEtl(df=GetCIMAward(ebsOneebsEbs0165AwardDf,ebsOneebsEbs0165GradingSchemeDf,ebsOneebsEbs0165VerifiersDf), 
             entity="Award", 
             businessKey="AwardCode,AwardId"
            )
def AwardRuleset():
  TemplateEtl(df=GetCIMAwardRuleset(ebsOneebsEbs0165AwardRuleSetDf), 
             entity="AwardRuleset",
             businessKey="AwardRulesetId"
            )
def Unit():
  TemplateEtl(df=GetCIMUnit(ebsOneebsEbs0165UnitInstancesDf, refAnimalUseDf, refOccupationDf, refIndustryDf, refResourceAllocationModelCategoryDf, refFieldOfEducationDf, refProductSubTypeDf, start_date, end_date),
              entity="Unit", 
              businessKey="UnitId,UnitCode")
def Course():
  df = GetCIMCourse(ebsOneebsEbs0165UnitInstancesDf, refAnimalUseDf, refQualificationTypeDf, refOccupationDf, refIndustryDf, refFieldOfEducationDf, refProductSubTypeDf, refAQFLevelDf, start_date, end_date)
  TemplateEtl(df, entity = "Course", businessKey = "CourseId,CourseCode")
def UnitAccreditation():
  TemplateEtl(df=GetCIMUnitAccreditation(ebsOneebsEbs0165AccreditationDf, ebsOneebsEbs0165UnitInstancesDf), 
             entity="UnitAccreditation",
             businessKey="AccreditationId"
            )
def CourseAccreditation():
  TemplateEtl(df=GetCIMCourseAccreditation(ebsOneebsEbs0165AccreditationDf, ebsOneebsEbs0165UnitInstancesDf), 
             entity="CourseAccreditation",
             businessKey="AccreditationId"
            )
def CourseAward():
  TemplateEtl(df=GetCIMCourseAward(ebsOneebsEbs0165UnitInstanceAwardDf, ebsOneebsEbs0165AwardDf, ebsOneebsEbs0165UnitInstancesDf), 
             entity="CourseAward",
             businessKey="CourseCode,AwardId"
            )
def CourseUnit():
  TemplateEtl(df=GetCIMCourseUnit(ebsOneebsEbs0165LinkDf, ebsOneebsEbs0165UnitInstancesDf), 
             entity="CourseUnit",
             businessKey="CourseCode,UnitCode"
            )
def Location():
  TemplateEtl(df=GetCIMLocation(ebsOneebsEbs0165LocationsDf, ebsOneebsEbs0165AddressesDf, ebsOneebsEbs0165OrganisationUnitsDf, ebsOneebsEbs0165OrgUnitLinksDf, refInstituteDf), 
              entity="Location", 
              businessKey="LocationCode")
def TeachingSection():
  TemplateEtl(df=GetCIMTeachingSection(ebsOneebsEbs0165OrganisationUnitsDf), 
              entity="TeachingSection", 
              businessKey="TeachingSectionCode")
def OfferingSkillsPoint():
  TemplateEtl(df=GetCIMOfferingSkillsPoint(ebsOneebsEbs0165OrganisationUnitsDf), 
              entity="OfferingSkillsPoint", 
              businessKey="OfferingSkillsPointCostCentreCode")
def UnitOffering():
  TemplateEtl(df=GetCIMUnitOffering(ebsOneebsEbs0165UnitInstanceOccurrencesDf,refMasterReferenceDataDf,ebsOneebsEbs0165UIOLinksDf,start_date,end_date), 
              entity="UnitOffering", 
              businessKey="UnitOfferingID")
def CourseOffering():
  TemplateEtl(df=GetCIMCourseOffering(ebsOneebsEbs0165UnitInstanceOccurrencesDf,start_date,end_date), 
              entity="CourseOffering", 
              businessKey="CourseOfferingID")
  


# COMMAND ----------

def UnitOfferings():
  TemplateEtl(df=GetCIMUnitOfferings(ebsOneebsEbs0165UnitInstanceOccurrencesDf,start_date,end_date),
  entity="UnitOfferings",
  businessKey="UnitOfferingsKey",
  AddSK=False)


# COMMAND ----------

def CourseOfferings():
  TemplateEtl(df=GetCIMCourseOfferings(ebsOneebsEbs0165UnitInstanceOccurrencesDf,start_date,end_date),
  entity="CourseOfferings",
  businessKey="CourseOfferingsKey",
  AddSK=False)

# COMMAND ----------

#TODO: PLACE THIS INTO A DEVOPS SCRIPT FOR CI/CD
def DatabaseChanges():
  #CREATE stage AND cim DATABASES IS NOT PRESENT
  spark.sql("CREATE DATABASE IF NOT EXISTS stage")
  spark.sql("CREATE DATABASE IF NOT EXISTS cim")  

  #PRADA-1655 - ADD NEW LocationOrganisationCode COLUMN
  #try:
   # spark.sql("ALTER TABLE cim.location ADD COLUMNS ( LocationOrganisationCode string )")
 # except:
  #  e=False

# COMMAND ----------

LoadDimensions = True
LoadFacts = True

# COMMAND ----------

# DBTITLE 1,Main - ETL
def Main():
 # DatabaseChanges()
  #==============
  # DIMENSIONS
  #==============
  
  if LoadDimensions:
    LogEtl("Start Dimensions")
    Award()
    AwardRuleset()
    Unit()
    Course()
    UnitAccreditation()
    CourseAccreditation()
    CourseAward()
    CourseUnit()
    Location()
    TeachingSection()
    OfferingSkillsPoint()
    UnitOffering()
    CourseOffering()
    
    LogEtl("End Dimensions")

  #==============
  # FACTS
  #==============
  if LoadFacts:
    LogEtl("Start Facts")
    UnitOfferings()
    CourseOfferings()
  
    LogEtl("End Facts")
    
  

# COMMAND ----------

Main()
