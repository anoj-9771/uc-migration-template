# Databricks notebook source
###########################################################################################################################
# Function: GetAvetmissUnit
#  GETS UNIT DIMENSION 
# Parameters: 
#  ebsOneebsEbs0165UnitInstancesDf = trusted.oneebs_ebs_0165_unit_instances
#  ebsOneebsEbs0900UnitInstancesDf = trusted.oneebs_ebs_0900_unit_instances
#  refAnimalUseDf = reference.animal_use
#  refOccupationDf = reference.occupation
#  refIndustryDf = reference.industry
#  refResourceAllocationModelCategoryDf = reference.resource_allocation_model_category
#  refFieldOfEducationDf = reference.field_of_education
#  refProductSubTypeDf = reference.product_sub_type
# Returns:
#  Dataframe of transformed Location
#############################################################################################################################
def GetAvetmissUnit(ebsOneebsEbs0165UnitInstancesDf, ebsOneebsEbs0900UnitInstancesDf, refAnimalUseDf, refOccupationDf, refIndustryDf, refResourceAllocationModelCategoryDf, refFieldOfEducationDf, refProductSubTypeDf, startDate, endDate):
  #ALIAS TABLES
  df = GeneralAliasDataFrameColumns(ebsOneebsEbs0165UnitInstancesDf, "unit_instances_0165_").where("CTYPE_CALENDAR_TYPE_CODE = 'UNIT' AND FES_STATUS NOT IN ('TGAREF', 'DRAFT')")
  unit_instances_0900Df = GeneralAliasDataFrameColumns(ebsOneebsEbs0900UnitInstancesDf, "unit_instances_0900_")
  AnimalUseDf = GeneralAliasDataFrameColumns(refAnimalUseDf, "AnimalUse_")
  OccupationDf = GeneralAliasDataFrameColumns(refOccupationDf, "Occupation_")
  IndustryDf = GeneralAliasDataFrameColumns(refIndustryDf, "Industry_")
  RAMDf = GeneralAliasDataFrameColumns(refResourceAllocationModelCategoryDf, "RAM_")
  FieldOfEducationDf = GeneralAliasDataFrameColumns(refFieldOfEducationDf, "FieldOfEducation_")
  ProductSubTypeDf = GeneralAliasDataFrameColumns(refProductSubTypeDf, "ProductSubType_")
  
  #JOIN TABLES
  df = df.join(unit_instances_0900Df, unit_instances_0900Df.unit_instances_0900_FES_UNIT_INSTANCE_CODE == df.unit_instances_0165_FES_UNIT_INSTANCE_CODE, how="left") \
    .join(AnimalUseDf, AnimalUseDf.AnimalUse_AnimalUseCode == df.unit_instances_0165_FES_USER_5, how="left") \
    .join(OccupationDf, OccupationDf.Occupation_OccupationCode == df.unit_instances_0165_COURSE_OCCUPATION, how="left") \
    .join(IndustryDf, IndustryDf.Industry_IndustryCode == df.unit_instances_0165_CENSUS_SUBJECT, how="left") \
    .join(RAMDf, RAMDf.RAM_ResourceAllocationModelCategoryID == df.unit_instances_0165_RAM_CATEGORY, how="left") \
    .join(FieldOfEducationDf, FieldOfEducationDf.FieldOfEducation_FieldOfEducationID == df.unit_instances_0165_NZSCED, how="left") \
    .join(ProductSubTypeDf, ProductSubTypeDf.ProductSubType_ProductSubTypeCode == df.unit_instances_0165_UNIT_CATEGORY, how="left") 
  
  #INCREMENTALS
  df = df.where(expr(f"unit_instances_0165__DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
     OR unit_instances_0900__DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
     OR ProductSubType__DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
     OR RAM__DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
     OR FieldOfEducation__DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
     OR AnimalUse__DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
     OR Industry__DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
     OR Occupation__DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
     "))

  #SELECT / TRANSFORM #PRADA-1275
  df = df.selectExpr(
	"CAST(unit_instances_0165_UI_ID AS BIGINT) as UnitId"
    ,"unit_instances_0165_FES_UNIT_INSTANCE_CODE as UnitCode"
    ,"unit_instances_0165_FES_LONG_DESCRIPTION as UnitName"
    ,"unit_instances_0165_FES_STATUS as UnitStatus"
    ,"unit_instances_0165_FES_USER_5 as AnimalUseCode"
    ,"AnimalUse_AnimalUseName AS AnimalUseName"
    ,"unit_instances_0165_COURSE_OCCUPATION as OccupationCode"
    ,"Occupation_OccupationName AS OccupationName"
    ,"unit_instances_0165_CENSUS_SUBJECT as IndustryCode"
    ,"Industry_IndustryName AS IndustryName"
    ,"unit_instances_0165_RAM_CATEGORY as ResourceAllocationModelCategoryCode"
    ,"RAM_ResourceAllocationModelCategoryName AS ResourceAllocationModelCategoryName"
    ,"unit_instances_0165_NZSCED as FieldOfEducationId"
    ,"FieldOfEducation_FieldOfEducationName AS FieldOfEducationName"
    ,"FieldOfEducation_FieldOfEducationDescription AS FieldOfEducationDescription"
    ,"unit_instances_0165_UNIT_CATEGORY as UnitCategory"
    ,"ProductSubType_ProductSubTypeName AS UnitCategoryName"
    ,"unit_instances_0900_FES_LONG_DESCRIPTION as TGAUnitName"
    ,"cast(NULL as varchar(250)) as Sponsor /*???*/"
    ,"unit_instances_0165_SCOPE_APPROVED AS ScopeApproved"
    ,"unit_instances_0165_FES_USER_10 AS RecommendedUsage"
    ,"cast(NULL as varchar(250)) as ProgramArea /*???*/"
    ,"unit_instances_0165_NATIONAL_COURSE_CODE as NationalCourseCode"
    ,"CASE \
      WHEN unit_instances_0165_IS_VOCATIONAL = 'Y' THEN 'Yes - Vocational' \
      ELSE 'No - Vocational' \
     END AS IsVocational"
   ,"unit_instances_0165_MAXIMUM_HOURS as NominalHours"
   ,"unit_instances_0165_UI_type as UIType"
  )
  
  df = df.filter(instr(df.UnitName, 'DO NOT USE') == 0)
  return df
