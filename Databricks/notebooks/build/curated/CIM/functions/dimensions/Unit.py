# Databricks notebook source
###########################################################################################################################
# Function: GetCIMUnit
#  GETS UNIT DIMENSION 
# Parameters: 
#  ebsOneebsEbs0165UnitInstancesDf = trusted.oneebs_ebs_0165_unit_instances
#  refAnimalUseDf = reference.animal_use
#  refOccupationDf = reference.occupation
#  refIndustryDf = reference.industry
#  refResourceAllocationModelCategoryDf = reference.resource_allocation_model_category
#  refFieldOfEducationDf = reference.field_of_education
#  refProductSubTypeDf = reference.product_sub_type
# Returns:
#  Dataframe of transformed Location
#############################################################################################################################
def GetCIMUnit(ebsOneebsEbs0165UnitInstancesDf, refAnimalUseDf, refOccupationDf, refIndustryDf, refResourceAllocationModelCategoryDf, refFieldOfEducationDf, refProductSubTypeDf, startDate, endDate):
  #ALIAS TABLES
  df = GeneralAliasDataFrameColumns(ebsOneebsEbs0165UnitInstancesDf, "ui_").where("UI_LEVEL = 'UNIT'")
  AnimalUseDf = GeneralAliasDataFrameColumns(refAnimalUseDf, "au_")
  OccupationDf = GeneralAliasDataFrameColumns(refOccupationDf, "o_")
  IndustryDf = GeneralAliasDataFrameColumns(refIndustryDf, "i_")
  RAMDf = GeneralAliasDataFrameColumns(refResourceAllocationModelCategoryDf, "ram_")
  FieldOfEducationDf = GeneralAliasDataFrameColumns(refFieldOfEducationDf, "foe_")
  ProductSubTypeDf = GeneralAliasDataFrameColumns(refProductSubTypeDf, "pst_")
  
  #JOIN TABLES
  df = df.join(AnimalUseDf, AnimalUseDf.au_AnimalUseCode == df.ui_FES_USER_5, how="left") \
    .join(OccupationDf, OccupationDf.o_OccupationCode == df.ui_COURSE_OCCUPATION, how="left") \
    .join(IndustryDf, IndustryDf.i_IndustryCode == df.ui_CENSUS_SUBJECT, how="left") \
    .join(RAMDf, RAMDf.ram_ResourceAllocationModelCategoryID == df.ui_RAM_CATEGORY, how="left") \
    .join(FieldOfEducationDf, FieldOfEducationDf.foe_FieldOfEducationID == df.ui_NZSCED, how="left") \
    .join(ProductSubTypeDf, ProductSubTypeDf.pst_ProductSubTypeCode == df.ui_UNIT_CATEGORY, how="left")
  
  #INCREMENTALS
  df = df.where(expr(f"ui__DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
     OR pst__DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
     OR ram__DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
     OR foe__DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
     OR au__DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
     OR i__DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
     OR o__DLTrustedZoneTimeStamp BETWEEN '{startDate}' AND '{endDate}' \
     "))

  #SELECT / TRANSFORM #PRADA-1275
  df = df.selectExpr(
	"CAST(ui_UI_ID AS BIGINT) as UnitId"
    ,"ui_FES_UNIT_INSTANCE_CODE as UnitCode"
    ,"ui_FES_LONG_DESCRIPTION as UnitName"
    ,"ui_FES_STATUS as UnitStatus"
    ,"ui_FES_USER_5 as AnimalUseCode"
    ,"au_AnimalUseName AS AnimalUseName"
    ,"ui_COURSE_OCCUPATION as OccupationCode"
    ,"o_OccupationName AS OccupationName"
    ,"ui_CENSUS_SUBJECT as IndustryCode"
    ,"i_IndustryName AS IndustryName"
    ,"ui_NZSCED as FieldOfEducationId"
    ,"foe_FieldOfEducationName AS FieldOfEducationName"
    ,"foe_FieldOfEducationDescription AS FieldOfEducationDescription"
    ,"ui_UNIT_CATEGORY as UnitCategory"
    ,"pst_ProductSubTypeName AS UnitCategoryName"
    ,"ui_SCOPE_APPROVED AS ScopeApproved"
    ,"ui_FES_USER_10 AS RecommendedUsage"
    ,"ui_NATIONAL_COURSE_CODE as NationalUnitCode"
    ,"ui_IS_VOCATIONAL AS IsVocational"
    ,"ui_MAXIMUM_HOURS as NominalHours"
    ,"ui_UI_type as UnitType"
    ,"ui_EFFECTIVE_START_DATE as StartDate"
    ,"ui_EFFECTIVE_END_DATE as EndDate"
  )
  
  df = df.filter(instr(df.UnitName, 'DO NOT USE') == 0)
  return df
