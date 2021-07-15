# Databricks notebook source
###########################################################################################################################
# Function: GetCimOfferingSkillsPoint
#  GETS OfferingSkillsPoint DIMENSION 
# Parameters: 
#  orgUnitDf = OneEBS_EBS_0165_Organisation_units
# Returns:
#  Dataframe of transformed OfferingSkillsPoint
#############################################################################################################################
def GetCIMOfferingSkillsPoint(OrgUnitDf):
  spark.udf.register("TidyCase", GeneralToTidyCase)
  #ALIAS TABLES
  df = GeneralAliasDataFrameColumns(OrgUnitDf.where("ORGANISATION_TYPE = 'FA'"), "OU_")

  #JOIN TABLES

  #SELECT / TRANSFORM
  df = df.selectExpr(
	 "OU_ORGANISATION_CODE as OfferingSkillsPointCostCentreCode"
    ,"OU_FES_FULL_NAME as OfferingSkillsPointName"
    ,"ROW_NUMBER() OVER(PARTITION BY OU_ORGANISATION_CODE ORDER BY OU__transaction_date desc) AS TeachingSectionRank"
  )
  
  df = df.where(expr("TeachingSectionRank == 1")).drop("TeachingSectionRank") \
  .orderBy("OfferingSkillsPointCostCentreCode")

  return df
