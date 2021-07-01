# Databricks notebook source
###########################################################################################################################
# Function: GetCimTeachingSection
#  GETS TeachingSection DIMENSION 
# Parameters: 
#  orgUnitDf = OneEBS_EBS_0165_Organisation_units
# Returns:
#  Dataframe of transformed CIM TeachingSection
#############################################################################################################################
def GetCIMTeachingSection(OrgUnitDf):
  spark.udf.register("TidyCase", GeneralToTidyCase)
  #ALIAS TABLES
  df = GeneralAliasDataFrameColumns(OrgUnitDf.where("ORGANISATION_TYPE = 'TS'"), "OU_")

  #JOIN TABLES
  
  #SELECT / TRANSFORM
  df = df.selectExpr(
	 "OU_ORGANISATION_CODE as TeachingSectionCode"
    ,"CASE WHEN SUBSTRING(OU_FES_FULL_NAME, 4, 1) = '-' THEN SUBSTRING(OU_FES_FULL_NAME, 5, CHAR_LENGTH(OU_FES_FULL_NAME) - 4) ELSE OU_FES_FULL_NAME END as TeachingSectionDescription"
    ,"ROW_NUMBER() OVER(PARTITION BY OU_ORGANISATION_CODE ORDER BY OU__transaction_date desc) AS TeachingSectionRank"
  )
  
  df = df.where(expr("TeachingSectionRank == 1")).drop("TeachingSectionRank") \
  .orderBy("TeachingSectionCode")

  return df
