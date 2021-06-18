# Databricks notebook source
###########################################################################################################################
# Function: GetCimAward
#  GETS AWARD DIMENSION 
# Parameters: 
#  awardDf = OneEBS_EBS_0165_awards
#  gradingSchemeDf = OneEBS_EBS_0165_grading_schemes
#  verifiersDf = OneEBS_EBS_0165_verifiers
# Returns:
#  Dataframe of transformed AWARD
#############################################################################################################################
def GetCIMAward(AwardDf, GradingSchemeDf, VerifiersDf):
  spark.udf.register("TidyCase", GeneralToTidyCase)
  #ALIAS TABLES
  df = GeneralAliasDataFrameColumns(AwardDf, "a_")
  gsdf = GeneralAliasDataFrameColumns(GradingSchemeDf, "gs_")
  vdf = GeneralAliasDataFrameColumns(VerifiersDf.where("RV_DOMAIN = 'QUAL_AWARD_CATEGORY_CODE'"), "v_")

  #JOIN TABLES
  df = df.join(gsdf, expr("a_GRADING_SCHEME_ID = gs_ID and gs__RecordCurrent = 1"), how="left")
  df = df.join(vdf, expr("a_AWARD_CATEGORY_CODE = v_LOW_VALUE and v__RecordCurrent = 1"), how="left")
  
  #SELECT / TRANSFORM
  df = df.selectExpr(
	 "CAST(a_ID AS BIGINT) as AwardId"
    ,"LTRIM(RTRIM(a_CODE)) as AwardCode"
    ,"a_TITLE as AwardName"
	,"a_DESCRIPTION as AwardDescription"
	,"a_NATIONAL_CODE as AwardNationalCode"
	,"a_SPECIALISATION as AwardSpecialisation"
    ,"a_AWARD_CATEGORY_CODE as AwardCategoryCode"
    ,"a_IS_ACTIVE as AwardActiveFlag"
    ,"gs_DESCRIPTION as GradingScheme"
    ,"v_FES_LONG_DESCRIPTION as QualificationType"
    ,"a_TRANSCRIPT_ID as TranscriptDocumentType"
    ,"a_USER_12 as TestamurDocumentType"
    ,"ROW_NUMBER() OVER(PARTITION BY a_ID, RTRIM(LTRIM(a_CODE)) ORDER BY a__transaction_date desc) AS AwardRank"
  )
  
  df = df.where(expr("AwardRank == 1")).drop("AwardRank") \
  .orderBy("AwardCode")

  return df
