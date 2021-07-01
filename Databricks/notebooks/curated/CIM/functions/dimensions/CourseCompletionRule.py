# Databricks notebook source
###########################################################################################################################
# Function: GetCimCourseCompletionRule
#  GETS COURSE COMPLETION RULE DIMENSION 
# Parameters:
# Returns:
#  Dataframe of transformed COURSE COMPLETION RULE DIMENSION 
#############################################################################################################################
def  GetCimCourseCompletionRule(AwardRuleSetDf,AwardDf,UnitInstanceOccurancesDf,RuleSetRulesDf, RulesDf,RuleSetDf,verifiersDf):
  spark.udf.register("TidyCase", GeneralToTidyCase)
  #ALIAS TABLES
  rdf = GeneralAliasDataFrameColumns(AwardRuleSetDf, "r_")
  aDf = GeneralAliasDataFrameColumns(AwardDf, "a_")
  UIDF = GeneralAliasDataFrameColumns(UnitInstanceOccurancesDf, "UIO_")
  rrDf = GeneralAliasDataFrameColumns(RuleSetRulesDf, "rr_")
  rsDf = GeneralAliasDataFrameColumns(RuleSetDf, "rs_")
  rlDf= GeneralAliasDataFrameColumns(RulesDf, "rl_")
  vDf= GeneralAliasDataFrameColumns(verifiersDf, "v_")
  
  #JOIN TABLES
  df = aDf \
    .join(UIDF, UIDF.UIO_AWARD_CODE == aDf.a_CODE, how = "inner") \
    .join(rdf, rdf.r_AWARD_ID == aDf.a_ID, how="inner") \
    .join(rsDf, rsDf.rs_ID == rdf.r_RULESET_ID, how="inner") \
    .join(rrDf, rrDf.rr_RULESET_ID ==  rdf.r_RULESET_ID, how="inner") \
    .join(rlDf, rlDf.rl_ID == rrDf.rr_RULESET_ID, how="inner")
  
  
  #SELECT / TRANSFORM
  df = df.selectExpr(	
  "a_CODE as AwardCode",
  "UIO_CALOCC_OCCURRENCE_CODE as CaloccCode",
  "a_NATIONAL_CODE as NationalCode",
  "a_DESCRIPTION as AwardName",
  "rl_DESCRIPTION as CompletionRule",
  "a_AWARD_CATEGORY_CODE as AwardQualificationLevel"
  )
  
  return df
