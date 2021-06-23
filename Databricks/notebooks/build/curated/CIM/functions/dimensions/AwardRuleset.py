# Databricks notebook source
###########################################################################################################################
# Function: GetCimAwardRuleset
#  GETS AwardRuleset DIMENSION 
# Parameters: 
#  awardRulesetDf = OneEBS_EBS_0165_award_rulesets
# Returns:
#  Dataframe of transformed AwardRuleset
#############################################################################################################################
def GetCIMAwardRuleset(AwardRulesetDf):
  spark.udf.register("TidyCase", GeneralToTidyCase)
  #ALIAS TABLES
  df = GeneralAliasDataFrameColumns(AwardRulesetDf, "ar_")
  
  #JOIN TABLES
  
  #SELECT / TRANSFORM
  df = df.selectExpr(
	 "CAST(ar_ID as BIGINT) as AwardRulesetId"
    ,"ar_AWARD_ID as AwardId"
    ,"ar_RULESET_ID as RuleSet"
    ,"ar_START_DATE as StartDate"
	,"ar_END_DATE as EndDate"
    ,"ROW_NUMBER() OVER( PARTITION BY ar_ID ORDER BY ar__transaction_date desc) AS AwardRulesetRank"
  )
  
  df = df.where(expr("AwardRulesetRank == 1")).drop("AwardRulesetRank") \
  .orderBy("AwardRulesetId")

  return df
