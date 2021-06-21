# Databricks notebook source
###########################################################################################################################
# Function: GetCimCourseAward
#  GETS CourseAward DIMENSION 
# Parameters: 
#  awardDf = OneEBS_EBS_0165_uint_instance_awards
#  awardDf = OneEBS_EBS_0165_awards
#  unitInstancesDf = OneEBS_EBS_0165_unit_instances
# Returns:
#  Dataframe of transformed CourseAward
#############################################################################################################################
def GetCIMCourseAward(unitInstanceAwardDf, awardDf, unitInstancesDf):
  spark.udf.register("TidyCase", GeneralToTidyCase)
  
  #ALIAS TABLES
  dfUIA = GeneralAliasDataFrameColumns(unitInstanceAwardDf, "UIA_")
  dfA = GeneralAliasDataFrameColumns(awardDf, "A_")
  dfUI = GeneralAliasDataFrameColumns(unitInstancesDf.where("UI_LEVEL = 'COURSE'"), "UI_")
  
  #JOIN TABLES
  df = dfUIA.join(dfA, dfA.A_ID == dfUIA.UIA_AWARD_ID, how="inner")  \
      .join(dfUI, dfUI.UI_FES_UNIT_INSTANCE_CODE == dfUIA.UIA_UNIT_INSTANCE_CODE, how="inner") 
  
  #SELECT / TRANSFORM
  df = df.selectExpr(
     "UI_FES_UNIT_INSTANCE_CODE as CourseCode"
    ,"CAST(A_ID as bigint) as AwardId"
    ,"UIA_MAIN as IsMainAward"
    ,"UIA_TYPE as Type"
    ,"ROW_NUMBER() OVER( PARTITION BY UIA_UNIT_INSTANCE_CODE, UIA_AWARD_ID ORDER BY UIA__transaction_date desc) AS CourseAwardRank"
  )
  
  df = df.where(expr("CourseAwardRank == 1")).drop("CourseAwardRank") \
  .orderBy("AwardId")

  return df
