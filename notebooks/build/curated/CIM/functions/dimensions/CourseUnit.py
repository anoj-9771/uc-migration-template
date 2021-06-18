# Databricks notebook source
###########################################################################################################################
# Function: GetCimCourseUnit
#  GETS CourseUnit DIMENSION 
# Parameters: 
#  LinkDf = OneEBS_EBS_0165_ui_links
#  unitInstancesDf = OneEBS_EBS_0165_unit_instances
# Returns:
#  Dataframe of transformed CourseUnit
#############################################################################################################################
def GetCIMCourseUnit(LinkDf, unitInstancesDf):
  spark.udf.register("TidyCase", GeneralToTidyCase)
  
  #ALIAS TABLES
  dfL = GeneralAliasDataFrameColumns(LinkDf.where("LINK_TYPE IN ('STRUCTURAL','COREQ')"), "L_")
  dfUIU = GeneralAliasDataFrameColumns(unitInstancesDf.where("UI_LEVEL = 'UNIT'"), "UIU_")
  dfUIC = GeneralAliasDataFrameColumns(unitInstancesDf.where("UI_LEVEL = 'COURSE'"), "UIC_")
  
  #JOIN TABLES
  df = dfL.join(dfUIU, dfUIU.UIU_FES_UNIT_INSTANCE_CODE == dfL.L_UI_CODE_FROM, how="inner") 
  df = dfL.join(dfUIC, dfUIC.UIC_FES_UNIT_INSTANCE_CODE == dfL.L_UI_CODE_TO, how="inner") 
  
  #SELECT / TRANSFORM
  df = df.selectExpr(
     "L_UI_CODE_FROM as CourseCode"
    ,"L_UI_CODE_TO as UnitCode"
    ,"ROW_NUMBER() OVER( PARTITION BY L_UI_CODE_FROM, L_UI_CODE_TO ORDER BY L__transaction_date desc) AS CourseUnitRank"
  )
  
  df = df.where(expr("CourseUnitRank == 1")).drop("CourseUnitRank")

  return df
