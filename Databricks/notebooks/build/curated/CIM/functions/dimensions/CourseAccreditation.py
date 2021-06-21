# Databricks notebook source
###########################################################################################################################
# Function: GetCimCourseAccreditation
#  GETS CourseAccreditation DIMENSION 
# Parameters: 
#  AccreditationDf = OneEBS_EBS_0165_accreditation
# Returns:
#  Dataframe of transformed CourseAccreditation
#############################################################################################################################
def GetCIMCourseAccreditation(AccreditationDf, unitInstancesDf):
  spark.udf.register("TidyCase", GeneralToTidyCase)
  
  #ALIAS TABLES
  dfA = GeneralAliasDataFrameColumns(AccreditationDf, "A_")
  dfUI = GeneralAliasDataFrameColumns(unitInstancesDf.where("UI_LEVEL = 'COURSE'"), "UI_")
  
  #JOIN TABLES
  df = dfA.join(dfUI, dfUI.UI_FES_UNIT_INSTANCE_CODE == dfA.A_UNIT_INSTANCE_CODE, how="inner") \
  
  #SELECT / TRANSFORM
  df = df.selectExpr(
	 "CAST(A_ID as BIGINT) as AccreditationId"
    ,"UI_FES_UNIT_INSTANCE_CODE as CourseCode"
    ,"A_ONGOING_MONITORING as Description"
    ,"A_APPLICATION as JobRoles"
    ,"A_COPYRIGHT_ACKNOWLEDGEMENT as PathwayIn"
	,"A_CONSULTATION as PathwayFrom"
    ,"A_PHYSICAL_RESOURCES as EntryRequirements"
    ,"A_LICENSING_INFORMATION as Licensing"
    ,"A_ASSESSMENT_STRATEGIES as UnitDescriptor"
    ,"A_FRANCHISING as ApplicationOfUnit"
    ,"A_OUTCOMES as Outcomes"
    ,"A_ACCREDITATION_AUTHORITY as AccreditationAuthority"
    ,"ROW_NUMBER() OVER( PARTITION BY A_ID ORDER BY A__transaction_date desc) AS CourseAccreditationRank"
  )
  
  df = df.where(expr("CourseAccreditationRank == 1")).drop("CourseAccreditationRank") \
  .orderBy("AccreditationId")

  return df
