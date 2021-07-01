# Databricks notebook source
###########################################################################################################################
# Function: GetCimUnitAccreditation
#  GETS UnitAccreditation DIMENSION 
# Parameters: 
#  AccreditationDf = OneEBS_EBS_0165_accreditation
# Returns:
#  Dataframe of transformed UnitAccreditation
#############################################################################################################################
def GetCIMUnitAccreditation(AccreditationDf, unitInstancesDf):
  spark.udf.register("TidyCase", GeneralToTidyCase)
  
  #ALIAS TABLES
  dfA = GeneralAliasDataFrameColumns(AccreditationDf, "A_")
  dfUI = GeneralAliasDataFrameColumns(unitInstancesDf.where("UI_LEVEL = 'UNIT'"), "UI_")
  
  #JOIN TABLES
  df = dfA.join(dfUI, dfUI.UI_FES_UNIT_INSTANCE_CODE == dfA.A_UNIT_INSTANCE_CODE, how="inner") \
  
  #SELECT / TRANSFORM
  df = df.selectExpr(
	 "CAST(A_ID as BIGINT) as AccreditationId"
    ,"UI_FES_UNIT_INSTANCE_CODE as UnitCode"
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
    ,"ROW_NUMBER() OVER( PARTITION BY A_ID ORDER BY A__transaction_date desc) AS UnitAccreditationRank"
  )
  
  df = df.where(expr("UnitAccreditationRank == 1")).drop("UnitAccreditationRank") \
  .orderBy("AccreditationId")

  return df
