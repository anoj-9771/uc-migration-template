# Databricks notebook source
from pyspark.sql.functions import *
from datetime import datetime
from pyspark.sql.window import *
from pyspark.sql.types import *

# COMMAND ----------

def SaveChecklistActions(dfChkChecklists, dfChkNameSegments, dfChkActions, dfChkTasks, dfChkTaskStauses, dfChkStauses, dfChkStauses2, dfChkInstances):
  
  #Alias Column Names so that they are unique
  dfChkChecklists=GeneralAliasDataFrameColumns(dfChkChecklists, "CC_")
  dfChkNameSegments=GeneralAliasDataFrameColumns(dfChkNameSegments, "CNS_")
  dfChkActions=GeneralAliasDataFrameColumns(dfChkActions, "CA_")
  dfChkTasks=GeneralAliasDataFrameColumns(dfChkTasks, "CT_")
  dfChkTaskStauses=GeneralAliasDataFrameColumns(dfChkTaskStauses, "CTS_")
  dfChkStauses=GeneralAliasDataFrameColumns(dfChkStauses, "CS_")
  dfChkStauses2=GeneralAliasDataFrameColumns(dfChkStauses2, "CS2_")
  dfChkInstances=GeneralAliasDataFrameColumns(dfChkInstances, "CI_")
  
  #Inserting data to dfTempInstanceName and updating the InstanceName field by adding extra ~ delimiters
  dfTempInstanceName = dfChkChecklists.join(dfChkInstances, dfChkChecklists.CC_CheckListId == dfChkInstances.CI_CheckListId).filter((col("CC_CheckListAcronym")=="OFFERING-NEW") |(col("CC_CheckListAcronym")=="OFFERING-AMND") |(col("CC_CheckListAcronym")=="LRN-ConEx") | (col("CC_CheckListAcronym")=="LRN-AppTrain") | (col("CC_CheckListAcronym")=="LRN-CT") |(col("CC_CheckListAcronym")=="LRN-CT/RPL") | (col("CC_CheckListAcronym")=="LRN-RPL") | (col("CC_CheckListAcronym")=="LRN-CT-RPL"))
  
  dfTempInstanceName = dfTempInstanceName.selectExpr("CC_CheckListId as ChecklistId","CI_InstanceId as InstanceId","CI_InstanceName as InstanceName").withColumn("InstanceName",concat(col("InstanceName"),lit('~'*20)))

  # Inserting data to dfTempSplitInstanceName along with positions of data needs to be picked from the delimited data
  dfTempSplitInstanceName = dfChkChecklists.join(dfChkInstances, dfChkChecklists.CC_CheckListId == dfChkInstances.CI_CheckListId).filter((col("CI_InstanceName")!="TEST") & (col("CI_InitiatedDate")>="2018-01-01"))\
    .join(dfTempInstanceName, (dfChkInstances.CI_CheckListId == dfTempInstanceName.ChecklistId) & (dfChkInstances.CI_InstanceId == dfTempInstanceName.InstanceId))\
    .join(dfChkNameSegments, dfChkInstances.CI_CheckListId == dfChkNameSegments.CNS_CheckListId)\
    .join(dfChkActions, dfChkInstances.CI_InstanceId == dfChkActions.CA_InstanceId)\
    .join(dfChkTasks, ((dfChkChecklists.CC_CheckListId == dfChkTasks.CT_CheckListId) & (dfChkActions.CA_TaskNo == dfChkTasks.CT_TaskNo)), how="left")\
    .join(dfChkTaskStauses, dfChkActions.CA_TaskStatusId == dfChkTaskStauses.CTS_TaskStatusId)\
    .join(dfChkStauses, dfChkInstances.CI_StatusId == dfChkStauses.CS_StatusId)\
    .join(dfChkStauses2, dfChkStauses2.CS2_StatusId == dfChkInstances.CI_HighestStatusId)

  dfTempSplitInstanceName = dfTempSplitInstanceName.selectExpr(
      "CI_CheckListId as CheckListId"
    , "CA_InstanceId as InstanceId"
    , "CA_TaskNo as TaskNo"
    , "CA_ActionedDate as ActionedDate"
    , "CA_CreationDate as CreationDate"
    , "CA_Actioner as Actioner"
    , "CI_InitiatedDate as InitiatedDate"
    , "CI_InitiatedBy as InitiatedBy"
    , "CI_HighestStatusId as HighestStatusId"
    , "CNS_SegmentNo as SegmentNo"
    , "CNS_ControlId as ControlId"
    , "CNS_DDLOptionsDomain as DDLOptionsDomain"
    , "InstanceName"
    , "CS_StatusName as StatusName"
    , "CTS_TaskStatusName as TaskStatusName"
    , "CS2_StatusName as Final_ChecklistStatus"
    , "CT_TaskName"
    , "CT_TaskOwnerLong"
  ).drop_duplicates()

  split_col = split(dfTempSplitInstanceName['InstanceName'], '~')
  
  dfTempSplitInstanceName = dfTempSplitInstanceName.withColumn("CT_TaskName",trim(col("CT_TaskName")))\
                                                .withColumn("CT_TaskOwnerLong",trim(col("CT_TaskOwnerLong")))\
                                                .withColumn("subval_1",split_col.getItem(0))\
                                                .withColumn("subval_2",split_col.getItem(1))\
                                                .withColumn("subval_3",split_col.getItem(2))\
                                                .withColumn("subval_4",split_col.getItem(3))\
                                                .withColumn("subval_5",split_col.getItem(4))\
                                                .withColumn("subval_6",split_col.getItem(5))\
                                                .withColumn("subval_7",split_col.getItem(6))\
                                                .withColumn("subval_8",split_col.getItem(7))\
                                                .withColumn("subval_9",split_col.getItem(8))\
                                                .withColumn("subval_10",split_col.getItem(9))\
                                                .withColumn("subval_11",split_col.getItem(10))\
                                                .withColumn("subval_12",split_col.getItem(11))\
                                                .withColumn("subval_13",split_col.getItem(12))\
                                                .withColumn("subval_14",split_col.getItem(13))\
                                                .withColumn("subval_15",split_col.getItem(14))\
                                                .withColumn("subval_16",split_col.getItem(15))\
                                                .withColumn("subval_17",split_col.getItem(16))\
                                                .withColumn("subval_18",split_col.getItem(17))\
                                                .withColumn("subval_19",split_col.getItem(18))\
                                                .withColumn("subval_20",split_col.getItem(19))\

  
  dfTempChecklistActions = _GetChecklistActions_Step1(dfChkChecklists, dfChkNameSegments, dfTempSplitInstanceName)
  
  dfTempChecklistActions = _GetChecklistActions_Step2(dfTempChecklistActions)
  
#   dfTempChecklistActions = dfTempChecklistActions.replace("???",None, "ChangeType")
  
  dfTempChecklistActions = dfTempChecklistActions \
    .withColumn("CourseCode", \
                when(instr(upper(col("CourseCode")),'V')==0, col("CourseCode") )\
                .when(instr(col("CourseCode"),'-') >= 8, col("CourseCode").substr(lit(1), instr(col("CourseCode"), '-')-1))\
                .when(instr(upper(col("CourseCode")).substr(8,80),'V') !=0, col("CourseCode").substr(lit(1), instr(col("CourseCode").substr(8,80),'V')-1+7))\
                .otherwise(col("CourseCode")))\
    .withColumn("ActionedDate", \
                when(IsValidDate_udf((dfTempChecklistActions.ActionedDate).cast(StringType()), lit("'%Y-%m-%d %H:%M:%S', ''")) == True, col("ActionedDate"))\
                .otherwise(None))\
    .withColumn("CreationDate", \
                when(IsValidDate_udf((dfTempChecklistActions.CreationDate).cast(StringType()), lit("'%Y-%m-%d %H:%M:%S', ''")) == True, col("CreationDate"))\
                .otherwise(None))\
    .withColumn("InitiatedDate", 
                when(IsValidDate_udf((dfTempChecklistActions.InitiatedDate).cast(StringType()), lit("'%Y-%m-%d %H:%M:%S', ''")) == True, col("InitiatedDate"))\
                .otherwise(None))\
  
  dfTempChecklistActions = dfTempChecklistActions.selectExpr(
      "CheckListId"
    , "InstanceId"
    , "TaskNo"
    , "ActionedDate"
    , "CreationDate"
    , "Actioner"
    , "CT_TaskName as TaskName"
    , "CT_TaskOwnerLong as TaskOwner"
    , "InitiatedDate"
    , "InitiatedBy"
    , "HighestStatusId"
    , "SkillPoint"
    , "Location"
    , "FundingType"
    , "AttendanceMode"
    , "CostCentre"
    , "VTO"
    , "CourseCode"
    , "Calocc"
    , "OfferType"
    , "LearnerNo"
    , "DOB"
    , "FirstName"
    , "LastName"
    , "Quantity"
    , "StartDate"
    , "EndDate"
    , "Year"
    , "Month"
    , "Day"
    , "Term"
    , "ChangeType"
    , "Marketing"
    , "StatusName"
    , "TaskStatusName"
    , "Final_ChecklistStatus"
  ).repartition(832).drop_duplicates()
  
  # Filter the data
  dfTempValidRecords = _GetChecklistActions_ValidRecords(dfTempChecklistActions)
  dfTempValidRecords = dfTempValidRecords.where("Year >= '2018' or Year is null")
  
  eCheckListSaveDataFrameToCurated(dfTempValidRecords, 'ChecklistActions')
  
  dfTempInvalidRecords = _GetChecklistActions_InValidRecords(dfTempChecklistActions)
  eCheckListSaveDataFrameToCurated(dfTempInvalidRecords, 'ChecklistActionsInvalidRecords')

  
  return dfTempValidRecords


# COMMAND ----------

def _GetChecklistActions_Step1(dfChkChecklists, dfChkNameSegments, dfTempSplitInstanceName):
  
  print("Starting Step 1")
  #   Inserting ChecklistIDs and SegmentNos into respective dataframes after applying required filters for each of columns
  dfTempLocation = dfChkChecklists.join(dfChkNameSegments, dfChkChecklists.CC_CheckListId == dfChkNameSegments.CNS_CheckListId).filter((col("CC_CheckListAcronym")=="OFFERING-NEW") | (col("CC_CheckListAcronym")=="OFFERING-AMND") |(col("CC_CheckListAcronym")=="LRN-ConEx") | (col("CC_CheckListAcronym")=="LRN-AppTrain") | (col("CC_CheckListAcronym")=="LRN-CT") | (col("CC_CheckListAcronym")=="LRN-CT/RPL") | (col("CC_CheckListAcronym")=="LRN-RPL") | (col("CC_CheckListAcronym")=="LRN-CT-RPL")).filter((col("CNS_ControlId") == "ddl_location")) 
  dfTempLocation = dfTempLocation.selectExpr("CC_CheckListId as TL_ChecklistId", "CNS_SegmentNo as TL_SegmentNo", "CNS_ControlId as TL_ControlId")

  dfSkillPoint = dfChkChecklists.join(dfChkNameSegments, dfChkChecklists.CC_CheckListId == dfChkNameSegments.CNS_CheckListId).filter((col("CC_CheckListAcronym")=="OFFERING-NEW") | (col("CC_CheckListAcronym")=="OFFERING-AMND") |(col("CC_CheckListAcronym")=="LRN-ConEx") | (col("CC_CheckListAcronym")=="LRN-AppTrain") | (col("CC_CheckListAcronym")=="LRN-CT") | (col("CC_CheckListAcronym")=="LRN-CT/RPL") | (col("CC_CheckListAcronym")=="LRN-RPL") | (col("CC_CheckListAcronym")=="LRN-CT-RPL")).filter((col("CNS_ControlId") == "ddl_skills_point")).filter((col("CNS_ddloptionsdomain") == "SKILLS_POINTS_TAFENSW")) 
  dfSkillPoint = dfSkillPoint.selectExpr("CC_CheckListId as TSP_ChecklistId", "CNS_SegmentNo as TSP_SegmentNo", "CNS_ControlId as TSP_ControlId")

  dfTempFundingType = dfChkChecklists.join(dfChkNameSegments, dfChkChecklists.CC_CheckListId == dfChkNameSegments.CNS_CheckListId).filter((col("CC_CheckListAcronym")=="OFFERING-NEW") | (col("CC_CheckListAcronym")=="OFFERING-AMND") |(col("CC_CheckListAcronym")=="LRN-ConEx") | (col("CC_CheckListAcronym")=="LRN-AppTrain") | (col("CC_CheckListAcronym")=="LRN-CT") | (col("CC_CheckListAcronym")=="LRN-CT/RPL") | (col("CC_CheckListAcronym")=="LRN-RPL") | (col("CC_CheckListAcronym")=="LRN-CT-RPL")).filter((col("CNS_ControlId") == "ddl_funding_type")) 
  dfTempFundingType = dfTempFundingType.selectExpr("CC_CheckListId as TFT_ChecklistId", "CNS_SegmentNo as TFT_SegmentNo", "CNS_ControlId as TFT_ControlId")

  dfTempAttendanceMode = dfChkChecklists.join(dfChkNameSegments, dfChkChecklists.CC_CheckListId == dfChkNameSegments.CNS_CheckListId).filter((col("CC_CheckListAcronym")=="OFFERING-NEW") | (col("CC_CheckListAcronym")=="OFFERING-AMND") |(col("CC_CheckListAcronym")=="LRN-ConEx") | (col("CC_CheckListAcronym")=="LRN-AppTrain") | (col("CC_CheckListAcronym")=="LRN-CT") | (col("CC_CheckListAcronym")=="LRN-CT/RPL") | (col("CC_CheckListAcronym")=="LRN-RPL") | (col("CC_CheckListAcronym")=="LRN-CT-RPL")).filter((col("CNS_ControlId") == "ddl_att_mode")) 
  dfTempAttendanceMode = dfTempAttendanceMode.selectExpr("CC_CheckListId as TAM_ChecklistId", "CNS_SegmentNo as TAM_SegmentNo", "CNS_ControlId as TAM_ControlId")

  dfTempCostCentre = dfChkChecklists.join(dfChkNameSegments, dfChkChecklists.CC_CheckListId == dfChkNameSegments.CNS_CheckListId).filter((col("CC_CheckListAcronym")=="OFFERING-NEW") | (col("CC_CheckListAcronym")=="OFFERING-AMND")  |(col("CC_CheckListAcronym")=="LRN-ConEx") | (col("CC_CheckListAcronym")=="LRN-AppTrain") | (col("CC_CheckListAcronym")=="LRN-CT") | (col("CC_CheckListAcronym")=="LRN-CT/RPL") | (col("CC_CheckListAcronym")=="LRN-RPL") |  (col("CC_CheckListAcronym")=="LRN-CT-RPL")).filter((col("CNS_ControlId") == "ddl_cc") | (col("CNS_ControlId") == "tb_teaching_section")) 
  dfTempCostCentre = dfTempCostCentre.selectExpr("CC_CheckListId as TCC_ChecklistId", "CNS_SegmentNo as TCC_SegmentNo", "CNS_ControlId as TCC_ControlId")

  dfTempVTO = dfChkChecklists.join(dfChkNameSegments, dfChkChecklists.CC_CheckListId == dfChkNameSegments.CNS_CheckListId).filter((col("CC_CheckListAcronym")=="OFFERING-NEW") | (col("CC_CheckListAcronym")=="OFFERING-AMND") |(col("CC_CheckListAcronym")=="LRN-ConEx") | (col("CC_CheckListAcronym")=="LRN-AppTrain") | (col("CC_CheckListAcronym")=="LRN-CT") | (col("CC_CheckListAcronym")=="LRN-CT/RPL") | (col("CC_CheckListAcronym")=="LRN-RPL") | (col("CC_CheckListAcronym")=="LRN-CT-RPL")).filter((col("CNS_ControlId") == "tb_vto")) 
  dfTempVTO = dfTempVTO.selectExpr("CC_CheckListId as TVTO_ChecklistId", "CNS_SegmentNo as TVTO_SegmentNo", "CNS_ControlId as TVTO_ControlId")

  dfTempCourseCode = dfChkChecklists.join(dfChkNameSegments, dfChkChecklists.CC_CheckListId == dfChkNameSegments.CNS_CheckListId).filter((col("CC_CheckListAcronym")=="OFFERING-NEW") | (col("CC_CheckListAcronym")=="OFFERING-AMND") |(col("CC_CheckListAcronym")=="LRN-ConEx") | (col("CC_CheckListAcronym")=="LRN-AppTrain") | (col("CC_CheckListAcronym")=="LRN-CT") | (col("CC_CheckListAcronym")=="LRN-CT/RPL") | (col("CC_CheckListAcronym")=="LRN-RPL") | (col("CC_CheckListAcronym")=="LRN-CT-RPL")).filter((col("CNS_ControlId") == "tb_course_code") | (col("CNS_ControlId") == "tb_national_code") | (col("CNS_ControlId") == "tb_product_code") | (col("CNS_ControlId") == "tb_lpi")) 
  dfTempCourseCode = dfTempCourseCode.selectExpr("CC_CheckListId as TCD_ChecklistId", "CNS_SegmentNo as TCD_SegmentNo", "CNS_ControlId as TCD_ControlId")

  dfTempCalocc = dfChkChecklists.join(dfChkNameSegments, dfChkChecklists.CC_CheckListId == dfChkNameSegments.CNS_CheckListId).filter((col("CC_CheckListAcronym")=="OFFERING-NEW") | (col("CC_CheckListAcronym")=="OFFERING-AMND") | (col("CC_CheckListAcronym")=="LRN-ConEx") | (col("CC_CheckListAcronym")=="LRN-AppTrain") | (col("CC_CheckListAcronym")=="LRN-CT") | (col("CC_CheckListAcronym")=="LRN-CT/RPL") | (col("CC_CheckListAcronym")=="LRN-RPL") |  (col("CC_CheckListAcronym")=="LRN-CT-RPL")).filter((col("CNS_ControlId") == "tb_calocc"))
  dfTempCalocc = dfTempCalocc.selectExpr("CC_CheckListId as TC_ChecklistId", "CNS_SegmentNo as TC_SegmentNo", "CNS_ControlId as TC_ControlId")

  dfTempOfferType = dfChkChecklists.join(dfChkNameSegments, dfChkChecklists.CC_CheckListId == dfChkNameSegments.CNS_CheckListId).filter((col("CC_CheckListAcronym")=="OFFERING-NEW") | (col("CC_CheckListAcronym")=="OFFERING-AMND") |(col("CC_CheckListAcronym")=="LRN-ConEx") | (col("CC_CheckListAcronym")=="LRN-AppTrain") | (col("CC_CheckListAcronym")=="LRN-CT") | (col("CC_CheckListAcronym")=="LRN-CT/RPL") | (col("CC_CheckListAcronym")=="LRN-RPL") | (col("CC_CheckListAcronym")=="LRN-CT-RPL")).filter((col("CNS_ControlId") == "ddl_offering_type") | (col("CNS_ControlId") == "ddl_offrg_type")) 
  dfTempOfferType = dfTempOfferType.selectExpr("CC_CheckListId as TOT_ChecklistId", "CNS_SegmentNo as TOT_SegmentNo", "CNS_ControlId as TOT_ControlId")

  dfTempLearnerNo = dfChkChecklists.join(dfChkNameSegments, dfChkChecklists.CC_CheckListId == dfChkNameSegments.CNS_CheckListId).filter((col("CC_CheckListAcronym")=="OFFERING-NEW") | (col("CC_CheckListAcronym")=="OFFERING-AMND") |(col("CC_CheckListAcronym")=="LRN-ConEx") | (col("CC_CheckListAcronym")=="LRN-AppTrain") | (col("CC_CheckListAcronym")=="LRN-CT") | (col("CC_CheckListAcronym")=="LRN-CT/RPL") | (col("CC_CheckListAcronym")=="LRN-RPL") | (col("CC_CheckListAcronym")=="LRN-CT-RPL")).filter((col("CNS_ControlId") == "tb_learner_no")) 
  dfTempLearnerNo = dfTempLearnerNo.selectExpr("CC_CheckListId as TLN_ChecklistId", "CNS_SegmentNo as TLN_SegmentNo", "CNS_ControlId as TLN_ControlId")

  dfTempDOB = dfChkChecklists.join(dfChkNameSegments, dfChkChecklists.CC_CheckListId == dfChkNameSegments.CNS_CheckListId).filter((col("CC_CheckListAcronym")=="OFFERING-NEW") | (col("CC_CheckListAcronym")=="OFFERING-AMND") |(col("CC_CheckListAcronym")=="LRN-ConEx") | (col("CC_CheckListAcronym")=="LRN-AppTrain") | (col("CC_CheckListAcronym")=="LRN-CT") | (col("CC_CheckListAcronym")=="LRN-CT/RPL") | (col("CC_CheckListAcronym")=="LRN-RPL") | (col("CC_CheckListAcronym")=="LRN-CT-RPL")).filter((col("CNS_ControlId") == "tb_dob")) 
  dfTempDOB = dfTempDOB.selectExpr("CC_CheckListId as TDOB_ChecklistId", "CNS_SegmentNo as TDOB_SegmentNo", "CNS_ControlId as TDOB_ControlId")

  dfTempFirstName = dfChkChecklists.join(dfChkNameSegments, dfChkChecklists.CC_CheckListId == dfChkNameSegments.CNS_CheckListId).filter((col("CC_CheckListAcronym")=="OFFERING-NEW") | (col("CC_CheckListAcronym")=="OFFERING-AMND") |(col("CC_CheckListAcronym")=="LRN-ConEx") | (col("CC_CheckListAcronym")=="LRN-AppTrain") | (col("CC_CheckListAcronym")=="LRN-CT") | (col("CC_CheckListAcronym")=="LRN-CT/RPL") | (col("CC_CheckListAcronym")=="LRN-RPL") | (col("CC_CheckListAcronym")=="LRN-CT-RPL")).filter((col("CNS_ControlId") == "tb_fname")) 
  dfTempFirstName = dfTempFirstName.selectExpr("CC_CheckListId as TFN_ChecklistId", "CNS_SegmentNo as TFN_SegmentNo", "CNS_ControlId as TFN_ControlId")

  dfTempLastName = dfChkChecklists.join(dfChkNameSegments, dfChkChecklists.CC_CheckListId == dfChkNameSegments.CNS_CheckListId).filter((col("CC_CheckListAcronym")=="OFFERING-NEW") | (col("CC_CheckListAcronym")=="OFFERING-AMND") |(col("CC_CheckListAcronym")=="LRN-ConEx") | (col("CC_CheckListAcronym")=="LRN-AppTrain") | (col("CC_CheckListAcronym")=="LRN-CT") | (col("CC_CheckListAcronym")=="LRN-CT/RPL") | (col("CC_CheckListAcronym")=="LRN-RPL") | (col("CC_CheckListAcronym")=="LRN-CT-RPL")).filter((col("CNS_ControlId") == "tb_lname")) 
  dfTempLastName = dfTempLastName.selectExpr("CC_CheckListId as TLM_ChecklistId", "CNS_SegmentNo as TLM_SegmentNo", "CNS_ControlId as TLM_ControlId")

  dfTempQuantity = dfChkChecklists.join(dfChkNameSegments, dfChkChecklists.CC_CheckListId == dfChkNameSegments.CNS_CheckListId).filter((col("CC_CheckListAcronym")=="OFFERING-NEW") | (col("CC_CheckListAcronym")=="OFFERING-AMND") |(col("CC_CheckListAcronym")=="LRN-ConEx") | (col("CC_CheckListAcronym")=="LRN-AppTrain") | (col("CC_CheckListAcronym")=="LRN-CT") | (col("CC_CheckListAcronym")=="LRN-CT/RPL") | (col("CC_CheckListAcronym")=="LRN-RPL") | (col("CC_CheckListAcronym")=="LRN-CT-RPL")).filter((col("CNS_ControlId") == "tb_qty")) 
  dfTempQuantity = dfTempQuantity.selectExpr("CC_CheckListId as TQ_ChecklistId", "CNS_SegmentNo as TQ_SegmentNo", "CNS_ControlId as TQ_ControlId")

  dfTempStartDate = dfChkChecklists.join(dfChkNameSegments, dfChkChecklists.CC_CheckListId == dfChkNameSegments.CNS_CheckListId).filter((col("CC_CheckListAcronym")=="OFFERING-NEW") | (col("CC_CheckListAcronym")=="OFFERING-AMND") |(col("CC_CheckListAcronym")=="LRN-ConEx") | (col("CC_CheckListAcronym")=="LRN-AppTrain") | (col("CC_CheckListAcronym")=="LRN-CT") | (col("CC_CheckListAcronym")=="LRN-CT/RPL") | (col("CC_CheckListAcronym")=="LRN-RPL") | (col("CC_CheckListAcronym")=="LRN-CT-RPL")).filter((col("CNS_ControlId") == "tb_start_date") | (col("CNS_ControlId") == "tb_tc_start_date") | (col("CNS_ControlId") == "lbl_date")) 
  dfTempStartDate = dfTempStartDate.selectExpr("CC_CheckListId as TSD_ChecklistId", "CNS_SegmentNo as TSD_SegmentNo", "CNS_ControlId as TSD_ControlId")

  dfTempEndDate = dfChkChecklists.join(dfChkNameSegments, dfChkChecklists.CC_CheckListId == dfChkNameSegments.CNS_CheckListId).filter((col("CC_CheckListAcronym")=="OFFERING-NEW") | (col("CC_CheckListAcronym")=="OFFERING-AMND") |(col("CC_CheckListAcronym")=="LRN-ConEx") | (col("CC_CheckListAcronym")=="LRN-AppTrain") | (col("CC_CheckListAcronym")=="LRN-CT") | (col("CC_CheckListAcronym")=="LRN-CT/RPL") | (col("CC_CheckListAcronym")=="LRN-RPL") | (col("CC_CheckListAcronym")=="LRN-CT-RPL")).filter((col("CNS_ControlId") == "tb_tc_end_date")) 
  dfTempEndDate = dfTempEndDate.selectExpr("CC_CheckListId as TED_ChecklistId", "CNS_SegmentNo as TED_SegmentNo", "CNS_ControlId as TED_ControlId")

  dfTempYear = dfChkChecklists.join(dfChkNameSegments, dfChkChecklists.CC_CheckListId == dfChkNameSegments.CNS_CheckListId).filter((col("CC_CheckListAcronym")=="OFFERING-NEW") | (col("CC_CheckListAcronym")=="OFFERING-AMND") |(col("CC_CheckListAcronym")=="LRN-ConEx") | (col("CC_CheckListAcronym")=="LRN-AppTrain") | (col("CC_CheckListAcronym")=="LRN-CT") | (col("CC_CheckListAcronym")=="LRN-CT/RPL") | (col("CC_CheckListAcronym")=="LRN-RPL") | (col("CC_CheckListAcronym")=="LRN-CT-RPL")).filter((col("CNS_ControlId") == "tb_year") | (col("CNS_ControlId") == "ddl_year")) 
  dfTempYear = dfTempYear.selectExpr("CC_CheckListId as TY_ChecklistId", "CNS_SegmentNo as TY_SegmentNo", "CNS_ControlId as TY_ControlId")

  dfTempMonth = dfChkChecklists.join(dfChkNameSegments, dfChkChecklists.CC_CheckListId == dfChkNameSegments.CNS_CheckListId).filter((col("CC_CheckListAcronym")=="OFFERING-NEW") | (col("CC_CheckListAcronym")=="OFFERING-AMND") |(col("CC_CheckListAcronym")=="LRN-ConEx") | (col("CC_CheckListAcronym")=="LRN-AppTrain") | (col("CC_CheckListAcronym")=="LRN-CT") | (col("CC_CheckListAcronym")=="LRN-CT/RPL") | (col("CC_CheckListAcronym")=="LRN-RPL") | (col("CC_CheckListAcronym")=="LRN-CT-RPL")).filter((col("CNS_ControlId") == "ddl_month") | (col("CNS_ControlId") == "lbl_date") | (col("CNS_ControlId") == "tb_tc_start_date") | (col("CNS_ControlId") == "tb_start_date")) 
  dfTempMonth = dfTempMonth.selectExpr("CC_CheckListId as TM_ChecklistId", "CNS_SegmentNo as TM_SegmentNo", "CNS_ControlId as TM_ControlId")

  dfTempDay = dfChkChecklists.join(dfChkNameSegments, dfChkChecklists.CC_CheckListId == dfChkNameSegments.CNS_CheckListId).filter((col("CC_CheckListAcronym")=="OFFERING-NEW") | (col("CC_CheckListAcronym")=="OFFERING-AMND") |(col("CC_CheckListAcronym")=="LRN-ConEx") | (col("CC_CheckListAcronym")=="LRN-AppTrain") | (col("CC_CheckListAcronym")=="LRN-CT") | (col("CC_CheckListAcronym")=="LRN-CT/RPL") | (col("CC_CheckListAcronym")=="LRN-RPL") | (col("CC_CheckListAcronym")=="LRN-CT-RPL")).filter((col("CNS_ControlId") == "ddl_day") | (col("CNS_ControlId") == "lbl_date") | (col("CNS_ControlId") == "tb_start_date")) 
  dfTempDay = dfTempDay.selectExpr("CC_CheckListId as TD_ChecklistId", "CNS_SegmentNo as TD_SegmentNo", "CNS_ControlId as TD_ControlId")

  dfTempTerm = dfChkChecklists.join(dfChkNameSegments, dfChkChecklists.CC_CheckListId == dfChkNameSegments.CNS_CheckListId).filter((col("CC_CheckListAcronym")=="OFFERING-NEW") | (col("CC_CheckListAcronym")=="OFFERING-AMND") |(col("CC_CheckListAcronym")=="LRN-ConEx") | (col("CC_CheckListAcronym")=="LRN-AppTrain") | (col("CC_CheckListAcronym")=="LRN-CT") | (col("CC_CheckListAcronym")=="LRN-CT/RPL") | (col("CC_CheckListAcronym")=="LRN-RPL") | (col("CC_CheckListAcronym")=="LRN-CT-RPL")).filter((col("CNS_ControlId") == "ddl_term")) 
  dfTempTerm = dfTempTerm.selectExpr("CC_CheckListId as TT_ChecklistId", "CNS_SegmentNo as TT_SegmentNo", "CNS_ControlId as TT_ControlId")

  dfTempChangeType = dfChkChecklists.join(dfChkNameSegments, dfChkChecklists.CC_CheckListId == dfChkNameSegments.CNS_CheckListId).filter((col("CC_CheckListAcronym")=="OFFERING-NEW") | (col("CC_CheckListAcronym")=="OFFERING-AMND") |(col("CC_CheckListAcronym")=="LRN-ConEx") | (col("CC_CheckListAcronym")=="LRN-AppTrain") | (col("CC_CheckListAcronym")=="LRN-CT") | (col("CC_CheckListAcronym")=="LRN-CT/RPL") | (col("CC_CheckListAcronym")=="LRN-RPL") | (col("CC_CheckListAcronym")=="LRN-CT-RPL")).filter((col("CNS_ControlId") == "ddl_change_type")) 
  dfTempChangeType = dfTempChangeType.selectExpr("CC_CheckListId as TCT_ChecklistId", "CNS_SegmentNo as TCT_SegmentNo", "CNS_ControlId as TCT_ControlId")

  dfTempMarketing = dfChkChecklists.join(dfChkNameSegments, dfChkChecklists.CC_CheckListId == dfChkNameSegments.CNS_CheckListId).filter((col("CC_CheckListAcronym")=="OFFERING-NEW") | (col("CC_CheckListAcronym")=="OFFERING-AMND") |(col("CC_CheckListAcronym")=="LRN-ConEx") | (col("CC_CheckListAcronym")=="LRN-AppTrain") | (col("CC_CheckListAcronym")=="LRN-CT") | (col("CC_CheckListAcronym")=="LRN-CT/RPL") | (col("CC_CheckListAcronym")=="LRN-RPL") | (col("CC_CheckListAcronym")=="LRN-CT-RPL")).filter((col("CNS_ControlId") == "ddl_marketing")) 
  dfTempMarketing = dfTempMarketing.selectExpr("CC_CheckListId as TMK_ChecklistId", "CNS_SegmentNo as TMK_SegmentNo", "CNS_ControlId as TMK_ControlId")

  dfTempChecklistActions = dfTempSplitInstanceName.join(dfTempLocation, dfTempSplitInstanceName.CheckListId  == dfTempLocation.TL_ChecklistId, how="left")
  dfTempChecklistActions = dfTempChecklistActions.join(dfSkillPoint, dfTempSplitInstanceName.CheckListId == dfSkillPoint.TSP_ChecklistId, how="left")
  dfTempChecklistActions = dfTempChecklistActions.join(dfTempFundingType, dfTempSplitInstanceName.CheckListId == dfTempFundingType.TFT_ChecklistId, how="left")
  dfTempChecklistActions = dfTempChecklistActions.join(dfTempAttendanceMode, dfTempSplitInstanceName.CheckListId == dfTempAttendanceMode.TAM_ChecklistId, how="left")
  dfTempChecklistActions = dfTempChecklistActions.join(dfTempCostCentre, dfTempSplitInstanceName.CheckListId == dfTempCostCentre.TCC_ChecklistId, how="left")
  dfTempChecklistActions = dfTempChecklistActions.join(dfTempVTO, dfTempSplitInstanceName.CheckListId == dfTempVTO.TVTO_ChecklistId, how="left")
  dfTempChecklistActions = dfTempChecklistActions.join(dfTempCourseCode, dfTempSplitInstanceName.CheckListId == dfTempCourseCode.TCD_ChecklistId, how="left")
  dfTempChecklistActions = dfTempChecklistActions.join(dfTempCalocc, dfTempSplitInstanceName.CheckListId == dfTempCalocc.TC_ChecklistId, how="left")
  dfTempChecklistActions = dfTempChecklistActions.join(dfTempOfferType, dfTempSplitInstanceName.CheckListId == dfTempOfferType.TOT_ChecklistId, how="left")
  dfTempChecklistActions = dfTempChecklistActions.join(dfTempLearnerNo, dfTempSplitInstanceName.CheckListId == dfTempLearnerNo.TLN_ChecklistId, how="left")
  dfTempChecklistActions = dfTempChecklistActions.join(dfTempDOB, dfTempSplitInstanceName.CheckListId == dfTempDOB.TDOB_ChecklistId, how="left")
  dfTempChecklistActions = dfTempChecklistActions.join(dfTempFirstName, dfTempSplitInstanceName.CheckListId == dfTempFirstName.TFN_ChecklistId, how="left")
  dfTempChecklistActions = dfTempChecklistActions.join(dfTempLastName, dfTempSplitInstanceName.CheckListId == dfTempLastName.TLM_ChecklistId, how="left")
  dfTempChecklistActions = dfTempChecklistActions.join(dfTempQuantity, dfTempSplitInstanceName.CheckListId == dfTempQuantity.TQ_ChecklistId, how="left")
  dfTempChecklistActions = dfTempChecklistActions.join(dfTempStartDate, dfTempSplitInstanceName.CheckListId == dfTempStartDate.TSD_ChecklistId, how="left")
  dfTempChecklistActions = dfTempChecklistActions.join(dfTempEndDate, dfTempSplitInstanceName.CheckListId == dfTempEndDate.TED_ChecklistId, how="left")
  dfTempChecklistActions = dfTempChecklistActions.join(dfTempYear, dfTempSplitInstanceName.CheckListId == dfTempYear.TY_ChecklistId, how="left")
  dfTempChecklistActions = dfTempChecklistActions.join(dfTempMonth, dfTempSplitInstanceName.CheckListId == dfTempMonth.TM_ChecklistId, how="left")
  dfTempChecklistActions = dfTempChecklistActions.join(dfTempDay, dfTempSplitInstanceName.CheckListId == dfTempDay.TD_ChecklistId, how="left")
  dfTempChecklistActions = dfTempChecklistActions.join(dfTempTerm, dfTempSplitInstanceName.CheckListId == dfTempTerm.TT_ChecklistId, how="left")
  dfTempChecklistActions = dfTempChecklistActions.join(dfTempChangeType, dfTempSplitInstanceName.CheckListId == dfTempChangeType.TCT_ChecklistId, how="left")
  dfTempChecklistActions = dfTempChecklistActions.join(dfTempMarketing, dfTempSplitInstanceName.CheckListId == dfTempMarketing.TMK_ChecklistId, how="left")  
  
  print("Finished Step 1")

  return dfTempChecklistActions

# COMMAND ----------

def _GetChecklistActions_Step2(dfTempChecklistActions):
  
  print("Starting Step 2")

  dfTempChecklistActions = dfTempChecklistActions.withColumn("TaskName", when(col("CT_TaskName").isNull(),concat(lit("Task "),col("TaskNo").cast("string"))).otherwise(col("CT_TaskName")))\
                                               .withColumn("SkillPoint", when(col("TSP_SegmentNo")==1,col("subval_1"))\
                                                                        .when(col("TSP_SegmentNo")==2,col("subval_2"))\
                                                                        .when(col("TSP_SegmentNo")==3,col("subval_3"))\
                                                                        .when(col("TSP_SegmentNo")==4,col("subval_4"))\
                                                                        .when(col("TSP_SegmentNo")==5,col("subval_5"))\
                                                                        .when(col("TSP_SegmentNo")==6,col("subval_6"))\
                                                                        .when(col("TSP_SegmentNo")==7,col("subval_7"))\
                                                                        .when(col("TSP_SegmentNo")==8,col("subval_8"))\
                                                                        .when(col("TSP_SegmentNo")==9,col("subval_9"))\
                                                                        .when(col("TSP_SegmentNo")==10,col("subval_10"))\
                                                                        .when(col("TSP_SegmentNo")==11,col("subval_11"))\
                                                                        .when(col("TSP_SegmentNo")==12,col("subval_12"))\
                                                                        .when(col("TSP_SegmentNo")==13,col("subval_13"))\
                                                                        .when(col("TSP_SegmentNo")==14,col("subval_14"))\
                                                                        .when(col("TSP_SegmentNo")==15,col("subval_15"))\
                                                                        .when(col("TSP_SegmentNo")==16,col("subval_16"))\
                                                                        .when(col("TSP_SegmentNo")==17,col("subval_17"))\
                                                                        .when(col("TSP_SegmentNo")==18,col("subval_18"))\
                                                                        .when(col("TSP_SegmentNo")==19,col("subval_19"))\
                                                                        .when(col("TSP_SegmentNo")==20,col("subval_20"))\
                                                                        .otherwise(None))\
                                               .withColumn("Location", when(col("TL_SegmentNo")==1,col("subval_1"))\
                                                                        .when(col("TL_SegmentNo")==2,col("subval_2"))\
                                                                        .when(col("TL_SegmentNo")==3,col("subval_3"))\
                                                                        .when(col("TL_SegmentNo")==4,col("subval_4"))\
                                                                        .when(col("TL_SegmentNo")==5,col("subval_5"))\
                                                                        .when(col("TL_SegmentNo")==6,col("subval_6"))\
                                                                        .when(col("TL_SegmentNo")==7,col("subval_7"))\
                                                                        .when(col("TL_SegmentNo")==8,col("subval_8"))\
                                                                        .when(col("TL_SegmentNo")==9,col("subval_9"))\
                                                                        .when(col("TL_SegmentNo")==10,col("subval_10"))\
                                                                        .when(col("TL_SegmentNo")==11,col("subval_11"))\
                                                                        .when(col("TL_SegmentNo")==12,col("subval_12"))\
                                                                        .when(col("TL_SegmentNo")==13,col("subval_13"))\
                                                                        .when(col("TL_SegmentNo")==14,col("subval_14"))\
                                                                        .when(col("TL_SegmentNo")==15,col("subval_15"))\
                                                                        .when(col("TL_SegmentNo")==16,col("subval_16"))\
                                                                        .when(col("TL_SegmentNo")==17,col("subval_17"))\
                                                                        .when(col("TL_SegmentNo")==18,col("subval_18"))\
                                                                        .when(col("TL_SegmentNo")==19,col("subval_19"))\
                                                                        .when(col("TL_SegmentNo")==20,col("subval_20"))\
                                                                        .otherwise(None))\
                                               .withColumn("FundingType", when(col("TFT_SegmentNo")==1,col("subval_1"))\
                                                                        .when(col("TFT_SegmentNo")==2,col("subval_2"))\
                                                                        .when(col("TFT_SegmentNo")==3,col("subval_3"))\
                                                                        .when(col("TFT_SegmentNo")==4,col("subval_4"))\
                                                                        .when(col("TFT_SegmentNo")==5,col("subval_5"))\
                                                                        .when(col("TFT_SegmentNo")==6,col("subval_6"))\
                                                                        .when(col("TFT_SegmentNo")==7,col("subval_7"))\
                                                                        .when(col("TFT_SegmentNo")==8,col("subval_8"))\
                                                                        .when(col("TFT_SegmentNo")==9,col("subval_9"))\
                                                                        .when(col("TFT_SegmentNo")==10,col("subval_10"))\
                                                                        .when(col("TFT_SegmentNo")==11,col("subval_11"))\
                                                                        .when(col("TFT_SegmentNo")==12,col("subval_12"))\
                                                                        .when(col("TFT_SegmentNo")==13,col("subval_13"))\
                                                                        .when(col("TFT_SegmentNo")==14,col("subval_14"))\
                                                                        .when(col("TFT_SegmentNo")==15,col("subval_15"))\
                                                                        .when(col("TFT_SegmentNo")==16,col("subval_16"))\
                                                                        .when(col("TFT_SegmentNo")==17,col("subval_17"))\
                                                                        .when(col("TFT_SegmentNo")==18,col("subval_18"))\
                                                                        .when(col("TFT_SegmentNo")==19,col("subval_19"))\
                                                                        .when(col("TFT_SegmentNo")==20,col("subval_20"))\
                                                                        .otherwise(None))\
                                               .withColumn("AttendanceMode", when(col("TAM_SegmentNo")==1,col("subval_1"))\
                                                                        .when(col("TAM_SegmentNo")==2,col("subval_2"))\
                                                                        .when(col("TAM_SegmentNo")==3,col("subval_3"))\
                                                                        .when(col("TAM_SegmentNo")==4,col("subval_4"))\
                                                                        .when(col("TAM_SegmentNo")==5,col("subval_5"))\
                                                                        .when(col("TAM_SegmentNo")==6,col("subval_6"))\
                                                                        .when(col("TAM_SegmentNo")==7,col("subval_7"))\
                                                                        .when(col("TAM_SegmentNo")==8,col("subval_8"))\
                                                                        .when(col("TAM_SegmentNo")==9,col("subval_9"))\
                                                                        .when(col("TAM_SegmentNo")==10,col("subval_10"))\
                                                                        .when(col("TAM_SegmentNo")==11,col("subval_11"))\
                                                                        .when(col("TAM_SegmentNo")==12,col("subval_12"))\
                                                                        .when(col("TAM_SegmentNo")==13,col("subval_13"))\
                                                                        .when(col("TAM_SegmentNo")==14,col("subval_14"))\
                                                                        .when(col("TAM_SegmentNo")==15,col("subval_15"))\
                                                                        .when(col("TAM_SegmentNo")==16,col("subval_16"))\
                                                                        .when(col("TAM_SegmentNo")==17,col("subval_17"))\
                                                                        .when(col("TAM_SegmentNo")==18,col("subval_18"))\
                                                                        .when(col("TAM_SegmentNo")==19,col("subval_19"))\
                                                                        .when(col("TAM_SegmentNo")==20,col("subval_20"))\
                                                                        .otherwise(None))\
                                               .withColumn("CostCentre", when(col("TCC_SegmentNo")==1,col("subval_1"))\
                                                                        .when(col("TCC_SegmentNo")==2,col("subval_2"))\
                                                                        .when(col("TCC_SegmentNo")==3,col("subval_3"))\
                                                                        .when(col("TCC_SegmentNo")==4,col("subval_4"))\
                                                                        .when(col("TCC_SegmentNo")==5,col("subval_5"))\
                                                                        .when(col("TCC_SegmentNo")==6,col("subval_6"))\
                                                                        .when(col("TCC_SegmentNo")==7,col("subval_7"))\
                                                                        .when(col("TCC_SegmentNo")==8,col("subval_8"))\
                                                                        .when(col("TCC_SegmentNo")==9,col("subval_9"))\
                                                                        .when(col("TCC_SegmentNo")==10,col("subval_10"))\
                                                                        .when(col("TCC_SegmentNo")==11,col("subval_11"))\
                                                                        .when(col("TCC_SegmentNo")==12,col("subval_12"))\
                                                                        .when(col("TCC_SegmentNo")==13,col("subval_13"))\
                                                                        .when(col("TCC_SegmentNo")==14,col("subval_14"))\
                                                                        .when(col("TCC_SegmentNo")==15,col("subval_15"))\
                                                                        .when(col("TCC_SegmentNo")==16,col("subval_16"))\
                                                                        .when(col("TCC_SegmentNo")==17,col("subval_17"))\
                                                                        .when(col("TCC_SegmentNo")==18,col("subval_18"))\
                                                                        .when(col("TCC_SegmentNo")==19,col("subval_19"))\
                                                                        .when(col("TCC_SegmentNo")==20,col("subval_20"))\
                                                                        .otherwise(None))\
                                               .withColumn("VTO", when(col("TVTO_SegmentNo")==1,col("subval_1"))\
                                                                        .when(col("TVTO_SegmentNo")==2,col("subval_2"))\
                                                                        .when(col("TVTO_SegmentNo")==3,col("subval_3"))\
                                                                        .when(col("TVTO_SegmentNo")==4,col("subval_4"))\
                                                                        .when(col("TVTO_SegmentNo")==5,col("subval_5"))\
                                                                        .when(col("TVTO_SegmentNo")==6,col("subval_6"))\
                                                                        .when(col("TVTO_SegmentNo")==7,col("subval_7"))\
                                                                        .when(col("TVTO_SegmentNo")==8,col("subval_8"))\
                                                                        .when(col("TVTO_SegmentNo")==9,col("subval_9"))\
                                                                        .when(col("TVTO_SegmentNo")==10,col("subval_10"))\
                                                                        .when(col("TVTO_SegmentNo")==11,col("subval_11"))\
                                                                        .when(col("TVTO_SegmentNo")==12,col("subval_12"))\
                                                                        .when(col("TVTO_SegmentNo")==13,col("subval_13"))\
                                                                        .when(col("TVTO_SegmentNo")==14,col("subval_14"))\
                                                                        .when(col("TVTO_SegmentNo")==15,col("subval_15"))\
                                                                        .when(col("TVTO_SegmentNo")==16,col("subval_16"))\
                                                                        .when(col("TVTO_SegmentNo")==17,col("subval_17"))\
                                                                        .when(col("TVTO_SegmentNo")==18,col("subval_18"))\
                                                                        .when(col("TVTO_SegmentNo")==19,col("subval_19"))\
                                                                        .when(col("TVTO_SegmentNo")==20,col("subval_20"))\
                                                                        .otherwise(None))\
                                               .withColumn("Calocc", when(col("TC_SegmentNo")==1,col("subval_1"))\
                                                                        .when(col("TC_SegmentNo")==2,col("subval_2"))\
                                                                        .when(col("TC_SegmentNo")==3,col("subval_3"))\
                                                                        .when(col("TC_SegmentNo")==4,col("subval_4"))\
                                                                        .when(col("TC_SegmentNo")==5,col("subval_5"))\
                                                                        .when(col("TC_SegmentNo")==6,col("subval_6"))\
                                                                        .when(col("TC_SegmentNo")==7,col("subval_7"))\
                                                                        .when(col("TC_SegmentNo")==8,col("subval_8"))\
                                                                        .when(col("TC_SegmentNo")==9,col("subval_9"))\
                                                                        .when(col("TC_SegmentNo")==10,col("subval_10"))\
                                                                        .when(col("TC_SegmentNo")==11,col("subval_11"))\
                                                                        .when(col("TC_SegmentNo")==12,col("subval_12"))\
                                                                        .when(col("TC_SegmentNo")==13,col("subval_13"))\
                                                                        .when(col("TC_SegmentNo")==14,col("subval_14"))\
                                                                        .when(col("TC_SegmentNo")==15,col("subval_15"))\
                                                                        .when(col("TC_SegmentNo")==16,col("subval_16"))\
                                                                        .when(col("TC_SegmentNo")==17,col("subval_17"))\
                                                                        .when(col("TC_SegmentNo")==18,col("subval_18"))\
                                                                        .when(col("TC_SegmentNo")==19,col("subval_19"))\
                                                                        .when(col("TC_SegmentNo")==20,col("subval_20"))\
                                                                        .otherwise(None))\
                                               .withColumn("OfferType", when(col("TOT_SegmentNo")==1,col("subval_1"))\
                                                                        .when(col("TOT_SegmentNo")==2,col("subval_2"))\
                                                                        .when(col("TOT_SegmentNo")==3,col("subval_3"))\
                                                                        .when(col("TOT_SegmentNo")==4,col("subval_4"))\
                                                                        .when(col("TOT_SegmentNo")==5,col("subval_5"))\
                                                                        .when(col("TOT_SegmentNo")==6,col("subval_6"))\
                                                                        .when(col("TOT_SegmentNo")==7,col("subval_7"))\
                                                                        .when(col("TOT_SegmentNo")==8,col("subval_8"))\
                                                                        .when(col("TOT_SegmentNo")==9,col("subval_9"))\
                                                                        .when(col("TOT_SegmentNo")==10,col("subval_10"))\
                                                                        .when(col("TOT_SegmentNo")==11,col("subval_11"))\
                                                                        .when(col("TOT_SegmentNo")==12,col("subval_12"))\
                                                                        .when(col("TOT_SegmentNo")==13,col("subval_13"))\
                                                                        .when(col("TOT_SegmentNo")==14,col("subval_14"))\
                                                                        .when(col("TOT_SegmentNo")==15,col("subval_15"))\
                                                                        .when(col("TOT_SegmentNo")==16,col("subval_16"))\
                                                                        .when(col("TOT_SegmentNo")==17,col("subval_17"))\
                                                                        .when(col("TOT_SegmentNo")==18,col("subval_18"))\
                                                                        .when(col("TOT_SegmentNo")==19,col("subval_19"))\
                                                                        .when(col("TOT_SegmentNo")==20,col("subval_20"))\
                                                                        .otherwise(None))\
                                               .withColumn("LearnerNo", when(col("TLN_SegmentNo")==1,col("subval_1"))\
                                                                        .when(col("TLN_SegmentNo")==2,col("subval_2"))\
                                                                        .when(col("TLN_SegmentNo")==3,col("subval_3"))\
                                                                        .when(col("TLN_SegmentNo")==4,col("subval_4"))\
                                                                        .when(col("TLN_SegmentNo")==5,col("subval_5"))\
                                                                        .when(col("TLN_SegmentNo")==6,col("subval_6"))\
                                                                        .when(col("TLN_SegmentNo")==7,col("subval_7"))\
                                                                        .when(col("TLN_SegmentNo")==8,col("subval_8"))\
                                                                        .when(col("TLN_SegmentNo")==9,col("subval_9"))\
                                                                        .when(col("TLN_SegmentNo")==10,col("subval_10"))\
                                                                        .when(col("TLN_SegmentNo")==11,col("subval_11"))\
                                                                        .when(col("TLN_SegmentNo")==12,col("subval_12"))\
                                                                        .when(col("TLN_SegmentNo")==13,col("subval_13"))\
                                                                        .when(col("TLN_SegmentNo")==14,col("subval_14"))\
                                                                        .when(col("TLN_SegmentNo")==15,col("subval_15"))\
                                                                        .when(col("TLN_SegmentNo")==16,col("subval_16"))\
                                                                        .when(col("TLN_SegmentNo")==17,col("subval_17"))\
                                                                        .when(col("TLN_SegmentNo")==18,col("subval_18"))\
                                                                        .when(col("TLN_SegmentNo")==19,col("subval_19"))\
                                                                        .when(col("TLN_SegmentNo")==20,col("subval_20"))\
                                                                        .otherwise(None))\
                                               .withColumn("DOB", when(col("TDOB_SegmentNo")==1,col("subval_1"))\
                                                                        .when(col("TDOB_SegmentNo")==2,col("subval_2"))\
                                                                        .when(col("TDOB_SegmentNo")==3,col("subval_3"))\
                                                                        .when(col("TDOB_SegmentNo")==4,col("subval_4"))\
                                                                        .when(col("TDOB_SegmentNo")==5,col("subval_5"))\
                                                                        .when(col("TDOB_SegmentNo")==6,col("subval_6"))\
                                                                        .when(col("TDOB_SegmentNo")==7,col("subval_7"))\
                                                                        .when(col("TDOB_SegmentNo")==8,col("subval_8"))\
                                                                        .when(col("TDOB_SegmentNo")==9,col("subval_9"))\
                                                                        .when(col("TDOB_SegmentNo")==10,col("subval_10"))\
                                                                        .when(col("TDOB_SegmentNo")==11,col("subval_11"))\
                                                                        .when(col("TDOB_SegmentNo")==12,col("subval_12"))\
                                                                        .when(col("TDOB_SegmentNo")==13,col("subval_13"))\
                                                                        .when(col("TDOB_SegmentNo")==14,col("subval_14"))\
                                                                        .when(col("TDOB_SegmentNo")==15,col("subval_15"))\
                                                                        .when(col("TDOB_SegmentNo")==16,col("subval_16"))\
                                                                        .when(col("TDOB_SegmentNo")==17,col("subval_17"))\
                                                                        .when(col("TDOB_SegmentNo")==18,col("subval_18"))\
                                                                        .when(col("TDOB_SegmentNo")==19,col("subval_19"))\
                                                                        .when(col("TDOB_SegmentNo")==20,col("subval_20"))\
                                                                        .otherwise(None))\
                                               .withColumn("FirstName", when(col("TFN_SegmentNo")==1,col("subval_1"))\
                                                                        .when(col("TFN_SegmentNo")==2,col("subval_2"))\
                                                                        .when(col("TFN_SegmentNo")==3,col("subval_3"))\
                                                                        .when(col("TFN_SegmentNo")==4,col("subval_4"))\
                                                                        .when(col("TFN_SegmentNo")==5,col("subval_5"))\
                                                                        .when(col("TFN_SegmentNo")==6,col("subval_6"))\
                                                                        .when(col("TFN_SegmentNo")==7,col("subval_7"))\
                                                                        .when(col("TFN_SegmentNo")==8,col("subval_8"))\
                                                                        .when(col("TFN_SegmentNo")==9,col("subval_9"))\
                                                                        .when(col("TFN_SegmentNo")==10,col("subval_10"))\
                                                                        .when(col("TFN_SegmentNo")==11,col("subval_11"))\
                                                                        .when(col("TFN_SegmentNo")==12,col("subval_12"))\
                                                                        .when(col("TFN_SegmentNo")==13,col("subval_13"))\
                                                                        .when(col("TFN_SegmentNo")==14,col("subval_14"))\
                                                                        .when(col("TFN_SegmentNo")==15,col("subval_15"))\
                                                                        .when(col("TFN_SegmentNo")==16,col("subval_16"))\
                                                                        .when(col("TFN_SegmentNo")==17,col("subval_17"))\
                                                                        .when(col("TFN_SegmentNo")==18,col("subval_18"))\
                                                                        .when(col("TFN_SegmentNo")==19,col("subval_19"))\
                                                                        .when(col("TFN_SegmentNo")==20,col("subval_20"))\
                                                                        .otherwise(None))\
                                               .withColumn("LastName", when(col("TLM_SegmentNo")==1,col("subval_1"))\
                                                                        .when(col("TLM_SegmentNo")==2,col("subval_2"))\
                                                                        .when(col("TLM_SegmentNo")==3,col("subval_3"))\
                                                                        .when(col("TLM_SegmentNo")==4,col("subval_4"))\
                                                                        .when(col("TLM_SegmentNo")==5,col("subval_5"))\
                                                                        .when(col("TLM_SegmentNo")==6,col("subval_6"))\
                                                                        .when(col("TLM_SegmentNo")==7,col("subval_7"))\
                                                                        .when(col("TLM_SegmentNo")==8,col("subval_8"))\
                                                                        .when(col("TLM_SegmentNo")==9,col("subval_9"))\
                                                                        .when(col("TLM_SegmentNo")==10,col("subval_10"))\
                                                                        .when(col("TLM_SegmentNo")==11,col("subval_11"))\
                                                                        .when(col("TLM_SegmentNo")==12,col("subval_12"))\
                                                                        .when(col("TLM_SegmentNo")==13,col("subval_13"))\
                                                                        .when(col("TLM_SegmentNo")==14,col("subval_14"))\
                                                                        .when(col("TLM_SegmentNo")==15,col("subval_15"))\
                                                                        .when(col("TLM_SegmentNo")==16,col("subval_16"))\
                                                                        .when(col("TLM_SegmentNo")==17,col("subval_17"))\
                                                                        .when(col("TLM_SegmentNo")==18,col("subval_18"))\
                                                                        .when(col("TLM_SegmentNo")==19,col("subval_19"))\
                                                                        .when(col("TLM_SegmentNo")==20,col("subval_20"))\
                                                                        .otherwise(None))\
                                               .withColumn("Quantity", when(col("TQ_SegmentNo")==1,col("subval_1"))\
                                                                        .when(col("TQ_SegmentNo")==2,col("subval_2"))\
                                                                        .when(col("TQ_SegmentNo")==3,col("subval_3"))\
                                                                        .when(col("TQ_SegmentNo")==4,col("subval_4"))\
                                                                        .when(col("TQ_SegmentNo")==5,col("subval_5"))\
                                                                        .when(col("TQ_SegmentNo")==6,col("subval_6"))\
                                                                        .when(col("TQ_SegmentNo")==7,col("subval_7"))\
                                                                        .when(col("TQ_SegmentNo")==8,col("subval_8"))\
                                                                        .when(col("TQ_SegmentNo")==9,col("subval_9"))\
                                                                        .when(col("TQ_SegmentNo")==10,col("subval_10"))\
                                                                        .when(col("TQ_SegmentNo")==11,col("subval_11"))\
                                                                        .when(col("TQ_SegmentNo")==12,col("subval_12"))\
                                                                        .when(col("TQ_SegmentNo")==13,col("subval_13"))\
                                                                        .when(col("TQ_SegmentNo")==14,col("subval_14"))\
                                                                        .when(col("TQ_SegmentNo")==15,col("subval_15"))\
                                                                        .when(col("TQ_SegmentNo")==16,col("subval_16"))\
                                                                        .when(col("TQ_SegmentNo")==17,col("subval_17"))\
                                                                        .when(col("TQ_SegmentNo")==18,col("subval_18"))\
                                                                        .when(col("TQ_SegmentNo")==19,col("subval_19"))\
                                                                        .when(col("TQ_SegmentNo")==20,col("subval_20"))\
                                                                        .otherwise(None))\
                                               .withColumn("StartDate", when(col("TSD_SegmentNo")==1,col("subval_1"))\
                                                                        .when(col("TSD_SegmentNo")==2,col("subval_2"))\
                                                                        .when(col("TSD_SegmentNo")==3,col("subval_3"))\
                                                                        .when(col("TSD_SegmentNo")==4,col("subval_4"))\
                                                                        .when(col("TSD_SegmentNo")==5,col("subval_5"))\
                                                                        .when(col("TSD_SegmentNo")==6,col("subval_6"))\
                                                                        .when(col("TSD_SegmentNo")==7,col("subval_7"))\
                                                                        .when(col("TSD_SegmentNo")==8,col("subval_8"))\
                                                                        .when(col("TSD_SegmentNo")==9,col("subval_9"))\
                                                                        .when(col("TSD_SegmentNo")==10,col("subval_10"))\
                                                                        .when(col("TSD_SegmentNo")==11,col("subval_11"))\
                                                                        .when(col("TSD_SegmentNo")==12,col("subval_12"))\
                                                                        .when(col("TSD_SegmentNo")==13,col("subval_13"))\
                                                                        .when(col("TSD_SegmentNo")==14,col("subval_14"))\
                                                                        .when(col("TSD_SegmentNo")==15,col("subval_15"))\
                                                                        .when(col("TSD_SegmentNo")==16,col("subval_16"))\
                                                                        .when(col("TSD_SegmentNo")==17,col("subval_17"))\
                                                                        .when(col("TSD_SegmentNo")==18,col("subval_18"))\
                                                                        .when(col("TSD_SegmentNo")==19,col("subval_19"))\
                                                                        .when(col("TSD_SegmentNo")==20,col("subval_20"))\
                                                                        .otherwise(None))\
                                               .withColumn("EndDate", when(col("TED_SegmentNo")==1,col("subval_1"))\
                                                                        .when(col("TED_SegmentNo")==2,col("subval_2"))\
                                                                        .when(col("TED_SegmentNo")==3,col("subval_3"))\
                                                                        .when(col("TED_SegmentNo")==4,col("subval_4"))\
                                                                        .when(col("TED_SegmentNo")==5,col("subval_5"))\
                                                                        .when(col("TED_SegmentNo")==6,col("subval_6"))\
                                                                        .when(col("TED_SegmentNo")==7,col("subval_7"))\
                                                                        .when(col("TED_SegmentNo")==8,col("subval_8"))\
                                                                        .when(col("TED_SegmentNo")==9,col("subval_9"))\
                                                                        .when(col("TED_SegmentNo")==10,col("subval_10"))\
                                                                        .when(col("TED_SegmentNo")==11,col("subval_11"))\
                                                                        .when(col("TED_SegmentNo")==12,col("subval_12"))\
                                                                        .when(col("TED_SegmentNo")==13,col("subval_13"))\
                                                                        .when(col("TED_SegmentNo")==14,col("subval_14"))\
                                                                        .when(col("TED_SegmentNo")==15,col("subval_15"))\
                                                                        .when(col("TED_SegmentNo")==16,col("subval_16"))\
                                                                        .when(col("TED_SegmentNo")==17,col("subval_17"))\
                                                                        .when(col("TED_SegmentNo")==18,col("subval_18"))\
                                                                        .when(col("TED_SegmentNo")==19,col("subval_19"))\
                                                                        .when(col("TED_SegmentNo")==20,col("subval_20"))\
                                                                        .otherwise(None))\
                                               .withColumn("Year", when(col("TY_SegmentNo")==1,col("subval_1"))\
                                                                        .when(col("TY_SegmentNo")==2,col("subval_2"))\
                                                                        .when(col("TY_SegmentNo")==3,col("subval_3"))\
                                                                        .when(col("TY_SegmentNo")==4,col("subval_4"))\
                                                                        .when(col("TY_SegmentNo")==5,col("subval_5"))\
                                                                        .when(col("TY_SegmentNo")==6,col("subval_6"))\
                                                                        .when(col("TY_SegmentNo")==7,col("subval_7"))\
                                                                        .when(col("TY_SegmentNo")==8,col("subval_8"))\
                                                                        .when(col("TY_SegmentNo")==9,col("subval_9"))\
                                                                        .when(col("TY_SegmentNo")==10,col("subval_10"))\
                                                                        .when(col("TY_SegmentNo")==11,col("subval_11"))\
                                                                        .when(col("TY_SegmentNo")==12,col("subval_12"))\
                                                                        .when(col("TY_SegmentNo")==13,col("subval_13"))\
                                                                        .when(col("TY_SegmentNo")==14,col("subval_14"))\
                                                                        .when(col("TY_SegmentNo")==15,col("subval_15"))\
                                                                        .when(col("TY_SegmentNo")==16,col("subval_16"))\
                                                                        .when(col("TY_SegmentNo")==17,col("subval_17"))\
                                                                        .when(col("TY_SegmentNo")==18,col("subval_18"))\
                                                                        .when(col("TY_SegmentNo")==19,col("subval_19"))\
                                                                        .when(col("TY_SegmentNo")==20,col("subval_20"))\
                                                                        .otherwise(None))\
                                               .withColumn("Month", when(col("TM_SegmentNo")==1,col("subval_1"))\
                                                                        .when(col("TM_SegmentNo")==2,col("subval_2"))\
                                                                        .when(col("TM_SegmentNo")==3,col("subval_3"))\
                                                                        .when(col("TM_SegmentNo")==4,col("subval_4"))\
                                                                        .when(col("TM_SegmentNo")==5,col("subval_5"))\
                                                                        .when(col("TM_SegmentNo")==6,col("subval_6"))\
                                                                        .when(col("TM_SegmentNo")==7,col("subval_7"))\
                                                                        .when(col("TM_SegmentNo")==8,col("subval_8"))\
                                                                        .when(col("TM_SegmentNo")==9,col("subval_9"))\
                                                                        .when(col("TM_SegmentNo")==10,col("subval_10"))\
                                                                        .when(col("TM_SegmentNo")==11,col("subval_11"))\
                                                                        .when(col("TM_SegmentNo")==12,col("subval_12"))\
                                                                        .when(col("TM_SegmentNo")==13,col("subval_13"))\
                                                                        .when(col("TM_SegmentNo")==14,col("subval_14"))\
                                                                        .when(col("TM_SegmentNo")==15,col("subval_15"))\
                                                                        .when(col("TM_SegmentNo")==16,col("subval_16"))\
                                                                        .when(col("TM_SegmentNo")==17,col("subval_17"))\
                                                                        .when(col("TM_SegmentNo")==18,col("subval_18"))\
                                                                        .when(col("TM_SegmentNo")==19,col("subval_19"))\
                                                                        .when(col("TM_SegmentNo")==20,col("subval_20"))\
                                                                        .otherwise(None))\
                                               .withColumn("Day", when(col("TD_SegmentNo")==1,col("subval_1"))\
                                                                        .when(col("TD_SegmentNo")==2,col("subval_2"))\
                                                                        .when(col("TD_SegmentNo")==3,col("subval_3"))\
                                                                        .when(col("TD_SegmentNo")==4,col("subval_4"))\
                                                                        .when(col("TD_SegmentNo")==5,col("subval_5"))\
                                                                        .when(col("TD_SegmentNo")==6,col("subval_6"))\
                                                                        .when(col("TD_SegmentNo")==7,col("subval_7"))\
                                                                        .when(col("TD_SegmentNo")==8,col("subval_8"))\
                                                                        .when(col("TD_SegmentNo")==9,col("subval_9"))\
                                                                        .when(col("TD_SegmentNo")==10,col("subval_10"))\
                                                                        .when(col("TD_SegmentNo")==11,col("subval_11"))\
                                                                        .when(col("TD_SegmentNo")==12,col("subval_12"))\
                                                                        .when(col("TD_SegmentNo")==13,col("subval_13"))\
                                                                        .when(col("TD_SegmentNo")==14,col("subval_14"))\
                                                                        .when(col("TD_SegmentNo")==15,col("subval_15"))\
                                                                        .when(col("TD_SegmentNo")==16,col("subval_16"))\
                                                                        .when(col("TD_SegmentNo")==17,col("subval_17"))\
                                                                        .when(col("TD_SegmentNo")==18,col("subval_18"))\
                                                                        .when(col("TD_SegmentNo")==19,col("subval_19"))\
                                                                        .when(col("TD_SegmentNo")==20,col("subval_20"))\
                                                                        .otherwise(None))\
                                               .withColumn("Term", when(col("TT_SegmentNo")==1,col("subval_1"))\
                                                                        .when(col("TT_SegmentNo")==2,col("subval_2"))\
                                                                        .when(col("TT_SegmentNo")==3,col("subval_3"))\
                                                                        .when(col("TT_SegmentNo")==4,col("subval_4"))\
                                                                        .when(col("TT_SegmentNo")==5,col("subval_5"))\
                                                                        .when(col("TT_SegmentNo")==6,col("subval_6"))\
                                                                        .when(col("TT_SegmentNo")==7,col("subval_7"))\
                                                                        .when(col("TT_SegmentNo")==8,col("subval_8"))\
                                                                        .when(col("TT_SegmentNo")==9,col("subval_9"))\
                                                                        .when(col("TT_SegmentNo")==10,col("subval_10"))\
                                                                        .when(col("TT_SegmentNo")==11,col("subval_11"))\
                                                                        .when(col("TT_SegmentNo")==12,col("subval_12"))\
                                                                        .when(col("TT_SegmentNo")==13,col("subval_13"))\
                                                                        .when(col("TT_SegmentNo")==14,col("subval_14"))\
                                                                        .when(col("TT_SegmentNo")==15,col("subval_15"))\
                                                                        .when(col("TT_SegmentNo")==16,col("subval_16"))\
                                                                        .when(col("TT_SegmentNo")==17,col("subval_17"))\
                                                                        .when(col("TT_SegmentNo")==18,col("subval_18"))\
                                                                        .when(col("TT_SegmentNo")==19,col("subval_19"))\
                                                                        .when(col("TT_SegmentNo")==20,col("subval_20"))\
                                                                        .otherwise(None))\
                                               .withColumn("ChangeType", when(col("TCT_SegmentNo")==1,col("subval_1"))\
                                                                        .when(col("TCT_SegmentNo")==2,col("subval_2"))\
                                                                        .when(col("TCT_SegmentNo")==3,col("subval_3"))\
                                                                        .when(col("TCT_SegmentNo")==4,col("subval_4"))\
                                                                        .when(col("TCT_SegmentNo")==5,col("subval_5"))\
                                                                        .when(col("TCT_SegmentNo")==6,col("subval_6"))\
                                                                        .when(col("TCT_SegmentNo")==7,col("subval_7"))\
                                                                        .when(col("TCT_SegmentNo")==8,col("subval_8"))\
                                                                        .when(col("TCT_SegmentNo")==9,col("subval_9"))\
                                                                        .when(col("TCT_SegmentNo")==10,col("subval_10"))\
                                                                        .when(col("TCT_SegmentNo")==11,col("subval_11"))\
                                                                        .when(col("TCT_SegmentNo")==12,col("subval_12"))\
                                                                        .when(col("TCT_SegmentNo")==13,col("subval_13"))\
                                                                        .when(col("TCT_SegmentNo")==14,col("subval_14"))\
                                                                        .when(col("TCT_SegmentNo")==15,col("subval_15"))\
                                                                        .when(col("TCT_SegmentNo")==16,col("subval_16"))\
                                                                        .when(col("TCT_SegmentNo")==17,col("subval_17"))\
                                                                        .when(col("TCT_SegmentNo")==18,col("subval_18"))\
                                                                        .when(col("TCT_SegmentNo")==19,col("subval_19"))\
                                                                        .when(col("TCT_SegmentNo")==20,col("subval_20"))\
                                                                        .otherwise(None))\
                                               .withColumn("Marketing", when(col("TMK_SegmentNo")==1,col("subval_1"))\
                                                                        .when(col("TMK_SegmentNo")==2,col("subval_2"))\
                                                                        .when(col("TMK_SegmentNo")==3,col("subval_3"))\
                                                                        .when(col("TMK_SegmentNo")==4,col("subval_4"))\
                                                                        .when(col("TMK_SegmentNo")==5,col("subval_5"))\
                                                                        .when(col("TMK_SegmentNo")==6,col("subval_6"))\
                                                                        .when(col("TMK_SegmentNo")==7,col("subval_7"))\
                                                                        .when(col("TMK_SegmentNo")==8,col("subval_8"))\
                                                                        .when(col("TMK_SegmentNo")==9,col("subval_9"))\
                                                                        .when(col("TMK_SegmentNo")==10,col("subval_10"))\
                                                                        .when(col("TMK_SegmentNo")==11,col("subval_11"))\
                                                                        .when(col("TMK_SegmentNo")==12,col("subval_12"))\
                                                                        .when(col("TMK_SegmentNo")==13,col("subval_13"))\
                                                                        .when(col("TMK_SegmentNo")==14,col("subval_14"))\
                                                                        .when(col("TMK_SegmentNo")==15,col("subval_15"))\
                                                                        .when(col("TMK_SegmentNo")==16,col("subval_16"))\
                                                                        .when(col("TMK_SegmentNo")==17,col("subval_17"))\
                                                                        .when(col("TMK_SegmentNo")==18,col("subval_18"))\
                                                                        .when(col("TMK_SegmentNo")==19,col("subval_19"))\
                                                                        .when(col("TMK_SegmentNo")==20,col("subval_20"))\
                                                                        .otherwise(None))\
                                               .withColumn("CourseCode", when(col("TCD_SegmentNo")==1,col("subval_1"))\
                                                                        .when(col("TCD_SegmentNo")==2,col("subval_2"))\
                                                                        .when(col("TCD_SegmentNo")==3,col("subval_3"))\
                                                                        .when(col("TCD_SegmentNo")==4,col("subval_4"))\
                                                                        .when(col("TCD_SegmentNo")==5,col("subval_5"))\
                                                                        .when(col("TCD_SegmentNo")==6,col("subval_6"))\
                                                                        .when(col("TCD_SegmentNo")==7,col("subval_7"))\
                                                                        .when(col("TCD_SegmentNo")==8,col("subval_8"))\
                                                                        .when(col("TCD_SegmentNo")==9,col("subval_9"))\
                                                                        .when(col("TCD_SegmentNo")==10,col("subval_10"))\
                                                                        .when(col("TCD_SegmentNo")==11,col("subval_11"))\
                                                                        .when(col("TCD_SegmentNo")==12,col("subval_12"))\
                                                                        .when(col("TCD_SegmentNo")==13,col("subval_13"))\
                                                                        .when(col("TCD_SegmentNo")==14,col("subval_14"))\
                                                                        .when(col("TCD_SegmentNo")==15,col("subval_15"))\
                                                                        .when(col("TCD_SegmentNo")==16,col("subval_16"))\
                                                                        .when(col("TCD_SegmentNo")==17,col("subval_17"))\
                                                                        .when(col("TCD_SegmentNo")==18,col("subval_18"))\
                                                                        .when(col("TCD_SegmentNo")==19,col("subval_19"))\
                                                                        .when(col("TCD_SegmentNo")==20,col("subval_20"))\
                                                                        .otherwise(None))\
  
  dfTempChecklistActions = dfTempChecklistActions.replace("???",None, "ChangeType")
  print("Finished Step 2")
  
  return dfTempChecklistActions

# COMMAND ----------

def _GetChecklistActions_ValidRecords(dfTempChecklistActions): 

  print("Starting Valid Records")

  dfTempValidRecords  = dfTempChecklistActions.filter((IsValidDate_udf(dfTempChecklistActions["StartDate"],lit("'%d%m%Y','%Y%m%d','%d-%m-%Y','%Y-%m-%d','%Y/%m/%d','%d/%m/%Y','%d/%m/%y',''")) == True) & (IsValidDate_udf(dfTempChecklistActions["EndDate"],lit("'%d%m%Y','%Y%m%d','%d-%m-%Y','%Y-%m-%d','%Y/%m/%d','%d/%m/%Y','%d/%m/%y',''")) == True) & (IsValidDate_udf(dfTempChecklistActions["Year"],lit("'%Y',''")) == True) & (IsValidDate_udf(dfTempChecklistActions["Month"],lit("'%d%m%Y','%Y%m%d','%d-%m-%Y','%Y-%m-%d','%Y/%m/%d','%d/%m/%Y','%d/%m/%y','%m',''")) == True) & (IsValidDate_udf(dfTempChecklistActions["Day"],lit("'%d%m%Y','%Y%m%d','%d-%m-%Y','%Y-%m-%d','%Y/%m/%d','%d/%m/%Y','%d/%m/%y','%d',''")) == True))
  #print(dfTempValidRecords.distinct().count())

  dfTempValidRecords = dfTempValidRecords \
    .withColumn("StartDateTemp", \
                     when(to_date(col('StartDate'), 'dd/MM/yyyy').isNotNull(), to_date(col('StartDate'), 'dd/MM/yyyy'))\
                     .when(to_date(col('StartDate'), 'dd/MM/yy').isNotNull(),  to_date(col('StartDate'), 'dd/MM/yy'))\
                     .when(to_date(col('StartDate'), 'yyyyMMdd').isNotNull(), to_date(col('StartDate'), 'yyyyMMdd'))\
                     .when(to_date(col('StartDate'), 'ddMMyyyy').isNotNull(), to_date(col('StartDate'), 'ddMMyyyy'))\
                     .when(to_date(col('StartDate'), 'dMyyyy').isNotNull(), to_date(col('StartDate'), 'dMyyyy'))\
                    ) \
    .withColumn("MonthTemp", \
                     when(to_date(col('Month'), 'dd/MM/yyyy').isNotNull(), to_date(col('Month'), 'dd/MM/yyyy'))\
                     .when(to_date(col('Month'), 'dd/MM/yy').isNotNull(),  to_date(col('Month'), 'dd/MM/yy'))\
                     .when(to_date(col('Month'), 'yyyyMMdd').isNotNull(), to_date(col('Month'), 'yyyyMMdd'))\
                     .when(to_date(col('Month'), 'ddMMyyyy').isNotNull(), to_date(col('Month'), 'ddMMyyyy'))\
                     .when(to_date(col('Month'), 'dMyyyy').isNotNull(), to_date(col('Month'), 'dMyyyy'))\
                     .when(to_date(col('Month'), 'MM').isNotNull(), to_date(col('Month'), 'MM'))\
                    ) \
    .withColumn("DayTemp", \
                     when(to_date(col('Day'), 'dd/MM/yyyy').isNotNull(), to_date(col('Day'), 'dd/MM/yyyy'))\
                     .when(to_date(col('Day'), 'dd/MM/yy').isNotNull(),  to_date(col('Day'), 'dd/MM/yy'))\
                     .when(to_date(col('Day'), 'yyyyMMdd').isNotNull(), to_date(col('Day'), 'yyyyMMdd'))\
                     .when(to_date(col('Day'), 'ddMMyyyy').isNotNull(), to_date(col('Day'), 'ddMMyyyy'))\
                     .when(to_date(col('Day'), 'dMyyyy').isNotNull(), to_date(col('Day'), 'dMyyyy'))\
                     .when(to_date(col('Day'), 'dd').isNotNull(), to_date(col('Day'), 'dd'))\
                    )

  dfTempValidRecords = dfTempValidRecords \
    .withColumn("YearNew", expr("CASE WHEN Year is null or Year = '' then Year(StartDateTemp) ELSE Year END")) \
    .withColumn("MonthNew", expr("CASE WHEN LENGTH(Month) < 4 then Month ELSE SUBSTRING(MonthTemp, 6,2) END")) \
    .withColumn("DateNew", expr("CASE WHEN LENGTH(Day) < 4 then Day ELSE SUBSTRING(DayTemp, 9, 2) END")) \

  dfTempValidRecords = dfTempValidRecords \
    .withColumn("YearNew_1", expr("CASE WHEN YearNew < 100 THEN CAST(YearNew+2000 AS INT) ELSE YearNew END")) \


  dfTempValidRecords = dfTempValidRecords.selectExpr(
      "CheckListId"
    , "InstanceId"
    , "TaskNo"
    , "ActionedDate"
    , "CreationDate"
    , "Actioner"
    , "TaskName"
    , "TaskOwner"
    , "InitiatedDate"
    , "InitiatedBy"
    ,"HighestStatusId"
    , "SkillPoint"
    , "Location"
    , "FundingType"
    , "AttendanceMode"
    , "CostCentre"
    , "VTO"
    , "CourseCode"
    , "Calocc"
    , "OfferType"
    , "LearnerNo"
    , "DOB"
    , "FirstName"
    , "LastName"
    , "Quantity"
    , "StartDate"
    , "EndDate"
    , "YearNew_1 as Year"
    , "MonthNew as Month"
    , "DateNew as Day"
    , "Term"
    , "ChangeType"
    , "Marketing"
    , "StatusName"
    , "TaskStatusName"
    , "Final_ChecklistStatus")
  
  print("Finished Valid Records")

  return dfTempValidRecords


# COMMAND ----------

def _GetChecklistActions_InValidRecords(dfTempChecklistActions): 

  print("Starting Invalid Records")

  dfTempInvalidRecords  = dfTempChecklistActions.filter((IsValidDate_udf(dfTempChecklistActions["StartDate"],lit("'%d%m%Y','%Y%m%d','%d-%m-%Y','%Y-%m-%d','%Y/%m/%d','%d/%m/%Y','%d/%m/%y',''")) == False) | (IsValidDate_udf(dfTempChecklistActions["EndDate"],lit("'%d%m%Y','%Y%m%d','%d-%m-%Y','%Y-%m-%d','%Y/%m/%d','%d/%m/%Y','%d/%m/%y',''")) == False) | (IsValidDate_udf(dfTempChecklistActions["Year"],lit("'%Y',''")) == False) | (IsValidDate_udf(dfTempChecklistActions["Month"],lit("'%d%m%Y','%Y%m%d','%d-%m-%Y','%Y-%m-%d','%Y/%m/%d','%d/%m/%Y','%d/%m/%y','%m',''")) == False) | (IsValidDate_udf(dfTempChecklistActions["Day"],lit("'%d%m%Y','%Y%m%d','%d-%m-%Y','%Y-%m-%d','%Y/%m/%d','%d/%m/%Y','%d/%m/%y','%d',''")) == False))
  
  print("Finished Invalid Records")

  return dfTempInvalidRecords


# COMMAND ----------

# dfChkChecklists=spark.sql("select * from trusted.echecklist_dbo_checklists_checklists")
# dfChkWorkUnitLocales=spark.sql("select * from trusted.echecklist_dbo_checklists_work_unit_locales")
# dfChkNameSegments = spark.sql("select * from trusted.echecklist_dbo_checklists_name_segments")
# dfChkActions = spark.sql("select * from trusted.echecklist_dbo_checklists_actions")
# dfChkTasks = spark.sql("select * from trusted.echecklist_dbo_checklists_tasks")
# dfChkTaskStauses = spark.sql("select * from trusted.echecklist_dbo_checklists_task_statuses")
# dfChkStauses = spark.sql("select * from trusted.echecklist_dbo_checklists_statuses")
# dfChkStauses2 = spark.sql("select * from trusted.echecklist_dbo_checklists_statuses")
# dfChkInstances = spark.sql("select * from trusted.echecklist_dbo_checklists_instances")
# dfChkInstHistory= spark.sql("select * from trusted.echecklist_dbo_checklists_instance_history")



# COMMAND ----------

# SaveChecklistActions(dfChkChecklists, dfChkNameSegments, dfChkActions, dfChkTasks, dfChkTaskStauses, dfChkStauses, dfChkStauses2, dfChkInstances)

# COMMAND ----------


