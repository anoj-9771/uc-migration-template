# Databricks notebook source
from pyspark.sql.functions import *
from datetime import datetime
from pyspark.sql.window import *
from pyspark.sql.types import *

# COMMAND ----------

def SaveChecklistActionsSummary(dfChkInstHistory, dfChkInstances, dfValidRecords):
  
  #Alias columns
  dfCIH=GeneralAliasDataFrameColumns(dfChkInstHistory, "CIH_")
  dfCI=GeneralAliasDataFrameColumns(dfChkInstances, "CI_")
  dfVR=GeneralAliasDataFrameColumns(dfValidRecords, "VR_")
  dfVR.printSchema()
  
  dfChecklistActionsSummary = dfCIH.join(dfCI, dfCIH.CIH_InstanceId == dfCI.CI_InstanceId)
  dfChecklistActionsSummary = dfChecklistActionsSummary.join(dfVR, dfVR.VR_InstanceId == dfCI.CI_InstanceId)
  
  #Formatting the date
  dfChecklistActionsSummary = dfChecklistActionsSummary.withColumn("InitiatedDate", to_date(col("CI_InitiatedDate"),"yyyy-MM-dd"))
  
  #Filtering the records
  dfChecklistActionsSummary = dfChecklistActionsSummary.selectExpr("CI_InstanceId as InstanceId", "CIH_Action as Action","InitiatedDate").filter(col("InitiatedDate") >= '2018-01-01')
  
  dfChecklistActionsSummary = dfChecklistActionsSummary.withColumn("No_checklists_Completed", when(col("Action")=='COMPLETED',1).otherwise(0))\
                                     .withColumn("No_checklists_Cancelled", when(col("Action")=='CANCELLED',1).otherwise(0))
  
  dfChecklistActionsSummary = dfChecklistActionsSummary.selectExpr("InstanceId", "No_checklists_Completed", "No_checklists_Cancelled")
  
  #Output Columns
  dfChecklistActionsSummary = dfChecklistActionsSummary.groupBy("InstanceId")\
         .agg(sum("No_checklists_Completed").alias("No_checklists_Completed"),\
          sum("No_checklists_Cancelled").alias("No_checklists_Cancelled"))
  
  eCheckListSaveDataFrameToCurated(dfChecklistActionsSummary, 'ChecklistActionsSummary')
  
   #display(df)
  return dfChecklistActionsSummary
