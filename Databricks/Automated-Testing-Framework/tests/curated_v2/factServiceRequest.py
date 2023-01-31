# Databricks notebook source
# MAGIC %run ../../common/common-atf

# COMMAND ----------

keyColumns =  'serviceRequestGUID'
mandatoryColumns = 'serviceRequestGUID'

columns = ("""serviceRequestSK
,serviceRequestId
,serviceRequestGUID
,receivedCategoryFK
,resolutionCategoryFK
,channelFK
,contactPersonFK
,reportByPersonFK
,serviceTeamSK
,contractFK
,responsibleEmployeeFK
,processTypeFK
,propertyFK
,statusFK
,salesEmployeeFK
,totalDuration
,workDuration
,sourceName
,sourceCode
,issueResponsibility
,issueResponsibilityCode
,postingDate
,serviceRequestStartDate
,serviceRequestEndDate
,numberOfInteractions
,notificationNumber
,serviceRequestDescription
,direction
,directionCode
,maximoWorkOrderNumber
,projectId
,agreementNumber
,urgencyScore
,impactScore
,recommendedPriority
,serviceLifeCycle
,serviceLifeCycleUnit
,activityPriorityCode
,CreatedDateTime
,CreatedBy
,CreatedByName
,changeDate
,changeDateTime
,changedBy
,changedByName
""")


# COMMAND ----------

#ALWAYS RUN THIS AT THE END
RunTests()
