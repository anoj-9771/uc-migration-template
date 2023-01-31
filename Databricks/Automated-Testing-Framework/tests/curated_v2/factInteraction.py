# Databricks notebook source
# MAGIC %run ../../common/common-atf

# COMMAND ----------

keyColumns =  'interactionID'
mandatoryColumns = 'interactionID'

columns = ("""interactionID
,processTypeFK
,communicationChannelFK
,statusFK
,responsibleEmployeeFK
,contactPersonFK
,propertyFK
,interactionCategory
,interactionGUID
,externalNumber
,description
,createdDate
,priority
,direction
,directionCode
""")


# COMMAND ----------

#ALWAYS RUN THIS AT THE END
RunTests()
