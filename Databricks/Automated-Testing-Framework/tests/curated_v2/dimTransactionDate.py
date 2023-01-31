# Databricks notebook source
# MAGIC %run ../../common/common-atf

# COMMAND ----------

keyColumns =  'transactionCode'
mandatoryColumns = 'transactionCode'

columns = ("""activityDate
,transactionDescription
,transactionCode
,createdBy
,createdDateTime
,changedBy
,changedDateTime
""")

# COMMAND ----------

#ALWAYS RUN THIS AT THE END
RunTests()
