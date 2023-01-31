# Databricks notebook source
# MAGIC %run ../../common/common-atf

# COMMAND ----------

keyColumns =  'objectTypeCode,statusProfile,statusCode'
mandatoryColumns = 'statusProfile,statusCode,statusShortDescription,status,objectTypeCode'

columns = ("""statusProfile
,statusCode
,statusShortDescription
,status
,objectTypeCode
,statusUsage
""")


# COMMAND ----------

#ALWAYS RUN THIS AT THE END
RunTests()
