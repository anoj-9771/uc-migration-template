# Databricks notebook source
# MAGIC %run ../../common/common-atf

# COMMAND ----------

keyColumns =  'statusProfile,statusCode'
mandatoryColumns = 'statusProfile,statusCode,statusShortDescription,status'

columns = ("""statusProfile
,statusCode
,statusShortDescription
,status
""")


# COMMAND ----------

#ALWAYS RUN THIS AT THE END
RunTests()
