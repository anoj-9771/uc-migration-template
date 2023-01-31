# Databricks notebook source
# MAGIC %run ../../common/common-atf

# COMMAND ----------

keyColumns =  'processTypeCode,SourceSystemCode'
mandatoryColumns = 'processTypeCode,processTypeUsage'

columns = ("""processTypeCode,
processTypeShortDescription,
processTypeDescription,
processTypeUsage,
SourceSystemCode
""")


# COMMAND ----------

#ALWAYS RUN THIS AT THE END
RunTests()
