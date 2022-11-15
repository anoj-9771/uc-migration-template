# Databricks notebook source
# MAGIC %run ../../common/common-atf

# COMMAND ----------

keyColumns =  'processTypeCode'
mandatoryColumns = 'processTypeCode,processTypeShortDescription,processTypeDescription'

columns = ("""processTypeCode,
processTypeShortDescription,
processTypeDescription
""")


# COMMAND ----------

#ALWAYS RUN THIS AT THE END
RunTests()
