# Databricks notebook source
# MAGIC %run ../../common/common-atf

# COMMAND ----------

keyColumns = 'primaryFK, secondaryFK, relationshipType'
mandatoryColumns = 'primaryFK, secondaryFK, relationshipType'

col_list = [
        "primaryFK"
        ,"secondaryFK"
        ,"relationshipType"
]

columns = ",".join(col_list)

# COMMAND ----------

columns

# COMMAND ----------

#Ignore BusinessKeyLength check
_automatedMethods[GetDatabaseName()] = [x for x in _automatedMethods.get(GetDatabaseName()) if x != 'BusinessKeyLength']

# COMMAND ----------

#ALWAYS RUN THIS AT THE END
RunTests()
