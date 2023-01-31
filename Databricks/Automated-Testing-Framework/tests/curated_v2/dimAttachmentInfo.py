# Databricks notebook source
# MAGIC %run ../../common/common-atf

# COMMAND ----------

keyColumns =  'documentId'
mandatoryColumns = 'fileName, fileType, fileSize'

col_list = [
        "fileName"
        ,"fileType"
        ,"fileSize"
        ,"documentId"
#         ,"createdBy"
        ,"createdByUserID"
        ,"createdDateTime"
#         ,"modifiedBy"
        ,"modifiedByUserID"
        ,"modifiedDateTime"
#         ,"sourceSystemCode"
]

columns = ",".join(col_list)

# COMMAND ----------

columns

# COMMAND ----------

#ALWAYS RUN THIS AT THE END
RunTests()
