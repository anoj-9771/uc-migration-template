# Databricks notebook source
# MAGIC %run ../../common/common-atf

# COMMAND ----------

keyColumns =  'objectTypeCode,workNoteType,workNoteId'
mandatoryColumns = 'objectTypeCode,workNoteType,workNoteId'

columns = ("""objectID
,objectTypeCode
,objectType
,workNoteId
,workNoteLineNumber
,workNoteType
,workNotes
,createdBySK
,createdTimeStamp
,modifiedBySK
,modifiedTimeStamp
""")


# COMMAND ----------

#ALWAYS RUN THIS AT THE END
RunTests()

# COMMAND ----------


