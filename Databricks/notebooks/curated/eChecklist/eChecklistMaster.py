# Databricks notebook source
from pyspark.sql.functions import *
from datetime import datetime
from pyspark.sql.window import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %run ./functions/eChecklist-all

# COMMAND ----------

# DBTITLE 1,Get Base Data
dfChkChecklists=spark.sql("select * from trusted.echecklist_dbo_checklists_checklists")
dfChkWorkUnitLocales=spark.sql("select * from trusted.echecklist_dbo_checklists_work_unit_locales")
dfChkNameSegments = spark.sql("select * from trusted.echecklist_dbo_checklists_name_segments")
dfChkActions = spark.sql("select * from trusted.echecklist_dbo_checklists_actions")
dfChkTasks = spark.sql("select * from trusted.echecklist_dbo_checklists_tasks")
dfChkTaskStauses = spark.sql("select * from trusted.echecklist_dbo_checklists_task_statuses")
dfChkStauses = spark.sql("select * from trusted.echecklist_dbo_checklists_statuses")
dfChkStauses2 = spark.sql("select * from trusted.echecklist_dbo_checklists_statuses")
dfChkInstances = spark.sql("select * from trusted.echecklist_dbo_checklists_instances")
dfChkInstHistory= spark.sql("select * from trusted.echecklist_dbo_checklists_instance_history")

# COMMAND ----------

# DBTITLE 1,ChecklistTemplates
#call the functions
SaveChecklistTemplates(dfChkChecklists, dfChkWorkUnitLocales)
#write df into a table - truncate and reload - no history



# COMMAND ----------

# DBTITLE 1,ChecklistActions-ValidRecords
SaveChecklistActions(dfChkChecklists, dfChkNameSegments, dfChkActions, dfChkTasks, dfChkTaskStauses, dfChkStauses, dfChkStauses2, dfChkInstances)


# COMMAND ----------

# DBTITLE 1,ChecklistActionsSummary
#call the functions
dfValidRecords = spark.sql("select * from echecklist.checklistactions")
SaveChecklistActionsSummary(dfChkInstHistory, dfChkInstances,dfValidRecords)
#write dfCT into a table - truncate and reload - no history
