# Databricks notebook source
# MAGIC %run ../../../includes/include-all-util

# COMMAND ----------

ECHECKLIST_DATABASE_NAME = "echecklist"
ECHECKLIST_DATALAKE_FOLDER = "echecklist"
ECHECKLIST_AZURE_SCHEMA = "tsEChecklist"

# COMMAND ----------

# DBTITLE 1,Function to Save DataFrame to Curated and Azure SQL for eChecklist
def eCheckListSaveDataFrameToCurated(df, table_name, save_to_azure = True):
  
  #Save data frame to eChecklist Databse (Curated Zone)
  DeltaSaveDataFrameToCurated(df, ECHECKLIST_DATABASE_NAME, ECHECKLIST_DATALAKE_FOLDER, table_name, ADS_WRITE_MODE_OVERWRITE, ECHECKLIST_AZURE_SCHEMA, save_to_azure)

