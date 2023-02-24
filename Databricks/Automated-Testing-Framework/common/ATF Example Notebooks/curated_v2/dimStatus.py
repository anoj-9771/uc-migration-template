# Databricks notebook source
# MAGIC %run /Automated-Testing-Framework/common/common-atf

# COMMAND ----------

#define mapping document path and sheet name
DOC_PATH = 'dbfs:/mnt/data/mapping_documents_UC3/UC03_DataMapping_dimStatus_v0_4'
SHEET_NAME = 'dimStatus'

# COMMAND ----------

RunTests()
ClearCache()
