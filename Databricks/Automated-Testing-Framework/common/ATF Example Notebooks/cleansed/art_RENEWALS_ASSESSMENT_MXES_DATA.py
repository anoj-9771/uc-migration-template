# Databricks notebook source
# MAGIC %run /Automated-Testing-Framework/common/common-atf

# COMMAND ----------

#define mapping document path and sheet name
DOC_PATH = 'dbfs:/mnt/data/mapping_documents_UC3/ART_Raw2Cleansed_V2'
SHEET_NAME = 'Cleansed_mapping_v1'

# COMMAND ----------

RunTests()
ClearCache()
