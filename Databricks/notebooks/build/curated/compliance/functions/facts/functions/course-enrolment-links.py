# Databricks notebook source
# MAGIC %run /build/includes/util-log

# COMMAND ----------

# MAGIC %run /build/includes/connect-sqldb

# COMMAND ----------

try:
  LogEtl("Collecting linked fee records recursively for course enrolments - starts...")
  AzSqlExecTSQL("exec compliance.PopulateAvetmissFeeRecord")
  LogEtl("Collecting linked fee records recursively for course enrolments - ends...")
  dbutils.notebook.exit("1")
except Exception as e:
  raise e
  
