# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS ${vars.environment}

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${vars.environment}.testinglog
# MAGIC   (testID as string,
# MAGIC   testTimestamp as timestamp,
# MAGIC   testSystem as string,
# MAGIC   testTable as string,
# MAGIC   sourceFields as string,
# MAGIC   sourceSchema as string,
# MAGIC   sourceRowCount as int,
# MAGIC   targetFields as string,
# MAGIC   targetSchema as string,
# MAGIC   targetRowCount as int,
# MAGIC   passCount as int,
# MAGIC   failCount as int,
# MAGIC   status as string)
