# Databricks notebook source
# MAGIC %run ../../atf-common

# COMMAND ----------

def SimpleTestCase1():
    Assert("1","1")

# COMMAND ----------

def TestSql():
    table = f"curated.{GetNotebookName()}"
    df = spark.sql(f"SELECT * FROM {table}")
    Assert(table,table)

# COMMAND ----------

def SimpleTestCase2():
    Assert("a","b")

# COMMAND ----------

def Jira_1234():
    Assert("a","b")

# COMMAND ----------

#ALWAYS RUN THIS AT THE END
RunTests()

# COMMAND ----------


