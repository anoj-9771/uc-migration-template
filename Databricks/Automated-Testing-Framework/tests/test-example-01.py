# Databricks notebook source
# MAGIC %run ../common/common-atf

# COMMAND ----------

# DBTITLE 1,Pass
def SimpleTestPass():
    Assert("a","a")

# COMMAND ----------

# DBTITLE 1,Fail
def SimpleTestFail():
    Assert(1,2)

# COMMAND ----------

# DBTITLE 1,Exception thrown (no table)
def ThrowError():
    df = spark.table("curated.dimlocation")
    df = df.select("LocationID1")
    
    Assert(1,1)

# COMMAND ----------

# DBTITLE 1,Test Results
#ALWAYS RUN THIS AT THE END
RunTests()
