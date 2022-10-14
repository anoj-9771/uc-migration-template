# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

# DBTITLE 1,Cleanup For Demo
CleanSelf()
DEBUG = 1

# COMMAND ----------

Save(spark.sql(f"SELECT 1 {BK}, 'Version_1' TestingName \
UNION SELECT 2 {BK}, 'Version_1' TestingName \
"))
DisplaySelf()

# COMMAND ----------

Save(spark.sql(f"SELECT 1 {BK}, 'Version_1' TestingName"))
DisplaySelf()

# COMMAND ----------

Save(spark.sql(f"SELECT 1 {BK}, 'Version_2' TestingName"))
DisplaySelf()

# COMMAND ----------

Save(spark.sql(f"SELECT 2 {BK}, 'Version_2' TestingName"))
DisplaySelf()

# COMMAND ----------

Save(spark.sql(f"SELECT 2 {BK}, 'Version_3' TestingName"))
DisplaySelf()

# COMMAND ----------

Save(spark.sql(f"SELECT 3 {BK}, 'Version_1' TestingName"))
DisplaySelf()
