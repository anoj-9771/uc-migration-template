# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

CleanSelf()
DEBUG = 1

# COMMAND ----------

# DBTITLE 1,01/08/2022 - Inital Load
Save(spark.sql(f"""
SELECT 3000010||'2022-01-01' {BK}, 70 PropertyType, '2022-01-01' ValidFrom, '9999-12-31' ValidTo
"""))
DisplaySelf()

# COMMAND ----------

# DBTITLE 1,02/08/2022 - Multiple changes to time slice as Property Type changed
Save(spark.sql(f"""
SELECT 3000010||'2021-12-01' {BK}, 70 PropertyType, '2021-12-01' ValidFrom, '2022-01-31' ValidTo
UNION SELECT 3000010||'2022-02-01' {BK}, 998 PropertyType, '2022-02-01' ValidFrom, '9999-12-31' ValidTo
"""))
DisplaySelf()

# COMMAND ----------


