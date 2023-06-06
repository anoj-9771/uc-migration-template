# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ---------- 

# MAGIC %run ../../Common/common-helpers 
# COMMAND ---------- 


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

# DBTITLE 1,02/08/2022 - Time slice and property type changed
Save(spark.sql(f"""
SELECT 3000010||'2022-01-05' {BK}, 90 PropertyType, '2022-01-05' ValidFrom, '9999-12-31' ValidTo
"""))
DisplaySelf()

# COMMAND ----------


