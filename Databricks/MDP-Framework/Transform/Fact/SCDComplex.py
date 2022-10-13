# Databricks notebook source
# MAGIC %run ../../Common/common-transform

# COMMAND ----------

# DBTITLE 1,Cleanup For Demo
CleanSelf()
DEBUG = 1

# COMMAND ----------

# DBTITLE 1,First insert
Save(
spark.sql(f"SELECT 1 || '-' || 'a' {BK}, '1' Key_1 ,'a' Key_2, '2024-01-01' ValidFrom, '9999-12-31' ValidTo, 'Version_1' TestingName \
UNION SELECT 2 || '-' || 'b' {BK}, '2' Key_1 ,'b' Key_2, '2020-01-01' ValidFrom, '2022-05-04' ValidTo, 'Version_1' TestingName \
")

)
DisplaySelf()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM GETDATE() 
# MAGIC WHERE BETWEEN ValidFrom AND ValidTo
# MAGIC --WHERE BETWEEN _Created AND _Ended
# MAGIC _Current = 1
