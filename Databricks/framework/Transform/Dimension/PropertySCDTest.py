# Databricks notebook source
# MAGIC %run ../../Common/common-transform 

# COMMAND ---------- 

# MAGIC %run ../../Common/common-helpers 
# COMMAND ---------- 


# COMMAND ----------

# DBTITLE 1,Cleanup For Demo
CleanSelf()
DEBUG = 1

# COMMAND ----------

# DBTITLE 1,First insert
Save(spark.sql(f"""
SELECT 3192669||'1984-07-01'  {BK}, 70 type, '1984-07-01' sapfrom, '9999-12-31' sapto
UNION SELECT 3101005||'1984-07-01' {BK}, 70 type, '1984-07-01' sapfrom, '9999-12-31' sapto
UNION SELECT 3143939||'1984-07-01' {BK}, 70 type, '1984-07-01' sapfrom, '9999-12-31' sapto
"""))
DisplaySelf()

# COMMAND ----------

# DBTITLE 1,New BK=3 v1
Save(spark.sql(f"""
SELECT 3192669||'1984-07-01' {BK}, 70 type, '1984-07-01' sapfrom, '9999-12-31' sapto
UNION SELECT 3101005||'1984-07-01' {BK}, 70 type, '1984-07-01' sapfrom, '2021-06-30' sapto
UNION SELECT 3143939||'1984-07-01' {BK}, 70 type, '1984-07-01' sapfrom, '9999-12-31' sapto
UNION SELECT 3101005||'2021-07-01' {BK}, 998 type, '2021-07-01' sapfrom, '9999-12-31' sapto
"""))
DisplaySelf()

# COMMAND ----------

Save(spark.sql(f"""
SELECT 31010051984||'1984-08-01' {BK}, 70 type, '1984-07-01' sapfrom, '9999-12-31' sapto
"""))
DisplaySelf()

# COMMAND ----------

Save(spark.sql(f"""
SELECT 31010051984||'1984-08-01' {BK}, 70 type, '1984-07-01' sapfrom, '9999-12-31' sapto
"""))
DisplaySelf()
