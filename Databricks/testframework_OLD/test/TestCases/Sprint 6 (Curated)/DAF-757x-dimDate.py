# Databricks notebook source
from pyspark.sql.types import *



# COMMAND ----------



df_dual = sc.parallelize([Row(r=Row("dummy"))]).toDF()

df_dual.printSchema()
df_dual.show()

df_dual.registerTempTable("dual")
result = sqlContext.sql("select 'Hello Spark Dual!' hi from dual")

result.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed.t_sapisu_scal_tt_date

# COMMAND ----------

# MAGIC %sql
# MAGIC DECLARE @StartDate DATE, @EndDate DATE
# MAGIC SELECT @StartDate = '2021-11-01', @EndDate = '2021-12-01'; 
# MAGIC WITH ListDates(AllDates) AS
# MAGIC (    SELECT @StartDate AS DATE
# MAGIC     UNION ALL
# MAGIC     SELECT DATEADD(DAY,1,AllDates)
# MAGIC     FROM ListDates 
# MAGIC     WHERE AllDates < @EndDate)
# MAGIC SELECT AllDates
# MAGIC FROM ListDates
# MAGIC GO
