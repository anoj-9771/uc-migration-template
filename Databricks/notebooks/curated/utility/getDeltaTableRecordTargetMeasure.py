# Databricks notebook source
# MAGIC %md
# MAGIC #Get target measure from curated delta table  

# COMMAND ----------

from pyspark.sql.functions import countDistinct, count
from pyspark.sql import SparkSession, SQLContext, Window

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# DBTITLE 1,Define Widgets (Parameters) at the start
#Initialize the Entity Object to be passed to the Notebook
dbutils.widgets.text("target_query", "", "01:Target_Query")


# COMMAND ----------

# DBTITLE 1,Get Values from Widget
target_query = dbutils.widgets.get("target_query")

print(target_query)

# COMMAND ----------

# DBTITLE 1,Execute query and record the target measure
sql_query = target_query

df_targetMeasure = spark.sql(target_query)
if df_targetMeasure.count() > 0:
  TotalNoRecords = df_targetMeasure.collect()[0][0]
  print(TotalNoRecords)
else:
  TotalNoRecords = -1


# COMMAND ----------

dbutils.notebook.exit(TotalNoRecords)
