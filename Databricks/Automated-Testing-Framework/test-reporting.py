# Databricks notebook source
from pyspark.sql.functions import expr, col
spark.sql("DROP TABLE IF EXISTS atf.f_testresult")
df = spark.read.option("multiline", "true").json("/mnt/datalake-raw/atf/*/*/*/*")
df = (df.selectExpr("explode(results) results", "run", "BatchId")
    .select("results.*", "run", "BatchId")
    .selectExpr(
        "BatchId"
        ,"RANK() OVER (PARTITION BY BatchId ORDER BY BatchId) Rank"
        ,"Object"
        ,"Type"
        ,"Case"
        ,"Input"
        ,"Output"
        ,"Result"
        ,"Passed"
        ,"Error"
        ,"Start"
        ,"End"
    )
 )
display(df)

#df.write.saveAsTable("atf.f_testresult")

# COMMAND ----------

df = spark.read.option("multiline", "true").json("/mnt/datalake-raw/atf/*/*/*/*")
display(df)

# COMMAND ----------


