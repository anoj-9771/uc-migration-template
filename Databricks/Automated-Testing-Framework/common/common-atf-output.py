# Databricks notebook source
from pyspark.sql.functions import expr, col

# COMMAND ----------

def PopulateTestResults():
    table = "atf.f_testresult"
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    runJobGroup = "split(run.jobGroup, '[-]', 0)"
    url = "|| '/' ||".join(["run.extraContext.api_url", "'?o=' || run.tags.orgId || '#job'", f"{runJobGroup}[1]", f"{runJobGroup}[2]", f"{runJobGroup}[3]"])
    df = spark.read.option("multiline", "true").json("/mnt/datalake-raw/atf/*/*/*/*")
    df = (df.selectExpr("explode(results) results", "run", "BatchId")
        .select("results.*", "run", "BatchId")
        .selectExpr(
            "BatchId"
            ,f"{url} url"
            ,"RANK() OVER (PARTITION BY BatchId ORDER BY BatchId) Rank"
            ,"split(Object, '[.]', 0)[0] Database" 
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
    df.write.saveAsTable(table)
    #display(df)
#PopulateTestResults()

# COMMAND ----------


