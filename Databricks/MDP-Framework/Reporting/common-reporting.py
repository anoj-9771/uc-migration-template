# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

# COMMAND ----------

def DatabaseAllRowCounts():
    databases = spark.sql("SHOW DATABASES").where("databaseName IN ('raw', 'cleansed', 'curated')")
    allRowCounts = None
    
    for db in databases.collect():
        database = db.databaseName
        databaseTables = (spark.sql(f"SHOW TABLES FROM {database}").alias("t")
                          .join(spark.sql(f"SHOW VIEWS FROM {database}").alias("v"), expr("t.tableName == v.viewName"), "left")
                          .selectExpr("t.*"
                              ,"IF(viewName IS NULL, 0, 1) IsView")).withColumn("RowCount", expr("0"))

        for t in databaseTables.collect():
            table = t.tableName
            try:
                count = 0
                #count = spark.table(f"{database}.{table}").count()
                databaseTables = databaseTables.withColumn("RowCount", when(expr(f"tableName = '{table}'"), expr(f"{count}")).otherwise(expr("RowCount")))
            except:
                pass
        allRowCounts = databaseTables if allRowCounts is None else allRowCounts.union(databaseTables)

    return allRowCounts

# COMMAND ----------

def WriteOutput():
    df = spark.table("edp.f_database_tables")
    df = df.withColumn("Date", expr("CAST('2022-11-30' AS DATE)"))
    df.coalesce(1).write.format("csv").mode("overwrite").save("/mnt/datalake-raw/reporting")
    dbutils.fs.ls("/mnt/datalake-raw/reporting")

# COMMAND ----------

def Transform():
    j = json.loads(spark.conf.get("spark.databricks.clusterUsageTags.clusterAllTags"))
    environment = [x['value'] for x in j if x['key'] == 'Environment'][0]
    allRowsDf = DatabaseAllRowCounts()
    mdpDf = spark.table("controldb.dbo_extractloadmanifest")
    nstDf = spark.sql("""
      SELECT
      SourceGroup SystemCode
      ,LOWER(SourceName) SourceName
      ,LOWER(REPLACE(SourceName, '_' || SourceLocation, '')) SourceSchema
      ,LOWER(SourceLocation) SourceTable
      FROM controldb.ctl_controltasks T
      JOIN controldb.ctl_controlprojects P ON P.ProjectId = T.ProjectId
      JOIN controldb.ctl_controlsource S ON S.SourceId = T.SourceId
    """)

    df = (allRowsDf.alias("C")
      .join(nstDf.alias("I"), expr("LOWER(C.tableName) = LOWER(I.SourceName) AND SourceTable NOT LIKE '%/%'"), "left")
      .join(mdpDf.alias("M"), expr("LOWER(C.tableName) = LOWER(M.SourceSchema || '_' || M.SourceTableName)"), "left")
    )
    df = df.selectExpr(
        f"'{environment}' Environment"
        ,"LOWER(C.database) Database"
        ,"CASE WHEN C.database = 'raw' THEN 0 ELSE 1 END DatabaseSort"
        ,"LOWER(C.tableName) TableName"
        ,"I.SystemCode NSTSystemCode"
        ,"M.SystemCode MDPSystemCode"
        ,"COALESCE(I.SystemCode, M.SystemCode) SystemCode"
        ,"""CASE 
            WHEN I.SystemCode IS NOT NULL THEN 'NST' 
            WHEN M.SystemCode IS NOT NULL THEN 'EDP' 
            ELSE NULL END Framework
        """
        ,"""CASE 
            WHEN I.SystemCode IS NOT NULL THEN 1
            WHEN M.SystemCode IS NOT NULL THEN 2
            ELSE 0 END FrameworkSort
        """
    )
    df = df.withColumn("Date", expr("CURRENT_TIMESTAMP()"))
    #display(df)
    #spark.sql(f"DROP TABLE IF EXISTS edp.f_database_tables")
    #df.write.saveAsTable("edp.f_database_tables")
    df.write.mode("append").saveAsTable("edp.f_database_tables")
Transform()

# COMMAND ----------


