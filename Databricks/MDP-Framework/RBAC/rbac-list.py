# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
df = spark.sql("SHOW TABLES IN cleansed")#.limit(3)

resultsDf = spark.createDataFrame([], 
        StructType([
            StructField('Principal', StringType()),
            StructField('ActionType', StringType()),
            StructField('ObjectType', StringType()),
            StructField('ObjectKey', StringType()),
            StructField('Schema', StringType()),
            StructField('Table', StringType()),
        ])
 )

for i in df.rdd.collect():
    sql = f"SHOW GRANTS ON TABLE `{i.database}`.`{i.tableName}`"
    resultsDf = resultsDf.union(
        spark.sql(sql).where(f"ObjectType = 'TABLE' AND ActionType != 'OWN'")
        .withColumn("ObjectKey", expr("replace(ObjectKey, '`', '')"))
        .withColumn("Schema", expr("split(ObjectKey, '[.]', 0)[0]"))
        .withColumn("Table", expr("split(ObjectKey, '[.]', 0)[1]"))
    ) 
resultsDf = resultsDf.withColumn("CreatedDTS", expr("CURRENT_TIMESTAMP()"))
#resultsDf.write.mode("append").saveAsTable("atf.f_rbac_table_assignment")
#display(resultsDf)

# COMMAND ----------


