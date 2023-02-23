# Databricks notebook source
def ApplyColumnMetadata():
    PATH = "/FileStore/iicats_cleansed_dev.csv"
    df = spark.read.option("header", "true").format("csv").load(PATH)
    df.display()
    
    # UPDATE COMMENT ON TABLE
    for i in ["ALTER TABLE {0} ALTER COLUMN `{1}` COMMENT '{2}';".format(i.CleansedTable.strip(" "), i.CleansedColumnName.strip(" "), i.Description) for i in df.collect()]:
        #print(i)
        spark.sql(i)
    table = df.select("CleansedTable").dropDuplicates().collect()[0][0]
    spark.sql(f"DESCRIBE TABLE {table}").display()
#ApplyColumnMetadata()

# COMMAND ----------


