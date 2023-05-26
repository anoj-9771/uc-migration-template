# Databricks notebook source
# MAGIC %run /Automated-Testing-Framework/common/common-atf

# COMMAND ----------

df = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("dataAddress", "TableList!A1") \
    .load("dbfs:/FileStore/dimTableList.xlsx") #.display()

uc3tbs = df.select("Num", "JiraNum", "Tablename", "MappingPath", "Sheetname", "FD_OR_SD") \
           .filter(df.UseCase == 'UC3dim') \
           .cache()

uc3tbs.display()

# COMMAND ----------

# DBTITLE 1,ATF dim tables 1-4
RunATFGivenRange("curated_v3", uc3tbs, 1, 4)

# COMMAND ----------

# DBTITLE 1,ATF dim tables 5-8
RunATFGivenRange("curated_v3", uc3tbs, 5, 8)

# COMMAND ----------

# DBTITLE 1,ATF dim tables 9-12
RunATFGivenRange("curated_v3", uc3tbs, 9, 12)

# COMMAND ----------

# DBTITLE 1,Useful Functions that can be used
PrintHelpFunc()
