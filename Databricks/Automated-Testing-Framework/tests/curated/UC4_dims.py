# Databricks notebook source
# MAGIC %run /Automated-Testing-Framework/common/common-atf

# COMMAND ----------

df = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("dataAddress", "TableList!A1") \
    .load("dbfs:/FileStore/dimTableList.xlsx") #.display()

uc4tbs = df.select("Num", "JiraNum", "Tablename", "MappingPath", "Sheetname", "FD_OR_SD") \
           .filter(df.UseCase == 'UC4dim') \
           .cache()

uc4tbs.display()

# COMMAND ----------

# DBTITLE 1,ATF dim tables 1-5
RunATFGivenRange("curated", uc4tbs, 1, 5)

# COMMAND ----------

# DBTITLE 1,ATF dimAssetSpec (separated due to longer runtime)
RunATFGivenTbNums("curated", uc4tbs, {6})

# COMMAND ----------

# DBTITLE 1,ATF dim tables 7-9
RunATFGivenRange("curated", uc4tbs, 7, 9)

# COMMAND ----------

# DBTITLE 1,Useful Functions that can be used
PrintHelpFunc()
