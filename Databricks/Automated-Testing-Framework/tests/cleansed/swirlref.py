# Databricks notebook source
# MAGIC %run ../../common/common-atf

# COMMAND ----------

#set system code to notebook name
list = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split("/")
systemCode = list[len(list)-1]
#print(f"SystemCode:{systemCode}")
#get full list of tables to be tested from extract load manifest where the system code matches and the extract is enabled
df = spark.table("controldb.dbo_extractLoadManifest").where(f"systemCode = '{systemCode}' and enabled = 1")

# COMMAND ----------

#define mapping document path and sheet name
DOC_PATH = 'dbfs:/mnt/data/mapping_documents_UC3/SWIRL_raw2cleansed_v1.xlsx' 
SHEET_NAME = 'cleansed_mapping'

# COMMAND ----------

#loop through df and run tests for each table printing out results
for i in df.collect():
    TABLE_FQN = f"cleansed.{i.DestinationSchema}_{i.DestinationTableName}"
    print(f"Running tests for table: {TABLE_FQN}...")
    RunTests()
