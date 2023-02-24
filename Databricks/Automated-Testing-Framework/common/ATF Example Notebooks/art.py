# Databricks notebook source
# MAGIC %run /Automated-Testing-Framework/common/common-atf

# COMMAND ----------

#set system code to notebook name
list = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split("/")
systemCode = list[len(list)-1]
print(systemCode)

#get full list of tables to be tested from extract load manifest where the system code matches and the extract is enabled
df = spark.table("controldb.dbo_extractLoadManifest").where(f"systemCode in('{systemCode}', concat('{systemCode}','data'), concat('{systemCode}','ref')) and enabled = 1") \
     .select("DestinationSchema","DestinationTableName").cache()
df.count()

# COMMAND ----------

#define mapping document path and sheet name
DOC_PATH = 'dbfs:/mnt/data/mapping_documents_UC3/ART_Raw2Cleansed_V2'
SHEET_NAME = 'Cleansed_mapping_v1'

# COMMAND ----------

# Prints all the tables in the source system
j = 1
for i in df.collect():
    TABLE_FQN = f"{j}. cleansed.{i.DestinationSchema}_{i.DestinationTableName}"
    print(TABLE_FQN)
    j+=1

# COMMAND ----------

# To run ATF by specifying table numbers from cmd4
tableNums = {3,4,7}
tableNo = 1
print(f"Running tests for the following tables:")  
for i in df.collect():
    if tableNo in tableNums:
        TABLE_FQN = f"cleansed.{i.DestinationSchema}_{i.DestinationTableName}"
        print(TABLE_FQN)
    tableNo+=1
    
print('')
tableNo = 1
for i in df.collect(): 
    if tableNo in tableNums:
        TABLE_FQN = f"cleansed.{i.DestinationSchema}_{i.DestinationTableName}"
        print(f"Running tests for table: {TABLE_FQN}...")
        RunTests()
    tableNo+=1

df.unpersist()
ClearCache()

# COMMAND ----------

# To run ATF on a smaller range of tables 
# Supply the range (1 ... Total no. of tables) of tables you wish to test from cmd4
start = 8
finish = 11

print(f"Total number of {systemCode} tables: {df.count()}. Running tests for the following tables (no.{start} - {finish}):")    
for i in df.collect()[start - 1:finish]: 
    TABLE_FQN = f"cleansed.{i.DestinationSchema}_{i.DestinationTableName}"
    print(TABLE_FQN)
    
print('')
for i in df.collect()[start - 1:finish]: 
    TABLE_FQN = f"cleansed.{i.DestinationSchema}_{i.DestinationTableName}"
    print(f"Running tests for table: {TABLE_FQN}...")
    RunTests()

df.unpersist()
ClearCache()

# COMMAND ----------

# To run ATF for all tables in the source system
for i in df.collect():
    TABLE_FQN = f"cleansed.{i.DestinationSchema}_{i.DestinationTableName}"
    print(f"Running tests for table: {TABLE_FQN}...")
    RunTests()

df.unpersist()
ClearCache()
