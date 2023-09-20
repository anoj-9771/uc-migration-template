# Databricks notebook source
# MAGIC %md 
# MAGIC Vno| Date      | Who         |Purpose
# MAGIC ---|:---------:|:-----------:|:--------:
# MAGIC 1  |23/03/2023 |Mag          |Run Delta Table Housekeeping Activities

# COMMAND ----------

dbutils.widgets.text("P_logpath", "")
dbutils.widgets.text("P_Schemanm", "")
dbutils.widgets.text("P_Schemapath", "")
dbutils.widgets.text("P_TableNameLoc", "")
dbutils.widgets.text("P_TableName", "")

# COMMAND ----------

#Read Parameters
try:
    logpath  = dbutils.widgets.get("P_logpath")
    Schemanm = dbutils.widgets.get("P_Schemanm")
    Schemapath = dbutils.widgets.get("P_Schemapath")
    TableLoc = dbutils.widgets.get("P_TableNameLoc")
    TableName = dbutils.widgets.get("P_TableName")
    print(f"Parameters: logpath={logpath}, Schemanm={Schemanm}, Schemapath={Schemapath}, TableNameLoc={TableLoc}, TableName={TableName}")
except:
    print(f"Parameter Reading Failed")

# COMMAND ----------

# from delta.tables import *
# import pandas as pd
# from time import strftime, localtime
# import datetime

# ### Parameterize ###
# #logpath  =  "/dbfs/FileStore/MaintenanceLogs/"
# #Schemanm =  "curated_v2"
# #Schemapath = "dbfs:/mnt/datalake-curated-v2/"
# #TableLoc   = 3
# #TableName = "All Tables"
# ###  ####


# file_info = []
# date = datetime.datetime.now().strftime("%Y_%m_%d-%I:%M:%S_%p")
# logfilenm = f"{Schemanm}Logs{date}.csv"
# procfilenm = f"ProcessErrorLogs_{date}.csv"


# def get_file_list(root_path):   
#     for dir_path in dbutils.fs.ls(root_path):
#         if dir_path.isFile() and dir_path.name.endswith("snappy.parquet"):
#             file_info.append({
#                 "File Name": dir_path.path,
#                 "Size": dir_path.size,
#                 "Timestamp": dir_path.modificationTime, 
#                 "TimestampLocal": str(datetime.datetime.fromtimestamp(dir_path.modificationTime/1000, datetime.timezone(datetime.timedelta(hours=10)))),
#                 "TableName": str(dir_path.path).split("/")[int(TableLoc)]
#                 })
#             yield dir_path.path
#         elif dir_path.isDir() and root_path != dir_path.path:
#             yield from get_file_list(dir_path.path)
            

# def writeLog(info):
#     with  open(f"{logpath}{procfilenm}", "w") as f_write:
#         f_write.write(info)
    
# def snapShot(state):
#     list(get_file_list(Schemapath))
#     df_list = pd.DataFrame(file_info)
#     if state == 'B':
#         df_list.to_csv(f'{logpath}B_{logfilenm}')
#         if TableName == 'All Tables':
#             TableList = df_list["TableName"].unique() 
#             return TableList            
#     else:
#         df_list.to_csv(f'{logpath}A_{logfilenm}')
        
        

# def processTable(TableName):
#     try:
#         deltaTable = DeltaTable.forName(spark, TableName)
#         deltaTable.vacuum()
#         spark.sql(f"ANALYZE TABLE {TableName} COMPUTE STATISTICS FOR ALL COLUMNS")
#         spark.sql(f"OPTIMIZE {TableName}")
#         writeLog(f"Table Operation -- {TableName} -- Successful")
#     except:
#         writeLog(f"Table Operation -- {TableName} -- Failed --Please check whether Table/Path exist in metastore")


# def process(df):    
#     if TableName == 'All Tables':        
#         for tbl in df:            
#             processTable(f"{Schemanm}.{tbl}")
#     else:
#         processTable(f"{Schemanm}.{TableName}")
        
        
# df = snapShot('B')
# process(df)
# snapShot('A')

# COMMAND ----------

def processTable(TableName):
    vacuum_blacklist = [
        'curated.fact.billedwaterconsumption'
        ,'curated.fact.dailysupplyapportionedconsumption'
        ,'curated.fact.dailysupplyapportionedaccruedconsumption'
        ,'curated.fact.monthlysupplyapportionedaggregate'
        ,'curated.fact.monthlysupplyapportionedconsumption'
        ,'curated.fact.monthlysupplyapportionedaccruedconsumption'
    ]
    try:
        if not any(TableName for t in vacuum_blacklist if TableName.lower() == t.lower()):
            spark.sql(f"VACUUM {TableName}")       
        spark.sql(f"ANALYZE TABLE {TableName} COMPUTE STATISTICS FOR ALL COLUMNS")
        spark.sql(f"OPTIMIZE {TableName}")
        print(f"Table Operation -- {TableName} -- Successful")
    except:
        print(f"Table Operation -- {TableName} -- Failed --Please check whether Table/Path exist in metastore")

tableDF = spark.sql(f"""SELECT concat_ws('.', table_catalog,table_schema,table_name) as tableName
                             FROM system.information_schema.tables 
                             WHERE data_source_format = 'DELTA' 
                               AND table_catalog like '%curated%' and table_schema in ('brg','dim', 'fact')
                    """).collect()


for tab in tableDF:
    processTable(tab.tableName)
