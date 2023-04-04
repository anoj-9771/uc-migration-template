# Databricks notebook source
# DBTITLE 1,Input system code(s) and tables
SystemCode = "('maximo','iicatsref')"
TableList = ('SWCLGA','docinfo','std_asset_type')

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %run ../Common/common-include-all

# COMMAND ----------

# DBTITLE 1,Extract Load Manifest
manifest_df = ( 
                spark.table("controldb.dbo_extractloadmanifest")
                    .filter(f"SystemCode in {SystemCode}")
                    .filter(f"SourceTableName in {TableList}")
               )
display(manifest_df)

# COMMAND ----------

# DBTITLE 1,Latest Batch Info
dataLoadStatus_df = spark.sql(f"select dataLoadStatus.SourceID,dataLoadStatus.SystemCode,dataLoadStatus.BatchID,dataLoadManifest.SourceTableName,dataLoadStatus.ID from (Select ROW_NUMBER() over (partition by systemcode, sourceid order by rawstartdts desc) row_num, systemCode, SourceId, BatchID, ID from controldb.dbo_ExtractLoadStatus) dataLoadStatus, controldb.dbo_ExtractLoadManifest dataLoadManifest where dataLoadStatus.SourceID = dataLoadManifest.SourceID and dataLoadStatus.SystemCode = dataLoadManifest.SystemCode and dataLoadStatus.row_num = 1 and dataLoadManifest.SystemCode in {SystemCode} and dataLoadManifest.SourceTableName in {TableList}")
display(dataLoadStatus_df)
dataLoadStatus_df.createOrReplaceTempView("vwDataLoadBatchInfo")

# COMMAND ----------

def DeleteDirectoryRecursive(dirname):
    files=dbutils.fs.ls(dirname)
    for f in files:
        if f.isDir():
            DeleteDirectoryRecursive(f.path)
        dbutils.fs.rm(f.path, recurse=True)
    dbutils.fs.rm(dirname, True)

# COMMAND ----------

def CleanTable(tableNameFqn):
    try:
        detail = spark.sql(f"DESCRIBE DETAIL {tableNameFqn}").collect()[0]
        DeleteDirectoryRecursive(detail.location)
    except:    
        pass
    
    try:
        spark.sql(f"DROP TABLE {tableNameFqn}")
    except:
        pass

# COMMAND ----------

# DBTITLE 1,Running this block will end the job run when there is 1 failure
# for j in manifest_df.collect():
#     systemCode = j.SystemCode
#     destinationSchema = j.DestinationSchema
#     destinationTableName = j.DestinationTableName
#     cleansedPath = j.CleansedPath
#     businessKey = j.BusinessKeyColumn
#     destinationKeyVaultSecret = j.DestinationKeyVaultSecret
#     extendedProperties = j.ExtendedProperties
#     watermarkColumn = j.WatermarkColumn
#     dataLakePath = cleansedPath.replace("/cleansed", "/mnt/datalake-cleansed")
#     sourceTableName = f"raw.{destinationSchema}_{destinationTableName}"
#     cleansedTableName = f"cleansed.{destinationSchema}_{destinationTableName}"

#     #print(f" Cleaning up table: {cleansedTableName}")
#     try:
#         CleanTable(cleansedTableName)
#     except Exception as e:
#         pass

#     #GET LAST CLEANSED LOAD TIMESTAMP
#     try:
#         lastLoadTimeStamp = spark.sql(f"select max(_DLCleansedZoneTimeStamp) as lastLoadTimeStamp from {cleansedTableName}").collect()[0][0]
#         print(lastLoadTimeStamp)
#     except Exception as e:
#         if "Table or view not found" in str(e):
#             lastLoadTimeStamp = '2022-01-01'  

#     sourceDataFrame = spark.sql(f"select * from {sourceTableName} where _DLRawZoneTimeStamp > '{lastLoadTimeStamp}'")
#     sourceDataFrame = sourceDataFrame.groupby(sourceDataFrame.columns[0:-1]).count().drop("count")
#     CleansedSourceCount = sourceDataFrame.count()

#     # FIX BAD COLUMNS
#     sourceDataFrame = sourceDataFrame.toDF(*(c.replace(' ', '_') for c in sourceDataFrame.columns))

#     # CLEANSED QUERY FROM RAW TO FLATTEN OBJECT
#     if(extendedProperties):
#         extendedProperties = json.loads(extendedProperties)
#         cleansedPath = extendedProperties.get("CleansedQuery")
#         if(cleansedPath):
#             sourceDataFrame = spark.sql(cleansedPath.replace("{tableFqn}", sourceTableName))

#     # APPLY CLEANSED FRAMEWORK
#     cleanseDataFrame = CleansedTransform(sourceDataFrame, sourceTableName.lower(), systemCode)
#     cleanseDataFrame = cleanseDataFrame.withColumn("_DLCleansedZoneTimeStamp",current_timestamp()) \
#                                        .withColumn("_RecordCurrent",lit('1')) \
#                                        .withColumn("_RecordDeleted",lit('0')) \
#                                        .withColumn("_RecordStart",current_timestamp()) \
#                                        .withColumn("_RecordEnd",to_timestamp(lit("9999-12-31"), "yyyy-MM-dd"))

#     # GET LATEST RECORD OF THE BUSINESS KEY
#     if(extendedProperties):
#         groupOrderBy = extendedProperties.get("GroupOrderBy")
#         if(groupOrderBy):
#             cleanseDataFrame.createOrReplaceTempView("vwCleanseDataFrame")
#             cleanseDataFrame = spark.sql(f"select * from (select vwCleanseDataFrame.*, row_number() OVER (Partition By {businessKey} order by {groupOrderBy}) row_num from vwCleanseDataFrame) where row_num = 1 ").drop("row_num")   

#     tableName = f"{destinationSchema}_{destinationTableName}"
#     CreateDeltaTable(cleanseDataFrame, f"cleansed.{tableName}", dataLakePath) if j.BusinessKeyColumn is None else CreateOrMerge(cleanseDataFrame, f"cleansed.{tableName}", dataLakePath, j.BusinessKeyColumn)

#     CleansedSinkCount = spark.table(cleansedTableName).count()
#     print(f"!!**SUCCESS**!! CleansedTable: {cleansedTableName} CleanseTableCount: {CleansedSinkCount}")
    
#     #Update LoadStatus with task stats
#     #currentTimeStamp = spark.sql("select current_timestamp() as currentTimeStamp").first()["currentTimeStamp"] 
#     extractLoadStatusID = spark.sql(f"select ID as extractLoadStatusID from vwDataLoadBatchInfo loginfo where loginfo.sourceTableName = '{destinationTableName}'").first()["extractLoadStatusID"]
#     updateSql = f"update dbo.extractLoadStatus set CleansedStatus ='Success', CleansedSinkCount = {CleansedSinkCount}, CleansedEndDTS = getdate(), EndedDTS = getdate() where ID = {extractLoadStatusID}"
#     #print(updateSql)
#     ExecuteStatement(updateSql)

# COMMAND ----------

# DBTITLE 1,Running this block will continue the job run even if there is table load failure
for j in manifest_df.collect():
    try:
        systemCode = j.SystemCode
        destinationSchema = j.DestinationSchema
        destinationTableName = j.DestinationTableName
        cleansedPath = j.CleansedPath
        businessKey = j.BusinessKeyColumn
        destinationKeyVaultSecret = j.DestinationKeyVaultSecret
        extendedProperties = j.ExtendedProperties
        watermarkColumn = j.WatermarkColumn
        dataLakePath = cleansedPath.replace("/cleansed", "/mnt/datalake-cleansed")
        rawTableNameMatchSource = None
        if extendedProperties:
            extendedProperties = json.loads(extendedProperties)
            rawTableNameMatchSource = extendedProperties.get("RawTableNameMatchSource")

        if rawTableNameMatchSource:
            sourceTableName = f"raw.{destinationSchema}_{sourceTable}".lower()
        else:
            sourceTableName = f"raw.{destinationSchema}_{destinationTableName}".lower()
        cleansedTableName = f"cleansed.{destinationSchema}_{destinationTableName}"
        
        #print(f" Cleaning up table: {cleansedTableName}")
        try:
            CleanTable(cleansedTableName)
        except Exception as e:
            pass
        
        #GET LAST CLEANSED LOAD TIMESTAMP
        try:
            lastLoadTimeStamp = spark.sql(f"select max(_DLCleansedZoneTimeStamp) as lastLoadTimeStamp from {cleansedTableName}").collect()[0][0]
            print(lastLoadTimeStamp)
        except Exception as e:
            if "Table or view not found" in str(e):
                lastLoadTimeStamp = '2022-01-01'  
                
        sourceDataFrame = spark.sql(f"select * from {sourceTableName} where _DLRawZoneTimeStamp > '{lastLoadTimeStamp}'")
        sourceDataFrame = sourceDataFrame.groupby(sourceDataFrame.columns[0:-1]).count().drop("count")
        CleansedSourceCount = sourceDataFrame.count()

        # FIX BAD COLUMNS
        sourceDataFrame = sourceDataFrame.toDF(*(c.replace(' ', '_') for c in sourceDataFrame.columns))
        
        # CLEANSED QUERY FROM RAW TO FLATTEN OBJECT
        if(extendedProperties):
            cleansedPath = extendedProperties.get("CleansedQuery")
            if(cleansedPath):
                sourceDataFrame = spark.sql(cleansedPath.replace("{tableFqn}", sourceTableName))
    
        # APPLY CLEANSED FRAMEWORK
        cleanseDataFrame = CleansedTransform(sourceDataFrame, sourceTableName.lower(), systemCode)
        cleanseDataFrame = cleanseDataFrame.withColumn("_DLCleansedZoneTimeStamp",current_timestamp()) \
                                           .withColumn("_RecordCurrent",lit('1')) \
                                           .withColumn("_RecordDeleted",lit('0')) \
                                           .withColumn("_RecordStart",current_timestamp()) \
                                           .withColumn("_RecordEnd",to_timestamp(lit("9999-12-31"), "yyyy-MM-dd"))

        # GET LATEST RECORD OF THE BUSINESS KEY
        if(extendedProperties):
            groupOrderBy = extendedProperties.get("GroupOrderBy")
            if(groupOrderBy):
                cleanseDataFrame.createOrReplaceTempView("vwCleanseDataFrame")
                cleanseDataFrame = spark.sql(f"select * from (select vwCleanseDataFrame.*, row_number() OVER (Partition By {businessKey} order by {groupOrderBy}) row_num from vwCleanseDataFrame) where row_num = 1 ").drop("row_num")   

        tableName = f"{destinationSchema}_{destinationTableName}"
        CreateDeltaTable(cleanseDataFrame, f"cleansed.{tableName}", dataLakePath) if j.BusinessKeyColumn is None else CreateOrMerge(cleanseDataFrame, f"cleansed.{tableName}", dataLakePath, j.BusinessKeyColumn)
        
        CleansedSinkCount = spark.table(cleansedTableName).count()
        print(f"!!**SUCCESS**!! CleansedTable: {cleansedTableName} CleanseTableCount: {CleansedSinkCount}")
        
        #Update LoadStatus with task stats
        #currentTimeStamp = spark.sql("select current_timestamp() as currentTimeStamp").first()["currentTimeStamp"] 
        extractLoadStatusID = spark.sql(f"select ID as extractLoadStatusID from vwDataLoadBatchInfo loginfo where loginfo.sourceTableName = '{destinationTableName}'").first()["extractLoadStatusID"]
        updateSql = f"update dbo.extractLoadStatus set CleansedStatus ='Success', CleansedSinkCount = {CleansedSinkCount}, CleansedEndDTS = getdate(), EndedDTS = getdate() where ID = {extractLoadStatusID}"
        #print(updateSql)
        ExecuteStatement(updateSql)
    except Exception as e:
        print(f"*******Cleansed Reload FAIL {cleansedTableName} Error: {e}")
        pass
