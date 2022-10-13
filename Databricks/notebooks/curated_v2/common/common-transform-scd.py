# Databricks notebook source
from pyspark.sql.functions import *
from delta.tables import *
from datetime import datetime, timedelta

# COMMAND ----------

# Notebook Parameters

dbutils.widgets.text(name = 'source_layer', defaultValue ='cleansed')
dbutils.widgets.text(name = 'target_layer', defaultValue ='curated')
dbutils.widgets.text(name = 'entity_name', defaultValue ='')
dbutils.widgets.text(name = 'entity_type', defaultValue ='dimension')
dbutils.widgets.text(name = 'scd_valid_start_date', defaultValue = 'now')
dbutils.widgets.text(name = 'scd_valid_end_date', defaultValue = '9999-12-31')
dbutils.widgets.text(name = 'business_key_cols', defaultValue = 'id')

BATCH_END_CODE = "000000000000" 
DATE_FORMAT = "yyMMddHHmmss"

SOURCE_LAYER = dbutils.widgets.get("source_layer")
TARGET_LAYER = dbutils.widgets.get("target_layer")
Entity_Name = dbutils.widgets.get("entity_name")
Entity_Type = dbutils.widgets.get("entity_type")
SCD_START_DATE = dbutils.widgets.get("scd_valid_start_date")
SCD_END_DATE =  dbutils.widgets.get("scd_valid_end_date")
BUSINESS_KEY_COLS = dbutils.widgets.get("business_key_cols")
SNAP_SHOT=1
DATE_GRANULARITY='Day'

# COMMAND ----------

class BlankClass(object):
    pass

class SCDTransform( BlankClass ):
    
    Counts = BlankClass()
        
    def __init__(self, Entity_Type, Entity_Name, BUSINESS_KEY_COLS, TARGET_LAYER, SNAP_SHOT, DATE_GRANULARITY):
        self.Snapshot = SNAP_SHOT
        self.DateGranularity = DATE_GRANULARITY
        self.EntityType = f"{Entity_Type}"
        self.EntityName = f"{Entity_Name}"
        self.BK = "_BusinessKey"
        self.BusinessKeyCols = f"{BUSINESS_KEY_COLS}"
        self.SurrogateKey = f"{self.EntityName}SK"
        self.SurrogateKey = self.SurrogateKey[0].lower() + self.SurrogateKey[1:]
        self.TargetTable = f"{TARGET_LAYER}.dim{self.EntityName}"
        self.MountPoint = DataLakeGetMountPoint(ADS_CONTAINER_CURATED_V2)
        self.DataLakePath = f"dbfs:{self.MountPoint}/dim{self.EntityName.lower()}/delta"

# COMMAND ----------

# Create CuratedTransform object

_ = SCDTransform(Entity_Type, Entity_Name, BUSINESS_KEY_COLS, TARGET_LAYER, SNAP_SHOT, DATE_GRANULARITY)

# COMMAND ----------

def addSCDColumns(dataFrame, scd_start_date = SCD_START_DATE, scd_end_date = SCD_END_DATE, _=_):
    
    if scd_start_date == 'now':
        scd_start_date = str(spark.sql("select cast(now() as string)").collect()[0][0])
    
    cols = dataFrame.columns
    df = dataFrame
    df = df.withColumn(_.BK, concat_ws('|', *(_.BusinessKeyCols.split(","))))
    df = df.withColumn("_RecordStart", expr(f"CAST('{scd_start_date}' AS TIMESTAMP)"))
    df = df.withColumn("_RecordEnd", 
                       expr("CAST(NULL AS TIMESTAMP)" if scd_end_date == "NULL" else f"CAST('{scd_end_date}' AS TIMESTAMP)"))
    df = df.withColumn("_RecordCurrent", expr("CAST(1 AS INT)"))
    df = df.withColumn("_RecordDeleted", expr("CAST(0 AS INT)"))
    df = df.withColumn("_DLCuratedZoneTimeStamp", expr("now()"))
    #df = df.withColumn(f"_Batch_SK", expr(f"DATE_FORMAT(_RecordStart, '{DATE_FORMAT}') || COALESCE(DATE_FORMAT(_RecordEnd, '{DATE_FORMAT}'), '{BATCH_END_CODE}') || _RecordCurrent"))
    #THIS IS LARGER THAN BIGINT 
    #df = df.withColumn("_Batch_SK", expr("CAST(_Batch_SK AS DECIMAL(25, 0))"))
    df = df.withColumn(_.SurrogateKey, md5(expr(f"concat({_.BK},'|',_RecordStart)")))
    df = df.select(_.SurrogateKey, *cols, *['_BusinessKey','_DLCuratedZoneTimeStamp','_RecordStart','_RecordEnd','_RecordDeleted','_RecordCurrent'])
    return df

def TableExists(tableFqn):
    return spark._jsparkSession.catalog().tableExists(tableFqn.split(".")[0], tableFqn.split(".")[1])

def CreateDeltaTable(dataFrame, targetTableFqn, dataLakePath):
    dataFrame.write \
    .format("delta") \
    .option("mergeSchema", "true") \
    .mode("overwrite") \
    .save(dataLakePath)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {targetTableFqn} USING DELTA LOCATION \'{dataLakePath}\'")

# COMMAND ----------

def SCDMerge(sourceDataFrame, scd_start_date = SCD_START_DATE, scd_end_date = SCD_END_DATE, _=_):
    
    targetTableFqn = f"{_.TargetTable}"
    
    print(f"SCD Merge Table {targetTableFqn} with Source Count {sourceDataFrame.count()}")
        
    if _.DateGranularity.lower() == 'day':
        if scd_start_date == 'now':
            scd_start_date = datetime.today().strftime('%Y-%m-%d')
        scd_expire_date = (datetime.strptime(scd_start_date, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
    else:
        if scd_start_date == 'now':
            scd_start_date = str(spark.sql("select cast(now() as string)").collect()[0][0])
        scd_expire_date = str(spark.sql("select cast(now() - INTERVAL 1 SECOND as string)").collect()[0][0])
        
    print("Add SCD Columns to Source Dataframe")
    
    sourceDataFrame = addSCDColumns(sourceDataFrame,scd_start_date, scd_end_date, _)

    # If Target table not exists, then create Target table with source data
    
    if (not(TableExists(targetTableFqn))):
        print(f"Table {targetTableFqn} not exists, Creat and load Table {targetTableFqn}")
        CreateDeltaTable(sourceDataFrame, targetTableFqn, _.DataLakePath)  
        return
    
    print("Checking new records")

    targetTable = spark.table(targetTableFqn)
    
    newRecords = sourceDataFrame.join(targetTable.where('_RecordCurrent = 1'), [f"{_.BK}"], 'leftanti')
    
    newCount = newRecords.count()

    if newCount > 0:
        print(f"Inserting {newCount} new record(s) into {targetTableFqn}")
        newRecords.select(targetTable.columns).write.insertInto(tableName=targetTableFqn)
        print(f"New records inserted")
    else:
        print("No new records")
        
    # Check deleted records if SNAP_SHOP is 1 (Full Load)
    
    if _.Snapshot == 1:
        
        print("Checking deleted(expired) records")
        
        expiredRecords = targetTable.where('_RecordCurrent = 1').join(sourceDataFrame, [f"{_.BK}"], 'leftanti')
        
        # Set _RecordDeleted = 1 for deleted records
        
        expiredRecords = expiredRecords.withColumn("_RecordDeleted", expr("CAST(1 AS INT)"))
        
        print(f"Number of deleted Records: {expiredRecords.count()}")
    
    # For records have BK exists in Target table, SCD Merge into Target table 
    
    print("Checking changed records")
    
    # Get Source data recordes which have changes compared with Target table
    
    _exclude = [_.SurrogateKey, _.BK, '_RecordStart', '_RecordEnd', '_RecordCurrent', "_Batch_SK","_DLCuratedZoneTimeStamp","_RecordDeleted"]
    _exclude = [c.lower() for c in _exclude]
    
    changeCondition = " OR ".join([f"ifnull(s.{c},'%^%') <> ifnull(t.{c},'%^%')" for c in targetTable.columns if c.lower() not in _exclude])
    
    changeRecords = sourceDataFrame.alias("s") \
        .join(targetTable.alias("t"), f"{_.BK}") \
        .where(f"t._RecordCurrent = 1 AND ({changeCondition})") 
    
    print(f"Number of SCD Changed Records: {changeRecords.count()}")
    
    if _.Snapshot == 1 and changeRecords.count() == 0 and expiredRecords.count() == 0:
        return
    
    if _.Snapshot == 0 and changeRecords.count() == 0:
        return

    # For SCD 2
    # 1. Change records will be inserted into Target table
    # 2. Corresponding Change records in Target table will be updated (SCD columns)
    
    stagedUpdates = changeRecords.selectExpr("NULL BK", "s.*") \
        .unionByName( \
          changeRecords.selectExpr(f"{_.BK} BK", "s.*") \
        )
    
    # Only consider deletion case in Full load mode
    if _.Snapshot == 1:
        stagedUpdates = stagedUpdates.unionByName(expiredRecords.selectExpr(f"{_.BK} BK", "*"))
        
    insertValues = {
        f"{_.SurrogateKey}": f"s.{_.SurrogateKey}", 
        f"{_.BK}": f"s.{_.BK}",
        "_RecordStart": "s._RecordStart",
        "_RecordEnd": "s._RecordEnd",
        "_RecordCurrent": "1",
        "_RecordDeleted": "0",
        "_DLCuratedZoneTimeStamp": expr("now()")
        #"_Batch_SK": expr(f"DATE_FORMAT(s._RecordStart, 'yyMMddHHmmss') || COALESCE(DATE_FORMAT(s._RecordEnd, '{DATE_FORMAT}'), '{BATCH_END_CODE}') || 1")
    }
    for c in [i for i in targetTable.columns if i not in _exclude]:
        insertValues[f"{c}"] = f"s.{c}"
    
    print(f"SCD Merge {targetTableFqn} Started")
    
    DeltaTable.forName(spark, targetTableFqn).alias("t").merge(stagedUpdates.alias("s"), f"t.{_.BK} = BK") \
        .whenMatchedUpdate(
          condition = f"t._RecordCurrent = 1 ",  #AND ({changeCondition})
          set = {
            "_RecordEnd": expr(f"'{scd_expire_date}'"),
            "_RecordCurrent": "0",
            "_RecordDeleted": "s._RecordDeleted"
            #"_Batch_SK": 
            #  expr(f"DATE_FORMAT(s._RecordStart, '{DATE_FORMAT}') || COALESCE(DATE_FORMAT('{scd_start_date}' - INTERVAL 1 SECOND, '{DATE_FORMAT}'), '{BATCH_END_CODE}') || 0") 
          }
        ) \
        .whenNotMatchedInsert(
          values = insertValues
        ).execute()
    
    print(f"SCD Merge {targetTableFqn} Completed")