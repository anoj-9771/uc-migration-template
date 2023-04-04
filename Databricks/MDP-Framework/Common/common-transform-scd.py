# Databricks notebook source
from pyspark.sql.functions import *
from delta.tables import *

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

# COMMAND ----------

class BlankClass(object):
    pass

class CuratedTransform( BlankClass ):
    
    Counts = BlankClass()
        
    def __init__(self, Entity_Type, Entity_Name, BUSINESS_KEY_COLS, TARGET_LAYER):
        self.EntityType = f"{Entity_Type}"
        self.EntityName = f"{Entity_Name}"
        self.BK = "_BusinessKey"
        self.BusinessKeyCols = f"{BUSINESS_KEY_COLS}"
        self.SurrogateKey = f"{self.EntityName}SK"
        self.TargetTable = f"{TARGET_LAYER}.dim{self.EntityName}"
        self.DataLakePath = f"/mnt/datalake-{TARGET_LAYER}/dim{self.EntityName}".lower() #/mnt/datalake-curated/dimBusinessPartner

# COMMAND ----------

# Create CuratedTransform object

_ = CuratedTransform(Entity_Type, Entity_Name, BUSINESS_KEY_COLS, TARGET_LAYER)

# COMMAND ----------

def addSCDColumns(dataFrame, scd_start_date = SCD_START_DATE, scd_end_date = SCD_END_DATE, _=_):
    
    if scd_start_date == 'now':
        scd_start_date = str(spark.sql("select cast(now() as string)").collect()[0][0])
    
    df = dataFrame
    df = df.withColumn(_.BK, concat_ws('|', *(_.BusinessKeyCols.split(","))))
    df = df.withColumn("_RecordStart", expr(f"CAST('{scd_start_date}' AS TIMESTAMP)"))
    df = df.withColumn("_RecordEnd", 
                       expr("CAST(NULL AS TIMESTAMP)" if scd_end_date == "NULL" else f"CAST('{scd_end_date}' AS TIMESTAMP)"))
    df = df.withColumn("_RecordCurrent", expr("CAST(1 AS INT)"))
    df = df.withColumn(f"_Batch_SK", expr(f"DATE_FORMAT(_RecordStart, '{DATE_FORMAT}') || COALESCE(DATE_FORMAT(_RecordEnd, '{DATE_FORMAT}'), '{BATCH_END_CODE}') || _RecordCurrent"))
    #THIS IS LARGER THAN BIGINT 
    df = df.withColumn("_Batch_SK", expr("CAST(_Batch_SK AS DECIMAL(25, 0))"))
    cols = df.columns
    df = df.withColumn(_.SurrogateKey, md5(expr(f"concat({_.BK},'|',_RecordStart)")))
    df = df.select(_.SurrogateKey, *cols)
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
    
    print(f"SCD Merge Table {targetTableFqn}")
    
    if scd_start_date == 'now':
        scd_start_date = str(spark.sql("select cast(now() as string)").collect()[0][0])
    
    sourceDataFrame = addSCDColumns(sourceDataFrame,scd_start_date, scd_end_date, _)

    # If Target table not exists, then create Target table with source data
    
    if (not(TableExists(targetTableFqn))):
        print(f"Table {targetTableFqn} not exists, Creat and load Table {targetTableFqn}")
        CreateDeltaTable(sourceDataFrame, targetTableFqn, _.DataLakePath)  
        return

    targetTable = spark.table(targetTableFqn).cache()
    
    targetTable_BK_List = "','".join([\
                         str(c[f"{_.BK}"]) for c in \
                         targetTable.select(f"{_.BK}").collect()\
                        ])

    # For new records(new BK), insert into Target table
    
    newRecords = sourceDataFrame.where(f"{_.BK} NOT IN ('{targetTable_BK_List}')")
    newCount = newRecords.count()

    if newCount > 0:
        print(f"Inserting {newCount} new record(s) into {targetTableFqn}")
        newRecords.write.insertInto(tableName=targetTableFqn)
        
    # For records have BK exists in Target table, SCD Merge into Target table 
    
    sourceDataFrame = sourceDataFrame.where(f"{_.BK} IN ('{targetTable_BK_List}')")
    
    # Get Source data recordes which have changes compared with Target table
    
    _exclude = {_.SurrogateKey, _.BK, '_RecordStart', '_RecordEnd', '_RecordCurrent', "_Batch_SK"}
    
    changeCondition = " OR ".join([f"s.{c} <> t.{c}" for c in targetTable.columns if c not in _exclude])
    
    changeRecords = sourceDataFrame.alias("s") \
        .join(targetTable.alias("t"), f"{_.BK}") \
        .where(f"t._RecordCurrent = 1 AND ({changeCondition})") 
    
    print(f"Number of SCD Changed Records: {changeRecords.count()}")
    
    if changeRecords.count() == 0:
        return

    # For SCD 2
    # 1. Change records will be inserted into Target table
    # 2. Corresponding Change records in Target table will be updated (SCD columns)
    
    stagedUpdates = changeRecords.selectExpr("NULL BK", "s.*") \
        .unionByName( \
          sourceDataFrame.selectExpr(f"{_.BK} BK", "*") \
        )
    
    insertValues = {
        f"{_.SurrogateKey}": f"s.{_.SurrogateKey}", 
        f"{_.BK}": f"s.{_.BK}",
        "_RecordStart": "s._RecordStart",
        "_RecordEnd": "s._RecordEnd",
        "_RecordCurrent": "1",
        "_Batch_SK": expr(f"DATE_FORMAT(s._RecordStart, 'yyMMddHHmmss') || COALESCE(DATE_FORMAT(s._RecordEnd, '{DATE_FORMAT}'), '{BATCH_END_CODE}') || 1")
    }
    for c in [i for i in targetTable.columns if i not in _exclude]:
        insertValues[f"{c}"] = f"s.{c}"
    
    print(f"SCD Merge {targetTableFqn} Started")
    
    DeltaTable.forName(spark, targetTableFqn).alias("t").merge(stagedUpdates.alias("s"), f"t.{_.BK} = BK") \
        .whenMatchedUpdate(
          condition = f"t._RecordCurrent = 1 AND ({changeCondition})", 
          set = {
            "_RecordEnd": expr(f"'{scd_start_date}' - INTERVAL 1 SECOND"),
            "_RecordCurrent": "0",
            "_Batch_SK": 
              expr(f"DATE_FORMAT(s._RecordStart, '{DATE_FORMAT}') || COALESCE(DATE_FORMAT('{scd_start_date}' - INTERVAL 1 SECOND, '{DATE_FORMAT}'), '{BATCH_END_CODE}') || 0") 
          }
        ) \
        .whenNotMatchedInsert(
          values = insertValues
        ).execute()
    
    print(f"SCD Merge {targetTableFqn} Completed")
