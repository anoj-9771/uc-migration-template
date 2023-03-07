# Databricks notebook source
from delta.tables import *

# COMMAND ----------

# MAGIC %run ./common-spark

# COMMAND ----------

def PrefixColumn(columns, prefix=None):
  return ",".join([f"{prefix}.{k}" for k in columns.split(',')]) if(',' in columns) else f"{prefix}.{columns}"

# COMMAND ----------

def ConcatBusinessKey(columns, prefix=None):
  p = PrefixColumn(columns, prefix)
  return f"CONCAT({p})" if(',' in columns) else p

# COMMAND ----------

def BasicMerge(sourceDataFrame, targetTableFqn, businessKey=None):
  businessKey = spark.table(targetTableFqn).columns[0] if businessKey is None else businessKey
  s = ConcatBusinessKey(businessKey, "s")
  t = ConcatBusinessKey(businessKey, "t")
  
  df = DeltaTable.forName(spark, targetTableFqn).alias("t").merge(sourceDataFrame.alias("s"), f"{s} = {t}") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

# COMMAND ----------

def MergeSCDTable(sourceDataFrame, targetTableFqn, scdFromSource, BK, SK):
    targetTable = spark.table(targetTableFqn)
    # _exclude = {f"{_.SK}", f"{_.BK}", '_recordStart', '_recordEnd', '_Current', "_Batch_SK"}
    #Question - _recordCurrent, and _recordDeleted are not excluded, agree?    
    _exclude = {SK, BK, '_recordStart', '_recordEnd', '_recordCurrent', '_DLCuratedZoneTimeStamp'}
    # changeColumns = " OR ".join([f"s.{c} <=> t.{c}" for c in targetTable.columns if c not in _exclude])
    changeColumns = "!(" + " AND ".join([f"s.{c} <=> t.{c}" for c in targetTable.columns if c not in _exclude]) + ")"
    # bkList = "','".join([str(c[f"{_.BK}"]) for c in spark.table(targetTableFqn).select(f"{_.BK}").rdd.collect()])
    # newRecords = sourceDataFrame.where(f"{_.Name}_BK NOT IN ('{bkList}')")
    #Question - should we use recordCurrent or high date?
    # newRecords = sourceDataFrame.join(targetTable.where('_recordCurrent = 1'), [f"{_.BK}"], 'leftanti')
    # newCount = newRecords.count()

    # if newCount > 0:
    #     print(f"Inserting {newCount} new...")
    #     newRecords.write.insertInto(tableName=targetTableFqn)

    # sourceDataFrame = sourceDataFrame.where(f"{_.BK} IN ('{bkList}')")
    
    changeRecords = sourceDataFrame.alias("s") \
        .join(targetTable.alias("t"), BK) \
        .where(f"t._recordCurrent = 1 AND ({changeColumns})") 

    stagedUpdates = changeRecords.selectExpr("NULL BK", "s.*") \
        .unionByName( \
          sourceDataFrame.selectExpr("BK BK", "*") \
        )

    insertValues = {
        f"{_.SK}": f"{_.SK}", 
        # f"{_.BK}": f"COALESCE(s.BK, s.{_.BK})",
        f"{_.BK}": f"s.{_.BK}",
        "_DLCuratedZoneTimeStamp": "s._DLCuratedZoneTimeStamp",
        "_recordStart": "s._recordStart",
        "_recordEnd": "s._recordEnd",
        "_recordDeleted": "s._recordDeleted",
        # "_Batch_SK": expr(f"DATE_FORMAT(s._Created, 'yyMMddHHmmss') || COALESCE(DATE_FORMAT({DEFAULT_END_DATE}, '{DATE_FORMAT}'), '{BATCH_END_CODE}') || 1")
    }
    insertValues["_recordCurrent"]  = "s._recordCurrent" if scdFromSource else 1
       
    for c in [i for i in targetTable.columns if i not in _exclude]:
        insertValues[f"{c}"] = f"s.{c}"

    print("Merging...")
    DeltaTable.forName(spark, targetTableFqn).alias("t").merge(stagedUpdates.alias("s"), f"t.BK = BK") \
        .whenMatchedUpdate(
          condition = f"t._recordCurrent = 1 AND ({changeColumns})", 
          set = {
            "_recordEnd": expr(f"{DEFAULT_START_DATE} - INTERVAL 1 SECOND"),
            #Question
            "_recordCurrent": "0",
            # "_Batch_SK": expr(f"DATE_FORMAT(s._Created, '{DATE_FORMAT}') || COALESCE(DATE_FORMAT({DEFAULT_START_DATE} - INTERVAL 1 SECOND, '{DATE_FORMAT}'), '{BATCH_END_CODE}') || 0") 
          }
        ) \
        .whenNotMatchedInsert(
          values = insertValues
        ).execute()
    
    

# COMMAND ----------

def CreateOrMerge(sourceDataFrame, targetTableFqn, dataLakePath, businessKey=None, createTableConstraints = True):
    if (TableExists(targetTableFqn)):
        BasicMerge(sourceDataFrame, targetTableFqn, businessKey)
    else:
        #create delta table with not null constraints on buiness keys
        CreateDeltaTable(sourceDataFrame, targetTableFqn, dataLakePath, businessKey, createTableConstraints)
