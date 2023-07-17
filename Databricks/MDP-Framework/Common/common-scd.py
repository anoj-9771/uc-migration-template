# Databricks notebook source
from delta.tables import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %run ./common-spark

# COMMAND ----------

def CoalesceColumn(columns):
    return ",".join([f"coalesce({k},'') {k}" for k in columns.split(',')]) if(',' in columns) else f"coalesce({columns},'') {columns}"

# COMMAND ----------

def PrefixColumn(columns, prefix=None):
    return ",'|',".join([f"{prefix}.{k}" for k in columns.split(',')]) if(',' in columns) else f"{prefix}.{columns}"

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

def BasicMergeNoUpdate(sourceDataFrame, targetTableFqn, businessKey=None):
  businessKey = spark.table(targetTableFqn).columns[0] if businessKey is None else businessKey
  s = ConcatBusinessKey(businessKey, "s")
  t = ConcatBusinessKey(businessKey, "t")
  
  df = DeltaTable.forName(spark, targetTableFqn).alias("t").merge(sourceDataFrame.alias("s"), f"{s} = {t}") \
    .whenNotMatchedInsertAll() \
    .execute()

# COMMAND ----------

def MergeSCDTable(sourceDataFrame, targetTableFqn, BK, SK):
    targetTable = spark.table(targetTableFqn)
    # _exclude = {f"{_.SK}", f"{_.BK}", '_recordStart', '_recordEnd', '_Current', "_Batch_SK"}
    #Question - _recordCurrent, and _recordDeleted are not excluded, agree?    
    _exclude = {SK, BK, '_recordStart', '_recordEnd', '_recordCurrent', '_DLCuratedZoneTimeStamp'}
    # changeColumns = " OR ".join([f"s.{c} <=> t.{c}" for c in targetTable.columns if c not in _exclude])
    changeColumns = "!(" + " AND ".join([f"s.{c} <=> t.{c}" for c in targetTable.columns if c not in _exclude]) + ")"
    # bkList = "','".join([str(c[f"{_.BK}"]) for c in spark.table(targetTableFqn).select(f"{_.BK}").collect()])
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
          sourceDataFrame.selectExpr(f"{BK} BK", "*") \
        )
    sourceSCDColumnsExist = all(colName in sourceDataFrame.columns for colName in ["sourceValidFromTimestamp", "sourceValidToTimestamp"])
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
    insertValues["_recordCurrent"]  = "1"
       
    for c in [i for i in targetTable.columns if i not in _exclude]:
        insertValues[f"{c}"] = f"s.{c}"

    if sourceSCDColumnsExist:
        print("Merging...")
        DeltaTable.forName(spark, targetTableFqn).alias("t").merge(stagedUpdates.alias("s"), f"t.{BK} = s.BK") \
            .whenMatchedUpdate(
                condition = f"t._recordCurrent = 1 AND ({changeColumns}) AND s.sourceBusinessKey = s.BK", 
                set = {
                "_recordEnd": expr(f"{DEFAULT_START_DATE} - INTERVAL 1 SECOND"),
                "_recordCurrent": "0",
                "sourceRecordCurrent": "0",
                "sourceValidToTimestamp":  expr("s.sourceValidFromTimestamp - INTERVAL 1 SECOND"),
                }
            ) \
            .whenMatchedUpdate(
                condition = f"t._recordCurrent = 1 AND ({changeColumns})", 
                set = {
                "_recordEnd": expr(f"{DEFAULT_START_DATE} - INTERVAL 1 SECOND"),
                "_recordCurrent": "0",
                }
            ) \
            .whenNotMatchedInsert(
                values = insertValues
            ).execute()
    else:
        print("Merging...")
        DeltaTable.forName(spark, targetTableFqn).alias("t").merge(stagedUpdates.alias("s"), f"t.{BK} = s.BK") \
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

##Mags --Both scenario of _recordDelete on Upstream is handled in the merge
def mergeCDFTableSCD2(sourceDataFrame, targetTableFqn, BK, SK):
    updateDF = ((sourceDataFrame.filter(col("_change_type") == lit("update_postimage")))
                              .withColumn("_change_type", lit("update")))

    if updateDF is not None:
        sourceDF = sourceDataFrame.unionByName(updateDF)
    else:
        sourceDF = sourceDataFrame


    sourceMergeDF = (sourceDF.withColumn("_mergeBK", 
                                when((col("_change_type") == lit("update")) | (col("_change_type") == lit("delete")) 
                                     ,col(BK)).otherwise (lit(None))))
    

    selectColumns = [col for col in sourceMergeDF.columns if not (col.endswith('SK') or (col.startswith('_') and col not in [BK, '_recordDeleted']))]
    

    print("Merging CDF")
    (DeltaTable.forName(spark, targetTableFqn).alias("t")
                        .merge(sourceMergeDF.alias("s")
                        ,f"t.{BK} = s._mergeBK") 
        .whenMatchedUpdate(
          condition = "s._change_type = 'update' and t._recordCurrent = 1", 
          set = {"_recordEnd": expr(f"{DEFAULT_START_DATE} - INTERVAL 1 SECOND"),
                 "_recordCurrent": "0",
                 "_recordDeleted": "s._recordDeleted",
                }
        )
        .whenMatchedUpdate(
          condition = "s._change_type = 'delete' and t._recordCurrent = 1", 
          set = {"_recordEnd": expr(f"{DEFAULT_START_DATE} - INTERVAL 1 SECOND"),
                 "_recordDeleted": "1",
                 "_recordCurrent": "0",
                }
        ) 
        .whenNotMatchedInsert(
          condition = "s._change_type in ('update_postimage', 'insert') and s._recordDeleted != 1", 
          values = {**{col: f"s.{col}" for col in sourceMergeDF.columns if col in selectColumns}, 
                       f"{SK}": md5(expr(f"concat({_.BK},'|',{DEFAULT_START_DATE})")),
                       "_DLCuratedZoneTimeStamp": expr("now()"),
                       "_recordStart": "current_timestamp()",
                       "_recordEnd": "to_timestamp('9999-12-31 00:00:00')",
                       "_recordCurrent": "1",
                       "_recordDeleted": "0"
                    }
        ).execute())

# COMMAND ----------

def mergeCDFSCD1(sourceDataFrame, targetTableFqn, BK, SK):
    sourceDataFrame = sourceDataFrame.filter(col("_change_type") != lit("update_preimage"))
                           

    sourceMergeDF = (sourceDF.withColumn("_mergeBK", 
                                when((col("_change_type") == lit("update_postimage")) | (col("_change_type") == lit("delete")) 
                                     ,col(BK)).otherwise (lit(None))))
    

    selectColumns = [col for col in sourceMergeDF.columns if not (col.endswith('SK') or (col in ["_mergeBK"]))]
    updateExprs = {col: "s." + col for col in selectColumns}

    print("Merging CDF")
    (DeltaTable.forName(spark, targetTableFqn).alias("t")
                        .merge(sourceMergeDF.alias("s")
                        ,f"t.{BK} = s._mergeBK") 
        .whenMatchedUpdate(
          condition = "s._change_type = 'update_postimage' and t._recordCurrent = 1", 
          set = (updateExprs)
        )
        .whenMatchedUpdate(
          condition = "s._change_type = 'delete' and t._recordCurrent = 1", 
          set = {"_recordEnd": expr(f"{DEFAULT_START_DATE} - INTERVAL 1 SECOND"),
                 "_recordDeleted": "1",
                 "_recordCurrent": "0",
                }
        ) 
        .whenNotMatchedInsert(
          condition = "s._change_type ='insert' and s._recordDeleted != 1", 
          values = {**{col: f"s.{col}" for col in sourceMergeDF.columns if col in selectColumns}, 
                       f"{SK}": md5(expr(f"concat({_.BK},'|',{DEFAULT_START_DATE})")),
                       "_DLCuratedZoneTimeStamp": expr("now()"),
                       "_recordStart": "current_timestamp()",
                       "_recordEnd": "to_timestamp('9999-12-31 00:00:00')",
                       "_recordCurrent": "1",
                       "_recordDeleted": "0"
                    }
        ).execute())

# COMMAND ----------

def AppendCDFTable(sourceDataFrame, targetTableFqn, BK, SK):
    sourceDataFrame = sourceDataFrame.filter(col("_change_type") == lit("insert"))
    selectColumns = [col for col in sourceDataFrame.columns if not (col.endswith('SK'))]
    sourceDataFrame = (sourceDataFrame.withColumns(f"{SK}", md5(expr(f"concat({_.BK},'|',{DEFAULT_START_DATE})")))
                                      .withColumns("_DLCuratedZoneTimeStamp", expr("now()"))
                                      .withColumns("_recordStart", expr("now()"))
                                      .withColumns("_recordEnd", expr("to_timestamp('9999-12-31 00:00:00')"))
                                      .withColumns("_recordCurrent", lit("1"))
                                      .withColumns("_recordDeleted", lit("0"))
                      )
    print("Appending CDF")
    AppendDeltaTable(sourceDataFrame, targetTableFqn, _.DataLakePath)

# COMMAND ----------

def CreateOrMerge(sourceDataFrame, targetTableFqn, dataLakePath, businessKey=None, createTableConstraints = True, mergeWithUpdate = True):
    if (TableExists(targetTableFqn)):
        if mergeWithUpdate: 
            BasicMerge(sourceDataFrame, targetTableFqn, businessKey)
        else:
            BasicMergeNoUpdate(sourceDataFrame, targetTableFqn, businessKey)
    else:
        #create delta table with not null constraints on buiness keys
        CreateDeltaTable(sourceDataFrame, targetTableFqn, dataLakePath, businessKey, createTableConstraints)
