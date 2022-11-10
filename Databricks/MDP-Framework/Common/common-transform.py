# Databricks notebook source
# MAGIC %run ./common-class

# COMMAND ----------

# MAGIC %run ./common-scd

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *
from datetime import *
import pytz

# COMMAND ----------

DEFAULT_SOURCE = "cleansed"
DEFAULT_TARGET = "curated"
DEFAULT_START_DATE = "NOW()"
DEFAULT_END_DATE = "9999-12-31"
BATCH_END_CODE = "000000000000"
DATE_FORMAT = "yyMMddHHmmss"
DEBUG = 0

# COMMAND ----------

#OVERRIDE DEFAULTS BY PARAMETERS
#print(f"Defaults: Source={DEFAULT_SOURCE}, Target={DEFAULT_TARGET}, Start={DEFAULT_START_DATE}, End={DEFAULT_END_DATE}")
try:
    DEFAULT_SOURCE = dbutils.widgets.get("default_source")
    DEFAULT_TARGET = dbutils.widgets.get("default_target")
    DEFAULT_START_DATE = dbutils.widgets.get("default_start_date")
    DEFAULT_END_DATE =  dbutils.widgets.get("default_end_date")
    print(f"Overwritten defaults: Source={DEFAULT_SOURCE}, Target={DEFAULT_TARGET}, Start={DEFAULT_START_DATE}, End={DEFAULT_END_DATE}")
except:
    pass

# COMMAND ----------

class CuratedTransform( BlankClass ):
  Counts = BlankClass()
  
  def __init__(self):
      list = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split("/")
      count=len(list)
      self.EntityType = list[count-2]
      self.Name = list[count-1]
      self.SK = f"{self.Name}SK"
      self.SK = self.Name[0].lower() + self.Name[1:]        
    #   self.BK = f"{self.Name}_BK"      
      self.BK = "_BusinessKey"
    #   self.EntityName = f"{self.EntityType[0:1]}_{self.Name}"
      self.EntityName = f"{'dim' if self.EntityType == 'Dimension' else 'fact'}{self.Name}"
      self.Destination = f"{DEFAULT_TARGET}.{self.EntityName}"
      self.DataLakePath = f"/mnt/datalake-{DEFAULT_TARGET}/{self.EntityType}/{self.EntityName}"
      self.Tables = []
      #self.Joins = []
  
  def _NotebookEnd(self):
    r = json.dumps(
      {
        "Counts" : self.Counts.__dict__,
        "CuratedTransform" : self.__dict__
      })
        
    print(r) if DEBUG == 1 else dbutils.notebook.exit(r)

# COMMAND ----------

_ = CuratedTransform()

BK = _.BK
SOURCE = DEFAULT_SOURCE

# COMMAND ----------

def GetTable(tableFqn):
    _.Tables.append(tableFqn) if tableFqn not in _.Tables else _.Tables
    return spark.table(tableFqn).cache()

# COMMAND ----------

def GetCurrentTable(tableFqn):
    return GetTable(tableFqn).where(expr("_current = 1"))

# COMMAND ----------

def IsDimension():
  return _.EntityType == "Dimension"

# COMMAND ----------

def _WrapSystemColumns(dataFrame):
#   return _AddSCD(_InjectSK(dataFrame)) #if IsDimension() else _InjectSK(dataFrame) 
  return _InjectSK(_AddSCD(dataFrame)) #if IsDimension() else _InjectSK(dataFrame) 

# COMMAND ----------

def SaveDelta(dataFrame):
    targetTableFqn = _.Destination
    dataLakePath = _.DataLakePath
    CreateDeltaTable(dataFrame, targetTableFqn, dataLakePath)

# COMMAND ----------

def Overwrite(dataFrame):
    CleanTable(_.Destination)
    CreateDeltaTable(dataFrame, _.Destination, _.DataLakePath)
    EndNotebook(dataFrame)

# COMMAND ----------

def CleanSelf():
    CleanTable(_.Destination)
    
def DisplaySelf():
    display(GetSelf())
    
def GetSelf():
    return spark.table(_.Destination)

# COMMAND ----------

def _ValidateBK(sourceDataFrame):
    print(f"Validating {_.Name} {_.BK}...")
    # dupesDf = sourceDataFrame.groupBy(f"{_.Name}_BK").count() \
    dupesDf = sourceDataFrame.groupBy(f"{_.BK}").count() \
      .withColumnRenamed("count", "n") \
      .where("n > 1") 
    
    if (dupesDf.count() > 1):
        print(f"WARNING! THERE ARE DUPES ON {_.Name} {_.BK}!")
        dupesDf.show()

# COMMAND ----------

def Update(sourceDataFrame):
    targetTableFqn = f"{_.Destination}"
    dt = DeltaTable.forName(spark, targetTableFqn)
    # _exclude = {f"{_.SK}", f"{_.BK}", '_Created', '_Ended', '_Current'}
    _exclude = {f"{_.SK}", f"{_.BK}", '_recordStart', '_recordEnd'}
    targetTable = spark.table(targetTableFqn)
    updates = {
    }
    for c in [i for i in targetTable.columns if i not in _exclude]:
        updates[f"{c}"] = f"s.{c}"
    
    dt.alias("t").merge(
        sourceDataFrame.alias("s")
        ,f"t.{_.BK} = s.{_.BK}"
        ) \
        .whenMatchedUpdate(
        set = updates
        ).execute()
    EndNotebook(sourceDataFrame)

# COMMAND ----------

def EndNotebook(df):
    _.Counts.SpotCount = df.count() if df is not None else 0
    _.Counts.InsertCount = -1#GetRandomNumber() #TODO
    _.Counts.UpdateCount = -1#GetRandomNumber() #TODO
    _.Counts.CuratedCount = GetTableRowCount(_.Destination)
    
    _.Columns = [c for c in df.columns] if df is not None else None
    _._NotebookEnd()

# COMMAND ----------

def UnioniseQuery(query):
    df = None
    for i in {"", "nz"}:
        id = 1 if i=="" else 2
        c = "" if i=="" else "_nz"
        localDf = spark.sql(query.replace(f"{DEFAULT_SOURCE}.dbo", f"{DEFAULT_SOURCE}.dbo{c}")).withColumn("Organisation", lit(id))

        if df == None:
            df = localDf
        else:
            df = df.unionAll(localDf)
    return df

# COMMAND ----------

def _InjectSK(dataFrame):
    skName = _.SK
    df = dataFrame
    #df = df.withColumn(skName, expr(f"XXHASH64(CONCAT(date_format(now(), '{DATE_FORMAT}'), '-', {_.Name}_BK), 512)"))
    # df = df.withColumn(skName, expr(f"CONCAT(date_format(now(), '{DATE_FORMAT}'), '-', {_.BK})"))
    df = df.withColumn(skName, md5(expr(f"concat({_.BK},'|',_recordStart)")))
    #df = df.withColumn(skName, expr(f"XXHASH64({_.Name}_BK, 512) || DATE_FORMAT(NOW(), '{DATE_FORMAT}')"))
    #df = df.withColumn(skName, expr(f"CAST((XXHASH64({_.Name}_BK, 512) || DATE_FORMAT(NOW(), '{DATE_FORMAT}')) AS BIGINT)"))
    #df = df.withColumn(skName, expr(f"CAST(LEFT({_.Name}_SK, 32) AS BIGINT)"))
    columns = [c for c in dataFrame.columns]
    columns.insert(0, skName)
    df = df.select(columns)

    return df

# COMMAND ----------

def _AddSCD(dataFrame):
    cols = dataFrame.columns
    df = dataFrame
    
    #Move BK to the end
    if _.BK in cols:
        cols.remove(_.BK)
        cols.append(_.BK)
    df = df.select(cols)

    df = df.withColumn("_DLCuratedZoneTimeStamp", expr("now()"))
    df = df.withColumn("_recordStart", expr(f"CAST({DEFAULT_START_DATE} AS TIMESTAMP)"))
    # df = df.withColumn("_recordEnd", expr("CAST(NULL AS TIMESTAMP)" if DEFAULT_END_DATE == "NULL" else f"CAST('{DEFAULT_END_DATE}' AS TIMESTAMP) + INTERVAL 1 DAY - INTERVAL 1 SECOND"))
    df = df.withColumn("_recordEnd", 
                    expr("CAST(NULL AS TIMESTAMP)" if DEFAULT_END_DATE == "NULL" else f"CAST('{DEFAULT_END_DATE}' AS TIMESTAMP)"))
    
    if "_recordcurrent" in [c.lower() for c in cols]:
        df = df.withColumn("_recordCurrent", expr("COALESCE(_recordCurrent, CAST(1 AS INT))"))
    else:    
        df = df.withColumn("_recordCurrent", expr("CAST(1 AS INT)"))

    if "_recorddeleted" in [c.lower() for c in cols]:
        df = df.withColumn("_recordDeleted", expr("COALESCE(_recordDeleted, CAST(0 AS INT))"))
    else:    
        df = df.withColumn("_recordDeleted", expr("CAST(0 AS INT)"))

    # cols = [c for c in cols if c.lower() not in ["_recordcurrent","_recorddeleted"]]
    
    # df = df.withColumn("_Batch_SK", expr(f"DATE_FORMAT(_Created, '{DATE_FORMAT}') || COALESCE(DATE_FORMAT(_Ended, '{DATE_FORMAT}'), '{BATCH_END_CODE}') || _Current"))
    #THIS IS LARGER THAN BIGINT 
    # df = df.withColumn("_Batch_SK", expr("CAST(_Batch_SK AS DECIMAL(25, 0))"))

    return df

# COMMAND ----------

def Save(sourceDataFrame):
    targetTableFqn = f"{_.Destination}"
    print(f"Saving {targetTableFqn}...")
    sourceDataFrame = _WrapSystemColumns(sourceDataFrame) if sourceDataFrame is not None else None

    if (not(TableExists(targetTableFqn))):
        print(f"Creating {targetTableFqn}...")
        CreateDeltaTable(sourceDataFrame, targetTableFqn, _.DataLakePath)  
        EndNotebook(sourceDataFrame)
        return

    targetTable = spark.table(targetTableFqn)
    # _exclude = {f"{_.SK}", f"{_.BK}", '_recordStart', '_recordEnd', '_Current', "_Batch_SK"}
    #Question - _recordCurrent, and _recordDeleted are not excluded, agree?    
    _exclude = {f"{_.SK}", f"{_.BK}", '_recordStart', '_recordEnd', '_DLCuratedZoneTimeStamp'}
    changeColumns = " OR ".join([f"s.{c} <=> t.{c}" for c in targetTable.columns if c not in _exclude])
    bkList = "','".join([str(c[f"{_.BK}"]) for c in spark.table(targetTableFqn).select(f"{_.BK}").rdd.collect()])
    # newRecords = sourceDataFrame.where(f"{_.Name}_BK NOT IN ('{bkList}')")
    #Question - should we use recordCurrent or high date?
    newRecords = sourceDataFrame.join(targetTable.where('_recordCurrent = 1'), [f"{_.BK}"], 'leftanti')
    newCount = newRecords.count()

    if newCount > 0:
        print(f"Inserting {newCount} new...")
        newRecords.write.insertInto(tableName=targetTableFqn)

    sourceDataFrame = sourceDataFrame.where(f"{_.BK} IN ('{bkList}')")
    
    changeRecords = sourceDataFrame.alias("s") \
        .join(targetTable.alias("t"), f"{_.BK}") \
        .where(f"t._recordCurrent = 1 AND ({changeColumns})") 

    stagedUpdates = changeRecords.selectExpr("NULL BK", "s.*") \
        .union( \
          sourceDataFrame.selectExpr(f"{_.BK} BK", "*") \
        )

    insertValues = {
        f"{_.SK}": f"{_.SK}", 
        f"{_.BK}": f"COALESCE(s.BK, s.{_.BK})",
        "_DLCuratedZoneTimeStamp": "s._DLCuratedZoneTimeStamp",
        "_recordStart": "s._recordStart",
        "_recordEnd": "s._recordEnd",
        #Question
        # "_recordCurrent": "1",
        "_recordCurrent": "s._recordCurrent",
        "_recordDeleted": "s._recordDeleted",
        # "_Batch_SK": expr(f"DATE_FORMAT(s._Created, 'yyMMddHHmmss') || COALESCE(DATE_FORMAT({DEFAULT_END_DATE}, '{DATE_FORMAT}'), '{BATCH_END_CODE}') || 1")
    }
    for c in [i for i in targetTable.columns if i not in _exclude]:
        insertValues[f"{c}"] = f"s.{c}"

    print("Merging...")
    DeltaTable.forName(spark, targetTableFqn).alias("t").merge(stagedUpdates.alias("s"), f"t.{_.BK} = BK") \
        .whenMatchedUpdate(
          condition = f"t._recordCurrent = 1 AND ({changeColumns})", 
          set = {
            "_recordEnd": expr(f"{DEFAULT_START_DATE} - INTERVAL 1 SECOND"),
            #Question
            # "_recordCurrent": "0",
            # "_Batch_SK": expr(f"DATE_FORMAT(s._Created, '{DATE_FORMAT}') || COALESCE(DATE_FORMAT({DEFAULT_START_DATE} - INTERVAL 1 SECOND, '{DATE_FORMAT}'), '{BATCH_END_CODE}') || 0") 
          }
        ) \
        .whenNotMatchedInsert(
          values = insertValues
        ).execute()
    
    
    EndNotebook(sourceDataFrame)
    return 
