# Databricks notebook source
# MAGIC %run ./common-class

# COMMAND ----------

# MAGIC %run ./common-scd

# COMMAND ----------

# MAGIC %run ./common-helpers

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *
from dateutil import parser
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
      self.SK = self.SK[0].lower() + self.SK[1:]        
    #   self.BK = f"{self.Name}_BK"      
      self.BK = "_BusinessKey"
    #   self.EntityName = f"{self.EntityType[0:1]}_{self.Name}"
      self.EntityName = f"{'dim' if self.EntityType == 'Dimension' else 'fact' if self.EntityType == 'Fact' else ''}{self.Name}"
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
TableName = _.EntityName 
SOURCE = DEFAULT_SOURCE

# COMMAND ----------

def GetTable(tableFqn):
    _.Tables.append(tableFqn) if tableFqn not in _.Tables else _.Tables
    return spark.table(tableFqn).cache()

# COMMAND ----------

def GetTableName(tableFqn):
    _.Tables.append(tableFqn) if tableFqn not in _.Tables else _.Tables
    return tableFqn

# COMMAND ----------

def GetCurrentTable(tableFqn):
    return GetTable(tableFqn).where(expr("_current = 1"))

# COMMAND ----------

def IsDimension():
  return _.EntityType == "Dimension"

# COMMAND ----------

def _WrapSystemColumns(dataFrame):  
    return _InjectSK(_AddSCD(dataFrame))

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
    if "_recordStart" in [c for c in cols]:
        df = df.withColumn("_recordStart", expr(f"COALESCE(_recordStart, CAST({DEFAULT_START_DATE} AS TIMESTAMP))"))
    else:  
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

def Save(sourceDataFrame,append=False):
    targetTableFqn = f"{_.Destination}"
    print(f"Saving {targetTableFqn}...")
    if (not(TableExists(targetTableFqn))):
        print(f"Creating {targetTableFqn}...")
        # Adjust _RecordStart date for first load
        sourceDataFrame = sourceDataFrame.withColumn("_recordStart", expr("CAST('1900-01-01' AS TIMESTAMP)"))
        sourceDataFrame = _WrapSystemColumns(sourceDataFrame) if sourceDataFrame is not None else None
        CreateDeltaTable(sourceDataFrame, targetTableFqn, _.DataLakePath)  
        EndNotebook(sourceDataFrame)
        return
    sourceDataFrame = _WrapSystemColumns(sourceDataFrame) if sourceDataFrame is not None else None
    if append:
        AppendDeltaTable(sourceDataFrame, targetTableFqn, _.DataLakePath)  
    else:    
        MergeSCDTable(sourceDataFrame, targetTableFqn,_.BK,_.SK)
    EndNotebook(sourceDataFrame)
    return 

# COMMAND ----------

def SaveDefaultSource(sourceDataFrame):
    targetTableFqn = f"{_.Destination}"
    print(f"Saving {targetTableFqn}...")
    if (not(TableExists(targetTableFqn))):
        print(f"Creating {targetTableFqn}...")
        # Adjust _RecordStart date for first load
        sourceDataFrame = sourceDataFrame.withColumn("_recordStart", expr("CAST('1900-01-01' AS TIMESTAMP)"))
        sourceDataFrame = _WrapSystemColumns(sourceDataFrame) if sourceDataFrame is not None else None

        if all(colName in sourceDataFrame.columns for colName in ["sourceValidFromDateTime", "sourceValidToDateTime"]):
            sourceDataFrame = sourceDataFrame.withColumn("sourceValidFromDateTime", when(col("sourceValidFromDateTime").isNull(), col("_recordStart")).otherwise(col("sourceValidFromDateTime"))) \
                                             .withColumn("sourceValidToDateTime", when(col("sourceValidToDateTime").isNull(), col("_recordEnd")).otherwise(col("sourceValidToDateTime"))) \
                                             .withColumn("sourceRecordCurrent", when(col("sourceRecordCurrent").isNull(), col("_recordCurrent")).otherwise(col("sourceRecordCurrent")))

        CreateDeltaTableR1W4(sourceDataFrame, targetTableFqn, _.DataLakePath)  
        EndNotebook(sourceDataFrame)
        return
    sourceDataFrame = _WrapSystemColumns(sourceDataFrame) if sourceDataFrame is not None else None
    MergeSCDTable(sourceDataFrame, targetTableFqn,_.BK,_.SK)
    EndNotebook(sourceDataFrame)
    return


# COMMAND ----------

####Mags#### to load/alter/modify drop columns without changing the existing content of the table
###################Drops and recreates entire table to retain onlyt the new schema ##Write Intensive ############
from functools import reduce
def filter_columns(df, columns_to_match=None):
    if columns_to_match:
        return [col for col in df.columns if not (col.endswith('SK') or col.startswith('_')) and col in columns_to_match]
    else:
        return [col for col in df.columns if not (col.endswith('SK') or col.startswith('_'))]

def get_column_types(df):
    return {col: dtype for col, dtype in df.dtypes}


def joinAdditionalColumns(oldDf, newDf, joinColumnsO, joinColumnsN, toAddColumns):
    ##should be a list
    if isinstance(joinColumnsO, str):
        joinColumnsO = [joinColumnsO]
    if isinstance(joinColumnsN, str):
        joinColumnsN = [joinColumnsN]
    if isinstance(toAddColumns, str):
        toAddColumns = [toAddColumns]
    newDf= newDf.select([col(c).alias(f"joinDf_{c}") for c in joinColumnsN] + [col(c) for c in toAddColumns])
    joinExpr = [col(c1) == col(f"joinDf_{c2}") for c1, c2 in zip(joinColumnsO, joinColumnsN)]   
    joinExpr = reduce(lambda x, y: x & y, joinExpr)
    print(joinExpr)   
    joinedDf = oldDf.join(newDf, on = joinExpr, how = "left")
    joinedDf = joinedDf.select([col for col in joinedDf.columns if not (col.startswith("joinDf_") and col in [f"joinDf_{c}" for c in joinColumnsN])])    
    return joinedDf


def arrangeColumns(pdf):
    columns = pdf.columns
    underscoreColumns = [col for col in columns if col.startswith('_')]
    underscoreNonColumns = [col for col in columns if not col.startswith('_')]
    finalColumns = underscoreNonColumns + underscoreColumns
    adf = pdf.select(*finalColumns)    
    return adf 

def isSchemaChanged(currentDataFrame):
    targetTableFqn = f"{_.Destination}"
    if (not(TableExists(targetTableFqn))):
        return False
    existingDataframe = spark.sql(f"select * from {_.Destination}")        
    columnsCurrent = filter_columns(currentDataFrame)    
    columnsExist = filter_columns(existingDataframe, columnsCurrent)   
    typesCurrent = get_column_types(currentDataFrame.select(columnsCurrent))    
    typesExist = get_column_types(existingDataframe.select(columnsExist))    
    if len(columnsCurrent) == len(columnsExist): # and all(typesCurrent[col] == typesExist[col] for col in columnsCurrent):
        return False
    else:
        return True


#######Call this in place of regular Save // if there is delta while conversion.. this has to handled seperately in main Transform logic notebooks.
def saveSchemaAndData(currentDataFrame, joinColumnsO, joinColumnsN):    
    if (not(isSchemaChanged(currentDataFrame))):
        save(currentDataFrame)
        return
    else:
        columnsCurrent = filter_columns(currentDataFrame)
        existingDataframe = spark.sql(f"select * from {_.Destination}")
        columnsExist = filter_columns(existingDataframe, columnsCurrent)
        addColumns = [col for col in columnsCurrent if col not in columnsExist]
        insertDF = joinAdditionalColumns(existingDataframe, currentDataFrame, joinColumnsO, joinColumnsN, addColumns)
        createDF = arrangeColumns(insertDF)
        createDF.createOrReplaceTempView("adfTemp")
        #CleanTable(_.Destination)
        spark.sql(f"CREATE OR REPLACE TABLE {_.Destination}  USING DELTA AS SELECT * from adfTemp")
        spark.sql(f"DROP VIEW IF EXISTS adfTemp")
        EndNotebook(createDF)
        return

# COMMAND ----------

def get_recent_cleansed_records(catalog, schema, table, business_date, target_date):
    cleansed_table_name = get_table_name(catalog,schema,table)
    target_table_name = f"{DEFAULT_TARGET}.{TableName}"
     # target_table_name = get_table_name(f"{DEFAULT_TARGET}","","{TableName}")
    try:
        latest_date = spark.sql(f"""select date_format(max({target_date}),'MM/dd/yyyy hh:mm:ss') from {target_table_name}""").first()[0]
        df = spark.sql(f"""select * from {cleansed_table_name} where {business_date} > '{latest_date}'""")
    except Exception as e:
        print(f"{target_table_name} table does not exist.This is first load")
        df = spark.sql(f"""select * from {cleansed_table_name}""")
    return df

# COMMAND ----------

def load_sourceValidFromTimeStamp(dataFrame,business_date=None):
    df = dataFrame
    try:
        # table_name = get_table_name(f"{DEFAULT_TARGET}","","{TableName}")
        table_name = f"{DEFAULT_TARGET}.{TableName}"
        spark.sql(f"DESCRIBE {table_name}")
        if business_date:
            df = df.withColumn("sourceValidFromTimestamp",col(business_date))
        else:
            df = df.withColumn("sourceValidFromTimestamp",expr(f"CAST({DEFAULT_START_DATE}) as timestamp)"))
    except:
        # First load
        df = df.withColumn("sourceValidFromTimestamp", expr("CAST('1900-01-01' AS TIMESTAMP)"))
    return df

