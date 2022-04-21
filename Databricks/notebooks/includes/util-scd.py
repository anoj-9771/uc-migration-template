# Databricks notebook source
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import *
import math
import pytz

# COMMAND ----------

# MAGIC %run ./global-variables-python

# COMMAND ----------

spark.conf.set("spark.sql.crossJoin.enabled",True)
CREATE_SQL_TARGET = False
TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
CURRET_TIMEZONE = "Australia/NSW"

# COMMAND ----------

def LogEtl(text):
  text = "{} - {}".format(datetime.now(pytz.timezone(CURRET_TIMEZONE)).strftime(TIME_FORMAT), text)
  print(text)

# COMMAND ----------

def SqlTableExists(tableName, schema):
  #CHECK SQL DATABASE
  query = "(SELECT COUNT(*) [table_count] from sys.tables t \
  where schema_name(t.schema_id) = '{schema}' \
  AND t.name = '{table}' \
  ) T".format(schema=schema,table=tableName)
  jdbcUrl = AzSqlGetDBConnectionURL()
  df = spark.read.jdbc(url=jdbcUrl, table=query)
  return True if df.rdd.collect()[0].table_count == 1 else False

# COMMAND ----------

def SqlTableExistsFqn(tableFqn):
  split = tableFqn.split(".")
  if(len(split) != 2):
     raise Exception("Must be in <databse>.<table> format!")
  return SqlTableExists(split[1], split[0])

# COMMAND ----------

def DataFrameToSql(dataFrame, destinationSqlTableName, mode="overwrite"):
  jdbcUrl = AzSqlGetDBConnectionURL()
  dataFrame.write.mode(mode).jdbc(jdbcUrl, destinationSqlTableName)

# COMMAND ----------

def CleanStageTables(dlStageTableFqn, sqlStageTableFqn):
  LogEtl(f"Cleaning stage tables [{dlStageTableFqn}] [{sqlStageTableFqn}].")
  _ExecuteSqlQuery(f"DROP TABLE {sqlStageTableFqn}")
  spark.sql(f"DROP TABLE IF EXISTS {dlStageTableFqn}")

# COMMAND ----------

def BusinessKeyListToColumn(businessKey, method="CONCAT"):
  businessKeyList = businessKey.split(",")
  bkList = "{}".format(", ".join([f"{col}" for col in businessKeyList]))
  return bkList if len(businessKeyList) == 1 else f"{method}({bkList})"

# COMMAND ----------

###########################################################################################################################
# Function: _GenerateScdMergeQuery
#  GENERATES A MERGE STATEMENT PROVIDED SOURCE AND TARGET PARAMETERS
#  OPERATES IN EITHER SPARK SQL OR SQL SERVER BY SETTING targetSqlServer FLAG
# Parameters: 
#  dataframe - DataFrame - Dataframe to generate from
#  businessKey - String - Business Key to set on merge sql
#  stageTableFqn - String - Fully qualified stage table name
#  targetTableFqn - String - Fully qualified target table name
#  targetSqlServer - String - Fully qualified SQL target table name
#  mode - String = insert/delete
# Returns:
#  String - SQL merge statement in either Spark or SQL Server format
#############################################################################################################################
def _GenerateScdMergeQuery(dataframe : object, businessKey : str, stageTableFqn : str, targetTableFqn : str, targetSqlServer : bool = False, mode : str = "insert"):
  columns = dataframe.columns
  brace = "\"" if targetSqlServer else "`"
  skColumn = columns[0]
  exceptionList = [businessKey, COL_RECORD_VERSION, COL_RECORD_START, COL_RECORD_END, COL_RECORD_CURRENT, COL_RECORD_DELETED, COL_DL_RAW_LOAD, COL_DL_TRUSTED_LOAD, COL_DL_CURATED_LOAD, skColumn]
  changeCondition = " OR ".join(["COALESCE(tbl_src.{b}{col}{b}, '') <> COALESCE(tbl_tgt.{b}{col}{b}, '')".format(col=col, b=brace) for col in columns if col not in exceptionList]) 
  changeConditionStg = changeCondition.replace("tgt", "stg").replace("src", "mn")
  businessKeyList = businessKey.split(",")
  srcBks = "{}".format(", ".join(["tbl_src.{b}{col}{b}".format(col=col, b=brace) for col in businessKeyList]))
  srcBks = srcBks if len(businessKeyList) == 1 else "CONCAT({})".format(srcBks)
  tgtBks = srcBks.replace("tbl_src", "tbl_tgt")
  mnBks = srcBks.replace("src", "mn")
  insertColumns = ", ".join(["{b}{col}{b}".format(col=col, b=brace) for col in _AddScdColumns(dataframe).columns]) 
  timestampMethod = "tbl_stg.{b}{c}{b}".format(c=COL_DL_CURATED_LOAD, b=brace) if targetSqlServer else "from_utc_timestamp(CURRENT_TIMESTAMP(), 'Australia/Sydney')" 
  startDateColumn = "tbl_stg.{b}{c}{b}".format(c=COL_RECORD_START, b=brace) if COL_RECORD_START in dataframe.columns else timestampMethod
  recordEnd = "DATEADD(SECOND, -1, {})".format(startDateColumn) if targetSqlServer else "({} - INTERVAL 1 seconds)".format(startDateColumn)
  defaultRecordEnd = "'9999-12-31 00:00:00'"
  defaultNewValues = f", {timestampMethod}, {startDateColumn}, {defaultRecordEnd}, {COL_RECORD_DELETED}, {COL_RECORD_CURRENT}"
  columnsWithScdValues = columns
  insertColumnValues = ", ".join(["tbl_stg.{b}{col}{b}".format(col=col, b=brace) for col in columnsWithScdValues if col not in [COL_DL_CURATED_LOAD, COL_RECORD_START, COL_RECORD_END, COL_RECORD_DELETED, COL_RECORD_CURRENT, skColumn]]) + defaultNewValues
  skValue = "tbl_stg.{}".format(skColumn) if targetSqlServer else "NULL"
  baseColumns = ", ".join(["tbl_tgt.{b}{col}{b}".format(col=col, b=brace) for col in columns if col not in [skColumn]])
  sourceQuery = f"SELECT *, 0 {COL_RECORD_DELETED}, 1 {COL_RECORD_CURRENT} FROM {stageTableFqn}" if mode == "insert" else f"SELECT {baseColumns}, 1 {COL_RECORD_DELETED}, 1 {COL_RECORD_CURRENT} FROM {targetTableFqn} tbl_tgt LEFT JOIN {stageTableFqn} tbl_src ON {srcBks} = {tgtBks} WHERE tbl_tgt.{COL_RECORD_DELETED} = 0 AND tbl_tgt.{COL_RECORD_CURRENT} = 1 AND {srcBks} IS NULL"
  changeCondition = "1=1" if mode == "delete" else changeCondition
  changeConditionStg = "1=1" if mode == "delete" else changeConditionStg
  
  mergeTemplate = f"\
WITH tbl_src AS ( \
	SELECT * FROM ( {sourceQuery} ) src \
) \
MERGE INTO {targetTableFqn} tbl_mn \
USING ( \
	SELECT {srcBks} AS merge_key, tbl_src.* FROM tbl_src \
	UNION ALL \
	SELECT null as merge_key, tbl_src.* \
    FROM tbl_src \
    LEFT JOIN {targetTableFqn} tbl_tgt ON {srcBks} = {tgtBks} \
	WHERE (tbl_tgt.{COL_RECORD_CURRENT} = 1 \
	AND ({changeCondition}) ) OR (tbl_tgt.{COL_RECORD_DELETED} = 1 AND tbl_tgt.{COL_RECORD_CURRENT} = 1) \
) tbl_stg \
ON {mnBks} = merge_key \
WHEN MATCHED \
  AND (tbl_mn.{COL_RECORD_CURRENT} = 1 \
  AND ({changeConditionStg})) OR (tbl_mn.{COL_RECORD_DELETED} = 1 AND tbl_mn.{COL_RECORD_CURRENT} = 1)  \
THEN UPDATE \
  SET {COL_RECORD_CURRENT} = 0, {COL_RECORD_END} = {recordEnd} \
WHEN NOT MATCHED THEN \
  INSERT ( {insertColumns} ) \
VALUES ( {skValue}, {insertColumnValues} )"
  mergeTemplate += ";" if targetSqlServer else ""
  
  return mergeTemplate

# COMMAND ----------

###########################################################################################################################
# Function: _AddScdColumns
#  ADDS SCD COLUMNS TO DATAFRAME
# Parameters: 
#  dataframe - DataFrame - Dataframe to add SCD columns
# Returns:
#  DataFrame - With SCD columns
#############################################################################################################################
def _AddScdColumns(dataFrame : object):
  exceptionList = [COL_RECORD_VERSION, COL_RECORD_START, COL_RECORD_END, COL_RECORD_CURRENT, COL_RECORD_DELETED, COL_DL_RAW_LOAD, COL_DL_TRUSTED_LOAD, COL_DL_CURATED_LOAD]

  base = dataFrame
  for col in dataFrame.columns:
    if col in exceptionList:
      base = base.drop(col)

  l = base.columns
  l.extend([COL_DL_CURATED_LOAD, COL_RECORD_START, COL_RECORD_END, COL_RECORD_DELETED, COL_RECORD_CURRENT])

  dataFrame = dataFrame.withColumn(COL_DL_CURATED_LOAD, lit("NULL").cast("timestamp"))
  if COL_RECORD_START not in dataFrame.columns: 
    dataFrame = dataFrame.withColumn(COL_RECORD_START, lit("NULL"))
  dataFrame = dataFrame.withColumn(COL_RECORD_START, dataFrame[COL_RECORD_START].cast("timestamp"))
  dataFrame = dataFrame.withColumn(COL_RECORD_END, lit("NULL").cast("timestamp"))
  dataFrame = dataFrame.withColumn(COL_RECORD_DELETED, lit("NULL").cast("int"))
  dataFrame = dataFrame.withColumn(COL_RECORD_CURRENT, lit("NULL").cast("int"))

  return dataFrame[l]

# COMMAND ----------

###########################################################################################################################
# Function: _StageDeltaRecords
#  STAGES DATAFRAME TO DATA LAKE
# Parameters: 
#  dataframe - DataFrame - Dataframe to stage
#  dlStageTableFqn - String - Fully qualified name stage table
# Returns:
#  None
#############################################################################################################################
def _StageDeltaRecords(dataFrame : object, dlStageTableFqn : str):
  #STAGE RECORDS 
  LogEtl(f"Stage Deltas. [{dlStageTableFqn}] ")
  count = dataFrame.count()
  LogEtl(f"Transformed {count:,} rows.")
  spark.sql(f"DROP TABLE IF EXISTS {dlStageTableFqn}")
  dataFrame.write.saveAsTable(dlStageTableFqn)
  spark.sql(f"REFRESH TABLE {dlStageTableFqn}")

# COMMAND ----------

###########################################################################################################################
# Function: _MergeSCDDataLake
#  PERFORMS SCD MERGE ON DATALAKE 
#  ASSUMES USE OF STAGED TABLE
# Parameters: 
#  dataframe - DataFrame - Dataframe to save
#  businessKey - String - Business Key to join
#  dlStageTableFqn - String - Fully qualified stage table name
#  dlTargetTableFqn - String - Fully qualified target table name
#  dataLakePath - String - Data lake path
#  mode - String - full/incremental
# Returns:
#  None
#############################################################################################################################
def _MergeSCDDataLake(dataFrame : object, businessKey : str, dlStageTableFqn : str, dlTargetTableFqn : str, dataLakePath : str, mode : str = "full"):
  #CREATE EMPTY DELTA TABLE IF NOT PRESENT 
  if DeltaTableExists(dlTargetTableFqn) == False:
    LogEtl("Creating empty DELTA table. [{}]".format(dlTargetTableFqn))
    emptyDf = _AddScdColumns(dataFrame).where(lit("1")==lit("0"))
    DataLakeWriteDeltaFqn(emptyDf, dlTargetTableFqn, dataLakePath)
    
  #MERGE DELTA TABLE
  LogEtl("Merging Delta.")
  targetDf = spark.sql("SELECT * FROM {}".format(dlTargetTableFqn))
  mergeDeltaSql = _GenerateScdMergeQuery(dataframe=dataFrame
                                         ,businessKey=businessKey
                                         ,stageTableFqn=dlStageTableFqn
                                         ,targetTableFqn=dlTargetTableFqn)
  #print(mergeDeltaSql)
  spark.sql(mergeDeltaSql)
  
  if mode == "full":
    #DETECT DELETIONS #PRADA-1275
    LogEtl("Detecting Deletions.")
    targetDf = spark.sql("SELECT * FROM {}".format(dlTargetTableFqn))
    deletionMergeSql = _GenerateScdMergeQuery(dataframe=dataFrame
                                         ,businessKey=businessKey
                                         ,stageTableFqn=dlStageTableFqn
                                         ,targetTableFqn=dlTargetTableFqn
                                         ,mode="delete")
    #print(deletionMergeSql)
    spark.sql(deletionMergeSql)
  
  #SEED SURROGATE KEY COLUMN
  _SeedSurrogateKeyColumn(dataFrame.columns[0], dlTargetTableFqn, businessKey)

  #REFRESH DL TABLE
  LogEtl("Refreshing Delta.")
  spark.sql("REFRESH TABLE {}".format(dlTargetTableFqn))

# COMMAND ----------

###########################################################################################################################
# Function: _SqlAlterSqlTableDateTime2
#  ALTERS A TARGET SQL SERVER TO USE DATETIME2 INSTAD OF DATETIME
# Parameters: 
#  sqlTargetFqn - String - Fully qualified target SQL table name
# Returns:
#  None
#############################################################################################################################
def _SqlAlterSqlTableDateTime2(sqlTargetFqn : str):
  query = "DECLARE @SQL NVARCHAR(4000) = (\
    SELECT \
    STRING_AGG( \
    REPLACE(REPLACE(REPLACE( \
    'ALTER TABLE [$schema$].[$tableName$] ALTER COLUMN [$columnName$] datetime2' \
    ,'$schema$', OBJECT_SCHEMA_NAME(c.object_id)) \
    ,'$tableName$', OBJECT_NAME(C.object_id)) \
    ,'$columnName$', c.name) \
    ,';') \
    FROM sys.columns c \
    WHERE \
    object_id = OBJECT_ID('{}') \
    AND system_type_id = 61 \
    ) \
    EXEC sp_executesql @SQL".format(sqlTargetFqn)
  
  _ExecuteSqlQuery(query)

# COMMAND ----------

############################################################################################################################
# Function: _GenerateCreateSqlTableQuery
#  GENERATES A CREATE SQL TABLE QUERY BASED OFF A DATAFRAME. SAMPLES THE DATA TO DETERMINE BEST FIT. 
# Parameters: 
#  dataFrame = DataFrame to create from
#  sqlTargetFqn = SQL Server Fully Qualified Name
# Returns:
#  string = SQL CREATE TABLE query
#############################################################################################################################
def _GenerateCreateSqlTableQuery(dataFrame : object, sqlTargetFqn : str):
  GROWTH = 1.125
  DEFAULT_SIZE = 2000
  convert = {"string": "nvarchar", "timestamp":"DateTime2", "double":"decimal(16,8)"}
  columns = []
  df = dataFrame

  for c in df.dtypes:
    col = c[0]
    lookup = convert.get(c[1])
    type = c[1] if not lookup else lookup
    size = "" if lookup != "nvarchar" else "({})".format(math.ceil(df.select(expr(f"COALESCE(MAX(LENGTH({col})) * {GROWTH}, {DEFAULT_SIZE}) C")).dropna().rdd.max()[0]))
    columns.append(f"[{col}] {type}{size} NULL")
  cols = ", ".join(columns)
  sql = f"CREATE TABLE {sqlTargetFqn} ({cols})"
  return sql

# COMMAND ----------

###########################################################################################################################
# Function: _SqlCreateEmptyTable
#  CREATES AN EMPTY TABLE ON SQL SERVER
# Parameters: 
#  dataframe - DataFrame - Dataframe to save
#  sqlTargetFqn - String - Fully qualified target SQL table name
# Returns:
#  None
#############################################################################################################################
def _SqlCreateEmptyTable(dataFrame : object, sqlTargetFqn : str):
  #CREATE EMPTY SQL TABLE IF NOT PRESENT 
  if SqlTableExistsFqn(sqlTargetFqn) == False:
    LogEtl("Creating empty SQL table. [{}]".format(sqlTargetFqn))
    emptyDf = dataFrame.limit(0)
    DataFrameToSql(emptyDf, sqlTargetFqn)
    _SqlAlterSqlTableDateTime2(sqlTargetFqn)

# COMMAND ----------

###########################################################################################################################
# Function: _SqlInjectSurrogateKeyColumn
#  INJECTS A SURROGATE KEY COLUMN AS FIRST COLUMN AND SETS THE IDENTITY VALUE
# Parameters: 
#  dataframe - DataFrame - Dataframe to inject surrogateKey
#  dlTargetTableFqn - String - Surrogate key column name
# Returns:
#  DataFrame - with surrogate key set
#############################################################################################################################
def _SqlInjectSurrogateKeyColumn(dataFrame : object, dlTargetTableFqn : str):
  LogEtl("Inject Surrogate Key.")
  #GET ENTITY
  entity = dlTargetTableFqn.split(".")[1]
  
  #SURROGATE KEY NAME 
  df = spark.createDataFrame(data="", schema=StructType([
      StructField("{}SK".format(GeneralToPascalCase(entity)), LongType(), False)
    ])
  )
  dataFrame = df.join(dataFrame)
  return dataFrame

# COMMAND ----------

###########################################################################################################################
# Function: _SeedSurrogateKeyColumn
#  SEEDS A SURROGATE KEY COLUMN WHEN MISSING
# Parameters: 
#  surrogateKeyColumn - String - Surrogate key column name
#  dlTargetTableFqn - String - Target DELTA table
# Returns:
#  None
#############################################################################################################################
def _SeedSurrogateKeyColumn(surrogateKeyColumn : str, dlTargetTableFqn : str, businessKeys : str):
  LogEtl("Seed Surrogate Key.")
  #GET LAST VALUE
  max = 0
  try:
    max = spark.table(dlTargetTableFqn).where(expr("{} IS NOT NULL".format(surrogateKeyColumn))).select(surrogateKeyColumn).rdd.max()[0]
  except: 
    i=0

  max = 0 if max is None else max
  
  cols = businessKeys.split(",")
  brace = '`'
  updateCondition = " AND ".join(["SRC.{b}{col}{b} = TGT.{b}{col}{b}".format(col=col, b=brace) for col in cols]) 

  #UPDATE SK COLUMN
  #This update syntax works only till Rumtime 6.4
  update = "UPDATE {table} SET {skColumn} = CAST(ROW_NUMBER() OVER (ORDER BY 1) AS BIGINT) + {max} WHERE {skColumn} IS NULL".format(table=dlTargetTableFqn, skColumn=surrogateKeyColumn, max=max)
  
  #The following UPDATE can work with the newer runtimes. However, it requires a unique Business Key
  update = f"WITH UpdateSK AS ( SELECT {businessKeys}, _RecordStart, CAST(ROW_NUMBER() OVER (ORDER BY 1) AS BIGINT) + {max} AS {surrogateKeyColumn} FROM {dlTargetTableFqn} WHERE {surrogateKeyColumn} IS NULL ) MERGE INTO {dlTargetTableFqn} TGT USING UpdateSK SRC ON {updateCondition} AND SRC._Recordstart = TGT._RecordStart WHEN MATCHED THEN UPDATE SET {surrogateKeyColumn} = SRC.{surrogateKeyColumn}"
  print(update)
  
  spark.sql(update)

# COMMAND ----------

###########################################################################################################################
# Function: _SqlSetSurrogateKey
#  GENERATES SURROGATE KEY ON NOMINATED COLUMN ON DATAFRAME
# Parameters: 
#  dataframe - DataFrame - Dataframe to set surrogateKey
#  surrogateKey - String - Surrogate key column name
#  businessKey - String - Business Key value(s)
# Returns:
#  DataFrame - with surrogate key set
#############################################################################################################################
def _SqlSetSurrogateKey(dataFrame : object, surrogateKey : str, businessKey : str):
  df = dataFrame.withColumn(surrogateKey, expr("ROW_NUMBER() OVER (ORDER BY 1)".format(businessKey=businessKey)).cast("BIGINT"))
  return df

# COMMAND ----------

###########################################################################################################################
# Function: _MergeScdSqlServer
#  PERFORMS SCD MERGE ON SQL SERVER 
#  ASSUMES USE OF STAGED TABLE
# Parameters: 
#  dataframe - DataFrame - Dataframe to save
#  businessKey - String - Business Key to join
#  dlStageTableFqn - String - Fully qualified stage table name
#  sqlStageTableFqn - String - Fully qualified stage SQL table name
#  sqlTargetFqn - String - Fully qualified target SQL table name
# Returns:
#  None
#############################################################################################################################
def _MergeScdSqlServer(dataFrame : object, businessKey : str, dlTargetTableFqn : str, sqlStageTableFqn : str, sqlTargetFqn : str):
  #GET SOURCE STRUCTURE
  sourceDataFrame = spark.table(dlTargetTableFqn)
  
  max = 0
  #GET MAX RECORD START
  try:
    skColumn = sourceDataFrame.columns[0]
    query = "(SELECT MAX({col}) MaxStart FROM {sqlTargetFqn}) T".format(sqlTargetFqn=sqlTargetFqn, col=skColumn)
    jdbcUrl = AzSqlGetDBConnectionURL()
    df = spark.read.jdbc(url=jdbcUrl, table=query)
    max = df.rdd.collect()[0].MaxStart
    max = 0 if max is None else max
  except:
    e=False
  
  #NEW STAGES
  newSqlStagedDf = spark.table(dlTargetTableFqn).where(expr("{} > '{}'".format(skColumn, max)))
  
  #NOTHING NEW
  if(newSqlStagedDf.count() == 0):
    LogEtl("SQL already current!")
    return

  #CREATE EMPTY SQL TARGET TABLE IF ENABLED
  if CREATE_SQL_TARGET and SqlTableExistsFqn(sqlTargetFqn) == False:
    LogEtl("Creating Target SQL table with best fit. [{}]".format(sqlTargetFqn))
    newTable = _GenerateCreateSqlTableQuery(sourceDataFrame, sqlTargetFqn)
    _ExecuteSqlQuery(newTable)
  
  #DROP STAGE TABLE
  try:
    _ExecuteSqlQuery("DROP TABLE {};".format(sqlStageTableFqn))
  except:
    e=False
  
  #CREATE EMPTY SQL STAGE TABLE
  _SqlCreateEmptyTable(sourceDataFrame.limit(0), sqlStageTableFqn)
  
  #STAGE SQL RECORDS
  LogEtl("Stage SQL Server. [{}]".format(sqlStageTableFqn))
  DataFrameToSql(newSqlStagedDf, sqlStageTableFqn, mode="append")
  
  #MERGE SQL SERVER
  LogEtl("Merging SQL Server.")
  mergeSqlServer = _GenerateScdMergeQuery(dataframe=newSqlStagedDf
                                         ,businessKey=businessKey
                                         ,stageTableFqn=sqlStageTableFqn
                                         ,targetTableFqn=sqlTargetFqn
                                         ,targetSqlServer=True)
  _ExecuteSqlQuery(mergeSqlServer)

# COMMAND ----------

###########################################################################################################################
# Function: _ExecuteSqlQuery
#  RUNS A SQL QUERY USING A SCALA NOTEBOOK
# Parameters: 
#  sql - String - SQL to run on SQL Server
# Returns:
#  None
#############################################################################################################################
def _ExecuteSqlQuery(sql : str):
  #WORKING AROUND LIMIT OF CALLING A NOTEBOOK WITH MORE THAN 10,000 BYTES
  
  #GET PATH
  raw = DataLakeGetMountPoint(ADS_CONTAINER_RAW)
  path = "{}/blob-sql/{}.txt".format(raw, GeneralRandomString(8))
  queryFilePath = "/dbfs" + path

  #WRITE CONTENTS
  dbutils.fs.put(path, sql, True)
  
  #RUN SCALA NOTEBOOK
  dbutils.notebook.run("/build/includes/util-sql-to-run", 0, {"queryFilePath" : queryFilePath})
  
  #CLEAN UP
  dbutils.fs.rm(path)

# COMMAND ----------

###########################################################################################################################
# Function: _ExecuteSqlQuery
#  RUNS A SQL QUERY USING A SCALA NOTEBOOK
# Parameters: 
#  sql - String - SQL to run on SQL Server
# Returns:
#  None
#############################################################################################################################
def _ExecuteSqlQueryOnDatabase(sql : str, databaseName : str):
  #WORKING AROUND LIMIT OF CALLING A NOTEBOOK WITH MORE THAN 10,000 BYTES
  
  #GET PATH
  raw = DataLakeGetMountPoint(ADS_CONTAINER_RAW)
  path = "{}/blob-sql/{}.txt".format(raw, GeneralRandomString(8))
  queryFilePath = "/dbfs" + path

  #WRITE CONTENTS
  dbutils.fs.put(path, sql, True)
  
  #RUN SCALA NOTEBOOK
  dbutils.notebook.run("/build/includes/util-sql-to-run", 0, {"queryFilePath" : queryFilePath, "databaseName" : databaseName})
  
  #CLEAN UP
  dbutils.fs.rm(path)

# COMMAND ----------

###########################################################################################################################  
# Function: ValidateBusinessKey
#  CHECKS TO SEE IF ONE OF COLUMNS IN BUSINESS KEY LIST IS BAD. SPECIFICALLY COMPOSITE KEYS THAT PARTICIPATE IN CONCAT(). #PRADA-1275
# Parameters:
#  dataFrame = Input DataFrame 
#  businessKey - String - Business Key to join
# Returns:
#  None
#############################################################################################################################
def ValidateBusinessKey(dataFrame : object, businessKey : str):
  badTypes = ["double"]
  keys = businessKey.split(",")
  for s in keys:
    for t in dataFrame.dtypes:
      column = t[0]
      dataType = t[1]
      if (column == s and dataType in badTypes):
        if len(keys) == 1:
          LogEtl(f"*** Warning *** [{column}]({dataType}) may cause unexpected BusinessKey mismatches.")
        else:
          exception = f"Exception: [{column}]({dataType}) is a bad type for BusinessKey!"
          LogEtl(exception)
          raise Exception(exception)

# COMMAND ----------

###########################################################################################################################
# Function: _AppendScdSqlServer
#  PERFORMS SCD APPEND ON SQL SERVER 
#  ASSUMES USE OF STAGED TABLE
# Parameters: 
#  dataframe - DataFrame - Dataframe to save
#  businessKey - String - Business Key to join
#  dlStageTableFqn - String - Fully qualified stage table name
#  sqlStageTableFqn - String - Fully qualified stage SQL table name
#  sqlTargetFqn - String - Fully qualified target SQL table name
# Returns:
#  None
#############################################################################################################################
def _AppendScdSqlServer(dataFrame : object, businessKey : str, dlTargetTableFqn : str, sqlStageTableFqn : str, sqlTargetFqn : str):
  #GET SOURCE STRUCTURE
  sourceDataFrame = spark.table(dlTargetTableFqn)
  
  #GET MAX RECORD START
  max = 0
  try:
    skColumn = sourceDataFrame.columns[0]
    query = f"(SELECT MAX({skColumn}) MaxStart FROM {sqlTargetFqn}) T"
    jdbcUrl = AzSqlGetDBConnectionURL()
    df = spark.read.jdbc(url=jdbcUrl, table=query)
    max = df.rdd.collect()[0].MaxStart
    max = 0 if max is None else max
    #print(f"{query} : {max}")
  except:
    e=False
  
  #NEW STAGES
  newSqlStagedDf = spark.table(dlTargetTableFqn).where(expr(f"{skColumn} > '{max}'"))
  print(f"{dlTargetTableFqn} : {skColumn} > '{max}'")
  
  #NOTHING NEW
  if(newSqlStagedDf.count() == 0):
    LogEtl("SQL already current!")
    return

  #CREATE EMPTY SQL TARGET TABLE IF ENABLED
  if CREATE_SQL_TARGET and SqlTableExistsFqn(sqlTargetFqn) == False:
    LogEtl(f"Creating Target SQL table with best fit. [{sqlTargetFqn}]")
    newTable = _GenerateCreateSqlTableQuery(sourceDataFrame, sqlTargetFqn)
    _ExecuteSqlQuery(newTable)
  
  #DROP STAGE TABLE
  try:
    _ExecuteSqlQuery(f"DROP TABLE {sqlStageTableFqn};")
  except:
    e=False
  
  #CREATE EMPTY SQL STAGE TABLE
  _SqlCreateEmptyTable(sourceDataFrame.limit(0), sqlStageTableFqn)
  
  #STAGE SQL RECORDS
  LogEtl(f"Stage SQL Server. [{sqlStageTableFqn}]")
  DataFrameToSql(newSqlStagedDf, sqlStageTableFqn, mode="append")
  
  #CORRECT SQL DRIFT #PRADA-1275
  LogEtl("Correcting SQL Drift.")
  bk = BusinessKeyListToColumn(businessKey)
  sk = dataFrame.columns[0]
  _ExecuteSqlQuery(f"WITH CTE AS ( \
  SELECT \
  [{sk}] \
  ,LEAD([{COL_RECORD_START}], 1) OVER(ORDER BY BK, [{sk}]) [{COL_RECORD_END}] \
  ,IIF(LEAD([{COL_RECORD_START}], 1) OVER(ORDER BY BK, [{sk}]) IS NULL, 1, 0) [{COL_RECORD_CURRENT}] \
  ,IIF(LEAD([{COL_RECORD_START}], 1) OVER(ORDER BY BK, [{sk}]) IS NULL, 1, 0) [{COL_RECORD_DELETED}] \
  FROM \
  ( \
      SELECT [{sk}] \
      ,{bk} BK \
      ,[{COL_RECORD_START}] \
      FROM {sqlTargetFqn} \
      WHERE {bk} IN (SELECT {bk} BK FROM {sqlStageTableFqn}) \
      UNION \
      SELECT [{sk}] \
      ,{bk} BK \
      ,[{COL_RECORD_START}] \
      FROM {sqlStageTableFqn} \
  ) T \
  ) \
  /*SELECT S.[{sk}] ,S.[{COL_RECORD_END}] ,S.[{COL_RECORD_CURRENT}]*/ \
  UPDATE T SET T.[{COL_RECORD_END}] = DATEADD(SECOND, -1, S.[{COL_RECORD_END}]) ,T.[{COL_RECORD_CURRENT}] = S.[{COL_RECORD_CURRENT}]\
  FROM {sqlTargetFqn} T \
  JOIN CTE S ON S.[{sk}] = T.[{sk}] \
  WHERE (S.[{COL_RECORD_CURRENT}] != T.[{COL_RECORD_CURRENT}]) \
  OR (S.[{COL_RECORD_DELETED}] != T.[{COL_RECORD_DELETED}]) \
  ")
  
  #APPEND SQL RECORDS
  LogEtl(f"Append SQL Server. [{sqlTargetFqn}]")
  DataFrameToSql(newSqlStagedDf, sqlTargetFqn, mode="append")

# COMMAND ----------

###########################################################################################################################
# Function: CleanTargetTables
#  PERFORMS A CLEAN ON DELTA TABLE AND/OR SQL TABLE
# Parameters: 
#  CLEAN_MODE - 
#   MODES:
#     00 = do nothing for delta and sql (default)
#     11 = delete delta & truncate sql
#     21 = drop delta & truncate sql
#     22 = drop delta & drop sql 
#   DIGIT MASK: 
#     FIRST = Delta table
#     LAST = SQL table
#   DIGIT OPERATION: 
#     0 = nothing, 1 = delete/truncate, 2 = drop
#  deltaTableFqn - String - Fully qualified target Delta table name
#  sqlTableFqn - String - Fully qualified target SQL table name
# Returns:
#  None
#############################################################################################################################
CLEAN_MODE = "00"
def CleanTargetTables(deltaTableFqn, sqlTableFqn):
  if CLEAN_MODE not in ["00", "11", "21", "22"]: #ONLY AVAILABLE MODES
    raise Exception(f"Bad clean mode [{CLEAN_MODE}]!")
  if CLEAN_MODE == "00":
    return
  LogEtl(f"Cleaning [{CLEAN_MODE}]...")

  deltaMode = CLEAN_MODE[0:1]
  sqlMode = CLEAN_MODE[-1]
  delta = f"DROP TABLE {deltaTableFqn}" if deltaMode == "2" else f"DELETE FROM {deltaTableFqn}"
  path = spark.sql(f"DESCRIBE DETAIL {deltaTableFqn}").select(expr("location")).rdd.collect()[0][0]
  sql = f"DROP TABLE {sqlTableFqn}" if sqlMode == "2" else f"TRUNCATE TABLE {sqlTableFqn}"
  LogEtl(f"{delta} | {path} | {sql}")
  
  try:
    if deltaMode == "2":
      dbutils.fs.rm(path, True)
  except:
    pass
  try:
    spark.sql(delta)
  except:
    pass
  try:
    _ExecuteSqlQuery(sql)
  except:
    pass

# COMMAND ----------

###########################################################################################################################
# Function: MergeSCD
#  PERFORMS FULL SCD MERGE OPERATION ON DATALAKE AND SQL SERVER
#  ASSUMES DATAFRAME IS ALREADY A DELTA DATASET
#  1. STAGE ON DATALAKE
#  2. MERGE ON DATALAKE
#  3. MERGE ON SQL SERVER
#  5. CLEAN UP 
# Parameters: 
#  dataframe - DataFrame - Dataframe to save
#  businessKey - String - Business Key to join
#  dlStageTableFqn - String - Fully qualified stage table name
#  dlTargetTableFqn - String - Fully qualified target table name
#  dataLakePath - String - Data lake path
#  sqlStageTableFqn - String - Fully qualified stage SQL table name
#  sqlTargetFqn - String - Fully qualified target SQL table name
# Returns:
#  None
#############################################################################################################################
def MergeSCD(dataFrame : object, businessKey : str, dlStageTableFqn : str, dlTargetTableFqn : str, dataLakePath : str, sqlStageTableFqn : str, sqlTargetFqn : str):
  LogEtl("Starting SCD Merge...")
  LogEtl("Caching.")
  dataFrame.cache()

  #GO CREATE THE TABLE YOURSELF
  if CREATE_SQL_TARGET == False:
    if SqlTableExistsFqn(sqlTargetFqn) == False:
      exception = f"Target SQL table [{sqlTargetFqn}] missing!"
      LogEtl(exception)
      raise Exception(exception)

  #VALIDATE BUSINESS KEY
  ValidateBusinessKey(dataFrame, businessKey)
  
  #CLEAN IF TOLD
  CleanTargetTables(dlTargetTableFqn, sqlTargetFqn)
  
  #STAGE RECORDS
  _StageDeltaRecords(dataFrame, dlStageTableFqn)

  #INJECT SURROGATE KEY COLUMN
  dataFrame = _SqlInjectSurrogateKeyColumn(dataFrame, dlTargetTableFqn)

  #MERGE DATA LAKE
  _MergeSCDDataLake(dataFrame, businessKey, dlStageTableFqn, dlTargetTableFqn, dataLakePath)
  
  #APPEND SCD SQL SERVER
  _AppendScdSqlServer(dataFrame, businessKey, dlTargetTableFqn, sqlStageTableFqn, sqlTargetFqn)

  #CLEAN STAGE TABLES
  #CleanStageTables(dlStageTableFqn, sqlStageTableFqn)

  LogEtl("Done SCD Merge!")

# COMMAND ----------

###########################################################################################################################
# Function: _AppendDeltaAndSql
#  PERFORMS FULL APPEND AND OVERWRITE OPERATION ON DATALAKE AND SQL SERVER
#  ASSUMES DATAFRAME IS ALREADY A DELTA DATASET
#  1. STAGE ON DATALAKE
#  2. APPEND AND OVERWRITE ON DATALAKE
#  3. MEAPPEND AND OVERWRITERGE ON SQL SERVER
#  4. CLEAN UP 
# Parameters: 
#  dataframe - DataFrame - Dataframe to save
#  businessKey - String - Business Key to join
#  dlStageTableFqn - String - Fully qualified stage table name
#  dlTargetTableFqn - String - Fully qualified target table name
#  dataLakePath - String - Data lake path
#  sqlStageTableFqn - String - Fully qualified stage SQL table name
#  sqlTargetFqn - String - Fully qualified target SQL table name
# Returns:
#  None
#############################################################################################################################
def _AppendDeltaAndSql(dataFrame : object, businessKey : str, dlStageTableFqn : str, dlTargetTableFqn : str, dataLakePath : str, sqlStageTableFqn : str, sqlTargetFqn : str):
  LogEtl("Starting Append...")
  LogEtl("Caching.")
  dataFrame.cache()
  
  #GO CREATE THE TABLE YOURSELF
  if CREATE_SQL_TARGET == False:
    if SqlTableExistsFqn(sqlTargetFqn) == False:
      LogEtl(f"Target SQL table [{sqlTargetFqn}] missing!")
      raise Exception(f"Target SQL table [{sqlTargetFqn}] missing!")
    
  #VALIDATE BUSINESS KEY
  ValidateBusinessKey(dataFrame, businessKey)

  #CLEAN IF TOLD
  CleanTargetTables(dlTargetTableFqn, sqlTargetFqn)
  
  #STAGE RECORDS
  _StageDeltaRecords(dataFrame, dlStageTableFqn)
  
  #SELECT COLUMN
  businessKeyList = businessKey.split(",")
  bkList = "{}".format(", ".join([f"{col}"for col in businessKeyList]))
  bk = bkList if len(businessKeyList) == 1 else f"CONCAT({bkList})"
  
  #CREATE EMPTY DELTA TABLE IF NOT PRESENT 
  if DeltaTableExists(dlTargetTableFqn) == False:
    #INJECT SURROGATE KEY COLUMN
    dataFrameWithSk = _SqlInjectSurrogateKeyColumn(dataFrame, dlTargetTableFqn)
  
    LogEtl(f"Creating empty DELTA table. [{dlTargetTableFqn}]")
    emptyDf = _AddScdColumns(dataFrameWithSk).where(lit("1")==lit("0"))
    # WRITE TO DATA LAKE
    emptyDf.write \
    .format('delta') \
    .option('mergeSchema', 'true') \
    .mode("append") \
    .save(dataLakePath)

    # CREATE DELTA TABLE
    query = f"CREATE TABLE IF NOT EXISTS {dlTargetTableFqn} USING DELTA LOCATION \'{dataLakePath}\'"
    spark.sql(query)

  #DELETE DATA LAKE
  LogEtl(f"Delete Delta. [{dlTargetTableFqn}]")
  spark.sql(f"DELETE FROM {dlTargetTableFqn} WHERE {bk} IN (SELECT DISTINCT {bk} FROM {dlStageTableFqn})")
  
  #APPEND DELTA TABLE
  LogEtl(f"Append Delta. [{dlTargetTableFqn}]")
  columns = ",".join([f"`{c}`" for c in dataFrame.columns])
  ts = f"from_utc_timestamp(CURRENT_TIMESTAMP(), '{CURRET_TIMEZONE}')"
  sk = spark.table(dlTargetTableFqn).columns[0]
  sql = f"SELECT NULL {sk}, {columns}, {ts} _DLCuratedZoneTimeStamp, {ts} _RecordStart, CAST('9999-12-31' AS TIMESTAMP) _RecordEnd, 0 _RecordDeleted, 1 _RecordCurrent FROM {dlStageTableFqn}"
  df = spark.sql(sql)
  df.write.mode("append").format("delta").save(dataLakePath)
  
  #SEED SURROGATE KEY COLUMN
  #Do not seed surrogate key if it is Append module (for Facts)
  #_SeedSurrogateKeyColumn(sk, dlTargetTableFqn, businessKey)

  #REFRESH DL TABLE
  LogEtl("Refreshing Delta.")
  spark.sql(f"REFRESH TABLE {dlTargetTableFqn}")

  #GET SOURCE STRUCTURE
  sourceDataFrame = spark.table(dlTargetTableFqn)
  
  #CREATE EMPTY SQL TARGET TABLE IF ENABLED
  if CREATE_SQL_TARGET and SqlTableExistsFqn(sqlTargetFqn) == False:
    LogEtl(f"Creating Target SQL table with best fit. [{sqlTargetFqn}]")
    newTable = _GenerateCreateSqlTableQuery(sourceDataFrame, sqlTargetFqn)
    _ExecuteSqlQuery(newTable)
  
  #STAGE DISTINC BKT SQL
  LogEtl(f"Stage SQL Server. [{sqlStageTableFqn}]")
  DataFrameToSql(spark.sql(f"SELECT DISTINCT {bk} FROM {dlStageTableFqn}"), sqlStageTableFqn, mode="overwrite")

  #DELETE SQL
  LogEtl(f"Delete SQL Server. [{sqlTargetFqn}]")
  _ExecuteSqlQuery(f"DELETE FROM {sqlTargetFqn} WHERE {bk} IN (SELECT {bk} FROM {sqlStageTableFqn})")
  
  #APPEND SQL
  LogEtl(f"Append SQL Server. [{sqlTargetFqn}]")
  DataFrameToSql(spark.sql(f"SELECT * FROM {dlTargetTableFqn} WHERE {bk} IN (SELECT DISTINCT {bk} FROM {dlStageTableFqn})"), sqlTargetFqn, mode="append")
  
  LogEtl("Append Done!")

# COMMAND ----------

