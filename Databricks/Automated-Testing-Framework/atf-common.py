# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *
from datetime import *
import pytz
import requests
import json
from ast import literal_eval

# COMMAND ----------

DEBUG = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())["rootRunId"] is None
_results = []
_localMethods = []
_currentNotebook = ""
_currentTestCase = ""
_startTimestamp = ""
DEFAULT_BATCH_ID = "0000"

try:
    DEFAULT_BATCH_ID = dbutils.widgets.get("DEFAULT_BATCH_ID")
    print(DEFAULT_BATCH_ID)
except:
    pass

# COMMAND ----------

def GetNotebookName():
    list = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split("/")
    count=len(list)
    return list[count-1]

# COMMAND ----------

def GetDatabaseName():
    list = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split("/")
    count=len(list)
    return list[count-2]

# COMMAND ----------

def GetSelfFqn():
    return f"{GetDatabaseName()}.{GetNotebookName()}"

# COMMAND ----------

def GetSelf():
    return spark.table(GetSelfFqn())

# COMMAND ----------

def JasonToDataFrame(jsonInput):
    jsonData = json.dumps(jsonInput)
    jsonDataList = []
    jsonDataList.append(jsonData)
    jsonRDD = sc.parallelize(jsonDataList)
    return spark.read.json(jsonRDD)

# COMMAND ----------

def DisplayJsonResults(json):
    display(
        JasonToDataFrame(json).selectExpr("explode(results) results")
        .select("results.*")
        .selectExpr(
            "Object"
            ,"Type"
            ,"Case"
            ,"Input"
            ,"Output"
            ,"Result"
            ,"Passed"
            ,"Error"
            ,"Start"
            ,"End"
        )
    )

# COMMAND ----------

def _TestingEnd():
    r = json.dumps( {
            "BatchId" : DEFAULT_BATCH_ID
            ,"run" : json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
            ,"results" : _results
        }
    )
    #dbutils.fs.put(f"/mnt/atf/run/{GetNotebookName()}.json", json.dumps({"results" : _results }, indent=3), True)
    #print(json.dumps({"results" : _results }, indent=3)) if DEBUG == 1 else dbutils.notebook.exit(r)
    DisplayJsonResults({"results" : _results }) if DEBUG == 1 else dbutils.notebook.exit(r)

# COMMAND ----------

def GetTimestamp():
    return spark.sql("SELECT STRING(CURRENT_TIMESTAMP())").collect()[0][0]

# COMMAND ----------

def _LogResults(inputValue : str, outputValue : str, passed : str, error : str):
     _results.append({
        "Object" : f"{GetSelfFqn()}",
        "Type" : "Automated" if _currentTestCase in _automatedMethods[GetDatabaseName()] else "Manaul",
        "Case" : _currentTestCase,
        "Input" : str(inputValue),
        "Output" : str(outputValue),
        "Result" : "Pass" if passed else "Fail",
        "Passed" : passed, #INT
        "Error" : error,
        "Start" : str(_startTimestamp),
        "End" : str(GetTimestamp())
    })

# COMMAND ----------

def loadActiveSourceDf(source):
    global sourceColumns
    source.createOrReplaceTempView("sourceView")
    
    try: 
        sourceDf = spark.sql(f"""SELECT {sourceColumns}
                             FROM sourceView 
                             WHERE _RecordDeleted = 0 and _RecordCurrent = 1""")
    except:
        sourceDf = spark.sql(f"SELECT {sourceColumns} FROM sourceView")
    
    return sourceDf

# COMMAND ----------

def loadInactiveSourceDf(source):
    global sourceColumns
    source.createOrReplaceTempView("sourceView")
    sourceDf = spark.sql(f"""SELECT {sourceColumns}
                         FROM sourceView 
                         WHERE _RecordDeleted = 1 and _RecordCurrent = 0""")
    return sourceDf

# COMMAND ----------

def loadActiveTargetDf(target):
    global targetColumns
    global keyColumns
    target.createOrReplaceTempView("targetView")
    
    keyColsList = keyColumns.split(",")
    sqlQuery = f"SELECT {targetColumns} FROM targetView WHERE _RecordDeleted = 0 "
    
    for column in keyColsList:
        column = column.strip()
        sqlQuery = sqlQuery + f"AND {column} <> -1 "
        
    print(sqlQuery)
    targetDf = spark.sql(sqlQuery) 

    return targetDf

# COMMAND ----------

def loadInactiveTargetDf(target):
    target.createOrReplaceTempView("targetView")
    targetDf = spark.sql(f"""SELECT {targetColumns} FROM targetView 
                         WHERE _RecordDeleted = 1 and _RecordCurrent = 0""")
    return targetDf

# COMMAND ----------

_automatedMethods = {
    "tests" : []
    ,"cleansed" : ["RowCounts", "ColumnColumns","CountValidateCurrentRecords"]
    ,"curated_v2" : ["RowCounts", "DuplicateSK", "DuplicateActiveSK", "MD5ValueSK", "DateValidation", "OverlapAndGapValidation", "CountValidateCurrentRecords", "CountValidateDeletedRecords", "DuplicateCheck", "DuplicateActiveCheck", "ExactDuplicates", "BusinessKeyNullCk", "BusinessKeyLengthCk", "ManBusinessColNullCk", "SourceTargetCountCheckActiveRecords", "SourceMinusTargetCountCheckActiveRecords", "TargetMinusSourceCountCheckActiveRecords", "SourceTargetCountCheckDeletedRecords", "SourceMinusTargetCountCheckDeletedRecords", "TargetMinusSourceCountCheckDeletedRecords", "ColumnColumns"]
}

# COMMAND ----------

def RowCounts():
    count = GetSelf().count()
    GreaterThan(count, 0)

# COMMAND ----------

def DuplicateSK():
    skColumn = GetSelf().columns[0]
    table = GetNotebookName()
    df = spark.sql(f"SELECT COUNT (*) as recCount FROM {GetSelfFqn()} GROUP BY {skColumn} HAVING COUNT (*) > 1")
    count = df.count()
    Assert(count, 0)

# COMMAND ----------

# automated function for sk_chk2 in "SK columns validation" in dimBusinessPartner_SCD_woTS
def DuplicateActiveSK():
    skColumn = GetSelf().columns[0]
    table = GetNotebookName()
    df = spark.sql(f"SELECT COUNT (*) as recCount FROM {GetSelfFqn()} WHERE _RecordCurrent = 1 and _recordDeleted = 0 GROUP BY {skColumn} HAVING COUNT(*) > 1")
    count = df.count()
    Assert(count, 0)

# COMMAND ----------

def MD5ValueSK():
    skColumn = GetSelf().columns[0]
    table = GetNotebookName()
    df = spark.sql(f"SELECT COUNT (*) as recCount FROM {GetSelfFqn()} WHERE {skColumn} is null or {skColumn} = '' or {skColumn} = ' ' or upper({skColumn}) ='NULL'")
    count = df.select("recCount").collect()[0][0]
    Assert(count, 0)

# COMMAND ----------

# date validation automated function
def DateValidation():
    table = GetNotebookName()
    df = spark.sql(f"SELECT * FROM {GetSelfFqn()} \
                WHERE date(_recordStart) > date(_recordEnd)")
    count = df.count()
    Assert(count, 0)

# COMMAND ----------

def OverlapAndGapValidation():
    global keyColumns
    table = GetNotebookName()
    df = spark.sql(f"Select {keyColumns}, start_date, end_date \
                   from (Select {keyColumns}, date(_recordStart) as start_date, date(_recordEnd) as end_date, \
                   max(date(_recordStart)) over (partition by {keyColumns} order by _recordStart rows between 1 following and 1 following) as nxt_date \
                   from {GetSelfFqn()}) \
                   where  DATEDIFF(day, nxt_date, end_date) <> 1")
    count = df.count()
    Assert(count, 0)

# COMMAND ----------

def CountValidateCurrentRecords():
    table = GetNotebookName()
    df = spark.sql(f"select count(*) as recCount from {GetSelfFqn()} \
                WHERE _RecordCurrent=1 and _RecordDeleted=0")
    count = df.select("recCount").collect()[0][0]
    GreaterThan(count,0)

# COMMAND ----------

def CountValidateDeletedRecords():
    table = GetNotebookName()
    df = spark.sql(f"select count(*) as recCount from {GetSelfFqn()} \
                WHERE _RecordCurrent=0 and _RecordDeleted=1")
    count = df.select("recCount").collect()[0][0]
    Assert(count,count)

# COMMAND ----------

def DuplicateCheck():
    global keyColumns
    table = GetNotebookName()
    df = spark.sql(f"SELECT {keyColumns}, COUNT(*) as recCount from {GetSelfFqn()} \
                GROUP BY {keyColumns} HAVING COUNT(*) > 1")
    count = df.count()
    Assert(count,0)

# COMMAND ----------

def DuplicateActiveCheck():
    global keyColumns
    table = GetNotebookName()
    df = spark.sql(f"SELECT {keyColumns}, COUNT(*) as recCount from {GetSelfFqn()} \
                WHERE _RecordCurrent=1 and _recordDeleted=0 \
                GROUP BY {keyColumns} HAVING COUNT(*) > 1")
    count = df.count()
    Assert(count,0)

# COMMAND ----------

def ExactDuplicates():
    global keyColumns
    global targetColumns
    target.createOrReplaceTempView("target_view")
    table = GetNotebookName()
    df = spark.sql(f"SELECT {targetColumns}, COUNT(*) as recCount \
                   FROM {GetSelfFqn()} \
                   GROUP BY {targetColumns} HAVING COUNT(*) > 1")
    count = df.count()
    Assert(count,0)

# COMMAND ----------

def BusinessKeyNullCk():
    global businessKey
    table = GetNotebookName()
    df = spark.sql(f"SELECT * FROM {GetSelfFqn()} \
                WHERE ({businessKey} is NULL or {businessKey} in ('',' ') or UPPER({businessKey})='NULL')")
    count = df.count()
    Assert(count,0)


# COMMAND ----------

def BusinessKeyLengthCk():
    global businessKey
    table = GetNotebookName()
    df = spark.sql(f"SELECT DISTINCT length({businessKey}) from {GetSelfFqn()} \
                      WHERE length({businessKey})<>2")
    count = df.count()
    Assert(count,1)
    

# COMMAND ----------

def ManBusinessColNullCk():
    global businessColumns
    table = GetNotebookName()
    busColsList = businessColumns.split(",")
    firstColumn = busColsList[0].strip()
    sqlQuery = f"Select * from {GetSelfFqn()} "
    
    for column in busColsList:
        column = column.strip()
        if column == firstColumn:
            sqlQuery = sqlQuery + f"WHERE ({column} is NULL or {column} in ('',' ') or UPPER({column})='NULL') "
        else:
            sqlQuery = sqlQuery + f"OR ({column} is NULL or {column} in ('',' ') or UPPER({column})='NULL') "
    
    #print(sqlQuery)
            
    df = spark.sql(sqlQuery)
    count = df.count()
    Assert(count,0)


# COMMAND ----------

def SourceTargetCountCheckActiveRecords():
    global source
    sourceDf = loadActiveSourceDf(source)
    
    global target
    targetDf = loadActiveTargetDf(target)
    
    sourceCount = sourceDf.count()
    targetCount = targetDf.count()
    
    Assert(sourceCount, targetCount)


# COMMAND ----------

def SourceMinusTargetCountCheckActiveRecords():
    global source
    sourceDf = loadActiveSourceDf(source)
    
    global target
    targetDf = loadActiveTargetDf(target)
    
    df = sourceDf.subtract(targetDf)
    count = df.count()
    
    Assert(count, 0)

# COMMAND ----------

def TargetMinusSourceCountCheckActiveRecords():
    global source
    sourceDf = loadActiveSourceDf(source)
    
    global target
    targetDf = loadActiveTargetDf(target)
    
    df = targetDf.subtract(sourceDf)
    count = df.count()
    
    Assert(count, 0) 

# COMMAND ----------

def SourceTargetCountCheckDeletedRecords():
    global source
    sourceDf = loadInactiveSourceDf(source)

    global target
    targetDf = loadInactiveTargetDf(target)

    sourceCount = sourceDf.count()
    targetCount = targetDf.count()

    Assert(sourceCount, targetCount)
    

# COMMAND ----------

def SourceMinusTargetCountCheckDeletedRecords():
    global source
    sourceDf = loadInactiveSourceDf(source)

    global target
    targetDf = loadInactiveTargetDf(target)

    df = sourceDf.subtract(targetDf)
    count = df.count()
    Assert(count, 0)    

    

# COMMAND ----------

def TargetMinusSourceCountCheckDeletedRecords():
    global source
    sourceDf = loadInactiveSourceDf(source)

    global target
    targetDf = loadInactiveTargetDf(target)

    df = targetDf.subtract(sourceDf)
    count = df.count()
    Assert(count, 0)    

# COMMAND ----------

def ColumnColumns():
    count = len(GetSelf().columns)
    Assert(count,count)

# COMMAND ----------

def GetTestMethodsToRun():
    methods = []
    for key, value in globals().items():
        if ((callable(value) 
            and value.__module__ == __name__ 
            and not(sys._getframe().f_code.co_name == key) 
            and key not in _localMethods)
            or (key in _automatedMethods.get(GetDatabaseName()))):
                methods.append(key)
    return methods

# COMMAND ----------

def RunTests():
    methods = GetTestMethodsToRun()
    if len(methods) == 0:
        print("No tests!")
        return
    for key, value in globals().items():
        if ((callable(value) 
            and value.__module__ == __name__ 
            and not(sys._getframe().f_code.co_name == key) 
            and key not in _localMethods)
            or (key in _automatedMethods.get(GetDatabaseName()))):
                print(f"Running {key}...")
                global _currentTestCase, _startTimestamp
                _currentTestCase = key
                _startTimestamp = GetTimestamp()
                try:
                    globals()[key]()
                except Exception as e:
                    _LogResults(0, 0, 0, str(e).split(";")[0])
                    pass
    _TestingEnd()

# COMMAND ----------

def AssertEquals(inputValue, outputValue):
    Assert(inputValue, outputValue, compare="Equals")
def GreaterThan(inputValue, outputValue):
    Assert(inputValue, outputValue, compare="Greater Than")
def LessThan(inputValue, outputValue):
    Assert(inputValue, outputValue, compare="Less Than")

# COMMAND ----------

def Assert(inputValue, outputValue, compare="Equals", errorMessage=None):
    error = ""
    passed = 0
    try:
        if compare == "Equals":
            assert inputValue == outputValue, f"Expecting value {compare} {outputValue}, got: {inputValue}" if errorMessage is None else errorMessage
        elif compare == "Greater Than":
            assert inputValue > outputValue, f"Expecting value {compare} {outputValue}, got: {inputValue}"
        elif compare == "Less Than":
            assert inputValue < outputValue, f"Expecting value {compare} {outputValue}, got: {inputValue}"
        else:
            raise Exception("Not implemented!")
        passed = 1
    except Exception as e:
        error = str(e)
    _LogResults(inputValue, outputValue, passed, error)

# COMMAND ----------

#LEAVE THIS TO IDENTIFY METHODS THAT BELONG TO COMMON
for key, value in list(globals().items()):
    if callable(value) and value.__module__ == __name__ and not(sys._getframe().f_code.co_name == key):
        _localMethods.append(key)
