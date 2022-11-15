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

def GetTimestamp():
    return spark.sql("SELECT STRING(CURRENT_TIMESTAMP())").collect()[0][0]

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
def GreaterThanEqual(inputValue, outputValue):
    Assert(inputValue, outputValue, compare="Greater Than Equal") 
def LessThan(inputValue, outputValue):
    Assert(inputValue, outputValue, compare="Less Than")

# COMMAND ----------

def TestNotImplemented():
    inputValue = 0
    outputValue = 0
    passed = 1
    comment = "Test not applicable to current table"
    _LogResults(inputValue, outputValue, passed, comment)

# COMMAND ----------

def Assert(inputValue, outputValue, compare="Equals", errorMessage=None):
    error = ""
    passed = 0
    try:
        if compare == "Equals":
            assert inputValue == outputValue, f"Expecting value {compare} {outputValue}, got: {inputValue}" if errorMessage is None else errorMessage
        elif compare == "Greater Than":
            assert inputValue > outputValue, f"Expecting value {compare} {outputValue}, got: {inputValue}"
        elif compare == "Greater Than Equal":
            assert inputValue >= outputValue, f"Expecting value {compare} {outputValue}, got: {inputValue}"    
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

# COMMAND ----------

_automatedMethods = {
    "tests" : []
    ,"cleansed" : ["RowCounts", "ColumnColumns","CountValidateCurrentRecords","CountValidateDeletedRecords","DuplicateCheck","DuplicateActiveCheck","BusinessKeyNullCk","BusinessKeyLengthCk"]
    ,"curated" : ["RowCounts", "DuplicateSK", "DuplicateActiveSK", "MD5ValueSK", "DateValidation","DateValidation1","CountCurrentRecordsC1D0", "CountCurrentRecordsC0D0", "OverlapAndGapValidation", "CountValidateCurrentRecords", "CountValidateDeletedRecords","CountDeletedRecords", "DuplicateCheck", "DuplicateActiveCheck", "ExactDuplicates", "BusinessKeyNullCk", "BusinessKeyLengthCk", "ManBusinessColNullCk", "SourceTargetCountCheckActiveRecords", "SourceMinusTargetCountCheckActiveRecords", "TargetMinusSourceCountCheckActiveRecords", "SourceTargetCountCheckDeletedRecords", "SourceMinusTargetCountCheckDeletedRecords", "TargetMinusSourceCountCheckDeletedRecords", "ColumnColumns","auditEndDateChk","auditCurrentChk","auditActiveOtherChk","auditDeletedChk"] 
    ,"curated_v2" : ["RowCounts","DuplicateSK","DuplicateActiveSK","NullValueChkSK","DateValidation","OverlapTimeStampValidation","OverlapAndGapValidation",
"CountRecordsC1D0","CountRecordsC0D0","CountRecordsC1D1","CountRecordsC0D1","DuplicateCheck","DuplicateActiveCheck","ExactDuplicates",
"BusinessKeyNullCk","BusinessKeyLengthCk","ManBusinessColNullCk","SourceTargetCountCheckC1D0","SourceTargetCountCheckC0D0",
"SourceTargetCountCheckC1D1","SourceTargetCountCheckC0D1","SourceMinusTargetC1D0","TargetMinusSourceC1D0","SourceMinusTargetC0D0","TargetMinusSourceC0D0",
"SourceMinusTargetC1D1","TargetMinusSourceC1D1","SourceMinusTargetC0D1","TargetMinusSourceC0D1","ColumnColumns","auditEndDateChk",
"auditCurrentChk","auditActiveOtherChk","auditDeletedChk" ]
}

# COMMAND ----------

def loadSourceDf(source):
    global columns
    source.createOrReplaceTempView("sourceView")
    sourceDf = spark.sql(f"SELECT {columns} FROM sourceView")
    
    return sourceDf

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

def GetTargetDf():
    global columns
    target = spark.sql(f"""
    select {columns}
    ,_recordStart
    ,_recordEnd
    ,_recordCurrent
    ,_recordDeleted
    from 
    {GetSelfFqn()}
    """) 
    return target

# COMMAND ----------

# This version includes the dummy record when extracting the target table, and source-target queries will fail since target will have an extra record
def loadActiveTargetDf():
    global columns
    global keyColumns
    
    target = GetTargetDf()
    target.createOrReplaceTempView("targetView")
    
    keyColsList = keyColumns.split(",")
    sqlQuery = f"SELECT {columns} FROM targetView WHERE _recordDeleted = 0 " 
        
    #print(sqlQuery)
    targetDf = spark.sql(sqlQuery) 

    return targetDf

# COMMAND ----------

# not implemented, this version is WIP, aim is to exclude dummy record when extracting target table 
def loadActiveTargetDf_test():
    global columns
    global keyColumns
    
    target = GetTargetDf()
    target.createOrReplaceTempView("targetView")
    
    keyColsList = keyColumns.split(",")
    sqlQuery = f"SELECT {columns} FROM targetView WHERE _recordDeleted = 0 and sourceSystemCode is not NULL" # added a condition to ignore the dummy record
    
    if (columns.upper().find('SOURCESYSTEMCODE') != -1):
        sqlQuery = sqlQuery + f"AND sourceSystemCode is not NULL "
    
    for column in keyColsList:
        column = column.strip()
        if (column.upper().find('DATE') == -1 and column.find('SK') == -1):
            sqlQuery = sqlQuery + f"AND {column} <> -1 "
            break
        
    #print(sqlQuery)
    targetDf = spark.sql(sqlQuery) 

    return targetDf

# COMMAND ----------

def loadActiveTargetDf_original(target):
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

def loadInactiveTargetDf():
    global columns
    target = GetTargetDf()
    target.createOrReplaceTempView("targetView")
    targetDf = spark.sql(f"""SELECT {columns} FROM targetView 
                         WHERE _recordDeleted = 1 and _recordCurrent = 0""")
    return targetDf

# COMMAND ----------

def loadInactiveTargetDf(target):
    target.createOrReplaceTempView("targetView")
    targetDf = spark.sql(f"""SELECT {targetColumns} FROM targetView 
                         WHERE _RecordDeleted = 1 and _RecordCurrent = 0""")
    return targetDf

# COMMAND ----------

def loadSourceDfC1D0(source):
    global columns
    source.createOrReplaceTempView("sourceView")
    sourceDf = spark.sql(f"""SELECT {columns}
                         FROM sourceView as a,
                         (Select date(max(_DLCuratedZoneTimeStamp)) as max_date from {GetSelfFqn()}) as b
                         WHERE (date(a._recordStart) <= b.max_date) and  (b.max_date<= date(a._recordEnd))
                         """)
    return sourceDf

# COMMAND ----------

def loadSourceDfC0D0(source):
    global columns
    source.createOrReplaceTempView("sourceView")
    sourceDf = spark.sql(f"""SELECT {columns}
                         FROM sourceView as a,
                         (Select date(max(_DLCuratedZoneTimeStamp)) as max_date from {GetSelfFqn()}) as b
                         WHERE ((date(a._recordStart) < b.max_date) and (b.max_date > date(a._recordEnd))) or ((date(a._recordStart) > b.max_date) and (b.max_date < date(a._recordEnd)))
                         """)
    return sourceDf

# COMMAND ----------

def loadSourceDfC0D1(source):
    global columns
    source.createOrReplaceTempView("sourceView")
    sourceDf = spark.sql(f"SELECT {columns} \
                         FROM sourceView as a, \
                         (Select date(max(_DLCuratedZoneTimeStamp)) as max_date from {GetSelfFqn()}) as b\
                         WHERE ((date(a._recordStart) < b.max_date) and (b.max_date > date(a._recordEnd))) or ((date(a._recordStart) > b.max_date) and (b.max_date < date(a._recordEnd)))\
                         ")
    return sourceDf

# COMMAND ----------

def loadSourceDfC1D1(source):
    global columns
    source.createOrReplaceTempView("sourceView")
    sourceDf = spark.sql(f"""SELECT {columns}
                         FROM sourceView as a,
                         (Select date(max(_DLCuratedZoneTimeStamp)) as max_date from {GetSelfFqn()}) as b
                         WHERE (date(a._recordStart) <= b.max_date) and  (b.max_date<= date(a._recordEnd))
                         """)
    return sourceDf

# COMMAND ----------

def loadTargetDfC1D0():
    global columns
    global keyColumns
    
    target = GetTargetDf()
    target.createOrReplaceTempView("targetView")
    
    keyColsList = keyColumns.split(",")
    sqlQuery = f"SELECT {columns} FROM targetView WHERE _recordCurrent = 1 and _recordDeleted = 0 and sourceSystemCode is not NULL" # added a condition to ignore the dummy record 
        
    #print(sqlQuery)
    targetDf = spark.sql(sqlQuery) 

    return targetDf

# COMMAND ----------

def loadTargetDfC0D0():
    global columns
    global keyColumns
    
    target = GetTargetDf()
    target.createOrReplaceTempView("targetView")
    
    keyColsList = keyColumns.split(",")
    sqlQuery = f"SELECT {columns} FROM targetView WHERE _recordCurrent = 0 and _recordDeleted = 0 and sourceSystemCode is not NULL" # added a condition to ignore the dummy record 
        
    #print(sqlQuery)
    targetDf = spark.sql(sqlQuery) 

    return targetDf

# COMMAND ----------

def loadTargetDfC0D1():
    global columns
    global keyColumns
    
    target = GetTargetDf()
    target.createOrReplaceTempView("targetView")
    
    keyColsList = keyColumns.split(",")
    sqlQuery = f"SELECT {columns} FROM targetView WHERE _recordCurrent = 0 and _recordDeleted = 1 and sourceSystemCode is not NULL" # added a condition to ignore the dummy record 
        
    #print(sqlQuery)
    targetDf = spark.sql(sqlQuery) 

    return targetDf

# COMMAND ----------

def loadTargetDfC1D1():
    global columns
    global keyColumns
    
    target = GetTargetDf()
    target.createOrReplaceTempView("targetView")
    
    keyColsList = keyColumns.split(",")
    sqlQuery = f"SELECT {columns} FROM targetView WHERE _recordCurrent = 1 and _recordDeleted = 1 and sourceSystemCode is not NULL" # added a condition to ignore the dummy record 
        
    #print(sqlQuery)
    targetDf = spark.sql(sqlQuery) 

    return targetDf

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

def NullValueChkSK():
    skColumn = GetSelf().columns[0]
    table = GetNotebookName()
    df = spark.sql(f"SELECT COUNT (*) as recCount FROM {GetSelfFqn()} WHERE {skColumn} is null or {skColumn} = '' or {skColumn} = ' ' or upper({skColumn}) ='NULL'")
    count = df.select("recCount").collect()[0][0]
    Assert(count, 0)

# COMMAND ----------

# date validation automated function
def DateValidation1():
    table = GetNotebookName()
    df = spark.sql(f"SELECT * FROM {GetSelfFqn()} \
                WHERE date(_recordStart) > date(_recordEnd)")
    count = df.count()
    Assert(count, 0)

# COMMAND ----------

# Date validation for _RecordStart and _RecordEnd
def DateValidation():
    table = GetNotebookName()
    df = spark.sql(f"SELECT * FROM {GetSelfFqn()} \
                WHERE date(_recordStart) > date(_recordEnd)")
    count = df.count()
    if count > 0: display(df)
    Assert(count, 0, errorMessage = "Failed date validation for _RecordStart and _RecordEnd")

# COMMAND ----------

# Date validation for validToDate and validFromDate
# not implemented yet
def DateValidation2():
    table = spark.sql(f"SELECT * FROM {GetSelfFqn()} ")
    columns = table.columns
    validToDateFlag = 0
    validFromDateFlag = 0
    
    for column in columns:
        column = column.upper()
        if column == 'VALIDTODATE': 
            validToDateFlag = 1
        if column == 'VALIDFROMDATE':
            validFromDateFlag = 1
            
    if validToDateFlag == 1 & validFromDateFlag == 1:
        df = spark.sql(f"SELECT * FROM {GetSelfFqn()} \
                        WHERE validFromDate > validToDate")
    
        count = df.count()
        if count > 0: display(df)
        Assert(count, 0, errorMessage = "Failed date validation for validToDate and validFromDate")
    
    else:
        TestNotImplemented()

# COMMAND ----------

# Date validation for _RecordStart = validToDate and _RecordEnd = validFromDate
# not implemented yet
def DateValidation3():
    table = spark.sql(f"SELECT * FROM {GetSelfFqn()} ")
    columns = table.columns
    validToDateFlag = 0
    validFromDateFlag = 0
    
    for column in columns:
        column = column.upper()
        if column == 'VALIDTODATE': 
            validToDateFlag = 1
        if column == 'VALIDFROMDATE':
            validFromDateFlag = 1
            
    if validToDateFlag == 1 & validFromDateFlag == 1:
        df = spark.sql(f"SELECT * FROM {GetSelfFqn()} \
                        WHERE (date(_recordStart) <> ValidFromDate) \
                        OR (date(_recordEnd)<>ValidToDate)")
        count = df.count()
        if count > 0: display(df)
        Assert(count, 0, errorMessage = f"Failed Date validation for _RecordStart = validToDate and _RecordEnd = validFromDate")
    
    else:
        TestNotImplemented()

# COMMAND ----------

def OverlapTimeStampValidation():
    global keyColumns
    df = spark.sql(f"Select {keyColumns}, start_datetm, end_datetm \
                   from (Select {keyColumns}, _recordStart as start_datetm, _recordEnd as end_datetm, \
                   max(_recordStart) over (partition by {keyColumns} order by _recordStart rows between 1 following and 1 following) as nxt_datetm \
                   from {GetSelfFqn()} where _recordDeleted=0) \
                   where  DATEDIFF(second, nxt_datetm, end_datetm) <> 1") # can check for <> 9999-12-31
    count = df.count()
    if count > 0: display(df)
    Assert(count, 0, errorMessage = "Failed - overlap in the timestamp")

# COMMAND ----------

def OverlapAndGapValidation():
    global keyColumns
    df = spark.sql(f"Select {keyColumns}, start_date, end_date \
                   from (Select {keyColumns}, date(_recordStart) as start_date, date(_recordEnd) as end_date, \
                   max(date(_recordStart)) over (partition by {keyColumns} order by _recordStart rows between 1 following and 1 following) as nxt_date \
                   from {GetSelfFqn()} where _recordDeleted=0) \
                   where  DATEDIFF(day, nxt_date, end_date) <> 1")  ## added _recordDeleted=0 to pick non deleted records.
    count = df.count()
    if count > 0: display(df)
    Assert(count, 0)

# COMMAND ----------

def CountRecordsC1D0():
    df = spark.sql(f"select count(*) as recCount from {GetSelfFqn()} \
                WHERE _RecordCurrent=1 and _RecordDeleted=0")
    count = df.select("recCount").collect()[0][0]
    GreaterThan(count,0)

# COMMAND ----------

def CountRecordsC0D0():
    df = spark.sql(f"select count(*) as recCount from {GetSelfFqn()} \
                WHERE _RecordCurrent=0 and _RecordDeleted=0")
    count = df.select("recCount").collect()[0][0]
    GreaterThanEqual(count,0)

# COMMAND ----------

def CountRecordsC1D1():
    df = spark.sql(f"select count(*) as recCount from {GetSelfFqn()} \
                WHERE _RecordCurrent=1 and _RecordDeleted=1")
    count = df.select("recCount").collect()[0][0]
    GreaterThanEqual(count,0)

# COMMAND ----------

def CountRecordsC0D1():
    df = spark.sql(f"select count(*) as recCount from {GetSelfFqn()} \
                WHERE _RecordCurrent=0 and _RecordDeleted=1")
    count = df.select("recCount").collect()[0][0]
    GreaterThanEqual(count,0)

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

def CountDeletedRecords():
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

def SourceTargetCountCheckC1D0():
    global source_a
    #sourceDf = loadActiveSourceDf(source)
    sourceDf=loadSourceDfC1D0(source_a)
    
    targetDf = loadTargetDfC1D0()
    
    sourceCount = sourceDf.count()
    targetCount = targetDf.count()
    
    Assert(sourceCount, targetCount)

# COMMAND ----------

def SourceTargetCountCheckC0D0():
    global source_a
    #sourceDf = loadActiveSourceDf(source)
    sourceDf=loadSourceDfC0D0(source_a)
    
    targetDf = loadTargetDfC0D0()
    
    sourceCount = sourceDf.count()
    targetCount = targetDf.count()
    
    Assert(sourceCount, targetCount)

# COMMAND ----------

def SourceTargetCountCheckC1D1():
    global source_d
    #sourceDf = loadActiveSourceDf(source)
    sourceDf=loadSourceDfC1D1(source_d)
    
    targetDf = loadTargetDfC1D1()
    
    sourceCount = sourceDf.count()
    targetCount = targetDf.count()
    
    Assert(sourceCount, targetCount)

# COMMAND ----------

def SourceTargetCountCheckC0D1():
    global source_d
    #sourceDf = loadActiveSourceDf(source)
    sourceDf=loadSourceDfC0D1(source_d)
    
    targetDf = loadTargetDfC0D1()
    
    sourceCount = sourceDf.count()
    targetCount = targetDf.count()
    
    Assert(sourceCount, targetCount)

# COMMAND ----------

def SourceMinusTargetC1D0():
    global source_a
    sourceDf = loadSourceDfC1D0(source_a)
    
    targetDf = loadTargetDfC1D0()
    
    df = sourceDf.subtract(targetDf)
    count = df.count()
    if count > 0: display(df)
    
    Assert(count, 0)

# COMMAND ----------

def TargetMinusSourceC1D0():
    global source_a
    sourceDf = loadSourceDfC1D0(source_a)
    
    targetDf = loadTargetDfC1D0()
    
    df = targetDf.subtract(sourceDf)
    count = df.count()
    if count > 0: display(df)
    
    Assert(count, 0) 

# COMMAND ----------

def SourceMinusTargetC0D0():
    global source_a
    sourceDf = loadSourceDfC0D0(source_a)
    
    targetDf = loadTargetDfC0D0()
    
    df = sourceDf.subtract(targetDf)
    count = df.count()
    if count > 0: display(df)
    
    Assert(count, 0)

# COMMAND ----------

def TargetMinusSourceC0D0():
    global source_a
    sourceDf = loadSourceDfC0D0(source_a)
    
    targetDf = loadTargetDfC0D0()
    
    df = targetDf.subtract(sourceDf)
    count = df.count()
    if count > 0: display(df)
    
    Assert(count, 0) 

# COMMAND ----------

def SourceMinusTargetC1D1():
    global source_d
    sourceDf = loadSourceDfC1D1(source_d)
    
    targetDf = loadTargetDfC1D1()
    
    df = sourceDf.subtract(targetDf)
    count = df.count()
    if count > 0: display(df)
    
    Assert(count, 0)

# COMMAND ----------

def TargetMinusSourceC1D1():
    global source_d
    sourceDf = loadSourceDfC1D1(source_d)
    
    targetDf = loadTargetDfC1D1()
    
    df = targetDf.subtract(sourceDf)
    count = df.count()
    if count > 0: display(df)
    
    Assert(count, 0) 

# COMMAND ----------

def SourceMinusTargetC0D1():
    global source_d
    sourceDf = loadSourceDfC0D1(source_d)
    
    targetDf = loadTargetDfC0D1()
    
    df = sourceDf.subtract(targetDf)
    count = df.count()
    if count > 0: display(df)
    
    Assert(count, 0)

# COMMAND ----------

def TargetMinusSourceC0D1():
    global source_d
    sourceDf = loadSourceDfC0D1(source_d)
    
    targetDf = loadTargetDfC0D1()
    
    df = targetDf.subtract(sourceDf)
    count = df.count()
    if count > 0: display(df)
    
    Assert(count, 0) 

# COMMAND ----------

def ColumnColumns():
    count = len(GetSelf().columns)
    Assert(count,count)

# COMMAND ----------

def auditEndDateChk():
    global keyColumns
    df = spark.sql(f"Select * from {GetSelfFqn()} \
                     where date(_RecordEnd) < (select date(max(_DLCuratedZoneTimeStamp)) as max_date from {GetSelfFqn()}) \
                     and _recordCurrent=1 and _recordDeleted=0")
    count = df.count()
    if count > 0: display(df)
    Assert(count,0)

# COMMAND ----------

def auditCurrentChk():
    global keyColumns
    table = GetNotebookName()
    df = spark.sql(f"select * from (\
                   select * from (   \
                   SELECT {keyColumns},date(_RecordStart) as start_dt ,date(_RecordEnd) as end_dt ,_RecordCurrent,_RecordDeleted,max_date from {GetSelfFqn()} as a, \
                       (Select date(max(_DLCuratedZoneTimeStamp)) as max_date from {GetSelfFqn()}) as b\
                       )where ((max_date < end_dt) and (max_date > start_dt))\
                       )where _RecordCurrent <> 1 and _RecordDeleted <> 0\
                   ")
    count = df.count()
    if count > 0: display(df)
    Assert(count,0)

# COMMAND ----------

def auditActiveOtherChk():
    global keyColumns
    table = GetNotebookName()
    df = spark.sql(f"select * from (\
                   select * from (   \
                   SELECT {keyColumns},date(_RecordStart) as start_dt ,date(_RecordEnd) as end_dt ,_RecordCurrent,_RecordDeleted,max_date from {GetSelfFqn()} as a, \
                       (Select date(max(_DLCuratedZoneTimeStamp)) as max_date from {GetSelfFqn()}) as b\
                       )where ((max_date > end_dt) and (max_date < start_dt))\
                       )where _RecordCurrent <> 0 and _RecordDeleted <> 0\
                   ")
    count = df.count()
    if count > 0: display(df)
    Assert(count,0)

# COMMAND ----------

def auditDeletedChk():
    global keyColumns
    table = GetNotebookName()
    df = spark.sql(f"select * from (\
                    select * from (   \
                    SELECT {keyColumns},date(_RecordStart) as start_dt ,date(_RecordEnd) as end_dt ,_RecordCurrent,_RecordDeleted,max_date from {GetSelfFqn()} as a, \
                       (Select date(max(_DLCuratedZoneTimeStamp)) as max_date from {GetSelfFqn()}) as b\
                       )where ((max_date < end_dt) and (max_date > start_dt)) and (end_dt<> '9999-12-31')\
                        )where _RecordCurrent = 1 and _RecordDeleted = 1 \
                   ")
    count = df.count()
    if count > 0: display(df)
    Assert(count,0)
