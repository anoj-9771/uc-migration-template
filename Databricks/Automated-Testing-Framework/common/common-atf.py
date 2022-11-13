# Databricks notebook source
# MAGIC %run ./common-atf-methods

# COMMAND ----------

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
