# Databricks notebook source
# DBTITLE 0,Note: Please call ClearCache() after running the ATF Script
# MAGIC %run ./common-atf-methods-unity

# COMMAND ----------

# MAGIC %run ./extensions/helper_functions-unity

# COMMAND ----------

# MAGIC %run /MDP-Framework/Common/common-unity-catalog-helper

# COMMAND ----------

# MAGIC %md
# MAGIC ####Change log from original ATF:
# MAGIC - GetNotebookName() renamed to GetTableName(), utilises GetTable(), and will return tablename regardless of whether 2 part or 3 part namespace is used
# MAGIC - GetDatabaseName() utilises GetSchema(), returns the source system or the schema (e.g. maximo, IICATS, dim, fact, brg etc.), previously would return raw/cleansed/curated
# MAGIC     - NOTE: GetSchema from common-unity-catalog-helper may need updating for GetDatabaseName()
# MAGIC - Added GetCatalogName(), returns prefix\_catalog (e.g dev\_cleansed, test\_curated etc.)
# MAGIC

# COMMAND ----------

TABLE_FQN = ''
LOAD_MAPPING_FLAG = False

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

# DEBUG = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())["rootRunId"] is None
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

def GetCatalogName():
    prefix = GetPrefix()
    if not TABLE_FQN:
        list = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split("/")
        count=len(list)
        lvl2dir, lvl3dir = list[count-2], list[count-3]
        # if not(any([i for i in ["raw", "cleansed", "curated"] if lvl2dir.lower().startswith(i)])):
        if not(any([i for i in ["raw", "cleansed", "curated"] if i in lvl2dir.lower()])):
            c = lvl3dir
        else:
            c = lvl2dir
        catalog = prefix+c
    else:
        catalog = GetCatalog(TABLE_FQN)
    return catalog

# COMMAND ----------

def GetDatabaseName():
    if not TABLE_FQN:
        list = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split("/")
        count=len(list)
        lvl2dir = list[count-2]
        if (any([i for i in ["dim", "fact", "brg"] if i in lvl2dir.lower()])):
            tbName = f"{GetCatalogName()}.{list[count-2]}.{list[count-1]}" 
        else:
            tbName = f"{list[count-2]}.{list[count-1]}" 
        database = GetSchema(tbName)
    else:
        database = GetSchema(TABLE_FQN)
    return database

# COMMAND ----------

def GetTableName():
    if not TABLE_FQN:
        list = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split("/")
        count=len(list)
        notebookName = list[count-1]
        lvl2dir = list[count-2]
        if not(any([i for i in ["raw", "cleansed", "curated"] if i in lvl2dir.lower()])):
            tableName = notebookName
        else:
            tableName = GetTable(notebookName)
    else:
        tableName = GetTable(TABLE_FQN)
    return tableName

# COMMAND ----------

# Plan for now:
#     Nikita - Update ATF script for 3 part namespace, update testing notebooks for my source systems
#     Stephen and Aparna - Update testing notebooks for their assignment source systems

#     7th - run current test scripts in test env, capture results
#     8th and 9th - run current test scripts in preprod env and capture results, give scripts to kelvin for prod, run new UC updated scripts in test env and compare results

# COMMAND ----------

def GetSelfFqn():
    if not TABLE_FQN:
        table_fqn = f"{GetCatalogName()}.{GetDatabaseName()}.{GetTableName()}" 
    else:
        table_fqn = ConvertTableName( TABLE_FQN )
    return table_fqn

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

# def _TestingEnd():
#     r = json.dumps( {
#             "BatchId" : DEFAULT_BATCH_ID
#             ,"run" : json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
#             ,"results" : _results
#         }
#     )
#     #dbutils.fs.put(f"/mnt/atf/run/{GetTableName()}.json", json.dumps({"results" : _results }, indent=3), True)
#     #print(json.dumps({"results" : _results }, indent=3)) if DEBUG == 1 else dbutils.notebook.exit(r)
#     DisplayJsonResults({"results" : _results }) if DEBUG == 1 else dbutils.notebook.exit(r)

# COMMAND ----------

def GetTimestamp():
    return spark.sql("SELECT STRING(CURRENT_TIMESTAMP())").collect()[0][0]

# COMMAND ----------

def _LogResults(inputValue : str, outputValue : str, passed : str, error : str):
     _results.append({
        "Object" : f"{GetSelfFqn()}",
        "Type" : "Automated" if _currentTestCase in _automatedMethods[GetCatalogName()] else "Manual",
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
            or (key in _automatedMethods.get(GetCatalogName()))):
                methods.append(key)
    return methods

# COMMAND ----------

def RunTests():
    methods = GetTestMethodsToRun()
    if len(methods) == 0:
        print("No tests!")
        return
    
    global MAPPING_DOC, TAG_SHEET, LOAD_MAPPING_FLAG, UNIQUE_KEYS, MANDATORY_COLS, ALL_COLS, SRC_DF, TGT_DF
    if not MAPPING_DOC and LOAD_MAPPING_FLAG == False:
        if GetCatalogName().upper().endswith('CLEANSED'):
            try:
                MAPPING_DOC = loadCleansedMapping().cache()
                TAG_SHEET = loadCleansedMapping('TAG').cache()
                TAG_SHEET.count()
                
            except:
                MAPPING_DOC = loadCleansedMapping().cache()
        else:
            MAPPING_DOC = loadCuratedMapping().cache()
        MAPPING_DOC.count()
        LOAD_MAPPING_FLAG = True
        # MAPPING_DOC.display()
        
    UNIQUE_KEYS = GetUniqueKeys()
    
    if not GetCatalogName().upper().endswith('CLEANSED'):
        MANDATORY_COLS = GetMandatoryCols()
        ALL_COLS = GetColumns()
    
    if DO_ST_TESTS == True: 
        if GetCatalogName().upper().endswith('CLEANSED'):
            SRC_DF, TGT_DF = GetSrcTgtDfs(TABLE_FQN)
        else: # curated table - S-T all recs
            SRC_DF = spark.sql(SOURCE_QUERY)
            SRC_DF.createOrReplaceTempView("sourceView")
            SRC_DF = spark.sql(f"SELECT {ALL_COLS} FROM sourceView")
            TGT_DF = spark.sql(AutomatedTargetQuery())
        SRC_DF.cache()
        TGT_DF.cache()
        SRC_DF.count()
        TGT_DF.count()
        print(SRC_DF.count())
        print(TGT_DF.count())
        SRC_DF.display()
        TGT_DF.display()
    
    # for key, value in globals().items():
    #     if ((callable(value) 
    #         and value.__module__ == __name__ 
    #         and not(sys._getframe().f_code.co_name == key) 
    #         and key not in _localMethods)
    #         or (key in _automatedMethods.get(GetCatalogName()))):
    #             print(f"Running {key}...")
    #             global _currentTestCase, _startTimestamp
    #             _currentTestCase = key
    #             _startTimestamp = GetTimestamp()
    #             try:
    #                 globals()[key]()
    #             except Exception as e:
    #                 _LogResults(0, 0, 0, str(e).split(";")[0])
    #                 pass
    
    # UNIQUE_KEYS = ''
    # MANDATORY_COLS = ''
    # ALL_COLS = ''
    # if DO_ST_TESTS == True and GetCatalogName().upper().endswith('CLEANSED'):
    #     SRC_DF.unpersist()
    #     TGT_DF.unpersist()
    
    # _TestingEnd()
    del _results[0:len(_results)]

# COMMAND ----------

def ClearCache():
    global MAPPING_DOC, TAG_SHEET
    try:
        MAPPING_DOC.unpersist()
    except:
        pass
    try:
        TAG_SHEET.unpersist()
    except:
        pass

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
            assert inputValue > outputValue, f"Expecting value {compare} {outputValue}, got: {inputValue}" if errorMessage is None else errorMessage
        elif compare == "Greater Than Equal":
            assert inputValue >= outputValue, f"Expecting value {compare} {outputValue}, got: {inputValue}" if errorMessage is None else errorMessage   
        elif compare == "Less Than":
            assert inputValue < outputValue, f"Expecting value {compare} {outputValue}, got: {inputValue}" if errorMessage is None else errorMessage
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
