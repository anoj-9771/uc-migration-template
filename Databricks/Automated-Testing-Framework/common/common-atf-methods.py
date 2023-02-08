# Databricks notebook source
# DBTITLE 1,Note: Please call ClearCache() after running the ATF Script
_automatedMethods = {
    "tests": [],
    "cleansed": [
        "CleansedSchemaChk"
        ,"DuplicateKeysChk"
        ,"KeysNotNullOrBlankChk"
        ,"AuditColsIncludedChk"
        ,"CountRecordsC1D0" 
        ,"CountRecordsC0D1" 
        ,"SrcTgtCountChk"
        ,"RowCounts"
        ,"ColumnCounts"
        ,"CleansedSrcTgtCountChk"
        ,"CleansedSrcMinusTgtChk"
        ,"CleansedTgtMinusSrcChk"
    ],
    "curated": [
        "DummyRecChk"
        ,"DuplicateKeysChk"
        ,"DuplicateKeysActiveRecsChk"
        ,"KeysNotNullOrBlankChk"
        ,"KeysAreSameLengthChk"
        ,"DuplicateSKChk"
        ,"DuplicateSKActiveRecsChk"
        ,"SKNotNullOrBlankChk"
        ,"DateValidation"
        ,"AuditColsIncludedChk"
        ,"AuditEndDateChk"
        ,"AuditCurrentChk"
        ,"AuditActiveOtherChk"
        ,"AuditDeletedChk"
        ,"CountRecordsC1D0" 
        ,"CountRecordsC0D0"
        ,"CountRecordsC1D1" 
        ,"CountRecordsC0D1"
        ,"SrcTgtCountChk"
        ,"RowCounts"
        ,"ColumnCounts"
    ],
    "curated_v2": [
    ]
}

#COPY THE AUTOMATED TESTS
_automatedMethods["curated_v2"] = _automatedMethods["curated"]

DOC_PATH = ''
SHEET_NAME = ''
UNIQUE_KEYS = ''
MAPPING_DOC = ''
TAG_SHEET = ''

# COMMAND ----------

# DBTITLE 1,Helper Functions
def ascii_ignore(x):
    return x.encode('ascii', 'ignore').decode('ascii')
ascii_udf = udf(ascii_ignore)

# COMMAND ----------

def TrimWhitespace(df):
    columns = [
        "CleansedColumnName", 
        "DataType"
    ]
    for column in columns:
        df = df.withColumn(column, regexp_replace(column, r"^\s+|\s+$", ""))
        df = df.withColumn(column, ascii_udf(column))
    return df 

# COMMAND ----------

def loadMappingDocument(sheetName = ""):
    if sheetName == "":
        sheetName = SHEET_NAME
        
    mappingData = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("dataAddress", f"{sheetName}!A1") \
    .load(f"{DOC_PATH}")
    
    if sheetName == SHEET_NAME:
        mappingData = mappingData.selectExpr(
                                     "UPPER(UniqueKey) as UniqueKey",
                                     "UPPER(DataType) as DataType",
                                     "RawTable",
                                     "UPPER(CleansedTable) as CleansedTable",
                                     "RawColumnName",
                                     "CleansedColumnName",
                                     "TransformTag",
                                     "CustomTransform"        
                      ).filter(mappingData.CleansedTable.isNotNull())
        mappingData = TrimWhitespace(mappingData)
    elif sheetName == 'TAG':
        mappingData = mappingData.selectExpr(
                                 "`Tag Name` as TagName",
                                 "`SQL syntax` as SQLsyntax"
                      )
    return mappingData

# COMMAND ----------

def GetTargetSchemaDf():
    target = spark.sql(f"Select * from {GetSelfFqn()}")
    tgtSchema = target._jdf.schema().treeString()
    tgtSchema = tgtSchema.strip(" rot").strip().splitlines()
    tgtSchemaList = []

    for line in tgtSchema:
        line = line.replace('nullable = ', "")
        
        filterChars = '|-:='
        for char in filterChars:
            line = line.replace(char, "")
        
        line = line.replace('(f', "f").replace('(t', "t").strip(')').strip()

        if line.startswith("_") == True:
            continue

        row = tuple(map(str, line.split()))
        tgtSchemaList.append(row)
        
    
    dfStructure = StructType([StructField('CleansedColumnName', StringType()),
                              StructField('DataType', StringType()),
                              StructField('UniqueKey', StringType())])

    targetDf = spark.createDataFrame(tgtSchemaList, dfStructure)
    targetDf.createOrReplaceTempView("tableView")
    targetDf = spark.sql("""
        Select 
        CleansedColumnName, 
        UPPER(DataType) as DataType,
        case when UniqueKey = 'false' then 'Y' else null end as UniqueKey
        from tableView
    """)
    return targetDf

# COMMAND ----------

def GetMappingSchemaDf():
    mappingData = MAPPING_DOC
    mappingData.createOrReplaceTempView("mappingView")
    
    mappingDf = spark.sql(f"""
        Select 
        CleansedColumnName, 
        case 
            when DataType = 'DATETIME' then 'TIMESTAMP'
            when DataType = 'TIME' then 'TIMESTAMP'
            when DataType = 'BIGINT' then 'LONG'
            when DataType like "%CHAR%" then 'STRING'
            when DataType like "INT%" then 'INTEGER'
            when DataType like "DAT%" then 'DATE'
            else DataType
        end as DataType,
        case 
            when trim(UniqueKey) = '' then Null
            when trim(UniqueKey) = ' ' then Null
            else UniqueKey
        end as UniqueKey
        from mappingView
        where CleansedTable = UPPER("{GetSelfFqn()}")
    """)
    return mappingDf

# COMMAND ----------

def GetUniqueKeys():
    mappingData = MAPPING_DOC
    mappingData.createOrReplaceTempView("mappingView")
    
    df = spark.sql(f"""
        Select 
        CleansedColumnName
        from mappingView
        where CleansedTable = UPPER("{GetSelfFqn()}")
        and UniqueKey = 'Y'
    """)
    
    keys = ""
    for row in df.collect():
        keys = keys + row.CleansedColumnName + ",\n"                
    keys = keys.strip().strip(',')
    
    return keys

# COMMAND ----------

def AutomatedSourceQuery(tablename = ""):
    if tablename == "":
        tablename = GetSelfFqn()

    mappingData = MAPPING_DOC
    tags = TAG_SHEET
    #lookup = LOOKUP_SHEET

    mappingData.createOrReplaceTempView("mappingView")
    tags.createOrReplaceTempView("tagView")
    #lookup.createOrReplaceTempView("lookupView")
    
    df = spark.sql(f"""
        Select 
        DataType,
        TransformTag,
        CustomTransform,
        RawColumnName,
        CleansedColumnName,
        RawTable
        from mappingView
        where CleansedTable = UPPER("{tablename}")
    """)
    
    sourceSystem = GetNotebookName().split("_")[0]
    sqlQuery = "SELECT \n"
    for row in df.collect():
        if row.TransformTag is not None:
            sqlCode = spark.sql(f"Select * from tagView where TagName = '{row.TransformTag}'") \
                            .select("SQLsyntax").collect()[0][0] 
            if row.TransformTag.upper() == 'TRIMCOALESCE' and row.DataType == 'DATETIME':
                sqlCode = sqlCode.replace("$c$", f"CAST(a.{row.RawColumnName} AS TIMESTAMP)") 
            elif row.TransformTag.upper() == 'TRIMCOALESCE':
                sqlCode = sqlCode.replace("$c$", f"CAST(a.{row.RawColumnName} AS {row.DataType})") 
            else: 
                sqlCode = sqlCode.replace("$c$", f"a.{row.RawColumnName}") 
        elif row.CustomTransform is not None:
            sqlCode = row.CustomTransform
        elif row.DataType == 'DATETIME' or row.DataType == 'TIME':
            sqlCode = f"CAST(a.{row.RawColumnName} AS TIMESTAMP)"
        else:
            sqlCode = f"CAST(a.{row.RawColumnName} AS {row.DataType})"
        
        sqlCode = sqlCode + " as " + row.CleansedColumnName + ",\n"
        
        sqlQuery = sqlQuery + sqlCode
    
    sqlQuery = sqlQuery.strip().strip(',')
    
    rawTable = df.select("RawTable").collect()[0][0]
    sqlQuery = sqlQuery + "\n" + f"from {rawTable} a\n"
    
    return sqlQuery

# COMMAND ----------

def AutomatedTargetQuery(tablename = ""):
    if tablename == "":
        tablename = GetSelfFqn()
        
    mappingData = MAPPING_DOC
    mappingData.createOrReplaceTempView("mappingView")
    
    df = spark.sql(f"""
        Select 
        DataType,
        CleansedColumnName
        from mappingView
        where CleansedTable = UPPER("{tablename}")
    """)
    
    sqlCode = ""
    for row in df.collect():
        sqlCode = sqlCode + row.CleansedColumnName + ",\n"    
            
    sqlCode = sqlCode.strip().strip(',')
    sqlQuery = "Select \n" + f"{sqlCode} \n" + f"from {tablename}"
    
    return sqlQuery

# COMMAND ----------

def GetSrcTgtDfs(tablename):
    srcQuery = AutomatedSourceQuery(tablename)
    tgtQuery = AutomatedTargetQuery(tablename)
    sourceDf = spark.sql(srcQuery).drop_duplicates()
    targetDf = spark.sql(tgtQuery)
    return sourceDf, targetDf

# COMMAND ----------

# DBTITLE 1,Common Test Cases - Cleansed + Curated
# Mapping doc path: 'dbfs:/mnt/data/mapping_documents_UC3/<DOC_PATH>'
# Schema Check can currently only handle Cleansed tables
def CleansedSchemaChk():
    targetDf = GetTargetSchemaDf()
    
    mappingDf = GetMappingSchemaDf()
    
    diff1 = targetDf.subtract(mappingDf)
    count = diff1.count()
    diff2 = mappingDf.subtract(targetDf)
    if diff2.count() > count: count = diff2.count()
    if count > 0: 
        print('Target Schema:')
        display(targetDf)
        print('Mapping Schema:')
        display(mappingDf)
        print('Mismatching fields - Columns in Target that are not in Mapping (Target-Mapping):')
        display(diff1)
        print('Mismatching fields - Columns in Mapping that are not in Target (Mapping-Target):')
        display(diff2)

    Assert(count, 0) 

# COMMAND ----------

def DuplicateKeysChk():
    keyColumns = UNIQUE_KEYS
    if keyColumns.strip() == "":
        Assert(0, 0, compare = "Greater Than", errorMessage = f"Test could not be executed, No Unique Key column/s were specified in the mapping.")
    else:
        if GetDatabaseName().upper() == 'CURATED' or GetDatabaseName().upper() == "CURATED_V2":
            keyColumns = keyColumns + ', _RecordStart'
        df = spark.sql(f"""SELECT {keyColumns}, COUNT(*) as RecCount FROM {GetSelfFqn()} 
                           GROUP BY {keyColumns} HAVING COUNT(*) > 1""")
        count = df.count()
        if count > 0: display(df)
        Assert(count,0)

# COMMAND ----------

# DBTITLE 0,Untitled
def KeysNotNullOrBlankChk():
    keyColumns = UNIQUE_KEYS
    if keyColumns.strip() == "":
        Assert(0, 0, compare = "Greater Than", errorMessage = f"Test could not be executed, No Unique Key column/s were specified in the mapping.")
    else:
        keyColsList = keyColumns.split(",")
        firstColumn = keyColsList[0].strip()
        sqlQuery = f"SELECT * FROM {GetSelfFqn()} "

        for column in keyColsList:
            column = column.strip()
            if column == firstColumn:
                sqlQuery = sqlQuery + f"WHERE ({column} is NULL or {column} in ('',' ') or UPPER({column})='NULL') "
            else:
                sqlQuery = sqlQuery + f"OR ({column} is NULL or {column} in ('',' ') or UPPER({column})='NULL') "

        df = spark.sql(sqlQuery)
        count = df.count()
        if count > 0: display(df)
        Assert(count,0)

# COMMAND ----------

def AuditColsIncludedChk():
    df = spark.table(GetSelfFqn())
    auditCols = ['_RecordStart', '_RecordEnd', '_RecordCurrent', '_RecordDeleted', '_DLCleansedZoneTimeStamp']
    targetAuditCols = []
    for column in df.columns:
        if column.startswith("_") == True:
            targetAuditCols.append(column)
    
    auditColsNotInTarget = [x for x in auditCols if x not in targetAuditCols]
    
    Assert(len(auditColsNotInTarget), 0, errorMessage = f"Missing audit column(s): {auditColsNotInTarget}")

# COMMAND ----------

def CountRecordsCnDn(C, D):
    try:
        df = spark.sql(f"""SELECT count(*) as RecCount FROM {GetSelfFqn()} 
                       WHERE _RecordCurrent = {C} and _RecordDeleted = {D}""")
        count = df.select("RecCount").collect()[0][0]

        if C == 1 and D == 0:
            GreaterThan(count,0)
        else:
            GreaterThanEqual(count,0)
    except:
        Assert(0, 0, compare = "Greater Than", errorMessage = f"Test could not be executed, columns '_RecordCurrent' and/or '_RecordDeleted' were not found in Target table.")

# COMMAND ----------

def CountRecordsC1D0():
    CountRecordsCnDn(1, 0)
def CountRecordsC0D0():
    CountRecordsCnDn(0, 0)
def CountRecordsC1D1():
    CountRecordsCnDn(1, 1)
def CountRecordsC0D1():
    CountRecordsCnDn(0, 1)

# COMMAND ----------

def SrcTgtCountChk():
    if DO_ST_TESTS == False:
        if GetDatabaseName().upper() == 'CURATED' or GetDatabaseName().upper() == "CURATED_V2":
            source = 'cleansed'
        else:
            source = 'raw'

        tablename = GetSelfFqn().split(".")[1]

        sourceDf = spark.table(f"{source}.{tablename}").drop_duplicates()
        targetDf = spark.table(f"{GetSelfFqn()}")

        sourceCount = sourceDf.count()
        targetCount = targetDf.count()

        Assert(sourceCount, targetCount, errorMessage = f"""Record counts are not matching - Source contains {sourceCount} records, Target contains {targetCount} records. Note: If records in Target have been filtered from Source using a watermark column (e.g. rowStamp, di_Sequence_Number etc), this test case may be ignored.""")

# COMMAND ----------

def RowCounts():
    count = GetSelf().count()
    GreaterThan(count, 0)

# COMMAND ----------

def ColumnCounts():
    count = len(GetSelf().columns)
    Assert(count,count)

# COMMAND ----------

# DBTITLE 1,Cleansed Test Cases
def CleansedSrcTgtCountChk():
    if DO_ST_TESTS == True:
        sourceDf, targetDf = SRC_DF, TGT_DF

        sourceCount = sourceDf.count()
        targetCount = targetDf.count()

        Assert(sourceCount, targetCount)

# COMMAND ----------

def CleansedSrcMinusTgtChk():    
    if DO_ST_TESTS == True:
        sourceDf, targetDf = SRC_DF, TGT_DF

        df = sourceDf.subtract(targetDf)
        count = df.count()
        if count > 0: 
            print(f'Records that are in source but not in target ({count} total records): ')
            display(df)

        Assert(count, 0, errorMessage = f"""Mismatching records found - {count} records are in source but not in target""")

# COMMAND ----------

def CleansedTgtMinusSrcChk():   
    if DO_ST_TESTS == True:
        sourceDf, targetDf = SRC_DF, TGT_DF

        df = targetDf.subtract(sourceDf)
        count = df.count()
        if count > 0: 
            print(f'Records that are in target but not in source ({count} total records): ')
            display(df)

        Assert(count, 0, errorMessage = f"""Mismatching records found - {count} records are in target but not in source""")

# COMMAND ----------

# DBTITLE 1,Curated Test Cases
def DuplicateKeysActiveRecsChk():
    keyColumns = UNIQUE_KEYS
    if keyColumns.strip() == "":
        Assert(0, 0, compare = "Greater Than", errorMessage = f"Test could not be executed, No Unique Key column/s were specified in the mapping.")
    else:
        df = spark.sql(f"""SELECT {keyColumns}, COUNT(*) as recCount FROM {GetSelfFqn()} 
                           WHERE _RecordCurrent=1 and _recordDeleted=0 
                           GROUP BY {keyColumns} HAVING COUNT(*) > 1""")
        count = df.count()
        if count > 0: display(df)
        Assert(count,0)

# COMMAND ----------

def KeysAreSameLengthChk():
    keyColumns = UNIQUE_KEYS
    if keyColumns.strip() == "":
        Assert(0, 0, compare = "Greater Than", errorMessage = f"Test could not be executed, No Unique Key column/s were specified in the mapping.")
    else:
        keyColsList = keyColumns.split(",")

        for column in keyColsList:
            column = column.strip()

            sqlQuery = f"SELECT DISTINCT length({column}) FROM {GetSelfFqn()} \
                         WHERE length({column}) <> 2 "

            if (column.upper().find('DATE') == -1): # if not a date column, add extra condition
                sqlQuery = sqlQuery + f"AND {column} <> 'Unknown'"

            df = spark.sql(sqlQuery)
            count = df.count()
            if count > 1: display(df)
            Assert(count,1, errorMessage = f"Failed check for key column: {column}")

# COMMAND ----------

# DBTITLE 1,Curated Test Cases from UC1 
def DummyRecChk():
    try:
        df = spark.sql(f"SELECT COUNT (*) as RecCount FROM {GetSelfFqn()} WHERE sourceSystemCode is NULL")
        count = df.count()
        if count != 1: display(df)
        Assert(count, 1)
    # added to handle dim tables which do not have column 'sourceSystemCode' - dimWaterNetwork, dimSewerNetwork, dimStormWaterNetwork
    except:
        keyColumns = UNIQUE_KEYS
        if keyColumns.strip() == "":
            Assert(0, 0, compare = "Greater Than", errorMessage = f"Test could not be executed, No Unique Key column/s were specified in the mapping.")
        else:
            keyColsList = keyColumns.split(",")
            firstColumn = keyColsList[0].strip()
            sqlQuery = f"SELECT COUNT (*) as RecCount FROM {GetSelfFqn()} "

            for column in keyColsList:
                column = column.strip()
                if column == firstColumn:
                    sqlQuery = sqlQuery + f"WHERE {column} = 'Unknown' "
                else:
                    sqlQuery = sqlQuery + f"OR {column} = 'Unknown' "

# COMMAND ----------

def DuplicateSKChk():
    skColumn = GetSelf().columns[0]
    df = spark.sql(f"SELECT COUNT (*) as RecCount FROM {GetSelfFqn()} GROUP BY {skColumn} HAVING COUNT (*) > 1")
    count = df.count()
    if count > 0: display(df)
    Assert(count, 0)

# COMMAND ----------

def DuplicateSKActiveRecsChk():
    skColumn = GetSelf().columns[0]
    df = spark.sql(f"SELECT COUNT (*) as RecCount FROM {GetSelfFqn()} WHERE _RecordCurrent = 1 and _RecordDeleted = 0 GROUP BY {skColumn} HAVING COUNT(*) > 1")
    count = df.count()
    if count > 0: display(df)
    Assert(count, 0)

# COMMAND ----------

def SKNotNullOrBlankChk():
    skColumn = GetSelf().columns[0]
    df = spark.sql(f"SELECT COUNT (*) as RecCount FROM {GetSelfFqn()} WHERE {skColumn} is null or {skColumn} = '' or {skColumn} = ' ' or upper({skColumn}) ='NULL'")
    count = df.select("RecCount").collect()[0][0]
    if count > 0: display(df)
    Assert(count, 0)

# COMMAND ----------

def DateValidation():
    df = spark.sql(f"""SELECT * FROM {GetSelfFqn()} 
                   WHERE date(_RecordStart) > date(_RecordEnd)""")
    count = df.count()
    if count > 0: display(df)
    Assert(count, 0, errorMessage = "Failed date validation for _RecordStart and _RecordEnd")

# COMMAND ----------

def AuditEndDateChk():
    keyColumns = UNIQUE_KEYS
    if keyColumns.strip() == "":
        Assert(0, 0, compare = "Greater Than", errorMessage = f"Test could not be executed, No Unique Key column/s were specified in the mapping.")
    else:
        df = spark.sql(f"""SELECT * FROM {GetSelfFqn()} 
                         WHERE date(_RecordEnd) < (SELECT date(max(_DLCuratedZoneTimeStamp)) as max_date FROM {GetSelfFqn()}) 
                         and _RecordCurrent = 1 and _RecordDeleted = 0""")
        count = df.count()
        if count > 0: display(df)
        Assert(count,0)

# COMMAND ----------

def AuditCurrentChk():
    keyColumns = UNIQUE_KEYS
    if keyColumns.strip() == "":
        Assert(0, 0, compare = "Greater Than", errorMessage = f"Test could not be executed, No Unique Key column/s were specified in the mapping.")
    else:
        df = spark.sql(f"""SELECT * FROM (
                       SELECT * FROM (   
                       SELECT {keyColumns}, date(_RecordStart) as start_dt,date(_RecordEnd) as end_dt, _RecordCurrent, _RecordDeleted, max_date FROM {GetSelfFqn()} as a, 
                           (SELECT date(max(_DLCuratedZoneTimeStamp)) as max_date FROM {GetSelfFqn()}) as b 
                           )WHERE ((max_date < end_dt) and (max_date > start_dt)) and _RecordDeleted <> 1 
                           )WHERE _RecordCurrent <> 1 
                       """)
        count = df.count()
        if count > 0: display(df)
        Assert(count,0)

# COMMAND ----------

def AuditActiveOtherChk():
    keyColumns = UNIQUE_KEYS
    if keyColumns.strip() == "":
        Assert(0, 0, compare = "Greater Than", errorMessage = f"Test could not be executed, No Unique Key column/s were specified in the mapping.")
    else:
        df = spark.sql(f"""SELECT * FROM (
                       SELECT * FROM (   
                       SELECT {keyColumns}, date(_RecordStart) as start_dt, date(_RecordEnd) as end_dt, _RecordCurrent, _RecordDeleted, max_date FROM {GetSelfFqn()} as a, 
                           (SELECT date(max(_DLCuratedZoneTimeStamp)) as max_date FROM {GetSelfFqn()}) as b 
                           )WHERE ((max_date > end_dt) and (max_date < start_dt)) 
                           )WHERE _RecordCurrent <> 0 and _RecordDeleted <> 0 
                       """)
        count = df.count()
        if count > 0: display(df)
        Assert(count,0)

# COMMAND ----------

def AuditDeletedChk():
    keyColumns = UNIQUE_KEYS
    if keyColumns.strip() == "":
        Assert(0, 0, compare = "Greater Than", errorMessage = f"Test could not be executed, No Unique Key column/s were specified in the mapping.")
    else:
        df = spark.sql(f"""SELECT * FROM (
                       SELECT * FROM (   
                       SELECT {keyColumns},date(_RecordStart) as start_dt, date(_RecordEnd) as end_dt, _RecordCurrent, _RecordDeleted, max_date FROM {GetSelfFqn()} as a, 
                           (SELECT date(max(_DLCuratedZoneTimeStamp)) as max_date FROM {GetSelfFqn()}) as b
                           )WHERE ((max_date < end_dt) and (max_date > start_dt)) and (end_dt<> '9999-12-31')
                           )WHERE _RecordCurrent = 1 and _RecordDeleted = 1 
                       """)
        count = df.count()
        if count > 0: display(df)
        Assert(count,0)
