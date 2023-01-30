# Databricks notebook source
_automatedMethods = {
    "tests": [],
    "cleansed": [
        "CleansedSchemaCheck"
        ,"DuplicateKeysCheck"
        ,"BusinessKeysNullCk"
        ,"CountRecordsC1D0" 
        ,"CountRecordsC0D1" 
        ,"CheckAuditColsPresent"
        ,"RowCounts"
        ,"ColumnColumns"
    ],
    "curated": [
        "RowCounts"
        ,"ColumnCounts"
        ,"CountValidateCurrentRecords"
        ,"CountValidateDeletedRecords"
        
        ,"Duplicate"
        ,"DuplicateActive"
        ,"BusinessKeyNull"
        ,"BusinessKeyLength"
        
        ,"DuplicateSK"
        ,"DuplicateActiveSK"
        ,"MD5ValueSK"
        ,"DateValidation"
        ,"CountCurrentRecordsC1D0"
        ,"CountCurrentRecordsC0D0"
        ,"CountDeletedRecords"
        ,"ExactDuplicates"
        ,"ManBusinessColNull"
        ,"AuditEndDate"
        ,"AuditCurrent"
        ,"AuditActiveOther"
        ,"AuditDeleted"
    ],
    "curated_v2": [
    ]
}

#COPY THE AUTOMATED TESTS
_automatedMethods["curated_v2"] = _automatedMethods["curated"]

DOC_PATH = ''
SHEET_NAME = ''

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

# Date validation for _RecordStart and _RecordEnd
def DateValidation1():
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

def CountCurrentRecordsC1D0():
    df = spark.sql(f"select count(*) as recCount from {GetSelfFqn()} \
                WHERE _RecordCurrent=1 and _RecordDeleted=0")
    count = df.select("recCount").collect()[0][0]
    GreaterThan(count,0)

# COMMAND ----------

def CountCurrentRecordsC1D0():
    df = spark.sql(f"select count(*) as recCount from {GetSelfFqn()} \
                WHERE _RecordCurrent=1 and _RecordDeleted=0")
    count = df.select("recCount").collect()[0][0]
    GreaterThan(count,0)

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

def CountDeletedRecords():
    table = GetNotebookName()
    df = spark.sql(f"select count(*) as recCount from {GetSelfFqn()} \
                WHERE _RecordCurrent=0 and _RecordDeleted=1")
    count = df.select("recCount").collect()[0][0]
    Assert(count,count)

# COMMAND ----------

def DuplicateCount():
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
    global columns
    df = spark.sql(f"SELECT {columns}, COUNT(*) as recCount \
                   FROM {GetSelfFqn()} \
                   GROUP BY {columns} HAVING COUNT(*) > 1")
    count = df.count()
    if count > 0: display(df)
    Assert(count,0)

# COMMAND ----------

def BusinessKeyNull():
    global keyColumns
    keyColsList = keyColumns.split(",")
    firstColumn = keyColsList[0].strip()
    sqlQuery = f"Select * from {GetSelfFqn()} "
    
    for column in keyColsList:
        column = column.strip()
        if column == firstColumn:
            sqlQuery = sqlQuery + f"WHERE ({column} is NULL or {column} in ('',' ') or UPPER({column})='NULL') "
        else:
            sqlQuery = sqlQuery + f"OR ({column} is NULL or {column} in ('',' ') or UPPER({column})='NULL') "
        
    #print(sqlQuery)
    
    df = spark.sql(sqlQuery)
    count = df.count()
    if count > 0: display(df)
    Assert(count,0)


# COMMAND ----------

def BusinessKeyLength():
    global keyColumns
    keyColsList = keyColumns.split(",")
    
    for column in keyColsList:
        column = column.strip()
        
        sqlQuery = f"SELECT DISTINCT length({column}) from {GetSelfFqn()} \
                     WHERE length({column}) <> 2 "

        if (column.upper().find('DATE') == -1): # if not a date column, add extra condition
            sqlQuery = sqlQuery + f"AND {column} <> 'Unknown'"
            
        df = spark.sql(sqlQuery)
        count = df.count()
        if count > 1: display(df)
        Assert(count,1, errorMessage = f"Failed check for key column: {column}")
    

# COMMAND ----------

def ManBusinessColNull():
    global mandatoryColumns
    busColsList = mandatoryColumns.split(",")
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
    if count > 0: display(df)
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

def ColumnCounts():
    count = len(GetSelf().columns)
    Assert(count,count)

# COMMAND ----------

def AuditEndDate():
    global keyColumns
    df = spark.sql(f"Select * from {GetSelfFqn()} \
                     where date(_RecordEnd) < (select date(max(_DLCuratedZoneTimeStamp)) as max_date from {GetSelfFqn()}) \
                     and _recordCurrent=1 and _recordDeleted=0")
    count = df.count()
    if count > 0: display(df)
    Assert(count,0)

# COMMAND ----------

def AuditCurrent():
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

def AuditDeleted():
    global keyColumns
    table = GetNotebookName()
    df = spark.sql(f"select * from (\
                   SELECT {keyColumns},date(_RecordStart) as start_dt ,date(_RecordEnd) as end_dt ,_RecordCurrent,_RecordDeleted,max_date from {GetSelfFqn()} as a, \
                       (Select date(max(_DLCuratedZoneTimeStamp)) as max_date from {GetSelfFqn()}) as b\
                        where _RecordDeleted =1\
                        )where ((max_date > end_dt) and (max_date > start_dt))\
                   ")
    count = df.count()
    if count > 0: display(df)
    Assert(count,0)

# COMMAND ----------

# removes non ascii characters
def ascii_ignore(x):
    return x.encode('ascii', 'ignore').decode('ascii')
ascii_udf = udf(ascii_ignore)

# COMMAND ----------

def TrimWhitespace(df):
    for column in df.columns:
        df = df.withColumn(column, regexp_replace(column, r"^\s+|\s+$", ""))
        if column != "UniqueKey":
            df = df.withColumn(column, ascii_udf(column))
    return df 

# COMMAND ----------

def loadMappingDocument():
    
    mappingData = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("dataAddress", f"{SHEET_NAME}!A1") \
    .load(f"{DOC_PATH}")
    
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
        order by CleansedColumnName
    """)
    return targetDf

# COMMAND ----------

def GetMappingSchemaDf():
    mappingData = loadMappingDocument()
    mappingData.createOrReplaceTempView("mappingView")
    
    df = spark.sql(f"""
        Select 
        CleansedTable,
        CleansedColumnName, 
        DataType,
        UniqueKey
        from mappingView
        where UPPER(CleansedTable) = UPPER("{GetSelfFqn()}")
    """)
    
    df = TrimWhitespace(df)
    df = df.withColumn("DataType", upper("DataType"))
    df = df.withColumn("UniqueKey", upper("UniqueKey"))
    df.createOrReplaceTempView("cleanMappingView")
    
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
        UniqueKey
        from cleanMappingView
        where UPPER(CleansedTable) = UPPER("{GetSelfFqn()}")
        order by CleansedColumnName
    """)
    return mappingDf

# COMMAND ----------

# mapping doc is currently stored at: 'dbfs:/mnt/data/mapping_documents_UC3/<mapping doc name>'
def CleansedSchemaCheck():
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

def GetUniqueKeys():
    mappingData = loadMappingDocument()
    mappingData.createOrReplaceTempView("mappingView")
    
    df = spark.sql(f"""
        Select 
        CleansedColumnName
        from mappingView
        where UPPER(CleansedTable) = UPPER("{GetSelfFqn()}")
        and UniqueKey = 'Y'
    """)
    
    keys = ""
    for index, key in enumerate(df.collect()):
        keys = keys + key.CleansedColumnName
        if index != len(df.collect()) - 1:
            keys = keys + ",\n"
        else: 
            keys = keys + "\n"
    
    return keys

# COMMAND ----------

def DuplicateKeysCheck():
    keyColumns = GetUniqueKeys()
    df = spark.sql(f"""SELECT {keyColumns}, COUNT(*) as RecCount FROM {GetSelfFqn()} 
                   GROUP BY {keyColumns} HAVING COUNT(*) > 1""")
    count = df.count()
    if count > 0: display(df)
    Assert(count,0)

# COMMAND ----------

def BusinessKeysNullCk():
    keyColumns = GetUniqueKeys()
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

def CountRecordsCnDn(C, D):
    df = spark.sql(f"SELECT count(*) as recCount FROM {GetSelfFqn()} \
                WHERE _RecordCurrent = {C} and _RecordDeleted = {D}")
    count = df.select("recCount").collect()[0][0]
    
    if C == 1 and D == 0:
        GreaterThan(count,0)
    else:
        GreaterThanEqual(count,0)

# COMMAND ----------

def CountRecordsC1D0():
    CountRecordsCnDn(1, 0)
def CountRecordsC0D1():
    CountRecordsCnDn(0, 1)

# COMMAND ----------

def CheckAuditColsPresent():
    df = spark.sql(f"select * from {GetSelfFqn()}")
    auditCols = ['_RecordStart', '_RecordEnd', '_RecordCurrent', '_RecordDeleted', '_DLCleansedZoneTimeStamp']
    targetAuditCols = []
    for column in df.columns:
        if column.startswith("_") == True:
            targetAuditCols.append(column)
    
    auditColsNotInTarget = [x for x in auditCols if x not in targetAuditCols]
    
    Assert(len(auditColsNotInTarget), 0, errorMessage = f"Missing audit column(s): {auditColsNotInTarget}")
