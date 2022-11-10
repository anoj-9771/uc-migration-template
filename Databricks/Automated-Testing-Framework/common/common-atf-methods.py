# Databricks notebook source
_automatedMethods = {
    "tests" : []
    ,"cleansed" : ["RowCounts", "ColumnColumns","CountValidateCurrentRecords","CountValidateDeletedRecords","DuplicateCheck","DuplicateActiveCheck","BusinessKeyNullCk","BusinessKeyLengthCk"]
    ,"curated" : [ "RowCounts", "DuplicateSK", "DuplicateActiveSK", "MD5ValueSK", "DateValidation",  "CountValidateCurrentRecords", "CountValidateDeletedRecords"
                  #, "DuplicateCheck"
                  #"OverlapAndGapValidation",
                  #, "DuplicateActiveCheck"
                  #, "ExactDuplicates", "BusinessKeyNullCk", "BusinessKeyLengthCk", "ManBusinessColNullCk", "SourceTargetCountCheckActiveRecords", "SourceMinusTargetCountCheckActiveRecords", "TargetMinusSourceCountCheckActiveRecords", "SourceTargetCountCheckDeletedRecords", "SourceMinusTargetCountCheckDeletedRecords", "TargetMinusSourceCountCheckDeletedRecords", "ColumnColumns" 
                 ] 
    ,"curated_v2" : ["RowCounts", "DuplicateSK", "DuplicateActiveSK", "MD5ValueSK", "DateValidation","DateValidation1","CountCurrentRecordsC1D0", "CountCurrentRecordsC0D0", "OverlapAndGapValidation", "CountValidateCurrentRecords", "CountValidateDeletedRecords","CountDeletedRecords", "DuplicateCheck", "DuplicateActiveCheck", "ExactDuplicates", "BusinessKeyNullCk", "BusinessKeyLengthCk", "ManBusinessColNullCk", "SourceTargetCountCheckActiveRecords", "SourceMinusTargetCountCheckActiveRecords", "TargetMinusSourceCountCheckActiveRecords", "SourceTargetCountCheckDeletedRecords", "SourceMinusTargetCountCheckDeletedRecords", "TargetMinusSourceCountCheckDeletedRecords", "ColumnColumns","auditEndDateChk","auditCurrentChk","auditActiveOtherChk","auditDeletedChk" ]
}

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
                   SELECT {keyColumns},date(_RecordStart) as start_dt ,date(_RecordEnd) as end_dt ,_RecordCurrent,_RecordDeleted,max_date from {GetSelfFqn()} as a, \
                       (Select date(max(_DLCuratedZoneTimeStamp)) as max_date from {GetSelfFqn()}) as b\
                        where _RecordDeleted =1\
                        )where ((max_date > end_dt) and (max_date > start_dt))\
                   ")
    count = df.count()
    if count > 0: display(df)
    Assert(count,0)
