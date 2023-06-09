# Databricks notebook source
_automatedMethods = {
    "tests": [],
    "cleansed": [
        "SchemaChk"
        ,"DuplicateKeysChk"
        ,"ExactDuplicates"
        ,"KeysNotNullOrBlankChk"
        ,"AuditColsIncludedChk"
        ,"DateValidation"
        ,"CountRecordsC1D0" 
        ,"CountRecordsC0D1" 
        ,"SrcVsTgtCountChk"
        ,"RowCounts"
        ,"ColumnCounts"
        ,"ST_CountChk_AllRecs"
        ,"SmT_Chk_AllRecs"
        ,"TmS_Chk_AllRecs"
        ,"KeysNotNullOrBlankChk"
    ],
    "curated": [
        "SchemaChk"
        ,"DummyRecChk"
        ,"DuplicateKeysChk"
        ,"DuplicateKeysActiveRecsChk"
        ,"DuplicateSKChk"
        ,"DuplicateSKActiveRecsChk"
        ,"SKNotNullOrBlankChk"
        ,"ExactDuplicates"
        ,"DuplicateBusinessKeyChk"
        ,'BusinessKeyNotNullOrBlankChk'
        ,"BusinessKeyFormatChk"
        ,"MandatoryColsNotNullorBlankCk"
        ,"DateValidation"
        ,"DateValidationSD1"
        ,"DateValidationSD2"
        ,"OverlapTimeStampValidation"
        ,"AuditColsIncludedChk"
        ,"AuditEndDateChk"
        ,"AuditCurrentChk"
        ,"AuditActiveOtherChk"
        ,"AuditDeletedChk"
        ,"CountRecordsC1D0" 
        ,"CountRecordsC0D0"
        ,"CountRecordsC1D1" 
        ,"CountRecordsC0D1"
        #,"SrcVsTgtCountChk"
        ,"RowCounts"
        ,"ColumnCounts"
        ,"ST_CountChk_AllRecs"
        ,"SmT_Chk_AllRecs"
        ,"TmS_Chk_AllRecs"
        ,"Curated_ST_CountChkC1D0"
        ,"Curated_ST_CountChkC0D0"
        ,"Curated_ST_CountChkC1D1"
        ,"Curated_ST_CountChkC0D1"
        ,"Curated_SmT_ChkC1D0"
        ,"Curated_TmS_ChkC1D0"
        ,"Curated_SmT_ChkC0D0"
        ,"Curated_TmS_ChkC0D0"
        ,"Curated_SmT_ChkC1D1"
        ,"Curated_TmS_ChkC0D0"
        ,"Curated_SmT_ChkC0D1"
        ,"Curated_TmS_ChkC0D1"
        ,"KeysNotNullOrBlankChk"
        #,"KeysAreSameLengthChk"
    ],
    "curated_v2": [
    ],
    "curated_v3": [
    ]
}

#COPY THE AUTOMATED TESTS
_automatedMethods["curated_v2"] = _automatedMethods["curated"]
_automatedMethods["curated_v3"] = _automatedMethods["curated"]

DOC_PATH = ''
SHEET_NAME = ''
UNIQUE_KEYS = ''
MAPPING_DOC = ''
TAG_SHEET = ''

FD_OR_SD = 'FD'
MANDATORY_COLS = ''
ALL_COLS = ''

DO_ST_TESTS = False
TST_TM_CHKS = False
SHOW_ALL_FAILS = False

SOURCE_QUERY = ''
SOURCE_ACTIVE = ''
SOURCE_DELETED = ''
SD_TARGET = ''

# COMMAND ----------

# DBTITLE 1,Helper Functions
def ascii_ignore(x):
    try:
        return x.encode('ascii', 'ignore').decode('ascii')
    except:
        pass
#         print("Warning: Missing values for Target Column Name and/or Datatype, Schema Check and S-T tests may fail due to incomplete mapping.")
ascii_udf = udf(ascii_ignore)

# COMMAND ----------

def TrimWhitespace(df):
    columns = [
        "TargetColumnName", 
        "DataType"
    ]
    for column in columns:
        df = df.withColumn(column, regexp_replace(column, r"^\s+|\s+$", ""))
        df = df.withColumn(column, ascii_udf(column))
    return df 

# COMMAND ----------

def loadCleansedMapping(sheetName = ""):
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
                                     "RawTable as SourceTable",
                                     "UPPER(CleansedTable) as TargetTable",
                                     "RawColumnName as SourceColumnName",
                                     "CleansedColumnName as TargetColumnName",
                                     "TransformTag",
                                     "CustomTransform"        
                      )
        mappingData = mappingData.filter(mappingData.TargetTable.isNotNull())
        mappingData = TrimWhitespace(mappingData)
    elif sheetName == 'TAG':
        mappingData = mappingData.selectExpr(
                                 "`Tag Name` as TagName",
                                 "`SQL syntax` as SQLsyntax"
                      )

    return mappingData

# COMMAND ----------

def loadRefLookupSheet():
    if not REF_LOOKUP_PATH:
        REF_LOOKUP_PATH = "dbfs:/FileStore/Test/reference_lookup_v1.csv"
    refLookup = spark.read \
                .format("csv") \
                .option("header", True) \
                .option("inferSchema", True) \
                .load(f"{REF_LOOKUP_PATH}")
    return refLookup

# COMMAND ----------

def loadCuratedMapping(sheetName = ""):
    if sheetName == "":
        sheetName = SHEET_NAME
        
    mappingData = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("dataAddress", f"{sheetName}!A1") \
    .load(f"{DOC_PATH}")
    
    uKColNames = ["Unique Business Key", "Business Key"]
    tgtTbColNames = ["Dimension Name", "Target Table Name"]

    ukCol, tgtTbCol = '', ''
    for column in mappingData.columns:
        if column in uKColNames:
            ukCol = column
        elif column in tgtTbColNames:
            tgtTbCol = column

        if ukCol and tgtTbCol:
            break

    if not ukCol or not tgtTbCol:
        print("Error: Column name for Unique Key column and/or Target Table Name column is not as expected.")    

    if sheetName == SHEET_NAME:
        try:
            mappingData = mappingData.selectExpr(
                                         f"UPPER(`{ukCol}`) as UniqueKey",
                                         "UPPER(`Target Column Data Type`) as DataType",
                                         "`Source Table Name` as SourceTable",
                                         f"UPPER(`{tgtTbCol}`) as TargetTable",
                                         "`Source Column Name(s)` as SourceColumnName",
                                         "`Target Column Name` as TargetColumnName",
                                         "`Mandatory (Y/N)` as MandatoryCols",
                                         "`Source Data Type` as SrcDataType",
                                         "`Transformation Logic` as Transformations"
                          )
            mappingData = mappingData.filter(mappingData.TargetTable.isNotNull())
            mappingData = TrimWhitespace(mappingData)
        except:
            print("Error occured while loading mapping, column name(s) may not be as expected.")
        # except:
        #     try:
        #         mappingData = mappingData.selectExpr(
        #                                  "UPPER(`Unique Business Key`) as UniqueKey",
        #                                  "UPPER(`Target Column Data Type`) as DataType",
        #                                  "`Source Table Name` as SourceTable",
        #                                  "UPPER(`Target Table Name`) as TargetTable",
        #                                  "`Source Column Name(s)` as SourceColumnName",
        #                                  "`Target Column Name` as TargetColumnName",
        #                                  "`Mandatory (Y/N)` as MandatoryCols",
        #                                  "`Source Data Type` as SrcDataType",
        #                                  "`Transformation Logic` as Transformations"
        #                   )
        #     
        #     except:
        #         mappingData = mappingData.selectExpr(
        #                                     "UPPER(`Business Key`) as UniqueKey",
        #                                     "UPPER(`Target Column Data Type`) as DataType",
        #                                     "`Source Table Name` as SourceTable",
        #                                     "UPPER(`Dimension Name`) as TargetTable",
        #                                     "`Source Column Name(s)` as SourceColumnName",
        #                                     "`Target Column Name` as TargetColumnName",
        #                                     "`Mandatory (Y/N)` as MandatoryCols",
        #                                     "`Source Data Type` as SrcDataType",
        #                                     "`Transformation Logic` as Transformations"
        #                     )
            
        # mappingData = mappingData.filter(mappingData.TargetTable.isNotNull())
        # mappingData = TrimWhitespace(mappingData)
        
    return mappingData

# COMMAND ----------

def GetTargetSchemaDf():
    target = spark.sql(f"Select * from {GetSelfFqn()}")
    tgtSchema = target._jdf.schema().treeString()
    tgtSchema = tgtSchema.strip(" rot").strip().splitlines()
    tgtSchemaList = []

    for line in tgtSchema:
        line = line.replace('nullable = ', "")

        if line.startswith("|-- ") == True or line.startswith(" |-- ") == True:
            filterChars = '|-:='
            for char in filterChars:
                line = line.replace(char, "")
            
            line = line.replace('(f', "f").replace('(t', "t").strip(')').strip()

            if line.startswith("_") == True:
                continue

            row = tuple(map(str, line.split()))
            tgtSchemaList.append(row) 

    # for i in tgtSchemaList:
    #     print(i)

    dfStructure = StructType([StructField('TargetColumnName', StringType()),
                                StructField('DataType', StringType()),
                                StructField('Has nullable=false', StringType())])

    targetDf = spark.createDataFrame(tgtSchemaList, dfStructure)
    targetDf.createOrReplaceTempView("tableView")
    targetDf = spark.sql("""
        Select 
        TargetColumnName, 
        case 
            when UPPER(DataType) = 'STRUCT' then 'INHERIT'
            else UPPER(DataType)
        end as DataType,
        case when `Has nullable=false` = 'false' then 'Y' else null end as `Has nullable=false`
        from tableView
    """)

    return targetDf

# COMMAND ----------

def GetMappingSchemaDf():
    if GetDatabaseName().upper() == 'CLEANSED':
        tablename = GetSelfFqn()
    else:
        tablename = GetNotebookName()
    
    mappingData = MAPPING_DOC
    mappingData.createOrReplaceTempView("mappingView")
    
    startQuery = f"""
        Select 
        TargetColumnName, 
        case 
            when DataType = 'DATETIME' then 'TIMESTAMP'
            when DataType = 'TIME' then 'TIMESTAMP'
            when DataType = 'BIGINT' then 'LONG'
            when DataType like "%CHAR%" then 'STRING'
            when DataType like "INT%" then 'INTEGER'
            when DataType like "DAT%" then 'DATE'
            when DataType like "DECIMAL%" then REPLACE(DataType, ' ', '')
            else DataType
        end as DataType, 
        """
    
    endQuery = f"""
        from mappingView
        where TargetTable = UPPER("{tablename}")
        and TargetColumnName not like '\_%'
    """
    
    # for cleansed, we check that UniqueKey columns have null contraint
    if GetDatabaseName().upper() == 'CLEANSED':
        mappingDf = spark.sql(f""" 
            {startQuery}
            case 
                when trim(UniqueKey) = '' then Null
                when trim(UniqueKey) = ' ' then Null
                else UniqueKey
            end as `Has nullable=false`
            {endQuery}
        """)
        
    # for curated, we check that Mandatory columns have null contraint
    else: 
        mappingDf = spark.sql(f""" 
            {startQuery}
            case 
                when trim(MandatoryCols) = '' then Null
                when trim(MandatoryCols) = ' ' then Null
                else MandatoryCols
            end as `Has nullable=false`
            {endQuery}
        """)
        
    return mappingDf

# COMMAND ----------

def GetUniqueKeys():
    if GetDatabaseName().upper() == 'CLEANSED':
        tablename = GetSelfFqn()
    else:
        tablename = GetNotebookName()
        
    mappingData = MAPPING_DOC
    mappingData.createOrReplaceTempView("mappingView")
    
    df = spark.sql(f"""
        Select 
        TargetColumnName
        from mappingView
        where TargetTable = UPPER("{tablename}")
        and UniqueKey = 'Y'
    """)
    
    keys = ""
    for row in df.collect():
        keys = keys + row.TargetColumnName + ",\n"                
    keys = keys.strip().strip(',')
    
    return keys

# COMMAND ----------

def GetMandatoryCols():
#     Required if Cleansed has mandatory cols (currently does not)
#     if GetDatabaseName().upper() == 'CLEANSED':
#         tablename = GetSelfFqn()
#     else:
#         tablename = GetNotebookName()

    tablename = GetNotebookName()
        
    mappingData = MAPPING_DOC
    mappingData.createOrReplaceTempView("mappingView")
    
    df = spark.sql(f"""
        Select 
        TargetColumnName
        from mappingView
        where TargetTable = UPPER("{tablename}")
        and MandatoryCols = 'Y'
    """)
    
    cols = ""
    for row in df.collect():
        cols = cols + row.TargetColumnName + ",\n"                
    cols = cols.strip().strip(',')
    
    return cols

# COMMAND ----------

def GetForeignKeyCols():
#     Required if Cleansed has mandatory cols (currently does not)
#     if GetDatabaseName().upper() == 'CLEANSED':
#         tablename = GetSelfFqn()
#     else:
#         tablename = GetNotebookName()

    tablename = GetNotebookName()
        
    mappingData = MAPPING_DOC
    mappingData.createOrReplaceTempView("mappingView")
    
    df = spark.sql(f"""
        Select 
        TargetColumnName
        from mappingView
        where TargetTable = UPPER("{tablename}")
        and TargetColumnName like '%FK'
    """)
    
    cols = ""
    for row in df.collect():
        cols = cols + row.TargetColumnName + ",\n"                
    cols = cols.strip().strip(',')
    
    return cols

# COMMAND ----------

def AutomatedSourceQuery(tablename = ""):
    if tablename == "" and GetDatabaseName().upper() != 'CLEANSED':
        tablename = GetNotebookName()
    elif tablename == "":
        tablename = GetSelfFqn()
    
    mappingData = MAPPING_DOC
    mappingData.createOrReplaceTempView("mappingView")
    
    if TAG_SHEET:
        tags = TAG_SHEET
        tags.createOrReplaceTempView("tagView")
    
    df = spark.sql(f"""
        Select 
        DataType,
        TransformTag,
        CustomTransform,
        case 
            when SourceColumnName like 'features\_properties\_%' 
                then REPLACE(SourceColumnName, 'features_properties_', '')
            else REPLACE(SourceColumnName, 'features_', '')
            end as SourceColumnName,
        TargetColumnName,
        SourceTable
        from mappingView
        where TargetTable = UPPER("{tablename}")
    """)
    # df.display()

    sourceSystem = GetNotebookName().split("_")[0]
    # print(sourceSystem)

    if sourceSystem.upper() == 'HYDRA':
        jsonPath = f"dbfs:/mnt/datalake-landing/hydra/{GetNotebookName().split('_', 1)[1]}/*.json"
        jsonFile = spark.read.format("json").load(jsonPath)
        # print(jsonPath)
        jsonDf = jsonFile.drop('_corrupt_record', 'type')\
            .select('*', 'properties.*')\
            .filter('properties is not null')\
            .drop('properties')
        
        for column in jsonDf.columns:
            if column.upper() not in ['CHILDJOINS', 'PARENTJOINS', 'GEOMETRY']:
                jsonDf = jsonDf.withColumn(column, when(col(column) == "", None) \
                                                .otherwise(col(column))) 
        
        jsonDf.createOrReplaceTempView("jsonSourceView")
        # jsonDf.display()
        
    sqlQuery = "SELECT \n"
    for row in df.collect():
        if row.DataType.upper() == 'INHERIT':
            sqlCode = f"a.{row.SourceColumnName}"
        elif row.TransformTag is not None:
            sqlCode = spark.sql(f"Select * from tagView where TagName = '{row.TransformTag}'") \
                            .select("SQLsyntax").collect()[0][0] 
            if row.TransformTag.upper() == 'TRIMCOALESCE' and row.DataType == 'DATETIME':
                sqlCode = sqlCode.replace("$c$", f"CAST(a.{row.SourceColumnName} AS TIMESTAMP)") 
            elif row.TransformTag.upper() == 'TRIMCOALESCE':
                sqlCode = sqlCode.replace("$c$", f"CAST(a.{row.SourceColumnName} AS {row.DataType})") 
            else: 
                sqlCode = sqlCode.replace("$c$", f"a.{row.SourceColumnName}") 
        elif row.CustomTransform is not None and row.CustomTransform != "":
            sqlCode = row.CustomTransform
        elif row.DataType == 'DATETIME' or row.DataType == 'TIME':
            sqlCode = f"CAST(a.{row.SourceColumnName} AS TIMESTAMP)"
        else:
            sqlCode = f"CAST(a.{row.SourceColumnName} AS {row.DataType})"

        sqlCode = sqlCode + " as " + row.TargetColumnName + ",\n"

        sqlQuery = sqlQuery + sqlCode

    sqlQuery = sqlQuery.strip().strip(',')

    if sourceSystem.upper() == 'HYDRA':
        rawTable = "jsonSourceView"
    else:
        rawTable = df.select("SourceTable").collect()[0][0]

    sqlQuery = sqlQuery + "\n" + f"from {rawTable} a\n"
    # print(sqlQuery)
    
    return sqlQuery

# COMMAND ----------

def CuratedSrcQryTemplate(tablename = ""):
    mappingData = loadCuratedMapping()
    mappingData.createOrReplaceTempView("mappingView")
    tablename = GetNotebookName()

    df = spark.sql(f"""
            Select 
            DataType,
            SourceTable,
            SourceColumnName,
            TargetColumnName,
            Transformations
            from mappingView
            where TargetTable = UPPER("{tablename}")
            and TargetColumnName not like '\_%'
        """)

    df.createOrReplaceTempView("mappingView")
    rawTable = spark.sql(f"""
        Select SourceTable, count(SourceTable) from mappingView
        group by SourceTable
        order by count(SourceTable) desc
        limit 1
    """).select("SourceTable").collect()[0][0] 

    sqlQuery = 'sourceDf = spark.sql("""\nSELECT \n'
    joins = ""
    for row in df.collect():
        if row.TargetColumnName.endswith('SK'):
            continue
#         elif row.DataType == 'DATETIME' or row.DataType == 'TIME':
#             sqlCode = f"CAST(a.{row.SourceColumnName} AS TIMESTAMP)"
#         else:
#             sqlCode = f"CAST(a.{row.SourceColumnName} AS {row.DataType})"
        else:
            sqlCode = f"a.{row.SourceColumnName}"

        sqlCode = sqlCode + " as " + row.TargetColumnName + ",\n"

        sqlQuery = sqlQuery + sqlCode

        if row.Transformations is not None:
            joins = joins + "Transformation for " + row.TargetColumnName + "\n" + row.Transformations + "\n\n"
        
    joins = joins.strip()
    sqlQuery = sqlQuery.strip().strip(',')
    sqlQuery = sqlQuery + "\n" + f"from cleansed.{rawTable} a\n\n" + joins + '\n""")'
    print(sqlQuery)
    
    
#     df = spark.sql(f"""
#         Select 
#         UniqueKey,
#         SourceTable as SrcTb,
#         SourceColumnName as SrcColName,
#         SrcDataType as SrcDType,
#         TargetTable as TgtTb,
#         TargetColumnName as TgtColName,
#         DataType as TgtDType,
#         Transformations as TForm
#         from mappingView
#         where TargetTable = UPPER("{tablename}")
#     """)

#     srcTables = df.select(df.TgtTb, df.SrcTb)    \
#                         .distinct()    \
#                         .filter(df.SrcTb.isNotNull())    \
#                         .withColumn("alias", row_number().over(Window.partitionBy("TgtTb").orderBy("TgtTb")))
#     #srcTables.display()
    
#     lettersString = '_abcdefghijklmnopqrstuvwxyz'
#     letters = [x for x in letters]

#     for row in srcTables.collect():
#         tableLetter = letter[row.alias]
#         print(f'{row.SrcTb}s letter is {tableLetter}')
    
#     columnsQry = "SELECT \n"
#     joinedTbsQry = "SELECT \n"
#     for row in df.collect():
#         columnsQry = columnsQry + row.TgtColName + ",\n"  
#     columnsQry = columnsQry.strip().strip(',')
#     sqlQuery = "Select \n" + f"{sqlCode} \n" + f"from {tablename}"
    
    return sqlQuery

# COMMAND ----------

def AutomatedTargetQuery(tablename = ""):
    if GetDatabaseName().upper() != 'CLEANSED':
        tablename = GetNotebookName()
    elif tablename == "":
        tablename = GetSelfFqn()
        
    # mappingData = MAPPING_DOC
    if not MAPPING_DOC:
        mappingData = loadCuratedMapping().cache()
        mappingData.count()
    else:
        mappingData = MAPPING_DOC
    mappingData.createOrReplaceTempView("mappingView")

    df = spark.sql(f"""
        Select 
        DataType,
        TargetColumnName
        from mappingView
        where TargetTable = UPPER("{tablename}")
        and TargetColumnName not like '\_%'
    """)
    
    sqlCode = ""
    for row in df.collect():
        sqlCode = sqlCode + row.TargetColumnName + ",\n"    
            
    sqlCode = sqlCode.strip().strip(',')
    if GetDatabaseName().upper() == 'CLEANSED':
        sqlQuery = "Select \n" + f"{sqlCode} \n" + f"from {tablename}"
    else:
        sqlQuery = "Select \n" + f"{sqlCode} \n" + f"from {GetDatabaseName()}.{tablename}"
    
    return sqlQuery

# COMMAND ----------

def GetSrcTgtDfs(tablename):
    srcQuery = AutomatedSourceQuery(tablename)
    #print(srcQuery)
    tgtQuery = AutomatedTargetQuery(tablename)
    sourceDf = spark.sql(srcQuery).drop_duplicates()
    targetDf = spark.sql(tgtQuery)
    return sourceDf, targetDf

# COMMAND ----------

def GetColumns():
#     Required if Cleansed has mandatory cols (currently does not)
#     if GetDatabaseName().upper() == 'CLEANSED':
#         tablename = GetSelfFqn()
#     else:
#         tablename = GetNotebookName()

    tablename = GetNotebookName()
        
    mappingData = MAPPING_DOC
    mappingData.createOrReplaceTempView("mappingView")
    
    df = spark.sql(f"""
        Select 
        TargetColumnName
        from mappingView
        where TargetTable = UPPER("{tablename}")
    """)
    
    cols = ""
    for row in df.collect():
        if row.TargetColumnName is not None:
            cols = cols + row.TargetColumnName + ",\n" 
        else:
            print("\tWarning: Empty field detected in Target Column Name. Please check mapping.")         
            print("\tNote: Merged cells mapping may cause this error message.")           
    cols = cols.strip().strip(',')
    
    return cols

# COMMAND ----------

def DisplayFailedTest(df, count):
    if SHOW_ALL_FAILS == True:
        print(f"\tDisplaying failing records:")
        display(df)
    else:
        if count > 100: 
            print(f"\tDisplaying a sample (100 rows) of the failing records:")
            display(df.limit(100))
        else: 
            print(f"\tDisplaying all {count} failing records:")
            display(df)

# COMMAND ----------

# DBTITLE 1,Common Test Cases - Cleansed + Curated
def SchemaChk():
    targetDf = GetTargetSchemaDf()
    mappingDf = GetMappingSchemaDf()
    
    diff1 = targetDf.subtract(mappingDf)
    count = diff1.count()
    diff2 = mappingDf.subtract(targetDf)
    if diff2.count() > count: count = diff2.count()
    if count > 0: 
        print(f"\tTest Failed: Found mismatches between table schema and mapping.")
        print('Target Schema:')
        display(targetDf)
        print('Mapping Schema:')
        display(mappingDf)
        print(f'Schema Check mismatches for {GetNotebookName()}')
        print('Columns in Target that are not in Mapping (Target-Mapping):')
        display(diff1)
        print('Columns in Mapping that are not in Target (Mapping-Target):')
        display(diff2)

    Assert(count, 0, errorMessage = f"{count} mismatching fields found between table schema and mapping") 

# COMMAND ----------

def DuplicateKeysChk():
    keyColumns = UNIQUE_KEYS
    if keyColumns.strip() == "":
        Assert(0, 0, compare = "Greater Than", errorMessage = f"Test could not be executed, No Unique Key column/s were specified in the mapping.")
    else:
        if GetDatabaseName().upper() != "CLEANSED" and FD_OR_SD == 'SDUc3Plus':
            keyColumns = keyColumns + ', sourceValidFromTimestamp'
        elif GetDatabaseName().upper() != "CLEANSED":
            keyColumns = keyColumns + ', _RecordStart'
        
        count = -1
        try:
            df = spark.sql(f"""SELECT {keyColumns}, COUNT(*) as RecCount FROM {GetSelfFqn()} 
                            GROUP BY {keyColumns} HAVING COUNT(*) > 1""")
            count = df.count()
        except:
            if GetDatabaseName().upper() == "CURATED_V3" and FD_OR_SD == 'SDUc3Plus':
                keyColumns = UNIQUE_KEYS
                df = spark.sql(f"""SELECT {keyColumns}, sourceValidFromDatetime, COUNT(*) as RecCount FROM {GetSelfFqn()} 
                            GROUP BY {keyColumns}, sourceValidFromDatetime HAVING COUNT(*) > 1""")
                count = df.count()

        # count = df.count()
        if count > 0: 
            print(f"\tTest Failed: Found records with duplicate composite key combinations (Total: {count}).")
            DisplayFailedTest(df, count)
        
        Assert(count, 0, errorMessage = f"{count} duplicate composite keys found, expecting 0")

# COMMAND ----------

# the condition sourceSystemCode <> 'ACCESS' - needs to be removed when we have access data included as part of the testing. (20/12/2022) - GL 
def ExactDuplicates():
    table = spark.table(GetSelfFqn())
    df = table.select([c for c in table.columns if c not in {'_RecordStart', '_RecordEnd', '_RecordCurrent', '_RecordDeleted', '_DLCuratedZoneTimeStamp', '_BusinessKey'}])
    try:
        df = df.groupBy(df.columns)\
             .count()\
             .filter(col('sourceSystemCode') != 'ACCESS')\
             .filter(col('count') > 1)
    except:
        df = df.groupBy(df.columns)\
             .count()\
             .filter(col('count') > 1)
    
    count = df.count()
    if count > 0: 
        print(f"\tTest Failed: Found records with identical values for all fields (Total: {count}).")
        DisplayFailedTest(df, count)
    Assert(count, 0, errorMessage = f"{count} exact duplicate records found, expecting 0")

# COMMAND ----------

def AuditColsIncludedChk():
    df = spark.table(GetSelfFqn())
    
    if GetDatabaseName().upper() == 'CLEANSED':
        auditCols = ['_RECORDSTART', '_RECORDEND', '_RECORDCURRENT', '_RECORDDELETED', '_DLCLEANSEDZONETIMESTAMP']
    else:
        auditCols = ['_RECORDSTART', '_RECORDEND', '_RECORDCURRENT', '_RECORDDELETED', '_DLCURATEDZONETIMESTAMP', '_BUSINESSKEY']
    
    targetAuditCols = []
    for column in df.columns:
        if column.startswith("_") == True:
            targetAuditCols.append(column.upper())
    
    auditColsNotInTarget = [x for x in auditCols if x not in targetAuditCols]
    
    Assert(len(auditColsNotInTarget), 0, errorMessage = f"Missing audit column(s): {auditColsNotInTarget}")

# COMMAND ----------

def DateValidation():
    df = spark.sql(f"""SELECT * FROM {GetSelfFqn()} 
                   WHERE date(_RecordStart) > date(_RecordEnd)""")
    count = df.count()
    if count > 0: 
        print(f"\tTest Failed: Found records where _RecordStart > _RecordEnd (Total: {count}).")
        DisplayFailedTest(df, count)
    Assert(count, 0, errorMessage = f"{count} records where _RecordStart > _RecordEnd found, expecting 0")

# COMMAND ----------

def CountRecordsCnDn(C, D):
    try:
        df = spark.sql(f"""SELECT count(*) as RecCount FROM {GetSelfFqn()} 
                       WHERE _RecordCurrent = {C} and _RecordDeleted = {D}""")
        count = df.select("RecCount").collect()[0][0]

        if C == 1 and D == 0:
            Assert(count, 0, compare = "Greater Than", errorMessage = f"{count} active records in table, expecting 1 or more")
        else:
            Assert(count, 0, compare = "Greater Than Equal")
    except:
        Assert(0, 0, compare = "Greater Than", errorMessage = f"Test could not be executed, columns '_RecordCurrent' and/or '_RecordDeleted' not in table.")

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

def SrcVsTgtCountChk():
    if DO_ST_TESTS == False:
        if GetDatabaseName().upper() == 'CURATED' or GetDatabaseName().upper() == "CURATED_V2":
            source = 'cleansed'
        else:
            source = 'raw'

        tablename = GetSelfFqn().split(".")[1]

        sourceDf = spark.table(f"""{get_table_namespace(f'{source}',f'{tablename}')}""").drop_duplicates()
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

# DBTITLE 1,Curated Test Cases
def DuplicateKeysActiveRecsChk():
    keyColumns = UNIQUE_KEYS
    if keyColumns.strip() == "":
        Assert(0, 0, compare = "Greater Than", errorMessage = f"Test could not be executed, No Unique Key column/s were specified in the mapping.")
    else:
        if GetDatabaseName().upper() == "CURATED_V3" and FD_OR_SD == 'SDUc3Plus':
            keyColumns = keyColumns + ', sourceValidFromDatetime'
            
        df = spark.sql(f"""SELECT {keyColumns}, COUNT(*) as recCount FROM {GetSelfFqn()} 
                           WHERE _RecordCurrent=1 and _recordDeleted=0 
                           GROUP BY {keyColumns} HAVING COUNT(*) > 1""")
        count = df.count()
        if count > 0: 
            print(f"\tTest Failed: Found active records with duplicate composite key combinations (Total: {count}).")
            DisplayFailedTest(df, count)
        Assert(count, 0, errorMessage = f"{count} duplicate composite keys for active records found, expecting 0")

# COMMAND ----------

def DuplicateSKChk():
    skColumn = GetSelf().columns[0]
    df = spark.sql(f"SELECT {skColumn}, COUNT (*) as RecCount FROM {GetSelfFqn()} GROUP BY {skColumn} HAVING COUNT (*) > 1")
    count = df.count()
    if count > 0: 
        print(f"\tTest Failed: Found records with duplicate {skColumn}'s (Total: {count}).")
        DisplayFailedTest(df, count)
    Assert(count, 0, errorMessage = f"{count} Duplicate {skColumn}'s found, expecting 0")

# COMMAND ----------

def DuplicateSKActiveRecsChk():
    skColumn = GetSelf().columns[0]
    df = spark.sql(f"""SELECT {skColumn}, COUNT (*) as RecCount FROM {GetSelfFqn()} 
                       WHERE _RecordCurrent = 1 and _RecordDeleted = 0 
                       GROUP BY {skColumn} HAVING COUNT(*) > 1""")
    count = df.count()
    if count > 0: 
        print(f"\tTest Failed: Found active records with duplicate {skColumn}'s (Total: {count}).")
        DisplayFailedTest(df, count)
    Assert(count, 0, errorMessage = f"{count} Duplicate {skColumn}'s found for active records, expecting 0")

# COMMAND ----------

def SKNotNullOrBlankChk():
    skColumn = GetSelf().columns[0]
    df = spark.sql(f"SELECT COUNT (*) as RecCount FROM {GetSelfFqn()} WHERE {skColumn} is null or {skColumn} = '' or {skColumn} = ' ' or upper({skColumn}) ='NULL'")
    count = df.select("RecCount").collect()[0][0]
    
    if count > 0: 
        keyColumns = UNIQUE_KEYS
        df = spark.sql(f"""SELECT {skColumn}, {keyColumns} FROM {GetSelfFqn()} 
                           WHERE {skColumn} is null or {skColumn} = '' or {skColumn} = ' ' or upper({skColumn}) ='NULL'""")
        print(f"\tTest Failed: Found Nulls and/or Blank values in SK column - {skColumn} (Total: {count}).")
        DisplayFailedTest(df, count)
        
    Assert(count, 0, errorMessage = f"{count} Null/Blank values in SK column found, expecting 0")

# COMMAND ----------

def DuplicateBusinessKeyChk():
    df = spark.sql(f"SELECT _BusinessKey, COUNT (*) as RecCount FROM {GetSelfFqn()} GROUP BY _BusinessKey HAVING COUNT (*) > 1")
    count = df.count()
    if count > 0: 
        print(f"\tTest Failed: Found records with duplicate _BusinessKey values (Total: {count}).")
        DisplayFailedTest(df, count)
    Assert(count, 0, errorMessage = f"{count} Duplicate _BusinessKey values found, expecting 0")

# COMMAND ----------

def BusinessKeyNotNullOrBlankChk():
    df = spark.sql(f"""SELECT COUNT (*) as RecCount FROM {GetSelfFqn()}
                       WHERE (_BusinessKey is NULL or _BusinessKey in ('',' ') or UPPER(_BusinessKey)='NULL') """)
    count = df.select("RecCount").collect()[0][0]
    if count > 0: 
        keyColumns = UNIQUE_KEYS
        df = spark.sql(f"""SELECT {keyColumns}, _BusinessKey FROM {GetSelfFqn()} 
                       WHERE (_BusinessKey is NULL or _BusinessKey in ('',' ') or UPPER(_BusinessKey)='NULL') """)
        print(f"\tTest Failed: Found Nulls and/or Blank values in _BusinessKey column (Total: {count}).")
        DisplayFailedTest(df, count)
    Assert(count, 0, errorMessage = f"{count} Null/Blank values in _BusinessKey found, expecting 0")

# COMMAND ----------

def BusinessKeyFormatChk1():
    keyColumns = UNIQUE_KEYS
    bkQry = ''
    if keyColumns.strip() == "":
        Assert(0, 0, compare = "Greater Than", errorMessage = f"Test could not be executed, No Unique Key column/s were specified in the mapping.")
    else:
        keysList = keyColumns.split(',')
        for k in keysList:
            if k == keysList[0]:
                bkQry = f"when(col('{k}').isNull(), lit('')).otherwise(col('{k}'))"
                if len(keysList) != 1:
                    bkQry = f"{bkQry}, lit('|'), \\"
            else:
                key = k.strip()
                bkQry = f"{bkQry}\n\twhen(col('{key}').isNull(), lit('')).otherwise(col('{key}'))"
                if k != keysList[len(keysList) - 1]:
                    bkQry = f"{bkQry}, lit('|'), \\"

        bkQry = f"concat({bkQry}).alias('_BusinessKey')"            
        # print(bkQry)


    df = spark.table(GetSelfFqn())
    atfBK = df.select(eval(bkQry))
    tbBK = df.select('_BusinessKey')

    diff1 = atfBK.subtract(tbBK)
    count = diff1.count()
    diff2 = tbBK.subtract(atfBK)
    if diff2.count() > count: count = diff2.count()
    if count > 0: 
        print(f"\tTest Failed: Unexpected _BusinessKey values found - Observed mismatches between _BusinessKey recreated by ATF vs target table.")
        print('Mismatches: _BusinessKey values in Target:')
        display(diff2)
        print('Mismatches: _BusinessKey values recreated by ATF:')
        display(diff1)
    
    Assert(count, 0, errorMessage = f"{count} Unexpected _BusinessKey values found") 

# COMMAND ----------

def BusinessKeyFormatChk():
    keyColumns = UNIQUE_KEYS
    manColumns = MANDATORY_COLS
    sqlQry = 'SELECT\n'
    if keyColumns.strip() == "":
        Assert(0, 0, compare = "Greater Than", errorMessage = f"Test could not be executed, No Unique Key column/s were specified in the mapping.")
    else:
        keysList, manlist = keyColumns.split(','), manColumns.split(',')
        newKeysList, newManList = [], []
        for i in keysList:
            newKeysList.append(i.strip().strip(','))
        for i in manlist:
            newManList.append(i.strip().strip(','))

        # print('New keys list is:')
        # print(newKeysList)
        # print('length of keys list is: ' +str(len(newKeysList)))
        
        
        # print('New Man list is:')
        # print(newManList)

        manSet = set(newManList)

        if len(newKeysList) == 1:
            sqlQry = f'select {newKeysList[0]} as _BusinessKey from {GetSelfFqn()}'
        else:
            sqlCode = ''
            manColFlag = 0
            for k in newKeysList:
                # k = k.strip()
                # print('current key:\n' + k)
                if k in manSet and manColFlag == 0:
                    sqlCode = sqlCode + f'{k},\n'
                    manColFlag = 1
                elif manColFlag == 0 and k == newKeysList[-1]:
                    sqlCode = sqlCode + f"coalesce({k}, ''),\n"
                elif manColFlag == 0:
                    sqlCode = sqlCode + f"coalesce(concat({k}, '|'), ''),\n"
                else:
                    sqlCode = sqlCode + f"coalesce(concat('|', {k}), ''),\n"
                # elif manColFlag == 0 and k == keyList[-1]:

            sqlCode = sqlCode.strip().strip(',')
            sqlQry = sqlQry + f"concat({sqlCode}) as _BusinessKey from {GetSelfFqn()}"
        
        # print('\nsqlQry is:')
        # print(sqlQry)

        src = spark.sql(sqlQry)
        tgt = spark.table(GetSelfFqn()).select('_BusinessKey')
        # display(src)

    diff1 = src.subtract(tgt)
    count = diff1.count()
    diff2 = tgt.subtract(src)
    if diff2.count() > count: count = diff2.count()
    if count > 0: 
        print(f"\tTest Failed: Unexpected _BusinessKey values found - Observed mismatches between _BusinessKey recreated by ATF vs target table.")
        print('Mismatches: _BusinessKey values in Target:')
        display(diff2)
        print('Mismatches: _BusinessKey values recreated by ATF:')
        display(diff1)
    
    Assert(count, 0, errorMessage = f"{count} Unexpected _BusinessKey values found") 

# COMMAND ----------

def MandatoryColsNotNullorBlankCk():
    manCols = MANDATORY_COLS
    if manCols.strip() == "":
        Assert(0, 0, compare = "Greater Than", errorMessage = f"Test could not be executed, No Mandatory column/s were specified in the mapping.")
    else:
        manColsList = manCols.split(",")
        firstColumn = manColsList[0].strip()
        sqlQuery = f"SELECT {manCols} FROM {GetSelfFqn()} "

        for column in manColsList:
            column = column.strip()
            if column == firstColumn:
                sqlQuery = sqlQuery + f"WHERE ({column} is NULL or {column} in ('',' ') or UPPER({column})='NULL') "
            else:
                sqlQuery = sqlQuery + f"OR ({column} is NULL or {column} in ('',' ') or UPPER({column})='NULL') "

        df = spark.sql(sqlQuery)
        count = df.count()
        if count > 0: 
            print(f"\tTest Failed: Found Nulls and/or Blank values in Mandatory column (Total: {count}).")
            DisplayFailedTest(df, count)
        Assert(count, 0, errorMessage = f"{count} Null/Blank values in one or more Mandatory columns found, expecting 0")

# COMMAND ----------

# Previously named ManualDateCheck_1
def DateValidationSD1():
    driver = FD_OR_SD
    if driver == 'SD':
        df = spark.sql(f"""SELECT * FROM {GetSelfFqn()} 
                       WHERE ValidFromDate > ValidToDate""")
        count = df.count()
        
        if count > 0: 
            print(f"\tTest Failed: Found records where ValidFromDate > ValidToDate (Total: {count}).")
            DisplayFailedTest(df, count)
        
        Assert(count, 0, errorMessage = f"{count} records where ValidFromDate > ValidToDate found, expecting 0")

# COMMAND ----------

# Previously named ManualDateCheck_2
def DateValidationSD2():
    driver = FD_OR_SD
    if driver == 'SD':
        df = spark.sql(f"""SELECT * FROM {GetSelfFqn()} 
                       WHERE (ValidFromDate <> date(_RecordStart)) OR  (ValidToDate <> date(_RecordEnd))""")
        count = df.count()
        if count > 0: 
            print(f"\tTest Failed: Found records where ValidFromDate/ValidToDate is not equal to date(_RecordStart/_RecordEnd) (Total: {count}).")
            DisplayFailedTest(df, count)
        Assert(count, 0, errorMessage = f"{count} records where ValidFromDate/ValidToDate is not equal to date(_RecordStart/_RecordEnd), expecting 0")

# COMMAND ----------

def OverlapTimeStampValidation():
    keyColumns = UNIQUE_KEYS
    driver = FD_OR_SD
    df = ''
    if driver == 'FD':
        df = spark.sql(f"SELECT {keyColumns}, start_datetm, end_datetm \
                   FROM (SELECT {keyColumns}, _recordStart as start_datetm, _recordEnd as end_datetm, \
                   max(_recordStart) over (partition by {keyColumns} order by _recordStart rows between 1 following and 1 following) as nxt_datetm \
                   FROM {GetSelfFqn()} WHERE _recordDeleted=0) \
                   WHERE  DATEDIFF(second,end_datetm,nxt_datetm) <> 1") # can check for <> 9999-12-31
        count = df.count()
        if count > 0: 
            print(f"\tTest Failed: Found records where _RecordStart and _RecordEnd dates/timestamps overlap or have gaps for the same composite key (Total: {count}).")
            DisplayFailedTest(df, count)
        Assert(count, 0, errorMessage = f"{count} records with dates/timestamps that overlap or have gaps, expecting 0")
        
    elif driver == 'SD':
        df = spark.sql(f"SELECT {keyColumns}, start_date, end_date \
                   FROM (SELECT {keyColumns}, date(_recordStart) as start_date, date(_recordEnd) as end_date, \
                   max(date(_recordStart)) over (partition by {keyColumns} order by _recordStart rows between 1 following and 1 following) as nxt_date \
                   FROM {GetSelfFqn()} ) \
                   WHERE  DATEDIFF(day, nxt_date, end_date) <> 1")  ## added _recordDeleted=0 to pick non deleted records.
        count = df.count()
        if count > 0: 
            print(f"\tTest Failed: Found records where _RecordStart and _RecordEnd dates/timestamps overlap or have gaps for the same composite key  (Total: {count}).")
            DisplayFailedTest(df, count)
        Assert(count, 0, errorMessage = f"{count} records with with dates/timestamps that overlap or have gaps, expecting 0")

    elif driver == 'SDUc3Plus':
        df = spark.sql(f"SELECT {keyColumns}, start_datetm, end_datetm \
                   FROM (SELECT {keyColumns}, sourceValidFromTimestamp as start_datetm, sourceValidToTimestamp as end_datetm, \
                   max(sourceValidFromTimestamp) over (partition by {keyColumns} order by sourceValidFromTimestamp rows between 1 following and 1 following) as nxt_datetm \
                   FROM {GetSelfFqn()} WHERE _recordDeleted=0) \
                   WHERE  DATEDIFF(millisecond,end_datetm,nxt_datetm) <> 1") # can check for <> 9999-12-31
        count = df.count()
        if count > 0: 
            print(f"\tTest Failed: Found records where _RecordStart and _RecordEnd dates/timestamps overlap or have gaps for the same composite key (Total: {count}).")
            DisplayFailedTest(df, count)
        Assert(count, 0, errorMessage = f"{count} records with dates/timestamps that overlap or have gaps, expecting 0")   
    
    else:
        Assert(0, 0, compare = "Greater Than", errorMessage = f"Table driver was not in the correct format or was not supplied")
    

# COMMAND ----------

def AuditEndDateChk():
    if FD_OR_SD == 'FD':
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
    if FD_OR_SD == 'FD':
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
    if FD_OR_SD == 'FD':
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
    if FD_OR_SD == 'FD':
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

# COMMAND ----------

def DummyRecChk():
    skColumn = GetSelf().columns[0]
    print(skColumn)
    try:
        df = spark.sql(f"SELECT * FROM {GetSelfFqn()} WHERE {skColumn} = '-1'")
        display(df)
        count = df.count()
        if count > 1: 
            print(f"\tTest Failed: Found {count} dummy records, expecting only 1.")
            display(df)
        Assert(count, 1, errorMessage = f"{count} dummy records found, expecting 1")
    # added to handle dim tables which do not have column 'sourceSystemCode' - dimWaterNetwork, dimSewerNetwork, dimStormWaterNetwork
    except:
        try:
            df = spark.sql(f"SELECT * FROM {GetSelfFqn()} WHERE sourceSystemCode is NULL")
            count = df.count()
            if count > 1: 
                print(f"\tTest Failed: Found {count} dummy records, expecting only 1.")
                display(df)
            Assert(count, 1, errorMessage = f"{count} dummy records found, expecting 1")
        except:
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
                        sqlQuery = sqlQuery + f"WHERE {column} = 'Unknown' OR {column} = 'UNKNOWN'"
                    else:
                        sqlQuery = sqlQuery + f"OR {column} = 'Unknown' OR {column} = 'UNKNOWN'"
                
                df = spark.sql(sqlQuery)
                count = df.count()
                if count > 1: 
                    print(f"\tTest Failed: Found {count} dummy records, expecting only 1.")
                    display(df)
                Assert(count, 1, errorMessage = f"{count} dummy records found, expecting 1")

# COMMAND ----------

# DBTITLE 1,Fact Table Test Cases
def FKColsNotNullorBlankCk():
    fkCols = GetForeignKeyCols()
    if fkCols.strip() == "":
        Assert(0, 0, compare = "Greater Than", errorMessage = f"Test could not be executed, No Foreign Key column/s were specified in the mapping.")
    else:
        fkColsList = fkCols.split(",")
        firstColumn = fkColsList[0].strip()
        sqlQuery = f"SELECT {fkCols} FROM {GetSelfFqn()} "

        for column in fkColsList:
            column = column.strip()
            if column == firstColumn:
                sqlQuery = sqlQuery + f"WHERE ({column} is NULL or {column} in ('',' ') or UPPER({column})='NULL') "
            else:
                sqlQuery = sqlQuery + f"OR ({column} is NULL or {column} in ('',' ') or UPPER({column})='NULL') "

        df = spark.sql(sqlQuery)
        count = df.count()
        if count > 0: 
            print(f"\tTest Failed: Found Nulls and/or Blank values in one or more FK columns (Total: {count}).")
            DisplayFailedTest(df, count)
        Assert(count, 0, errorMessage = f"{count} Null/Blank values in one or more FK columns found, expecting 0")

# COMMAND ----------

# DBTITLE 1,Testing Team Test Cases - S-T checks
def ST_CountChk_AllRecs():
    if DO_ST_TESTS == True:
#         if GetDatabaseName().upper() != 'CLEANSED':
#             SRC_DF = spark.sql(SOURCE_QUERY)
            
        sourceDf, targetDf = SRC_DF, TGT_DF

        sourceCount = sourceDf.count()
        targetCount = targetDf.count()

        Assert(sourceCount, targetCount)

# COMMAND ----------

def SmT_Chk_AllRecs():    
    if DO_ST_TESTS == True:
#         if GetDatabaseName().upper() != 'CLEANSED':
#             SRC_DF = spark.sql(SOURCE_QUERY)
        
        sourceDf, targetDf = SRC_DF, TGT_DF

        df = sourceDf.subtract(targetDf)
        count = df.count()
        if count > 0: 
            print(f'Records that are in source but not in target ({count} total records): ')
            display(df)

        Assert(count, 0, errorMessage = f"""Mismatching records found - {count} records are in source but not in target""")

# COMMAND ----------

def TmS_Chk_AllRecs():   
    if DO_ST_TESTS == True:
#         if GetDatabaseName().upper() != 'CLEANSED':
#             SRC_DF = spark.sql(SOURCE_QUERY)

        sourceDf, targetDf = SRC_DF, TGT_DF
            
        df = targetDf.subtract(sourceDf)
        count = df.count()
        if count > 0: 
            print(f'Records that are in target but not in source ({count} total records): ')
            display(df)

        Assert(count, 0, errorMessage = f"""Mismatching records found - {count} records are in target but not in source""")

# COMMAND ----------

# DBTITLE 1,Testing Team Test Cases - Other
def KeysNotNullOrBlankChk():
    if TST_TM_CHKS == True or GetDatabaseName().upper() == 'CLEANSED':
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

def KeysAreSameLengthChk():
    if TST_TM_CHKS == True:
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

# DBTITLE 1,Helper Functions - Curated S-T Tests
def GetTargetDf():
    columns = ALL_COLS
    target = spark.sql(f"""
    SELECT {columns}
    ,_RecordStart
    ,_RecordEnd
    ,_RecordCurrent
    ,_RecordDeleted
    FROM 
    {GetSelfFqn()}
    """) 
    return target

# COMMAND ----------

def GetJoinQuery():
    keyColumns = UNIQUE_KEYS
    keyColsList = keyColumns.split(",")
    
    NoDateColsStr = ""
    NoDateColsList = []
    for column in keyColsList:
        if (column.upper().find('DATE') == -1): # if it is NOT a date col, add to NoDateColsList
            NoDateColsList.append(column)
            NoDateColsStr = NoDateColsStr + column + ", "
    
    NoDateColsStr = NoDateColsStr.strip(" ,")
    
    firstColumn = NoDateColsList[0].strip()

    sqlQuery = f"LEFT JOIN (Select {NoDateColsStr}, date(max(_RecordEnd)) as max_actEnd from sourceact group by {NoDateColsStr}) as c on "

    for column in NoDateColsList:
        column = column.strip()
        if column == firstColumn:
            sqlQuery = sqlQuery + f"c.{column} = a.{column} "
        else:
            sqlQuery = sqlQuery + f"AND c.{column} = a.{column} " 
    
    return sqlQuery

# COMMAND ----------

def LoadSourceDfCnDn(srcQuery, C, D):
    columns = ALL_COLS
    source = spark.sql(srcQuery)
    source.createOrReplaceTempView("sourceView")
    
    if FD_OR_SD == 'SD':
        sourceDf = spark.sql(f"SELECT {columns} FROM sourceView as a ")
        return sourceDf
    else:
        if C == 1 and D == 0:
            sourceDf = spark.sql(f"""SELECT {columns}
                                 FROM sourceView as a,
                                 (SELECT date(max(_DLCuratedZoneTimeStamp)) as max_date FROM {GetSelfFqn()}) as b
                                 WHERE (date(a._RecordStart) <= b.max_date) and  (b.max_date<= date(a._RecordEnd))
                                 """)
        elif C == 0 and D == 0:
            sourceDf = spark.sql(f"""SELECT {columns}
                                 FROM sourceView as a,
                                 (SELECT date(max(_DLCuratedZoneTimeStamp)) as max_date FROM {GetSelfFqn()}) as b
                                 WHERE ((date(a._RecordStart) < b.max_date) and (b.max_date > date(a._RecordEnd))) or ((date(a._RecordStart) > b.max_date) and (b.max_date < date(a._RecordEnd)))
                                 """)

        elif C == 0 and D == 1:
            source_a = spark.sql(SOURCE_ACTIVE)
            source_a.createOrReplaceTempView("sourceact")  
            joinQuery = GetJoinQuery()
            sourceDf = spark.sql(f"""SELECT a.* except(_recordStart,_recordEnd) FROM (SELECT {columns}, _recordStart, _recordEnd
                                 FROM sourceView) as a, 
                                 (SELECT date(max(_DLCuratedZoneTimeStamp)) as max_date FROM {GetSelfFqn()}) as b 
                                 {joinQuery}
                                 WHERE (((date(a._RecordStart) < b.max_date) and (b.max_date > date(a._RecordEnd))) or ((date(a._RecordStart) > b.max_date) and (b.max_date < date(a._RecordEnd)))) 
                                 or (c.max_actEnd > date(a._RecordEnd))
                                 """)
        elif C == 1 and D == 1:
            source_a = spark.sql(SOURCE_ACTIVE)
            source_a.createOrReplaceTempView("sourceact")
            joinQuery = GetJoinQuery()
            sourceDf = spark.sql(f"""SELECT a.* except(_recordStart,_recordEnd) FROM (SELECT {columns}, _recordStart, _recordEnd
                                 FROM sourceView) as a, 
                                 (SELECT date(max(_DLCuratedZoneTimeStamp)) as max_date FROM {GetSelfFqn()}) as b 
                                 {joinQuery}
                                 WHERE ((date(a._RecordStart) <= b.max_date) and  (b.max_date<= date(a._RecordEnd))) and (coalesce(c.max_actEnd,'1900-01-01') < date(a._RecordEnd))
                                 """)
        return sourceDf

# COMMAND ----------

def LoadTargetDfCnDn(C = -1, D = -1):
    columns = ALL_COLS
    
    if FD_OR_SD == 'SD':
        target = SD_TARGET
    else:
        target = GetTargetDf()
    target.createOrReplaceTempView("targetView")
    
    sqlQuery = f"SELECT {columns} FROM targetView "
    if C != -1 and D != -1:
        sqlQuery = sqlQuery + f"WHERE _RecordCurrent = {C} and _RecordDeleted = {D} "
        
    # added case where we may only want to take either _RecordCurrent or _RecordDeleted, but not both
    # not currently used but may be useful in future
    elif C == -1:
        sqlQuery = sqlQuery + f"WHERE _RecordDeleted = {D} "
    elif D == -1:
        sqlQuery = sqlQuery + f"WHERE _RecordCurrent = {C} "
    
    # exclude dummy rec
    if (columns.upper().find('SOURCESYSTEMCODE') != -1):
        sqlQuery = sqlQuery + f"AND sourceSystemCode is not NULL "         # AND sourceSystemCode <> 'ACCESS'" 
    else: 
        keyColumns = UNIQUE_KEYS
        firstKeyColumn = keyColumns.split(",")[0].strip()
        sqlQuery = sqlQuery + f"AND {firstKeyColumn} <> 'Unknown' "        # for when "sourceSystemCode" is not a column in target table
    
    targetDf = spark.sql(sqlQuery) 

    return targetDf

# COMMAND ----------

# DBTITLE 1,Curated S-T tests
# Base Query for S-T Count Check
def Curated_ST_CountChkCnDn(srcQuery, C, D):
    if DO_ST_TESTS == True:    
        sourceDf = LoadSourceDfCnDn(srcQuery, C, D)
        targetDf = LoadTargetDfCnDn(C, D)

        sourceCount = sourceDf.count()
        targetCount = targetDf.count()

        Assert(sourceCount, targetCount)

# COMMAND ----------

# Base Query for Source minus Target
def Curated_SmT_ChkCnDn(srcQuery, C, D):
    if DO_ST_TESTS == True:
        sourceDf = LoadSourceDfCnDn(srcQuery, C, D)
        targetDf = LoadTargetDfCnDn(C, D)

        df = sourceDf.subtract(targetDf)
        count = df.count()
        if count > 0: display(df)

        Assert(count, 0)

# COMMAND ----------

# Base Query for Target minus Source
def Curated_TmS_ChkCnDn(srcQuery, C, D):
    if DO_ST_TESTS == True:
        sourceDf = LoadSourceDfCnDn(srcQuery, C, D)
        targetDf = LoadTargetDfCnDn(C, D)

        df = targetDf.subtract(sourceDf)
        count = df.count()
        if count > 0: display(df)

        Assert(count, 0) 

# COMMAND ----------

# S-T count checks with different C & D inputs (C = recCurrent, D = recDeleted)
def Curated_ST_CountChkC1D0():
    Curated_ST_CountChkCnDn(SOURCE_ACTIVE, 1, 0)
def Curated_ST_CountChkC0D0():
    if FD_OR_SD == 'FD': Curated_ST_CountChkCnDn(SOURCE_ACTIVE, 0, 0)
def Curated_ST_CountChkC1D1():
    if FD_OR_SD == 'FD': 
        Curated_ST_CountChkCnDn(SOURCE_DELETED, 1, 1)
    
def Curated_ST_CountChkC0D1():
    Curated_ST_CountChkCnDn(SOURCE_DELETED, 0, 1)

# COMMAND ----------

# S minus T checks with different C & D inputs (C = recCurrent, D = recDeleted)
def Curated_SmT_ChkC1D0():
    Curated_SmT_ChkCnDn(SOURCE_ACTIVE, 1, 0)
def Curated_TmS_ChkC1D0():
    Curated_TmS_ChkCnDn(SOURCE_ACTIVE, 1, 0)
    
def Curated_SmT_ChkC0D0(): 
    if FD_OR_SD == 'FD': Curated_SmT_ChkCnDn(SOURCE_ACTIVE, 0, 0)
def Curated_TmS_ChkC0D0():
    if FD_OR_SD == 'FD': Curated_TmS_ChkCnDn(SOURCE_ACTIVE, 0, 0)

def Curated_SmT_ChkC1D1(): 
    if FD_OR_SD == 'FD': Curated_SmT_ChkCnDn(SOURCE_DELETED, 1, 1)
def Curated_TmS_ChkC0D0():
    if FD_OR_SD == 'FD': Curated_TmS_ChkCnDn(SOURCE_DELETED, 1, 1)
        
def Curated_SmT_ChkC0D1(): 
    Curated_SmT_ChkCnDn(SOURCE_DELETED, 0, 1)
def Curated_TmS_ChkC0D1():
    Curated_TmS_ChkCnDn(SOURCE_DELETED, 0, 1)

# COMMAND ----------

def SourceTargetCountCheckC1D0():
    source_a = SOURCE_ACTIVE
    sourceDf = LoadSourceDfCnDn(source_a, 1, 0)
    targetDf = LoadTargetDfCnDn(1, 0)
    
    sourceCount = sourceDf.count()
    targetCount = targetDf.count()
    
    Assert(sourceCount, targetCount)

# COMMAND ----------

def SourceTargetCountCheckC0D0():
    if FD_OR_SD == 'FD':
        source_a = SOURCE_ACTIVE
        sourceDf = LoadSourceDfCnDn(source_a, 0, 0)
        targetDf = LoadTargetDfCnDn(0, 0)

        sourceCount = sourceDf.count()
        targetCount = targetDf.count()

        Assert(sourceCount, targetCount)

# COMMAND ----------

def SourceTargetCountCheckC1D1():
    if FD_OR_SD == 'FD':
        source_d = SOURCE_DELETED
        sourceDf = LoadSourceDfCnDn(source_d, 1, 1)
        targetDf = LoadTargetDfCnDn(1, 1)

        sourceCount = sourceDf.count()
        targetCount = targetDf.count()

        Assert(sourceCount, targetCount)

# COMMAND ----------

def SourceTargetCountCheckC0D1():
    source_d = SOURCE_DELETED
    sourceDf = LoadSourceDfCnDn(source_d, 0, 1)
    targetDf = LoadTargetDfCnDn(0, 1)
    
    sourceCount = sourceDf.count()
    targetCount = targetDf.count()
    
    Assert(sourceCount, targetCount)

# COMMAND ----------

def SourceMinusTargetC1D0():
    source_a = SOURCE_ACTIVE
    sourceDf = LoadSourceDfCnDn(source_a, 1, 0)
    targetDf = LoadTargetDfCnDn(1, 0)
    
    df = sourceDf.subtract(targetDf)
    count = df.count()
    if count > 0: display(df)
    
    Assert(count, 0)

# COMMAND ----------

def TargetMinusSourceC1D0():
    source_a = SOURCE_ACTIVE
    sourceDf = LoadSourceDfCnDn(source_a, 1, 0)
    targetDf = LoadTargetDfCnDn(1, 0)
    
    df = targetDf.subtract(sourceDf)
    count = df.count()
    if count > 0: display(df)
    
    Assert(count, 0) 

# COMMAND ----------

def SourceMinusTargetC0D0():
    if FD_OR_SD == 'FD':
        source_a = SOURCE_ACTIVE
        sourceDf = LoadSourceDfCnDn(source_a, 0, 0)
        targetDf = LoadTargetDfCnDn(0, 0)

        df = sourceDf.subtract(targetDf)
        count = df.count()
        if count > 0: display(df)

        Assert(count, 0)

# COMMAND ----------

def TargetMinusSourceC0D0():
    if FD_OR_SD == 'FD':
        source_a = SOURCE_ACTIVE
        sourceDf = LoadSourceDfCnDn(source_a, 0, 0)
        targetDf = LoadTargetDfCnDn(0, 0)

        df = targetDf.subtract(sourceDf)
        count = df.count()
        if count > 0: display(df)

        Assert(count, 0) 

# COMMAND ----------

def SourceMinusTargetC1D1():
    if FD_OR_SD == 'FD':
        source_d = SOURCE_DELETED
        sourceDf = LoadSourceDfCnDn(source_d, 1, 1)
        targetDf = LoadTargetDfCnDn(1, 1)

        df = sourceDf.subtract(targetDf)
        count = df.count()
        if count > 0: display(df)

        Assert(count, 0)

# COMMAND ----------

def TargetMinusSourceC1D1():
    if FD_OR_SD == 'FD':
        source_d = SOURCE_DELETED
        sourceDf = LoadSourceDfCnDn(source_d, 1, 1)
        targetDf = LoadTargetDfCnDn(1, 1)

        df = targetDf.subtract(sourceDf)
        count = df.count()
        if count > 0: display(df)

        Assert(count, 0) 

# COMMAND ----------

def SourceMinusTargetC0D1():
    source_d = SOURCE_DELETED
    sourceDf = LoadSourceDfCnDn(source_d, 0, 1)
    targetDf = LoadTargetDfCnDn(0, 1)
    
    df = sourceDf.subtract(targetDf)
    count = df.count()
    if count > 0: display(df)
    
    Assert(count, 0)

# COMMAND ----------

def TargetMinusSourceC0D1():
    source_d = SOURCE_DELETED
    sourceDf = LoadSourceDfCnDn(source_d, 0, 1)
    targetDf = LoadTargetDfCnDn(0, 1)
    
    df = targetDf.subtract(sourceDf)
    count = df.count()
    if count > 0: display(df)
    
    Assert(count, 0) 

# COMMAND ----------

# DOC_PATH = ''
# SHEET_NAME = ''

# MAPPING_DOC = loadCuratedMapping() loadCleansedMapping() 
# mappingData = MAPPING_DOC
# mappingData.createOrReplaceTempView("mappingView")
# mappingData.display()


# TAG_SHEET = loadCleansedMapping('TAG')
# tags = TAG_SHEET
# tags.createOrReplaceTempView("tagView")
# tags.display()

# tablename = 
