# Databricks notebook source
def PrintHelpFunc():
    print("List of helper functions available to use:")
    print("RunATFGivenRange(database, tbListDf, start, finish)  - Run ATF for table numbers in the user supplied range (start - finish)")
    print("RunATFGivenTbNums(database, tbListDf, tbNumsSet)     - Run ATF for table numbers in the user supplied set")
    print("RunATFAllTables(database, tbListDf)                  - Run ATF for all tables")
    print("DisplayTbRange(database, tbListDf, start, finish)    - Display data for table numbers in the supplied range")
    print("DisplayTbSetNums(database, tbListDf, tbNumsSet)      - Display data for table numbers in the supplied set")
    print("PrintSchemaRange(database, tbListDf, start, finish)  - Print schema for table numbers in the supplied range")
    print("PrintSchemaSetNums(database, tbListDf, tbNumsSet)    - Print schema for table numbers in the supplied set")
    print("DisplayDistinct(database, tbName)                    - Display distinct values of all columns in the supplied table")
    print("TestPlanNames(tbListDf)                              - Print the ALM Test Plan names for the tables in the supplied dataframe")
    print("CompareCounts(sourceDf, targetDf)                    - Print the counts of the recreated source and the target curated table")
    print("S_TandT_S(sourceDf, targetDf)                        - Compare the data between the source and target, and display records that are one table but not the other")
    print("S_TChecks(sourceDf, targetDf)                        - Do both CompareCounts and S_TChecks")
    print("DeleteAllMappings(folder)                            - Delete the specified folder from DBFS: dbfs:/FileStore/{folder}")
    print("DeleteTableList()                                    - Delete excel file with curated table info: dbfs:/FileStore/dimTableList.xlsx")
    print("DeleteMapping(tbname)                                - Delete mapping of the specified table: dbfs:/FileStore/UCX_mapping/{tbname}'")
    print("DisplayMappingGivenRange(tbListDf, start, finish)    - Display mapping for table numbers user supplied range (start - finish)")
    print("DisplayMappingGivenTbNums(tbListDf, tbNumsSet)       - Display mapping for table numbers in the supplied set")
    print("PrintHelpFunc()                                      - Print the available helper functions")

    print("The scripts for the above functions can be found in /Users/o0dc@sydneywater.com.au/ATF/extensions/helper_functions")

PrintHelpFunc()

# COMMAND ----------

def RunATFGivenRange(database, tbListDf, start, finish):
    global TABLE_FQN, DOC_PATH, SHEET_NAME, MAPPING_DOC, FD_OR_SD
    df = tbListDf

    print(f"Total number of tables in sourceSystem: {df.count()}. Running tests for the following tables (no.{start} - {finish}):")    
    for tb in df.collect()[start - 1:finish]: 
        TABLE_FQN = f"{database}.{tb.Tablename}"
        print(TABLE_FQN)
        
    print('')
    for tb in df.collect()[start - 1:finish]: 
        try:
            TABLE_FQN = f"{database}.{tb.Tablename}"
            print(f"Running tests for table: {TABLE_FQN}...")
            DOC_PATH, SHEET_NAME = tb.MappingPath, tb.Sheetname
            FD_OR_SD = tb.FD_OR_SD
            MAPPING_DOC = loadCuratedMapping().cache()
            MAPPING_DOC.count()
            RunTests()
            print("\n\n")
        except:
            # print(f"Error: Execution of ATF for {TABLE_FQN} has failed.")
            try:
                TABLE_FQN = f"curated_v3.{tb.Tablename}"
                print(f"Running tests for table: {TABLE_FQN}...")
                DOC_PATH, SHEET_NAME = tb.MappingPath, tb.Sheetname
                FD_OR_SD = tb.FD_OR_SD
                MAPPING_DOC = loadCuratedMapping().cache()
                MAPPING_DOC.count()
                RunTests()
                print("\n\n")
            except:
                print(f"Error: Execution of ATF for {TABLE_FQN} has failed.")

                

        try:
            ClearCache()
        except:
            pass

    df.unpersist()

# COMMAND ----------

def RunATFGivenTbNums(database, tbListDf, tbNumsSet):   
    global TABLE_FQN, DOC_PATH, SHEET_NAME, MAPPING_DOC, FD_OR_SD
    df = tbListDf
    currNum = 1

    print(f"Running tests for the following tables:")  
    for tb in df.collect(): 
        if currNum in tbNumsSet:
            TABLE_FQN = f"{database}.{tb.Tablename}"
            print(TABLE_FQN)
        currNum+=1
        
    print('')
    currNum = 1
    for tb in df.collect(): 
        if currNum in tbNumsSet:
            try:
                TABLE_FQN = f"{database}.{tb.Tablename}"
                print(f"Running tests for table: {TABLE_FQN}...")
                DOC_PATH, SHEET_NAME = tb.MappingPath, tb.Sheetname
                FD_OR_SD = tb.FD_OR_SD
                print("This table is " + FD_OR_SD)
                MAPPING_DOC = loadCuratedMapping().cache()
                MAPPING_DOC.count()
                RunTests()
                print("\n\n")
            except:
                print(f"Error: Execution of ATF for {TABLE_FQN} has failed.")
                try:
                    TABLE_FQN = f"curated_v3.{tb.Tablename}"
                    print(f"Running tests for table: {TABLE_FQN}...")
                    DOC_PATH, SHEET_NAME = tb.MappingPath, tb.Sheetname
                    FD_OR_SD = tb.FD_OR_SD
                    MAPPING_DOC = loadCuratedMapping().cache()
                    MAPPING_DOC.count()
                    RunTests()
                    print("\n\n")
                except:
                    print(f"Error: Execution of ATF for {TABLE_FQN} has failed.")
            
            try:
                ClearCache()
            except:
                pass

        currNum+=1
        continue


# COMMAND ----------

def RunATFAllTables(database, tbListDf):
    global TABLE_FQN, DOC_PATH, SHEET_NAME, MAPPING_DOC, FD_OR_SD
    df = tbListDf
    
    for tb in df.collect(): 
        try:
            TABLE_FQN = f"{database}.{tb.Tablename}"
            print(f"Running tests for table: {TABLE_FQN}...")
            DOC_PATH, SHEET_NAME = tb.MappingPath, tb.Sheetname
            FD_OR_SD = tb.FD_OR_SD
            MAPPING_DOC = loadCuratedMapping().cache()
            MAPPING_DOC.count()
            RunTests()
            print("\n\n")
        except:
            print(f"Error: Execution of ATF for {TABLE_FQN} has failed.")

        try:
            ClearCache()
        except:
            pass


# COMMAND ----------

def DisplayTbRange(database, tbListDf, start = 1, finish = 10):
    for tb in tbListDf.collect()[start-1:finish]:
        print(f"Displaying table: {database}.{tb.Tablename}")
        spark.table(f"{database}.{tb.Tablename}").display()
        print("\n")

# COMMAND ----------

def DisplayTbSetNums(database, tbListDf, tbNumsSet):
    currNum = 1
    for tb in tbListDf.collect():
        if currNum in tbNumsSet:
            print(f"Displaying table: {database}.{tb.Tablename}")
            spark.table(f"{database}.{tb.Tablename}").display()
            print("\n")
        currNum+=1

# COMMAND ----------

def PrintSchemaRange(database, tbListDf, start = 1, finish = 10):
    for tb in tbListDf.collect()[start-1:finish]:
        print(f"Printing schema for table: {database}.{tb.Tablename}")
        spark.table(f"{database}.{tb.Tablename}").printSchema()
        print("\n")


# COMMAND ----------

def PrintSchemaSetNums(database, tbListDf, tbNumsSet):
    currNum = 1
    for tb in tbListDf.collect():
        if currNum in tbNumsSet:
            print(f"Printing schema for table: {database}.{tb.Tablename}")
            spark.table(f"{database}.{tb.Tablename}").printSchema()
            print("\n")
        currNum+=1

# COMMAND ----------

def DisplayRecordStart(database, tbListDf):
    for tb in tbListDf.collect():
        print(f"Distinct _recordStart values in {database}.{tb.Tablename}")
        spark.table(f"{database}.{tb.Tablename}").select("_recordStart").distinct().display()
    print("\n")

# COMMAND ----------

def DisplayDistinct(database, tbName):
    df = spark.table(f"{database}.{tbName}")
    
    for column in df.columns:
        print(f"{tbName} - Displaying distinct values for field: {column}")
        dd = df.select(column).distinct().display()
    print("\n")

# COMMAND ----------

def TestPlanNames(tbListDf):
    for tb in tbListDf.collect():
        print(f"{tb.JiraNum}_{tb.Tablename}_Verify day 0 data for {tb.Tablename}")
        # print("\n")

# COMMAND ----------

def CompareCounts(sourceDf, targetDf):
    sourceCount = sourceDf.count()
    targetCount = targetDf.count()
    print("source count is: " + str(sourceCount))
    print("target count is: " + str(targetCount))

def S_TandT_S(sourceDf, targetDf):
    s_t = sourceDf.subtract(targetDf)
    print('Number of records in source not in target: ' + str(s_t.count()))
    print('Records in source not in target: ')
    display(s_t)
    
    t_s = targetDf.subtract(sourceDf)
    print('Number of records in target not in source: ' + str(t_s.count()))
    print('Records in target not in source: ')
    display(t_s)

def S_TChecks(sourceDf, targetDf):
    CompareCounts(sourceDf, targetDf)
    S_TandT_S(sourceDf, targetDf)

# COMMAND ----------

def DeleteAllMappings(folder):
    dbutils.fs.rm(f'dbfs:/FileStore/{folder}', True)
    dbutils.fs.mkdirs(f'dbfs:/FileStore/{folder}')

def DeleteTableList():
    dbutils.fs.rm('dbfs:/FileStore/dimTableList.xlsx', True)

def DeleteMapping(tbname):
    df = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("dataAddress", "TableList!A1") \
    .load("dbfs:/FileStore/dimTableList.xlsx") #.display()

    tb = df.filter(df.Tablename == tbname).select("MappingPath", "UseCase")
    uc = tb.UseCase[:3]
    dbutils.fs.rm(f'dbfs:/FileStore/{uc}_mapping/{tb.Tablename}', True)


# COMMAND ----------

def DisplayMappingGivenRange(tbListDf, start, finish): 
    global TABLE_FQN, DOC_PATH, SHEET_NAME, MAPPING_DOC, FD_OR_SD
    df = tbListDf

    for tb in df.collect()[start - 1:finish]: 
        try:
            print(f"Displaying mapping document for table: {tb.Tablename}...")
            DOC_PATH, SHEET_NAME = tb.MappingPath, tb.Sheetname
            FD_OR_SD = tb.FD_OR_SD
            MAPPING_DOC = loadCuratedMapping().cache()
            MAPPING_DOC.display()
        except:
            print(f"Error: Mapping document for {tb.Tablename} could not be loaded.")                

        try:
            ClearCache()
        except:
            pass

    df.unpersist()

# COMMAND ----------

def DisplayMappingGivenTbNums(tbListDf, tbNumsSet): 
    global TABLE_FQN, DOC_PATH, SHEET_NAME, MAPPING_DOC, FD_OR_SD
    df = tbListDf

    currNum = 1
    for tb in df.collect(): 
        if currNum in tbNumsSet:
            try:
                print(f"Displaying mapping document for table: {tb.Tablename}...")
                DOC_PATH, SHEET_NAME = tb.MappingPath, tb.Sheetname
                FD_OR_SD = tb.FD_OR_SD
                MAPPING_DOC = loadCuratedMapping().cache()
                MAPPING_DOC.display()
            except:
                print(f"Error: Mapping document for {tb.Tablename} could not be loaded.")
                
            try:
                ClearCache()
            except:
                pass

        currNum+=1
        continue

# COMMAND ----------


