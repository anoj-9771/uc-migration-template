# Databricks notebook source
# MAGIC %run ./common-cleansed

# COMMAND ----------

# MAGIC %run ../Common/common-include-all

# COMMAND ----------

def PreviewTransform(systemCode, tableName, showTransform=True):
    from pprint import pprint
    # 1. DISPLAY RAW TABLE
    if showTransform:
        display(spark.table(tableName))
    
    # 2. DISPLAY CLEANSED TABLE
    df = CleansedTransform(None, tableName, systemCode, showTransform)
    display(df)
    
    # 3. DISPLAY SCHEMA
    if showTransform:
        pprint(df.schema.fields)

# COMMAND ----------

def PreviewLookup(lookupTag):
    table, key, value, refaultReturn, whereclause = lookupTags.get(lookupTag) if "|" not in lookupTag else lookupTag.split("|")
    display(spark.table(table).selectExpr(f"{key} Key", f"{value} Value").where("Value IS NOT NULL"))

# COMMAND ----------

def ValidateLookups():
    for i in lookupTags:
        tag = i
        table, key, value, returnNull, whereClause = lookupTags.get(i)
        try:
            spark.table(table).selectExpr(f"{key} Key", f"{value} Value")
            print(f"[ OK ] - {tag} - {table}")
        except Exception as error:
            print(f"[ !! ] - {tag} - {error}")

# COMMAND ----------

def DisplayAllTags():
    list = []

    for key, value in [
        ["DefaultTransform", defaultTransformTags]
        ,["DefaultDataTypes", defaultDataTypes]
        ,["LookupTags", lookupTags]
    ]:
        for k, v in value.items():
            list.append([key, k, str(v)])
    display(list)

# COMMAND ----------

def ImportCleansedMapping(systemCode):
    path = f"/FileStore/Cleansed/{systemCode.lower()}_cleansed.csv"
    transforms = LoadCsv(path)
    dataTypeFailures, transformFailures, lookupFailures, customTransformFailures = 0, 0, 0, 0
    
    if transforms is None:
        return
    
    # VALIDATE DATATYPES
    df = transforms.selectExpr("TRIM(LOWER(DataType)) DataType").dropDuplicates().na.drop()
    print("""
    Validating Data Types...
    -------------------------
    """)
    for t in df.collect():
        dataType = defaultDataTypes.get(t.DataType.lower())
        dataType = t.DataType if dataType is None else dataType
        
        sql = f"SELECT CAST('' AS {dataType.lower()}) A"
        try:
            spark.sql(sql).count()
        except Exception as error:
            dataTypeFailures+=1
            message = str(error).split(".")[0]
            print(f"[ !! ] - {sql} - {message}")

    # VALIDATE TRANSFORM TAGS
    print("""
    Validating Transform Tags...
    -------------------------
    """)
    df = transforms.selectExpr("TRIM(LOWER(TransformTag)) TransformTag").dropDuplicates().na.drop()
    #display(df)
    for t in df.collect():
        tag = t.TransformTag
        try:
            o = defaultTransformTags[tag]
        except Exception as error:
            transformFailures+=1
            print(f"[ !! ] - {tag} - {error}")

    # VALIDATE CUSTOM TRANSFORM
    print("""
    Validating Custom Transform...
    -------------------------
    """)
    df = transforms.where("CustomTransform IS NOT NULL")
    for t in df.collect():
        try:
            spark.table(t.RawTable).selectExpr(f"{t.CustomTransform}").count()
        except Exception as e:
            customTransformFailures+=1
            print(f"[ !! ] - {t.CustomTransform} - {e}")

    total = dataTypeFailures + transformFailures + lookupFailures + customTransformFailures
    if total == 0:
        print(f"""{path} is all good! Copying...""")
        dbutils.fs.cp(path, f"/mnt/datalake-raw/cleansed_csv/{systemCode}_cleansed.csv", True)

    else:
        print(f"""
===================================================================
{path} has {total} failures!
===================================================================
Data Type Failures = {dataTypeFailures}
Transform Tag Failures = {transformFailures}
Custom Transform Failures = {customTransformFailures}
        """)

# COMMAND ----------

def ImportCleansedMappingByRules(systemCode):
    if systemCode.lower()[-3:] == 'ref': 
        systemCode = systemCode.replace('ref','')
    if systemCode.lower()[-4:] == 'data':
        systemCode = systemCode.replace('data','')    
        
    path = f"/FileStore/Cleansed/{systemCode.lower()}_cleansed_by_rules.csv"
    transforms = LoadCsv(path)
    dataTypeFailures, transformFailures, lookupFailures, customTransformFailures = 0, 0, 0, 0
    
    if transforms is None:
        return
    
    # VALIDATE DATATYPES
    df = transforms.selectExpr("TRIM(LOWER(DataType)) DataType").dropDuplicates().na.drop()
    print("""
    Validating Data Types...
    -------------------------
    """)
    for t in df.collect():
        if t.DataType.lower() == 'nochange':
            continue
        
        dataType = defaultDataTypes.get(t.DataType.lower())
        dataType = t.DataType if dataType is None else dataType
        
        sql = f"SELECT CAST('' AS {dataType.lower()}) A"
        try:
            spark.sql(sql).count()
        except Exception as error:
            dataTypeFailures+=1
            message = str(error).split(".")[0]
            print(f"[ !! ] - {sql} - {message}")

    # VALIDATE TRANSFORM TAGS
    print("""
    Validating Transform Tags...
    -------------------------
    """)
    df = transforms.selectExpr("TRIM(LOWER(TransformTag)) TransformTag").dropDuplicates().na.drop()
    #display(df)
    for t in df.collect():
        tag = t.TransformTag
        try:
            o = defaultTransformTags[tag]
        except Exception as error:
            transformFailures+=1
            print(f"[ !! ] - {tag} - {error}")

    # VALIDATE CUSTOM TRANSFORM
    print("""
    Validating Custom Transform...
    -------------------------
    """)
    df = transforms.where("CustomTransform IS NOT NULL")
    for t in df.collect():
        try:
            spark.table(t.RawTable).selectExpr(f"{t.CustomTransform}").count()
        except Exception as e:
            customTransformFailures+=1
            print(f"[ !! ] - {t.CustomTransform} - {e}")

    total = dataTypeFailures + transformFailures + lookupFailures + customTransformFailures
    total = dataTypeFailures
    if total == 0:
        print(f"""{path} is all good! Copying...""")
        dbutils.fs.cp(path, f"/mnt/datalake-raw/cleansed_csv/{systemCode}_cleansed_by_rules.csv", True)

    else:
        print(f"""
===================================================================
{path} has {total} failures!
===================================================================
Data Type Failures = {dataTypeFailures}
Transform Tag Failures = {transformFailures}
Custom Transform Failures = {customTransformFailures}
        """)        

# COMMAND ----------

def ImportCleansedReference():
    path = f"/FileStore/Cleansed/reference_lookup.csv"
    referenceFile = LoadCsv(path)
    lookupFailures = 0
    
    # VALIDATE LOOKUP TAGS
    print("""
    Validating Lookup Tags...
    -------------------------
    """)
    df = referenceFile.where("Parent like '%lookup%'").dropDuplicates().where("ExtendedProperties IS NOT NULL")

    for t in df.collect():
        whereClause = TryGetJsonProperty(t.ExtendedProperties, "filter")
        try:
            whereClause = "1=1" if whereClause == "" else whereClause
            spark.table(t.Group).where(whereClause).selectExpr(f"{t.Key} Key", f"{t.Value} Value")
        except Exception as e:
            lookupFailures+=1
            print(f"[ !! ] - {t.Parent} - {e}")
    
    if lookupFailures == 0:
        print(f"""{path} is all good! Copying...""")
        dbutils.fs.cp(path, f"{CLEANSED_PATH}/reference_lookup.csv")

    else:
        print(f"""
Lookup Tag Failures = {lookupFailures}
        """)

# COMMAND ----------

def WriteRawToCleansedTable(rawTableName):
    sourceTableName = rawTableName
    zone, tableName = rawTableName.split(".")
    
    # GET CONFIG
    control = spark.table("controldb.dbo_extractloadmanifest").where(f"LOWER(DestinationSchema || '_' || DestinationTableName) = '{tableName}'").collect()
    
    if len(control) == 0:
        raise Exception(f"Table not found {tableName}!")

    control = control[0]
    #print(control)
    sourceDataFrame = spark.table(rawTableName)
    lastLoadTimeStamp = '2022-01-01' #DEFAULT
    cleansedTableName = f"cleansed.{control.DestinationSchema}_{control.DestinationTableName}"
    systemCode = control.SystemCode.replace("data", "").replace("ref", "")
    dataLakePath = control.CleansedPath.replace("/cleansed", "/mnt/datalake-cleansed")
    businessKey = control.BusinessKeyColumn
    
    #GET LAST CLEANSED LOAD TIMESTAMP
    try:
        lastLoadTimeStamp = spark.sql(f"select max(_DLCleansedZoneTimeStamp) as lastLoadTimeStamp from {cleansedTableName}").collect()[0][0]
        #print(lastLoadTimeStamp)
    except Exception as e:
        pass
    
    # APPLY CLEANSED FRAMEWORK
    cleanseDataFrame = CleansedTransform(sourceDataFrame, sourceTableName.lower(), systemCode)
    cleanseDataFrame = cleanseDataFrame.withColumn("_DLCleansedZoneTimeStamp",current_timestamp()) \
                                       .withColumn("_RecordCurrent",lit('1')) \
                                       .withColumn("_RecordDeleted",lit('0')) 

    # FIX BAD COLUMNS
    sourceDataFrame = sourceDataFrame.toDF(*(c.replace(' ', '_') for c in sourceDataFrame.columns))
    
    # WRITE DELTA
    CreateDeltaTable(cleanseDataFrame, cleansedTableName) if businessKey is None else CreateOrMerge(cleanseDataFrame, cleansedTableName, businessKey)

# COMMAND ----------

def UnitTests():
    pass
    #ValidateLookups()
    #DisplayAllTags()
    #PreviewLookup("lookup-crm-communication-channel")
    
    #ImportCleansedReference()
    #ImportCleansedMapping("bom")
    #ImportCleansedMapping("sap")
    #ImportCleansedMapping("crm")
    #ImportCleansedMapping("maximo")
    #ImportCleansedMapping("isu")
    
    #PreviewTransform("bom", "raw.bom_dailyweatherobservation_parramattanorth") # GOOD
    #PreviewTransform("maximo", "raw.maximo_ACCOUNTDEFAULTS") # GOOD
    #PreviewTransform("sap", "raw.crm_zcst_source") # GOOD
    #PreviewTransform("sap", "raw.crm_0bpartner_attr") # TABLE NOT IN SHEET
    #PreviewTransform("crm", "raw.crm_0crm_srv_req_inci_h")
    #PreviewTransform("sap", "raw.isu_zdmt_rate_type") # GOOD
    
    #WriteRawToCleansedTable("raw.isu_zdmt_rate_type")
#UnitTests()
