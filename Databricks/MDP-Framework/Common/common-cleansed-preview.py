# Databricks notebook source
# MAGIC %run ./common-cleansed

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
#PreviewTransform("bom", "raw.bom_dailyweatherobservation_parramattanorth") # GOOD
#PreviewTransform("maximo", "raw.maximo_ACCOUNTDEFAULTS") # GOOD
#PreviewTransform("sap", "raw.crm_zcst_source") # GOOD
#PreviewTransform("sap", "raw.crm_0bpartner_attr") # TABLE NOT IN SHEET
#PreviewTransform("crm", "raw.crm_0crm_srv_req_inci_h")

# COMMAND ----------

def PreviewLookup(lookupTag):
    table, key, value, refaultReturn, whereclause = lookupTags.get(lookupTag) if "|" not in lookupTag else lookupTag.split("|")
    display(spark.table(table).selectExpr(f"{key} Key", f"{value} Value").where("Value IS NOT NULL"))
#PreviewLookup("lookup-crm-communication-channel")

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
#ValidateLookups()

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
#DisplayAllTags()

# COMMAND ----------

def ImportCleansedMapping(systemCode):
    path = f"/FileStore/Cleansed/{systemCode.lower()}_cleansed.csv"
    transforms = LoadCsv(path)
    dataTypeFailures, transformFailures, lookupFailures, customTransformFailures = 0, 0, 0, 0
    
    # VALIDATE DATATYPES
    df = transforms.selectExpr("TRIM(LOWER(DataType)) DataType").dropDuplicates().na.drop()
    print("""
    Validating Data Types...
    -------------------------
    """)
    for t in df.rdd.collect():
        dataType = defaultDataTypes.get(t.DataType.lower())
        dataType = t.DataType if dataType is None else dataType
        
        sql = f"SELECT CAST('' AS {dataType.lower()}) A"
        try:
            spark.sql(sql).count()
            #print(sql)
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
    for t in df.rdd.collect():
        tag = t.TransformTag
        try:
            o = defaultTransformTags[tag]
        except Exception as error:
            transformFailures+=1
            print(f"[ !! ] - {tag} - {error}")

    # VALIDATE LOOKUP TAGS
    #print("""
    #Validating Lookup Tags...
    #-------------------------
    #""")
    #df = transforms.selectExpr("TRIM(LOWER(Lookup)) Lookup").dropDuplicates().na.drop()
    #for t in df.rdd.collect():
        #lookup = t.Lookup
        #try:
            #tag = lookupTags.get(lookup)
            
            #if tag is None:
                #raise Exception(f"Tag {lookup} not found!")
            
            #table, key, value, returnNull, whereClause = lookupTags[lookup]
            #spark.table(table).where(whereClause).selectExpr(f"{key} Key", f"{value} Value")
        #except Exception as e:
            #lookupFailures+=1
            #print(f"[ !! ] - {lookup} - {e}")

    # VALIDATE CUSTOM TRANSFORM
    print("""
    Validating Custom Transform...
    -------------------------
    """)
    df = transforms.where("CustomTransform IS NOT NULL")
    for t in df.rdd.collect():
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

#ImportCleansedMapping("bom")
#ImportCleansedMapping("crm")
#ImportCleansedMapping("maximo")

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
    #display(df)
    #return 
    for t in df.rdd.collect():
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
#ImportCleansedReference()

# COMMAND ----------


