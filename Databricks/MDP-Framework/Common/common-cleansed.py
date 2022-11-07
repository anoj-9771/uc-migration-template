# Databricks notebook source
CLEANSED_PATH = "/mnt/datalake-raw/cleansed_csv"

# COMMAND ----------

defaultTransformTags = {
    "upper" : "upper($c$)"
    ,"lower" : "lower($c$)"
    ,"ltrim" : "ltrim($c$)"
    ,"rtrim" : "rtrim($c$)"
    ,"trim" : "trim($c$)"
    ,"str-yyyy-mm-dd-to-date" : "to_date(trim($c$),'yyyy-MM-dd')"
    ,"flag-x-yes-no" : "case WHEN $c$='X' then 'Yes' Else 'No' end "
    ,"flag-x-true-false" : "case WHEN $c$='X' then 'True' Else 'False' end "
    ,"flag-1or0-yes-no" : "case WHEN $c$='1' then 'Yes' when $c$='0' then 'No' end "
    ,"flag-int-inbound-outbound" : "'A'"
    ,"int-utc-to-sydney-datetime" : "'A'"
    ,"int-to-datetime" : "'A'"
}

# COMMAND ----------

defaultDataTypes = {
    "decimal" : "DECIMAL(18, 2)"
    ,"time" : "string"
    ,"datetime" : "timestamp"
}

# COMMAND ----------

# https://elogin.ads.swc/confluence/display/DAF/Cleansed+Framework
# "<Name-Of-Tag>" : [ "<TableFqn>", "<Key/LookupColumn>", "<Value/ReturnColumn>", "<DefaultReturn>", "<WhereClause>"]
# NOW USING reference-lookup.csv instead
lookupTags = {
    #"Lookup-Access-LGA" : [ "CLEANSED.access_z309_tlocalgovt", "LGACode", "LGA", "", "" ]
    #,"lookup-crm-service-type" : [ "Cleansed.crm_0crm_proc_type_text", "serviceRequestTypeCode", "serviceRequestDescription", "", "" ]
    #,"lookup-crm-service-type" : [ "Cleansed.crm_0crm_proc_type_text", "serviceRequestTypeCode", "serviceRequestDescription", "", "" ]
    #,"lookup-crm-status-text" : [ "Cleansed.crm_zbcs_ds_crmstatus_txt", "statusProfile|statusCode", "status", "", "" ]
    #,"lookup-crm-source" : [ "Cleansed.ZCST_SOURCE", "sourceCode", "source", "", "" ]
    #,"lookup-crm-service-team" : [ "Cleansed.crm_0bpartner_attr", "partner", "partner", "", "Filter: Type = 2" ]
    #,"lookup-partner-function-text" : [ "Cleansed.CRMC_PARTNER_FT", "partnerFunction", "description", "", "" ]
    #,"lookup-crm-domain-text" : [ "Clenased.crm_dd07t", "domainName||domainValueKey", "domainValueText", "", "" ]
    #,"lookup-crm-category" : [ "Cleansed.crm_0crm_category_text", "categoryCode", "categoryDescription", "", "" ]
    #,"Lookup-Access-Debit-Type" : [ "CLEANSED.access_z309_tdebittype", "LEFT(debitTypeCode, 2)", "debitType", "", "" ]
    #,"Lookup-Access-Debit-Reason" : [ "CLEANSED.access_z309_tdebitreason", "debitTypeCode||debitReasonCode", "debitReason", "", "" ]
}

# COMMAND ----------

def LoadCsv(path):
    # VALIDATE FILE PATH
    try: 
        dbutils.fs.ls(path)
        print(f"Found mapping sheet: {path}")
    except Exception:
        print(f"Mapping sheet not found! {path}")
        return None
        
    return (spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("multiline", "true")
            .option("quote", "\"") 
            .load(path))

# COMMAND ----------

def TryGetJsonProperty(jsonText, name):
    if jsonText is None:
        return ""
 
    jsonText = jsonText.replace("\"\"", "\"")
    import json
    if jsonText is None:
        return None
    
    jsonText = jsonText.replace("\"{", "{").replace("}\"", "}")
    json = json.loads(jsonText)
    return json.get(name)

# COMMAND ----------

def PopulateLookupTags():
    # LOAD TAGS
    df = LoadCsv(f"{CLEANSED_PATH}/reference_lookup.csv").where("Parent like '%lookup%'")
    global lookupTags
    lookupTags = {}
    
    for i in df.rdd.collect():
        whereClause = TryGetJsonProperty(i.ExtendedProperties, "filter")
        lookupTags[i.Parent] = [i.Group, i.Key, i.Value, "", whereClause]
PopulateLookupTags()

# COMMAND ----------

def StaticReplacement(sourceDataFrame, lookupTag, columnName):
    print("Static Replacement")
    # LOAD AND FILTER CSV
    df = LoadCsv(f"{CLEANSED_PATH}/reference_lookup.csv")
    whereClause = f"Parent || '-' || Group  = '{lookupTag}'"
    df = df.where(whereClause)
    
    # CHECK
    if df.count() == 0:
        print(whereClause)
        return sourceDataFrame
    
    # SET DEFAULT VALUE
    defaultDf = df.select("Key", "Value").where("Key = 'DEFAULT_NULL'").rdd.collect()
    defaultValue = "NULL" if len(defaultDf) == 0 else defaultDf[0].Value
    
    # CONSTRUCT CASE STATEMENT
    caseStatement = "".join([f"WHEN $c$ = '{i.Key}' THEN '{i.Value}' " for i in df.select("Key", "Value").where("Key != 'DEFAULT_NULL'").rdd.collect()])
    caseStatement = "CASE " + caseStatement + f" ELSE '{defaultValue}' END "
    caseStatement = caseStatement.replace("$c$", columnName)
    
    return sourceDataFrame.withColumn(columnName, expr(caseStatement))

# COMMAND ----------

def LookupValue(sourceDataFrame, lookupTag, columnName):
    tagParameters = lookupTags.get(lookupTag.lower())

    # TRY LOOKUP THEN STATIC REPLACEMENT
    if tagParameters is None:
        return StaticReplacement(sourceDataFrame, lookupTag, columnName)
    
    table, key, value, returnNull, whereClause = tagParameters if "|" not in lookupTag else lookupTag.split("|")
    whereClause = whereClause or "1=1"

    # USE SYSTEM WIDE UNKNOWN RETURN
    emptyDefault = "'(Unknown)'" if returnNull == "U" else "NULL"

    df = (sourceDataFrame
                .join(
                        spark.table(table)
                        .where(whereClause)
                        .selectExpr(f"{key} Key", f"{value} Value")
                    ,expr(f"CAST({columnName} AS STRING) == CAST(Key AS STRING)"), "left")
                .withColumn(columnName, expr(f"COALESCE(Value, {emptyDefault})"))
                .drop(*["Key", "Value"])
           )
    
    return df

# COMMAND ----------

def TransformRow(rawColumnName, transformTag, customTransform, cleansedColumnName):
    if customTransform is not None:
        return f"{customTransform} AS {cleansedColumnName}"
    
    if transformTag is not None:
        tag = defaultTransformTags.get(transformTag, "$c$").replace("$c$", rawColumnName)
        return f"{tag} AS {cleansedColumnName}"

    return f"{rawColumnName} AS {cleansedColumnName}"

# COMMAND ----------

def GetLast(path, delim="/"):
    list = path.split(delim)
    count=len(list)
    return list[count-1]

# COMMAND ----------

def DataTypeConvertRow(columnName, dataType):
    changedDataType = defaultDataTypes.get(dataType)
    changedDataType = dataType if changedDataType is None else changedDataType
    
    return f"CAST({columnName} AS {changedDataType})"

# COMMAND ----------

from pyspark.sql.functions import expr, col
def CleansedTransform(dataFrame, tableFqn, systemCode, showTransform=False):
    tableFqn = tableFqn.lower()
    dataFrame = spark.table(tableFqn) if dataFrame is None else dataFrame
    path = f"{CLEANSED_PATH}/{systemCode.lower()}_cleansed.csv"

    # 1. LOAD CLEANSED CSV
    allTransforms = LoadCsv(path)
    
    # CSV NOT FOUND RETURN
    if allTransforms is None:
        return dataFrame
    
    # POPULATE LOOKUP TAGS
    PopulateLookupTags()
    
    # 2. LOOKUP TRANSFORM
    transforms = allTransforms.where(f"RawTable = '{tableFqn}'")
    display(transforms) if showTransform else None

    # !NO TRANSFORMS!
    if transforms.count() == 0:
        print(f"Not Transforming, table {tableFqn} not found in sheet! ")
        return dataFrame

    try:
        # 3. APPLY CUSTOM IN-LINE TRANSFORMS, THEN TAGS
        transformedDataFrame = dataFrame.selectExpr(
            [TransformRow(i.RawColumnName, i.TransformTag, i.CustomTransform, i.CleansedColumnName) for i in transforms.rdd.collect()]
        )
    except Exception as e:
        print(f"Custom Transform failed, exception: {e}")
        return dataFrame

    try:
        # 4. LOOKUPS
        for l in transforms.where("Lookup IS NOT NULL").dropDuplicates().rdd.collect():
            transformedDataFrame = LookupValue(transformedDataFrame, l.Lookup.lower(), l.CleansedColumnName)
    except Exception as e:
        print(f"Lookup failed, exception: {e}")
        return dataFrame

    try:
        # 5. APPLY DATA TYPE CONVERSION
        transformedDataFrame = transformedDataFrame.selectExpr(
            [DataTypeConvertRow(i.CleansedColumnName, i.DataType.lower()) for i in transforms.rdd.collect()]
        )
    except Exception as e:
        print(f"DataType failed, exception: {e}")
        return dataFrame
        
    print(f"Successfully transformed {tableFqn}!")
    # RETURN TRANSFORMED DATAFRAME
    return transformedDataFrame

# COMMAND ----------


