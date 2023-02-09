# Databricks notebook source
CLEANSED_PATH = "/mnt/datalake-raw/cleansed_csv"

# COMMAND ----------

defaultTransformTags = {
    "upper" : "upper($c$)"
    ,"lower" : "lower($c$)"
    ,"ltrim" : "ltrim($c$)"
    ,"rtrim" : "rtrim($c$)"
    ,"trim" : "trim($c$)"
    ,"trimcoalesce": "coalesce(trim($c$),'')"
    ,"trimcoalesceint": "coalesce(trim(cast($c$ as bigint)),'')"
    ,"str-dd-MMM-yy-to-date" : "to_date(right(concat('0',$c$),9),'dd-MMM-yy')"
    ,"str-yyyymmdd-to-date" : "to_date(trim($c$),'yyyyMMdd')"
    ,"str-yyyy-mm-dd-to-date" : "to_date(trim($c$),'yyyy-MM-dd')"
    ,"str-dd-MMM-yyyy-HH-mm-to-timestamp" : "to_timestamp(right(concat('0',$c$),17),'dd-MMM-yyyy HH:mm')"
     ,"str-dd/MM/yyyy-HH-mm-to-timestamp" : "to_timestamp(right(concat('0',split($c$,' ' )[0]),10)||' '||right(concat('0',split($c$,' ' )[1]),5),'dd/MM/yyyy HH:mm')"
    ,"flag-x-yes-no" : "case WHEN $c$='X' then 'Y' Else 'N' end "
    ,"flag-x-true-false" : "case WHEN $c$='X' then 'T' Else 'F' end "
    ,"flag-TorF-yes-no" : "case when $c$='T' then 'Y'   when $c$='F' then 'N'  end "
    ,"flag-1or0-yes-no" : "case WHEN $c$='1' then 'Y' when $c$='0' then 'N' end "
    ,"flag-int-inbound-outbound" : " case WHEN $c$='0' then 'I' Else 'O' end "
    ,"int-utc-to-sydney-datetime" : " case WHEN $c$='99991231235959' then to_timestamp(substring($c$,1,4)||'-'||substring($c$,5,2)||'-'||substring($c$,7,2) \
||' '||substring($c$,9,2)||':'||substring($c$,11,2)||':'||substring($c$,13,2)) else from_utc_timestamp(substring($c$,1,4)||'-'||substring($c$,5,2)||'-'||substring($c$,7,2) \
||' '||substring($c$,9,2)||':'||substring($c$,11,2)||':'||substring($c$,13,2)||'.0','Australia/Sydney') end "
    ,"int-to-datetime" : " to_timestamp(substring($c$,1,4)||'-'||substring($c$,5,2)||'-'||substring($c$,7,2) \
||' '||substring($c$,9,2)||':'||substring($c$,11,2)||':'||substring($c$,13,2)) "
    ,"double-to-datetime" : " to_timestamp(substring(cast($c$ as BIGINT),1,4)||'-'||substring(cast($c$ as BIGINT),5,2) \
    ||'-'||substring(cast($c$ as BIGINT),7,2)||' '||substring(cast($c$ as BIGINT),9,2)||':'||substring(cast($c$ as BIGINT),11,2) \
    ||':'||substring(cast($c$ as BIGINT),13,2)) "
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
    systemCode = systemCode.lower()
    
    if systemCode[-3:] == 'ref': 
        systemCode = systemCode.replace('ref','')
    if systemCode[-4:] == 'data':
        systemCode = systemCode.replace('data','')
    
    path = f"{CLEANSED_PATH}/{systemCode}_cleansed.csv"

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

def ApplyTransformRules(dataFrame, transform_rules):
    df_columns = spark.createDataFrame(dataFrame.schema.fieldNames(),'string').withColumnRenamed('value','RawColumnName')
    df_columns.createOrReplaceTempView("df_columns")
    transform_rules.createOrReplaceTempView("transform_rules")    
    
    transforms=spark.sql("""
        WITH _transform_rules AS
        (
            --only one set of RawTablePattern will be applied
            select *
            from transform_rules
            where RawTablePattern = (
                select RawTablePattern
                from transform_rules
                order by RulesOrder
                limit 1
            )
        ),
        _transforms AS (
            select RawTablePattern, RawColumnName, RawColumnPattern,
                   case 
                    when CleansedColumnName = RawColumnPattern then RawColumnName
                    when CleansedColumnName is null then lower(left(RawColumnName,1))||substr(RawColumnName,2)
                    else CleansedColumnName 
                   end as CleansedColumnName,
                   DataType, TransformTag, CustomTransform, Lookup, 
                   --Once Continue is set to 'N', ignore the rest of the rules
                   min(Continue) over (partition by RawColumnName order by RulesOrder 
                                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) Continue,
                   RulesOrder
            from df_columns, _transform_rules
            where df_columns.RawColumnName rlike '^'||_transform_rules.RawColumnPattern||'$'
        ),
        transforms AS (
            --Check which rules should be included
            select _transforms.*,
                   lag(Continue, 1, 'Y') over (partition by RawColumnName order by RulesOrder) Include
            from _transforms
        )    
        select *
        from transforms
        where Include = 'Y'
        order by RulesOrder
    """    
    )    
    
    return transforms

from pyspark.sql.functions import expr, col
def CleansedTransformByRules(dataFrame, tableFqn, systemCode, showTransform=False):
    global transforms
    tableFqn = tableFqn.lower()
    dataFrame = spark.table(tableFqn) if dataFrame is None else dataFrame
    systemCode = systemCode.lower()
    
    if systemCode[-3:] == 'ref': 
        systemCode = systemCode.replace('ref','')
    if systemCode[-4:] == 'data':
        systemCode = systemCode.replace('data','')
    
    path = f"{CLEANSED_PATH}/{systemCode}_cleansed_by_rules.csv"

    # 1. LOAD CLEANSED CSV
    allTransforms = LoadCsv(path)
    
    # CSV NOT FOUND RETURN
    if allTransforms is None:
        return dataFrame
    
    # POPULATE LOOKUP TAGS
    PopulateLookupTags()
    
    # 2. LOOKUP TRANSFORM
    transform_rules = allTransforms.where(f"'{tableFqn.lower()}' rlike '^'||RawTablePattern||'$'")
    display(transform_rules) if showTransform else None

    # !NO TRANSFORMS!
    if transform_rules.count() == 0:
        print(f"Not Transforming, table {tableFqn} not found in sheet! ")
        return dataFrame

    transforms=ApplyTransformRules(dataFrame, transform_rules)
    
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
            [f"{i.CleansedColumnName}" if i.DataType.lower() == 'nochange' else DataTypeConvertRow(i.CleansedColumnName, i.DataType.lower()) 
             for i in transforms.rdd.collect()]
        )
    except Exception as e:
        print(f"DataType failed, exception: {e}")
        return dataFrame
        
    print(f"Successfully transformed {tableFqn}!")
    # RETURN TRANSFORMED DATAFRAME
    return transformedDataFrame
