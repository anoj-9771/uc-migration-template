# Databricks notebook source
# MAGIC %run ../Common/common-include-all

# COMMAND ----------

spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.caseSensitive", "true")
task = dbutils.widgets.get("task")
j = json.loads(task)
systemCode = j.get("SystemCode")
destinationSchema = j.get("DestinationSchema")
destinationTableName = j.get("DestinationTableName")
cleansedPath = j.get("CleansedPath")
businessKey = j.get("BusinessKeyColumn")
destinationKeyVaultSecret = j.get("DestinationKeyVaultSecret")
extendedProperties = j.get("ExtendedProperties")
dataLakePath = cleansedPath.replace("/cleansed", "/mnt/datalake-cleansed")
sourceTableName = f"raw.{destinationSchema}_{destinationTableName}"

# COMMAND ----------

def RemoveDuplicateColumns(dataFrame):
    seen = set()
    dupes = [x for x in dataFrame.columns if x.lower() in seen or seen.add(x.lower())]
    for d in dupes:
        dataFrame = dataFrame.drop(d)
    return dataFrame

# COMMAND ----------

template = f"RIGHT(SHA2(CAST($c$ AS STRING), 256), 16)"
masks = {
    "redacted" : "'[*** REDACTED ***]'"
    ,"firstname" : template
    ,"lastname" : template
    ,"email" : template
    ,"address" : template
    ,"phone" : template
    ,"mobile" : template
    ,"mob" : template
    ,"datareference" : template
    ,"longitude" : template
    ,"applicant" : template
}

def MaskPIIColumn(column):
    for k, v in masks.items():
        if k in column.lower():
            return f"$l$ `{column}`".replace("$l$", v.replace("$c$", f"`{column}`"))
    return f"`{column}`"     

def MaskTable(dataFrame):
    return dataFrame.selectExpr(
        [MaskPIIColumn(c) for c in dataFrame.columns]
    )

# COMMAND ----------

sourceDataFrame = spark.table(sourceTableName)

# CLEANSED QUERY FROM RAW TO FLATTEN OBJECT
if(extendedProperties):
  extendedProperties = json.loads(extendedProperties)
  cleansedQuery = extendedProperties.get("CleansedQuery")
  if(cleansedQuery):
    sourceDataFrame = spark.sql(cleansedQuery.replace("{tableFqn}", sourceTableName))
    
# FIX BAD COLUMNS
sourceDataFrame = sourceDataFrame.toDF(*(RemoveBadCharacters(c) for c in sourceDataFrame.columns))

# REMOVE DUPE COLUMNS
sourceDataFrame = RemoveDuplicateColumns(sourceDataFrame)

# APPLY CLEANSED FRAMEWORK
sourceDataFrame = CleansedTransform(sourceDataFrame, sourceTableName, systemCode)

# MASK TABLE
sourceDataFrame = MaskTable(sourceDataFrame)

tableName = f"{destinationSchema}_{destinationTableName}"
CreateDeltaTable(sourceDataFrame, f"cleansed.{tableName}", dataLakePath) if j.get("BusinessKeyColumn") is None else CreateOrMerge(sourceDataFrame, f"cleansed.{tableName}", dataLakePath, j.get("BusinessKeyColumn"))
