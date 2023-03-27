# Databricks notebook source
dbutils.widgets.text(name="task", defaultValue="", label="task")

# COMMAND ----------

# MAGIC %run ../Common/common-include-all

# COMMAND ----------

spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
task = dbutils.widgets.get("task")
j = json.loads(task)
systemCode = j.get("SystemCode")
sourceTable = j.get("SourceTableName")
destinationSchema = j.get("DestinationSchema")
destinationTableName = j.get("DestinationTableName")
cleansedPath = j.get("CleansedPath")
businessKey = j.get("BusinessKeyColumn")
destinationKeyVaultSecret = j.get("DestinationKeyVaultSecret")
extendedProperties = j.get("ExtendedProperties")
dataLakePath = cleansedPath.replace("/cleansed", "/mnt/datalake-cleansed")
rawTableNameMatchSource = None

if extendedProperties:
    extendedProperties = json.loads(extendedProperties)
    rawTableNameMatchSource = extendedProperties.get("RawTableNameMatchSource")
if rawTableNameMatchSource:
    sourceTableName = get_table_name('raw', j['DestinationSchema'], j['SourceTableName']).lower()
else:
    sourceTableName = get_table_name('raw', j['DestinationSchema'], j['DestinationTableName']).lower()
cleansedTableName = get_table_name('cleansed', j['DestinationSchema'], j['DestinationTableName']).lower()
source_table_name_nonuc = f'raw.{destinationSchema}_{sourceTableName}'

# COMMAND ----------

#GET LAST CLEANSED LOAD TIMESTAMP
try:
    lastLoadTimeStamp = spark.sql(f"select max(_DLCleansedZoneTimeStamp) as lastLoadTimeStamp from {cleansedTableName}").collect()[0][0]
    print(lastLoadTimeStamp)
except Exception as e:
    if "Table or view not found" in str(e):
        lastLoadTimeStamp = '2022-01-01'            

# COMMAND ----------

sourceDataFrame = spark.sql(f"select * from {sourceTableName} where _DLRawZoneTimeStamp > '{lastLoadTimeStamp}'")
sourceDataFrame = sourceDataFrame.groupby(sourceDataFrame.columns[0:-1]).count().drop("count")
CleansedSourceCount = sourceDataFrame.count()

# FIX BAD COLUMNS
sourceDataFrame = sourceDataFrame.toDF(*(c.replace(' ', '_') for c in sourceDataFrame.columns))

# CLEANSED QUERY FROM RAW TO FLATTEN OBJECT
if(extendedProperties):
    cleansedPath = extendedProperties.get("CleansedQuery")
    if(cleansedPath):
        sourceDataFrame = spark.sql(cleansedPath.replace("{tableFqn}", sourceTableName))
    
# APPLY CLEANSED FRAMEWORK
cleanseDataFrame = CleansedTransform(sourceDataFrame, sourceTableName.lower(), systemCode)
cleanseDataFrame = cleanseDataFrame.withColumn("_DLCleansedZoneTimeStamp",current_timestamp()) \
                                   .withColumn("_RecordCurrent",lit('1')) \
                                   .withColumn("_RecordDeleted",lit('0')) \
                                   .withColumn("_RecordStart",current_timestamp()) \
                                   .withColumn("_RecordEnd",to_timestamp(lit("9999-12-31"), "yyyy-MM-dd"))

# GET LATEST RECORD OF THE BUSINESS KEY
if(extendedProperties):
    groupOrderBy = extendedProperties.get("GroupOrderBy")
    if(groupOrderBy):
        cleanseDataFrame.createOrReplaceTempView("vwCleanseDataFrame")
        cleanseDataFrame = spark.sql(f"select * from (select vwCleanseDataFrame.*, row_number() OVER (Partition By {businessKey} order by {groupOrderBy}) row_num from vwCleanseDataFrame) where row_num = 1 ").drop("row_num")   

CreateDeltaTable(cleanseDataFrame, cleansedTableName, dataLakePath) if j.get("BusinessKeyColumn") is None else CreateOrMerge(cleanseDataFrame, cleansedTableName, dataLakePath, j.get("BusinessKeyColumn"))

# COMMAND ----------

CleansedSinkCount = spark.table(cleansedTableName).count()
#print(f"Cleansed Source Count: {CleansedSourceCount} Cleansed Sink Count: {CleansedSinkCount}")
dbutils.notebook.exit({"CleansedSourceCount": CleansedSourceCount, "CleansedSinkCount": CleansedSinkCount})
