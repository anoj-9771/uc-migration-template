# Databricks notebook source
##################################################################
#Include Notebook
#1.Include all util user function for the notebook
#2.Define and get Widgets/Parameters
#3.Spark Config
#4.Function: Create stage and curated database if not exist
#5.Function: Load data into Curated delta table
##################################################################

# COMMAND ----------

from pyspark.sql.window import Window
from datetime import datetime

# COMMAND ----------

# DBTITLE 1,1. Include all util user functions for this notebook
# MAGIC %run ./includes/util-common

# COMMAND ----------

# MAGIC %run ./common-transform-scd

# COMMAND ----------

# DBTITLE 1,2. Define and get Widgets/Parameters
#Set Parameters
dbutils.widgets.removeAll()

dbutils.widgets.text("Start_Date","")
dbutils.widgets.text("End_Date","")

#Get Parameters
start_date = dbutils.widgets.get("Start_Date")
end_date = dbutils.widgets.get("End_Date")

params = {"start_date": start_date, "end_date": end_date}

#DEFAULT IF ITS BLANK
start_date = "2000-01-01" if not start_date else start_date
end_date = "9999-12-31" if not end_date else end_date

#Print Date Range
print(f"Start_Date = {start_date}| End_Date = {end_date}")

# COMMAND ----------

# DBTITLE 1,3. Spark Config
# When set to true Spark SQL will automatically select a compression codec for each column based on statistics of the data.
spark.conf.set("spark.sql.inMemoryColumnarStorage.compressed",True)

# Limit of total size of serialized results of all partitions for each Spark action (e.g. collect) in bytes. Should be at least 1M, or 0 for unlimited. Jobs will be aborted if the total size is above this limit. Having a high limit may cause out-of-memory errors in driver (depends on spark.driver.memory and memory overhead of objects in JVM). Setting a proper limit can protect the driver from out-of-memory errors.
#spark.conf.set("spark.driver.maxResultSize",0)

#Configures the maximum size in bytes for a table that will be broadcast to all worker nodes when performing a join. By setting this value to -1 broadcasting can be disabled.
# spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 0)

# COMMAND ----------

# DBTITLE 1,4. Function: Load data into Curated delta table
def TemplateEtl(df : object, entity, businessKey, schema, writeMode, AddSK = True ):
    rawEntity = entity
    entity = GeneralToPascalCase(rawEntity)
    LogEtl(f"Starting {entity}.")

    v_COMMON_SQL_SCHEMA = "dbo"
    v_COMMON_CURATED_DATABASE = "curated"
    v_COMMON_DATALAKE_FOLDER = "curated"

    DeltaSaveDataFrameToDeltaTable(df, 
                                   rawEntity, 
                                   ADS_DATALAKE_ZONE_CURATED, 
                                   v_COMMON_CURATED_DATABASE, 
                                   v_COMMON_DATALAKE_FOLDER, 
                                   writeMode,
                                   schema,
                                   track_changes = False, 
                                   is_delta_extract = False, 
                                   business_key = businessKey, 
                                   AddSKColumn = AddSK, 
                                   delta_column = "", 
                                   start_counter = "0", 
                                   end_counter = "0")

    #Commenting the below code, pending decision on Synapse
#     delta_table = f"{v_COMMON_CURATED_DATABASE}.{rawEntity}"
#     print(delta_table)
#     dw_table = f"{v_COMMON_SQL_SCHEMA}.{rawEntity}"
#     print(dw_table)

#     maxDate = SynapseExecuteSQLRead("SELECT isnull(cast(max([_RecordStart]) as varchar(50)),'2000-01-01') as maxval FROM " + dw_table + " ").first()["maxval"]
#     print(maxDate)

#     DeltaSyncToSQLDW(delta_table, v_COMMON_SQL_SCHEMA, entity, businessKey, start_counter = maxDate, data_load_mode = ADS_WRITE_MODE_MERGE, additional_property = "")

    LogEtl(f"Finished {entity}.")

# COMMAND ----------

def TemplateEtlSCD(df : object, entity, businessKey, schema, target_layer='curated', scd_valid_start_date='now', scd_valid_end_date='9999-12-31T23:59:59', snap_shot=1, date_granularity='Second'):

    Entity_Type = 'dimension'
    TARGET_LAYER = ADS_DATABASE_CURATED
    if is_uc():
        Entity_Name = entity[4:]
    else:
        Entity_Name = entity[3:]
    BUSINESS_KEY_COLS = businessKey
    SCD_START_DATE = scd_valid_start_date
    SCD_END_DATE = scd_valid_end_date
    SNAP_SHOT=snap_shot
    DATE_GRANULARITY = date_granularity

    _ = SCDTransform(Entity_Type, Entity_Name, BUSINESS_KEY_COLS, TARGET_LAYER, SNAP_SHOT, DATE_GRANULARITY)

    print(f"SCD Common Notebook Parameters:\n"
          f"TARGET_LAYER={TARGET_LAYER}\n"
          f"Entity_Type={_.EntityType}\n"
          f"Entity_Name={_.EntityName}\n"
          f"TargetTable={_.TargetTable}\n"
          f"SurrogateKey={_.SurrogateKey}\n"
          f"DataLakePath = {_.DataLakePath}\n"
          f"BUSINESS_KEY_COLS={_.BusinessKeyCols}\n"
          f"SCD_START_DATE={SCD_START_DATE}\n"
          f"SCD_END_DATE={SCD_END_DATE}\n"
          f"SNAP_SHOT={SNAP_SHOT}\n"
          f"DATE_GRANULARITY={DATE_GRANULARITY}\n")

    SCDMerge(df, scd_start_date = SCD_START_DATE, scd_end_date = SCD_END_DATE, _=_)
    
    verifyTableSchema(f"{TARGET_LAYER}.{entity}", schema)
    
    LogEtl(f"Finished {entity}.")

    

# COMMAND ----------

def TemplateTimeSliceEtlSCD_2(df : object, entity, businessKey, schema, target_layer='curated'):
    
    LogEtl(f"Starting {entity}.")

    v_COMMON_SQL_SCHEMA = "dbo"
    v_COMMON_CURATED_DATABASE = "curated"
    v_COMMON_DATALAKE_FOLDER = "curated"
    
    TARGET_LAYER = ADS_DATABASE_CURATED
    
    TargetTable = f"{TARGET_LAYER}.{entity}"
    mount_point = DataLakeGetMountPoint(ADS_CONTAINER_CURATED)
    TargetTableDataLakePath = f"dbfs:{mount_point}/{entity.lower()}/delta"
    EntityName = entity[3:]
    SurrogateKey = f"{EntityName}SK"
    SurrogateKey = SurrogateKey[0].lower() + SurrogateKey[1:]
    
    print(f"Time Slice Etl Parameters:\n"
          f"EntityName={EntityName}\n"
          f"SurrogateKey={SurrogateKey}\n"
          f"TargetTable={TargetTable}\n"
          f"TargetTableDataLakePath={TargetTableDataLakePath}\n")
    
    # based on TimeSlice validFromDate, validToDate to populate '_RecordStart', '_RecordEnd', '_RecordCurrent'
    df = df.withColumn("_BusinessKey", concat_ws('|', *(businessKey.split(","))))
    df = df.withColumn("_RecordStart", expr("CAST(ifnull(validFromDate,'1900-01-01') as timestamp)"))
    df = df.withColumn("_RecordEnd", expr("CAST((CAST(ifnull(validToDate,'9999-12-31') as date) +1) - INTERVAL 1 SECOND as timestamp)"))
    
    if "_RecordDeleted" not in df.columns:
        df = df.withColumn("_RecordDeleted", expr("CAST(0 AS INT)"))
    else:
        df = df.withColumn("_RecordDeleted", when(col('_RecordDeleted').isNull(),expr(f"CAST(0 AS INT)")).otherwise(col('_RecordDeleted'))) 
    
    df = df.withColumn("_DLCuratedZoneTimeStamp", expr("now()"))
    df = df.withColumn(SurrogateKey, md5(expr(f"concat(_BusinessKey,'|',_RecordStart)")))
    
    # Latest _RecordStart date record marked as RecordCurrent = 1
    #window_Spec  = Window.partitionBy("_BusinessKey").orderBy(col("_RecordStart").desc())
    #df = df.withColumn("_RecordStart_Order",row_number().over(window_Spec))
    #df = df.withColumn("_RecordCurrent", when(col("_RecordStart_Order") == 1, 1).otherwise(0))
    #df = df.drop("_RecordStart_Order")
    
    # current datetime is between _RecordStart and _RecordEnd, then marked as RecordCurrent = 1
    df = df.withColumn("_RecordCurrent", when(current_timestamp().between(col("_RecordStart"),col("_RecordEnd")), 1).otherwise(0))
    
    df = df.select([field.name for field in schema] + ['_BusinessKey','_DLCuratedZoneTimeStamp','_RecordStart','_RecordEnd','_RecordDeleted','_RecordCurrent'])
    
    print("Overwrite Target Table")
    
    df.write.mode("overwrite").saveAsTable(TargetTable, path=TargetTableDataLakePath)
    
    verifyTableSchema(f"{TARGET_LAYER}.{entity}", schema)

    LogEtl(f"Finished {entity}.")

# COMMAND ----------

def TemplateTimeSliceEtlSCD(df : object, entity, businessKey, schema, fromDateCol='validFromDate', toDateCol='validToDate', target_layer='curated'):
    
    LogEtl(f"Starting {entity}.")

    v_COMMON_SQL_SCHEMA = "dbo"
    v_COMMON_CURATED_DATABASE = "curated"
    v_COMMON_DATALAKE_FOLDER = "curated"
    
    TARGET_LAYER = ADS_DATABASE_CURATED
    
    TargetTable = f"{TARGET_LAYER}.{entity}"
    mount_point = DataLakeGetMountPoint(ADS_CONTAINER_CURATED)
    TargetTableDataLakePath = f"dbfs:{mount_point}/{entity.lower()}/delta"
    if is_uc():
        EntityName = entity[4:]
    else:
        EntityName = entity[3:]
    SurrogateKey = f"{EntityName}SK"
    SurrogateKey = SurrogateKey[0].lower() + SurrogateKey[1:]
    
    print(f"Time Slice Etl Parameters:\n"
          f"EntityName={EntityName}\n"
          f"SurrogateKey={SurrogateKey}\n"
          f"TargetTable={TargetTable}\n"
          f"TargetTableDataLakePath={TargetTableDataLakePath}\n")
    
    # based on TimeSlice validFromDate, validToDate to populate '_RecordStart', '_RecordEnd', '_RecordCurrent'
    df = df.withColumn("_BusinessKey", concat_ws('|', *(businessKey.split(","))))
    #df = df.withColumn("_RecordStart", expr(f"CAST(ifnull({fromDateCol},'1900-01-01') as timestamp)"))
    df = df.withColumn("_RecordStart", expr(f"cast(case when {fromDateCol} = '1000-01-01' then '1000-01-01T00:00:00.000+1000' when isnull({fromDateCol}) then '1900-01-01' else {fromDateCol} end as timestamp)"))
    #df = df.withColumn("_RecordEnd", expr(f"CAST((CAST(ifnull({toDateCol},'9999-12-31') as date) +1) - INTERVAL 1 SECOND as timestamp)"))
    
    if "_RecordDeleted" not in df.columns:
        df = df.withColumn("_RecordDeleted", expr("CAST(0 AS INT)"))
    else:
        df = df.withColumn("_RecordDeleted", when(col('_RecordDeleted').isNull(),expr(f"CAST(0 AS INT)")).otherwise(col('_RecordDeleted'))) 
    
    df = df.withColumn("_RecordEnd", when((col('_RecordDeleted')==1) & (col(toDateCol) > col('_DLCleansedZoneTimeStamp')), col('_DLCleansedZoneTimeStamp'))\
                       .otherwise(expr(f"CAST((CAST(ifnull({toDateCol},'9999-12-31') as date) +1) - INTERVAL 1 SECOND as timestamp)")))
    
    df = df.withColumn("_DLCuratedZoneTimeStamp", expr("now()"))
    df = df.withColumn(SurrogateKey, md5(expr(f"concat(_BusinessKey,'|',_RecordStart)")))
    
    # Latest _RecordStart date record marked as RecordCurrent = 1
    #window_Spec  = Window.partitionBy("_BusinessKey").orderBy(col("_RecordStart").desc(),)
    #df = df.withColumn("_RecordStart_Order",row_number().over(window_Spec))
    #df = df.withColumn("_RecordCurrent", when(col("_RecordStart_Order") == 1, 1).otherwise(0))
    #df = df.drop("_RecordStart_Order")
    
    # If current date is between vaildFrom and validTo, and _CurrentDeleted is 0, then _CurrentRecord = 1  
    #df = df.withColumn("_RecordCurrent", when(
    #    (current_date().between(col("_RecordStart"),col("_RecordEnd"))) & (col("_RecordDeleted") == 0) , 1).otherwise(0))
    
    df = df.withColumn("_RecordCurrent", when(col("_RecordDeleted") == 1 , 0).otherwise(1))
    
    df = df.select([field.name for field in schema] + ['_BusinessKey','_DLCuratedZoneTimeStamp','_RecordStart','_RecordEnd','_RecordDeleted','_RecordCurrent'])
    
    print("Overwrite Target Table")
    if is_uc():
        df.write.mode("overwrite").saveAsTable(TargetTable)
    else:
        df.write.mode("overwrite").saveAsTable(TargetTable, path=TargetTableDataLakePath)
    
    verifyTableSchema(f"{TARGET_LAYER}.{entity}", schema)

    LogEtl(f"Finished {entity}.")

# COMMAND ----------

# DBTITLE 1,5. Function: Create stage and curated database if not exist
def DatabaseChanges():
  #CREATE stage AND curated DATABASES IF NOT PRESENT
  spark.sql("CREATE DATABASE IF NOT EXISTS stage")
  spark.sql("CREATE DATABASE IF NOT EXISTS curated")  


# COMMAND ----------

#curnt_table = f'{ADS_DATABASE_CURATED}.dimDevice'
#curnt_pk = 'deviceNumber' 
#curnt_recordStart_pk = 'deviceNumber'
#history_table = f'{ADS_DATABASE_CURATED}.dimDeviceHistory'
#history_table_pk = 'deviceNumber'

def appendRecordStartFromHistoryTable(df,history_table,history_table_pk,curnt_pk,history_table_pk_convert,curnt_recordStart_pk):
    
    history_table = spark.sql(f"""
                            select {history_table_pk_convert}, min(_RecordStart) as _RecordStart from {history_table} group by {history_table_pk} 
                                """)
    df_ = df.join(history_table, curnt_recordStart_pk, how='left').select(df.columns + ['_RecordStart'])
    return df_   

#df_ = appendRecordStartFromHistoryTable(df,history_table,history_table_pk)

def updateDBTableWithLatestRecordStart(df_, curnt_table, curnt_pk):
    
    if (not(TableExistsUC(curnt_table))):
        return

    df_db = spark.sql(f"""
                        select {curnt_pk}, min(_RecordStart) as _RecordStart from {curnt_table} group by {curnt_pk} 
                            """)

    df_update = df_db.alias('db')\
                    .join(df_.alias('new'), curnt_pk.split(','), how='inner')\
                    .where('db._RecordStart > new._RecordStart')\
                    .selectExpr(*(curnt_pk.split(',') + ['db._RecordStart as db_RecordStart','new._RecordStart as new_RecordStart'])) 
    
    num_update = df_update.count()
    
    print(f"There are {num_update} records have historic earlist start dates changes")
    
    if num_update == 0 :
        return

    key_conditions = " AND ".join(['s.' + c + '=' + 't.' + c for c in curnt_pk.split(",")])

    DeltaTable.forName(spark, curnt_table).alias('t')\
        .merge(df_update.alias('s'),'(' + key_conditions + ') AND (t._RecordStart = s.db_RecordStart)')\
        .whenMatchedUpdate(set={"_RecordStart":"s.new_RecordStart"})\
        .execute()
    
#updateDBTableWithLatestRecordStart(df_, curnt_table, curnt_pk)

# COMMAND ----------

def viewExists(viewFqn):
    spark.sql(f"use catalog {viewFqn.split('.')[0]}")
    return (
        spark.sql(f"show views in {'.'.join(viewFqn.split('.')[:-1])} like '{viewFqn.split('.')[-1]}'").count() == 1
    )