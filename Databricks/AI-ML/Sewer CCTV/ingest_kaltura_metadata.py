# Databricks notebook source
# MAGIC %md
# MAGIC ## 0. Create Kaltura Tables in the cleansed layer if they don't exist

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create cctv_kaltura_flavors table in cleansed layer
# MAGIC CREATE TABLE IF NOT EXISTS cleansed.cctv_kaltura_flavors
# MAGIC (id STRING,
# MAGIC   entryId STRING,
# MAGIC   partnerId BIGINT,
# MAGIC   version BIGINT,
# MAGIC   size STRING,
# MAGIC   tags STRING,
# MAGIC   fileExt STRING,
# MAGIC   createdAt TIMESTAMP,
# MAGIC   updatedAt TIMESTAMP,
# MAGIC   description STRING,
# MAGIC   sizeInBytes STRING,
# MAGIC   flavorParamsId STRING,
# MAGIC   width BIGINT,
# MAGIC   height BIGINT,
# MAGIC   bitrate BIGINT,
# MAGIC   frameRate BIGINT,
# MAGIC   isOriginal BOOLEAN,
# MAGIC   isWeb BOOLEAN,
# MAGIC   containerFormat STRING,
# MAGIC   videoCodecId STRING,
# MAGIC   status STRING,
# MAGIC   isDefault BOOLEAN,
# MAGIC   language BIGINT,
# MAGIC   processed_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA 
# MAGIC LOCATION 'dbfs:/mnt/datalake-cleansed/kaltura/cctv_kaltura_flavors'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create cctv_kaltura_metadata table in cleansed layer
# MAGIC CREATE TABLE IF NOT EXISTS cleansed.cctv_kaltura_metadata
# MAGIC (id BIGINT,
# MAGIC   partnerId BIGINT,
# MAGIC   metadataProfileId BIGINT,
# MAGIC   metadataProfileVersion BIGINT,
# MAGIC   metadataObjectType STRING,
# MAGIC   objectId STRING,
# MAGIC   version BIGINT,
# MAGIC   status STRING,
# MAGIC   AssessedByName STRING,
# MAGIC   AssessedByDate TIMESTAMP,
# MAGIC   ParentWorkOrderNumber STRING,
# MAGIC   ChildWorkOrderNumbers STRING,
# MAGIC   WorkOrderDescription STRING,
# MAGIC   AssetNumbers STRING,
# MAGIC   TaskCode STRING,
# MAGIC   Suburb STRING,
# MAGIC   AddressStreet STRING,
# MAGIC   Product STRING,
# MAGIC   Contractor STRING,
# MAGIC   UpstreamMH STRING,
# MAGIC   DownstreamMH STRING,
# MAGIC   DirectionOfSurvey STRING,
# MAGIC   DateOfCompletedInspection TIMESTAMP,
# MAGIC   TimeOfCompletedInspection TIMESTAMP,
# MAGIC   PackageName STRING,
# MAGIC   Cleaned STRING,
# MAGIC   SurveyedLength DECIMAL,
# MAGIC   DiscardDate TIMESTAMP,
# MAGIC   Condition STRING,
# MAGIC   Serviceability STRING,
# MAGIC   Infiltration STRING,
# MAGIC   createdAt TIMESTAMP,
# MAGIC   updatedAt TIMESTAMP,
# MAGIC   processed_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA 
# MAGIC LOCATION 'dbfs:/mnt/datalake-cleansed/kaltura/cctv_kaltura_metadata'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create cctv_kaltura_attachments table in cleansed layer
# MAGIC CREATE TABLE IF NOT EXISTS cleansed.cctv_kaltura_attachments
# MAGIC (id STRING,
# MAGIC   entryId STRING,
# MAGIC   partnerId BIGINT,
# MAGIC   version BIGINT,
# MAGIC   size STRING,
# MAGIC   tags STRING,
# MAGIC   fileExt STRING,
# MAGIC   createdAt TIMESTAMP,
# MAGIC   updatedAt TIMESTAMP,
# MAGIC   description STRING,
# MAGIC   partnerDescription STRING,
# MAGIC   sizeInBytes STRING,
# MAGIC   filename STRING,
# MAGIC   title STRING,
# MAGIC   format STRING,
# MAGIC   status STRING,
# MAGIC   processed_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA 
# MAGIC LOCATION 'dbfs:/mnt/datalake-cleansed/kaltura/cctv_kaltura_attachments'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create cctv_kaltura_attachments table in cleansed layer
# MAGIC CREATE TABLE IF NOT EXISTS cleansed.cctv_kaltura_media_entry
# MAGIC (id STRING,
# MAGIC   name STRING,
# MAGIC   description STRING,
# MAGIC   partnerId BIGINT,
# MAGIC   userId STRING,
# MAGIC   creatorId STRING,
# MAGIC   tags STRING,
# MAGIC   categories STRING,
# MAGIC   categoriesIds STRING,
# MAGIC   status STRING,
# MAGIC   moderationStatus STRING,
# MAGIC   moderationCount BIGINT,
# MAGIC   type STRING,
# MAGIC   createdAt TIMESTAMP,
# MAGIC   updatedAt TIMESTAMP,
# MAGIC   rank FLOAT,
# MAGIC   totalRank BIGINT,
# MAGIC   votes BIGINT,
# MAGIC   downloadUrl STRING,
# MAGIC   searchText STRING,
# MAGIC   licenseType STRING,
# MAGIC   version BIGINT,
# MAGIC   thumbnailUrl STRING,
# MAGIC   replacementStatus STRING,
# MAGIC   partnerSortValue BIGINT,
# MAGIC   conversionProfileId BIGINT,
# MAGIC   rootEntryId STRING,
# MAGIC   operationAttributes STRING,
# MAGIC   entitledUsersEdit STRING,
# MAGIC   entitledUsersPublish STRING,
# MAGIC   entitledUsersView STRING,
# MAGIC   capabilities STRING,
# MAGIC   displayInSearch STRING,
# MAGIC   blockAutoTranscript BOOLEAN,
# MAGIC   plays BIGINT,
# MAGIC   views BIGINT,
# MAGIC   lastPlayedAt TIMESTAMP,
# MAGIC   width BIGINT,
# MAGIC   height BIGINT,
# MAGIC   duration BIGINT,
# MAGIC   msDuration BIGINT,
# MAGIC   mediaType STRING,
# MAGIC   conversionQuality STRING,
# MAGIC   sourceType STRING,
# MAGIC   dataUrl STRING,
# MAGIC   flavorParamsIds STRING,
# MAGIC   processed_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA 
# MAGIC LOCATION 'dbfs:/mnt/datalake-cleansed/kaltura/cctv_kaltura_media_entry'

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Utility Functions

# COMMAND ----------

def JdbcConnectionFromSqlConnectionString(connectionString):
    connectionList = dict()

    for i in connectionString.split(";"):
        if(len(i.split("=")) != 2): 
            continue

        key = i.split("=")[0].strip()
        value = i.split("=")[1].strip()
        connectionList[key] = value

    server = connectionList["Server"].replace(",1433", "").replace("tcp:", "")
    database = connectionList["Initial Catalog"]
    username = connectionList["User ID"]
    password = connectionList["Password"]

    jdbc = f"jdbc:sqlserver://{server};DatabaseName={database};Persist Security Info=False;user={username};Password={password};"
    return jdbc

# COMMAND ----------

def RunQuery(sql):
    jdbc = JdbcConnectionFromSqlConnectionString(dbutils.secrets.get(scope = "ADS", key = "daf-sql-sewercctv-connectionstring"))
    return spark.read.option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").jdbc(url=jdbc, table=f"({sql}) T")

# COMMAND ----------

def ExecuteStatement(sql):
    jdbc = JdbcConnectionFromSqlConnectionString(dbutils.secrets.get(scope = "ADS", key = "daf-sql-sewercctv-connectionstring"))
    connection = spark._sc._gateway.jvm.java.sql.DriverManager.getConnection(jdbc)
    connection.prepareCall(sql).execute()
    connection.close()

# COMMAND ----------

def WriteTableToAzureSQL(df, mode, targetTable):
    jdbc = JdbcConnectionFromSqlConnectionString(dbutils.secrets.get(scope = "ADS", key = "daf-sql-sewercctv-connectionstring"))
    df.select("*").write.format("jdbc") \
            .mode(mode) \
            .option("url", jdbc) \
            .option("dbtable", targetTable) \
            .save()

# COMMAND ----------

try:
    spark.sql("CREATE DATABASE sewerCCTV")
except:
    pass

# COMMAND ----------

def CreateSewerCCTVViews():
    jdbc = JdbcConnectionFromSqlConnectionString(dbutils.secrets.get(scope = "ADS", key = "daf-sql-sewercctv-connectionstring"))

    sql = """
    SELECT TABLE_SCHEMA as [SchemaName]
           ,TABLE_Name as [TableName]
           from information_schema.tables where table_type='BASE TABLE'
    
"""
    df = spark.read.option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").jdbc(url=jdbc, table=f"({sql}) T")

    for i in df.rdd.collect():
        schema = i.SchemaName
        table = i.TableName

        if(table.lower() in ["__refactorlog", "sysdiagrams"]):
            continue
        
        df = spark.read.option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").jdbc(url=jdbc, table=f"{schema}.{table}")
        
        df.createOrReplaceTempView(f"sewercctv_{schema}_{table}")
        
        try:
            spark.sql(f"DROP TABLE sewerCCTV.{schema}_{table}")
        except:
            pass
        sql = f"""CREATE TABLE sewercctv.{schema}_{table}
                USING org.apache.spark.sql.jdbc
                OPTIONS (
                  url \"{jdbc}\",
                  dbtable \"{schema}.{table}\"
                )"""
        #print(sql)
        spark.sql(sql)
        #break
        
        #spark.sql(f"DROP VIEW controldb_{schema}_{table}")

        #break


# COMMAND ----------

CreateSewerCCTVViews()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read Kaltura (JSON) data from blob storage

# COMMAND ----------

dbutils.widgets.text(name="json_file_name", defaultValue="kaltura_json_2.json", label="json_file_name")
dbutils.widgets.get("json_file_name")

filepath = "/mnt/blob-kaltura/Inbound/" + dbutils.widgets.get("json_file_name")
filepath

# COMMAND ----------

from pyspark.sql import functions as psf

df_kaltura_flavors = (spark.read.json(filepath)
                      .withColumn("flavors", psf.col("flavors")[0])
                      .withColumn("createdAt", psf.to_timestamp(psf.col("flavors.createdAt"), format='yyyy-MM-dd HH:mm:ss'))
                      .withColumn("updatedAt", psf.to_timestamp(psf.col("flavors.updatedAt"), format='yyyy-MM-dd HH:mm:ss'))
                      .withColumn("processed_timestamp", psf.current_timestamp())
                      .where(psf.col("flavors").isNotNull())
                      .select("update", 
                              "flavors.id",
                              "flavors.entryId",
                              "flavors.partnerId",
                              "flavors.version",
                              "flavors.size",
                              "flavors.tags",
                              "flavors.fileExt",
                              "createdAt",
                              "updatedAt",
                              "flavors.description",
                              "flavors.sizeInBytes",
                              "flavors.flavorParamsId",
                              "flavors.width",
                              "flavors.height",
                              "flavors.bitrate",
                              "flavors.frameRate",
                              "flavors.isOriginal",
                              "flavors.isWeb",
                              "flavors.containerFormat",
                              "flavors.videoCodecId",
                              "flavors.status",
                              "flavors.language",
                              "flavors.isDefault",
                              "processed_timestamp"
                             )
                         )
df_kaltura_flavors.createOrReplaceTempView("tmp_datalake_kaltura_flavors")

df_kaltura_attachments = (spark.read.json(filepath)
                          .withColumn("attachments", psf.col("attachments")[0])
                          .withColumn("createdAt", psf.to_timestamp(psf.col("attachments.createdAt"), format='yyyy-MM-dd HH:mm:ss'))
                          .withColumn("updatedAt", psf.to_timestamp(psf.col("attachments.updatedAt"), format='yyyy-MM-dd HH:mm:ss'))
                          .withColumn("processed_timestamp", psf.current_timestamp())
                          .where(psf.col("attachments").isNotNull())
                          .select("update", 
                                  "attachments.id",
                                  "attachments.entryId",
                                  "attachments.partnerId",
                                  "attachments.version",
                                  "attachments.size",
                                  "attachments.tags",
                                  "attachments.fileExt",
                                  "createdAt",
                                  "updatedAt",
                                  "attachments.description",
                                  "attachments.partnerDescription",
                                  "attachments.sizeInBytes",
                                  "attachments.filename",
                                  "attachments.title",
                                  "attachments.format",
                                  "attachments.status",
                                  "processed_timestamp"
                                 )
                         )
df_kaltura_attachments.createOrReplaceTempView("tmp_datalake_kaltura_attachments")


df_kaltura_metadata = spark.read.json(filepath).withColumn("metadata", psf.col("metadata")[0])
df_kaltura_metadata = (df_kaltura_metadata
                       .withColumn("AssessedByDate", psf.to_timestamp(psf.col("metadata.jsonData.AssessedByDate"), format='yyyy-MM-dd HH:mm:ss'))
                       .withColumn("DiscardDate", psf.to_timestamp(psf.col("metadata.jsonData.DiscardDate"), format='yyyy-MM-dd HH:mm:ss'))
                       .withColumn("DateOfCompletedInspection", psf.to_timestamp(psf.col("metadata.jsonData.DateOfCompletedInspection"), format='yyyy-MM-dd HH:mm:ss'))
                       .withColumn("TimeOfCompletedInspection", psf.to_timestamp(psf.col("metadata.jsonData.TimeOfCompletedInspection"), format='yyyy-MM-dd HH:mm:ss'))
                       .withColumn("createdAt", psf.to_timestamp(psf.col("metadata.createdAt"), format='yyyy-MM-dd HH:mm:ss'))
                       .withColumn("updatedAt", psf.to_timestamp(psf.col("metadata.updatedAt"), format='yyyy-MM-dd HH:mm:ss'))
                       .withColumn("SurveyedLength", psf.col("metadata.jsonData.SurveyedLength").cast('decimal'))
                       .withColumn("processed_timestamp", psf.current_timestamp())
                       .where(psf.col("metadata").isNotNull())
                       .select("update", 
                               "metadata.id",
                               "metadata.partnerId",
                               "metadata.metadataProfileId",
                               "metadata.metadataProfileVersion",
                               "metadata.metadataObjectType",
                               "metadata.objectId",
                               "metadata.version",
                               "metadata.status",
                               "metadata.jsonData.AssessedByName",
                               "AssessedByDate",
                               "metadata.jsonData.ParentWorkOrderNumber",
                               "metadata.jsonData.ChildWorkOrderNumbers",
                               "metadata.jsonData.WorkOrderDescription",
                               "metadata.jsonData.AssetNumbers",
                               "metadata.jsonData.TaskCode",
                               "metadata.jsonData.Suburb",
                               "metadata.jsonData.AddressStreet",
                               "metadata.jsonData.Product",
                               "metadata.jsonData.Contractor",
                               "metadata.jsonData.UpstreamMH",
                               "metadata.jsonData.DownstreamMH",
                               "metadata.jsonData.DirectionOfSurvey",
                               "DateOfCompletedInspection",
                               "TimeOfCompletedInspection",
                               "metadata.jsonData.PackageName",
                               "metadata.jsonData.Cleaned",
                               "SurveyedLength",
                               "DiscardDate",
                               "metadata.jsonData.Condition",
                               "metadata.jsonData.Serviceability",
                               "metadata.jsonData.Infiltration",
                               "createdAt",
                               "updatedAt",
                               "processed_timestamp")
                         )
df_kaltura_metadata.createOrReplaceTempView("tmp_datalake_kaltura_metadata")

df_kaltura_media_entry = (spark.read.json(filepath)
                          .withColumn("processed_timestamp", psf.current_timestamp())
                          .where(psf.col("mediaEntry").isNotNull())
                          .withColumn("createdAt", psf.to_timestamp(psf.col("mediaEntry.createdAt"), format='yyyy-MM-dd HH:mm:ss'))
                          .withColumn("updatedAt", psf.to_timestamp(psf.col("mediaEntry.updatedAt"), format='yyyy-MM-dd HH:mm:ss'))
                          .withColumn("lastPlayedAt", psf.to_timestamp(psf.col("mediaEntry.lastPlayedAt"), format='yyyy-MM-dd HH:mm:ss'))
                          .withColumn("operationAttributes", psf.col("mediaEntry.operationAttributes").cast("string"))
                          .select("update", 
                                  "mediaEntry.id",
                                  "mediaEntry.name",
                                  "mediaEntry.description",
                                  "mediaEntry.partnerId",
                                  "mediaEntry.userId",
                                  "mediaEntry.creatorId",
                                  "mediaEntry.tags",
                                  "mediaEntry.categories",
                                  "mediaEntry.categoriesIds",
                                  "mediaEntry.status",
                                  "mediaEntry.moderationStatus",
                                  "mediaEntry.moderationCount",
                                  "mediaEntry.type",
                                  "createdAt",
                                  "updatedAt",
                                  "mediaEntry.rank",
                                  "mediaEntry.totalRank",
                                  "mediaEntry.votes",
                                  "mediaEntry.downloadUrl",
                                  "mediaEntry.searchText",
                                  "mediaEntry.licenseType",
                                  "mediaEntry.version",
                                  "mediaEntry.thumbnailUrl",
                                  "mediaEntry.replacementStatus",
                                  "mediaEntry.partnerSortValue",
                                  "mediaEntry.conversionProfileId",
                                  "mediaEntry.rootEntryId",
                                  "operationAttributes",
                                  "mediaEntry.entitledUsersEdit",
                                  "mediaEntry.entitledUsersPublish",
                                  "mediaEntry.entitledUsersView",
                                  "mediaEntry.capabilities",
                                  "mediaEntry.displayInSearch",
                                  "mediaEntry.blockAutoTranscript",
                                  "mediaEntry.plays",
                                  "mediaEntry.views",
                                  "lastPlayedAt",
                                  "mediaEntry.width",
                                  "mediaEntry.height",
                                  "mediaEntry.duration",
                                  "mediaEntry.msDuration",
                                  "mediaEntry.mediaType",
                                  "mediaEntry.conversionQuality",
                                  "mediaEntry.sourceType",
                                  "mediaEntry.dataUrl",
                                  "mediaEntry.flavorParamsIds",
                                  "processed_timestamp")
                         )
df_kaltura_media_entry.createOrReplaceTempView("tmp_datalake_kaltura_media_entry")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Write Kaltura Data to Cleansed Layer

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC MERGE INTO cleansed.cctv_kaltura_flavors as tgt
# MAGIC USING tmp_datalake_kaltura_flavors as src
# MAGIC ON (tgt.id = src.id)
# MAGIC 
# MAGIC WHEN MATCHED THEN
# MAGIC     UPDATE SET id = src.id,
# MAGIC         entryId = src.entryId,
# MAGIC         partnerId = src.partnerId,
# MAGIC         version = src.version,
# MAGIC         size = src.size,
# MAGIC         tags = src.tags,
# MAGIC         fileExt = src.fileExt,
# MAGIC         createdAt = src.createdAt,
# MAGIC         updatedAt = src.updatedAt,
# MAGIC         description = src.description,
# MAGIC         sizeInBytes = src.sizeInBytes,
# MAGIC         flavorParamsId = src.flavorParamsId,
# MAGIC         width  = src.width,
# MAGIC         height = src.height,
# MAGIC         bitrate = src.bitrate,
# MAGIC         frameRate = src.frameRate,
# MAGIC         isOriginal = src.isOriginal,
# MAGIC         isWeb = src.isWeb,
# MAGIC         containerFormat = src.containerFormat,
# MAGIC         videoCodecId = src.videoCodecId,
# MAGIC         status = src.status,
# MAGIC         isDefault = src.isDefault,
# MAGIC         language = src.language,
# MAGIC         processed_timestamp = src.processed_timestamp
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (
# MAGIC     id,
# MAGIC     entryId,
# MAGIC     partnerId,
# MAGIC     version,
# MAGIC     size,
# MAGIC     tags,
# MAGIC     fileExt,
# MAGIC     createdAt,
# MAGIC     updatedAt,
# MAGIC     description,
# MAGIC     sizeInBytes,
# MAGIC     flavorParamsId,
# MAGIC     width,
# MAGIC     height,
# MAGIC     bitrate,
# MAGIC     frameRate,
# MAGIC     isOriginal,
# MAGIC     isWeb,
# MAGIC     containerFormat,
# MAGIC     videoCodecId,
# MAGIC     status,
# MAGIC     isDefault,
# MAGIC     language,
# MAGIC     processed_timestamp
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     src.id,
# MAGIC     src.entryId,
# MAGIC     src.partnerId,
# MAGIC     src.version,
# MAGIC     src.size,
# MAGIC     src.tags,
# MAGIC     src.fileExt,
# MAGIC     src.createdAt,
# MAGIC     src.updatedAt,
# MAGIC     src.description,
# MAGIC     src.sizeInBytes,
# MAGIC     src.flavorParamsId,
# MAGIC     src.width,
# MAGIC     src.height,
# MAGIC     src.bitrate,
# MAGIC     src.frameRate,
# MAGIC     src.isOriginal,
# MAGIC     src.isWeb,
# MAGIC     src.containerFormat,
# MAGIC     src.videoCodecId,
# MAGIC     src.status,
# MAGIC     src.isDefault,
# MAGIC     src.language,
# MAGIC     src.processed_timestamp
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC MERGE INTO cleansed.cctv_kaltura_metadata as tgt
# MAGIC USING tmp_datalake_kaltura_metadata as src
# MAGIC ON (tgt.id = src.id)
# MAGIC WHEN MATCHED THEN
# MAGIC     UPDATE SET id = src.id,
# MAGIC         partnerId = src.partnerId,
# MAGIC         metadataProfileId = src.metadataProfileId,
# MAGIC         metadataProfileVersion = src.metadataProfileVersion,
# MAGIC         metadataObjectType = src.metadataObjectType,
# MAGIC         objectId = src.objectId,
# MAGIC         version = src.version,
# MAGIC         status = src.status,
# MAGIC         AssessedByName = src.AssessedByName,
# MAGIC         AssessedByDate = src.AssessedByDate,
# MAGIC         ParentWorkOrderNumber = src.ParentWorkOrderNumber,
# MAGIC         ChildWorkOrderNumbers = src.ChildWorkOrderNumbers,
# MAGIC         WorkOrderDescription = src.WorkOrderDescription,
# MAGIC         AssetNumbers = src.AssetNumbers,
# MAGIC         TaskCode = src.TaskCode,
# MAGIC         Suburb = src.Suburb,
# MAGIC         AddressStreet = src.AddressStreet,
# MAGIC         Product = src.Product,
# MAGIC         Contractor = src.Contractor,
# MAGIC         UpstreamMH = src.UpstreamMH,
# MAGIC         DownstreamMH = src.DownstreamMH,
# MAGIC         DirectionOfSurvey = src.DirectionOfSurvey,
# MAGIC         DateOfCompletedInspection = src.DateOfCompletedInspection,
# MAGIC         TimeOfCompletedInspection = src.TimeOfCompletedInspection,
# MAGIC         PackageName = src.PackageName,
# MAGIC         Cleaned = src.Cleaned,
# MAGIC         SurveyedLength = src.SurveyedLength,
# MAGIC         DiscardDate = src.DiscardDate,
# MAGIC         Condition = src.Condition,
# MAGIC         Serviceability = src.Serviceability,
# MAGIC         Infiltration = src.Infiltration,
# MAGIC         createdAt = src.createdAt,
# MAGIC         updatedAt = src.updatedAt,
# MAGIC         processed_timestamp = src.processed_timestamp
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (
# MAGIC     id,
# MAGIC     partnerId,
# MAGIC     metadataProfileId,
# MAGIC     metadataProfileVersion,
# MAGIC     metadataObjectType,
# MAGIC     objectId,
# MAGIC     version,
# MAGIC     status,
# MAGIC     AssessedByName,
# MAGIC     AssessedByDate,
# MAGIC     ParentWorkOrderNumber,
# MAGIC     ChildWorkOrderNumbers,
# MAGIC     WorkOrderDescription,
# MAGIC     AssetNumbers,
# MAGIC     TaskCode,
# MAGIC     Suburb,
# MAGIC     AddressStreet,
# MAGIC     Product,
# MAGIC     Contractor,
# MAGIC     UpstreamMH,
# MAGIC     DownstreamMH,
# MAGIC     DirectionOfSurvey,
# MAGIC     DateOfCompletedInspection,
# MAGIC     TimeOfCompletedInspection,
# MAGIC     PackageName,
# MAGIC     Cleaned,
# MAGIC     SurveyedLength,
# MAGIC     DiscardDate,
# MAGIC     Condition,
# MAGIC     Serviceability,
# MAGIC     Infiltration,
# MAGIC     createdAt,
# MAGIC     updatedAt,
# MAGIC     processed_timestamp
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     src.id,
# MAGIC     src.partnerId,
# MAGIC     src.metadataProfileId,
# MAGIC     src.metadataProfileVersion,
# MAGIC     src.metadataObjectType,
# MAGIC     src.objectId,
# MAGIC     src.version,
# MAGIC     src.status,
# MAGIC     src.AssessedByName,
# MAGIC     src.AssessedByDate,
# MAGIC     src.ParentWorkOrderNumber,
# MAGIC     src.ChildWorkOrderNumbers,
# MAGIC     src.WorkOrderDescription,
# MAGIC     src.AssetNumbers,
# MAGIC     src.TaskCode,
# MAGIC     src.Suburb,
# MAGIC     src.AddressStreet,
# MAGIC     src.Product,
# MAGIC     src.Contractor,
# MAGIC     src.UpstreamMH,
# MAGIC     src.DownstreamMH,
# MAGIC     src.DirectionOfSurvey,
# MAGIC     src.DateOfCompletedInspection,
# MAGIC     src.TimeOfCompletedInspection,
# MAGIC     src.PackageName,
# MAGIC     src.Cleaned,
# MAGIC     src.SurveyedLength,
# MAGIC     src.DiscardDate,
# MAGIC     src.Condition,
# MAGIC     src.Serviceability,
# MAGIC     src.Infiltration,
# MAGIC     src.createdAt,
# MAGIC     src.updatedAt,
# MAGIC     src.processed_timestamp
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC MERGE INTO cleansed.cctv_kaltura_attachments as tgt
# MAGIC USING tmp_datalake_kaltura_attachments as src
# MAGIC ON (tgt.id = src.id)
# MAGIC WHEN MATCHED THEN
# MAGIC     UPDATE SET id = src.id,
# MAGIC         entryId = src.entryId,
# MAGIC         partnerId = src.partnerId,
# MAGIC         version = src.version,
# MAGIC         size = src.size,
# MAGIC         tags = src.tags,
# MAGIC         fileExt = src.fileExt,
# MAGIC         createdAt = src.createdAt,
# MAGIC         updatedAt = src.updatedAt,
# MAGIC         description = src.description,
# MAGIC         partnerDescription = src.partnerDescription,
# MAGIC         sizeInBytes = src.sizeInBytes,
# MAGIC         filename = src.filename,
# MAGIC         title = src.title,
# MAGIC         format = src.format,
# MAGIC         status = src.status,
# MAGIC         processed_timestamp = src.processed_timestamp
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (
# MAGIC     id,
# MAGIC     entryId,
# MAGIC     partnerId,
# MAGIC     version,
# MAGIC     size,
# MAGIC     tags,
# MAGIC     fileExt,
# MAGIC     createdAt,
# MAGIC     updatedAt,
# MAGIC     description,
# MAGIC     partnerDescription,
# MAGIC     sizeInBytes,
# MAGIC     filename,
# MAGIC     title,
# MAGIC     format,
# MAGIC     status,
# MAGIC     processed_timestamp
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     src.id,
# MAGIC     src.entryId,
# MAGIC     src.partnerId,
# MAGIC     src.version,
# MAGIC     src.size,
# MAGIC     src.tags,
# MAGIC     src.fileExt,
# MAGIC     src.createdAt,
# MAGIC     src.updatedAt,
# MAGIC     src.description,
# MAGIC     src.partnerDescription,
# MAGIC     src.sizeInBytes,
# MAGIC     src.filename,
# MAGIC     src.title,
# MAGIC     src.format,
# MAGIC     src.status,
# MAGIC     src.processed_timestamp
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO cleansed.cctv_kaltura_media_entry as tgt
# MAGIC USING tmp_datalake_kaltura_media_entry as src
# MAGIC ON (tgt.id = src.id)
# MAGIC WHEN MATCHED THEN
# MAGIC     UPDATE SET id = src.id,
# MAGIC         name = src.name,
# MAGIC         description = src.description,
# MAGIC         partnerId = src.partnerId,
# MAGIC         userId = src.userId,
# MAGIC         creatorId = src.creatorId,
# MAGIC         tags = src.tags,
# MAGIC         categories = src.categories,
# MAGIC         categoriesIds = src.categoriesIds,
# MAGIC         status = src.status,
# MAGIC         moderationStatus = src.moderationStatus,
# MAGIC         moderationCount = src.moderationCount,
# MAGIC         type = src.type,
# MAGIC         createdAt = src.createdAt,
# MAGIC         updatedAt = src.updatedAt,
# MAGIC         rank = src.rank,
# MAGIC         totalRank = src.totalRank,
# MAGIC         votes = src.votes,
# MAGIC         downloadUrl = src.downloadUrl,
# MAGIC         searchText = src.searchText,
# MAGIC         licenseType = src.licenseType,
# MAGIC         version = src.version,
# MAGIC         thumbnailUrl = src.thumbnailUrl,
# MAGIC         replacementStatus = src.replacementStatus,
# MAGIC         partnerSortValue = src.partnerSortValue,
# MAGIC         conversionProfileId = src.conversionProfileId,
# MAGIC         rootEntryId = src.rootEntryId,
# MAGIC         operationAttributes = src.operationAttributes,
# MAGIC         entitledUsersEdit = src.entitledUsersEdit,
# MAGIC         entitledUsersPublish = src.entitledUsersPublish,
# MAGIC         entitledUsersView = src.entitledUsersView,
# MAGIC         capabilities = src.capabilities,
# MAGIC         displayInSearch = src.displayInSearch,
# MAGIC         blockAutoTranscript = src.blockAutoTranscript,
# MAGIC         plays = src.plays,
# MAGIC         views = src.views,
# MAGIC         lastPlayedAt = src.lastPlayedAt,
# MAGIC         width = src.width,
# MAGIC         height = src.height,
# MAGIC         duration = src.duration,
# MAGIC         msDuration = src.msDuration,
# MAGIC         mediaType = src.mediaType,
# MAGIC         conversionQuality = src.conversionQuality,
# MAGIC         sourceType = src.sourceType,
# MAGIC         dataUrl = src.dataUrl,
# MAGIC         flavorParamsIds = src.flavorParamsIds,
# MAGIC         processed_timestamp = src.processed_timestamp
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (
# MAGIC     id,
# MAGIC     name,
# MAGIC     description,
# MAGIC     partnerId,
# MAGIC     userId,
# MAGIC     creatorId,
# MAGIC     tags,
# MAGIC     categories,
# MAGIC     categoriesIds,
# MAGIC     status,
# MAGIC     moderationStatus,
# MAGIC     moderationCount,
# MAGIC     type,
# MAGIC     createdAt,
# MAGIC     updatedAt,
# MAGIC     rank,
# MAGIC     totalRank,
# MAGIC     votes,
# MAGIC     downloadUrl,
# MAGIC     searchText,
# MAGIC     licenseType,
# MAGIC     version,
# MAGIC     thumbnailUrl,
# MAGIC     replacementStatus,
# MAGIC     partnerSortValue,
# MAGIC     conversionProfileId,
# MAGIC     rootEntryId,
# MAGIC     operationAttributes,
# MAGIC     entitledUsersEdit,
# MAGIC     entitledUsersPublish,
# MAGIC     entitledUsersView,
# MAGIC     capabilities,
# MAGIC     displayInSearch,
# MAGIC     blockAutoTranscript,
# MAGIC     plays,
# MAGIC     views,
# MAGIC     lastPlayedAt,
# MAGIC     width,
# MAGIC     height,
# MAGIC     duration,
# MAGIC     msDuration,
# MAGIC     mediaType,
# MAGIC     conversionQuality,
# MAGIC     sourceType,
# MAGIC     dataUrl,
# MAGIC     flavorParamsIds,
# MAGIC     processed_timestamp
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     src.id,
# MAGIC     src.name,
# MAGIC     src.description,
# MAGIC     src.partnerId,
# MAGIC     src.userId,
# MAGIC     src.creatorId,
# MAGIC     src.tags,
# MAGIC     src.categories,
# MAGIC     src.categoriesIds,
# MAGIC     src.status,
# MAGIC     src.moderationStatus,
# MAGIC     src.moderationCount,
# MAGIC     src.type,
# MAGIC     src.createdAt,
# MAGIC     src.updatedAt,
# MAGIC     src.rank,
# MAGIC     src.totalRank,
# MAGIC     src.votes,
# MAGIC     src.downloadUrl,
# MAGIC     src.searchText,
# MAGIC     src.licenseType,
# MAGIC     src.version,
# MAGIC     src.thumbnailUrl,
# MAGIC     src.replacementStatus,
# MAGIC     src.partnerSortValue,
# MAGIC     src.conversionProfileId,
# MAGIC     src.rootEntryId,
# MAGIC     src.operationAttributes,
# MAGIC     src.entitledUsersEdit,
# MAGIC     src.entitledUsersPublish,
# MAGIC     src.entitledUsersView,
# MAGIC     src.capabilities,
# MAGIC     src.displayInSearch,
# MAGIC     src.blockAutoTranscript,
# MAGIC     src.plays,
# MAGIC     src.views,
# MAGIC     src.lastPlayedAt,
# MAGIC     src.width,
# MAGIC     src.height,
# MAGIC     src.duration,
# MAGIC     src.msDuration,
# MAGIC     src.mediaType,
# MAGIC     src.conversionQuality,
# MAGIC     src.sourceType,
# MAGIC     src.dataUrl,
# MAGIC     src.flavorParamsIds,
# MAGIC     src.processed_timestamp
# MAGIC   )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Write Kaltura Data to Azure SQL Staging

# COMMAND ----------

WriteTableToAzureSQL(df=df_kaltura_flavors, mode="overwrite", targetTable="tmp_datalake_kaltura_flavors")
WriteTableToAzureSQL(df=df_kaltura_attachments, mode="overwrite", targetTable="tmp_datalake_kaltura_attachments")
WriteTableToAzureSQL(df=df_kaltura_metadata, mode="overwrite", targetTable="tmp_datalake_kaltura_metadata")
WriteTableToAzureSQL(df=df_kaltura_media_entry, mode="overwrite", targetTable="tmp_datalake_kaltura_media_entry")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Merge Kaltura data from tmp datalake view to dbo staging and then to CCTVPortal

# COMMAND ----------

ExecuteStatement("EXEC  [dbo].[sp_merge_datalakeview_to_dbo_kaltura]")
ExecuteStatement("EXEC  [dbo].[sp_merge_dbo_to_CCTVPortal_kaltura]")

# COMMAND ----------


