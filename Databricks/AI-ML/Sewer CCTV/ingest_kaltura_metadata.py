# Databricks notebook source
# MAGIC %sql
# MAGIC -- SET JOB POOL TO USE UTC DATE. THIS IS NECESSARY OTHERWISE OLD DATA WOULD OTHERWISE BE RETRIEVED
# MAGIC SET TIME ZONE 'Australia/Sydney';

# COMMAND ----------

# MAGIC %run MDP-Framework/Common/common-helpers

# COMMAND ----------

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
# MAGIC   PriorityJustification STRING,
# MAGIC   OperationalArea STRING,
# MAGIC   AssetNumbers STRING,
# MAGIC   TaskCode STRING,
# MAGIC   Suburb STRING,
# MAGIC   AddressStreet STRING,
# MAGIC   Product STRING,
# MAGIC   Contractor STRING,
# MAGIC   UpstreamMH STRING,
# MAGIC   DownstreamMH STRING,
# MAGIC   DirectionOfSurvey STRING,
# MAGIC   DateOfCompletedInspectionString STRING,
# MAGIC   TimeOfCompletedInspectionString STRING,
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
# MAGIC   sourceFileName STRING,
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

    for i in df.collect():
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

spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")

# COMMAND ----------

dbutils.widgets.text(name="json_file_name", defaultValue="kaltura_json_2.json", label="json_file_name")
dbutils.widgets.get("json_file_name")

filepath = "/mnt/blob-kaltura/Inbound/" + dbutils.widgets.get("json_file_name")
filepath

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

schema = StructType([
    StructField("update",BooleanType(),True),
    StructField("attachments",
                ArrayType(
                    StructType([
                        StructField("createdAt",StringType(),True),
                        StructField("description",StringType(),True),
                        StructField("entryId",StringType(),True),
                        StructField("fileExt",StringType(),True),
                        StructField("filename",StringType(),True),
                        StructField("format",StringType(),True),
                        StructField("id",StringType(),True),
                        StructField("partnerDescription",StringType(),True),
                        StructField("partnerId",LongType(),True),
                        StructField("size",LongType(),True),
                        StructField("sizeInBytes",LongType(),True),
                        StructField("status",StringType(),True),
                        StructField("tags",StringType(),True),
                        StructField("title",StringType(),True),
                        StructField("updatedAt",StringType(),True),
                        StructField("version",LongType(),True)
                    ]), True
                ),True
               ),
    StructField("flavors",
                ArrayType(
                    StructType([
                        StructField("bitrate",LongType(),True),
                        StructField("containerFormat",StringType(),True),
                        StructField("createdAt",StringType(),True),
                        StructField("description",StringType(),True),
                        StructField("entryId",StringType(),True),
                        StructField("fileExt",StringType(),True),
                        StructField("flavorParamsId",LongType(),True),
                        StructField("frameRate",DoubleType(),True),
                        StructField("height",LongType(),True),
                        StructField("id",StringType(),True),
                        StructField("isDefault",BooleanType(),True),
                        StructField("isOriginal",BooleanType(),True),
                        StructField("isWeb",BooleanType(),True),
                        StructField("language",BooleanType(),True),
                        StructField("partnerId",LongType(),True),
                        StructField("size",LongType(),True),
                        StructField("sizeInBytes",LongType(),True),
                        StructField("status",StringType(),True),
                        StructField("tags",StringType(),True),
                        StructField("updatedAt",StringType(),True),
                        StructField("version",LongType(),True),
                        StructField("videoCodecId",StringType(),True),
                        StructField("width",LongType(),True)
                    ]),True
                ),True
               ),
    StructField("mediaEntry",
                StructType([
                    StructField("application",StringType(),True),
                    StructField("blockAutoTranscript",BooleanType(),True),
                    StructField("capabilities",StringType(),True),
                    StructField("categories",StringType(),True),
                    StructField("categoriesIds",StringType(),True),
                    StructField("conversionProfileId",LongType(),True),
                    StructField("conversionQuality",StringType(),True),
                    StructField("createdAt",StringType(),True),
                    StructField("creatorId",StringType(),True),
                    StructField("dataUrl",StringType(),True),
                    StructField("description",StringType(),True),
                    StructField("displayInSearch",StringType(),True),
                    StructField("downloadUrl",StringType(),True),
                    StructField("duration",LongType(),True),
                    StructField("entitledUsersEdit",StringType(),True),
                    StructField("entitledUsersPublish",StringType(),True),
                    StructField("entitledUsersView",StringType(),True),
                    StructField("flavorParamsIds",StringType(),True),
                    StructField("height",LongType(),True),
                    StructField("id",StringType(),True),
                    StructField("lastPlayedAt",StringType(),True),
                    StructField("licenseType",StringType(),True),
                    StructField("mediaType",StringType(),True),
                    StructField("moderationCount",LongType(),True),
                    StructField("moderationStatus",StringType(),True),
                    StructField("msDuration",LongType(),True),
                    StructField("name",StringType(),True),
                    StructField("operationAttributes",ArrayType(StringType(),True),True),
                    StructField("partnerId",LongType(),True),
                    StructField("partnerSortValue",LongType(),True),
                    StructField("plays",LongType(),True),
                    StructField("rank",DoubleType(),True),
                    StructField("replacementStatus",StringType(),True),
                    StructField("rootEntryId",StringType(),True),
                    StructField("searchText",StringType(),True),
                    StructField("sourceType",StringType(),True),
                    StructField("status",StringType(),True),
                    StructField("tags",StringType(),True),
                    StructField("thumbnailUrl",StringType(),True),
                    StructField("totalRank",LongType(),True),
                    StructField("type",StringType(),True),
                    StructField("updatedAt",StringType(),True),
                    StructField("userId",StringType(),True),
                    StructField("version",LongType(),True),
                    StructField("views",LongType(),True),
                    StructField("votes",LongType(),True),
                    StructField("width",LongType(),True)
                ]),True
                ),
    StructField("metadata",
            ArrayType(
                StructType([
                    StructField("createdAt",StringType(),True),
                    StructField("id",LongType(),True),
                    StructField("jsonData",
                                StructType([
                                    StructField("AddressStreet",StringType(),True),
                                    StructField("AssessedByDate",StringType(),True),
                                    StructField("AssessedByName",StringType(),True),
                                    StructField("AssetNumbers",StringType(),True),
                                    StructField("ChildWorkOrderNumbers",StringType(),True),
                                    StructField("Cleaned",StringType(),True),
                                    StructField("Condition",StringType(),True),
                                    StructField("Contractor",StringType(),True),
                                    StructField("DateOfCompletedInspection",StringType(),True),
                                    StructField("DirectionOfSurvey",StringType(),True),
                                    StructField("DiscardDate",StringType(),True),
                                    StructField("DownstreamMH",StringType(),True),
                                    StructField("FacilityNo",StringType(),True),
                                    StructField("Infiltration",StringType(),True),
                                    StructField("Location",StringType(),True),
                                    StructField("PackageName",StringType(),True),
                                    StructField("ParentWorkOrderNumber",StringType(),True),
                                    StructField("Product",StringType(),True),
                                    StructField("Serviceability",StringType(),True),
                                    StructField("Suburb",StringType(),True),
                                    StructField("SurveyedLength",StringType(),True),
                                    StructField("TaskCode",StringType(),True),
                                    StructField("TimeOfCompletedInspection",StringType(),True),
                                    StructField("UpstreamMH",StringType(),True),
                                    StructField("WorkOrderDescription",StringType(),True),
                                    StructField("PriorityJustification",StringType(),True),
                                    StructField("OperationalArea",StringType(),True)
                                ]), True),
                    StructField("metadataObjectType",StringType(),True),
                    StructField("metadataProfileId",LongType(),True),
                    StructField("metadataProfileVersion",LongType(),True),
                    StructField("objectId",StringType(),True),
                    StructField("partnerId",LongType(),True),
                    StructField("status",StringType(),True),
                    StructField("updatedAt",StringType(),True),
                    StructField("version",LongType(),True),
                    StructField("update",BooleanType(),True)
                ]), True), True)
])

# COMMAND ----------

from pyspark.sql import functions as psf

df_kaltura_flavors = (spark.read.schema(schema).json(filepath)
                     .withColumn("flavors", psf.explode(psf.col("flavors")))
                      .withColumn("createdAt", psf.to_timestamp(psf.col("flavors.createdAt"), format='yyyy-MM-dd HH:mm:ss'))
                      .withColumn("updatedAt", psf.to_timestamp(psf.col("flavors.updatedAt"), format='yyyy-MM-dd HH:mm:ss'))
                      .withColumn("language", psf.col("flavors.language").cast('long'))
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
                              "flavors.isDefault",
                              "language",
                              "processed_timestamp"
                             )
                         )
df_kaltura_flavors.createOrReplaceTempView("tmp_datalake_kaltura_flavors")

df_kaltura_attachments = (spark.read.schema(schema).json(filepath)
                          .withColumn("attachments", psf.col("attachments")[0])
                          .withColumn("createdAt", psf.to_timestamp(psf.col("attachments.createdAt"), format='yyyy-MM-dd HH:mm:ss'))
                          .withColumn("updatedAt", psf.to_timestamp(psf.col("attachments.updatedAt"), format='yyyy-MM-dd HH:mm:ss'))
                          .withColumn("id", 
                                   psf.when(psf.length(psf.col("attachments.id")) > 10,
                                            "Error - String Length Exceeded (10)"
                                           ).otherwise(psf.col("attachments.id"))
                                  )
                          .withColumn("filename", 
                                   psf.when(psf.length(psf.col("attachments.filename")) > 500,
                                            "Error - String Length Exceeded (500)"
                                           ).otherwise(psf.col("attachments.filename"))
                                  )
                          .withColumn("title", 
                                   psf.when(psf.length(psf.col("attachments.title")) > 500,
                                            "Error - String Length Exceeded (500)"
                                           ).otherwise(psf.col("attachments.title"))
                                  )
                          .withColumn("fileExt", 
                                   psf.when(psf.length(psf.col("attachments.fileExt")) > 10,
                                            "Error - String Length Exceeded (10)"
                                           ).otherwise(psf.col("attachments.fileExt"))
                                  )
                          .withColumn("processed_timestamp", psf.current_timestamp())
                          .where(psf.col("attachments").isNotNull())
                          .select("update", 
                                  "id",
                                  "attachments.entryId",
                                  "attachments.partnerId",
                                  "attachments.version",
                                  "attachments.size",
                                  "attachments.tags",
                                  "fileExt",
                                  "createdAt",
                                  "updatedAt",
                                  "attachments.description",
                                  "attachments.partnerDescription",
                                  "attachments.sizeInBytes",
                                  "filename",
                                  "attachments.title",
                                  "attachments.format",
                                  "attachments.status",
                                  "processed_timestamp"
                                 )
                         )
df_kaltura_attachments.createOrReplaceTempView("tmp_datalake_kaltura_attachments")


df_kaltura_metadata = spark.read.schema(schema).json(filepath).withColumn("metadata", psf.col("metadata")[0])
df_kaltura_metadata = (df_kaltura_metadata
                       .withColumn("AssessedByDate", psf.to_timestamp(psf.col("metadata.jsonData.AssessedByDate"), format='yyyy-MM-dd HH:mm:ss'))
                       .withColumn("AssessedByDate", 
                                   psf.when(psf.col("AssessedByDate") < psf.lit("1754-01-01T00:00:00.000+1100"), 
                                            psf.to_timestamp(psf.lit("1754-01-01 00:00:00.000"), format='yyyy-MM-dd HH:mm:ss.SSSS')).otherwise(psf.col("AssessedByDate")))
                       .withColumn("DiscardDate", psf.to_timestamp(psf.col("metadata.jsonData.DiscardDate"), format='yyyy-MM-dd HH:mm:ss'))
                      .withColumn("DiscardDate", 
                                   psf.when(psf.col("DiscardDate") < psf.lit("1754-01-01T00:00:00.000+1100"), 
                                            psf.to_timestamp(psf.lit("1754-01-01 00:00:00.000"), format='yyyy-MM-dd HH:mm:ss.SSSS')).otherwise(psf.col("DiscardDate"))
                                 )
                       .withColumn("DateOfCompletedInspectionString", psf.col("metadata.jsonData.DateOfCompletedInspection"))
                       .withColumn("DateOfCompletedInspectionString", 
                                   psf.when((((psf.length(psf.col("DateOfCompletedInspectionString")) != 10) |
                                             ~(psf.col("DateOfCompletedInspectionString").contains("/"))) &
                                             (psf.length(psf.col("DateOfCompletedInspectionString")) != 0)
                                            ), psf.lit("01/01/1753")).otherwise(psf.col("DateOfCompletedInspectionString"))
                                  )
                       .withColumn("TimeOfCompletedInspectionString", psf.col("metadata.jsonData.TimeOfCompletedInspection"))
                       .withColumn("createdAt", psf.to_timestamp(psf.col("metadata.createdAt"), format='yyyy-MM-dd HH:mm:ss'))
                       .withColumn("createdAt", 
                                   psf.when(psf.col("createdAt") < psf.lit("1754-01-01T00:00:00.000+1100"), 
                                            psf.to_timestamp(psf.lit("1754-01-01 00:00:00.000"), format='yyyy-MM-dd HH:mm:ss.SSSS')).otherwise(psf.col("createdAt")))
                       .withColumn("updatedAt", psf.to_timestamp(psf.col("metadata.updatedAt"), format='yyyy-MM-dd HH:mm:ss'))
                       .withColumn("updatedAt", 
                                   psf.when(psf.col("updatedAt") < psf.lit("1754-01-01T00:00:00.000+1100"), 
                                            psf.to_timestamp(psf.lit("1754-01-01 00:00:00.000"), format='yyyy-MM-dd HH:mm:ss.SSSS')).otherwise(psf.col("updatedAt")))
                       .withColumn("SurveyedLength", psf.col("metadata.jsonData.SurveyedLength").cast('decimal'))
                       .withColumn("ParentWorkOrderNumber", 
                                   psf.when(psf.length(psf.col("metadata.jsonData.ParentWorkOrderNumber")) > 50,
                                            "Error - String Length Exceeded (50)"
                                           ).otherwise(psf.col("metadata.jsonData.ParentWorkOrderNumber"))
                                  )
                       .withColumn("ChildWorkOrderNumbers", 
                                   psf.when(psf.length(psf.col("metadata.jsonData.ChildWorkOrderNumbers")) > 500,
                                            "Error - String Length Exceeded (500)"
                                           ).otherwise(psf.col("metadata.jsonData.ChildWorkOrderNumbers"))
                                  )
                       .withColumn("AssetNumbers", 
                                   psf.when(psf.length(psf.col("metadata.jsonData.AssetNumbers")) > 200,
                                            "Error - String Length Exceeded (200)"
                                           ).otherwise(psf.col("metadata.jsonData.AssetNumbers"))
                                  )
                       .withColumn("TaskCode", 
                                   psf.when(psf.length(psf.col("metadata.jsonData.TaskCode")) > 200,
                                            "Error - String Length Exceeded (200)"
                                           ).otherwise(psf.col("metadata.jsonData.TaskCode"))
                                  )
                       .withColumn("Suburb", 
                                   psf.when(psf.length(psf.col("metadata.jsonData.Suburb")) > 200,
                                            "Error - String Length Exceeded (200)"
                                           ).otherwise(psf.col("metadata.jsonData.Suburb"))
                                  )
                       .withColumn("AddressStreet", 
                                   psf.when(psf.length(psf.col("metadata.jsonData.AddressStreet")) > 200,
                                            "Error - String Length Exceeded (200)"
                                           ).otherwise(psf.col("metadata.jsonData.AddressStreet"))
                                  )
                       .withColumn("Product", 
                                   psf.when(psf.length(psf.col("metadata.jsonData.Product")) > 200,
                                            "Error - String Length Exceeded (200)"
                                           ).otherwise(psf.col("metadata.jsonData.Product"))
                                  )
                       .withColumn("Contractor", 
                                   psf.when(psf.length(psf.col("metadata.jsonData.Contractor")) > 200,
                                            "Error - String Length Exceeded (200)"
                                           ).otherwise(psf.col("metadata.jsonData.Contractor"))
                                  )
                       .withColumn("UpstreamMH", 
                                   psf.when(psf.length(psf.col("metadata.jsonData.UpstreamMH")) > 200,
                                            "Error - String Length Exceeded (200)"
                                           ).otherwise(psf.col("metadata.jsonData.UpstreamMH"))
                                  )
                       .withColumn("DownstreamMH", 
                                   psf.when(psf.length(psf.col("metadata.jsonData.DownstreamMH")) > 200,
                                            "Error - String Length Exceeded (200)"
                                           ).otherwise(psf.col("metadata.jsonData.DownstreamMH"))
                                  )
                       .withColumn("DirectionOfSurvey", 
                                   psf.when(psf.length(psf.col("metadata.jsonData.DirectionOfSurvey")) > 200,
                                            "Error - String Length Exceeded (200)"
                                           ).otherwise(psf.col("metadata.jsonData.DirectionOfSurvey"))
                                  )
                       .withColumn("PackageName", 
                                   psf.when(psf.length(psf.col("metadata.jsonData.PackageName")) > 200,
                                            "Error - String Length Exceeded (200)"
                                           ).otherwise(psf.col("metadata.jsonData.PackageName"))
                                  )
                       .withColumn("Cleaned", 
                                   psf.when(psf.length(psf.col("metadata.jsonData.Cleaned")) > 200,
                                            "Error - String Length Exceeded (200)"
                                           ).otherwise(psf.col("metadata.jsonData.Cleaned"))
                                  )
                       .withColumn("Condition", 
                                   psf.when(psf.length(psf.col("metadata.jsonData.Condition")) > 10,
                                            "Error - String Length Exceeded (10)"
                                           ).otherwise(psf.col("metadata.jsonData.Condition"))
                                  )
                       .withColumn("Serviceability", 
                                   psf.when(psf.length(psf.col("metadata.jsonData.Serviceability")) > 10,
                                            "Error - String Length Exceeded (10)"
                                           ).otherwise(psf.col("metadata.jsonData.Serviceability"))
                                  )
                       .withColumn("Infiltration", 
                                   psf.when(psf.length(psf.col("metadata.jsonData.Infiltration")) > 10,
                                            "Error - String Length Exceeded (10)"
                                           ).otherwise(psf.col("metadata.jsonData.Infiltration"))
                                  )
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
                               "ParentWorkOrderNumber",
                               "ChildWorkOrderNumbers",
                               "metadata.jsonData.WorkOrderDescription",
                               "AssetNumbers",
                               "TaskCode",
                               "Suburb",
                               "AddressStreet",
                               "Product",
                               "Contractor",
                               "UpstreamMH",
                               "DownstreamMH",
                               "DirectionOfSurvey",
                               "DateOfCompletedInspectionString",
                               "TimeOfCompletedInspectionString",
                               "PackageName",
                               "Cleaned",
                               "SurveyedLength",
                               "DiscardDate",
                               "Condition",
                               "Serviceability",
                               "Infiltration",
                               "createdAt",
                               "updatedAt",
                               "processed_timestamp")
                         )
df_kaltura_metadata.createOrReplaceTempView("tmp_datalake_kaltura_metadata")

df_kaltura_media_entry = (spark.read.schema(schema).json(filepath)
                          .withColumn("processed_timestamp", psf.current_timestamp())
                          .where(psf.col("mediaEntry").isNotNull())
                          .withColumn("sourceFileName", psf.lit(filepath))
                          .withColumn("createdAt", psf.to_timestamp(psf.col("mediaEntry.createdAt"), format='yyyy-MM-dd HH:mm:ss'))
                          .withColumn("updatedAt", psf.to_timestamp(psf.col("mediaEntry.updatedAt"), format='yyyy-MM-dd HH:mm:ss'))
                          .withColumn("lastPlayedAt", psf.to_timestamp(psf.col("mediaEntry.lastPlayedAt"), format='yyyy-MM-dd HH:mm:ss'))
                          .withColumn("operationAttributes", psf.col("mediaEntry.operationAttributes").cast("string"))
                          .withColumn("id", 
                                   psf.when(psf.length(psf.col("mediaEntry.id")) > 10,
                                            "Error - String Length Exceeded (10)"
                                           ).otherwise(psf.col("mediaEntry.id"))
                                  )
                          .withColumn("name", 
                                   psf.when(psf.length(psf.col("mediaEntry.name")) > 500,
                                            "Error - String Length Exceeded (500)"
                                           ).otherwise(psf.col("mediaEntry.name"))
                                  )
                          .withColumn("creatorId", 
                                   psf.when(psf.length(psf.col("mediaEntry.creatorId")) > 50,
                                            "Error - String Length Exceeded (50)"
                                           ).otherwise(psf.col("mediaEntry.creatorId"))
                                  )
                          .withColumn("downloadUrl", 
                                   psf.when(psf.length(psf.col("mediaEntry.downloadUrl")) > 200,
                                            "Error - String Length Exceeded (200)"
                                           ).otherwise(psf.col("mediaEntry.downloadUrl"))
                                  )
                          .select("update", 
                                  "id",
                                  "name",
                                  "mediaEntry.description",
                                  "mediaEntry.partnerId",
                                  "mediaEntry.userId",
                                  "creatorId",
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
                                  "downloadUrl",
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
                                  "sourceFileName",
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
# MAGIC         PriorityJustification = src.PriorityJustification,
# MAGIC         OperationalArea = src.OperationalArea,
# MAGIC         AssetNumbers = src.AssetNumbers,
# MAGIC         TaskCode = src.TaskCode,
# MAGIC         Suburb = src.Suburb,
# MAGIC         AddressStreet = src.AddressStreet,
# MAGIC         Product = src.Product,
# MAGIC         Contractor = src.Contractor,
# MAGIC         UpstreamMH = src.UpstreamMH,
# MAGIC         DownstreamMH = src.DownstreamMH,
# MAGIC         DirectionOfSurvey = src.DirectionOfSurvey,
# MAGIC         DateOfCompletedInspectionString = src.DateOfCompletedInspectionString,
# MAGIC         TimeOfCompletedInspectionString = src.TimeOfCompletedInspectionString,
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
# MAGIC     PriorityJustification,
# MAGIC     OperationalArea,
# MAGIC     AssetNumbers,
# MAGIC     TaskCode,
# MAGIC     Suburb,
# MAGIC     AddressStreet,
# MAGIC     Product,
# MAGIC     Contractor,
# MAGIC     UpstreamMH,
# MAGIC     DownstreamMH,
# MAGIC     DirectionOfSurvey,
# MAGIC     DateOfCompletedInspectionString,
# MAGIC     TimeOfCompletedInspectionString,
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
# MAGIC     src.PriorityJustification,
# MAGIC     src.OperationalArea,
# MAGIC     src.AssetNumbers,
# MAGIC     src.TaskCode,
# MAGIC     src.Suburb,
# MAGIC     src.AddressStreet,
# MAGIC     src.Product,
# MAGIC     src.Contractor,
# MAGIC     src.UpstreamMH,
# MAGIC     src.DownstreamMH,
# MAGIC     src.DirectionOfSurvey,
# MAGIC     src.DateOfCompletedInspectionString,
# MAGIC     src.TimeOfCompletedInspectionString,
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

# MAGIC %md
# MAGIC ## 1.2 Get data from Maximo

# COMMAND ----------

df_kaltura_metadata = spark.sql(f"""
        SELECT *
        FROM cleansed.cctv_kaltura_metadata
        """
    )

df_kaltura_metadata = (
            df_kaltura_metadata
            .selectExpr(
                "id",
                "partnerId",
                "metadataProfileId",
                "metadataProfileVersion",
                "metadataObjectType",
                "objectId",
                "version",
                "status",
                "AssessedByName",
                "AssessedByDate",
                "ParentWorkOrderNumber",
                "Case when ParentWorkOrderNumber = 'n/a' then ' ' \
                when ParentWorkOrderNumber is null then ' '\
                when ParentWorkOrderNumber = ChildWorkOrderNumbers then ' '\
                else ParentWorkOrderNumber\
                end as joinParentWorkOrderNumber",
                "ChildWorkOrderNumbers",
                "WorkOrderDescription",
                "AssetNumbers",
                "TaskCode",
                "Suburb",
                "AddressStreet",
                "Product",
                "Contractor",
                "UpstreamMH",
                "DownstreamMH",
                "DirectionOfSurvey",
                "DateOfCompletedInspectionString",
                "TimeOfCompletedInspectionString",
                "PackageName",
                "Cleaned",
                "SurveyedLength",
                "DiscardDate",
                "Condition",
                "Serviceability",
                "Infiltration",
                "createdAt",
                "updatedAt",
                "processed_timestamp")
    )

df_maximo = spark.sql(f"""
        SELECT  
                workOrder, parentWo, asset, priorityJustification, description, description ||'|'|| workOrder || '|' || asset as WorkOrderDescription,   taskCode, 
                addressSuburb, addressStreetNumber, street, location
        FROM {get_env()}cleansed.maximo.workorder
        """
    )

df_maximo = (
    df_maximo
    .selectExpr(
        "workOrder as max_workOrder", 
        "parentWo as max_parentWo", 
        "asset as max_asset", 
        "priorityJustification as max_priorityJustification", 
        "description as max_description",
        "WorkOrderDescription as max_WorkOrderDescription",   
        "TaskCode as max_taskCode", 
        "addressSuburb as max_addressSuburb", 
        "addressStreetNumber as max_addressStreetNumber", 
        "street as max_street", 
        "location as max_location",
        "coalesce(parentWo, ' ') as joinParentWO"
    )
)
    
df_locoper = spark.sql(f"""
        SELECT  
                location, operationalArea
        FROM {get_env()}cleansed.maximo.locoper
        """
    )

df_maximo = (
    df_maximo
    .join(
        df_locoper,
        (
            df_maximo.max_location == df_locoper.location
        ),
        how="left"
    )
    .select(df_maximo['*'], df_locoper['operationalArea'])
)

df_enahanced_kaltura_metadata = (
    df_kaltura_metadata
    .join(
        df_maximo,
        (
            (df_kaltura_metadata.joinParentWorkOrderNumber == df_maximo.joinParentWO)
            & (df_kaltura_metadata.ChildWorkOrderNumbers == df_maximo.max_workOrder)
        ),
        how="left"
    )
    .select(df_kaltura_metadata['*'], df_maximo['*'])
    .drop("joinParentWorkOrderNumber")
)

df_enahanced_kaltura_metadata = (
    df_enahanced_kaltura_metadata
    .selectExpr(
        "id",
        "partnerId",
        "metadataProfileId",
        "metadataProfileVersion",
        "metadataObjectType",
        "objectId",
        "version",
        "status",
        "AssessedByName",
        "AssessedByDate",
        "ParentWorkOrderNumber",
        "ChildWorkOrderNumbers",
        "coalesce(max_WorkOrderDescription,WorkOrderDescription) as WorkOrderDescription",
        "(Case when CHARINDEX('CASE',upper(trim(max_description))) > 0 or CHARINDEX('CASE',upper(trim(WorkOrderDescription))) > 0 then 'CASE' \
            when upper(trim(max_TaskCode)) = 'SO2F' then 'REPT'\
            when upper(trim(max_TaskCode)) = 'SO2M' then 'CRIT'\
            when upper(trim(max_TaskCode)) = 'SO2B' then 'TRAV'\
            when upper(trim(max_TaskCode)) = 'SO2W' then 'WATW'\
            when upper(trim(max_TaskCode)) in ('SO2D','SP2K') then (Case \
                        when substring(upper(trim(max_priorityJustification)),1,4) in ('CRIT','CRCM','MHIP',\
                        'WATW','WACC','WAEP','WAER','INTS','ODOR','ODCC','MULT','MULC','OPER','OPCC','OPSC',\
                        'OPOH','REPT','RECC','SALT','SEEP','SECC','SUBX','SUCC','SUSC','SUBS','WETW','WECC') then substring(upper(trim(max_priorityJustification)),1,4)\
                        when substring(upper(trim(max_description)),1,4) in ('CRIT','CRCM','MHIP',\
                        'WATW','WACC','WAEP','WAER','INTS','ODOR','ODCC','MULT','MULC','OPER','OPCC','OPSC',\
                        'OPOH','REPT','RECC','SALT','SEEP','SECC','SUBX','SUCC','SUSC','SUBS','WETW','WECC') then substring(upper(trim(max_description)),1,4) else ' '\
                        end)\
            else ' '\
            end) as priorityJustification",
        "operationalArea",
        "(Case \
            when  charindex('SEWER RELINING',upper(max_description)) > 0 or charindex('POST LINING',upper(workOrderDescription)) > 0 then 'SO9D' \
            else  taskCode end)  as TaskCode",
        "coalesce(max_addressSuburb,Suburb) as Suburb",
        "coalesce(concat(trim(max_addressStreetNumber), ' ', trim(max_street) ),AddressStreet) as AddressStreet",
        "(coalesce(Case \
            when  Substring(upper(max_taskCode),1,1) = 'S' then 'WASTEWATER' \
            when  Substring(upper(max_taskCode),1,1) = 'W' then 'WATER' else  Product \
            end,Product)) as Product",
        "AssetNumbers",
        "Contractor",
        "UpstreamMH",
        "DownstreamMH",
        "DirectionOfSurvey",
        "DateOfCompletedInspectionString",
        "TimeOfCompletedInspectionString",
        "PackageName",
        "Cleaned",
        "SurveyedLength",
        "DiscardDate",
        "Condition",
        "Serviceability",
        "Infiltration",
        "createdAt",
        "updatedAt",
        "processed_timestamp"
    )
    )

df_enahanced_kaltura_metadata.createOrReplaceTempView("tmp_datalake_kaltura_metadata")

# COMMAND ----------


#Run this the first time after promoting the code
# spark.sql(f"""
#     ALTER TABLE {get_env()}cleansed.cctv.kaltura_metadata ADD COLUMN (PriorityJustification STRING AFTER WorkOrderDescription)
#     """)


# COMMAND ----------

#Run this the first time after promoting the code
# spark.sql(f"""
#     ALTER TABLE cleansed.cctv_kaltura_metadata ADD COLUMN (PriorityJustification STRING AFTER WorkOrderDescription)
#     """)

# COMMAND ----------

#Run this the first time after promoting the code
# spark.sql(f"""
# ALTER TABLE {get_env()}cleansed.cctv.kaltura_metadata ADD COLUMN (OperationalArea STRING AFTER PriorityJustification)
# """)

# COMMAND ----------

#Run this the first time after promoting the code
# spark.sql(f"""
# ALTER TABLE cleansed.cctv_kaltura_metadata ADD COLUMN (OperationalArea STRING AFTER PriorityJustification)
# """)

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
# MAGIC         PriorityJustification = src.PriorityJustification,
# MAGIC         OperationalArea = src.OperationalArea,
# MAGIC         AssetNumbers = src.AssetNumbers,
# MAGIC         TaskCode = src.TaskCode,
# MAGIC         Suburb = src.Suburb,
# MAGIC         AddressStreet = src.AddressStreet,
# MAGIC         Product = src.Product,
# MAGIC         Contractor = src.Contractor,
# MAGIC         UpstreamMH = src.UpstreamMH,
# MAGIC         DownstreamMH = src.DownstreamMH,
# MAGIC         DirectionOfSurvey = src.DirectionOfSurvey,
# MAGIC         DateOfCompletedInspectionString = src.DateOfCompletedInspectionString,
# MAGIC         TimeOfCompletedInspectionString = src.TimeOfCompletedInspectionString,
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
# MAGIC     PriorityJustification,
# MAGIC     OperationalArea,
# MAGIC     AssetNumbers,
# MAGIC     TaskCode,
# MAGIC     Suburb,
# MAGIC     AddressStreet,
# MAGIC     Product,
# MAGIC     Contractor,
# MAGIC     UpstreamMH,
# MAGIC     DownstreamMH,
# MAGIC     DirectionOfSurvey,
# MAGIC     DateOfCompletedInspectionString,
# MAGIC     TimeOfCompletedInspectionString,
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
# MAGIC     src.PriorityJustification,
# MAGIC     src.OperationalArea,
# MAGIC     src.AssetNumbers,
# MAGIC     src.TaskCode,
# MAGIC     src.Suburb,
# MAGIC     src.AddressStreet,
# MAGIC     src.Product,
# MAGIC     src.Contractor,
# MAGIC     src.UpstreamMH,
# MAGIC     src.DownstreamMH,
# MAGIC     src.DirectionOfSurvey,
# MAGIC     src.DateOfCompletedInspectionString,
# MAGIC     src.TimeOfCompletedInspectionString,
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
# MAGIC         sourceFileName = src.sourceFileName,
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
# MAGIC     sourceFileName,
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
# MAGIC     src.sourceFileName,
# MAGIC     src.processed_timestamp
# MAGIC   )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Write Kaltura Data to Azure SQL Staging

# COMMAND ----------

WriteTableToAzureSQL(df=df_kaltura_flavors, mode="overwrite", targetTable="tmp_datalake_kaltura_flavors")
WriteTableToAzureSQL(df=df_kaltura_attachments, mode="overwrite", targetTable="tmp_datalake_kaltura_attachments")
WriteTableToAzureSQL(df=df_kaltura_metadata, mode="overwrite", targetTable="tmp_datalake_kaltura_metadata")
WriteTableToAzureSQL(df=df_kaltura_media_entry.drop("sourceFileName"), mode="overwrite", targetTable="tmp_datalake_kaltura_media_entry")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Merge Kaltura data from tmp datalake view to dbo staging and then to CCTVPortal

# COMMAND ----------

ExecuteStatement("EXEC  [dbo].[sp_merge_datalakeview_to_dbo_kaltura]")

# COMMAND ----------

ExecuteStatement("EXEC  [dbo].[sp_merge_dbo_to_CCTVPortal_kaltura]")

# COMMAND ----------

file_name = dbutils.widgets.get("json_file_name")
dbutils.fs.mv(f"dbfs:{filepath}", f"dbfs:/mnt/blob-kaltura/ARCHIVE/{file_name}")
