# Databricks notebook source
# MAGIC %sql
# MAGIC -- Create cctv_video_metadata table in raw layer
# MAGIC CREATE TABLE IF NOT EXISTS raw.cctv_video_metadata 
# MAGIC (video_id STRING,
# MAGIC  video_mount_point STRING,
# MAGIC  video_blob_storage STRING,
# MAGIC  fps INT,
# MAGIC  total_frames INT,
# MAGIC  total_msecs INT,
# MAGIC  _DLRawZoneTimeStamp TIMESTAMP 
# MAGIC )
# MAGIC USING DELTA 
# MAGIC LOCATION 'dbfs:/mnt/datalake-raw/sewercctv/cctv_video_metadata';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create cctv_video_metadata table in raw layer
# MAGIC CREATE TABLE IF NOT EXISTS stage.cctv_video_metadata 
# MAGIC (video_id STRING,
# MAGIC  video_mount_point STRING,
# MAGIC  video_blob_storage STRING,
# MAGIC  fps INT,
# MAGIC  total_frames INT,
# MAGIC  total_msecs INT,
# MAGIC  _DLRawZoneTimeStamp TIMESTAMP 
# MAGIC )
# MAGIC USING DELTA 
# MAGIC LOCATION 'dbfs:/mnt/datalake-stage/stage/cctv_video_metadata'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create cctv_video_frames table in raw layer
# MAGIC CREATE TABLE IF NOT EXISTS raw.cctv_video_frames
# MAGIC (video_id STRING,
# MAGIC  timestamp INT,
# MAGIC  image STRUCT<origin STRING, height: INT, width:INT, nChannels:INT, mode:INT, data:binary>,
# MAGIC  image_url STRING,
# MAGIC  _DLRawZoneTimeStamp TIMESTAMP
# MAGIC )
# MAGIC PARTITIONED BY (video_id)
# MAGIC LOCATION 'dbfs:/mnt/datalake-raw/sewercctv/cctv_video_frames'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create cctv_video_frames table in raw layer
# MAGIC CREATE TABLE IF NOT EXISTS stage.cctv_video_frames
# MAGIC (video_id STRING,
# MAGIC  timestamp INT,
# MAGIC  image STRUCT<origin STRING, height: INT, width:INT, nChannels:INT, mode:INT, data:binary>,
# MAGIC  image_url STRING,
# MAGIC  _DLRawZoneTimeStamp TIMESTAMP
# MAGIC )
# MAGIC PARTITIONED BY (video_id)
# MAGIC LOCATION 'dbfs:/mnt/datalake-stage/stage/cctv_video_frames'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create cctv_ocr_extract table in raw layer
# MAGIC CREATE TABLE IF NOT EXISTS raw.cctv_ocr_extract
# MAGIC (video_id STRING,
# MAGIC  timestamp INT,
# MAGIC  image STRUCT<origin STRING, height: INT, width:INT, nChannels:INT, mode:INT, data:binary>,
# MAGIC  image_url STRING,
# MAGIC  RecognizeText_bdc240b316bc_error STRUCT<response string,
# MAGIC                                          status STRUCT<
# MAGIC                                            protocolVersion STRUCT<
# MAGIC                                              protocol STRING,
# MAGIC                                              major INT,
# MAGIC                                              minor INT
# MAGIC                                              >,
# MAGIC                                            statusCode int,
# MAGIC                                            reasonPhrase string
# MAGIC                                            >
# MAGIC                                          >,
# MAGIC ocr STRUCT<status STRING,
# MAGIC             recognitionResult STRUCT<
# MAGIC               lines ARRAY<
# MAGIC                 STRUCT<
# MAGIC                   boundingBox ARRAY<
# MAGIC                     INT
# MAGIC                   >,
# MAGIC                   text STRING,
# MAGIC                   words ARRAY<
# MAGIC                     STRUCT<
# MAGIC                       boundingBox ARRAY<
# MAGIC                         INT
# MAGIC                       >,
# MAGIC                       text STRING
# MAGIC                     >
# MAGIC                   >
# MAGIC                 >
# MAGIC               >
# MAGIC             >
# MAGIC            >,
# MAGIC _DLRawZoneTimeStamp TIMESTAMP
# MAGIC )
# MAGIC PARTITIONED BY (video_id)
# MAGIC LOCATION 'dbfs:/mnt/datalake-raw/sewercctv/cctv_ocr_extract'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create cctv_ocr_extract table in raw layer
# MAGIC CREATE TABLE IF NOT EXISTS stage.cctv_ocr_extract
# MAGIC (video_id STRING,
# MAGIC  timestamp INT,
# MAGIC  image STRUCT<origin STRING, height: INT, width:INT, nChannels:INT, mode:INT, data:binary>,
# MAGIC  image_url STRING,
# MAGIC  RecognizeText_bdc240b316bc_error STRUCT<response string,
# MAGIC                                          status STRUCT<
# MAGIC                                            protocolVersion STRUCT<
# MAGIC                                              protocol STRING,
# MAGIC                                              major INT,
# MAGIC                                              minor INT
# MAGIC                                              >,
# MAGIC                                            statusCode int,
# MAGIC                                            reasonPhrase string
# MAGIC                                            >
# MAGIC                                          >,
# MAGIC ocr STRUCT<status STRING,
# MAGIC             recognitionResult STRUCT<
# MAGIC               lines ARRAY<
# MAGIC                 STRUCT<
# MAGIC                   boundingBox ARRAY<
# MAGIC                     INT
# MAGIC                   >,
# MAGIC                   text STRING,
# MAGIC                   words ARRAY<
# MAGIC                     STRUCT<
# MAGIC                       boundingBox ARRAY<
# MAGIC                         INT
# MAGIC                       >,
# MAGIC                       text STRING
# MAGIC                     >
# MAGIC                   >
# MAGIC                 >
# MAGIC               >
# MAGIC             >
# MAGIC            >,
# MAGIC _DLRawZoneTimeStamp TIMESTAMP
# MAGIC )
# MAGIC PARTITIONED BY (video_id)
# MAGIC LOCATION 'dbfs:/mnt/datalake-stage/stage/cctv_ocr_extract'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create cctv_ocr_extract table in raw layer
# MAGIC CREATE TABLE IF NOT EXISTS cleansed.cctv_ocr_extract 
# MAGIC (video_id STRING,
# MAGIC  timestamp INT,
# MAGIC  distance_m STRING,
# MAGIC  contractor_annotation STRING,
# MAGIC  _DLCleansedZoneTimeStamp TIMESTAMP 
# MAGIC )
# MAGIC PARTITIONED BY (video_id)
# MAGIC LOCATION 'dbfs:/mnt/datalake-cleansed/sewercctv/cctv_ocr_extract'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create cctv_ocr_extract table in cleansed layer
# MAGIC CREATE TABLE IF NOT EXISTS stage.cctv_ocr_extract_cleansed
# MAGIC (video_id STRING,
# MAGIC  timestamp INT,
# MAGIC  distance_m STRING,
# MAGIC  contractor_annotation STRING,
# MAGIC  _DLCleansedZoneTimeStamp TIMESTAMP 
# MAGIC )
# MAGIC PARTITIONED BY (video_id)
# MAGIC LOCATION 'dbfs:/mnt/datalake-stage/stage/cctv_ocr_extract_cleansed'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create cctv_contractor_annotations table in raw layer
# MAGIC CREATE TABLE IF NOT EXISTS cleansed.cctv_contractor_annotations 
# MAGIC (video_id STRING,
# MAGIC  contractor_annotation STRING,
# MAGIC  start_timestamp STRING,
# MAGIC  end_timestamp STRING,
# MAGIC  start_distance_m FLOAT,
# MAGIC  end_distance_m FLOAT,
# MAGIC  _DLCleansedZoneTimeStamp TIMESTAMP 
# MAGIC )
# MAGIC PARTITIONED BY (video_id)
# MAGIC LOCATION 'dbfs:/mnt/datalake-cleansed/sewercctv/cctv_contractor_annotations'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create cctv_contractor_annotations table in raw layer
# MAGIC CREATE TABLE IF NOT EXISTS stage.cctv_contractor_annotations 
# MAGIC (video_id STRING,
# MAGIC  contractor_annotation STRING,
# MAGIC  start_timestamp STRING,
# MAGIC  end_timestamp STRING,
# MAGIC  start_distance_m FLOAT,
# MAGIC  end_distance_m FLOAT,
# MAGIC  _DLCleansedZoneTimeStamp TIMESTAMP 
# MAGIC )
# MAGIC PARTITIONED BY (video_id)
# MAGIC LOCATION 'dbfs:/mnt/datalake-stage/stage/cctv_contractor_annotations'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create cctv_ai_image_classifications table in raw layer
# MAGIC CREATE TABLE IF NOT EXISTS raw.cctv_ai_image_classifications
# MAGIC (video_id STRING,
# MAGIC  timestamp INT,
# MAGIC  defect STRING,
# MAGIC  confidence FLOAT,
# MAGIC  score FLOAT,
# MAGIC  _DLRawZoneTimeStamp TIMESTAMP 
# MAGIC )
# MAGIC PARTITIONED BY (video_id)
# MAGIC LOCATION 'dbfs:/mnt/datalake-raw/sewercctv/cctv_ai_image_classifications'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create cctv_ai_image_classifications table in raw layer
# MAGIC CREATE TABLE IF NOT EXISTS stage.cctv_ai_image_classifications
# MAGIC (video_id STRING,
# MAGIC  timestamp INT,
# MAGIC  defect STRING,
# MAGIC  confidence FLOAT,
# MAGIC  score FLOAT,
# MAGIC  _DLRawZoneTimeStamp TIMESTAMP 
# MAGIC )
# MAGIC PARTITIONED BY (video_id)
# MAGIC LOCATION 'dbfs:/mnt/datalake-stage/stage/cctv_ai_image_classifications'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create cctv_ai_image_classifications table in raw layer
# MAGIC CREATE TABLE IF NOT EXISTS cleansed.cctv_group_ai_identified_defects
# MAGIC (video_id STRING,
# MAGIC  defect STRING,
# MAGIC  avg_probability DOUBLE,
# MAGIC  score FLOAT,
# MAGIC  start_timestamp STRING,
# MAGIC  end_timestamp STRING,
# MAGIC  start_distance_m FLOAT,
# MAGIC  end_distance_m FLOAT,
# MAGIC  _DLCleansedZoneTimeStamp TIMESTAMP
# MAGIC )
# MAGIC PARTITIONED BY (video_id)
# MAGIC LOCATION 'dbfs:/mnt/datalake-cleansed/sewercctv/cctv_group_ai_identified_defects'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create cctv_ai_image_classifications table in raw layer
# MAGIC CREATE TABLE IF NOT EXISTS stage.cctv_group_ai_identified_defects
# MAGIC (video_id STRING,
# MAGIC  defect STRING,
# MAGIC  avg_probability DOUBLE,
# MAGIC  score FLOAT,
# MAGIC  start_timestamp STRING,
# MAGIC  end_timestamp STRING,
# MAGIC  start_distance_m FLOAT,
# MAGIC  end_distance_m FLOAT,
# MAGIC  _DLCleansedZoneTimeStamp TIMESTAMP
# MAGIC )
# MAGIC PARTITIONED BY (video_id)
# MAGIC LOCATION 'dbfs:/mnt/datalake-stage/stage/cctv_group_ai_identified_defects'
