# Databricks notebook source
# MAGIC %run ./000_Includes

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

  #default Widget Parameter
#define notebook widget to accept video_id parameter
dbutils.widgets.text(name="video_id", defaultValue="0_oiif5iqr", label="video_id")
_VIDEO_ID = dbutils.widgets.get("video_id").replace(".mp4",'')

# COMMAND ----------

#define schema for AI model 
from pyspark.sql import functions as psf
from pyspark.sql import types as t
from pyspark.sql import Window as W

applyReturnSchema = t.StructType([
    t.StructField('video_id', t.StringType()),
    t.StructField('timestamp', t.IntegerType()),
    t.StructField('defect', t.StringType()),
    t.StructField('confidence', t.FloatType()),
    t.StructField('score', t.FloatType())
])

#define raw images dataframe for selected CCTV video
df_raw_images = (spark.table("raw.cctv_video_frames")
                 .where(psf.col("video_id") == _VIDEO_ID)
                 .orderBy("timestamp")
                 .drop("_DLRawZoneTimeStamp")
                 .distinct()
                )

#define window for calculating timestamp difference between defects
w_video = W.partitionBy("video_id", "defect").orderBy("timestamp")

df_image_classifications = (df_raw_images
                            .withColumn("path", psf.regexp_replace(psf.col("image.origin"), "dbfs:", "/dbfs"))
                            .drop("image")
                            .mapInPandas(predictImagesUDF, schema=applyReturnSchema)
                            .withColumn("_DLRawZoneTimeStamp", psf.current_timestamp())
                            .orderBy("timestamp")
                           )




df_image_classifications.write.mode("append").insertInto('raw.cctv_ai_image_classifications')
