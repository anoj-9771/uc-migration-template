# Databricks notebook source
# MAGIC %run ./000_Includes

# COMMAND ----------

#default Widget Parameter
#define notebook widget to accept video_id parameter
dbutils.widgets.text(name="video_id", defaultValue="0_oiif5iqr", label="video_id")

_VIDEO_ID = dbutils.widgets.get("video_id").replace(".mp4",'')

# COMMAND ----------

mount_location='/dbfs/mnt/blob-sewercctvvideos/Inbound'
blob_location='blob-sewercctvvideos/Inbound'

# COMMAND ----------

from pyspark.sql import functions as psf
from pyspark.sql import types as t
from pyspark.sql import functions as F

from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import concat, col, lit, substring, to_utc_timestamp, from_utc_timestamp, datediff, countDistinct, count, greatest
from pyspark.sql import functions as F

#Schema definition for video metadata dataframe
video_metadata_schema = t.StructType([
  t.StructField('video_id', t.StringType()),
  t.StructField('video_mount_point', t.StringType()),
  t.StructField('video_blob_storage', t.StringType()),
  t.StructField('fps', t.IntegerType()),
  t.StructField('total_frames', t.IntegerType()),
  t.StructField('total_msecs', t.IntegerType())
])

#define  mount point location to retrieve video from blob storage
video_mount_point = f"{mount_location}/{_VIDEO_ID}.mp4"

#video_mount_point
#define blob storage location for video
video_blob_storage = f"{ADS_SUBSCRIPTION}/{blob_location}/{_VIDEO_ID}.mp4"
#video_blob_storage
#extract video metadata

data = getVideoMetadata(_VIDEO_ID, video_mount_point, video_blob_storage)

#define dataframe with all the extracted metadata points
df_cctv_metadata = (spark.createDataFrame(data=data, schema=video_metadata_schema)
                    .withColumn("_DLRawZoneTimeStamp", psf.current_timestamp())  #timestamp for when the data has been processed
          )

#data inserted to the video metadata table
df_cctv_metadata.write.mode("append").insertInto('stage.cctv_video_metadata')
