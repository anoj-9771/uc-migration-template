# Databricks notebook source
# MAGIC %run ./000_Includes

# COMMAND ----------

#default Widget Parameter
#define notebook widget to accept video_id parameter
#define notebook widget to accept video_id parameter
dbutils.widgets.text(name="video_id", defaultValue="0_oiif5iqr", label="video_id")
_VIDEO_ID = dbutils.widgets.get("video_id").replace(".mp4",'')

ADS_KV_ACCOUNT_SCOPE = "ADS"
BLOB_FQN_KEY_NAME = "daf-blob-fqn"
_BLOB_FQN = dbutils.secrets.get(scope=ADS_KV_ACCOUNT_SCOPE, key=BLOB_FQN_KEY_NAME)
_BLOB_SEWERCCTVIMAGES_MNT_PNT = f"/mnt/blob-sewercctvimages/{_VIDEO_ID}"
dbutils.fs.mkdirs(_BLOB_SEWERCCTVIMAGES_MNT_PNT)

# COMMAND ----------

from pyspark.sql import functions as psf
from pyspark.sql import types as t

#1. ----- Obtain cctv metadata for selected video_id -----
df_cctv_metadata = (spark.table("stage.cctv_video_metadata")
                    .where(psf.col("video_id") == _VIDEO_ID)
                    .select("video_id", "video_mount_point", "total_msecs")
                   )

applyReturnSchema = t.StructType([
    t.StructField('video_id', t.StringType()),
    t.StructField('timestamp', t.IntegerType()),
    t.StructField('path', t.StringType())
])

#configure batch processing for spark
# spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "50")

#2. ----- Build list of desired frames to be extracted then save each frame to png in blob storage -----
df_image_download_output = (df_cctv_metadata
                             .withColumn("timestamp", 
                                         psf.explode(getFrameTimestampsUDF(psf.col("total_msecs"), psf.lit(500)))
                                        ) #call getFrameTimestamps()
                             .withColumn("blob_image_mnt_pt", psf.lit(_BLOB_SEWERCCTVIMAGES_MNT_PNT))
                             .repartition(numPartitions=(sc.defaultParallelism*2))
                             .mapInPandas(getImageFilesUDF, schema=applyReturnSchema)
                            )

df_image_download_output.collect() #execute saving of images to blob storage

# COMMAND ----------

#3. ----- Define dataframe to load extracted images -----           
df_raw_images = (spark.read.format("image").option("dropInvalid", True).load(_BLOB_SEWERCCTVIMAGES_MNT_PNT)
                 .withColumn("video_id", 
                             psf.split(psf.split(psf.col("image.origin"), "/")[3], "-")[0]
                            ) #extract video id from image path name
                 .withColumn("timestamp", 
                             psf.split(psf.split(psf.split(psf.col("image.origin"), "/")[4], "-")[1], ".png")[0].cast('int')
                            ) #extract timestamp from image path name
                 .where(psf.col("video_id") == _VIDEO_ID) #filter on selected video
                 .withColumn("image_url", psf.concat(psf.lit("https://"), 
                                                     psf.lit(_BLOB_FQN),
                                                     psf.lit("/"),
                                                     psf.split(psf.col("image.origin"), "-", 2)[1]
                                                    ) #create https url to blob storage location of the image - to be passed to cog services api downstream
                            )
                 .select("video_id", "timestamp", "image", "image_url")
                 .withColumn("_DLRawZoneTimeStamp", psf.current_timestamp())  #timestamp for when the data has been processed
                 .orderBy("timestamp")
                )

#4. ----- Data inserted to the cctv video frames table -----
df_raw_images.write.format("delta").insertInto('stage.cctv_video_frames')
