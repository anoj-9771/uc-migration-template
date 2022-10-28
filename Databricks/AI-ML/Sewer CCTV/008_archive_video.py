# Databricks notebook source
from pyspark.sql import functions as psf

#default Widget Parameter
#define notebook widget to accept video_id parameter
dbutils.widgets.text(name="video_id", defaultValue="0_oiif5iqr", label="video_id")

_VIDEO_ID = dbutils.widgets.get("video_id").replace(".mp4",'')

# COMMAND ----------

#data inserted to the video metadata table
df_cctv_metadata = (spark.table("stage.cctv_video_metadata")
                    .where(psf.col("video_id") == _VIDEO_ID)
                    .dropDuplicates()
                   )
df_cctv_metadata.write.mode("append").insertInto('raw.cctv_video_metadata')

#4. ----- Data inserted to the cctv video frames table -----
df_raw_images = (spark.table("stage.cctv_video_frames")
                    .where(psf.col("video_id") == _VIDEO_ID)
                    .dropDuplicates()
                   )
df_raw_images.write.format("delta").insertInto('raw.cctv_video_frames')

#data inserted to the raw ocr extract table
df_raw_ocr = (spark.table("stage.cctv_ocr_extract")
                    .where(psf.col("video_id") == _VIDEO_ID)
                    .dropDuplicates()
                   )
df_raw_ocr.write.mode("append").insertInto('raw.cctv_ocr_extract')

df_cleansed_ocr = (spark.table("stage.cctv_ocr_extract_cleansed")
                    .where(psf.col("video_id") == _VIDEO_ID)
                    .dropDuplicates()
                   )

df_cleansed_ocr.write.mode("append").insertInto('cleansed.cctv_ocr_extract')

df_contractor_annotations = (spark.table("stage.cctv_contractor_annotations")
                    .where(psf.col("video_id") == _VIDEO_ID)
                    .dropDuplicates()
                   )
df_contractor_annotations.write.insertInto("cleansed.cctv_contractor_annotations") #contractor identified defects

df_image_classifications = (spark.table("stage.cctv_ai_image_classifications")
                    .where(psf.col("video_id") == _VIDEO_ID)
                    .dropDuplicates()
                   )
df_image_classifications.write.mode("append").insertInto('raw.cctv_ai_image_classifications')

df_cctv_defect_series = (spark.table("stage.cctv_group_ai_identified_defects")
                    .where(psf.col("video_id") == _VIDEO_ID)
                    .dropDuplicates()
                   )
df_cctv_defect_series.write.insertInto("cleansed.cctv_group_ai_identified_defects")

# COMMAND ----------

dbutils.fs.mv(f"dbfs:/mnt/blob-sewercctvvideos/Inbound/{_VIDEO_ID}.mp4", f"dbfs:/mnt/blob-sewercctvvideos/Archive/{_VIDEO_ID}.mp4", recurse=False)

# COMMAND ----------

#for testing: move video from archive back to inbound
# dbutils.fs.mv(f"dbfs:/mnt/blob-sewercctvvideos/Archive/{_VIDEO_ID}.mp4", f"dbfs:/mnt/blob-sewercctvvideos/Inbound/{_VIDEO_ID}.mp4", recurse=False)
