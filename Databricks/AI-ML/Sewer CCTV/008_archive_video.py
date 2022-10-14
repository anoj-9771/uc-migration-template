# Databricks notebook source
#default Widget Parameter
#define notebook widget to accept video_id parameter
dbutils.widgets.text(name="video_id", defaultValue="0_oiif5iqr", label="video_id")

_VIDEO_ID = dbutils.widgets.get("video_id").replace(".mp4",'')

# COMMAND ----------

dbutils.fs.mv(f"dbfs:/mnt/blob-sewercctvvideos/Inbound/{_VIDEO_ID}.mp4", f"dbfs:/mnt/blob-sewercctvvideos/Archive/{_VIDEO_ID}.mp4", recurse=False)

# COMMAND ----------

#for testing: move video from archive back to inbound
# dbutils.fs.mv(f"dbfs:/mnt/blob-sewercctvvideos/Archive/{_VIDEO_ID}.mp4", f"dbfs:/mnt/blob-sewercctvvideos/Inbound/{_VIDEO_ID}.mp4", recurse=False)
