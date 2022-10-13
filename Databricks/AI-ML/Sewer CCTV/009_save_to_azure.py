# Databricks notebook source
# MAGIC %run ./jdbc-common

# COMMAND ----------

dbutils.widgets.text("video_id","0_04aw2g24")
_video_id = dbutils.widgets.get("video_id")

#set data frames from data lake, drop _DLCleansedZoneTimeStamp and remove any duplicates
df_ai_identified_defects = spark.table("cleansed.cctv_group_ai_identified_defects").where(f"video_id = '{_video_id}'")
df_ai_identified_defects = df_ai_identified_defects.drop("_DLCleansedZoneTimeStamp")
df_ai_identified_defects = df_ai_identified_defects.dropDuplicates()

df_contractor_annotations = spark.table("cleansed.cctv_contractor_annotations").where(f"video_id = '{_video_id}'")
df_contractor_annotations = df_contractor_annotations.drop("_DLCleansedZoneTimeStamp")
df_contractor_annotations = df_contractor_annotations.dropDuplicates()

# COMMAND ----------

#delete just to be safe if re-inserting
ExecuteStatement(f"delete from dbo.cctv_group_ai_identified_defects where video_id = '{_video_id}'")
ExecuteStatement(f"delete from dbo.cctv_contractor_annotations where video_id = '{_video_id}'")

#write data frames to corresponding tables
WriteTable(df_ai_identified_defects, "dbo.cctv_group_ai_identified_defects", "append")
WriteTable(df_contractor_annotations, "dbo.cctv_contractor_annotations", "append")
