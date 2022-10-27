# Databricks notebook source
# MAGIC %run ./SQL_Connect_SewerCCTV

# COMMAND ----------

dbutils.widgets.text("video_id","0_oiif5iqr")
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

# COMMAND ----------

from pyspark.sql.functions import col

#dedupe and find video id as video can exist multiple times (unable to use cte or temp table in run query), take last updated record 
_video_id_int = RunQuery(f"Select id, updated_timestamp from cctvportal.video where source_id = '{_video_id}' and source = 'kaltura' and active = 1").sort(col("updated_timestamp").desc()).first()[0]

#find video annotation data, transform and store in df
df_video_annotation = RunQuery(
       f"""
       Select video_id = {_video_id_int}, contractor_annotation as annotation, convert(varchar, start_timestamp, 114) as start_time, convert(varchar, end_timestamp, 114) as end_time, start_distance_m as start_distance, 
       end_distance_m as end_distance 
       from dbo.cctv_contractor_annotations 
       where video_id = '{_video_id}' 
       """
)
#remove pre existing records for the source video id
ExecuteStatement(f"""
    delete va 
    from cctvportal.video_annotation va
    join cctvportal.video v on va.video_id = v.id and v.source = 'kaltura'
    where v.source_id = '{_video_id}'
    """
)
WriteTable(df_video_annotation, "cctvportal.video_annotation", "append")

#REPEAT PROCESS FOR AI IDENTIFIED DEFECT
df_ai_identified_defect = RunQuery(f"""
    select
        video_id = {_video_id_int}, defect, avg_probability, score, start_time = start_timestamp, end_time = end_timestamp, start_distance = start_distance_m, end_distance = end_distance_m, outcome_classification = NULL, created_timestamp = getutcdate()
    from dbo.cctv_group_ai_identified_defects 
    where video_id = '{_video_id}' 
""")
ExecuteStatement(f"""
    delete va 
    from cctvportal.video_ai_identified_defect va
    join cctvportal.video v on va.video_id = v.id and v.source = 'kaltura'
    where v.source_id = '{_video_id}'
    """
)
WriteTable(df_ai_identified_defect, "cctvportal.video_ai_identified_defect", "append")
