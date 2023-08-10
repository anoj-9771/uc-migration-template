# Databricks notebook source
# MAGIC %md hive_metastore.raw.cctv_ai_image_classifications

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE hive_metastore.raw.cctv_ai_image_classifications_bkp SELECT * FROM hive_metastore.raw.cctv_ai_image_classifications

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.raw.cctv_ai_image_classifications_bkp

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE hive_metastore.raw.cctv_ai_image_classifications
# MAGIC SELECT 
# MAGIC video_id,
# MAGIC timestamp,
# MAGIC defect,
# MAGIC confidence,
# MAGIC CASE WHEN defect = 'Joint Displaced Radially' THEN 0 ELSE score END AS score,
# MAGIC _DLRawZoneTimeStamp
# MAGIC FROM  hive_metastore.raw.cctv_ai_image_classifications

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.raw.cctv_ai_image_classifications

# COMMAND ----------

# MAGIC %md hive_metastore.stage.cctv_group_ai_identified_defects

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE hive_metastore.stage.cctv_group_ai_identified_defects_bkp SELECT * FROM hive_metastore.stage.cctv_group_ai_identified_defects

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.stage.cctv_group_ai_identified_defects_bkp

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE hive_metastore.stage.cctv_group_ai_identified_defects
# MAGIC SELECT
# MAGIC video_id,
# MAGIC defect,
# MAGIC avg_probability,
# MAGIC CASE WHEN defect = 'Joint Displaced Radially' THEN 0 ELSE score END AS score,
# MAGIC start_timestamp,
# MAGIC end_timestamp,
# MAGIC start_distance_m,
# MAGIC end_distance_m,
# MAGIC _DLCleansedZoneTimeStamp
# MAGIC FROM  hive_metastore.stage.cctv_group_ai_identified_defects

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.stage.cctv_group_ai_identified_defects

# COMMAND ----------

# MAGIC %md hive_metastore.cleansed.cctv_group_ai_identified_defects

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE hive_metastore.cleansed.cctv_group_ai_identified_defects_bkp SELECT * FROM hive_metastore.cleansed.cctv_group_ai_identified_defects

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.cleansed.cctv_group_ai_identified_defects_bkp

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE hive_metastore.cleansed.cctv_group_ai_identified_defects
# MAGIC SELECT
# MAGIC video_id,
# MAGIC defect,
# MAGIC avg_probability,
# MAGIC CASE WHEN defect = 'Joint Displaced Radially' THEN 0 ELSE score END AS score,
# MAGIC start_timestamp,
# MAGIC end_timestamp,
# MAGIC start_distance_m,
# MAGIC end_distance_m,
# MAGIC _DLCleansedZoneTimeStamp
# MAGIC FROM  hive_metastore.cleansed.cctv_group_ai_identified_defects

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.cleansed.cctv_group_ai_identified_defects

# COMMAND ----------

# MAGIC %md Make the changes in the Azure SQL Server table

# COMMAND ----------

# MAGIC %run ./SQL_Connect_SewerCCTV

# COMMAND ----------

# MAGIC %md dbo

# COMMAND ----------

df_dbo_scctv_group_ai_identified_defects_bkp = RunQuery(
       f"""
        SELECT * FROM  dbo.scctv_group_ai_identified_defects
       """
)
WriteTable(df_dbo_scctv_group_ai_identified_defects_bkp, "dbo.scctv_group_ai_identified_defects_bkp", "overwrite")

# COMMAND ----------

df_dbo_scctv_group_ai_identified_defects = RunQuery(
       f"""
        SELECT
        video_id,
        defect,
        avg_probability,
        CASE WHEN defect = 'Joint Displaced Radially' THEN 0 ELSE score END AS score,
        start_timestamp,
        end_timestamp,
        start_distance_m,
        end_distance_m
        FROM  dbo.scctv_group_ai_identified_defects_bkp
       """
)
WriteTable(df_dbo_scctv_group_ai_identified_defects, "dbo.scctv_group_ai_identified_defects", "overwrite")

# COMMAND ----------

# MAGIC %md cctv_portal

# COMMAND ----------

df_cctvportal_video_ai_identified_defect_bkp = RunQuery(
       f"""
        SELECT * FROM  cctvportal.video_ai_identified_defect
       """
)
WriteTable(df_cctvportal_video_ai_identified_defect_bkp, "cctvportal.video_ai_identified_defect_bkp", "overwrite")

# COMMAND ----------

df_cctvportal_video_ai_identified_defect = RunQuery(
       f"""
        SELECT
            ID,
            VIDEO_ID,
            DEFECT,
            AVG_PROBABILITY,
            CASE WHEN DEFECT = 'Joint Displaced Radially' THEN NULL ELSE SCORE END AS SCORE,
            START_TIME,
            END_TIME,
            START_DISTANCE,
            END_DISTANCE,
            OUTCOME_CLASSIFICATION,
            DEFECT_SUGGESTED,
            START_TIME_SUGGESTED,
            END_TIME_SUGGESTED,
            CREATED_TIMESTAMP,
            UPDATED_TIMESTAMP
        FROM  cctvportal.video_ai_identified_defect_bkp
       """
)
WriteTable(df_cctvportal_video_ai_identified_defect, "cctvportal.video_ai_identified_defect", "overwrite")
