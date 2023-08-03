# Databricks notebook source
# MAGIC %md #0. Global Variables

# COMMAND ----------

# MAGIC %md #1. Import Libraries

# COMMAND ----------

from pyspark.sql import functions as F
import os
from datetime import datetime

# COMMAND ----------

# MAGIC %md #2. Import Data

# COMMAND ----------

df_EDP_cctv_metadata = spark.table("hive_metastore.cleansed.cctv_kaltura_metadata").alias("m")
df_video_description = spark.table('hive_metastore.cleansed.cctv_kaltura_media_entry').alias("vd")
df_maximo_workorder = spark.table('hive_metastore.cleansed.maximo_workorder').alias("mx")
df_maximo_long_description = spark.table('hive_metastore.cleansed.maximo_longdescription').alias("mxl")
df_asset = (spark.table('hive_metastore.cleansed.maximo_asset').where(F.col('_RecordCurrent') ==1)).alias('asset')

# COMMAND ----------

# MAGIC %md #3. View for removed backlog

# COMMAND ----------

# MAGIC %md ##3.1. Remove dam inspections

# COMMAND ----------

df_assessed_1 = (df_EDP_cctv_metadata
                  .filter(F.col('AssessedByDate').isNull() == False)  
                  .where((F.col("Contractor").isNull()) |
                         (F.col("Contractor") == "Aqualift") |
                         (F.col("Contractor") == "AUS-ROV") |
                         (F.col("Contractor") == "SCD") |
                         (F.col("Contractor") == "") 
                        )
                  .select("objectId")
                  .distinct()
                 )

# COMMAND ----------

# MAGIC %md ##3.2. Remove any reviewed videos based on maximo long description and task codes

# COMMAND ----------

df_assessed_2 = (df_EDP_cctv_metadata
                .join(df_maximo_workorder.select("workOrder", F.col('mx.taskCode'),'workOrderId'), 
                                on=df_EDP_cctv_metadata.ParentWorkOrderNumber==df_maximo_workorder.workOrder, 
                                how='left'
                                )
                          .join(df_maximo_long_description.select("ldkey",'ldtext'), on=df_maximo_workorder.workOrderId==df_maximo_long_description.ldkey, how='left')
                          .withColumn("longDescriptionComment", 
                                                    #   F.when(F.lower(F.col("ldtext")).contains("review"),'Reviewed')
                                                    #    .when(F.lower(F.col("ldtext")).contains("asses"),'Reviewed')
                                                       F.when(F.lower(F.col('ldtext')).contains('https://media.sydneywater'),'Reviewed')
                                                       .when(F.lower(F.col('ldtext')).contains('s1'),'Reviewed')
                                                       .when(F.lower(F.col('ldtext')).contains('r1'),'Reviewed')
                                                       .when(F.lower(F.col('ldtext')).contains('i1'),'Reviewed')
                                                       .when(F.lower(F.col('ldtext')).contains('s2'),'Reviewed')
                                                       .when(F.lower(F.col('ldtext')).contains('r2'),'Reviewed')
                                                       .when(F.lower(F.col('ldtext')).contains('i2'),'Reviewed')
                                                       .when(F.lower(F.col('ldtext')).contains('s3'),'Reviewed')
                                                       .when(F.lower(F.col('ldtext')).contains('r3'),'Reviewed')
                                                       .when(F.lower(F.col('ldtext')).contains('i3'),'Reviewed')
                                                       .when(F.lower(F.col('ldtext')).contains('s4'),'Reviewed')
                                                       .when(F.lower(F.col('ldtext')).contains('r4'),'Reviewed')
                                                       .when(F.lower(F.col('ldtext')).contains('i4'),'Reviewed')
                                                       .when(F.lower(F.col('ldtext')).contains('s5'),'Reviewed')
                                                       .when(F.lower(F.col('ldtext')).contains('r5'),'Reviewed')
                                                       .when(F.lower(F.col('ldtext')).contains('i5'),'Reviewed')                                                     
                                                       .otherwise('Other'))
                         .where((F.col('mx.taskCode').startswith('W')) |
                         (F.col("longDescriptionComment") == "Reviewed") |
                         (F.col("mx.taskCode") == "SP2L")|
                         (F.col("mx.taskCode") == "SP2A")|
                         (F.col("mx.taskCode") == "SP2B")|
                         (F.col("mx.taskCode") == "SO9D")|
                         (F.col("mx.taskCode") == "SP2R")
                        )
                         .select("objectId")
                         .distinct()
                         )

# COMMAND ----------

# MAGIC %md ##3.3. Remove duplicating videos

# COMMAND ----------

df_duplicating_videos_stage_0 = (df_video_description
                                 .join(df_EDP_cctv_metadata, on=((df_video_description.id==df_EDP_cctv_metadata.objectId)), how='inner')
                                 .withColumn("workOrder", F.when(F.col('ChildWorkOrderNumbers').isNull(),F.col('ParentWorkOrderNumber'))
                                                          .otherwise(F.col('ChildWorkOrderNumbers')))
                                 ).alias('df_0')
df_duplicating_videos_stage_1 = (df_duplicating_videos_stage_0
                                .groupBy('name','msDuration','AssetNumbers','workOrder')
                                .agg(F.countDistinct(F.col("objectId")).alias("COUNT_SOURCE_ID"))
                                .where(F.col('COUNT_SOURCE_ID') != 1)
                                .drop(F.col('COUNT_SOURCE_ID'))).alias('df_1')
df_duplicating_videos_stage_2 = (df_duplicating_videos_stage_0
                                 .join(df_duplicating_videos_stage_1, on=((df_duplicating_videos_stage_0.name==df_duplicating_videos_stage_1.name) & (df_duplicating_videos_stage_0.msDuration==df_duplicating_videos_stage_1.msDuration) & (df_duplicating_videos_stage_0.AssetNumbers==df_duplicating_videos_stage_1.AssetNumbers) & (df_duplicating_videos_stage_0.workOrder==df_duplicating_videos_stage_1.workOrder)), how='inner')
                                 .groupBy('df_0.name','df_0.msDuration','df_0.AssetNumbers','df_0.workOrder')
                                 .agg(F.max(F.col("objectId")).alias("MAX_SOURCE_ID")))
df_duplicating_videos_stage_3 = (df_duplicating_videos_stage_0
                                .groupBy('name','msDuration','AssetNumbers','workOrder')
                                .agg(F.countDistinct(F.col("objectId")).alias("COUNT_SOURCE_ID"))
                                .where(F.col('COUNT_SOURCE_ID') == 1)
                                ).alias('df_3')                                
df_duplicating_videos_stage_4 =(df_duplicating_videos_stage_0
                                .join(df_duplicating_videos_stage_2, on=((df_duplicating_videos_stage_0.name==df_duplicating_videos_stage_2.name) & (df_duplicating_videos_stage_0.msDuration==df_duplicating_videos_stage_2.msDuration) & (df_duplicating_videos_stage_0.AssetNumbers==df_duplicating_videos_stage_2.AssetNumbers) & (df_duplicating_videos_stage_0.workOrder==df_duplicating_videos_stage_2.workOrder) & (df_duplicating_videos_stage_0.objectId==df_duplicating_videos_stage_2.MAX_SOURCE_ID)), how='inner')
                                .select(F.col('objectId'))
                                .distinct())
df_duplicating_videos_stage_5 = (df_duplicating_videos_stage_0
                                 .join(df_duplicating_videos_stage_3, on=((df_duplicating_videos_stage_0.name==df_duplicating_videos_stage_3.name) & (df_duplicating_videos_stage_0.msDuration==df_duplicating_videos_stage_3.msDuration) & (df_duplicating_videos_stage_0.AssetNumbers==df_duplicating_videos_stage_3.AssetNumbers) & (df_duplicating_videos_stage_0.workOrder==df_duplicating_videos_stage_3.workOrder)), how='inner')
                                 .select(F.col('objectId'))
                                .distinct()) 
df_duplicating_videos_stage_6 = (df_duplicating_videos_stage_5
                                 .unionAll(df_duplicating_videos_stage_4))   
df_duplicating_videos_stage_7 =(df_duplicating_videos_stage_0
                                .join(df_duplicating_videos_stage_6, on=((df_duplicating_videos_stage_0.objectId==df_duplicating_videos_stage_6.objectId)), how='leftanti')
                                .select(F.col('objectId'))
                                .distinct()) 

# COMMAND ----------

# MAGIC %md ##3.4. Remove pre videos

# COMMAND ----------

df_pre_post_both_ =(df_duplicating_videos_stage_0
                  .withColumnRenamed('objectId','objectId_'))

df_pre_post_both = (df_pre_post_both_                  
                  .join(df_duplicating_videos_stage_6, on=((df_pre_post_both_.objectId_==df_duplicating_videos_stage_6.objectId)), how='inner')
                  .distinct()
                  .withColumn("preOrPost", F.when(F.lower(F.col("name")).contains("post"),'Post')
                                                       .when(F.lower(F.col("description")).contains("pos"),'Post')
                                                       .when(F.lower(F.col("name")).contains("pre"),'Pre')
                                                       .when(F.lower(F.col("description")).contains("pre"),'Pre')
                                                       .otherwise('Other')))
df_pre_post_count =(df_pre_post_both
                   .groupBy('workOrder','AssetNumbers')
                   .agg(F.countDistinct(F.col("preOrPost")).alias("COUNT_preOrPost"))
                   .where(F.col('COUNT_preOrPost') != 1)) 
df_pre =(df_pre_post_count
        .join(df_pre_post_both, on=((df_pre_post_count.workOrder==df_pre_post_both.workOrder) & (df_pre_post_count.AssetNumbers==df_pre_post_both.AssetNumbers)), how='inner')
        .where(F.col('preOrPost') == 'Pre')
        .select('objectId')
        .distinct()
                   )

# COMMAND ----------

# MAGIC %md ##3.5. Remove videos where the maximo workorder has a follow on workorder

# COMMAND ----------

df_follow_on =(df_duplicating_videos_stage_0
                  .withColumnRenamed('objectId','objectId_')
                  .join(df_maximo_workorder, on=df_duplicating_videos_stage_0.workOrder==df_maximo_workorder.workOrder, how='inner')
                  .where(F.col('hasFollowUpWork') == 1)
                  .select('objectId_')
                  .withColumnRenamed('objectId_','objectId')
                  .distinct())

# COMMAND ----------

# MAGIC %md ##3.6 Remove videos with task code SO2M and pipe is not VC

# COMMAND ----------

df_SO2M = (df_EDP_cctv_metadata
                .join(df_maximo_workorder.select("workOrder", F.col('mx.taskCode'),'workOrderId'), 
                                on=df_EDP_cctv_metadata.ParentWorkOrderNumber==df_maximo_workorder.workOrder, 
                                how='left'
                                )
                .join(df_asset.select("asset",'description'), 
                                on=df_EDP_cctv_metadata.AssetNumbers==df_asset.asset, 
                                how='inner'
                                )
           .where(~F.col('description').contains('VC'))
           .where(F.col('mx.taskCode') == 'SO2M')
           .select("objectId")
           .distinct())

# COMMAND ----------

# MAGIC %md ##3.7. Remove deleted videos

# COMMAND ----------

df_deleted = (df_video_description
              .where(F.col('status') == 'DELETED')
              .select("id")
              .withColumnRenamed('id','objectId')
              .distinct())

# COMMAND ----------

# MAGIC %md ##3.8. Remove images

# COMMAND ----------

df_images = (df_video_description
             .where(F.col('mediaType') == 'IMAGE')
             .select("id")
             .withColumnRenamed('id','objectId')
             .distinct())

# COMMAND ----------

# MAGIC %md ##3.9. Combine all dataframe to one

# COMMAND ----------

df_all_remove = (df_assessed_1
                 .union(df_assessed_2)
                 .union(df_duplicating_videos_stage_7)
                 .union(df_pre)
                 .union(df_follow_on)
                 .union(df_SO2M)
                 .union(df_deleted)
                 .union(df_images)
                 .distinct()
                 )

# COMMAND ----------

# MAGIC %md ##3.10. Create or replace view using the combined dataframe

# COMMAND ----------

df_all_remove.write.mode("overwrite").saveAsTable("hive_metastore.cleansed.cctv_removed_backlog")

# COMMAND ----------

# MAGIC %md #4. View for duplicated videos

# COMMAND ----------

df_video_pbi = spark.table('hive_metastore.sewercctv.cctvportal_video')
df_duplicated_pbi_1 = (df_duplicating_videos_stage_2)
df_duplicated_pbi_2 =(df_duplicating_videos_stage_0
                     .join(df_duplicating_videos_stage_1, on=((df_duplicating_videos_stage_0.name==df_duplicating_videos_stage_1.name) & (df_duplicating_videos_stage_0.msDuration==df_duplicating_videos_stage_1.msDuration) & (df_duplicating_videos_stage_0.AssetNumbers==df_duplicating_videos_stage_1.AssetNumbers) & (df_duplicating_videos_stage_0.workOrder==df_duplicating_videos_stage_1.workOrder)), how='inner')
                     .select('df_1.*','df_0.objectId')
                    )
df_duplicated_pbi_3 = (df_duplicated_pbi_2
                       .join(df_duplicated_pbi_1, on=((df_duplicated_pbi_2.name==df_duplicated_pbi_1.name) & (df_duplicated_pbi_2.msDuration==df_duplicated_pbi_1.msDuration) & (df_duplicated_pbi_2.AssetNumbers==df_duplicated_pbi_1.AssetNumbers) & (df_duplicated_pbi_2.workOrder==df_duplicated_pbi_1.workOrder)), how='left')
                       .select('MAX_SOURCE_ID','objectId')
                       .distinct()
                       .withColumnRenamed('MAX_SOURCE_ID','video_id_kept')
                       .withColumnRenamed('objectId','video_id_duplicate_removed')
                       .withColumn('check', F.when(F.col('video_id_kept') == F.col('video_id_duplicate_removed'),1)
                                                       .otherwise(0))
                       .where(F.col('check') == 0))
df_duplicated_pbi_4 = (df_duplicated_pbi_3
                       .join(df_video_pbi, on=((df_duplicated_pbi_3.video_id_kept==df_video_pbi.SOURCE_ID)), how='inner')
                       .withColumnRenamed('video_id_kept','video_id_kept_')
                       .select('video_id_kept_','ID')
                       .distinct())      
df_duplicated_pbi_5 = (df_duplicated_pbi_3
                       .join(df_video_pbi, on=((df_duplicated_pbi_3.video_id_duplicate_removed==df_video_pbi.SOURCE_ID)), how='inner')
                       .withColumnRenamed('ID','ID_removed')
                       .withColumnRenamed('video_id_duplicate_removed','video_id_duplicate_removed_')
                       .select('video_id_duplicate_removed_','ID_removed')
                       .distinct())    
df_duplicated_pbi_6 = (df_duplicated_pbi_3
                       .join(df_duplicated_pbi_4, on=((df_duplicated_pbi_3.video_id_kept==df_duplicated_pbi_4.video_id_kept_)), how='inner')
                       .join(df_duplicated_pbi_5, on=((df_duplicated_pbi_3.video_id_duplicate_removed==df_duplicated_pbi_5.video_id_duplicate_removed_)), how='inner')
                       .select('video_id_kept','video_id_duplicate_removed')
                       .distinct()
                       )                                     

# COMMAND ----------

df_duplicated_pbi_6.write.mode("overwrite").saveAsTable("hive_metastore.cleansed.cctv_duplicate_videos")
