# Databricks notebook source
  #default Widget Parameter
#define notebook widget to accept video_id parameter
dbutils.widgets.text(name="video_id", defaultValue="0_oiif5iqr", label="video_id")
_VIDEO_ID = dbutils.widgets.get("video_id").replace(".mp4",'')

# COMMAND ----------

#define dataframe with image classifications for the selected CCTV video
from pyspark.sql import functions as psf
from pyspark.sql import types as t
from pyspark.sql import Window as W

w_video = W.partitionBy("video_id", "defect").orderBy("timestamp")


df_image_classifications = (spark.table("stage.cctv_ai_image_classifications")
                            .drop("_DLRawZoneTimeStamp")
                            .where(psf.col("video_id") == _VIDEO_ID)
                            .where(psf.col("confidence") >= 0.75)
                            .where(psf.col("defect") != "No Defect") #remove the 'No Defect' classification
                            .withColumn("timestamp_diff", 
                                         (psf.col("timestamp") - psf.lag(psf.col("timestamp"),1).over(w_video))
                                        )
                            .withColumn("hour", psf.floor(psf.col("timestamp")/1000/60/60)) #break timestamp to h,m,s,S
                            .withColumn("minute", psf.floor(psf.col("timestamp")/1000/60%60))
                            .withColumn("second", psf.floor(psf.col("timestamp")/1000%60))
                            .withColumn("millisecond", psf.col("timestamp")%1000)
                            .orderBy("timestamp")
                           ).alias("df_image_classifications")

#buffer in milliseconds to group near by images in the video
frame_buffer_ms = 5500
defect_series_duration_ms = 1000

#define window for calculating timestamp difference
w_video = W.partitionBy("video_id", "defect").orderBy("timestamp")

df_grouped_defects = (df_image_classifications
                      .withColumnRenamed("timestamp_diff", "previous_timestamp_diff")
                      .withColumn("next_timestamp_diff",
                                  (psf.lead(psf.col("timestamp"),1).over(w_video) - psf.col("timestamp")))
                      .withColumn("event", 
                                    psf.when((((psf.col("previous_timestamp_diff") > frame_buffer_ms) |
                                               (psf.col("previous_timestamp_diff").isNull())) &
                                              ((psf.col("next_timestamp_diff") > frame_buffer_ms) |
                                               (psf.col("next_timestamp_diff").isNull()))
                                             ),"start-end")
                                        .when(((psf.col("previous_timestamp_diff") > frame_buffer_ms) |
                                               (psf.col("previous_timestamp_diff").isNull())), "start")
                                        .when(((psf.col("next_timestamp_diff") > frame_buffer_ms) |
                                               (psf.col("next_timestamp_diff").isNull())), "end")
                                        .otherwise(None)
                                   )
                      .where(psf.col("event").isNotNull() & (psf.col("event") != "start-end"))
                      .withColumn("duration", psf.when(psf.col("event") == "start",
                                                       psf.lead(psf.col("timestamp"),1).over(w_video) - psf.col("timestamp")
                                                      )
                                 )
                      .withColumn("start_timestamp", 
                                  psf.when(psf.col("event") == "start", 
                                           psf.concat_ws(":", psf.format_string("%02d", psf.col("hour")),
                                                         psf.format_string("%02d", psf.col("minute")),
                                                         psf.format_string("%02d", psf.col("second"))
                                                        )
                                          )
                                 )
                      .withColumn("start_timestamp", 
                                  psf.when(psf.col("start_timestamp").isNotNull(), 
                                           psf.concat_ws(".", psf.col("start_timestamp"), 
                                                         psf.format_string("%03d", psf.col("millisecond"))
                                                        )
                                          )
                                 )
                      .withColumn("end_timestamp", 
                                  psf.when(psf.lead(psf.col("event"),1).over(w_video) == "end",
                                           psf.concat_ws(":",
                                                         psf.format_string("%02d",psf.lead(psf.col("hour"),1).over(w_video)),
                                                        psf.format_string("%02d",psf.lead(psf.col("minute"),1).over(w_video)),
                                                         psf.format_string("%02d",psf.lead(psf.col("second"),1).over(w_video))
                                                        )
                                          )
                                 )
                      .withColumn("end_timestamp",
                                  psf.when(psf.col("end_timestamp").isNotNull(),
                                           psf.concat_ws(".", psf.col("end_timestamp"),
                                                         psf.format_string("%03d",
                                                                           psf.lead(psf.col("millisecond"),1).over(w_video)
                                                                          )
                                                        )
                                            )
                                   )
                      .withColumn("end_ts", 
                                  psf.when(psf.lead(psf.col("event"),1).over(w_video) == "end",
                                           psf.lead(psf.col("timestamp"),1).over(w_video)
                                          )
                                 )
                      .where(psf.col("start_timestamp").isNotNull() & psf.col("end_timestamp").isNotNull())
                      .where(psf.col("duration") >= defect_series_duration_ms)
                      .withColumnRenamed("timestamp", "start_ts")
                      .select("video_id", "defect", "start_timestamp", "end_timestamp", 
                              "start_ts", "end_ts", "duration", "score")
                      .orderBy("start_ts")
                      .alias("df_grouped_defects")
                      .repartition(numPartitions=(sc.defaultParallelism*2))
                      .cache()
                     )

# COMMAND ----------

df_cctv_defect_series = (df_grouped_defects
                         .join(df_image_classifications,
                               on=((psf.col("df_image_classifications.video_id") == psf.col("df_grouped_defects.video_id")) &
                                   (psf.col("df_image_classifications.defect") == psf.col("df_grouped_defects.defect")) &
                                   (psf.col("df_image_classifications.timestamp") >= psf.col("df_grouped_defects.start_ts")) &
                                   (psf.col("df_image_classifications.timestamp") <= psf.col("df_grouped_defects.end_ts"))
                                  ),
                               how='left'
                              )
                         .groupBy("df_grouped_defects.video_id", "df_grouped_defects.defect",
                                  "df_grouped_defects.start_timestamp","df_grouped_defects.end_timestamp",
                                  "df_grouped_defects.duration", "df_grouped_defects.start_ts", "df_grouped_defects.score"
                                 )
                         .agg(psf.mean(psf.col("df_image_classifications.confidence")).alias("avg_probability"))
                         .orderBy("df_grouped_defects.start_ts")
                         .select("df_grouped_defects.video_id", "df_grouped_defects.defect", 
                                 "avg_probability", "df_grouped_defects.score", 
                                 "df_grouped_defects.start_timestamp", "df_grouped_defects.end_timestamp"
                                )
                        )

# COMMAND ----------

df_ai = df_cctv_defect_series.alias("df_defects_ai")
df_st_text = (spark.table("stage.cctv_ocr_extract_cleansed")
              .where(psf.col("video_id") == _VIDEO_ID)
              .withColumn("hour", psf.floor(psf.col("timestamp")/1000/60/60))
              .withColumn("minute", psf.floor(psf.col("timestamp")/1000/60%60))
              .withColumn("second", psf.floor(psf.col("timestamp")/1000%60))
              .withColumn("millisecond", psf.col("timestamp")%1000)
              .withColumn("timestamp", psf.concat_ws(":", psf.format_string("%02d", psf.col("hour")),
                                                     psf.format_string("%02d", psf.col("minute")),
                                                     psf.format_string("%02d", psf.col("second"))
                                                    )
                         )
              .withColumn("timestamp", 
                          psf.when(psf.col("timestamp").isNotNull(), 
                                   psf.concat_ws(".", 
                                                 psf.col("timestamp"), 
                                                 psf.format_string("%03d", psf.col("millisecond"))
                                                )
                                  )
                         )
              .select("video_id", "timestamp", psf.col("distance_m").alias("start_distance_m"))
             ).alias("df_st_txt")

df_end_txt = df_st_text.withColumnRenamed("start_distance_m", "end_distance_m").alias("df_end_txt")
df_cctv_defect_series = (df_ai
                         .join(df_st_text, 
                               on=((psf.col("df_defects_ai.video_id")==psf.col("df_st_txt.video_id")) &
                                   (psf.col("df_defects_ai.start_timestamp")==psf.col("df_st_txt.timestamp"))
                                  ),
                               how='left'
                              )
                         .join(df_end_txt, 
                               on=((psf.col("df_defects_ai.video_id")==psf.col("df_end_txt.video_id")) &
                                   (psf.col("df_defects_ai.end_timestamp")==psf.col("df_end_txt.timestamp"))
                                  ), 
                               how='left'
                              )
                         .select("df_defects_ai.video_id", "defect", "avg_probability", 
                                 "score", "start_timestamp", "end_timestamp", 
                                 "df_st_txt.start_distance_m", "df_end_txt.end_distance_m"
                                )
                         #----- Add any additional columns required for the specific layer here ------
                         .withColumn("_DLCleansedZoneTimeStamp", psf.current_timestamp())
                         .repartition(numPartitions=(sc.defaultParallelism*2))
                        )

df_cctv_defect_series.write.insertInto("stage.cctv_group_ai_identified_defects")
