# Databricks notebook source
# MAGIC %run ./000_Includes

# COMMAND ----------

  #default Widget Parameter
#define notebook widget to accept video_id parameter
dbutils.widgets.text(name="video_id", defaultValue="0_oiif5iqr", label="video_id")
_VIDEO_ID = dbutils.widgets.get("video_id").replace(".mp4",'')

# COMMAND ----------

#define dataframe with the raw images for the selected video
from pyspark.sql import functions as psf
from pyspark.sql import types as t
from pyspark.sql import Window as W


df_cleansed_ocr = (spark.table("stage.cctv_ocr_extract_cleansed")
                   .where(psf.col("video_id") == _VIDEO_ID)
                   .dropDuplicates(subset=['video_id', 'timestamp'])
                   .orderBy("timestamp")
                   .drop("_DLCleansedZoneTimeStamp")
                  )

w = W.partitionBy("video_id").orderBy("timestamp")
  
#find the start and end point of the contract annotations over the length of the video
df_contractor_annotations = (findStartEndofString(df=(df_cleansed_ocr
                                                      .where((psf.col("contractor_annotation") != "") &
                                                             (psf.col("distance_m").isNotNull()))
                                                     ),
                                                  string_col_name="contractor_annotation"
                                                 )
                             
                            )

# #convert timestamp into string with the format hh:mm:ss.SSS
df_contractor_annotations = (df_contractor_annotations
                             .withColumn("ts",
                                         psf.concat_ws(":", 
                                                       psf.format_string("%02d", psf.floor(psf.col("timestamp")/1000/60/60)),
                                                       psf.format_string("%02d", psf.floor(psf.col("timestamp")/1000/60%60)),
                                                       psf.format_string("%02d", psf.floor(psf.col("timestamp")/1000%60))
                                                      )
                                         )
                              .withColumn("ts", 
                                          psf.when(psf.col("timestamp").isNotNull(),
                                                   psf.concat_ws(".", 
                                                                 psf.col("ts"), 
                                                                 psf.format_string("%03d", psf.col("timestamp")%1000)
                                                                )
                                                  )
                                         )
                            )

# #grab the start/end values for the distance variable
df_contractor_annotations = (df_contractor_annotations
                             .withColumn("end_distance_m", 
                                         psf.when(((psf.col("event") == "end") & 
                                                   (psf.lag(psf.col("event"),1).over(w) == "start")
                                                  ), psf.col("distance_m"))
                                        )
                             .withColumn("start_distance_m", 
                                         psf.when(((psf.col("event") == "end") & 
                                                   (psf.lag(psf.col("event"),1).over(w) == "start")
                                                  ), psf.lag(psf.col("distance_m"),1).over(w))
                                        )
                             .withColumn("end_distance_m", 
                                         psf.when(psf.col("end_distance_m").isNull(),
                                                  psf.lead(psf.col("start_distance_m"),1).over(w)
                                                 )
                                            .otherwise(psf.col("end_distance_m"))
                                        )
                            )

# #grab the start/end values for the timestamp variable
df_contractor_annotations = (df_contractor_annotations
                             .withColumn("end_timestamp", 
                                         psf.when(((psf.col("event") == "end") & 
                                                   (psf.lag(psf.col("event"),1).over(w) == "start")
                                                  ), psf.col("ts"))
                                        )
                             .withColumn("start_timestamp", 
                                         psf.when(((psf.col("event") == "end") & 
                                                   (psf.lag(psf.col("event"),1).over(w) == "start")
                                                  ), psf.lag(psf.col("ts"),1).over(w))
                                        )
                             .where(psf.length(psf.col("contractor_annotation")) >= 30)
                            )
                              
df_contractor_annotations = (df_contractor_annotations
                             .where(psf.col("start_timestamp").isNotNull() &
                                    psf.col("end_timestamp").isNotNull()
                                   )
                             .select("video_id", "contractor_annotation", 
                                     "start_timestamp", "end_timestamp", 
                                     "start_distance_m", "end_distance_m"
                                    )
                             .withColumn("_DLCleansedZoneTimeStamp", psf.current_timestamp())
                            )

df_contractor_annotations.write.insertInto("stage.cctv_contractor_annotations") #contractor identified defects
