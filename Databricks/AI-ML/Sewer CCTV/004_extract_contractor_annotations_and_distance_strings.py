# Databricks notebook source
# MAGIC %run ./000_Includes

# COMMAND ----------

  #default Widget Parameter
#define notebook widget to accept video_id parameter
dbutils.widgets.text(name="video_id", defaultValue="0_oiif5iqr", label="video_id")
_VIDEO_ID = dbutils.widgets.get("video_id").replace(".mp4",'')

# COMMAND ----------

from pyspark.sql import Window as W
from pyspark.sql import functions as psf
from pyspark.sql import types as t

w = W.partitionBy("video_id").orderBy("timestamp")

#define raw ocr dataframe for the specified video id
df_raw_ocr = (spark.table("stage.cctv_ocr_extract")
              .where(psf.col("video_id") == _VIDEO_ID)
             )

df_raw_ocr = df_raw_ocr.drop("_DLRawZoneTimeStamp")

#flatten the raw ocr extract output so that the columns for text, 
#bounding box (x,y coords) have their own column in the dataframe
df_cleansed_ocr = (df_raw_ocr
                   .withColumn("pixel_height", psf.col("Image.height"))
                   .withColumn("pixel_width", psf.col("Image.width"))
                   .withColumn("text", psf.col("ocr.recognitionResult.lines.text"))
                   .withColumn("bbox", psf.col("ocr.recognitionResult.lines.boundingBox"))
                   .withColumn("tmp", psf.explode(psf.arrays_zip("text", "bbox")))
                   .select("video_id",
                            "timestamp", 
                            psf.col("tmp.text").alias("text"),
                            psf.col("tmp.bbox")[0].alias("x_left"),
                            psf.col("tmp.bbox")[1].alias("y_top"),
                            "pixel_width",
                            "pixel_height"
                           )
                    )

# COMMAND ----------

#Extract the distance text at the bottom right hand corner of the image
df_cleansed_ocr = (getTextInBoundingBox(df_cleansed_ocr, x0=0.65, x1=1, y0=0.88, y1=1, output_col_name="distance_m")
                   .withColumn("distance_m", 
                               psf.when(psf.length("distance_m") <= 12, 
                                        psf.col("distance_m")
                                       ).otherwise(None)
                              ) #ensure text extracted is not greater than 12 characters
                  )

#Clean distance text string
df_cleansed_ocr = cleanDistanceText(df_cleansed_ocr, "distance_m")

#Extract the contractor annotation text in the middle of the image
df_cleansed_ocr = getTextInBoundingBox(df_cleansed_ocr, x0=0, x1=1, y0=0.2, y1=0.8, output_col_name="contractor_annotation")

#Check for duplicate text extractions (duplicates may exist from multiple runs on the same video in the sandbox)
df_cleansed_ocr = (df_cleansed_ocr
                   .where(psf.col("distance_m").isNotNull() | 
                          psf.col("contractor_annotation").isNotNull()
                         ) #only keep records where we have distance or contractor annotations
                   .groupBy("video_id", "timestamp") #check for any duplicates on the same timestamp 
                   .agg(psf.max(psf.col("distance_m")).alias("distance_m"), #take the max distance reading if duplicates
                        psf.concat_ws(" ", 
                                      psf.collect_list(psf.col("contractor_annotation"))
                                     ).alias("contractor_annotation") #concatenate all the annotations if there are duplicates
                       )
                   .where(psf.col("distance_m").isNotNull()) #drop null distance records
                  )

#Check the distance value that has been extracted is within the same range 
#as the previous 20 distance values that have been extracted
df_cleansed_ocr = checkDistanceText(df_cleansed_ocr, "distance_m")

#ensure every raw image for the selected video has a distance value
df_cleansed_ocr = (spark.table("stage.cctv_video_frames")         
                   .select("video_id", "timestamp")
                   .where(psf.col("video_id") == _VIDEO_ID)
                   .join(df_cleansed_ocr, 
                         ['video_id', 'timestamp'], 
                         how='left'
                        )
                   .withColumn("distance_m", 
                               psf.when(psf.col("distance_m").isNull(),
                                        psf.last("distance_m", ignorenulls=True).over(w) 
                                       ).otherwise(psf.col("distance_m"))
                              ) #if the image doesn't have a distance value assign the previous timestamps distance
                   .orderBy("timestamp")
                   .select("video_id", "timestamp", "distance_m", "contractor_annotation")
                   #----- Add any additional columns required for the specific layer here ------
                   .withColumn("_DLCleansedZoneTimestamp",psf.current_timestamp())
                  )

df_cleansed_ocr.write.mode("append").insertInto('stage.cctv_ocr_extract_cleansed')
