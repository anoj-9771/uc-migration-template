# Databricks notebook source
# MAGIC %run ./000_Includes

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create cctv_ocr_extract table in raw layer
# MAGIC CREATE TABLE IF NOT EXISTS raw.cctv_ocr_extract
# MAGIC (video_id STRING,
# MAGIC  timestamp INT,
# MAGIC  image STRUCT<origin STRING, height: INT, width:INT, nChannels:INT, mode:INT, data:binary>,
# MAGIC  image_url STRING,
# MAGIC  RecognizeText_bdc240b316bc_error STRUCT<response string,
# MAGIC                                          status STRUCT<
# MAGIC                                            protocolVersion STRUCT<
# MAGIC                                              protocol STRING,
# MAGIC                                              major INT,
# MAGIC                                              minor INT
# MAGIC                                              >,
# MAGIC                                            statusCode int,
# MAGIC                                            reasonPhrase string
# MAGIC                                            >
# MAGIC                                          >,
# MAGIC ocr STRUCT<status STRING,
# MAGIC             recognitionResult STRUCT<
# MAGIC               lines ARRAY<
# MAGIC                 STRUCT<
# MAGIC                   boundingBox ARRAY<
# MAGIC                     INT
# MAGIC                   >,
# MAGIC                   text STRING,
# MAGIC                   words ARRAY<
# MAGIC                     STRUCT<
# MAGIC                       boundingBox ARRAY<
# MAGIC                         INT
# MAGIC                       >,
# MAGIC                       text STRING
# MAGIC                     >
# MAGIC                   >
# MAGIC                 >
# MAGIC               >
# MAGIC             >
# MAGIC            >,
# MAGIC _DLRawZoneTimeStamp TIMESTAMP
# MAGIC )
# MAGIC PARTITIONED BY (video_id)
# MAGIC LOCATION 'dbfs:/mnt/datalake-raw/sewercctv/cctv_ocr_extract'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create cctv_ocr_extract table in raw layer
# MAGIC CREATE TABLE IF NOT EXISTS stage.cctv_ocr_extract
# MAGIC (video_id STRING,
# MAGIC  timestamp INT,
# MAGIC  image STRUCT<origin STRING, height: INT, width:INT, nChannels:INT, mode:INT, data:binary>,
# MAGIC  image_url STRING,
# MAGIC  RecognizeText_bdc240b316bc_error STRUCT<response string,
# MAGIC                                          status STRUCT<
# MAGIC                                            protocolVersion STRUCT<
# MAGIC                                              protocol STRING,
# MAGIC                                              major INT,
# MAGIC                                              minor INT
# MAGIC                                              >,
# MAGIC                                            statusCode int,
# MAGIC                                            reasonPhrase string
# MAGIC                                            >
# MAGIC                                          >,
# MAGIC ocr STRUCT<status STRING,
# MAGIC             recognitionResult STRUCT<
# MAGIC               lines ARRAY<
# MAGIC                 STRUCT<
# MAGIC                   boundingBox ARRAY<
# MAGIC                     INT
# MAGIC                   >,
# MAGIC                   text STRING,
# MAGIC                   words ARRAY<
# MAGIC                     STRUCT<
# MAGIC                       boundingBox ARRAY<
# MAGIC                         INT
# MAGIC                       >,
# MAGIC                       text STRING
# MAGIC                     >
# MAGIC                   >
# MAGIC                 >
# MAGIC               >
# MAGIC             >
# MAGIC            >,
# MAGIC _DLRawZoneTimeStamp TIMESTAMP
# MAGIC )
# MAGIC PARTITIONED BY (video_id)
# MAGIC LOCATION 'dbfs:/mnt/datalake-stage/stage/cctv_ocr_extract'

# COMMAND ----------

#default Widget Parameter
#define notebook widget to accept video_id parameter
dbutils.widgets.text(name="video_id", defaultValue="0_oiif5iqr", label="video_id")

_VIDEO_ID = dbutils.widgets.get("video_id").replace(".mp4",'')

# COMMAND ----------

ADS_KV_ACCOUNT_SCOPE = "ADS"
ADS_COGCV_NAME = "daf-cognitive-services-computer-vision-name"
ADS_COGCV_KEY = "daf-cognitive-services-computer-vision-key"
_COG_CV_SUBSCRIPTION_KEY = dbutils.secrets.get(scope=ADS_KV_ACCOUNT_SCOPE, key=ADS_COGCV_KEY)
_COG_CV_NAME = dbutils.secrets.get(scope=ADS_KV_ACCOUNT_SCOPE, key=ADS_COGCV_NAME)


# COMMAND ----------

#The Synapse ML SDK for Cognitive Services is installed on the cluster using Maven
#Maven coordinates: com.microsoft.azure:synapseml_2.12:0.9.5
#Maven Repository: https://mmlspark.azureedge.net/maven

from synapse.ml import cognitive as synapsemlcog

_COG_CV_READ_TEXT_ENDPOINT = f"https://{_COG_CV_NAME}.cognitiveservices.azure.com/vision/v2.0/recognizeText"
_LOCATION = "australiaeast"

# COMMAND ----------

#define dataframe with the raw images for the selected video
from pyspark.sql import functions as psf
from pyspark.sql import types as t
from pyspark.sql.functions import lit
import dateutil.parser


df_raw_images_org = (spark.table("stage.cctv_video_frames")
                     .where(psf.col("video_id") == _VIDEO_ID)
                    )
df_raw_images = df_raw_images_org.drop("_DLRawZoneTimeStamp")

#define cognitive services variable to recognise text in an image
cog_recognizeText = (synapsemlcog.RecognizeText()
                 .setUrl(_COG_CV_READ_TEXT_ENDPOINT) #set endpoint for reading text in an image
                 .setSubscriptionKey(_COG_CV_SUBSCRIPTION_KEY) #set subscription key for cognitive services computer vision
                 .setLocation(_LOCATION) #set server location for cognitive services
                 .setImageUrlCol("image_url")
                 .setOutputCol("ocr") #define name of output col containing extracted text
                 .setMode("Printed") #desired text to be extracted is printed and not handwritten
                 .setConcurrency(5) #number of images to process in parallel
                )

#call cognitive services cv to recognise text in the video frame
df_raw_ocr = (cog_recognizeText             
              .transform(df_raw_images)
              .withColumn("_DLRawZoneTimeStamp", psf.current_timestamp())  #timestamp for when the data has been processed
              .orderBy("timestamp")
             )

#data inserted to the raw ocr extract table
df_raw_ocr.write.mode("append").insertInto('stage.cctv_ocr_extract')
