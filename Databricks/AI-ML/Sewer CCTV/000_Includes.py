# Databricks notebook source
dbutils.widgets.text(name="video_id", defaultValue="0_oiif5iqr", label="video_id")

# COMMAND ----------

from pyspark.sql import functions as psf
from pyspark.sql import types as t

# COMMAND ----------

# MAGIC %run /build/includes/global-variables-python

# COMMAND ----------

def getVideoMetadata(video_id, video_mount_point, video_blob_storage):
    """
    Extract metadata of video
    Arguments:
        video_mount_point: mount point location to retrieve video from blob storage
        video_blob_storage: blob storage location for video
    Returns:
        list of extracted metadata fields from the video
    """
    #cv2 is installed on the cluster using PyPi the package name is opencv-python
    import cv2
    import math

    #retrieve video from blob storage into memoery (using opencv library)
    vcap = cv2.VideoCapture(video_mount_point)

    #extract total number of frames in video
    total_frames = int(vcap.get(cv2.CAP_PROP_FRAME_COUNT))

    #extract frames per second value for video
    fps = math.ceil(vcap.get(cv2.CAP_PROP_FPS))

    #calculate total duration of the video in milliseconds
    total_msecs = int(((total_frames-1)/fps)*1000)

    #extract first image from video and obtain pixel height and width
    success,image = vcap.read()
    height, width, ch = image.shape

    #combine metadata variables into a list to be converted into a spark dataframe
    return [[video_id, 
           video_mount_point, 
           video_blob_storage, 
           fps, 
           total_frames, 
           total_msecs
          ]]

# COMMAND ----------

def getFrameTimestamps(total_msecs, msecs_interval):
    """
    Determines list of frames to be extracted
    Arguments:
        total_msecs: total runtime of the video in milliseconds
        msecs_interval: interval to extract frames from video in milliseconds (eg., extract frame at 500ms interval)
    Returns:
        List of timestamp of each frame to be extracted
    """
    return list(range(0,total_msecs-msecs_interval, msecs_interval))

#register getFrameTimestamps() function as a spark udf
getFrameTimestampsUDF = psf.udf(lambda a,b: getFrameTimestamps(a,b), t.ArrayType(t.IntegerType()))

# COMMAND ----------

def getImageFiles(df_pandas):
    """
    Extracts raw image bytes from video
    Arguments:
        video_mount_point: mount point location to retrieve video from blob storage
        timestamp: timestamp at which the frame exists in the video
    Returns:
        image in the form of bytes
    """

    #cv2 is installed on the cluster using PyPi the package name is opencv-python
    import cv2
    from PIL import Image
    import os
    
    df_return = df_pandas.loc[:,['video_id', 'timestamp']]
    df_return.loc[:, 'path'] = None

    for idx, row in df_pandas.iterrows():
        #retrieve video from blob storage into memory (using opencv library)
        vcap = cv2.VideoCapture(row['video_mount_point'])

        #set timestamp location in video
        vcap.set(cv2.CAP_PROP_POS_MSEC,(row['timestamp']))

        #read image at set timestamp in the video
        success,image = vcap.read()

        img = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
        im_pil = Image.fromarray(img)
        path = f"/dbfs{row['blob_image_mnt_pt']}/{row['video_id']}-{row['timestamp']}.png"

        if not(os.path.exists(path)):
            im_pil.save(path, format='png')
            df_return.loc[idx,'path'] = path
        else:
            df_return.loc[idx,'path'] = f"Image already exists: {path}"
        
    return df_return
      
    
#register getImageFiles() function as a spark udf
# getImageFilesUDF = psf.udf(lambda b,c,d,e: getImageFiles(b,c,d,e), t.StringType())

# COMMAND ----------

from typing import Iterator
import pandas as pd
import mlflow

def getImageFilesUDF(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    for df_p in iterator:
        yield(getImageFiles(df_p))

# COMMAND ----------

def getTextInBoundingBox(df, x0, x1, y0, y1, output_col_name, x_col_name="x_left", y_col_name="y_top", height_col_name="pixel_height", width_col_name="pixel_width" ):
  """
    Extracts the text within the specified bounding box
    Arguments:
        df: dataframe
        x0: x coordinate for the left edge of the bounding box measured from the left most point of the image (percentage val between 0-1)
        x1: x coordinate for the right edge of the bounding box measured from the left most point of the image  (percentage val between 0-1)
        y0: y coordinate for the top edge of the bounding box measured from the top most point of the image (percentage val between 0-1)
        y1: y coordinate for the bottom edge of the bounding box measured from the top most point of the image (percentage val between 0-1)
        output_col_name: name of the outpul column
        x_col_name: name of the column containing the x coordinate for the text measured from the left most point - default: "x_left"
        y_col_name: name of the column containing the y coordinate for the text measured from the top most point - default: "y_top"
        height_col_name: name of the column containing the image pixel height - default: "pixel_height"
        width_col_name: name of the column containing the imaeg pixel width - default: "pixel_width"
    Returns:
        dataframe with the extracted text within the specified bounding box in a new column
    """
  from pyspark.sql import functions as psf
  
  return (df
          .withColumn(output_col_name,
                     psf.when(
                       (((psf.col(x_col_name) >= x0*psf.col(width_col_name))) &
                       ((psf.col(x_col_name) <= x1*psf.col(width_col_name))) &
                       ((psf.col(y_col_name) >= y0*psf.col(height_col_name))) &
                       ((psf.col(y_col_name) <= y1*psf.col(height_col_name))))
                     ,psf.col("text")
                     ).otherwise(None)
                     )
         )

# COMMAND ----------

def cleanDistanceText(df, distance_col_name):
    """
    Cleans string with the distance readings
    Arguments:
        df: dataframe
        distance_col_name: name of the column with the distance text to be cleaned
    Returns:
        dataframe with the cleaned distance text
    """
    from pyspark.sql import functions as psf

    return (df
      .withColumn(distance_col_name, psf.trim(distance_col_name))
      .withColumn(distance_col_name, psf.regexp_replace(distance_col_name, ' ', '.'))
      .withColumn(distance_col_name, psf.regexp_replace(distance_col_name, ',', '.'))
      .withColumn(distance_col_name, psf.regexp_replace(distance_col_name, ': ', '.'))
      .withColumn(distance_col_name, psf.regexp_replace(distance_col_name, '\.\.\.', '.'))
      .withColumn(distance_col_name, psf.regexp_replace(distance_col_name, '\.\.', '.'))
      .withColumn(distance_col_name, psf.regexp_replace(distance_col_name, '.m', ''))
      .withColumn(distance_col_name, psf.regexp_replace(distance_col_name, 'm', ''))
      .withColumn(distance_col_name, psf.regexp_replace(distance_col_name, '-', ''))
      .withColumn(distance_col_name, psf.regexp_replace(distance_col_name, ' 0', ''))
      .withColumn(distance_col_name, psf.regexp_replace(distance_col_name, ' ', ''))
      .withColumn(distance_col_name, psf.regexp_replace(distance_col_name, ':', ''))
      .withColumn(distance_col_name, psf.substring(distance_col_name, 1,5))
      .withColumn(distance_col_name, psf.col(distance_col_name).cast('float'))
      .withColumn(distance_col_name+"_2", psf.col(distance_col_name))
      .withColumn(distance_col_name, psf.when(psf.col(distance_col_name).isNull(),
                                                  psf.lead(psf.col(distance_col_name+"_2"),1).over(w)
                                                 ).otherwise(psf.col(distance_col_name))
                 )
     )

# COMMAND ----------

def checkDistanceText(df, distance_col_name):
  """
    Check the distance value that has been extracted is within the same range as the previous 20 distance values that have been extracted
    Arguments:
        df: dataframe
        distance_col_name: name of the column with the distance text to be cleaned
    Returns:
        dataframe with the cleaned distance text
    """
  from pyspark.sql import Window as W
  from pyspark.sql import functions as psf
  
  w_back = W.partitionBy("video_id").orderBy("timestamp").rowsBetween(-20,0)
  
  #check if 0 has been read as an 8
  df = (df
        .withColumn("median", 
                    psf.percentile_approx(psf.col(distance_col_name), 0.5).over(w_back)
                   ) #calculate median distance over the last 20 records
        .withColumn("diff", 
                    psf.col(distance_col_name) - psf.col("median")
                   ) #calclulate diff between current distance and the median over the last 20 records
        .withColumn(distance_col_name,  
                    psf.when(((psf.col(distance_col_name).cast('int') == 8) & (psf.col("diff") > 7)),
                             psf.col(distance_col_name) - 8
                            ).otherwise(psf.col(distance_col_name))#                                               
                   ) #if the current distance is 8.x m and the difference over the last
                     #20 records is > 7 then the OCR extract has read a 0 as an 8
       )
  
  #check if current distance is outside the range of it's neighbours
  df = (df
        .withColumn("median", 
                    psf.percentile_approx(psf.col(distance_col_name), 0.5).over(w_back)
                   ) #calculate median distance over the last 20 records
        .withColumn("diff", psf.col(distance_col_name) - psf.col("median"))
        .withColumn(distance_col_name, 
                    psf.when(((psf.col(distance_col_name).cast('int') == 8) & (psf.col("diff") > 7)),
                             psf.col(distance_col_name) - 8
                            ).otherwise(psf.col(distance_col_name))
                   )
        .withColumn("median", 
                    psf.percentile_approx(psf.col(distance_col_name), 0.5).over(w_back)
                   ) #calculate median distance over the last 20 records
        .withColumn("diff", 
                    psf.col(distance_col_name) - psf.col("median")
                   ) #calclulate diff between current distance and the median over the last 20 records
        .withColumn(distance_col_name, 
                    psf.when((psf.col("diff") > 3), 
                             psf.col("median")
                            ).otherwise(psf.col(distance_col_name))
                   ) #if the difference between the current value and the median
                     #over the last 20 records is still greater than 3 then take the median value
       )
  
  #Update first frames median value to 0
  df = (df
        .withColumn("median", 
                    psf.percentile_approx(psf.col(distance_col_name), 0.5).over(w_back)
                   ) #calculate median distance over the last 20 records
        .withColumn("median", 
                    psf.when(psf.col("median").isNull(), 0).otherwise(psf.col("median"))
                   ) #if the median is Null then this is the first frame in the video - set the median distance to 0
       )
  
  return df

# COMMAND ----------

def findStartEndofString(df, string_col_name, levenshtein_var=40):
  """
    Find the start and end of a string within consecutive time series. Using the levenshtein function compare the similarity of the string to the previous value and the subsequent value,
    when the difference between the current string and its previous or next value in the timeseries is greater than the 'levenshtein_var' then the start/end of the string sequence has been found
    Arguments:
        df: dataframe
        string_col_name: name of the column with the string to be analysed with the levenshtein function
        levenshtein_var: acceptable difference between current value and previous/next string value to determine whether the string is significantly different
    Returns:
        dataframe with the identified start/end locations of a string in a time series.
    """
  
  from pyspark.sql import Window as W
  w = W.partitionBy("video_id").orderBy("timestamp")
  
  return (df
          .withColumn("ls_distance_end", 
                      psf.levenshtein(psf.col(string_col_name), psf.lead(psf.col(string_col_name),1).over(w))
                     )
          .withColumn("ls_distance_start", 
                      psf.levenshtein(psf.col(string_col_name), psf.lag(psf.col(string_col_name),1).over(w))
                     )
          .where(((psf.col("ls_distance_start") <= levenshtein_var) & 
                  (psf.col("ls_distance_end") <= levenshtein_var)
                 ))
          .withColumn("ls_distance_end", 
                      psf.levenshtein(psf.col(string_col_name), psf.lead(psf.col(string_col_name),1).over(w))
                     )
          .withColumn("ls_distance_start", 
                      psf.levenshtein(psf.col(string_col_name), psf.lag(psf.col(string_col_name),1).over(w))
                     )
          .withColumn("event", 
                      psf.when(((psf.col("ls_distance_start") >= levenshtein_var) & 
                                (psf.col("ls_distance_end") >= levenshtein_var)
                               ), "start-end")
                      .when(((psf.col("ls_distance_end") >= levenshtein_var) | 
                             (psf.col("ls_distance_end").isNull())
                            ), "end")
                      .when(((psf.col("ls_distance_start") >= levenshtein_var) | 
                             (psf.col("ls_distance_start").isNull())
                            ), "start")
                      .otherwise(None)
                     )
          .where(psf.col("event").isNotNull())
         )
  

# COMMAND ----------

from typing import Iterator
import pandas as pd
import mlflow

def predictImagesUDF(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    """
    Wrapper call for mapInPandas
    """
    logged_model = '/dbfs/mnt/blob-sewercctvmodel/mlflow artifacts'
    loaded_model = mlflow.pyfunc.load_model(logged_model)
    
    for df_p in iterator:
        yield(loaded_model.predict(df_p))