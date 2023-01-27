# Databricks notebook source
# MAGIC %md # Import library

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SET JOB POOL TO USE UTC DATE. THIS IS NECESSARY OTHERWISE OLD DATA WOULD OTHERWISE BE RETRIEVED
# MAGIC SET TIME ZONE 'Australia/Sydney';

# COMMAND ----------

from pyspark.sql import functions as psf
from pyspark.sql import Window as W
from pyspark.sql import types as t
from pyspark.sql.types import IntegerType, StringType, DoubleType, StructType, StructField, FloatType
import mlflow
from pyspark.sql.functions import struct

dbutils.widgets.text(name="current_model_runtime", defaultValue="2022-02-10 08:33:00.000", label="current_model_runtime")
CURRENT_MODEL_RUNTIME = dbutils.widgets.get("current_model_runtime")
print(CURRENT_MODEL_RUNTIME)

# COMMAND ----------

# MAGIC %md # Load model

# COMMAND ----------

# # ----------------------- load registered model--------------
_MODEL_NAME = "riverwatch-pollution-classifier"
_MODEL_VERSION = "Production"
logged_model = f'models:/{_MODEL_NAME}/{_MODEL_VERSION}'
 
# # ------Load model as a Spark UDF. Override result_type if the model does not return double values.------------
loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri=logged_model, env_manager='conda', result_type='string')

# # ----------------------- Setting max nodes per CPU batch ---------------
# spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "12")

# COMMAND ----------

# MAGIC %md # Import data and predict

# COMMAND ----------

# # ----------------------- input data---------------------------
df_up_water_quality_features = (spark.table("cleansed.urbanplunge_water_quality_features")
                                .drop("_DLCleansedZoneTimeStamp")
                                .dropDuplicates()
                               )
# .repartition(10)
# # display(df_up_water_quality_features)
df_water_quality_prediction_tmp = (df_up_water_quality_features
                                   .withColumn("feature_date",psf.to_date("timestamp"))
                                   .withColumn("current_date", psf.to_date(psf.lit(CURRENT_MODEL_RUNTIME)
                                                                          )
                                              )
                                   .where(psf.col("feature_date") == psf.col("current_date"))
    #                                .where(psf.col("timestamp") == 
    #                                       (psf.floor(psf.unix_timestamp(psf.lit(CURRENT_MODEL_RUNTIME),
    #                                                                     "yyyy-MM-dd HH:mm:ss.SSS"
    #                                                                    )/3600
    #                                                 )*3600
    #                                       ).cast("timestamp") - psf.expr('INTERVAL 1 HOURS')
    #                                      )
                                   .withColumn('waterQualityPredictionSW',loaded_model())
                                   .withColumn("waterQualityPredictionBeachwatch",psf.when(((psf.col("rain_48") >= 0)           
                                                 & (psf.col("rain_48") < 12)), "Unlikely")
                                                   .when(((psf.col("rain_48") >= 12) 
                                                 & (psf.col("rain_48") < 20)), "Possible")
                                                   .when((psf.col("rain_48") >= 20), "Likely")
                                              )
                                   .withColumn("_DLCleansedZoneTimeStamp", psf.current_timestamp())
                                  )
display(df_water_quality_prediction_tmp)

df_water_quality_prediction = (df_water_quality_prediction_tmp
                               .select("locationId",
                                           "siteName",
                                           "timestamp",
                                           "waterQualityPredictionSW",
                                           "waterQualityPredictionBeachwatch",
                                           "_DLCleansedZoneTimeStamp"
                                          )
                              )

display(df_water_quality_prediction)

## adding the site name/id for specific sites

# COMMAND ----------

# MAGIC %md # save predictions to table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create urbanplunge_water_quality_predictions table in cleansed layer
# MAGIC CREATE TABLE IF NOT EXISTS cleansed.urbanplunge_water_quality_predictions
# MAGIC (locationId INT,
# MAGIC siteName STRING,
# MAGIC timestamp TIMESTAMP,
# MAGIC waterQualityPredictionSW STRING,
# MAGIC waterQualityPredictionBeachwatch STRING,
# MAGIC _DLCleansedZoneTimeStamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA 
# MAGIC LOCATION 'dbfs:/mnt/datalake-cleansed/urbanplunge/urbanplunge_water_quality_predictions'

# COMMAND ----------

df_water_quality_prediction.write.mode("append").insertInto('cleansed.urbanplunge_water_quality_predictions')