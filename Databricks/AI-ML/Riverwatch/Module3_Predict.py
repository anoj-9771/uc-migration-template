# Databricks notebook source
# MAGIC %md # Import library

# COMMAND ----------

from pyspark.sql import functions as psf
from pyspark.sql import Window as W
from pyspark.sql import types as t
from pyspark.sql.types import IntegerType, StringType, DoubleType, StructType, StructField, FloatType
import mlflow
from pyspark.sql.functions import struct

# COMMAND ----------

# MAGIC %md # Load model

# COMMAND ----------

# # ----------------------- load registered model--------------
_MODEL_NAME = "riverwatch-pollution-classifier"
_MODEL_VERSION = "3"
logged_model = f'models:/{_MODEL_NAME}/{_MODEL_VERSION}'
 
# # ------Load model as a Spark UDF. Override result_type if the model does not return double values.------------
loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri=logged_model, env_manager='conda', result_type='string')

# # ----------------------- Setting max nodes per CPU batch ---------------
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "2")

# COMMAND ----------

# MAGIC %md # Import data and predict

# COMMAND ----------

# # ----------------------- input data---------------------------
infer_input_cato = spark.table("datalab.riverwatch_preprocessed_model_input").limit(10)
# .repartition(10)

prediction=(infer_input_cato
            .withColumn('results_BN',loaded_model())
            .withColumn("results_BW",psf.when(((psf.col("rain_48") >= 0) 
                          & (psf.col("rain_48") < 12)), "Unlikely")
                            .when(((psf.col("rain_48") >= 12) 
                          & (psf.col("rain_48") < 20)), "Possible")
                            .when((psf.col("rain_48") >= 20), "Likely")
                       )
            .select("results_BN","results_BW")
            )
display(prediction)

## adding the site name/id for specific sites
