# Databricks notebook source
# MAGIC %md # Import libraries

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SET JOB POOL TO USE UTC DATE. THIS IS NECESSARY OTHERWISE OLD DATA WOULD OTHERWISE BE RETRIEVED
# MAGIC SET TIME ZONE 'Australia/Sydney';

# COMMAND ----------

# MAGIC %run /build/includes/global-variables-python

# COMMAND ----------

# MAGIC %run /build/includes/util-general

# COMMAND ----------

dbutils.widgets.text(name="current_model_runtime", defaultValue="2022-02-10T08:33:00.000", label="current_model_runtime")
dbutils.widgets.text(name="last_model_runtime", defaultValue="2022-02-09T08:00:00.000", label="last_model_runtime")

# COMMAND ----------

from pyspark.sql import functions as psf
from pyspark.sql import Window as W
from pyspark.sql import types as t
from pyspark.sql.types import IntegerType, StringType, DoubleType, StructType, StructField, FloatType
import datetime
 
LAST_MODEL_RUNTIME = dbutils.widgets.get("last_model_runtime")
CURRENT_MODEL_RUNTIME = dbutils.widgets.get("current_model_runtime")
# CURRENT_MODEL_RUNTIME = datetime.datetime.now() # set current timestamp (using datetime function) for test

print(CURRENT_MODEL_RUNTIME)

# COMMAND ----------

# MAGIC %md # Import cleansed data

# COMMAND ----------

# table_names_in_cleansed_db = [t.name for t in spark.catalog.listTables("cleansed")]
# table_names_in_datalab_db = [t.name for t in spark.catalog.listTables("datalab")]


#----- Load IICATS Hierarchy Configuration table -----
# if "iicats_hierarchy_cnfgn" in table_names_in_cleansed_db:
if TableExists(f"{ADS_DATABASE_CLEANSED}.iicats.hierarchy_cnfgn"):
    df_hierarchy_cnfgn = spark.table(f"{ADS_DATABASE_CLEANSED}.iicats.hierarchy_cnfgn").alias("hcnfg")
    print("IICATS Hierarchy Config loaded from cleansed")
else:
    print("Cleansed tables for IICATS Hierarchy Config do not exist.")

    
#----- Load IICATS TSV Point Configuration table -----    
# if "iicats_tsv_point_cnfgn" in table_names_in_cleansed_db:
if TableExists(f"{ADS_DATABASE_CLEANSED}.iicats.tsv_point_cnfgn"):
    df_time_series_values_cnfgn = spark.table(f"{ADS_DATABASE_CLEANSED}.iicats.tsv_point_cnfgn").alias("tsvptcnfg")
    print("IICATS TSV Point Config loaded from cleansed")
else:
    print("Cleansed tables for IICATS TSV Config do not exist.")
    
    
#----- Load IICATS TSV table -----        
# if "iicats_tsv" in table_names_in_cleansed_db:
if TableExists(f"{ADS_DATABASE_CLEANSED}.iicats.tsv"):
    df_time_series_values = spark.table(f"{ADS_DATABASE_CLEANSED}.iicats.tsv").alias("tsv")
    print("IICATS TSV loaded from cleansed")
else:
    print("Cleansed tables for IICATS TSV do not exist.")
    
    
#----- Load BoM Daily Weather Observations - Sydney Airport -----        
# if "bom_dailyweatherobservation_sydneyairport" in table_names_in_cleansed_db:
if TableExists(f"{ADS_DATABASE_CLEANSED}.bom.dailyweatherobservation_sydneyairport"):
    df_sun = spark.table(f"{ADS_DATABASE_CLEANSED}.bom.dailyweatherobservation_sydneyairport")
    print("BoM Weather Observations loaded from cleansed")
else:
    print("Cleansed table for BoM Daily Weather Observation does not exist.")     
    
    
#----- Load BoM Daily Climate Data - Sydney Airport -----        
# if "bom_dailyclimatedata_sydneyairport" in table_names_in_cleansed_db:
if TableExists(f"{ADS_DATABASE_CLEANSED}.bom.dailyclimatedata_sydneyairport"):
    df_solar = spark.table(f"{ADS_DATABASE_CLEANSED}.bom.dailyclimatedata_sydneyairport")
    print("BoM Climate Data loaded from cleansed")
else:
    schema = StructType([
            StructField('Product Code', StringType()),
            StructField('Bureau of Meteorology station number', StringType()),
            StructField('Year', StringType()),
            StructField('Month', StringType()),
            StructField('Day', StringType()),
            StructField('Daily global solar exposure (MJ/m*m)', StringType())
        ])
    df_solar = spark.createDataFrame([], schema)
    print("BoM Solar: Empty DF Created")

# COMMAND ----------

# MAGIC %md ## Variables for time filter

# COMMAND ----------

RAIN_3_FILTER=-3
RAIN_24_FILTER=-24
RAIN_48_FILTER=-48
RAIN_72_FILTER=-72
RAIN_7d_FILTER=-168

EPOCH_TIMESTAMP_8d=3600*24*8
EPOCH_TIMESTAMP_2d=3600*24*2

# COMMAND ----------

# MAGIC %md ## Apply time filter
# MAGIC to focus on data need to be analysed

# COMMAND ----------

df_time_series_values = (df_time_series_values
#                          .withColumn("measurementResultAESTDateTime",psf.col("measurementResultAESTDateTime") - psf.expr("INTERVAL 10 HOURS"))
                          .withColumn("epoch_LAST_RUNTIME",psf.unix_timestamp(psf.date_trunc("hour",psf.lit(LAST_MODEL_RUNTIME)).cast("timestamp")))
                          .withColumn("epoch_CURRENT_RUNTIME",psf.unix_timestamp(psf.date_trunc("hour",psf.lit(CURRENT_MODEL_RUNTIME)).cast("timestamp")))
                          .withColumn("epochMeasurementResultAESTDateTime",psf.unix_timestamp(psf.col("measurementResultAESTDateTime")))
                          .where(psf.col("epochMeasurementResultAESTDateTime")<=psf.col("epoch_CURRENT_RUNTIME"))
                          .where(psf.col("epochMeasurementResultAESTDateTime")>=(psf.col("epoch_LAST_RUNTIME")-EPOCH_TIMESTAMP_8d))
                         )

df_sun=(df_sun
              .withColumn("epoch_LAST_RUNTIME",psf.unix_timestamp(psf.date_trunc("hour",psf.lit(LAST_MODEL_RUNTIME)).cast("timestamp")))
              .withColumn("epoch_CURRENT_RUNTIME",psf.unix_timestamp(psf.date_trunc("hour",psf.lit(CURRENT_MODEL_RUNTIME)).cast("timestamp")))
              .withColumn("epoch_Date",psf.unix_timestamp(psf.col("Date")))
              .where(psf.col("epoch_Date")<=psf.col("epoch_CURRENT_RUNTIME"))
              .where(psf.col("epoch_Date")>=(psf.col("epoch_LAST_RUNTIME")-EPOCH_TIMESTAMP_2d))
       )

df_solar=(df_solar
              .withColumn("Date", psf.date_format(psf.make_date("Year","Month","Day"), 'y-M-d').alias('Date'))
              .withColumn("epoch_LAST_RUNTIME",psf.unix_timestamp(psf.date_trunc("hour",psf.lit(LAST_MODEL_RUNTIME)).cast("timestamp")))
              .withColumn("epoch_CURRENT_RUNTIME",psf.unix_timestamp(psf.date_trunc("hour",psf.lit(CURRENT_MODEL_RUNTIME)).cast("timestamp")))
              .withColumn("epoch_Date",psf.unix_timestamp(psf.col("Date").cast('timestamp')))
              .where(psf.col("epoch_Date")<=psf.col("epoch_CURRENT_RUNTIME"))
              .where(psf.col("epoch_Date")>=(psf.col("epoch_LAST_RUNTIME")-EPOCH_TIMESTAMP_2d))
       )

# COMMAND ----------

# MAGIC %md ## Hourly rainfall all sites

# COMMAND ----------

list_object_internal_id = [557525,563912,564020,564074,564371,585597,585649]
df_time_series_values = (df_time_series_values
                         .where(psf.col('objectInternalId').isin(list_object_internal_id))
                         .distinct()
                         .withColumn("timestamp", psf.unix_timestamp(psf.col("measurementResultAESTDateTime")) - psf.unix_timestamp(psf.col("measurementResultAESTDateTime"))%3600)
                         .groupBy("objectInternalId", "timestamp")
                         .agg(psf.sum(psf.col("measurementResultValue")).alias("measurementResultValue"))
                         .alias("tsv")
                         .orderBy("timestamp")
                        )

# COMMAND ----------

# MAGIC %md ## Function Get hourly interval (from earliest to latest)

# COMMAND ----------

def getHourlyIntervals(first_timestamp, last_timestamp, secs_interval):
    return list(range(first_timestamp,last_timestamp+secs_interval, secs_interval))

getHourlyIntervalsUDF = psf.udf(lambda a,b,c: getHourlyIntervals(a,b,c), t.ArrayType(t.IntegerType()))

# COMMAND ----------

# MAGIC %md ## Join df_hierarchy_cnfgn and df_time_series_values_cnfgn

# COMMAND ----------

df_iicats_rainfall = (df_hierarchy_cnfgn
                      .where((psf.col("siteCd") == "GG0022") |
                             (psf.col("siteCd") == "GG0064") |
                             (psf.col("siteCd") == "GG0020") |
                             (psf.col("siteCd") == "GG0008") |
                             (psf.col("siteCd") == "GG0019") |
                             (psf.col("siteCd") == "GG0047") |
                             (psf.col("siteCd") == "GG0016")
                            )
                      
                      .where(psf.col("pointName") == "Rainfall 15M Total")
                      .orderBy(psf.col("effectiveFromDateTime").desc())
                      .join(df_time_series_values_cnfgn,
                            on=df_hierarchy_cnfgn.objectInternalId==df_time_series_values_cnfgn.objectInternalId,
                            how='left'
                           )
                      .groupBy("hcnfg.objectInternalId", "siteCd", "siteName",  "tsvptcnfg.objectInternalId", "pointInternalId", "timeBaseCd","pointStatisticTypeCd")
                      .agg(psf.max(psf.col("tsvptcnfg.sourceRecordCreationDateTime")).alias("HT_CRT_DT"))
                      .where(psf.col("timeBaseCd") == 15)
                      .where(psf.col("pointStatisticTypeCd") == "SN")
                      .join(df_time_series_values,
                            on=df_time_series_values_cnfgn.pointInternalId==df_time_series_values.objectInternalId,
                            how='left'
                           )
                       )

# COMMAND ----------

# MAGIC %md ## Get hourly interval per site

# COMMAND ----------

window = W.partitionBy("SITE_NM").orderBy(psf.unix_timestamp("timestamp"))

print(LAST_MODEL_RUNTIME)
df = (df_iicats_rainfall
      .groupBy("siteName", "pointInternalId")
      .agg(psf.min(psf.col("timestamp")).alias("first_timestamp"),
#            psf.max(psf.col("timestamp")).alias("last_timestamp")
          )
      .where(psf.col("first_timestamp").isNotNull())
       )

# COMMAND ----------

# MAGIC %md # Preprocess data to form model input

# COMMAND ----------

# MAGIC %md ## Create reference timestamp for filling missing hourly data

# COMMAND ----------

realtime_ref  = (df
        .withColumn("hourly_timestamps", getHourlyIntervalsUDF(psf.col("first_timestamp"),
                                                               psf.unix_timestamp(psf.date_trunc("hour",psf.lit(CURRENT_MODEL_RUNTIME)).cast("timestamp")), psf.lit(3600)))
        .withColumn("epoch_timestamp", psf.explode(psf.col("hourly_timestamps")))
        .withColumn("timestamp", psf.to_timestamp(psf.from_unixtime(psf.col("epoch_timestamp"))))
        .select("siteName", "pointInternalId", "epoch_timestamp","timestamp")
         .alias("ref")
       )

# COMMAND ----------

# MAGIC %md ## Fill missing hourly time slot for each site

# COMMAND ----------

realtime_missing_hourly_filled=(realtime_ref
        .join(df_time_series_values,
              on=((psf.col("ref.epoch_timestamp")==psf.col("tsv.timestamp")) &
                  (psf.col("ref.pointInternalId")==psf.col("tsv.objectInternalId"))
                 ),
              how='left'
             )
      .withColumn("measurementResultValue", psf.when(psf.col("measurementResultValue").isNull(), 0).otherwise(psf.col("measurementResultValue")))
       )

# COMMAND ----------

# MAGIC %md ## Allocate gauges geospatially related to swim sites

# COMMAND ----------

swim_site_specific_rainfall = (realtime_missing_hourly_filled
                               .withColumn("siteName", 
                                           psf.when(((psf.col("siteName") == "GG0019 CONCORD") |
                                                     (psf.col("siteName") == "GG0020 FIVEDOCK") |
                                                     (psf.col("siteName") == "GG0064 GLADESVILLE") 
                                                    ), "Bayview"
                                                   )
                                               .when(((psf.col("siteName") == "GG0047 RYDE") |
                                                      (psf.col("siteName") == "GG0016 HOMEBUSH") |
                                                      (psf.col("siteName") == "GG0064 GLADESVILLE") | 
                                                      (psf.col("siteName") == "GG0019 CONCORD")
                                                     ), "Putney Park"
                                                    )
                                                .otherwise(None)
                                          )
                               .where(psf.col("siteName").isNotNull())
                              )

# COMMAND ----------

# MAGIC %md ## Factors 1. rain 24

# COMMAND ----------

w24=W.partitionBy("siteName").orderBy("epoch_timestamp").rowsBetween(RAIN_24_FILTER,-1)

rain_24=(swim_site_specific_rainfall
         .groupBy("ref.timestamp", "epoch_timestamp", "siteName")
         .mean("measurementResultValue")
         .withColumn("rain_24", psf.sum(psf.col("avg(measurementResultValue)")).over(w24))
         .where(psf.col("ref.timestamp") == psf.to_timestamp(psf.lit(CURRENT_MODEL_RUNTIME)))
         .alias("past_rain_hours")
         )
#display(rain_24)

# COMMAND ----------

# MAGIC %md ## Factors 2. rain 48

# COMMAND ----------

w48=W.partitionBy("siteName").orderBy("epoch_timestamp").rowsBetween(RAIN_48_FILTER,-1)

rain_48=(swim_site_specific_rainfall
         .withColumn("date", psf.to_date(psf.col("ref.timestamp")))
         .groupBy("ref.timestamp", "epoch_timestamp", "siteName")
         .mean("measurementResultValue")
         .withColumn("rain_48", psf.sum(psf.col("avg(measurementResultValue)")).over(w48))
         .where(psf.col("ref.timestamp") == psf.to_timestamp(psf.lit(CURRENT_MODEL_RUNTIME)))
         .orderBy("epoch_timestamp")
         .alias("past_rain_hours")
        )
#display(rain_48)

# COMMAND ----------

# MAGIC %md ## Factors 3. rain 72

# COMMAND ----------

w72=W.partitionBy("siteName").orderBy("epoch_timestamp").rowsBetween(RAIN_72_FILTER,-1)

rain_72=(swim_site_specific_rainfall
       .withColumn("date", psf.to_date(psf.col("ref.timestamp")))
                .groupBy("ref.timestamp", "epoch_timestamp", "siteName")
                .mean("measurementResultValue")
                .withColumn("rain_72", psf.sum(psf.col("avg(measurementResultValue)")).over(w72))
              .where(psf.col("ref.timestamp") == psf.to_timestamp(psf.lit(CURRENT_MODEL_RUNTIME)))
               .orderBy("epoch_timestamp")
                  .alias("past_rain_hours")
      )
#display(rain_72)

# COMMAND ----------

# MAGIC %md ## Factors 4. rain 7d

# COMMAND ----------

w7d=W.partitionBy("siteName").orderBy("epoch_timestamp").rowsBetween(RAIN_7d_FILTER,-1)

rain_7d=(swim_site_specific_rainfall
       .withColumn("date", psf.to_date(psf.col("ref.timestamp")))
                .groupBy("ref.timestamp", "epoch_timestamp", "siteName")
                .mean("measurementResultValue")
                .withColumn("rain_7d", psf.sum(psf.col("avg(measurementResultValue)")).over(w7d))
                .where(psf.col("ref.timestamp") == psf.to_timestamp(psf.lit(CURRENT_MODEL_RUNTIME)))
               .orderBy("epoch_timestamp")
                  .alias("past_rain_hours")
      )
#display(rain_7d)

# COMMAND ----------

# MAGIC %md ## Factors 5. rain intensity

# COMMAND ----------

w3=W.partitionBy("siteName").orderBy("epoch_timestamp").rowsBetween(RAIN_3_FILTER,-1)
w48=W.partitionBy("siteName").orderBy("epoch_timestamp").rowsBetween(RAIN_48_FILTER,-1)

Rintensity=(swim_site_specific_rainfall
            .withColumn("date", psf.to_date(psf.col("ref.timestamp")))
            .groupBy("ref.timestamp", "epoch_timestamp", "siteName")
            .mean("measurementResultValue")
            .withColumn("rain_int", psf.mean(psf.col("avg(measurementResultValue)")).over(w3))
            .withColumn("Rintensity", psf.max(psf.col("rain_int")).over(w48))
            .where(psf.col("ref.timestamp") == psf.to_timestamp(psf.lit(CURRENT_MODEL_RUNTIME)))
            .orderBy("epoch_timestamp")
            .alias("past_rain_hours")
          )

#display(Rintensity)

# COMMAND ----------

# MAGIC %md ## Factors 6. rain duration

# COMMAND ----------

w48=W.partitionBy("siteName").orderBy("epoch_timestamp").rowsBetween(RAIN_48_FILTER,-1)

Rduration=(swim_site_specific_rainfall
           .withColumn("date", psf.to_date(psf.col("ref.timestamp")))
           .groupBy("ref.timestamp", "epoch_timestamp", "siteName")
           .mean("measurementResultValue")
           .withColumn("rain_dur", psf.when(psf.col('avg(measurementResultValue)') >= 2., 1).otherwise(0))
           .withColumn("Rduration", psf.sum(psf.col("rain_dur")).over(w48))
           .where(psf.col("ref.timestamp") == psf.to_timestamp(psf.lit(CURRENT_MODEL_RUNTIME)))
           .orderBy("epoch_timestamp")
           .alias("past_rain_hours")
          )

#display(Rduration)

# COMMAND ----------

# MAGIC %md ## Factors 7. rain distribution

# COMMAND ----------

w48partNM = W.partitionBy("siteName").orderBy("epoch_timestamp").rowsBetween(RAIN_48_FILTER, -1)

Rdistribution = (swim_site_specific_rainfall
                    .withColumn("rain_48_ES",psf.sum(psf.col("measurementResultValue")).over(w48partNM))
                    .withColumn("flag:rain_48>=2mm",psf.when(psf.col("rain_48_ES") >= 2., 1).otherwise(0))
                    .groupBy("ref.timestamp", "epoch_timestamp","siteName")
                    .sum("flag:rain_48>=2mm")
                    .withColumnRenamed("sum(flag:rain_48>=2mm)", "Rdistribution")
                 .where(psf.col("ref.timestamp") == psf.to_timestamp(psf.lit(CURRENT_MODEL_RUNTIME)))
                    .orderBy("epoch_timestamp")
                    .select("ref.timestamp", "epoch_timestamp","Rdistribution","siteName")
                   )

#display(Rdistribution)

# COMMAND ----------

# MAGIC %md ## Factors 8. days_after_rain_20mm (24 hours daily with floating days)

# COMMAND ----------

# The days between the water quality sample and the 'first preceding' rain event must have had less then 20mm to have been counted.
daily_seconds=3600*24
wdaily = W.orderBy("epoch_timestamp")
days_after_rain_20mm = (swim_site_specific_rainfall
                        .groupBy("ref.timestamp", "epoch_timestamp", "siteName")
                        .mean("measurementResultValue")
                        .withColumn("rain_24", psf.sum(psf.col("avg(measurementResultValue)")).over(w24))
                        .withColumnRenamed("rain_24", "rain_daily")
                        .withColumn("timestamp", psf.to_timestamp(psf.from_unixtime(psf.col("epoch_timestamp"))))
                        .orderBy("epoch_timestamp")
                        .withColumn('epoch_day_rain_20mm', psf.when(psf.col('rain_daily')>=20, psf.col('epoch_timestamp')))
                        .withColumn('preceding_epoch_day_rain_20mm', psf.last('epoch_day_rain_20mm', ignorenulls=True).over(wdaily.rowsBetween(W.unboundedPreceding, -1)))
                        .withColumn("days_after_rain_20mm",(psf.col("epoch_timestamp")-psf.col("preceding_epoch_day_rain_20mm"))/daily_seconds)
                        .where(psf.col("ref.timestamp") == psf.to_timestamp(psf.lit(CURRENT_MODEL_RUNTIME)))
#                         .alias("drydays20mm")
#                      .select("timestamp","epoch_timestamp", "siteName", "days_after_rain_20mm")
                      )
#display(days_after_rain_20mm)

# COMMAND ----------

# MAGIC %md ## Factors 9. sun_24

# COMMAND ----------

win = W.orderBy("date")
# display(df_sun)
sun_24=(df_sun
        .dropDuplicates(["Date"])
        .withColumnRenamed("Date", "date")
        .withColumn("sun_24", psf.lag("Sunshine_hours",1).over(win).cast("double"))
        .where(psf.col("date") == psf.to_date(psf.lit(CURRENT_MODEL_RUNTIME)))
        .withColumn("timestamp", psf.to_timestamp(psf.lit(CURRENT_MODEL_RUNTIME)))
        .select("timestamp", "Sunshine_hours", "sun_24")
        
       )
#display(sun_24)

# COMMAND ----------

# MAGIC %md ## Factors 10. solar_24

# COMMAND ----------

wsolor24 = W.partitionBy("Bureau of Meteorology station number").orderBy("date")

solar_24=(df_solar
          .withColumn("timestamp", psf.to_timestamp(psf.lit(CURRENT_MODEL_RUNTIME)))
#         .where(psf.col("Bureau of Meteorology station number")==66037) #66037 is the gauge number of Sydney Airport
#         .withColumn("date", psf.date_format(psf.make_date("Year","Month","Day"), 'y-M-d').alias('date'))
#         .withColumn("solar_24", psf.lag("Daily global solar exposure (MJ/m*m)",1).over(wsolor24).cast("double"))
#         .select("date", "Daily global solar exposure (MJ/m*m)", "solar_24")
       )
#display(solar_24)

# COMMAND ----------

# MAGIC %md ## Preprocess done

# COMMAND ----------

model_realtime_input = (rain_24
                       .join(rain_48,
                             on=["epoch_timestamp", "siteName"],
                             how="left")
                       .join(rain_72,
                             on=["epoch_timestamp", "siteName"],
                             how="left")
                       .join(rain_7d,
                             on=["epoch_timestamp", "siteName"],
                             how="left")
                       .join(Rintensity,
                             on=["epoch_timestamp", "siteName"],
                             how="left")
                       .join(Rduration,
                             on=["epoch_timestamp", "siteName"],
                             how="left")
                       .join(Rdistribution,
                             on=["epoch_timestamp", "siteName"],
                             how="left")
                       .join(days_after_rain_20mm,
                             on=["epoch_timestamp", "siteName"],
                             how="left")
#                        .join(sun_24,
#                              on="timestamp",
#                              how="left")
#                        .join(solar_24,
#                              on="timestamp",
#                              how="left")
#                        .withColumn("timestamp", psf.to_timestamp(psf.from_unixtime(psf.col("epoch_timestamp"))))
                        .withColumn("sun_24", psf.lit(None))
                        .withColumn("solar_24", psf.lit(None))
                       .orderBy("epoch_timestamp")
                       .select("epoch_timestamp","siteName", "ref.timestamp","rain_24","rain_48","rain_72","rain_7d"
                               ,"Rintensity","Rduration","Rdistribution","days_after_rain_20mm","sun_24","solar_24")
                       .na.fill(value=0,subset=["rain_24","rain_48","rain_72","rain_7d"
                               ,"Rintensity","Rduration","Rdistribution"])
                        .na.fill(value=7,subset=["days_after_rain_20mm"])
                        
                )
                        

max_timestamp = (model_realtime_input
                 .groupBy("siteName")
                 .agg(psf.max(psf.col("ref.timestamp")).alias("max_timestamp"))
                 .withColumnRenamed("siteName", "maxSiteName")
                ).alias("max")

model_realtime_input = (max_timestamp
                         .join(model_realtime_input, on=((max_timestamp.max_timestamp==model_realtime_input.timestamp) &
                                                         (max_timestamp.maxSiteName==model_realtime_input.siteName)
                                                        ),
                               how='left'
                              )
                        .drop("max_timestamp")
                        .drop("maxSiteName")
                        )

#display(model_realtime_input)

# COMMAND ----------

# MAGIC %md ## Categorise preprocessed data for inferencing
# MAGIC
# MAGIC This does not include enterocci, Salinity(EC) and Stormwatep_pct data

# COMMAND ----------

catranges_su24 = ['0.0-1', '1.1-5', '2.>=5']
catranges_rdur = ['0.0', '1.0-2', '2.2-4', '3.>=4']
catranges_ent = ['0.0-32', '1.32-39', '2.39-51', '3.51-158', '4.>=158']
catranges_r24 = ['0.0', '1.0-6', '2.6-12', '3.12-20', '4.>=20']
catranges_r48 = ['0.0', '1.0-6', '2.6-14', '3.14-20', '4.>=20']
catranges_r72 = ['0.0', '1.0-14', '2.14-30', '3.>=30']
catranges_r7d = ['0.0-10', '1.10-50', '2.>=50']
catranges_rdis = ['0.0', '1.0-5', '2.>=7']
catranges_so24 = ['0.0-1', '1.1-4', '2.4-9', '3.>=9']
catranges_ecc = ['0.0-40', '1.40-45', '2.45-50', '3.>=50']
catranges_rint = ['0.0', '1.0-2', '2.2-4', '3.>=4']
catranges_ctopct = ['0.0-5', '1.5-15', '2.15-25', '3.>=25']
catranges_dar20 = ['0.0-1', '1.1-2', '2.2-3', '3.3-4', '4.>=4']

infer_input_cato= (model_realtime_input
                       
    #---------------------------------- Categories
                       .withColumn("rain_24_cat", psf.when(psf.col("rain_24") == 0, catranges_r24[0])
                                                     .when(((psf.col("rain_24") > 0) 
                                                            & (psf.col("rain_24") < 6)), catranges_r24[1])
                                                     .when(((psf.col("rain_24") >= 6) 
                                                            & (psf.col("rain_24") < 12)), catranges_r24[2])
                                                     .when(((psf.col("rain_24") >= 12) 
                                                            & (psf.col("rain_24") < 20)), catranges_r24[3])
                                                     .when((psf.col("rain_24") >= 20), catranges_r24[4])
                                  )
                       .withColumn("rain_48_cat", psf.when(psf.col("rain_48") == 0, catranges_r48[0])
                                                     .when(((psf.col("rain_48") > 0) 
                                                            & (psf.col("rain_48") < 6)), catranges_r48[1])
                                                     .when(((psf.col("rain_48") >= 6) 
                                                            & (psf.col("rain_48") < 14)), catranges_r48[2])
                                                     .when(((psf.col("rain_48") >= 14) 
                                                            & (psf.col("rain_48") < 20)), catranges_r48[3])
                                                     .when((psf.col("rain_48") >= 20), catranges_r48[4])
                                  )
                       .withColumn("rain_72_cat", psf.when(psf.col("rain_72") == 0, catranges_r72[0])
                                                     .when(((psf.col("rain_72") > 0) 
                                                            & (psf.col("rain_72") < 14)), catranges_r72[1])
                                                     .when(((psf.col("rain_72") >= 14) 
                                                            & (psf.col("rain_72") < 30)), catranges_r72[2])
                                                     .when((psf.col("rain_72") >= 30), catranges_r72[3])
                                  )
                       .withColumn("rain_7d_cat", psf.when(((psf.col("rain_7d") >= 0) 
                                                            & (psf.col("rain_7d") < 10)), catranges_r7d[0])
                                                     .when(((psf.col("rain_7d") >= 10) 
                                                            & (psf.col("rain_7d") < 50)), catranges_r7d[1])
                                                     .when((psf.col("rain_7d") >= 50), catranges_r7d[2])
                                  )
                       .withColumn("Rintensity_cat", psf.when(psf.col("Rintensity") == 0, catranges_rint[0])
                                                     .when(((psf.col("Rintensity") > 0) 
                                                            & (psf.col("Rintensity") < 2)), catranges_rint[1])
                                                     .when(((psf.col("Rintensity") >= 2) 
                                                            & (psf.col("Rintensity") < 4)), catranges_rint[2])
                                                     .when((psf.col("Rintensity") >= 4), catranges_rint[3])
                                  )
                       .withColumn("Rduration_cat", psf.when(psf.col("Rduration") == 0, catranges_rdur[0])
                                                     .when(((psf.col("Rduration") > 0) 
                                                            & (psf.col("Rduration") < 2)), catranges_rdur[1])
                                                     .when(((psf.col("Rduration") >= 2) 
                                                            & (psf.col("Rduration") < 4)), catranges_rdur[2])
                                                     .when((psf.col("Rduration") >= 4), catranges_rdur[3])
                                  )
                       .withColumn("Rdistribution_cat", psf.when(psf.col("Rdistribution") == 0, catranges_rdis[0])
                                                     .when(((psf.col("Rdistribution") > 0) 
                                                            & (psf.col("Rdistribution") < 7)), catranges_rdis[1])
                                                     .when((psf.col("Rdistribution") >= 7), catranges_rdis[2])
                                  )
                       .withColumn("sun_24_cat", psf.when(((psf.col("sun_24") >= 0) 
                                                           & (psf.col("sun_24") < 1)), catranges_su24[0])
                                                     .when(((psf.col("sun_24") >= 1) 
                                                            & (psf.col("sun_24") < 5)), catranges_su24[1])
                                                     .when((psf.col("sun_24") >= 5), catranges_su24[2])
                                  )
                        .withColumn("solar_24_cat", psf.when(((psf.col("solar_24") >= 0) 
                                                              & (psf.col("solar_24") < 1)), catranges_so24[0])
                                                     .when(((psf.col("solar_24") >= 1) 
                                                            & (psf.col("solar_24") < 4)), catranges_so24[1])
                                                     .when(((psf.col("solar_24") >= 4) 
                                                            & (psf.col("solar_24") < 9)), catranges_so24[2])
                                                     .when((psf.col("solar_24") >= 9), catranges_so24[3])
                                  )
                        .withColumn("days_after_rain_20mm_cat", psf.when(((psf.col("days_after_rain_20mm") >= 0) 
                                                                          & (psf.col("days_after_rain_20mm") < 1)), catranges_dar20[0])
                                                                    .when(((psf.col("days_after_rain_20mm") >= 1) 
                                                                                & (psf.col("days_after_rain_20mm") < 2)), catranges_dar20[1])
                                                                    .when(((psf.col("days_after_rain_20mm") >= 2) 
                                                                                & (psf.col("days_after_rain_20mm") < 3)), catranges_dar20[2])
                                                                    .when(((psf.col("days_after_rain_20mm") >= 3) 
                                                                                & (psf.col("days_after_rain_20mm") < 4)), catranges_dar20[3])
                                                                    .when((psf.col("days_after_rain_20mm") >= 4), catranges_dar20[4])
                                   )
                   .withColumn("locationId",psf.when(psf.col("siteName")=="Bayview",1)
                               .when(psf.col("siteName")=="Putney Park",2)
                              )
                   .withColumn("_DLCleansedZoneTimeStamp", psf.current_timestamp())
                   .select("locationId",
                           "siteName",
                           "timestamp",
                           "rain_24",
                           "rain_24_cat",
                           "rain_48",
                           "rain_48_cat",
                           "rain_72",
                           "rain_72_cat",
                           "rain_7d",
                           "rain_7d_cat",
                           "Rintensity",
                           "Rintensity_cat",
                           "Rduration",
                           "Rduration_cat",
                           "Rdistribution",
                           "Rdistribution_cat",
                           "days_after_rain_20mm",
                           "days_after_rain_20mm_cat",
                           "sun_24",
                           "sun_24_cat",
                           "solar_24",
                           "solar_24_cat",
                           "_DLCleansedZoneTimeStamp"
                          )
               
                )
#display(infer_input_cato)

# COMMAND ----------

# MAGIC %md # Save the table

# COMMAND ----------

spark.conf.set("c.catalog_name", ADS_DATABASE_CLEANSED)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create urbanplunge_water_quality_predictions table in cleansed layer
# MAGIC CREATE TABLE IF NOT EXISTS ${c.catalog_name}.urbanplunge.water_quality_features
# MAGIC (
# MAGIC locationId INT,
# MAGIC siteName STRING,
# MAGIC timestamp TIMESTAMP,
# MAGIC rain_24 DOUBLE,
# MAGIC rain_24_cat STRING,
# MAGIC rain_48 DOUBLE,
# MAGIC rain_48_cat STRING,
# MAGIC rain_72 DOUBLE,
# MAGIC rain_72_cat STRING,
# MAGIC rain_7d DOUBLE,
# MAGIC rain_7d_cat STRING,
# MAGIC Rintensity DOUBLE,
# MAGIC Rintensity_cat STRING,
# MAGIC Rduration DOUBLE,
# MAGIC Rduration_cat STRING,
# MAGIC Rdistribution DOUBLE,
# MAGIC Rdistribution_cat STRING,
# MAGIC days_after_rain_20mm DOUBLE,
# MAGIC days_after_rain_20mm_cat STRING,
# MAGIC sun_24 DOUBLE, 
# MAGIC sun_24_cat STRING,
# MAGIC solar_24 DOUBLE,
# MAGIC solar_24_cat STRING,
# MAGIC _DLCleansedZoneTimeStamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA 
# MAGIC
# MAGIC -- df_model_output.write.mode("append").insertInto('cleansed.urbanplunge_water_quality_predictions')

# COMMAND ----------

# #data inserted to the video metadata table
infer_input_cato.write.mode("append").insertInto(f"{spark.conf.get('c.catalog_name')}.urbanplunge.water_quality_features")
