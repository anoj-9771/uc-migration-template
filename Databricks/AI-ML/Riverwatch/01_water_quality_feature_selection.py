# Databricks notebook source
# MAGIC %md # Import libraries

# COMMAND ----------

# MAGIC %run /build/includes/global-variables-python

# COMMAND ----------

dbutils.widgets.text(name="current_model_runtime", defaultValue="2022-02-10T08:33:00.000", label="current_model_runtime")
dbutils.widgets.text(name="last_model_runtime", defaultValue="2022-02-09T08:00:00.000", label="current_model_runtime")

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

try: 
    df_hierarchy_cnfgn = spark.table("cleansed.iicats_hierarchy_cnfgn").alias("hcnfg")
    print("IICATS Hierarchy Config loaded from cleansed")
except:
        try:
            df_hierarchy_cnfgn = spark.table("datalab.iicats_hierarchy_cnfgn_riverwatch_2022").alias("hcnfg")
            print("IICATS Hierarchy Config loaded from datalab")
        except ValueError:
                print("Cleansed & Datalab tables for IICATS Hierarchy Config do not exist.")
                
try: 
    df_time_series_values_cnfgn = spark.table("cleansed.iicats_tsv_point_cnfgn").alias("tsvptcnfg")
    print("IICATS TSV Config loaded from cleansed")
except:
        try:
            df_time_series_values_cnfgn = spark.table("datalab.iicats_tsv_point_cnfgn_riverwatch_2022").alias("tsvptcnfg")
            print("IICATS TSV Config loaded from datalab")
        except ValueError:
                print("Cleansed & Datalab tables for IICATS TSV Config do not exist.") 
                
try: 
    df_time_series_values = spark.table("cleansed.iicats_tsv").alias("tsv")
    print("IICATS TSV loaded from cleansed")
except:
        try:
            df_time_series_values = spark.table("datalab.iicats_tsv_riverwatch_2022").alias("tsv")
            print("IICATS TSV loaded from datalab")
        except ValueError:
                print("Cleansed & Datalab tables for IICATS TSV do not exist.")    

try: 
    df_sun=spark.table("cleansed.bom_dailyweatherobservation_sydneyairport")
    print("BoM Weather Observations loaded from cleansed")
except:
        try:
            df_sun=spark.table("cleansed.bom_dailyweatherobservation_sydneyairport")
            print("BoM Weather Observations loaded from datalab")
        except ValueError:
                print("Cleansed & Datalab tables for BoM Weather Observations do not exist.")  
                
try: 
    df_solar=spark.table("cleansed.bom_dailyclimatedata_sydneyairport")
    print("BoM Climate Data loaded from cleansed")
except:
        try:
            df_solar=spark.table("datalab.solar_exposure_2022")
            print("BoM Climate Data loaded from datalab")
        except ValueError:
                print("Cleansed & Datalab tables for Climate Data do not exist.")   

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
                          .withColumn("epoch_LAST_RUNTIME",psf.unix_timestamp(psf.date_trunc("hour",psf.lit(LAST_MODEL_RUNTIME)).cast("timestamp")))
                          .withColumn("epoch_CURRENT_RUNTIME",psf.unix_timestamp(psf.date_trunc("hour",psf.lit(CURRENT_MODEL_RUNTIME)).cast("timestamp")))
                          .withColumn("epoch_TSV_AEST_DT",psf.unix_timestamp(psf.col("TSV_AEST_DT")))
                          .where(psf.col("epoch_TSV_AEST_DT")<=psf.col("epoch_CURRENT_RUNTIME"))
                          .where(psf.col("epoch_TSV_AEST_DT")>=(psf.col("epoch_LAST_RUNTIME")-EPOCH_TIMESTAMP_8d))
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

df_time_series_values = (df_time_series_values
                         .distinct()
                         .withColumn("timestamp", psf.unix_timestamp(psf.col("TSV_AEST_DT")) - psf.unix_timestamp(psf.col("TSV_AEST_DT"))%3600)
                         .groupBy("CDB_OBJ_ID", "timestamp")
                         .agg(psf.sum(psf.col("TSV_RSLT_VAL")).alias("TSV_RSLT_VAL"))
                         .alias("tsv")
                         .orderBy("timestamp")
#                          .where(psf.col("CDB_OBJ_ID")==557525)
                        )

# COMMAND ----------

# MAGIC %md ## Function Get hourly interval (from earliest to latest)

# COMMAND ----------

def getHourlyIntervals(first_timestamp, last_timestamp, secs_interval):
    return list(range(first_timestamp,last_timestamp, secs_interval))

getHourlyIntervalsUDF = psf.udf(lambda a,b,c: getHourlyIntervals(a,b,c), t.ArrayType(t.IntegerType()))

# COMMAND ----------

# MAGIC %md ## Join df_hierarchy_cnfgn and df_time_series_values_cnfgn

# COMMAND ----------

df_iicats_rainfall = (df_hierarchy_cnfgn
                      .where((psf.col("SITE_CD") == "GG0022") |
                             (psf.col("SITE_CD") == "GG0064") |
                             (psf.col("SITE_CD") == "GG0020") |
                             (psf.col("SITE_CD") == "GG0008") |
                             (psf.col("SITE_CD") == "GG0019") |
                             (psf.col("SITE_CD") == "GG0047") |
                             (psf.col("SITE_CD") == "GG0016")
                            )
                      
                      .where(psf.col("OBJ_NM") == "Rainfall 15M Total")
                      .orderBy(psf.col("EFF_FROM_DT").desc())
                      .join(df_time_series_values_cnfgn,
                            on=df_hierarchy_cnfgn.CDB_OBJ_ID==df_time_series_values_cnfgn.CDB_OBJ_ID,
                            how='left'
                           )
                      .groupBy("hcnfg.CDB_OBJ_ID", "SITE_CD", "SITE_NM",  "tsvptcnfg.CDB_OBJ_ID", "PNT_CDB_OBJ_ID", "TM_BASE_CD", "STAT_TYP_CD")
                      .agg(psf.max(psf.col("tsvptcnfg.HT_CRT_DT")).alias("HT_CRT_DT"))
                      .where(psf.col("TM_BASE_CD") == 15)
                      .where(psf.col("STAT_TYP_CD") == "SN")
                      .join(df_time_series_values,
                            on=df_time_series_values_cnfgn.PNT_CDB_OBJ_ID==df_time_series_values.CDB_OBJ_ID,
                            how='left'
                           )
                       )

# COMMAND ----------

# MAGIC %md ## Get hourly interval per site

# COMMAND ----------

window = W.partitionBy("SITE_NM").orderBy(psf.unix_timestamp("timestamp"))

print(LAST_MODEL_RUNTIME)
df = (df_iicats_rainfall
      .groupBy("SITE_NM", "PNT_CDB_OBJ_ID")
      .agg(psf.min(psf.col("timestamp")).alias("first_timestamp"),
           psf.max(psf.col("timestamp")).alias("last_timestamp")
          )
       )

# COMMAND ----------

# MAGIC %md # Preprocess data to form model input

# COMMAND ----------

# MAGIC %md ## Create reference timestamp for filling missing hourly data

# COMMAND ----------

realtime_ref  = (df
        .withColumn("hourly_timestamps", getHourlyIntervalsUDF(psf.col("first_timestamp"), psf.col("last_timestamp"), psf.lit(3600)))
        .withColumn("epoch_timestamp", psf.explode(psf.col("hourly_timestamps")))
        .withColumn("timestamp", psf.to_timestamp(psf.from_unixtime(psf.col("epoch_timestamp"))))
        .select("SITE_NM", "PNT_CDB_OBJ_ID", "epoch_timestamp","timestamp")
         .alias("ref")
       )

# COMMAND ----------

# MAGIC %md ## Fill missing hourly time slot for each site

# COMMAND ----------

realtime_missing_hourly_filled=(realtime_ref
        .join(df_time_series_values,
              on=((psf.col("ref.epoch_timestamp")==psf.col("tsv.timestamp")) &
                  (psf.col("ref.PNT_CDB_OBJ_ID")==psf.col("tsv.CDB_OBJ_ID"))
                 ),
              how='left'
             )
      .withColumn("TSV_RSLT_VAL", psf.when(psf.col("TSV_RSLT_VAL").isNull(), 0).otherwise(psf.col("TSV_RSLT_VAL")))
       )

# COMMAND ----------

# MAGIC %md ## Allocate gauges geospatially related to swim sites

# COMMAND ----------

swim_site_specific_rainfall = (realtime_missing_hourly_filled
                               .withColumn("siteName", 
                                           psf.when(((psf.col("SITE_NM") == "GG0019 CONCORD") |
                                                     (psf.col("SITE_NM") == "GG0020 FIVEDOCK") |
                                                     (psf.col("SITE_NM") == "GG0064 GLADESVILLE") 
                                                    ), "Bayview"
                                                   )
                                               .when(((psf.col("SITE_NM") == "GG0047 RYDE") |
                                                      (psf.col("SITE_NM") == "GG0016 HOMEBUSH") |
                                                      (psf.col("SITE_NM") == "GG0064 GLADESVILLE") | 
                                                      (psf.col("SITE_NM") == "GG0019 CONCORD")
                                                     ), "Putney Park"
                                                    )
                                                .otherwise(None)
                                          )
                               .where(psf.col("siteName").isNotNull())
                              )

# COMMAND ----------

# MAGIC %md ## Factors 1. rain 24

# COMMAND ----------

w24=W.orderBy("epoch_timestamp").rowsBetween(RAIN_24_FILTER,-1)

rain_24=(swim_site_specific_rainfall
                .withColumn("date", psf.to_date(psf.col("ref.timestamp")))
                .groupBy("date", "epoch_timestamp", "siteName")
                .mean("TSV_RSLT_VAL")
                .withColumn("rain_24", psf.sum(psf.col("avg(TSV_RSLT_VAL)")).over(w24))
#                 .withColumn("timestamp", psf.to_timestamp(psf.from_unixtime(psf.col("epoch_timestamp"))))
                .orderBy("epoch_timestamp")
                .withColumn("epoch_LAST_RUNTIME",psf.unix_timestamp(psf.lit(LAST_MODEL_RUNTIME).cast("timestamp")))
                .where(psf.col("epoch_timestamp")>=psf.col("epoch_LAST_RUNTIME"))
                .alias("past_rain_hours")
        )

# COMMAND ----------

# MAGIC %md ## Factors 2. rain 48

# COMMAND ----------

w48=W.orderBy("epoch_timestamp").rowsBetween(RAIN_48_FILTER,-1)

rain_48=(swim_site_specific_rainfall
       .withColumn("date", psf.to_date(psf.col("ref.timestamp")))
                .groupBy("date", "epoch_timestamp", "siteName")
                .mean("TSV_RSLT_VAL")
                .withColumn("rain_48", psf.sum(psf.col("avg(TSV_RSLT_VAL)")).over(w48))
#                 .withColumn("timestamp", psf.to_timestamp(psf.from_unixtime(psf.col("epoch_timestamp"))))
               .orderBy("epoch_timestamp")
                  .alias("past_rain_hours")
        )

# COMMAND ----------

# MAGIC %md ## Factors 3. rain 72

# COMMAND ----------

w72=W.orderBy("epoch_timestamp").rowsBetween(RAIN_72_FILTER,-1)

rain_72=(swim_site_specific_rainfall
       .withColumn("date", psf.to_date(psf.col("ref.timestamp")))
                .groupBy("date", "epoch_timestamp", "siteName")
                .mean("TSV_RSLT_VAL")
                .withColumn("rain_72", psf.sum(psf.col("avg(TSV_RSLT_VAL)")).over(w72))
#                 .withColumn("timestamp", psf.to_timestamp(psf.from_unixtime(psf.col("epoch_timestamp"))))
               .orderBy("epoch_timestamp")
                  .alias("past_rain_hours")
      )

# COMMAND ----------

# MAGIC %md ## Factors 4. rain 7d

# COMMAND ----------

w7d=W.orderBy("epoch_timestamp").rowsBetween(RAIN_7d_FILTER,-1)

rain_7d=(swim_site_specific_rainfall
       .withColumn("date", psf.to_date(psf.col("ref.timestamp")))
                .groupBy("date", "epoch_timestamp", "siteName")
                .mean("TSV_RSLT_VAL")
                .withColumn("rain_7d", psf.sum(psf.col("avg(TSV_RSLT_VAL)")).over(w7d))
#                 .withColumn("timestamp", psf.to_timestamp(psf.from_unixtime(psf.col("epoch_timestamp"))))
               .orderBy("epoch_timestamp")
                  .alias("past_rain_hours")
      )

# COMMAND ----------

# MAGIC %md ## Factors 5. rain intensity

# COMMAND ----------

w3=W.orderBy("epoch_timestamp").rowsBetween(RAIN_3_FILTER,-1)
w48=W.orderBy("epoch_timestamp").rowsBetween(RAIN_48_FILTER,-1)

Rintensity=(swim_site_specific_rainfall
       .withColumn("date", psf.to_date(psf.col("ref.timestamp")))
                .groupBy("date", "epoch_timestamp", "siteName")
                .mean("TSV_RSLT_VAL")
                .withColumn("rain_int", psf.mean(psf.col("avg(TSV_RSLT_VAL)")).over(w3))
                .withColumn("Rintensity", psf.max(psf.col("rain_int")).over(w48))
#                 .withColumn("timestamp", psf.to_timestamp(psf.from_unixtime(psf.col("epoch_timestamp"))))
               .orderBy("epoch_timestamp")
                  .alias("past_rain_hours")
          )

# COMMAND ----------

# MAGIC %md ## Factors 6. rain duration

# COMMAND ----------

w48=W.orderBy("epoch_timestamp").rowsBetween(RAIN_48_FILTER,-1)

Rduration=(swim_site_specific_rainfall
       .withColumn("date", psf.to_date(psf.col("ref.timestamp")))
                .groupBy("date", "epoch_timestamp", "siteName")
                .mean("TSV_RSLT_VAL")
                .withColumn("rain_dur", psf.when(psf.col('avg(TSV_RSLT_VAL)') >= 2., 1).otherwise(0))
                .withColumn("Rduration", psf.sum(psf.col("rain_dur")).over(w48))
#                 .withColumn("timestamp", psf.to_timestamp(psf.from_unixtime(psf.col("epoch_timestamp"))))
               .orderBy("epoch_timestamp")
                  .alias("past_rain_hours")
      )

# COMMAND ----------

# MAGIC %md ## Factors 7. rain distribution

# COMMAND ----------

w48partNM = W.partitionBy("SITE_NM").orderBy("epoch_timestamp").rowsBetween(RAIN_48_FILTER, -1)

Rdistribution = (swim_site_specific_rainfall
                    .withColumn("rain_48_ES",psf.sum(psf.col("TSV_RSLT_VAL")).over(w48partNM))
                    .withColumn("flag:rain_48>=2mm",psf.when(psf.col("rain_48_ES") >= 2., 1).otherwise(0))
                    .groupBy("epoch_timestamp","siteName")
                    .sum("flag:rain_48>=2mm")
                    .withColumnRenamed("sum(flag:rain_48>=2mm)", "Rdistribution")
                    .withColumn("timestamp", psf.to_timestamp(psf.from_unixtime(psf.col("epoch_timestamp"))))
                    .orderBy("epoch_timestamp")
                    .select("epoch_timestamp","Rdistribution","siteName")
                   )

# COMMAND ----------

# MAGIC %md ## Factors 8. days_after_rain_20mm (24 hours daily with floating days)

# COMMAND ----------

# The days between the water quality sample and the 'first preceding' rain event must have had less then 20mm to have been counted.
daily_seconds=3600*24
wdaily = W.orderBy("epoch_timestamp")
days_after_rain_20mm = (rain_24
                     .withColumnRenamed("rain_24", "rain_daily")
                     .withColumn("timestamp", psf.to_timestamp(psf.from_unixtime(psf.col("epoch_timestamp"))))
                     .orderBy("epoch_timestamp")
                     .withColumn('epoch_day_rain_20mm', psf.when(psf.col('rain_daily')>=20, psf.col('epoch_timestamp')))
                     .withColumn('preceding_epoch_day_rain_20mm', psf.last('epoch_day_rain_20mm', ignorenulls=True).over(wdaily.rowsBetween(W.unboundedPreceding, -1)))
                     .withColumn("days_after_rain_20mm",(psf.col("epoch_timestamp")-psf.col("preceding_epoch_day_rain_20mm"))/daily_seconds)
                     .alias("drydays20mm")
                     .select("timestamp","epoch_timestamp", "siteName", "days_after_rain_20mm")
                      )
display(days_after_rain_20mm)

# COMMAND ----------

# MAGIC %md ## Factors 9. sun_24

# COMMAND ----------

win = W.orderBy("date")
# display(df_sun)
sun_24=(df_sun
        .dropDuplicates(["Date"])
        .withColumnRenamed("Date", "date")
        .withColumn("sun_24", psf.lag("Sunshine_hours",1).over(win).cast("double"))
        .select("date", "Sunshine_hours", "sun_24",)
        
       )
display(sun_24)

# COMMAND ----------

# MAGIC %md ## Factors 10. solar_24

# COMMAND ----------

wsolor24 = W.partitionBy("Bureau of Meteorology station number").orderBy("date")

solar_24=(df_solar
        .where(psf.col("Bureau of Meteorology station number")==66037) #66037 is the gauge number of Sydney Airport
        .withColumn("date", psf.date_format(psf.make_date("Year","Month","Day"), 'y-M-d').alias('date'))
        .withColumn("solar_24", psf.lag("Daily global solar exposure (MJ/m*m)",1).over(wsolor24).cast("double"))
        .select("date", "Daily global solar exposure (MJ/m*m)", "solar_24")
       )
display(solar_24)

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
                       .join(sun_24,
                             on="date",
                             how="left")
                       .join(solar_24,
                             on="date",
                             how="left")
                       .withColumn("timestamp", psf.to_timestamp(psf.from_unixtime(psf.col("epoch_timestamp"))))
                       .orderBy("epoch_timestamp")
                       .select("epoch_timestamp","siteName", "timestamp","rain_24","rain_48","rain_72","rain_7d"
                               ,"Rintensity","Rduration","Rdistribution","days_after_rain_20mm","sun_24","solar_24")
                       .na.fill(value=0,subset=["rain_24","rain_48","rain_72","rain_7d"
                               ,"Rintensity","Rduration","Rdistribution","days_after_rain_20mm"])
                        .where(psf.col("timestamp") == (psf.floor(psf.unix_timestamp(psf.lit(CURRENT_MODEL_RUNTIME), "yyyy-MM-dd'T'hh:mm:ss.SSS")/3600)*3600).cast("timestamp") - psf.expr('INTERVAL 1 HOURS'))
                       )
display(model_realtime_input)

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
display(infer_input_cato)

# COMMAND ----------

# MAGIC %md # Save the table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create urbanplunge_water_quality_predictions table in cleansed layer
# MAGIC CREATE TABLE IF NOT EXISTS cleansed.urbanplunge_water_quality_features
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
# MAGIC LOCATION 'dbfs:/mnt/datalake-cleansed/urbanplunge/urbanplunge_water_quality_features'
# MAGIC 
# MAGIC -- df_model_output.write.mode("append").insertInto('cleansed.urbanplunge_water_quality_predictions')

# COMMAND ----------

#data inserted to the video metadata table
infer_input_cato.write.mode("append").insertInto('cleansed.urbanplunge_water_quality_features')
