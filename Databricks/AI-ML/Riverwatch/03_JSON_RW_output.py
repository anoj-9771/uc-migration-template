# Databricks notebook source
import json
import re
from pyspark.sql import functions as psf
from pyspark.sql import Window as W
from pyspark.sql import types as t
from pyspark.sql.types import IntegerType, StringType, DoubleType, StructType, StructField, FloatType
# import mlflow
from pyspark.sql.functions import struct
from pyspark.sql.functions import split
from datetime import datetime
import pandas as pd

# COMMAND ----------

def delete_multiple_element(outerlayer, indices):
    list_object=outerlayer['locations']
    indices = sorted(indices, reverse=True)
    for idx in indices:
        if idx < len(list_object):
            list_object.pop(idx)
    return outerlayer

# COMMAND ----------

#---------1
bom_weatherforecast_original = (spark.table("cleansed.bom_weatherforecast")
                      )
# display(bom_weatherforecast_original)
#---------2
bom_dailyweatherobservation_original = (spark.table("cleansed.bom_dailyweatherobservation_sydneyairport")
                                       )
# display(bom_dailyweatherobservation_original.orderBy("date",ascending=False))
#---------3
vw_beachwatch_info_original=(spark.table("cleansed.vw_beachwatch_pollution_weather_forecast")
                            )
# display(vw_beachwatch_info_original.orderBy(psf.col("updated"),ascending=False))
#---------4
rw_tide_temp_info_original=(spark.table("cleansed.bom_fortdenision_tide")
                           )
# display(rw_tide_temp_info_original)
#---------5
df_rwBN_water_quality = (spark.table("cleansed.urbanplunge_water_quality_predictions")
                        )
# display(df_rwBN_water_quality)

# COMMAND ----------

from datetime import datetime, timedelta
import time

dbutils.widgets.text(name="process_timestamp", defaultValue="2022-02-07T12:00:00.000", label="process_timestamp")
 
TIME_VARIABLE=dbutils.widgets.get("process_timestamp")

#--------------- open and prepare json file -----------
with open('/dbfs/mnt/blob-urbanplunge/RW_locations.json', 'r') as f:
    RW_locations = json.load(f)

with open('/dbfs/mnt/blob-urbanplunge/RW_icon_code.json', 'r') as f:
    RW_icon_code = json.load(f)
    
with open('/dbfs/mnt/blob-urbanplunge/RW_notice.json', 'r') as f:
    RW_notice = json.load(f)    

RW_header={
  "header": {
    "refresh_message": "Water Quality model using Sydney Water River Watch & NSW Government Beach Watch",
    "publish_time": "Model Generated Time in AEST",
    "publish_date": "Model Generated Date"
  }
}

# # =========Remove Inactive sites==================
removelist=[]
for index,location in enumerate(RW_locations['locations']):
    if location["status"] == "Inactive":
        removelist.append(index)

RW_locations_InactiveRevmoved=delete_multiple_element(RW_locations, removelist)

for index,location in enumerate(RW_locations_InactiveRevmoved['locations']):
#=========Active==================    
    if location["status"] == "Active":

        beachwatch_info = (vw_beachwatch_info_original
                           .where(psf.col("locationId") == location["locationId"])
                           .withColumn("BW_date",psf.to_date(psf.col("updated")))
                           .orderBy("BW_date",ascending=False)
                           .toPandas()
                          )
        Dawn_Fraser_Pool_tempinfo = (vw_beachwatch_info_original # this is for ocean temp only, the Dawnfraser ocean temprature is applied to all sites
                                     .where(psf.col("locationId")==3)
                                     .withColumn("BW_date",psf.to_date(psf.col("updated")))
                                     .orderBy("BW_date",ascending=False)
                                     .toPandas()
                                    )
        
        w = W.partitionBy("element_instance").orderBy("_start-time-utc")
        rw_Tide=(rw_tide_temp_info_original
                     .withColumnRenamed("element_time-local","startTimeLocal")
                     .withColumn("current_datetime",psf.to_timestamp(psf.lit(TIME_VARIABLE)))
                     .where(psf.col("startTimeLocal")>psf.col("current_datetime"))
                     .na.drop(subset=["element_VALUE","element_instance"])
                     .withColumn("high&lowTide", psf.row_number().over(w))
                     .where(psf.col("high&lowTide")==1) 
                     )
        rw_highTide= (rw_Tide
                     .where(psf.col("element_instance")=="high")
                     .toPandas()
                     )
        rw_lowTide= (rw_Tide
                     .where(psf.col("element_instance")=="low")
                     .toPandas()
                     )
    

        forecast_icon_airtemp = (bom_weatherforecast_original
                                 #---screen lcation and bom station----
                                 .where(psf.col("_type")=="location")
                                 .where(psf.col("_description")==location['BOM_station'])
                                 #---only look at column we need---
                                 .withColumn("available_days_data",psf.explode(psf.col("forecast-period")))
                                 .withColumn("_start-time-local",psf.to_date(psf.col("available_days_data")["_start-time-local"]))
                                 .withColumn("elements",psf.explode(psf.col("available_days_data")["element"]))
                                 .withColumn("types",psf.col("elements")["_type"])
                                 .withColumn("types_value",psf.col("elements")["_VALUE"])
                                )

        forecast_icon_code=(forecast_icon_airtemp
                           .where(psf.col("types")=="forecast_icon_code")
                            .select("types_value")
                            .toPandas()
                           )

        minairtemp=(forecast_icon_airtemp
                           .where(psf.col("types")=="air_temperature_minimum")
                            .select("types_value")
                            .toPandas()
                           )
        maxairtemp=(forecast_icon_airtemp
                           .where(psf.col("types")=="air_temperature_maximum")
                            .select("types_value")
                            .toPandas()
                           )

        forecast_description_text = (bom_weatherforecast_original
                                 #---screen lcation and bom station----
                                  .where(psf.col("_type")=="metropolitan")
                                  .where(psf.col("_description") == location['BOM_station'])
                                  #---only look at column we need---
                                  .withColumn("available_days_data",psf.explode(psf.col("forecast-period")))
                                  .withColumn("_start-time-local",psf.to_date(psf.col("available_days_data")["_start-time-local"]))
                                  .withColumn("IssueTime",psf.col("available_days_data")["_start-time-local"])
                                  .orderBy("_start-time-local")
                                  .withColumn("text",psf.explode(psf.col("available_days_data")["text"]))
                                  .withColumn("types",psf.col("text")["_type"])
                                  .where(psf.col("types")=="forecast")
                                  .withColumn("VALUE",psf.col("text")["_VALUE"])
                                  .toPandas()
                               )

        uv=(bom_weatherforecast_original
                              #---screen lcation and bom station----
                              .where(psf.col("_type")=="metropolitan")
                              .where(psf.col("_description") == location['BOM_station'])
                              #---only look at column we need---
                              .withColumn("available_days_data",psf.explode(psf.col("forecast-period")))
                              .withColumn("_start-time-local",psf.to_date(psf.col("available_days_data")["_start-time-local"]))
                              .withColumn("text",psf.explode(psf.col("available_days_data")["text"]))
                              .withColumn("types",psf.col("text")["_type"])
                              .where(psf.col("types")=="uv_alert")
                              .withColumn("VALUE",psf.col("text")["_VALUE"])
                              .withColumn("uv_interpret", psf.regexp_extract(psf.col("VALUE"), r"\[([^()]+)\]$", 1))
                              .withColumn("uv_value", psf.regexp_extract(psf.col("VALUE"), r"reach ([^()]+) \[", 1))
                              .orderBy("_start-time-local")
                              .na.drop(subset=["types","VALUE","uv_interpret","uv_value"])#remove all null rows in column "element_VALUE" and "element_instance" to avoid null is selected for tide info
                              .toPandas()
                                  )

        rain_wind=(bom_dailyweatherobservation_original
                         .sort(psf.desc("date"))
                         .withColumn("latest_wind_speed",psf.when(psf.isnull(psf.col("3pm_wind_speed_kmh")),psf.col("9am_wind_speed_kmh"))
                                                            .otherwise(psf.col("3pm_wind_speed_kmh"))
                                   )
                         .withColumn("latest_wind_dire",psf.when(psf.isnull(psf.col("3pm_wind_speed_kmh")),psf.col("9am_wind_direction"))
                                                            .otherwise(psf.col("3pm_wind_direction"))
                                   )
                         .na.drop(subset=["Rainfall_mm"])
                         .toPandas()
                  )

        #-----------obtain water quality and header info @ Bay View Park------------
        if location["source"] == "RiverWatch":
            Water_quality = (df_rwBN_water_quality
                       .where(psf.col("locationId") == location["locationId"])
                       .orderBy("_DLCleansedZoneTimeStamp", ascending=False)
                       .withColumn("split_DLCleansedZoneTimeStamp", psf.split(psf.col("_DLCleansedZoneTimeStamp"),'T'))
                       .withColumn("Date", psf.split(psf.col("split_DLCleansedZoneTimeStamp")[0],' ').getItem(0))
                       .withColumn("Time", psf.split(psf.col("split_DLCleansedZoneTimeStamp")[0],' ').getItem(1))
                       .toPandas()
                            )
            
            water_quality = Water_quality.waterQualityPredictionBeachwatch[0] #unlikely/possible/likely
            ocean_temp = str(Dawn_Fraser_Pool_tempinfo.oceanTemp[0]) 
            current_temp= str(Dawn_Fraser_Pool_tempinfo.airTemp[0]) 
            
            #-----------tide info----------------------------
            tidal_adjust=datetime.strptime(location["tidal_adjustment"], '+%H:%M').time()
            tidal_adjust_timedelt=timedelta(hours=tidal_adjust.hour, minutes=tidal_adjust.minute)

            highTideTime=datetime.strptime(str(rw_highTide.startTimeLocal[0].time()), '%H:%M:%S').time()
            highTideTime_timedelt=timedelta(hours=highTideTime.hour, minutes=highTideTime.minute)
            lowTideTime=datetime.strptime(str(rw_lowTide.startTimeLocal[0].time()), '%H:%M:%S').time()
            lowTideTime_timedelt=timedelta(hours=lowTideTime.hour, minutes=lowTideTime.minute)
            if location["tidal_adjustment"]=="No Tide":
                high_tide=str(highTideTime_timedelt) 
                low_tide=str(lowTideTime_timedelt) 
            else:
                high_tide=str(highTideTime_timedelt+tidal_adjust_timedelt)
                low_tide=str(lowTideTime_timedelt+tidal_adjust_timedelt)

            high_tide_height_m = str(rw_highTide.element_VALUE[0]) 
            low_tide_height_m = str(rw_lowTide.element_VALUE[0]) 

#             high_tide = "bom_fortdenision_tide info" # need data source
#             high_tide_height_m = "bom_fortdenision_tide info" # need data source
#             low_tide = "bom_fortdenision_tide info" # need data source
#             low_tide_height_m = "bom_fortdenision_tide info" # need data source
            
            #-----------obtain weather information ------------
            bom_station= location["BOM_station"]
            issue_time_local_tz=str(forecast_description_text.IssueTime[0])
            forecast_icon=forecast_icon_code.types_value[0] 
            forecast_text=forecast_description_text.VALUE[0]
            for icon in RW_icon_code["icon_meaning"]:
                if icon == forecast_icon: # icon here is string
                    forecast_precise = RW_icon_code["icon_meaning"][icon] 
            air_temp_min=str(minairtemp.types_value[0]) 
            air_temp_max=str(maxairtemp.types_value[0]) 
            rainfall_since9am = str(round(rain_wind.Rainfall_mm[0])) 
            windspeed_kmh = str(rain_wind.latest_wind_speed[0]) 
            wind_direction = rain_wind.latest_wind_dire[0]
            uv_forecast = str(uv.uv_value[0]) + "/11+" 
            uv_meaning = uv.uv_interpret[0] 
            

        elif location["source"] == "Beachwatch":
            water_quality = beachwatch_info.waterQuality[0] #unlikely/possible/likely
            #-----------tide info----------------------------
            tidal_adjust=datetime.strptime(location["tidal_adjustment"], '+%H:%M').time()
            tidal_adjust_timedelt=timedelta(hours=tidal_adjust.hour, minutes=tidal_adjust.minute)

            highTideTime=datetime.strptime(beachwatch_info.highTideTime[0], '%H:%M').time()
            highTideTime_timedelt=timedelta(hours=highTideTime.hour, minutes=highTideTime.minute)
            lowTideTime=datetime.strptime(beachwatch_info.lowTideTime[0], '%H:%M').time()
            lowTideTime_timedelt=timedelta(hours=lowTideTime.hour, minutes=lowTideTime.minute)

            if location["tidal_adjustment"]=="No Tide":
                high_tide=str(highTideTime_timedelt) 
                low_tide=str(lowTideTime_timedelt)
            else:
                high_tide=str(highTideTime_timedelt+tidal_adjust_timedelt)
                low_tide=str(lowTideTime_timedelt+tidal_adjust_timedelt) 

            high_tide_height_m = str(beachwatch_info.highTideMeters[0]) 
            low_tide_height_m = str(beachwatch_info.lowTideMeters[0]) 

            #-----------temp info-----------------------
            ocean_temp = str(beachwatch_info.oceanTemp[0]) 
            current_temp= str(beachwatch_info.airTemp[0]) 
            
            #-----------obtain weather information ------------
            bom_station= location["BOM_station"]
            issue_time_local_tz=str(forecast_description_text.IssueTime[0])
            forecast_icon=forecast_icon_code.types_value[0]  
            forecast_text=forecast_description_text.VALUE[0] 
            for icon in RW_icon_code["icon_meaning"]:
                if icon == forecast_icon: # icon here is string
                    forecast_precise = RW_icon_code["icon_meaning"][icon] 
            air_temp_min=str(minairtemp.types_value[0]) 
            air_temp_max=str(maxairtemp.types_value[0]) 
            rainfall_since9am = str(round(rain_wind.Rainfall_mm[0])) 
            windspeed_kmh = str(rain_wind.latest_wind_speed[0]) 
            wind_direction = rain_wind.latest_wind_dire[0] 
            uv_forecast = str(uv.uv_value[0]) + "/11+" 
            uv_meaning = uv.uv_interpret[0] 
        
        elif location["source"] == "Unmonitored":
            water_quality = "Unmonitored" #unlikely/possible/likely
            #-----------tide info----------------------------
#             tidal_adjust=datetime.strptime(location["tidal_adjustment"], '+%H:%M').time()
#             tidal_adjust_timedelt=timedelta(hours=tidal_adjust.hour, minutes=tidal_adjust.minute)

#             highTideTime=datetime.strptime(beachwatch_info.highTideTime[0], '%H:%M').time()
#             highTideTime_timedelt=timedelta(hours=highTideTime.hour, minutes=highTideTime.minute)
#             lowTideTime=datetime.strptime(beachwatch_info.lowTideTime[0], '%H:%M').time()
#             lowTideTime_timedelt=timedelta(hours=lowTideTime.hour, minutes=lowTideTime.minute)

#             if location["tidal_adjustment"]=="NoTide":
#                 high_tide=str(highTideTime_timedelt) # done
#                 low_tide=str(lowTideTime_timedelt) # done
#             else:
#                 high_tide=str(highTideTime_timedelt+tidal_adjust_timedelt) # done
#                 low_tide=str(lowTideTime_timedelt+tidal_adjust_timedelt) # done
            high_tide="Unmonitored" 
            low_tide="Unmonitored" 
#             print(low_tide)
            high_tide_height_m = "Unmonitored" 
            low_tide_height_m = "Unmonitored" 
            #-----------temp info-----------------------
            ocean_temp = str(Dawn_Fraser_Pool_tempinfo.oceanTemp[0]) 
            current_temp= str(Dawn_Fraser_Pool_tempinfo.airTemp[0]) 
        
            bom_station= location["BOM_station"]
            issue_time_local_tz=str(forecast_description_text.IssueTime[0])
            forecast_icon=forecast_icon_code.types_value[0]  
            forecast_text=forecast_description_text.VALUE[0] 
            for icon in RW_icon_code["icon_meaning"]:
                if icon == forecast_icon: # icon here is string
                    forecast_precise = RW_icon_code["icon_meaning"][icon] 
            air_temp_min=str(minairtemp.types_value[0]) 
            air_temp_max=str(maxairtemp.types_value[0]) 
            rainfall_since9am = str(round(rain_wind.Rainfall_mm[0]))
            windspeed_kmh = str(rain_wind.latest_wind_speed[0])
            wind_direction = rain_wind.latest_wind_dire[0]
            uv_forecast = str(uv.uv_value[0]) + "/11+"
            uv_meaning = uv.uv_interpret[0]
            
        #-----------append water quality to JSON at the level of location name @ Bay View Park------------
        json_water_quality = {"water_quality": {"pollution": water_quality}}
        #-----------append weather to JSON at the level of location name @ Bay View Park------------
        json_weather={"weather": {"bom_station": bom_station,
                                 "issue_time":issue_time_local_tz, 
                                 "forecast_icon": forecast_icon,
                                 "forecast_text": forecast_text,
                                 "forecast_precise":forecast_precise,
                                 "air_temp_min": air_temp_min,
                                 "air_temp_max": air_temp_max,
                                 "ocean_temp": ocean_temp,
                                 "current_temp": current_temp,
                                 "high_tide": high_tide,
                                 "high_tide_height_m": high_tide_height_m,
                                 "low_tide": low_tide,
                                 "low_tide_height_m": low_tide_height_m,
                                 "rainfall_since9am": rainfall_since9am,
                                 "windspeed_kmh": windspeed_kmh,
                                 "wind_direction": wind_direction,
                                 "uv_forecast": uv_forecast,
                                 "uv_meaning": uv_meaning
                                 }
                     }
    #         print(json_weather)
        #-------------------------update/add water quality and weather information in RW_locations.json-------------------------
        RW_locations["locations"][index].update(json_water_quality)# appendting water_quality to JSON
        RW_locations["locations"][index].update(json_weather)# appendting weather to JSON
        del RW_locations["locations"][index]["BOM_station"]
        del RW_locations["locations"][index]["tidal_adjustment"]
        del RW_locations["locations"][index]["status"]
    elif location["status"] == "Inactive":
        #--------if inactive remove the item of this postion from the JSON list-------------
        RW_locations['locations'].pop(index)
        print(str(index))
        #--------if inactive do nothing-------------
        pass
    
#------------------------------update header file--------------------------
CURRENT_DATETIME=datetime.now() # set current timestamp (using datetime function) for test
# print(CURRENT_MODEL_RUNTIME)
Publish_time= CURRENT_DATETIME.time()  # refresh message in the header file
Publish_date= CURRENT_DATETIME.date()  # refresh message in the header file
refresh_message={"refresh_message": "Water Quality model using Sydney Water River Watch & NSW Government Beach Watch"}# refresh message in the header file
publish_time={"publish_time": f"{Publish_time}"}  # publish_time in the header file
publish_date={"publish_date": f"{Publish_date}"}  # publish_date in the header file
RW_header["header"].update(refresh_message)
RW_header["header"].update(publish_time)
RW_header["header"].update(publish_date)
        
#---------------union all json file together to make the RW_output.json file---------------------
RW_output=dict(list(RW_notice.items()) + list(RW_header.items()) + list(RW_locations.items()))
#---------------convert RW_output.json to the indent structure for checking easily
# RW_output = json.dumps(RW_output, indent = 4)

# print(json.dumps(RW_output, indent = 4))
# print(json.dumps(RW_output))

# COMMAND ----------

with open('/dbfs/mnt/blob-urbanplunge/RW_output.json', 'w') as f:
    json.dump(RW_output, f)

# COMMAND ----------

print(json.dumps(RW_output, indent = 4))

# COMMAND ----------


