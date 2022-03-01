# Databricks notebook source
##################################################################
#Master Notebook
#1.Include all util user function for the notebook
#2.Include all dimension/bridge/fact user function for the notebook
#3.Define and get Widgets/Parameters
#4.Spark Config
#5.Function: Load data into Curated delta table
#6.Function: Load Dimensions/Bridge/Facts
#7.Function: Create stage and curated database if not exist
#8.Flag Dimension/Bridge/Fact load
#9.Function: Main - ETL
#10.Call Main function
#11.Exit Notebook
##################################################################

# COMMAND ----------

# DBTITLE 1,1. Include all util user functions for this notebook
# MAGIC %run ../../includes/include-all-util

# COMMAND ----------

# DBTITLE 1,3. Define and get Widgets/Parameters
#Set Parameters
dbutils.widgets.removeAll()

dbutils.widgets.text("Start_Date","")
dbutils.widgets.text("End_Date","")

#Get Parameters
start_date = dbutils.widgets.get("Start_Date")
end_date = dbutils.widgets.get("End_Date")

params = {"start_date": start_date, "end_date": end_date}

#DEFAULT IF ITS BLANK
start_date = "2000-01-01" if not start_date else start_date
end_date = "2099-12-31" if not end_date else end_date

#Print Date Range
print(f"Start_Date = {start_date}| End_Date = {end_date}")

# COMMAND ----------

# DBTITLE 1,4. Spark Config
# When set to true Spark SQL will automatically select a compression codec for each column based on statistics of the data.
spark.conf.set("spark.sql.inMemoryColumnarStorage.compressed",True)

# Limit of total size of serialized results of all partitions for each Spark action (e.g. collect) in bytes. Should be at least 1M, or 0 for unlimited. Jobs will be aborted if the total size is above this limit. Having a high limit may cause out-of-memory errors in driver (depends on spark.driver.memory and memory overhead of objects in JVM). Setting a proper limit can protect the driver from out-of-memory errors.
#spark.conf.set("spark.driver.maxResultSize",0)

#Configures the maximum size in bytes for a table that will be broadcast to all worker nodes when performing a join. By setting this value to -1 broadcasting can be disabled.
# spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 0)

# COMMAND ----------

# DBTITLE 1,Test - Remove it
# #Remove - For testing
#
# spark.sql("DROP TABLE raw.iot_iot_sw_telemetry_alarm_")
# spark.sql("DROP TABLE raw.iot_iot_sw_telemetry_alarm_")

# spark.sql("DELETE FROM curated.dimweather")
# spark.sql("DROP TABLE curated.dimweather")

# spark.sql("DELETE FROM stage.dimweather")
# spark.sql("DROP TABLE stage.dimweather")

# df = spark.sql("select * from cleansed.stg_sapisu_0uc_connobj_attr_2 where haus = 6206050")
# df = spark.sql("select * from cleansed.t_sapisu_0uc_connobj_attr_2 where propertyNumber = 6206050")


# spark.sql("update curated.dimproperty set propertyType = 'Single Dwelling', _recordCurrent = 1 where propertyid = 3100038") #2357919
# spark.sql("update curated.dimproperty set propertyType = 'test12', _RecordStart = '2021-10-09T09:59:24.000+0000' where propertyid = 3100005") # Single Dwelling
# df = spark.sql("select * from curated.dimproperty where propertyid = 3100005") #2357919
# display(df)
# spark.sql("update curated.dimproperty set  _RecordStart = '2021-10-08T09:59:24.000+0000' where dimpropertysk = 895312") # Single Dwelling
# df = spark.sql("select * from curated.dimproperty where dimpropertysk = 895312") 
# display(df)

# spark.sql("TRUNCATE TABLE raw.iot_iot_sw_telemetry_alarm")
# df = spark.sql("select count(*) from raw.iot_iot_sw_telemetry_alarm") 
# display(df)


# COMMAND ----------

# DBTITLE 1,5. Function: Weather
###########################################################################################################################
# Function: getWeather
#  GETS Weather DIMENSION 
# Returns:
#  Dataframe of transformed Property
#############################################################################################################################
# Method
# 1.Create Function
# 2.Load Cleansed layer table data into dataframe and transform
# 3.JOIN TABLES
# 4.UNION TABLES
# 5.SELECT / TRANSFORM
#############################################################################################################################
#1.Create Function
def getWeather():

    #spark.udf.register("TidyCase", GeneralToTidyCase)  

    #dimWeather
    #2.Load Cleansed layer table data into dataframe
    dfBom715 = spark.sql(f"select y,\
                                  n2,\
                                  x,\
                                  start_time,\
                                  valid_time,\
                                  proj,\
                                  y_bounds,\
                                  x_bounds,\
                                  precipitation, \
                                  'BoM' as sourceSystemCode \
                                  from {ADS_DATABASE_CLEANSED}.bom_bom715 ")

    #Add date and hour column to df
    dfBom715 = dfBom715.withColumn("date_only", to_date(col("start_time")))\
                       .withColumn("hour_only", hour(col("start_time")))
    #Filtering time between - 9am to mid-night as per requirement
    dfBom715 = dfBom715.filter(dfBom715.hour_only.between(9,23))

    #Aggregation - apply wet weather logi (sum of precipitation > 10 in a day from 9am)
    gdf = dfBom715.groupBy("y", "n2", "x", "proj", "y_bounds", "x_bounds", "date_only")  
    dfBom715 = gdf.agg(sum(col("precipitation")).alias("sum_precipitation"))
    
    #Adding is_wet_weather column to df
    dfBom715 = dfBom715.withColumn("is_wet_weather", when((col("sum_precipitation") >= 10),True).otherwise(False))

    #Dummy Record to be added to Meter Dimension


    #3.JOIN TABLES  
    #4.UNION TABLES

    #5.SELECT / TRANSFORM
    
    #6.Apply schema definition
    df = dfBom715
   
    return df


# COMMAND ----------

# DBTITLE 1,6. Function: Load data into curated delta table
def TemplateEtl(df : object, entity, AddSK = True):
    rawEntity = entity
    entity = GeneralToPascalCase(rawEntity)
    LogEtl(f"Starting {entity}.")
    
    source_group = "bom715data"
    target_table = entity
    
    #Save Data frame into curated delta table (final)
    DeltaSaveDataframeDirect(df, source_group, target_table, ADS_DATABASE_CURATED, ADS_CONTAINER_CURATED, "overwrite", "")

    LogEtl(f"Finished {entity}.")

# COMMAND ----------

# DBTITLE 1,6.1 Function: Load tables
#Call BusinessPartner function to load DimBusinessPartnerGroup
def weather():
    TemplateEtl(df=getWeather(), 
             entity="weather",
             AddSK=False
            )  


# #Call Date function to load DimDate
# def date():
#     TemplateEtl(df=getDate(), 
#              entity="dimDate", 
#              businessKey="calendarDate",
#              AddSK=True
#             )


# COMMAND ----------

# DBTITLE 1,7. Function: Create stage and curated database if not exist
def DatabaseChanges():
  #CREATE stage AND curated DATABASES IS NOT PRESENT
  spark.sql("CREATE DATABASE IF NOT EXISTS stage")
  spark.sql("CREATE DATABASE IF NOT EXISTS curated")  


# COMMAND ----------

# DBTITLE 1,8. Function: Main - ETL
def Main():
    DatabaseChanges()
    
    #==============
    # Load Curated tables
    #==============
    LogEtl("Start: Cureated table - Weather")
    weather()
    LogEtl("End: Cureated table - Weather.")    


# COMMAND ----------

# DBTITLE 1,9. Call Main function
Main()

# COMMAND ----------

# DBTITLE 1,11. Exit Notebook
dbutils.notebook.exit("1")
