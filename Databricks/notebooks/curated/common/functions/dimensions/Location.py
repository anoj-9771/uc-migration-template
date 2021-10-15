# Databricks notebook source
###########################################################################################################################
# Function: GetCommonLocation
#  GETS Location DIMENSION 
# Returns:
#  Dataframe of transformed Location
#############################################################################################################################
# Method
# 1.Create Function
# 2.Load Cleansed layer table data into dataframe and transform
# 3.JOIN TABLES
# 4.UNION TABLES
# 5.SELECT / TRANSFORM
#############################################################################################################################
#1.Create Function
def getLocation():
  
  #spark.udf.register("TidyCase", GeneralToTidyCase)  
  
  #DimLocation
  #2.Load Cleansed layer table data into dataframe
  HydraLocationDf = spark.sql(f"select propertyNumber as locationID, first(propertyAddress) as formattedAddress, cast(null as string) as streetName, cast(null as string) as streetType, \
                                     first(LGA) as LGA, first(suburb) as suburb, 'NSW' as state, first(latitude) as latitude, first(longitude) as longitude \
                                     from {ADS_DATABASE_CLEANSED}.hydra_tlotparcel \
                                     where propertyNumber is not null \
                                     group by propertyNumber")
  
  dummyDimRecDf = spark.createDataFrame([(-1, "Unknown")],["locationID", "formattedAddress"])
  
  #3.JOIN TABLES  
  
  #4.UNION TABLES
  HydraLocationDf = HydraLocationDf.unionByName(dummyDimRecDf, allowMissingColumns = True)
  
  #5.SELECT / TRANSFORM
  HydraLocationDf = HydraLocationDf.selectExpr( \
	 "LocationID" \
    ,"formattedAddress" \
    ,"streetName" \
    ,"StreetType" \
    ,"LGA" \
    ,"suburb" \
    ,"state" \
    ,"CAST(latitude AS DECIMAL(9,6)) as latitude" \
    ,"CAST(longitude AS DECIMAL(9,6)) as longitude"                   
  )
  display(HydraLocationDf)
  #6.Apply schema definition
  newSchema = StructType([
                            StructField("LocationID", IntegerType(), False),
                            StructField("formattedAddress", StringType(), True),
                            StructField("streetName", StringType(), True),
                            StructField("StreetType", StringType(), True),
                            StructField("LGA", StringType(), True),
                            StructField("suburb", StringType(), True),
                            StructField("state", StringType(), True),
                            StructField("latitude", DecimalType(9,6), True),
                            StructField("longitude", DecimalType(9,6), True)
                      ])
  
  HydraLocationDf = spark.createDataFrame(HydraLocationDf.rdd, schema=newSchema)
  return HydraLocationDf

# COMMAND ----------

#df.count()

# COMMAND ----------

#%sql
#select count(distinct propertynumber) from cleansed.t_hydra_tlotparcel

# COMMAND ----------


