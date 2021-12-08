# Databricks notebook source
# MAGIC %sql
# MAGIC select * from cleansed.hydra_tlotparcel

# COMMAND ----------

###########################################################################################################################
# Function: getLocation
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
    #DimLocation
    #2.Load Cleansed layer table data into dataframe
    #collect parent properties and then parents of child properties so you get the parent address against the child property
    
    HydraLocationDf = spark.sql(f"select distinct b.architecturalObjectNumber as locationID, upper(trim(trim(coalesce(c.houseNumber2,'')||' '||coalesce(c.houseNumber1,''))||' '||trim(c.streetName||' '||coalesce(c.streetLine1,''))||' '||coalesce(c.streetLine2,''))|| \
                                     ', '||c.cityName||' NSW '||c.postCode)  as formattedAddress, upper(c.streetName) as streetName, upper(trim(coalesce(streetLine1,'')||' '||coalesce(streetLine2,''))) as streetType, d.LGA as LGA, \
                                     upper(c.cityName) as suburb, c.stateCode as state, c.postCode, a.latitude, a.longitude \
                                     from (select propertyNumber, first(latitude) as latitude, first(longitude) as longitude from {ADS_DATABASE_CLEANSED}.hydra_tlotparcel group by propertyNumber) a, \
                                          {ADS_DATABASE_CLEANSED}.isu_vibdnode b, \
                                          {ADS_DATABASE_CLEANSED}.isu_0funct_loc_attr c, \
                                          {ADS_DATABASE_CLEANSED}.isu_0uc_connobj_attr_2 d \
                                     where a.propertyNumber is not null \
                                     and a.propertyNumber = b.architecturalObjectNumber \
                                     and b.architecturalObjectNumber = c.functionalLocationNumber \
                                     and b.parentArchitecturalObjectNumber is null \
                                     and d.propertyNumber = b.architecturalObjectNumber \
                                     union all \
                                     select distinct b.architecturalObjectNumber as locationID, upper(trim(trim(coalesce(c.houseNumber2,'')||' '||coalesce(c.houseNumber1,''))||' '||trim(c.streetName||' '||coalesce(c.streetLine1,''))||' '||coalesce(c.streetLine2,''))|| \
                                     ', '||c.cityName||' NSW '||c.postCode)  as formattedAddress, upper(c.streetName) as streetName, upper(trim(coalesce(streetLine1,'')||' '||coalesce(streetLine2,''))) as streetType, d.LGA as LGA, \
                                     upper(c.cityName) as suburb, c.stateCode as state, c.postCode, a.latitude, a.longitude \
                                     from (select propertyNumber, first(latitude) as latitude, first(longitude) as longitude from {ADS_DATABASE_CLEANSED}.hydra_tlotparcel group by propertyNumber) a, \
                                          {ADS_DATABASE_CLEANSED}.isu_vibdnode b, \
                                          {ADS_DATABASE_CLEANSED}.isu_0funct_loc_attr c, \
                                          {ADS_DATABASE_CLEANSED}.isu_0uc_connobj_attr_2 d \
                                     where a.propertyNumber is not null \
                                     and a.propertyNumber = b.parentArchitecturalObjectNumber \
                                     and b.architecturalObjectNumber = c.functionalLocationNumber \
                                     and d.propertyNumber = b.architecturalObjectNumber")

    
    HydraLocationDf.createOrReplaceTempView('allLocations')
    #3.JOIN TABLES  

    #4.UNION TABLES
    #Create dummy record
    dummyRec = tuple([-1] + ['Unknown'] * (len(HydraLocationDf.columns) - 3) + [0,0]) 
    dummyDimRecDf = spark.createDataFrame([dummyRec],HydraLocationDf.columns)
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
    ,"postCode"
    ,"CAST(latitude AS DECIMAL(9,6)) as latitude" \
    ,"CAST(longitude AS DECIMAL(9,6)) as longitude"                   
    )

    #6.Apply schema definition
    newSchema = StructType([
                            StructField("LocationID", IntegerType(), False),
                            StructField("formattedAddress", StringType(), True),
                            StructField("streetName", StringType(), True),
                            StructField("StreetType", StringType(), True),
                            StructField("LGA", StringType(), True),
                            StructField("suburb", StringType(), True),
                            StructField("state", StringType(), True),
                            StructField("postCode", StringType(), True),
                            StructField("latitude", DecimalType(9,6), True),
                            StructField("longitude", DecimalType(9,6), True)
                      ])

#     HydraLocationDf = spark.createDataFrame(HydraLocationDf.rdd, schema=newSchema)
    return HydraLocationDf

# COMMAND ----------


