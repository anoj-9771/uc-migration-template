# Databricks notebook source
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
                                     ', '||c.cityName||' NSW '||c.postCode)  as formattedAddress, \
                                     upper(c.houseNumber2) as houseNumber2, \
                                     upper(c.houseNumber1) as houseNumber1, \
                                     upper(c.streetName) as streetName, \
                                     upper(trim(coalesce(streetLine1,'')||' '||coalesce(streetLine2,''))) as streetType, d.LGA as LGA, \
                                     upper(c.cityName) as suburb, c.stateCode as state, c.postCode, a.latitude, a.longitude \
                                     from (select propertyNumber, latitude, longitude from \
                                              (select propertyNumber, latitude, longitude, \
                                                row_number() over (partition by propertyNumber order by areaSize desc) recNum \
                                                from cleansed.hydra_tlotparcel where _RecordDeleted = 0 and _RecordCurrent = 1 ) \
                                                where recNum = 1) a, \
                                          {ADS_DATABASE_CLEANSED}.isu_vibdnode b, \
                                          {ADS_DATABASE_CLEANSED}.isu_0funct_loc_attr c, \
                                          {ADS_DATABASE_CLEANSED}.isu_0uc_connobj_attr_2 d \
                                     where a.propertyNumber is not null \
                                     and a.propertyNumber = b.architecturalObjectNumber \
                                     and b.architecturalObjectNumber = c.functionalLocationNumber \
                                     and b.parentArchitecturalObjectNumber is null \
                                     and d.propertyNumber = b.architecturalObjectNumber \
                                     and b._RecordDeleted = 0 \
                                     and b._RecordCurrent = 1 \
                                     and c._RecordDeleted = 0 \
                                     and c._RecordCurrent = 1 \
                                     and d._RecordDeleted = 0 \
                                     and d._RecordCurrent = 1 \
                                     union all \
                                     select distinct b.architecturalObjectNumber as locationID, \
                                     upper(trim(trim(coalesce(c1.houseNumber2,'')||' '||coalesce(c1.houseNumber1,''))||' '||trim(c1.streetName||' '||coalesce(c1.streetLine1,''))||' '||coalesce(c1.streetLine2,''))||', '||c1.cityName||' NSW '||c1.postCode)  as formattedAddress, \
                                     upper(c.houseNumber2) as houseNumber2, \
                                     upper(c.houseNumber1) as houseNumber1, \
                                     upper(c.streetName) as streetName, \
                                     upper(trim(coalesce(c.streetLine1,'')||' '||coalesce(c.streetLine2,''))) as streetType, d.LGA as LGA, \
                                     upper(c.cityName) as suburb, c.stateCode as state, c.postCode, a.latitude, a.longitude \
                                     from (select propertyNumber, latitude, longitude from \
                                              (select propertyNumber, latitude, longitude, \
                                                row_number() over (partition by propertyNumber order by areaSize desc) recNum \
                                                from cleansed.hydra_tlotparcel where _RecordDeleted = 0 and _RecordCurrent = 1 ) \
                                                where recNum = 1) a, \
                                          {ADS_DATABASE_CLEANSED}.isu_vibdnode b, \
                                          {ADS_DATABASE_CLEANSED}.isu_0funct_loc_attr c, \
                                          {ADS_DATABASE_CLEANSED}.isu_0funct_loc_attr c1, \
                                          {ADS_DATABASE_CLEANSED}.isu_0uc_connobj_attr_2 d \
                                     where a.propertyNumber is not null \
                                     and a.propertyNumber = b.parentArchitecturalObjectNumber \
                                     and b.architecturalObjectNumber = c.functionalLocationNumber \
                                     and b.parentArchitecturalObjectNumber = c1.functionalLocationNumber \
                                     and d.propertyNumber = b.architecturalObjectNumber \
                                     and b._RecordDeleted = 0 \
                                     and b._RecordCurrent = 1 \
                                     and c._RecordDeleted = 0 \
                                     and c._RecordCurrent = 1 \
                                     and d._RecordDeleted = 0 \
                                     and d._RecordCurrent = 1 \
                                ")

    
    HydraLocationDf.createOrReplaceTempView('allLocations')
    #3.JOIN TABLES  

    #4.UNION TABLES
    #Create dummy record
#    dummyRec = tuple([-1] + ['Unknown'] * (len(HydraLocationDf.columns) - 3) + [0,0]) 
#    dummyDimRecDf = spark.createDataFrame([dummyRec],HydraLocationDf.columns)
    dummyDimRecDf = spark.createDataFrame([("-1","Unknown","Unknown")], [ "locationID","formattedAddress","LGA"])
    HydraLocationDf = HydraLocationDf.unionByName(dummyDimRecDf, allowMissingColumns = True)

    #5.SELECT / TRANSFORM
    HydraLocationDf = HydraLocationDf.selectExpr( \
     "locationID" \
    ,"formattedAddress" \
    ,"houseNumber2" \
    ,"houseNumber1" \
    ,"streetName" \
    ,"streetType" \
    ,"LGA" \
    ,"suburb" \
    ,"state" \
    ,"postCode"
    ,"CAST(latitude AS DECIMAL(9,6)) as latitude" \
    ,"CAST(longitude AS DECIMAL(9,6)) as longitude"                   
    )

    #6.Apply schema definition
    newSchema = StructType([
                            StructField("locationID", StringType(), False),
                            StructField("formattedAddress", StringType(), True),
                            StructField("houseNumber2", StringType(), True),
                            StructField("houseNumber1", StringType(), True),
                            StructField("streetName", StringType(), True),
                            StructField("streetType", StringType(), True),
                            StructField("LGA", StringType(), True),
                            StructField("suburb", StringType(), True),
                            StructField("state", StringType(), True),
                            StructField("postCode", StringType(), True),
                            StructField("latitude", DecimalType(9,6), True),
                            StructField("longitude", DecimalType(9,6), True)
                      ])

    HydraLocationDf = spark.createDataFrame(HydraLocationDf.rdd, schema=newSchema)
    return HydraLocationDf

# COMMAND ----------


