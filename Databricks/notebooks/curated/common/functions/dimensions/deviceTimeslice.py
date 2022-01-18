# Databricks notebook source
###########################################################################################################################
# Function: getdeviceTimeslice
#  GETS deviceTimeslice
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
def getdeviceTimeslice():
    #deviceTimeslice
    #2.Load current Cleansed layer table data into dataframe

    df = spark.sql(f"select equipmentNumber as equipmentNumber , \
                            validToDate as validToDate , \
                            validFromDate as validFromDate , \
                            deviceCategoryCombination as deviceCategoryCombination , \
                            logicalDeviceNumber as logicalDeviceNumber , \
                            registerGroupCode as registerGroupCode , \
                            registerGroup as registerGroup , \
                            to_date(installationDate) as installationDate , \
                            to_date(deviceRemovalDate) as deviceRemovalDate , \
                            activityReasonCode as activityReasonCode , \
                            activityReason as activityReason , \
                            deviceLocation as deviceLocation , \
                            windingGroup as windingGroup , \
                            deletedIndicator as deletedIndicator , \
                            bwDeltaProcess as bwDeltaProcess , \
                            advancedMeterCapabilityGroup as advancedMeterCapabilityGroup , \
                            messageAttributeId as messageAttributeId , \
                            materialNumber as materialNumber , \
                            installationId as installationId , \
                            addressNumber as addressNumber , \
                            cityName as cityName , \
                            houseNumber as houseNumber , \
                            streetName as streetName , \
                            postalCode as postalCode , \
                            superiorFunctionalLocationNumber as superiorFunctionalLocationNumber , \
                            policeEventNumber as policeEventNumber , \
                            orderNumber as orderNumber , \
                            createdBy as createdBy  \
                            from {ADS_DATABASE_CLEANSED}.isu_0UC_DEVICEH_ATTR \
                             where _RecordDeleted = 0 \
                             and   _RecordCurrent = 1 \
                     ")

    df.createOrReplaceTempView('alldeviceTimeslice')
    #3.JOIN TABLES  

    #4.UNION TABLES
    #Create dummy record
#     dummyRec = tuple([-1] + ['Unknown'] * (len(HydraLocationDf.columns) - 3) + [0,0]) 
#     dummyDimRecDf = spark.createDataFrame([dummyRec],HydraLocationDf.columns)
#     HydraLocationDf = HydraLocationDf.unionByName(dummyDimRecDf, allowMissingColumns = True)

    #5.SELECT / TRANSFORM
    df = df.selectExpr( \
                  'equipmentNumber' \
                , 'validToDate' \
                , 'validFromDate' \
                , 'deviceCategoryCombination' \
                , 'logicalDeviceNumber' \
                , 'registerGroupCode' \
                , 'registerGroup' \
                , 'installationDate' \
                , 'deviceRemovalDate' \
                , 'activityReasonCode' \
                , 'activityReason' \
                , 'deviceLocation' \
                , 'windingGroup' \
                , 'deletedIndicator' \
                , 'bwDeltaProcess' \
                , 'advancedMeterCapabilityGroup' \
                , 'messageAttributeId' \
                , 'materialNumber' \
                , 'installationId' \
                , 'addressNumber' \
                , 'cityName' \
                , 'houseNumber' \
                , 'streetName' \
                , 'postalCode' 
                , 'superiorFunctionalLocationNumber' \
                , 'policeEventNumber' \
                , 'orderNumber' \
                , 'createdBy' 
            )

    #6.Apply schema definition
    newSchema = StructType([
                            StructField("equipmentNumber", StringType(), False),
                            StructField("validToDate", DateType(), False),
                            StructField("validFromDate", DateType(), True),
                            StructField("deviceCategoryCombination", StringType(), True),
                            StructField("logicalDeviceNumber", LongType(), True),
                            StructField("registerGroupCode", StringType(), True),
                            StructField("registerGroup", StringType(), True),
                            StructField("installationDate", DateType(), True),
                            StructField("deviceRemovalDate", DateType(), True),
                            StructField("activityReasonCode", StringType(), True),
                            StructField("activityReason", StringType(), True),
                            StructField("deviceLocation", StringType(), True),
                            StructField("windingGroup", StringType(), True),
                            StructField("deletedIndicator", StringType(), True),
                            StructField("bwDeltaProcess", StringType(), True),
                            StructField("advancedMeterCapabilityGroup", LongType(), True),
                            StructField("messageAttributeId", LongType(), True),
                            StructField("materialNumber", StringType(), True),
                            StructField("installationId", StringType(), True),
                            StructField("addressNumber", StringType(), True),
                            StructField("cityName", StringType(), True),
                            StructField("houseNumber", StringType(), True),
                            StructField("streetName", StringType(), True),
                            StructField("postalCode", StringType(), True),
                            StructField("superiorFunctionalLocationNumber", StringType(), True),
                            StructField("policeEventNumber", StringType(), True),
                            StructField("orderNumber", StringType(), True),
                            StructField("createdBy", StringType(), True)
                      ])

    df = spark.createDataFrame(df.rdd, schema=newSchema)
    return df

# COMMAND ----------


