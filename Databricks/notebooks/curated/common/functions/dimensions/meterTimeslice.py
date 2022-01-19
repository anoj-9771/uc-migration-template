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
def getmeterTimeslice():
    #meterTimeslice
    #2.Load current Cleansed layer table data into dataframe

    df = spark.sql(f"select dm.meterSK as meterSK , \
                            devh.equipmentNumber as equipmentNumber , \
                            devh.validToDate as validToDate , \
                            devh.validFromDate as validFromDate , \
                            devh.deviceCategoryCombination as deviceCategoryCombination , \
                            devh.logicalDeviceNumber as logicalDeviceNumber , \
                            devh.registerGroupCode as registerGroupCode , \
                            devh.registerGroup as registerGroup , \
                            devh.to_date(installationDate) as installationDate , \
                            devh.to_date(deviceRemovalDate) as deviceRemovalDate , \
                            devh.activityReasonCode as activityReasonCode , \
                            devh.activityReason as activityReason , \
                            devh.deviceLocation as deviceLocation , \
                            devh.windingGroup as windingGroup , \
                            devh.deletedIndicator as deletedIndicator , \
                            devh.bwDeltaProcess as bwDeltaProcess , \
                            devh.advancedMeterCapabilityGroup as advancedMeterCapabilityGroup , \
                            devh.messageAttributeId as messageAttributeId , \
                            devh.materialNumber as materialNumber , \
                            devh.installationId as installationId , \
                            devh.addressNumber as addressNumber , \
                            devh.cityName as cityName , \
                            devh.houseNumber as houseNumber , \
                            devh.streetName as streetName , \
                            devh.postalCode as postalCode , \
                            devh.superiorFunctionalLocationNumber as superiorFunctionalLocationNumber , \
                            devh.policeEventNumber as policeEventNumber , \
                            devh.orderNumber as orderNumber , \
                            devh.createdBy as createdBy  \
                            from {ADS_DATABASE_CLEANSED}.isu_0UC_DEVICEH_ATTR devh left outer join {ADS_DATABASE_CURATED}.dimMeter dm \
                             on devh.equipmentNumber = dimMeter.equipmentNumber
                             where devh._RecordDeleted = 0 \
                             and   devh._RecordCurrent = 1 \ 
                             and   dm._RecordDeleted = 0 \
                             and   dm._RecordCurrent = 1 \
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
                  'meterSK' \
                , 'equipmentNumber' \
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
                            StructField("meterSK", BigintType(), False),
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


