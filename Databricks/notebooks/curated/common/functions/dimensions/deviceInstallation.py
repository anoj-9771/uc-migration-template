# Databricks notebook source
###########################################################################################################################
# Function: getdeviceInstallation
#  GETS deviceInstallation
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
def getdeviceInstallation():
    #deviceInstallation
    #2.Load current Cleansed layer table data into dataframe

    df = spark.sql(f"select installationId as installationId, \
                            logicalDeviceNumber as logicalDeviceNumber, \
                            validToDate as validToDate, \
                            validFromDate as validFromDate, \
                            priceClassCode as priceClassCode, \
                            priceClass as priceClass, \
                            payRentalPrice as payRentalPrice, \
                            rateTypeCode as rateTypeCode, \
                            rateType as rateType, \
                            deletedIndicator as deletedIndicator, \
                            bwDeltaProcess as bwDeltaProcess, \
                            operationCode as operationCode \
                            from {ADS_DATABASE_CLEANSED}.isu_0UC_DEVINST_ATTR \
                            where _RecordDeleted = 0 \
                            and   _RecordCurrent = 1 \
                     ")

    df.createOrReplaceTempView('alldeviceInstallation')
    #3.JOIN TABLES  

    #4.UNION TABLES
    #Create dummy record
#     dummyRec = tuple([-1] + ['Unknown'] * (len(HydraLocationDf.columns) - 3) + [0,0]) 
#     dummyDimRecDf = spark.createDataFrame([dummyRec],HydraLocationDf.columns)
#     HydraLocationDf = HydraLocationDf.unionByName(dummyDimRecDf, allowMissingColumns = True)

    #5.SELECT / TRANSFORM
    df = df.selectExpr( \
                  'installationId' \
                , 'logicalDeviceNumber' \
                , 'validToDate' \
                , 'validFromDate' \
                , 'priceClassCode' \
                , 'priceClass' \
                , 'payRentalPrice' \
                , 'rateTypeCode' \
                , 'rateType' \
                , 'deletedIndicator' \
                , 'bwDeltaProcess' \
                , 'operationCode' 
            )

    #6.Apply schema definition
    newSchema = StructType([
                            StructField("installationId", StringType(), False),
                            StructField("logicalDeviceNumber", StringType(), False),
                            StructField("validToDate", DateType(), False),
                            StructField("validFromDate", DateType(), True),
                            StructField("priceClassCode", StringType(), True),
                            StructField("priceClass", StringType(), True),
                            StructField("payRentalPrice", StringType(), True),
                            StructField("rateTypeCode", StringType(), True),
                            StructField("rateType", StringType(), True),
                            StructField("deletedIndicator", StringType(), True),
                            StructField("bwDeltaProcess", StringType(), True),
                            StructField("operationCode", StringType(), True)
                      ])

    df = spark.createDataFrame(df.rdd, schema=newSchema)
    return df

# COMMAND ----------


