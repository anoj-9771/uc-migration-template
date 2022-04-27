# Databricks notebook source
###########################################################################################################################
# Function: getmeterInstallation
#  GETS meterInstallation
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
def getmeterInstallation():
    #meterInstallation
    #2.Load current Cleansed layer table data into dataframe

    df = spark.sql(f"select din.dimInstallationSK as installationSK, \
                            devin.installationId as installationId, \
                            devin.logicalDeviceNumber as logicalDeviceNumber, \
                            devin.validToDate as validToDate, \
                            devin.validFromDate as validFromDate, \
                            devin.priceClassCode as priceClassCode, \
                            devin.priceClass as priceClass, \
                            devin.payRentalPrice as payRentalPrice, \
                            devin.rateTypeCode as rateTypeCode, \
                            devin.rateType as rateType, \
                            devin.deletedIndicator as deletedIndicator, \
                            devin.bwDeltaProcess as bwDeltaProcess, \
                            devin.operationCode as operationCode \
                            from {ADS_DATABASE_CLEANSED}.isu_0UC_DEVINST_ATTR devin left outer join {ADS_DATABASE_CURATED}.dimInstallation din \
                                                                                        on devin.installationId = din.installationId \
                            where devin._RecordDeleted = 0 \
                            and   devin._RecordCurrent = 1 \
                            and   din._RecordDeleted = 0 \
                            and   din._RecordCurrent = 1 \
                     ")

    df.createOrReplaceTempView('alldeviceInstallation')
    #3.JOIN TABLES  

    #4.UNION TABLES
    #Create dummy record
#     dummyRec = tuple([-1] + ['Unknown'] * (len(Df.columns) - 2))
#     dummyDimRecDf = spark.createDataFrame([dummyRec],Df.columns)
    
#     Df = Df.unionByName(dummyDimRecDf, allowMissingColumns = True)
#     Display(Df)
    
    #5.SELECT / TRANSFORM
    df = df.selectExpr( \
                 'installationSK' \
                , 'installationId' \
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
    schema = StructType([
                            StructField("installationSK", LongType(), False),
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

    return df, schema
