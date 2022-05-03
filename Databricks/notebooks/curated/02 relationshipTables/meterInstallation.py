# Databricks notebook source
###########################################################################################################################
# Loads METERINSTALLATION relationship table 
#############################################################################################################################
# Method
# 1.Load Cleansed layer table data into dataframe and transform
# 2.JOIN TABLES
# 3.UNION TABLES
# 4.SELECT / TRANSFORM
# 5.SCHEMA DEFINITION
#############################################################################################################################

# COMMAND ----------

# MAGIC %run ../common/common-curated-includeMain

# COMMAND ----------

def getmeterInstallation():

    #1.Load current Cleansed layer table data into dataframe
    df = spark.sql(f"select din.installationSK as installationSK, \
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
    #2.JOIN TABLES  

    #3.UNION TABLES
    #Create dummy record
    #dummyRec = tuple([-1] + ['Unknown'] * (len(Df.columns) - 2))
    #dummyDimRecDf = spark.createDataFrame([dummyRec],Df.columns)
    
    #Df = Df.unionByName(dummyDimRecDf, allowMissingColumns = True)
    #Display(Df)
    
    #4.SELECT / TRANSFORM
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
                            StructField('meterInstallationSK', LongType(), False),
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

# COMMAND ----------

df, schema = getmeterInstallation()
TemplateEtl(df, entity="meterInstallation", businessKey="installationSK,installationId,logicalDeviceNumber,validToDate", schema=schema, AddSK=True)

# COMMAND ----------

dbutils.notebook.exit("1")
