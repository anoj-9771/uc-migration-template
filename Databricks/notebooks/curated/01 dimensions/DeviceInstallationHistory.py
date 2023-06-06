# Databricks notebook source
###########################################################################################################################
# Loads Device Installation History dimension 
#############################################################################################################################
# Method
# 1.Load Cleansed layer table data into dataframe and transform
# 2.SCHEMA DEFINITION
#############################################################################################################################

# COMMAND ----------

# MAGIC %run ../common/common-curated-includeMain

# COMMAND ----------

def getDeviceInstallationHistory():

    #Device Data from SAP ISU
    isuDeviceInstallHistDf  = spark.sql(f"""select 'ISU' as sourceSystemCode,
                                              installationNumber,
                                              logicalDeviceNumber,
                                              validToDate,
                                              validFromDate,
                                              priceClassCode,
                                              priceClass,
                                              rateTypeCode,
                                              rateType,
                                              payRentalPrice,
                                              dih._RecordDeleted,
                                              dih._DLCleansedZoneTimeStamp 
                                          from {ADS_DATABASE_CLEANSED}.isu.0UC_DEVINST_ATTR dih
                                          where dih._RecordCurrent = 1 
                                      """)
    
    dummyDimRecDf = spark.createDataFrame([("-1","-1","1900-01-01", "9999-12-31")], ["installationNumber","logicalDeviceNumber","validFromDate","validToDate"])   
    dfResult = isuDeviceInstallHistDf.unionByName(dummyDimRecDf, allowMissingColumns = True) 
    dfResult = dfResult.withColumn("validFromDate",col("validFromDate").cast("date")).withColumn("validToDate",col("validToDate").cast("date"))
    
    #5.Apply schema definition
    schema = StructType([
                            StructField('deviceInstallationHistorySK', StringType(), False),
                            StructField('sourceSystemCode', StringType(), True),
                            StructField('installationNumber', StringType(), False),
                            StructField('logicalDeviceNumber', StringType(), False),
                            StructField('validToDate', DateType(), False),
                            StructField('validFromDate', DateType(), True),
                            StructField('priceClassCode', StringType(), True),
                            StructField('priceClass', StringType(), True),
                            StructField('rateTypeCode', StringType(), True),
                            StructField('rateType', StringType(), True),
                            StructField('payRentalPrice', StringType(), True)
                      ])    
    
    return dfResult, schema

# COMMAND ----------

df, schema = getDeviceInstallationHistory()
TemplateTimeSliceEtlSCD(df, entity="dim.deviceInstallationHistory", businessKey="installationNumber,logicalDeviceNumber,validToDate", schema=schema)

# COMMAND ----------

dbutils.notebook.exit("1")
