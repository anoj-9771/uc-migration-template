# Databricks notebook source
###########################################################################################################################
# Loads Register History dimension 
#############################################################################################################################
# Method
# 1.Load Cleansed layer table data into dataframe and transform
# 2.SCHEMA DEFINITION
#############################################################################################################################

# COMMAND ----------

# MAGIC %run ../common/common-curated-includeMain

# COMMAND ----------

def getRegisterHistory():

    #Device Data from SAP ISU
    isuRegisterHistDf  = spark.sql(f"""select 'ISU' as sourceSystemCode,
                                          registerNumber,
                                          equipmentNumber as deviceNumber,
                                          validToDate,
                                          validFromDate,
                                          logicalRegisterNumber,
                                          divisionCategoryCode,
                                          divisionCategory,
                                          registerIdCode,
                                          registerId,
                                          registerTypeCode,
                                          registerType,
                                          registerCategoryCode,
                                          registerCategory,
                                          reactiveApparentOrActiveRegister as reactiveApparentOrActiveRegisterCode,
                                          reactiveApparentOrActiveRegisterTxt as reactiveApparentOrActiveRegister,
                                          unitOfMeasurementMeterReading,
                                          doNotReadFlag,
                                          rh._RecordDeleted,
                                          rh._DLCleansedZoneTimeStamp 
                                      from {ADS_DATABASE_CLEANSED}.isu_0UC_REGIST_ATTR rh
                                      where rh._RecordCurrent = 1 
                                      """)
    dummyDimRecDf = spark.createDataFrame([("-1","-1","1900-01-01", "9999-12-31")], ["registerNumber","deviceNumber","validFromDate","validToDate"])   
    dfResult = isuRegisterHistDf.unionByName(dummyDimRecDf, allowMissingColumns = True) 
    dfResult = dfResult.withColumn("validFromDate",col("validFromDate").cast("date")).withColumn("validToDate",col("validToDate").cast("date"))
    
    #5.Apply schema definition
    schema = StructType([
                            StructField('registerHistorySK', StringType(), False),
                            StructField('sourceSystemCode', StringType(), True),
                            StructField('registerNumber', StringType(), False),
                            StructField('deviceNumber', StringType(), False),
                            StructField('validToDate', DateType(), False),
                            StructField('validFromDate', DateType(), True),
                            StructField('logicalRegisterNumber', StringType(), True),
                            StructField('divisionCategoryCode', StringType(), True),
                            StructField('divisionCategory', StringType(), True),
                            StructField('registerIdCode', StringType(), True),
                            StructField('registerId', StringType(), True),
                            StructField('registerTypeCode', StringType(), True),
                            StructField('registerType', StringType(), True),
                            StructField('registerCategoryCode', StringType(), True),
                            StructField('registerCategory', StringType(), True),
                            StructField('reactiveApparentOrActiveRegisterCode', StringType(), True),
                            StructField('reactiveApparentOrActiveRegister', StringType(), True),
                            StructField('unitOfMeasurementMeterReading', StringType(), True),
                            StructField('doNotReadFlag', StringType(), True)
                      ])    
                
    return dfResult, schema

# COMMAND ----------

df, schema = getRegisterHistory()
TemplateTimeSliceEtlSCD(df, entity="dimRegisterHistory", businessKey="registerNumber,deviceNumber,validToDate", schema=schema)

# COMMAND ----------

dbutils.notebook.exit("1")
