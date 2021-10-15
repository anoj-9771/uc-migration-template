# Databricks notebook source
###########################################################################################################################
# Function: GetCommonMeter
#  GETS Meter DIMENSION 
# Returns:
#  Dataframe of transformed Metery
#############################################################################################################################
# Method
# 1.Create Function
# 2.Load Cleansed layer table data into dataframe and transform
# 3.JOIN TABLES
# 4.UNION TABLES
# 5.SELECT / TRANSFORM
#############################################################################################################################
#1.Create Function

def getMeter():
  
  #spark.udf.register("TidyCase", GeneralToTidyCase) 
  
  #DimProperty
  #2.Load Cleansed layer table data into dataframe
  
  #Meter Data from Access
  accessZ309TpropmeterDf = spark.sql(f"select 'Access' as sourceSystemCode, \
                                              coalesce(meterMakerNumber,'') as meterId, \
                                              meterSize, waterMeterType, \
                                              meterFittedDate, \
                                              meterRemovedDate, \
                                              row_number() over (partition by metermakernumber order by meterFittedDate desc) rownum \
                                      from {ADS_DATABASE_CLEANSED}.access_z309_tpropmeter \
                                      where (meterFittedDate <> meterRemovedDate or meterRemovedDate is null) \
                                             and _RecordCurrent = 1 and _RecordDeleted = 0 ")
  #Filter for active meter
  accessZ309TpropmeterDf = accessZ309TpropmeterDf.filter(col("rownum") == "1")
  
  #Drop unwanted columns
  accessZ309TpropmeterDf = accessZ309TpropmeterDf.drop(accessZ309TpropmeterDf.meterFittedDate) \
                                               .drop(accessZ309TpropmeterDf.meterRemovedDate) \
                                               .drop(accessZ309TpropmeterDf.rownum)
  
    
  #Meter Data from SAP ISU
  isu0ucDeviceAttrDf  = spark.sql(f"select 'ISU' as sourceSystemCode, materialNumber, equipmentNumber as meterId \
                                      from {ADS_DATABASE_CLEANSED}.isu_0uc_device_attr \
                                      where _RecordCurrent = 1 and _RecordDeleted = 0")
      
  isu0ucDevcatAttrDf  = spark.sql(f"select materialNumber, deviceCategoryDescription as meterSize, functionClass as waterMeterType \
                                      from {ADS_DATABASE_CLEANSED}.isu_0uc_devcat_attr \
                                      where _RecordCurrent = 1 and _RecordDeleted = 0")

  #Dummy Record to be added to Meter Dimension
  dummyDimRecDf = spark.createDataFrame([("ISU", "-1", "Unknown", "Unknown"), ("Access", "-1", "Unknown", "Unknown")], ["sourceSystemCode", "meterId", "meterSize", "waterMeterType"])
  
  #3.JOIN TABLES
  df = isu0ucDeviceAttrDf.join(isu0ucDevcatAttrDf, isu0ucDeviceAttrDf.materialNumber == isu0ucDevcatAttrDf.materialNumber, 
                                  how="leftouter") \
                            .drop(isu0ucDeviceAttrDf.materialNumber) \
                            .drop(isu0ucDevcatAttrDf.materialNumber)
  df = df.select("sourceSystemCode", "meterId", "meterSize", "waterMeterType")

  
  #4.UNION TABLES
  df = accessZ309TpropmeterDf.union(df)
  df = df.unionByName(dummyDimRecDf, allowMissingColumns = True)
  
  #5.Apply schema definition
  newSchema = StructType([
                            StructField("sourceSystemCode", StringType(), False),
                            StructField("meterId", StringType(), False),
                            StructField("meterSize", StringType(), True),
                            StructField("waterMeterType", StringType(), True)
                      ])
  
  df = spark.createDataFrame(df.rdd, schema=newSchema)
  #5.SELECT / TRANSFORM
  #df = df.selectExpr( \
  	 #"meterId", \
     #"sourceSystemCode", \
     #"meterSize", \
     #"waterType"
  #)  
  return df
  

# COMMAND ----------


