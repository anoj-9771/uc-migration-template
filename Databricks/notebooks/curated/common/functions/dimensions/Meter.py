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

def GetCommonMeter():
  
  #spark.udf.register("TidyCase", GeneralToTidyCase) 
  
  #DimProperty
  #2.Load Cleansed layer table data into dataframe
  
  #Meter Data from Access
  accessZ309TpropmeterDf = spark.sql("select 'Access' as sourceSystemCode, \
                                              meterMakerNumber as meterId, \
                                              meterSize, waterType, \
                                              meterFittedDate, \
                                              meterRemovedDate, \
                                              row_number() over (partition by metermakernumber order by meterFittedDate desc) rownum \
                                      from cleansed.t_access_z309_tpropmeter \
                                      where (meterFittedDate<>meterRemovedDate or meterRemovedDate is null) \
                                             and _RecordCurrent = 1 and _RecordDeleted = 0 ")
  #Filter for active meter
  accessZ309TpropmeterDf = accessZ309TpropmeterDf.filter(col("rownum") == "1")
  
  #Drop unwanted columns
  accessZ309TpropmeterDf = accessZ309TpropmeterDf.drop(accessZ309TpropmeterDf.meterFittedDate) \
                                               .drop(accessZ309TpropmeterDf.meterRemovedDate) \
                                               .drop(accessZ309TpropmeterDf.rownum)
  
  accessZ309TpropmeterDf = accessZ309TpropmeterDf.dropDuplicates() #Please remove once upstream data is fixed
    
  #Meter Data from SAP ISU  
  sapisu0ucDeviceAttrDf  = spark.sql("select materialNumber, 'SAP' as sourceSystemCode, equipmentNumber as meterId \
                                      from cleansed.t_sapisu_0uc_device_attr \
                                      where _RecordCurrent = 1 and _RecordDeleted = 0")
  sapisu0ucDeviceAttrDf  = sapisu0ucDeviceAttrDf.dropDuplicates() #Please remove once upstream data is fixed
      
  sapisu0ucDevcatAttrDf  = spark.sql("select materialNumber, deviceCategoryDescription as meterSize, functionClass as waterType \
                                      from cleansed.t_sapisu_0uc_devcat_attr \
                                      where _RecordCurrent = 1 and _RecordDeleted = 0")
  sapisu0ucDevcatAttrDf  = sapisu0ucDevcatAttrDf.dropDuplicates() #Please remove once upstream data is fixed
  
  #3.JOIN TABLES
  df = sapisu0ucDeviceAttrDf.join(sapisu0ucDevcatAttrDf, sapisu0ucDeviceAttrDf.materialNumber == sapisu0ucDevcatAttrDf.materialNumber, 
                                  how="leftouter") \
                            .drop(sapisu0ucDeviceAttrDf.materialNumber) \
                            .drop(sapisu0ucDevcatAttrDf.materialNumber)
  df = df.select("meterId", "sourceSystemCode", "meterSize", "waterType")
    


  #4.UNION TABLES
  df = accessZ309TpropmeterDf.union(df)

  #5.SELECT / TRANSFORM
  #df = df.selectExpr( \
  	 #"meterId", \
     #"sourceSystemCode", \
     #"meterSize", \
     #"waterType"
  #)  
  return df  
  
