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
  
  accessZ309TpropmeterDf = spark.sql("select 'Access' as sourceSystemCode, meterMakerNumber as meterId, meterSize, waterType \
                                      from cleansed.t_access_z309_tpropmeter \
                                      where _RecordCurrent = 1 and _RecordDeleted = 0")
  accessZ309TpropmeterDf = accessZ309TpropmeterDf.dropDuplicates() #Please remove once upstream data is fixed
    
  sapisu0ucDeviceAttrDf  = spark.sql("select materialNumber, 'SAP' as sourceSystemCode, equipmentNumber as meterId \
                                      from cleansed.t_sapisu_0uc_device_attr \
                                      where _RecordCurrent = 1 and _RecordDeleted = 0")
  sapisu0ucDeviceAttrDf  = sapisu0ucDeviceAttrDf.dropDuplicates() #Please remove once upstream data is fixed
    
  sapisu0ucDevcatAttrDf  = spark.sql("select materialNumber, deviceCategoryDescription as meterSize, functionClass as waterType\ 
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
  

# COMMAND ----------

# DBTITLE 1,Test Area
  accessZ309TpropmeterDf = spark.sql("select 'Access' as sourceSystemCode, meterMakerNumber as meterId, meterSize, waterType \
                                      from cleansed.t_access_z309_tpropmeter \
                                      where _RecordCurrent = 1 and _RecordDeleted = 0")
  accessZ309TpropmeterDf = accessZ309TpropmeterDf.dropDuplicates() #Please remove once upstream data is fixed
  display(accessZ309TpropmeterDf)
  print(accessZ309TpropmeterDf.count())
  
  sapisu0ucDeviceAttrDf  = spark.sql("select materialNumber, 'SAP' as sourceSystemCode, equipmentNumber as meterId \
                                      from cleansed.t_sapisu_0uc_device_attr \
                                      where _RecordCurrent = 1 and _RecordDeleted = 0")
  sapisu0ucDeviceAttrDf  = sapisu0ucDeviceAttrDf.dropDuplicates() #Please remove once upstream data is fixed
  display(sapisu0ucDeviceAttrDf)
  
  sapisu0ucDevcatAttrDf  = spark.sql("select materialNumber, deviceCategoryDescription as meterSize, functionClass as waterType \
                                      from cleansed.t_sapisu_0uc_devcat_attr \
                                      where _RecordCurrent = 1 and _RecordDeleted = 0")
  sapisu0ucDevcatAttrDf  = sapisu0ucDevcatAttrDf.dropDuplicates() #Please remove once upstream data is fixed
  display(sapisu0ucDevcatAttrDf)

  #3.JOIN TABLES
  df = sapisu0ucDeviceAttrDf.join(sapisu0ucDevcatAttrDf, sapisu0ucDeviceAttrDf.materialNumber == sapisu0ucDevcatAttrDf.materialNumber,
                                  how="leftouter")  \
                            .drop(sapisu0ucDeviceAttrDf.materialNumber) \
                            .drop(sapisu0ucDevcatAttrDf.materialNumber)
  display(df)
  print(df.count())
  
  df = df.select("meterId", "sourceSystemCode", "meterSize", "waterType")
  display(df)  
  
  df = accessZ309TpropmeterDf.union(df)
  display(df)
  print(df.count())
  
  
df = spark.sql ("select materialNumber, 'SAP' as sourceSystemCode, equipmentNumber as meterId \
                 from cleansed.t_sapisu_0uc_device_attr \
                 where _RecordCurrent = 1 and _RecordDeleted = 0")
display (df)

df1 = spark.sql("select count(*) from from cleansed.t_sapisu_0uc_device_attr")
display(df1)

df1 = spark.sql("select equipmentNumber, count(*) from cleansed.t_sapisu_0uc_device_attr group by equipmentNumber having count(*) > 1")
display(df1)

df  = spark.sql("select materialNumber, deviceCategoryDescription as meterSize, functionClass \
                 from cleansed.t_sapisu_0uc_devcat_attr \
                 where _RecordCurrent = 1 and _RecordDeleted = 0")
display (df)

df1 = spark.sql("select count(*) from from cleansed.t_sapisu_0uc_devcat_attr")
display(df1)

df1 = spark.sql("select materialNumber, count(*) from cleansed.t_sapisu_0uc_devcat_attr group by materialNumber having count(*) > 1")
display(df1)
  
