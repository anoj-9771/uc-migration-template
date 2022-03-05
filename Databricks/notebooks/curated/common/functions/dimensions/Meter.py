# Databricks notebook source
#%run ../../includes/util-common

# COMMAND ----------

# Run the above commands only when running this notebook independently, otherwise the curated master notebook would take care of calling the above notebooks

# COMMAND ----------

def getMeter():
    meterFunctionClasses = ['1000','2000','9000']
    #spark.udf.register("TidyCase", GeneralToTidyCase) 

    #2.Load Cleansed layer table data into dataframe
    #Meter Data from Access
    #notes for future: a number of column values can be derived with aditional code/queries. LastActivity reason code can only be mapped from meter change reason code/reason after conversion of values. The text is the same but the codes differ
    accessZ309TpropmeterDf = spark.sql(f"select 'ACCESS' as sourceSystemCode, \
                                              row_number() over (order by metermakernumber) as meterNumber, \
                                              coalesce(meterMakerNumber,'') as meterSerialNumber, \
                                              null as materialNumber, \
                                              case when meterClass = 'Standpipe' then 'Customer Standpipe' else 'Water Meter' end as usageMeterType, \
                                              meterSize, \
                                              case when waterMeterType = 'Potable' then 'Drinking Water' \
                                                   when waterMeterType = 'Recycled' then 'Recycled Water' else waterMeterType end as waterType, \
                                              null as meterCategoryCode, \
                                              null as meterCategory, \
                                              case when meterClass = 'Standpipe' then 'STANDPIPE' \
                                                   when meterGroup = 'Normal Reading' then 'NORMAL' \
                                                   when meterGroup = 'Automated Reading (AMI)' then 'AMI' \
                                                   when meterGroup = 'Remote Reading' then 'AMR' \
                                                   else null end as meterReadingType, \
                                              null as meterDescription, \
                                              meterFittedDate, \
                                              meterRemovedDate, \
                                              null as manufacturerName, \
                                              null as manufacturerSerialNumber, \
                                              null as manufacturerModelNumber, \
                                              null as inspectionRelevanceFlag, \
                                              row_number() over (partition by metermakernumber order by meterFittedDate desc) rownum \
                                      from {ADS_DATABASE_CLEANSED}.access_z309_tpropmeter \
                                      where (meterFittedDate <> meterRemovedDate or meterRemovedDate is null) \
                                      and _RecordCurrent = 1 \
                                      and _RecordDeleted = 0")
     
    #Filter for active meter
    accessZ309TpropmeterDf = accessZ309TpropmeterDf.filter(col("rownum") == "1")

    #Drop unwanted columns
    accessZ309TpropmeterDf = accessZ309TpropmeterDf.drop(accessZ309TpropmeterDf.rownum)

    print(f'{accessZ309TpropmeterDf.count():,} rows in accessZ309TpropmeterDf')
    #display(accessZ309TpropmeterDf)
    #Meter Data from SAP ISU
    isu0ucDeviceAttrDf  = spark.sql(f"select 'ISU' as sourceSystemCode, \
                                      equipmentNumber as meterNumber, \
                                      deviceNumber as meterSerialNumber, \
                                      materialNumber, \
                                      trim(LEADING '0' FROM deviceSize)||' mm' as meterSize, \
                                      assetManufacturerName as manufacturerName, \
                                      manufacturerSerialNumber, \
                                      manufacturerModelNumber, \
                                      case when inspectionRelevanceIndicator = 'X' then 'Y' else 'N' end as inspectionRelevanceFlag, \
                                      null as meterFittedDate, \
                                      null as meterRemovedDate \
                                      from {ADS_DATABASE_CLEANSED}.isu_0uc_device_attr a \
                                      where a._RecordCurrent = 1 \
                                      and a._RecordDeleted = 0 \
                                      ")
    
    print(f'{isu0ucDeviceAttrDf.count():,} rows in isu0ucDeviceAttrDf')
#    display(isu0ucDeviceAttrDf)

    isu0ucDevCatAttrDf  = spark.sql(f"select distinct a.materialNumber, \
                                        case when functionClassCode = '9000' then 'Customer Standpipe' else 'Water Meter' end as usageMeterType, \
                                        case when functionClassCode = '1000' then 'Drinking Water' \
                                             when functionClassCode = '2000' then 'Recycled Water' else functionClassCode end as waterType, \
                                        constructionClassCode as meterCategoryCode, \
                                        constructionClass as meterCategory, \
                                        deviceCategoryName as meterReadingType, \
                                        deviceCategoryDescription as meterDescription \
                                      from {ADS_DATABASE_CLEANSED}.isu_0uc_devcat_attr a \
                                      where a.functionClassCode in (1000, 2000, 9000) \
                                      and a._RecordCurrent = 1 \
                                      and a._RecordDeleted = 0")
    
    print(f'{isu0ucDevCatAttrDf.count():,} rows in isu0ucDevCatAttrDf')
#     display(isu0ucDevCatAttrDf)  
    
    #3.JOIN TABLES
    df = isu0ucDeviceAttrDf.join(isu0ucDevCatAttrDf, isu0ucDeviceAttrDf.materialNumber == isu0ucDevCatAttrDf.materialNumber, 
                                  how="leftouter") \
                            .drop(isu0ucDevCatAttrDf.materialNumber)
    print(f'{df.count():,} rows after merge 1')
    #display(df)
        
#     #re-order columns
    df = df.select('sourceSystemCode','meterNumber','meterSerialNumber','materialNumber','usageMeterType','meterSize','waterType','meterCategoryCode','meterCategory',
                    'meterReadingType','meterDescription','meterFittedDate','meterRemovedDate','manufacturerName','manufacturerSerialNumber','manufacturerModelNumber','inspectionRelevanceFlag')
    
    #4. UNION
    df = accessZ309TpropmeterDf.union(df)
    print(f'{df.count():,} rows after Union 1')
#     display(df)    
    
    dummyDimRecDf = spark.createDataFrame([("ISU","-1","Unknown"),("ACCESS","-2","Unknown"),("ISU","-3","NA"),("ACCESS","-4","NA")], ["sourceSystemCode", "meterNumber","meterDescription"])   
    df = df.unionByName(dummyDimRecDf, allowMissingColumns = True)    
    print(f'{df.count():,} rows after Union 2')
#     display(df)
    
    #5.Apply schema definition
    newSchema = StructType([
                            StructField('sourceSystemCode', StringType(), False),
                            StructField('meterNumber', StringType(), False),
                            StructField('meterSerialNumber', StringType(), True),
                            StructField('materialNumber', StringType(), True),
                            StructField('usageMeterType', StringType(), True),
                            StructField('meterSize', StringType(), True),
                            StructField('waterType', StringType(), True),
                            StructField('meterCategoryCode', StringType(), True),
                            StructField('meterCategory', StringType(), True),
                            StructField('meterReadingType', StringType(), True),
                            StructField('meterDescription', StringType(), True),
                            StructField('meterFittedDate', DateType(), True),
                            StructField('meterRemovedDate', DateType(), True),
                            StructField('manufacturerName', StringType(), True),
                            StructField('manufacturerSerialNumber', StringType(), True),
                            StructField('manufacturerModelNumber', StringType(), True),
                            StructField('inspectionRelevanceFlag', StringType(), True),   
                      ])    
    
    df = spark.createDataFrame(df.rdd, schema=newSchema)
#     display(df)    
    
    return df
  

# COMMAND ----------

#Dummy Record to be added to Meter Dimension
#     ISUDummy = tuple(['ISU','-1',"2099-12-31","2099-12-31",""] + ['Unknown'] * (len(df.columns) - 5)) #this only works as long as all output columns are string
#     ACCESSDummy = tuple(['ACCESS','-2',"2099-12-31","2099-12-31",""] + ['Unknown'] * (len(df.columns) -5)) #this only works as long as all output columns are string
#     dummyDimRecDf = spark.createDataFrame([ISUDummy, ACCESSDummy], df.columns)
#    df.write.saveAsTable('curated.dimMeterTest_new3')
