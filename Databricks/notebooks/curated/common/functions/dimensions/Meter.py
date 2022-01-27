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
                                              meterRemovedDate as validToDate, \
                                              meterRemovedDate as registerToDate, \
                                              '' as registerNumber, \
                                              coalesce(meterMakerNumber,'') as meterSerialNumber, \
                                              null as logicalDeviceNumber, \
                                              meterFittedDate as validFromDate, \
                                              null as materialNumber, \
                                              case when meterClass = 'Standpipe' then 'Customer Standpipe' else 'Water Meter' end as usageDeviceType, \
                                              meterSize, \
                                              case when waterMeterType in ('Potable','Recycled') then waterMeterType else null end as waterType, \
                                              null as meterCategoryCode, \
                                              meterCategory, \
                                              case when meterClass = 'Standpipe' then 'STANDPIPE' \
                                                   when meterGroup = 'Normal Reading' then 'NORMAL' \
                                                   when meterGroup = 'Automated Reading (AMI)' then 'AMI' \
                                                   when meterGroup = 'Remote Reading' then 'AMR' \
                                                   else null end as meterReadingType, \
                                              null as meterDescription, \
                                              null as manufacturerName, \
                                              null as manufacturerSerialNumber, \
                                              null as manufacturerModelNumber, \
                                              case when right(meterSize,2) = 'mm' then 'KL' else 'IN' end as measurementUnit, \
                                              null as registerFromDate, \
                                              null as latestActivityReasonCode, \
                                              null as latestActivityReason, \
                                              null as inspectionRelevanceFlag, \
                                              null as registerGroupCode, \
                                              null as registerGroup, \
                                              null as registerTypeCode, \
                                              null as registerType, \
                                              null as registerCategoryCode, \
                                              null as registerCategory, \
                                              null as registerIdCode, \
                                              null as registerId, \
                                              null as divisionCategoryCode, \
                                              null as divisionCategory, \
                                              row_number() over (partition by metermakernumber order by meterFittedDate desc) rownum \
                                      from {ADS_DATABASE_CLEANSED}.access_z309_tpropmeter \
                                      where (meterFittedDate <> meterRemovedDate or meterRemovedDate is null) \
                                      and _RecordCurrent = 1 \
                                      and _RecordDeleted = 0 ")
     
    #Filter for active meter
    accessZ309TpropmeterDf = accessZ309TpropmeterDf.filter(col("rownum") == "1")

    #Drop unwanted columns
    accessZ309TpropmeterDf = accessZ309TpropmeterDf.drop(accessZ309TpropmeterDf.rownum)

    #print(f'{accessZ309TpropmeterDf.count():,} rows in accessZ309TpropmeterDf')
    #display(accessZ309TpropmeterDf)
    #Meter Data from SAP ISU
    isu0ucDeviceAttrDf  = spark.sql(f"select 'ISU' as sourceSystemCode, \
                                      equipmentNumber as meterNumber, \
                                      deviceNumber as meterSerialNumber, \
                                      a.materialNumber, \
                                      trim(LEADING '0' FROM deviceSize)||' mm' as meterSize, \
                                      assetManufacturerName as manufacturerName, \
                                      manufacturerSerialNumber, \
                                      manufacturerModelNumber, \
                                      case when inspectionRelevanceIndicator = 'X' then 'Y' else 'N' end as inspectionRelevanceFlag \
                                      from {ADS_DATABASE_CLEANSED}.isu_0uc_device_attr a, \
                                           {ADS_DATABASE_CLEANSED}.isu_0uc_devcat_attr b \
                                      where a.materialNumber = b.materialNumber \
                                      and functionClassCode in ({','.join(meterFunctionClasses)}) \
                                      and a._RecordCurrent = 1 \
                                      and a._RecordDeleted = 0 \
                                      and b._RecordCurrent = 1 \
                                      and b._RecordDeleted = 0 \
                                     ")
    
    #print(f'{isu0ucDeviceAttrDf.count():,} rows in isu0ucDeviceAttrDf')
    #display(isu0ucDeviceAttrDf)
    #save to table for use in subsequent queries
    isu0ucDeviceAttrDf.createOrReplaceTempView('allMeters')

    isu0ucDeviceHAttrDf  = spark.sql(f"select equipmentNumber as meterNumber, \
                                        logicalDeviceNumber, \
                                        activityReasonCode as latestActivityReasonCode, \
                                        activityReason as latestActivityReason, \
                                        registerGroupCode, \
                                        registerGroup, \
                                        ToValidDate(validFromDate)as validFromDate, \
                                        ToValidDate(validToDate,'Mandatory') as validToDate \
                                      from {ADS_DATABASE_CLEANSED}.isu_0uc_deviceh_attr a \
                                      join allmeters b on a.equipmentNumber = b.meterNumber and current_date between a.validFromDate and a.validToDate \
                                      where _RecordCurrent = 1 \
                                      and _RecordDeleted = 0")
    
    #print(f'{isu0ucDeviceHAttrDf.count():,} rows in isu0ucDeviceHAttrDf')
    #display(isu0ucDeviceHAttrDf)
    isu0ucDevCatAttrDf  = spark.sql(f"select distinct a.materialNumber, \
                                        case when functionClassCode = '9000' then 'Customer Standpipe' else 'Water Meter' end as usageDeviceType, \
                                        case when functionClassCode = '1000' then 'Drinking Water' \
                                             when functionClassCode = '2000' then 'Recycled Water' else null end as waterType, \
                                        constructionClassCode as meterCategoryCode, \
                                        constructionClass as meterCategory, \
                                        deviceCategoryName as meterReadingType, \
                                        deviceCategoryDescription as meterDescription \
                                      from {ADS_DATABASE_CLEANSED}.isu_0uc_devcat_attr a \
                                      join allmeters b on a.materialNumber = b.materialNumber \
                                      where _RecordCurrent = 1 \
                                      and _RecordDeleted = 0")
    
    #print(f'{isu0ucDevCatAttrDf.count():,} rows in isu0ucDevCatAttrDf')
    #display(isu0ucDevCatAttrDf)
    isu0ucRegistAttrDf  = spark.sql(f"select equipmentNumber as meterNumber, \
                                        unitOfMeasurementMeterReading as measurementUnit, \
                                        min(registerNumber) as registerNumber, \
                                        registerTypeCode, \
                                        registerType, \
                                        registerCategoryCode, \
                                        registerCategory, \
                                        registerIdCode, \
                                        registerId, \
                                        divisionCategoryCode, \
                                        divisionCategory, \
                                        ToValidDate(validFromDate) as registerFromDate, \
                                        ToValidDate(validToDate,'MANDATORY') as registerToDate \
                                      from {ADS_DATABASE_CLEANSED}.isu_0uc_regist_attr a \
                                      join allmeters b on a.equipmentNumber = b.meterNumber and current_date between a.validFromDate and a.validToDate \
                                      where _RecordCurrent = 1 \
                                      and _RecordDeleted = 0 \
                                      group by equipmentNumber, unitOfMeasurementMeterReading, registerTypeCode, registerType, registerCategoryCode, registerCategory, registerIdCode, registerId, divisionCategoryCode, divisionCategory,validFromDate,validToDate")
	 
    #print(f'{isu0ucRegistAttrDf.count():,} rows in isu0ucRegistAttrDf')
    #display(isu0ucRegistAttrDf)
    
    #3.JOIN TABLES
    df = isu0ucDeviceAttrDf.join(isu0ucDeviceHAttrDf, isu0ucDeviceHAttrDf.meterNumber == isu0ucDeviceAttrDf.meterNumber, 
                                  how="leftouter") \
                            .drop(isu0ucDeviceHAttrDf.meterNumber)
    #print(f'{df.count():,} rows after merge 1')
    
    df = df.join(isu0ucDevCatAttrDf, isu0ucDevCatAttrDf.materialNumber == df.materialNumber, 
                                  how="leftouter") \
                            .drop(isu0ucDevCatAttrDf.materialNumber)
    #print(f'{df.count():,} rows after merge 2')
    df = df.join(isu0ucRegistAttrDf, isu0ucRegistAttrDf.meterNumber == df.meterNumber, 
                                  how="leftouter") \
                            .drop(isu0ucRegistAttrDf.meterNumber)
    #print(f'{df.count():,} rows after merge 3')
    
    #re-order columns
    df = df.select('sourceSystemCode','meterNumber','validToDate','registerToDate','registerNumber','meterSerialNumber','logicalDeviceNumber','validFromDate',
                    'materialNumber','usageDeviceType','meterSize','waterType','meterCategoryCode','meterCategory',
                    'meterReadingType','meterDescription','manufacturerName','manufacturerSerialNumber','manufacturerModelNumber',
                    'measurementUnit','registerFromDate','latestActivityReasonCode','latestActivityReason','inspectionRelevanceFlag',
                    'registerGroupCode','registerGroup','registerTypeCode','registerType','registerCategoryCode',
                    'registerCategory','registerIdCode','registerId','divisionCategoryCode','divisionCategory'
                  )
    
    
    df = accessZ309TpropmeterDf.union(df)
    #print(f'{df.count():,} rows after Union 1')
    #display(df)    
    
    #Dummy Record to be added to Meter Dimension
#     ISUDummy = tuple(['ISU','-1',"2099-12-31","2099-12-31",""] + ['Unknown'] * (len(df.columns) - 5)) #this only works as long as all output columns are string
#     ACCESSDummy = tuple(['ACCESS','-2',"2099-12-31","2099-12-31",""] + ['Unknown'] * (len(df.columns) -5)) #this only works as long as all output columns are string
#     dummyDimRecDf = spark.createDataFrame([ISUDummy, ACCESSDummy], df.columns)

    dummyDimRecDf = spark.createDataFrame([("ISU","-1","2099-12-31","2099-12-31","","1900-01-01"),("ACCESS","-2","2099-12-31","2099-12-31","","1900-01-01")], 
                                          ["sourceSystemCode", "meterNumber","validToDate","registerToDate","registerNumber","validFromDate"])    
    

    #check key columns for null    
    df = df.withColumn("validToDate", when(df.validToDate.isNull(), "2099-12-31").otherwise(df.validToDate)) \
           .withColumn("registerToDate", when(df.registerToDate.isNull(), "2099-12-31").otherwise(df.registerToDate)) \
           .withColumn("registerNumber", when(df.registerNumber.isNull(), " ").otherwise(df.registerNumber))
        
    df = df.unionByName(dummyDimRecDf, allowMissingColumns = True)
    
    df = df.withColumn("validToDate",df['validToDate'].cast(DateType())) \
           .withColumn("registerToDate",df['registerToDate'].cast(DateType())) \
           .withColumn("validFromDate",df['validFromDate'].cast(DateType())) \
           .withColumn("registerFromDate",df['registerFromDate'].cast(DateType())) 
    #print(f'{df.count():,} rows after Union 2')
    #display(df)
    
    #5.Apply schema definition
    newSchema = StructType([
                            StructField('sourceSystemCode', StringType(), False),
                            StructField('meterNumber', StringType(), False),
                            StructField('validToDate', DateType(), False),
                            StructField('registerToDate', DateType(), False),
                            StructField('registerNumber', StringType(), False),
                            StructField('meterSerialNumber', StringType(), True),
                            StructField('logicalDeviceNumber', StringType(), True),
                            StructField('validFromDate', DateType(), True),
                            StructField('materialNumber', StringType(), True),
                            StructField('usageDeviceType', StringType(), True),
                            StructField('meterSize', StringType(), True),
                            StructField('waterType', StringType(), True),
                            StructField('meterCategoryCode', StringType(), True),
                            StructField('meterCategory', StringType(), True),
                            StructField('meterReadingType', StringType(), True),
                            StructField('meterDescription', StringType(), True),
                            StructField('manufacturerName', StringType(), True),
                            StructField('manufacturerSerialNumber', StringType(), True),
                            StructField('manufacturerModelNumber', StringType(), True),
                            StructField('measurementUnit', StringType(), True),
                            StructField('registerFromDate', DateType(), True),                                    
                            StructField('latestActivityReasonCode', StringType(), True),
                            StructField('latestActivityReason', StringType(), True),
                            StructField('inspectionRelevanceFlag', StringType(), True),
                            StructField('registerGroupCode', StringType(), True),
                            StructField('registerGroup', StringType(), True),
                            StructField('registerTypeCode', StringType(), True),
                            StructField('registerType', StringType(), True),
                            StructField('registerCategoryCode', StringType(), True),
                            StructField('registerCategory', StringType(), True),
                            StructField('registerIdCode', StringType(), True),
                            StructField('registerId', StringType(), True),
                            StructField('divisionCategoryCode', StringType(), True),
                            StructField('divisionCategory', StringType(), True)                        
                      ])

    df = spark.createDataFrame(df.rdd, schema=newSchema)
    #display(df)
    #6.SELECT / TRANSFORM
    df = df.selectExpr( \
     "sourceSystemCode", \
     "meterNumber", \
     "meterSerialNumber", \
     "logicalDeviceNumber", \
     "validFromDate", \
     "validToDate", \
     "materialNumber", \
     "usageDeviceType", \
     "meterSize", \
     "waterType", \
     "meterCategoryCode", \
     "meterCategory", \
     "meterReadingType", \
     "meterDescription", \
     "manufacturerName", \
     "manufacturerSerialNumber", \
     "manufacturerModelNumber", \
     "measurementUnit", \
     "registerFromDate", \
     "registerToDate", \
     "latestActivityReasonCode", \
     "latestActivityReason", \
     "inspectionRelevanceFlag", \
     "registerNumber", \
     "registerGroupCode", \
     "registerGroup", \
     "registerTypeCode", \
     "registerType", \
     "registerCategoryCode", \
     "registerCategory", \
     "registerIdCode", \
     "registerId", \
     "divisionCategoryCode", \
     "divisionCategory"
    )  
    #display(df)
    
    return df
  
