# Databricks notebook source
###########################################################################################################################
# Loads METER dimension 
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

def getMeter():

    meterFunctionClasses = ['1000','2000','9000']
    #1.Load Cleansed layer table data into dataframe
    #notes for future: 
        #a number of column values can be derived with aditional code/queries. 
        #LastActivity reason code can only be mapped from meter change reason code/reason after conversion of values. The text is the same but the codes differ
    accessZ309TpropmeterDf = spark.sql(f"select 'ACCESS' as sourceSystemCode, \
                                              row_number() over (order by metermakernumber) as meterNumber, \
                                              coalesce(meterMakerNumber,'') as meterSerialNumber, \
                                              null as materialNumber, \
                                              case when meterClass = 'Standpipe' then 'Customer Standpipe' else 'Water Meter' end as usageMeterType, \
                                              SUBSTR(meterSize, 1, INSTR(meterSize, ' ')-1) as meterSize, \
                                              SUBSTR(meterSize, INSTR(meterSize, ' ')+1) as meterSizeUnit, \
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
                                      where _RecordCurrent = 1 \
                                      and _RecordDeleted = 0")
     
    #Filter for active meter
    accessZ309TpropmeterDf = accessZ309TpropmeterDf.filter(col("rownum") == "1")

    #Drop unwanted columns
    accessZ309TpropmeterDf = accessZ309TpropmeterDf.drop(accessZ309TpropmeterDf.rownum)
    accessZ309TpropmeterDf.createOrReplaceTempView('ACCESS')
    #print(f'{accessZ309TpropmeterDf.count():,} rows in accessZ309TpropmeterDf')
    #display(accessZ309TpropmeterDf)
    #Meter Data from SAP ISU
    isu0ucDeviceAttrDf  = spark.sql(f"select 'ISU' as sourceSystemCode, \
                                      equipmentNumber as meterNumber, \
                                      deviceNumber as meterSerialNumber, \
                                      materialNumber, \
                                      trim(LEADING '0' FROM deviceSize) as meterSize, \
                                      'mm' as meterSizeUnit, \
                                      assetManufacturerName as manufacturerName, \
                                      manufacturerSerialNumber, \
                                      manufacturerModelNumber, \
                                      case when inspectionRelevanceIndicator = 'X' then 'Y' else 'N' end as inspectionRelevanceFlag, \
                                      meterfitteddate.meterFittedDate as meterFittedDate, \
                                      meterremovedddate.meterRemovedDate as meterRemovedDate \
                                      from cleansed.isu_0uc_device_attr a \
                                      left outer join \
                                      (select equipmentNumber as meterNumber,min(installationDate) as meterFittedDate \
                                       FROM cleansed.isu_0uc_deviceh_attr \
                                       group by equipmentNumber \
                                      )meterfitteddate on a.equipmentNumber=meterfitteddate.meterNumber \
                                      left outer join \
                                      (select equipmentNumber as meterNumber,deviceRemovalDate as meterRemovedDate \
                                       FROM cleansed.isu_0uc_deviceh_attr where validToDate = '9999-12-31' \
                                      )meterremovedddate on a.equipmentNumber=meterremovedddate.meterNumber  \
                                      where a._RecordCurrent = 1 \
                                      and a._RecordDeleted = 0 \
                                      ")
    
    #print(f'{isu0ucDeviceAttrDf.count():,} rows in isu0ucDeviceAttrDf')
    #display(isu0ucDeviceAttrDf)

    isu0ucDevCatAttrDf  = spark.sql(f"select distinct a.materialNumber, \
                                        case when functionClassCode = '9000' then 'Customer Standpipe' else 'Water Meter' end as usageMeterType, \
                                        case when functionClassCode = '1000' then 'Drinking Water' \
                                             when functionClassCode = '2000' then 'Recycled Water' else 'Drinking Water' end as waterType, \
                                        constructionClassCode as meterCategoryCode, \
                                        constructionClass as meterCategory, \
                                        deviceCategoryName as meterReadingType, \
                                        deviceCategoryDescription as meterDescription \
                                      from {ADS_DATABASE_CLEANSED}.isu_0uc_devcat_attr a \
                                      where a.functionClassCode in (1000, 2000, 9000) \
                                      and a._RecordCurrent = 1 \
                                      and a._RecordDeleted = 0")
    
    #print(f'{isu0ucDevCatAttrDf.count():,} rows in isu0ucDevCatAttrDf')
    #display(isu0ucDevCatAttrDf)  
    
    #2.JOIN TABLES
    df = isu0ucDeviceAttrDf.join(isu0ucDevCatAttrDf, isu0ucDeviceAttrDf.materialNumber == isu0ucDevCatAttrDf.materialNumber, how="inner") \
                            .drop(isu0ucDevCatAttrDf.materialNumber)
    #print(f'{df.count():,} rows after merge 1')
    #display(df)
        
    #re-order columns
    df = df.select('sourceSystemCode','meterNumber','meterSerialNumber','materialNumber','usageMeterType','meterSize','meterSizeUnit','waterType','meterCategoryCode','meterCategory',
                    'meterReadingType','meterDescription','meterFittedDate','meterRemovedDate','manufacturerName','manufacturerSerialNumber','manufacturerModelNumber','inspectionRelevanceFlag')
    
    #3. UNION
    df.createOrReplaceTempView('ISU')
    dfResult = spark.sql("with ACCESSMtrs as (select meterSerialNumber \
                                              from   ACCESS \
                                              minus \
                                              select meterSerialNumber \
                                              from   ISU) \
                          select a.* \
                          from   ACCESS a, \
                                 ACCESSMtrs b \
                          where  a.meterSerialNumber = b.meterSerialNumber \
                          union all \
                          select * \
                          from   ISU")
    #= accessZ309TpropmeterDf.union(df)
    #print(f'{df.count():,} rows after Union 1')
    #display(df)    
    
    dummyDimRecDf = spark.createDataFrame([("ISU","-1","Unknown"),("ACCESS","-2","Unknown"),("ISU","-3","NA"),("ACCESS","-4","NA")], ["sourceSystemCode", "meterNumber","meterDescription"])   
    #df = df.unionByName(dummyDimRecDf, allowMissingColumns = True)    
    #print(f'{df.count():,} rows after Union 2')
    #display(df)
    
    #4.SELECT / TRANSFORM
    
    #5.Apply schema definition
    schema = StructType([
                            StructField('meterSK', LongType(), False),
                            StructField('sourceSystemCode', StringType(), False),
                            StructField('meterNumber', StringType(), False),
                            StructField('meterSerialNumber', StringType(), True),
                            StructField('materialNumber', StringType(), True),
                            StructField('usageMeterType', StringType(), True),
                            StructField('meterSize', StringType(), True),
                            StructField('meterSizeUnit', StringType(), True),
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
    
    #display(df)    
    
    return dfResult, schema

# COMMAND ----------

df, schema = getMeter()
TemplateEtl(df, entity="dimMeter", businessKey="meterNumber", schema=schema, AddSK=True)

# COMMAND ----------

#Dummy Record to be added to Meter Dimension
#     ISUDummy = tuple(['ISU','-1',"9999-12-31","9999-12-31",""] + ['Unknown'] * (len(df.columns) - 5)) #this only works as long as all output columns are string
#     ACCESSDummy = tuple(['ACCESS','-2',"9999-12-31","9999-12-31",""] + ['Unknown'] * (len(df.columns) -5)) #this only works as long as all output columns are string
#     dummyDimRecDf = spark.createDataFrame([ISUDummy, ACCESSDummy], df.columns)
#    df.write.saveAsTable('curated.dimMeterTest_new3')

# COMMAND ----------

dbutils.notebook.exit("1")
