# Databricks notebook source
###########################################################################################################################
# Loads DEVICE dimension 
#############################################################################################################################
# Method
# 1.Load Cleansed layer table data into dataframe and transform
# 2.SCHEMA DEFINITION
#############################################################################################################################

# COMMAND ----------

# MAGIC %run ../common/common-curated-includeMain

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Need to Run Device History Before Device

# COMMAND ----------

# MAGIC %run ./DeviceHistory

# COMMAND ----------

# MAGIC %run ./DeviceCharacteristics

# COMMAND ----------

def getDevice():

    #Device Data from SAP ISU
    isuDeviceDf  = spark.sql(f"""select 'ISU' as sourceSystemCode,
                                      d.equipmentNumber as deviceNumber,
                                      d.materialNumber,
                                      d.deviceNumber as deviceId,
                                      cast(d.inspectionRelevanceIndicator as string) as inspectionRelevanceIndicator,
                                      d.deviceSize,
                                      'mm' as deviceSizeUnit,
                                      d.assetManufacturerName,
                                      d.manufacturerSerialNumber,
                                      d.manufacturerModelNumber,
                                      d.objectNumber,
                                      dc.functionClassCode,
                                      dc.functionClass,
                                      dc.constructionClassCode,
                                      dc.constructionClass,
                                      dc.deviceCategoryName,
                                      dc.deviceCategoryDescription,
                                      dc.ptiNumber,
                                      dc.ggwaNumber,
                                      dc.certificationRequirementType,
                                      d._RecordDeleted 
                                from {ADS_DATABASE_CLEANSED}.isu_0uc_device_attr d
                                    left outer join {ADS_DATABASE_CLEANSED}.isu_0uc_devcat_attr dc
                                          on d.materialNumber = dc.materialNumber
                                where d._RecordCurrent = 1 
                                      and dc._RecordCurrent = 1 
                              """)
    
    dummyDimRecDf = spark.createDataFrame(["-1"], "string").toDF("deviceNumber")   
    dfResult = isuDeviceDf.unionByName(dummyDimRecDf, allowMissingColumns = True)    
    
    #Apply schema definition
    schema = StructType([
                            StructField('deviceSK', StringType(), False),
                            StructField('sourceSystemCode', StringType(), True),
                            StructField('deviceNumber', StringType(), False),
                            StructField('materialNumber', StringType(), True),
                            StructField('deviceId', StringType(), True),
                            StructField('inspectionRelevanceIndicator', StringType(), True),
                            StructField('deviceSize', StringType(), True),
                            StructField('deviceSizeUnit', StringType(), True),
                            StructField('assetManufacturerName', StringType(), True),
                            StructField('manufacturerSerialNumber', StringType(), True),
                            StructField('manufacturerModelNumber', StringType(), True),
                            StructField('objectNumber', StringType(), True),
                            StructField('functionClassCode', StringType(), True),
                            StructField('functionClass', StringType(), True),
                            StructField('constructionClassCode', StringType(), True),
                            StructField('constructionClass', StringType(), True),
                            StructField('deviceCategoryName', StringType(), True),
                            StructField('deviceCategoryDescription', StringType(), True),
                            StructField('ptiNumber', StringType(), True),
                            StructField('ggwaNumber', StringType(), True),   
                            StructField('certificationRequirementType', StringType(), True)
                      ])    
    
    return dfResult, schema

# COMMAND ----------

df, schema = getDevice()

curnt_table = f'{ADS_DATABASE_CURATED_V2}.dimDevice'
curnt_pk = 'deviceNumber' 
curnt_recordStart_pk = 'deviceNumber'
history_table = f'{ADS_DATABASE_CURATED_V2}.dimDeviceHistory'
history_table_pk = 'deviceNumber'
history_table_pk_convert = 'deviceNumber'

df_ = appendRecordStartFromHistoryTable(df,history_table,history_table_pk,curnt_pk,history_table_pk_convert,curnt_recordStart_pk)
updateDBTableWithLatestRecordStart(df_, curnt_table, curnt_pk)

TemplateEtlSCD(df_, entity="dimDevice", businessKey="deviceNumber", schema=schema)

# COMMAND ----------

dbutils.notebook.exit("1")
