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

def getDevice():

    #Device Data from SAP ISU
    isuDeviceDf  = spark.sql(f"""select 'ISU' as sourceSystemCode,
                                      d.equipmentNumber as deviceNumber,
                                      d.materialNumber,
                                      d.deviceNumber as deviceId,
                                      cast(d.inspectionRelevanceIndicator as string) as inspectionRelevanceIndicator,
                                      d.deviceSize,
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
TemplateEtlSCD(df, entity="dimDevice", businessKey="deviceNumber", schema=schema)

# COMMAND ----------

dbutils.notebook.exit("1")
