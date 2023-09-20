# Databricks notebook source
###########################################################################################################################
# Loads Device History dimension 
#############################################################################################################################
# Method
# 1.Load Cleansed layer table data into dataframe and transform
# 2.SCHEMA DEFINITION
#############################################################################################################################

# COMMAND ----------

# MAGIC %run ../common/common-curated-includeMain

# COMMAND ----------

def getDeviceHistory():

    #Device Data from SAP ISU
    isuDeviceHistDf  = spark.sql(f"""select 'ISU' as sourceSystemCode,
                                          equipmentNumber as deviceNumber,
                                          validToDate,
                                          validFromDate,
                                          logicalDeviceNumber,
                                          deviceLocation,
                                          deviceCategoryCombination,
                                          registerGroupCode,
                                          registerGroup,
                                          installationDate,
                                          deviceRemovalDate,
                                          activityReasonCode,
                                          activityReason,
                                          windingGroup,
                                          advancedMeterCapabilityGroup,
                                          messageAttributeId,
                                          min(installationDate) over (partition by equipmentNumber) as firstInstallationDate,
                                          case when validFromDate  <= current_date and validToDate >= current_date then deviceRemovalDate
                                               else null end as lastDeviceRemovalDate,
                                           dh._RecordDeleted,
                                           dh._DLCleansedZoneTimeStamp 
                                      from {ADS_DATABASE_CLEANSED}.isu.0UC_DEVICEH_ATTR dh
                                      where dh._RecordCurrent = 1 and dh._RecordDeleted <> -1
                                      """)
    dummyDimRecDf = spark.createDataFrame([("-1","1900-01-01", "9999-12-31")], ["deviceNumber","validFromDate","validToDate"])   
    dfResult = isuDeviceHistDf.unionByName(dummyDimRecDf, allowMissingColumns = True) 
    dfResult = dfResult.withColumn("validFromDate",col("validFromDate").cast("date")).withColumn("validToDate",col("validToDate").cast("date"))
    
    #5.Apply schema definition
    schema = StructType([
                            StructField('deviceHistorySK', StringType(), False),
                            StructField('sourceSystemCode', StringType(), True),
                            StructField('deviceNumber', StringType(), False),
                            StructField('validToDate', DateType(), False),
                            StructField('validFromDate', DateType(), True),
                            StructField('logicalDeviceNumber', LongType(), True),
                            StructField('deviceLocation', StringType(), True),
                            StructField('deviceCategoryCombination', StringType(), True),
                            StructField('registerGroupCode', StringType(), True),
                            StructField('registerGroup', StringType(), True),
                            StructField('installationDate', DateType(), True),
                            StructField('deviceRemovalDate', DateType(), True),
                            StructField('activityReasonCode', StringType(), True),
                            StructField('activityReason', StringType(), True),
                            StructField('windingGroup', StringType(), True),
                            StructField('advancedMeterCapabilityGroup', IntegerType(), True),
                            StructField('messageAttributeId', IntegerType(), True),
                            StructField('firstInstallationDate', DateType(), True),
                            StructField('lastDeviceRemovalDate', DateType(), True)
                      ])    
                
    return dfResult, schema

# COMMAND ----------

df, schema = getDeviceHistory()
TemplateTimeSliceEtlSCD(df, entity="dim.deviceHistory", businessKey="deviceNumber,validToDate", schema=schema)

# COMMAND ----------

#dbutils.notebook.exit("1")
