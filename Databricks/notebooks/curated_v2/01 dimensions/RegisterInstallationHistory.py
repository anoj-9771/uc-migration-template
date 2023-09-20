# Databricks notebook source
###########################################################################################################################
# Loads Register Installation History dimension 
#############################################################################################################################
# Method
# 1.Load Cleansed layer table data into dataframe and transform
# 2.SCHEMA DEFINITION
#############################################################################################################################

# COMMAND ----------

# MAGIC %run ../common/common-curated-includeMain

# COMMAND ----------

def getRegisterInstallationHistory():

    #Register Data from SAP ISU
    isuRegisterInstallHistDf  = spark.sql(f"""select 'ISU' as sourceSystemCode,
                                                  logicalRegisterNumber,
                                                  installationNumber,
                                                  validToDate,
                                                  validFromDate,
                                                  operationCode,
                                                  operationDescription,
                                                  rateTypeCode,
                                                  rateType,
                                                  registerNotRelevantToBilling,
                                                  rateFactGroupCode,
                                                  rateFactGroup,
                                                  rih._RecordDeleted,
                                                  rih._DLCleansedZoneTimeStamp 
                                              from {ADS_DATABASE_CLEANSED}.isu_0UC_REGINST_STR_ATTR rih
                                              where rih._RecordCurrent = 1 
                                        """)
    
    dummyDimRecDf = spark.createDataFrame([("-1","-1","1900-01-01", "9999-12-31")], ["logicalRegisterNumber","installationNumber","validFromDate","validToDate"])   
    dfResult = isuRegisterInstallHistDf.unionByName(dummyDimRecDf, allowMissingColumns = True)   
    dfResult = dfResult.withColumn("validFromDate",col("validFromDate").cast("date")).withColumn("validToDate",col("validToDate").cast("date"))
    
    #5.Apply schema definition
    schema = StructType([
                            StructField('registerInstallationHistorySK', StringType(), False),
                            StructField('sourceSystemCode', StringType(), True),
                            StructField('logicalRegisterNumber', StringType(), False),
                            StructField('installationNumber', StringType(), False),
                            StructField('validToDate', DateType(), False),
                            StructField('validFromDate', DateType(), True),
                            StructField('operationCode', StringType(), True),
                            StructField('operationDescription', StringType(), True),
                            StructField('rateTypeCode', StringType(), True),
                            StructField('rateType', StringType(), True),
                            StructField('registerNotRelevantToBilling', StringType(), True),
                            StructField('rateFactGroupCode', StringType(), True),
                            StructField('rateFactGroup', StringType(), True)
                      ])    
    
    return dfResult, schema

# COMMAND ----------

df, schema = getRegisterInstallationHistory()
TemplateTimeSliceEtlSCD(df, entity="dimRegisterInstallationHistory", businessKey="logicalRegisterNumber,installationNumber,validToDate", schema=schema)

# COMMAND ----------

dbutils.notebook.exit("1")
