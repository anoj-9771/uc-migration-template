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
                                                  rateFactGroupCode
                                              from {ADS_DATABASE_CLEANSED}.isu_0UC_REGINST_STR_ATTR rih
                                              where rih._RecordCurrent = 1 and  rih._RecordDeleted = 0
                                        """)
    
    dummyDimRecDf = spark.createDataFrame([("ISU","-1","-1","1900-01-01", "9999-12-31")], ["sourceSystemCode","logicalRegisterNumber","installationNumber","validFromDate","validToDate"])   
    dfResult = isuRegisterInstallHistDf.unionByName(dummyDimRecDf, allowMissingColumns = True)    
    
    #5.Apply schema definition
    schema = StructType([
                            StructField('registerInstallationHistorySK', StringType(), False),
                            StructField('sourceSystemCode', StringType(), False),
                            StructField('logicalRegisterNumber', StringType(), False),
                            StructField('installationNumber', StringType(), False),
                            StructField('validToDate', DateType(), False),
                            StructField('validFromDate', DateType(), True),
                            StructField('operationCode', StringType(), True),
                            StructField('operationDescription', StringType(), True),
                            StructField('rateTypeCode', StringType(), True),
                            StructField('rateType', StringType(), True),
                            StructField('registerNotRelevantToBilling', StringType(), True),
                            StructField('rateFactGroupCode', StringType(), True)
                      ])    
    
    return dfResult, schema

# COMMAND ----------

df, schema = getRegisterInstallationHistory()
TemplateTimeSliceEtlSCD(df, entity="dimRegisterInstallationHistory", businessKey="logicalRegisterNumber,installationNumber,validToDate", schema=schema)

# COMMAND ----------

dbutils.notebook.exit("1")
