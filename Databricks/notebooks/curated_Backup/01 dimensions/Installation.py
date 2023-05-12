# Databricks notebook source
###########################################################################################################################
# Loads INSTALLATION dimension 
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

def getInstallation():

    #1.Load Cleansed layer table data into dataframe
    isu0ucinstallaAttrDf  = spark.sql(f"select 'ISU' as sourceSystemCode, \
                                          installationNumber, \
                                          divisionCode, \
                                          division, \
                                          meterReadingControlCode, \
                                          meterReadingControl, \
                                          authorizationGroupCode, \
                                          serviceTypeCode, \
                                          serviceType, \
                                          createdDate, \
                                          createdBy, \
                                          lastChangedDate as changedDate, \
                                          lastChangedBy as changedBy, \
                                          propertyNumber \
                                      FROM {ADS_DATABASE_CLEANSED}.isu_0ucinstalla_attr_2 \
                                      WHERE _RecordCurrent = 1 \
                                      AND _RecordDeleted = 0")
    #print(f'Rows in isu0ucinstallaAttrDf:',isu0ucinstallaAttrDf.count())
    
    #Dummy Record to be added to Installation Dimension
    dummyDimRecDf = spark.createDataFrame([("-1", "Unknown")],["installationNumber", "division"])
        
    #2.JOIN TABLES

    
    #3.UNION TABLES
    df = isu0ucinstallaAttrDf.unionByName(dummyDimRecDf, allowMissingColumns = True)    

    #4.SELECT / TRANSFORM
    df = df.select("sourceSystemCode", \
                    "installationNumber", \
                    "divisionCode", \
                    "division", \
                    "meterReadingControlCode", \
                    "meterReadingControl", \
                    "authorizationGroupCode", \
                    "serviceTypeCode", \
                    "serviceType", \
                    "createdDate", \
                    "createdBy", \
                    "changedDate", \
                    "changedBy", \
                    "propertyNumber")
       
    #5.Apply schema definition
    schema = StructType([
                            StructField('installationSK', StringType(), False),
                            StructField('sourceSystemCode', StringType(), True),
                            StructField('installationNumber', StringType(), False),
                            StructField('divisionCode', StringType(), True),
                            StructField('division', StringType(), True),
                            StructField('meterReadingControlCode', StringType(), True),
                            StructField('meterReadingControl', StringType(), True),
                            StructField('authorizationGroupCode', StringType(), True),
                            StructField('serviceTypeCode', StringType(), True),
                            StructField('serviceType', StringType(), True),
                            StructField('createdDate', DateType(), True),
                            StructField('createdBy', StringType(), True),
                            StructField('changedDate', DateType(), True),
                            StructField('changedBy', StringType(), True),
                            StructField('propertyNumber', StringType(), True)
                      ])

    return df, schema  

# COMMAND ----------

df, schema = getInstallation()
TemplateEtl(df, entity="dimInstallation", businessKey="installationNumber", schema=schema, writeMode=ADS_WRITE_MODE_OVERWRITE, AddSK=True)

# COMMAND ----------

dbutils.notebook.exit("1")