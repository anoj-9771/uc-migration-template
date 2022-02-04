# Databricks notebook source
#%run ../../includes/util-common

# COMMAND ----------

# Run the above commands only when running this notebook independently, otherwise the curated master notebook would take care of calling the above notebooks

# COMMAND ----------

###########################################################################################################################
# Function: getInstallation
#  GETS Installation DIMENSION 
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

def getInstallation():
    #spark.udf.register("TidyCase", GeneralToTidyCase) 

    #2.Load Cleansed layer table data into dataframe
    
    isu0ucinstallaAttrDf  = spark.sql(f"select 'ISU' as sourceSystemCode, \
                                          installationId, \
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
    print(f'Rows in isu0ucinstallaAttrDf:',isu0ucinstallaAttrDf.count())
    
    #Dummy Record to be added to Installation Dimension
    dummyDimRecDf = spark.createDataFrame([("ISU", "-1"),("ACCESS","-2")],["sourceSystemCode", "installationId"])
        
    #3.JOIN TABLES

    
    #4.UNION TABLES
    df = isu0ucinstallaAttrDf.unionByName(dummyDimRecDf, allowMissingColumns = True)    

    #5.SELECT / TRANSFORM
    df = df.select("sourceSystemCode", \
                    "installationId", \
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
       
    #6.Apply schema definition
    newSchema = StructType([
                            StructField('sourceSystemCode', StringType(), True),
                            StructField('installationId', StringType(), False),
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

    df = spark.createDataFrame(df.rdd, schema=newSchema)
    return df  
