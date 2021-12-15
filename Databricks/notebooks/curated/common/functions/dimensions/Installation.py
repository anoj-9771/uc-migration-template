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
                                          lastChangedBy as changedBy\
                                      FROM {ADS_DATABASE_CLEANSED}.isu_0ucinstalla_attr_2 \
                                      WHERE _RecordCurrent = 1 \
                                      AND _RecordDeleted = 0")
    
    isu0ucinstallahAttr2Df  = spark.sql(f"select \
                                           installationId, \
                                           validFromDate, \
                                           validToDate, \
                                           rateCategoryCode, \
                                           rateCategory, \
                                           'TBD' as industryCode, \
                                           industry, \
                                           billingClassCode, \
                                           billingClass, \
                                           'TBD' as industrySystemCode, \
                                           'TBD' as industrySystem \
                                      FROM {ADS_DATABASE_CLEANSED}.isu_0ucinstallah_attr_2 \
                                      WHERE validToDate in (to_date('9999-12-31'),to_date('2099-12-31')) \
                                      AND _RecordCurrent = 1 \
                                      AND _RecordDeleted = 0")
    
    isu0ucIsu32  = spark.sql(f"select \
                                  installationId, \
                                  disconnectionDocumentNumber, \
                                  disconnectionActivityPeriod, \
                                  disconnectionObjectNumber, \
                                  disconnectionDate, \
                                  disconnectionActivityTypeCode, \
                                  disconnectionActivityType, \
                                  disconnectionObjectTypeCode, \
                                  disconnectionReasonCode, \
                                  disconnectionReason, \
                                  disconnectionReconnectionStatusCode, \
                                  disconnectionReconnectionStatus, \
                                  disconnectionDocumentStatusCode, \
                                  disconnectionDocumentStatus, \
                                  ProcessingVariantCode as disconnectionProcessingVariantCode, \
                                  ProcessingVariant as disconnectionProcessingVariant \
                                FROM {ADS_DATABASE_CLEANSED}.isu_0uc_isu_32 \
                                WHERE referenceObjectTypeCode = 'INSTLN' \
                                AND validToDate in (to_date('9999-12-31'),to_date('2099-12-31')) \
                                AND _RecordCurrent = 1 \
                                AND _RecordDeleted = 0")
    
    #Dummy Record to be added to Installation Dimension
    dummyDimRecDf = spark.createDataFrame([("ISU", "-1","2099-12-31")], ["sourceSystemCode", "installationId","validToDate"])
    dummyDimRecDf = dummyDimRecDf.withColumn("validToDate",dummyDimRecDf['validToDate'].cast(DateType()))
    
    #3.JOIN TABLES
    df = isu0ucinstallaAttrDf.join(isu0ucinstallahAttr2Df, isu0ucinstallaAttrDf.installationId == isu0ucinstallahAttr2Df.installationId, how="inner") \
                             .drop(isu0ucinstallahAttr2Df.installationId)
  
    df = df.join(isu0ucIsu32, df.installationId == isu0ucIsu32.installationId, how="left") \
           .drop(isu0ucIsu32.installationId)    
    
    df = df.select("sourceSystemCode", \
                    "installationId", \
                    "validFromDate", \
                    "validToDate", \
                    "divisionCode", \
                    "division", \
                    "rateCategoryCode", \
                    "rateCategory", \
                    "industryCode", \
                    "industry", \
                    "billingClassCode", \
                    "billingClass", \
                    "industrySystemCode", \
                    "industrySystem", \
                    "meterReadingControlCode", \
                    "meterReadingControl", \
                    "authorizationGroupCode", \
                    "serviceTypeCode", \
                    "serviceType", \
                    "disconnectionDocumentNumber", \
                    "disconnectionActivityPeriod", \
                    "disconnectionObjectNumber", \
                    "disconnectionDate", \
                    "disconnectionActivityTypeCode", \
                    "disconnectionActivityType", \
                    "disconnectionObjectTypeCode", \
                    "disconnectionReasonCode", \
                    "disconnectionReason", \
                    "disconnectionReconnectionStatusCode", \
                    "disconnectionReconnectionStatus", \
                    "disconnectionDocumentStatusCode", \
                    "disconnectionDocumentStatus", \
                    "disconnectionProcessingVariantCode", \
                    "disconnectionProcessingVariant", \
                    "createdDate", \
                    "createdBy", \
                    "changedDate", \
                    "changedBy") 
    
    #4.UNION TABLES
    df = df.unionByName(dummyDimRecDf, allowMissingColumns = True)
   
    #5.Apply schema definition
    newSchema = StructType([
                            StructField('sourceSystemCode', StringType(), True),
                            StructField('installationId', StringType(), False),
                            StructField('validFromDate', DateType(), True),
                            StructField('validToDate', DateType(), False),
                            StructField('divisionCode', StringType(), True),
                            StructField('division', StringType(), True),
                            StructField('rateCategoryCode', StringType(), True),
                            StructField('rateCategory', StringType(), True),
                            StructField('industryCode', StringType(), True),
                            StructField('industry', StringType(), True),
                            StructField('billingClassCode', StringType(), True),
                            StructField('billingClass', StringType(), True),
                            StructField('industrySystemCode', StringType(), True),
                            StructField('industrySystem', StringType(), True),
                            StructField('meterReadingControlCode', StringType(), True),
                            StructField('meterReadingControl', StringType(), True),
                            StructField('authorizationGroupCode', StringType(), True),
                            StructField('serviceTypeCode', StringType(), True),
                            StructField('serviceType', StringType(), True),
                            StructField('disconnectionDocumentNumber', StringType(), True),
                            StructField('disconnectionActivityPeriod', StringType(), True),
                            StructField('disconnectionObjectNumber', StringType(), True),
                            StructField('disconnectionDate', DateType(), True),
                            StructField('disconnectionActivityTypeCode', StringType(), True),
                            StructField('disconnectionActivityType', StringType(), True),
                            StructField('disconnectionObjectTypeCode', StringType(), True),
                            StructField('disconnectionReasonCode', StringType(), True),
                            StructField('disconnectionReason', StringType(), True),
                            StructField('disconnectionReconnectionStatusCode', StringType(), True),
                            StructField('disconnectionReconnectionStatus', StringType(), True),
                            StructField('disconnectionDocumentStatusCode', StringType(), True),
                            StructField('disconnectionDocumentStatus', StringType(), True),
                            StructField('disconnectionProcessingVariantCode', StringType(), True),
                            StructField('disconnectionProcessingVariant', StringType(), True),
                            StructField('createdDate', DateType(), True),
                            StructField('createdBy', StringType(), True),
                            StructField('changedDate', DateType(), True),
                            StructField('changedBy', StringType(), True)
                      ])

    df = spark.createDataFrame(df.rdd, schema=newSchema)
       
    #5.SELECT / TRANSFORM

    
    return df  
