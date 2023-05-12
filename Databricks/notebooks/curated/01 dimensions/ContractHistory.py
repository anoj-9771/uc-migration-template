# Databricks notebook source
###########################################################################################################################
# Loads Contract History dimension 
#############################################################################################################################
# Method
# 1.Load Cleansed layer table data into dataframe and transform
# 2.SCHEMA DEFINITION
#############################################################################################################################

# COMMAND ----------

# MAGIC %run ../common/common-curated-includeMain

# COMMAND ----------

def getContractHistory():

    #Contract Data from SAP ISU
    isuContractHistDf  = spark.sql(f"""select 'ISU' as sourceSystemCode
                                            ,ch.contractId
                                            ,ch.validFromDate
                                            ,ch.validToDate
                                            ,ch.CRMProduct
                                            ,ch.CRMObjectId
                                            ,ch.CRMDocumentItemNumber
                                            ,ch.marketingCampaign
                                            ,ch.individualContractId
                                            ,ch.productBeginFlag
                                            ,ch.productChangeFlag
                                            ,ch.replicationControlsCode
                                            ,ch.replicationControls
                                            ,uc.podUUID
                                            ,uc.headerTypeCode
                                            ,uc.headerType
                                            ,uc.isCancelledFlag
                                            ,ch.installationNumber
                                            ,ch.contractHeadGUID
                                            ,ch.contractPosGUID
                                            ,ch.productId
                                            ,ch.productGUID
                                            ,ch.createdDate
                                            ,ch.createdBy
                                            ,ch.lastChangedDate
                                            ,ch.lastChangedBy
                                            ,ch._RecordDeleted
                                            ,ch._DLCleansedZoneTimeStamp 
                                      from {ADS_DATABASE_CLEANSED}.isu_0uccontracth_attr_2 ch
                                      left outer join {ADS_DATABASE_CLEANSED}.crm_utilitiescontract uc
                                          on ch.contractId = uc.utilitiesContract and ch.validToDate = uc.contractEndDateE
                                          and uc._RecordCurrent = 1
                                      where ch._RecordCurrent = 1 and ch._RecordDeleted <> -1
                                      """)
    dummyDimRecDf = spark.createDataFrame([("-1","1900-01-01", "9999-12-31")], ["contractId","validFromDate","validToDate"])   
    dfResult = isuContractHistDf.unionByName(dummyDimRecDf, allowMissingColumns = True) 
    dfResult = dfResult.withColumn("validFromDate",col("validFromDate").cast("date")).withColumn("validToDate",col("validToDate").cast("date"))
    
    #5.Apply schema definition
    schema = StructType([
                            StructField('contractHistorySK', StringType(), False),
                            StructField('sourceSystemCode', StringType(), True),
                            StructField('contractId', StringType(), False),
                            StructField('validFromDate', DateType(), True),
                            StructField('validToDate', DateType(), False),
                            StructField('CRMProduct', StringType(), True),
                            StructField('CRMObjectId', StringType(), True),
                            StructField('CRMDocumentItemNumber', StringType(), True),
                            StructField('marketingCampaign', StringType(), True),
                            StructField('individualContractId', StringType(), True),
                            StructField('productBeginFlag', StringType(), True),
                            StructField('productChangeFlag', StringType(), True),
                            StructField('replicationControlsCode', StringType(), True),
                            StructField('replicationControls', StringType(), True),
                            StructField('podUUID', StringType(), True),
                            StructField('headerTypeCode', StringType(), True),
                            StructField('headerType', StringType(), True),
                            StructField('isCancelledFlag', StringType(), True),
                            StructField('installationNumber', StringType(), True),
                            StructField('contractHeadGUID', StringType(), True),
                            StructField('contractPosGUID', StringType(), True),
                            StructField('productId', StringType(), True),
                            StructField('productGUID', StringType(), True),
                            StructField('createdDate', DateType(), True),
                            StructField('createdBy', StringType(), True),
                            StructField('lastChangedDate', DateType(), True),
                            StructField('lastChangedBy', StringType(), True)
                      ])    
                
    return dfResult, schema

# COMMAND ----------

df, schema = getContractHistory()
TemplateTimeSliceEtlSCD(df, entity="dimContractHistory", businessKey="contractId,validToDate", schema=schema)

# COMMAND ----------

#dbutils.notebook.exit("1")
