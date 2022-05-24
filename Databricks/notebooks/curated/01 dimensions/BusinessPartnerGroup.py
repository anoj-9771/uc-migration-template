# Databricks notebook source
###########################################################################################################################
# Loads BUSINESSPARTNERGROUP dimension 
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

def getBusinessPartnerGroup():

    #1.Load Cleansed layer table data into dataframe
    #Business Partner Group Data from SAP ISU
    isu0bpartnerAttrDf  = spark.sql(f"select 'ISU' as sourceSystemCode, \
                                      a.businessPartnerNumber as businessPartnerGroupNumber, \
                                      a.validFromDate as validFromDate, \
                                      a.validToDate as validToDate, \
                                      a.businessPartnerGroupCode as businessPartnerGroupCode, \
                                      a.businessPartnerGroup as businessPartnerGroup, \
                                      a.nameGroup1 as businessPartnerGroupName1, \
                                      a.nameGroup2 as businessPartnerGroupName2, \
                                      a.externalBusinessPartnerNumber as externalNumber, \
                                      a.createdDateTime as createdDateTime, \
                                      a.createdBy as createdBy, \
                                      a.changedDateTime as lastUpdatedDateTime, \
                                      a.changedBy as lastUpdatedBy \
                                      FROM {ADS_DATABASE_CLEANSED}.isu_0bpartner_attr a \
                                      where a.businessPartnerCategoryCode = '3' \
                                      and a._RecordCurrent = 1 \
                                      and a._RecordDeleted = 0")
    #Business Partner Group Data from SAP CRM
    crm0bpartnerAttrDf  = spark.sql(f"select b.businessPartnerNumber as businessPartnerGroupNumber, \
                                      b.paymentAssistSchemeIndicator as paymentAssistSchemeFlag, \
                                      b.billAssistIndicator as billAssistFlag, \
                                      b.kidneyDialysisIndicator as kidneyDialysisFlag \
                                      FROM {ADS_DATABASE_CLEANSED}.crm_0bpartner_attr b \
                                      where b.businessPartnerCategoryCode = '3' \
                                      and b._RecordCurrent = 1 \
                                      and b._RecordDeleted = 0")      
    
    #Dummy Record to be added to Business Partner Group Dimension
    dummyDimRecDf = spark.createDataFrame([("-1", "Unknown")], ["businessPartnerGroupNumber","businessPartnerGroupName1"])
    
    #2.JOIN TABLES
    df = isu0bpartnerAttrDf.join(crm0bpartnerAttrDf, isu0bpartnerAttrDf.businessPartnerGroupNumber == crm0bpartnerAttrDf.businessPartnerGroupNumber, how="left")\
                            .drop(crm0bpartnerAttrDf.businessPartnerGroupNumber)
    
    df = df.withColumn('paymentAssistSchemeFlag', when ((col("paymentAssistSchemeFlag") == 'X'),'Y').otherwise('N')) \
           .withColumn('billAssistFlag', when ((col("billAssistFlag") == 'X'),'Y').otherwise('N')) \
           .withColumn('kidneyDialysisFlag', when ((col("kidneyDialysisFlag") == 'X'),'Y').otherwise('N'))
    
    df = df.select("sourceSystemCode","businessPartnerGroupNumber","validFromDate","validToDate", \
                                                "businessPartnerGroupCode","businessPartnerGroup","businessPartnerGroupName1","businessPartnerGroupName2","externalNumber", \
                                                "paymentAssistSchemeFlag","billAssistFlag","kidneyDialysisFlag","createdDateTime","createdBy","lastUpdatedDateTime","lastUpdatedBy") 
    
    #3.UNION TABLES
    df = df.unionByName(dummyDimRecDf, allowMissingColumns = True)

    #4.SELECT / TRANSFORM

    #5.Apply schema definition
    schema = StructType([
                            StructField('businessPartnerGroupSK', LongType(), False),
                            StructField('sourceSystemCode', StringType(), True),
                            StructField('businessPartnerGroupNumber', StringType(), False),
                            StructField('validFromDate', DateType(), True),
                            StructField('validToDate', DateType(), True),
                            StructField('businessPartnerGroupCode', StringType(), True),
                            StructField('businessPartnerGroup', StringType(), True),
                            StructField('businessPartnerGroupName1', StringType(), True),
                            StructField('businessPartnerGroupName2', StringType(), True),
                            StructField('externalNumber', StringType(), True),
                            StructField('paymentAssistSchemeFlag', StringType(), True),
                            StructField('billAssistFlag', StringType(), True),
                            StructField('kidneyDialysisFlag', StringType(), True),
                            StructField('createdDateTime', TimestampType(), True),
                            StructField('createdBy', StringType(), True),
                            StructField('lastUpdatedDateTime', TimestampType(), True),
                            StructField('lastUpdatedBy', StringType(), True)                        
                      ])

    return df, schema

# COMMAND ----------

df, schema = getBusinessPartnerGroup()
TemplateEtl(df, entity="dimBusinessPartnerGroup", businessKey="businessPartnerGroupNumber", schema=schema, writeMode=ADS_WRITE_MODE_MERGE, AddSK=True)

# COMMAND ----------

dbutils.notebook.exit("1")
