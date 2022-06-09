# Databricks notebook source
###########################################################################################################################
# Loads BUSINESSPARTNER dimension 
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

def getBusinessPartner():

    #1.Load Cleansed layer table data into dataframe
    #Business Partner Data from SAP ISU
    isu0bpartnerAttrDf  = spark.sql(f"select 'ISU' as sourceSystemCode, \
                                      businessPartnerNumber, \
                                      validFromDate, \
                                      validToDate, \
                                      businessPartnerCategoryCode, \
                                      businessPartnerCategory, \
                                      businessPartnerTypeCode, \
                                      businessPartnerType, \
                                      externalBusinessPartnerNumber as externalNumber, \
                                      businessPartnerGUID, \
                                      firstName, \
                                      lastName, \
                                      middleName, \
                                      nickName, \
                                      titleCode, \
                                      title, \
                                      dateOfBirth, \
                                      dateOfDeath, \
                                      naturalPersonIndicator as isunaturalPersonFlag, \
                                      personNumber, \
                                      personnelNumber, \
                                      case when businessPartnerCategoryCode = '2' then \
                                                      concat(coalesce(organizationName1, ''), ' ', coalesce(organizationName2, ''), ' ',coalesce(organizationName3, '')) else organizationName1 end as organizationName, \
                                      case when businessPartnerCategoryCode = '2' then organizationFoundedDate else null end as organizationFoundedDate, \
                                      createdDateTime, \
                                      createdBy, \
                                      changedDateTime, \
                                      changedBy \
                                      FROM {ADS_DATABASE_CLEANSED}.isu_0bpartner_attr \
                                      where businessPartnerCategoryCode in ('1','2') \
                                      and _RecordCurrent = 1 \
                                      and _RecordDeleted = 0")
    
    #Business Partner Data from SAP CRM
    crm0bpartnerAttrDf  = spark.sql(f"select businessPartnerNumber, \
                                      warWidowIndicator as warWidowFlag, \
                                      deceasedIndicator as deceasedFlag, \
                                      disabilityIndicator as disabilityFlag, \
                                      goldCardHolderIndicator as goldCardHolderFlag, \
                                      naturalPersonIndicator as crmnaturalPersonFlag, \
                                      pensionConcessionCardIndicator as pensionCardFlag, \
                                      pensionType as pensionType \
                                      FROM {ADS_DATABASE_CLEANSED}.crm_0bpartner_attr \
                                      where businessPartnerCategoryCode in ('1','2') \
                                      and _RecordCurrent = 1 \
                                      and _RecordDeleted = 0")  
    
    #Dummy Record to be added to Business Partner Group Dimension
    dummyDimRecDf = spark.createDataFrame([("-1", "Unknown")], ["businessPartnerNumber","firstName"])
    
    #2.JOIN TABLES
    df = isu0bpartnerAttrDf.join(crm0bpartnerAttrDf, isu0bpartnerAttrDf.businessPartnerNumber == crm0bpartnerAttrDf.businessPartnerNumber, how="left")\
                            .drop(crm0bpartnerAttrDf.businessPartnerNumber)
    
    df = df.withColumn('naturalPersonFlag', when ((col("isunaturalPersonFlag") == 'X') | (col("crmnaturalPersonFlag") == 'X'),'Y').otherwise('N')) \
           .drop("isunaturalPersonFlag").drop("crmnaturalPersonFlag")
        
    df = df.select("sourceSystemCode","businessPartnerNumber","validFromDate","validToDate", \
                   "businessPartnerCategoryCode","businessPartnerCategory","businessPartnerTypeCode", "businessPartnerType","externalNumber", \
                   "businessPartnerGUID", "firstName", "lastName", "middleName", "nickName", "titleCode", "title", "dateOfBirth", "dateOfDeath", \
                   "warWidowFlag", "deceasedFlag", "disabilityFlag", "goldCardHolderFlag", "naturalPersonFlag", "pensionCardFlag", "pensionType", \
                   "personNumber","personnelNumber","organizationName","organizationFoundedDate","createdDateTime","createdBy","changedDateTime", "changedBy") 
    
    #3.UNION TABLES
    df = df.unionByName(dummyDimRecDf, allowMissingColumns = True)
    
    #4.SELECT / TRANSFORM
    
    #5.Apply schema definition
    schema = StructType([
                            StructField('businessPartnerSK', StringType(), False),
                            StructField('sourceSystemCode', StringType(), True),
                            StructField('businessPartnerNumber', StringType(), False),
                            StructField('validFromDate', DateType(), True),
                            StructField('validToDate', DateType(), True),
                            StructField('businessPartnerCategoryCode', StringType(), True),
                            StructField('businessPartnerCategory', StringType(), True),
                            StructField('businessPartnerTypeCode', StringType(), True),
                            StructField('businessPartnerType', StringType(), True),
                            StructField('externalNumber', StringType(), True),       
                            StructField('businessPartnerGUID', StringType(), True),
                            StructField('firstName', StringType(), True),
                            StructField('lastName', StringType(), True),
                            StructField('middleName', StringType(), True),
                            StructField('nickName', StringType(), True),
                            StructField('titleCode', StringType(), True),
                            StructField('title', StringType(), True),
                            StructField('dateOfBirth', DateType(), True),                        
                            StructField('dateOfDeath', DateType(), True), 
                            StructField('warWidowFlag', StringType(), True), 
                            StructField('deceasedFlag', StringType(), True), 
                            StructField('disabilityFlag', StringType(), True), 
                            StructField('goldCardHolderFlag', StringType(), True), 
                            StructField('naturalPersonFlag', StringType(), True), 
                            StructField('pensionCardFlag', StringType(), True), 
                            StructField('pensionType', StringType(), True),        
                            StructField('personNumber', StringType(), True), 
                            StructField('personnelNumber', StringType(), True), 
                            StructField('organizationName', StringType(), True), 
                            StructField('organizationFoundedDate', DateType(), True), 
                            StructField('createdDateTime', TimestampType(), True),   
                            StructField('createdBy', StringType(), True), 
                            StructField('changedDateTime', TimestampType(), True), 
                            StructField('changedBy', StringType(), True)       
                      ]) 

    return df, schema

# COMMAND ----------

df, schema = getBusinessPartner()
TemplateEtl(df, entity="dimBusinessPartner", businessKey="businessPartnerNumber", schema=schema, writeMode=ADS_WRITE_MODE_MERGE, AddSK=True)

# COMMAND ----------

dbutils.notebook.exit("1")
