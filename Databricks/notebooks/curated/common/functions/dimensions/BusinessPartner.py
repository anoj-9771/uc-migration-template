# Databricks notebook source
#%run ../../includes/util-common

# COMMAND ----------

# Run the above commands only when running this notebook independently, otherwise the curated master notebook would take care of calling the above notebooks

# COMMAND ----------

###########################################################################################################################
# Function: getBusinessPartner
#  GETS Business Partner DIMENSION 
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

def getBusinessPartner():
    #spark.udf.register("TidyCase", GeneralToTidyCase) 

    #2.Load Cleansed layer table data into dataframe
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
    dummyDimRecDf = spark.createDataFrame([("ISU", "-1")], ["sourceSystemCode", "businessPartnerNumber"])
    
    #3.JOIN TABLES
    df = isu0bpartnerAttrDf.join(crm0bpartnerAttrDf, isu0bpartnerAttrDf.businessPartnerNumber == crm0bpartnerAttrDf.businessPartnerNumber, how="left")\
                            .drop(crm0bpartnerAttrDf.businessPartnerNumber)
    
    df = df.withColumn('naturalPersonFlag', when ((col("isunaturalPersonFlag") == 'X') | (col("crmnaturalPersonFlag") == 'X'),'Y').otherwise('N')) \
           .drop("isunaturalPersonFlag").drop("crmnaturalPersonFlag")
        
    df = df.select("sourceSystemCode","businessPartnerNumber","validFromDate","validToDate", \
                   "businessPartnerCategoryCode","businessPartnerCategory","businessPartnerTypeCode", "businessPartnerType","externalNumber", \
                   "businessPartnerGUID", "firstName", "lastName", "middleName", "nickName", "titleCode", "title", "dateOfBirth", "dateOfDeath", \
                   "warWidowFlag", "deceasedFlag", "disabilityFlag", "goldCardHolderFlag", "naturalPersonFlag", "pensionCardFlag", "pensionType", \
                   "personNumber","personnelNumber","organizationName","organizationFoundedDate","createdDateTime","createdBy","changedDateTime", "changedBy") 
    
    #4.UNION TABLES
    df = df.unionByName(dummyDimRecDf, allowMissingColumns = True)
    
    #5.Apply schema definition
    newSchema = StructType([
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

    df = spark.createDataFrame(df.rdd, schema=newSchema)
    #5.SELECT / TRANSFORM
    
    
    return df  
