# Databricks notebook source
#%run ../../includes/util-common

# COMMAND ----------

# Run the above commands only when running this notebook independently, otherwise the curated master notebook would take care of calling the above notebooks

# COMMAND ----------

###########################################################################################################################
# Function: getBusinessPartnerGroupRelation
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

def getBusinessPartnerGroupRelation():
    #spark.udf.register("TidyCase", GeneralToTidyCase) 

    #2.Load Cleansed layer table data into dataframe
    #Business Partner Group Relations Data from SAP ISU
    isu0bpRelationsAttrDf  = spark.sql(f"select 'ISU' as sourceSystemCode, \
                                      businessPartnerNumber1 as businessPartnerGroupNumber, \
                                      businessPartnerNumber2 as businessPartnerNumber, \
                                      validFromDate, \
                                      validToDate, \
                                      businessPartnerRelationshipNumber as relationshipNumber, \
                                      relationshipTypeCode, \
                                      relationshipType \
                                      FROM {ADS_DATABASE_CLEANSED}.isu_0bp_relations_attr \
                                      where relationshipTypeCode = 'ZSW009' \
                                      and relationshipDirection = '1'\
                                      and _RecordCurrent = 1 \
                                      and _RecordDeleted = 0")
    
    #3.Load dimension tables into dataframe

    dim0bpartnerDf = spark.sql(f"select sourceSystemCode, dimBusinessPartnerSK \
                                      from {ADS_DATABASE_CURATED}.dimBusinessPartner \
                                      where _RecordCurrent = 1 and _RecordDeleted = 0")    
     
    dim0bpGroupDf = spark.sql(f"select sourceSystemCode, dimBusinessPartnerGroupSK \
                                      from {ADS_DATABASE_CURATED}.dimBusinessPartnerGroup \
                                      where _RecordCurrent = 1 and _RecordDeleted = 0")   
    
    #4.Joins to derive SKs

    
    #5.UNION TABLES
    df = df.unionByName(dummyDimRecDf, allowMissingColumns = True)
    
    #6.Apply schema definition
    newSchema = StructType([
                            StructField('sourceSystemCode', StringType(), True),
                            StructField('businessPartnerNumber', StringType(), True),
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
                            StructField('changedBy', StringType(), True),        
                      ]) 

    df = spark.createDataFrame(df.rdd, schema=newSchema)
    #7.SELECT / TRANSFORM
    
    
    return df  
