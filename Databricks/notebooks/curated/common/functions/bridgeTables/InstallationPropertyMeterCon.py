# Databricks notebook source
#%run ../../includes/util-common

# COMMAND ----------

# Run the above commands only when running this notebook independently, otherwise the curated master notebook would take care of calling the above notebooks

# COMMAND ----------

###########################################################################################################################
# Function: getInstallationPropertyMeterCon
#  Gets Installation and it's associated Contract, Meter and Property SK value
# Returns:
#  Dataframe of Installation and it's associated Contract, Meter and Property SK value
#############################################################################################################################
# Method
# 1.Create Function
# 2.Load Cleansed layer table data into dataframe and transform
# 3.JOIN TABLES
# 4.UNION TABLES
# 5.SELECT / TRANSFORM
#############################################################################################################################
#1.Create Function

def getInstallationPropertyMeterCon():
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

    dim0bpartnerDf = spark.sql(f"select sourceSystemCode, dimBusinessPartnerSK, businessPartnerNumber, validFromDate,validToDate \
                                      from {ADS_DATABASE_CURATED}.dimBusinessPartner \
                                      where _RecordCurrent = 1 and _RecordDeleted = 0")    
       
    dim0bpGroupDf = spark.sql(f"select sourceSystemCode, dimBusinessPartnerGroupSK, businessPartnerGroupNumber,validFromDate,validToDate \
                                      from {ADS_DATABASE_CURATED}.dimBusinessPartnerGroup \
                                      where _RecordCurrent = 1 and _RecordDeleted = 0") 
    
    dummyDimRecDf = spark.sql(f"select dimBusinessPartnerSK as dummyDimSK, sourceSystemCode, 'dimBusinessPartner' as dimension from {ADS_DATABASE_CURATED}.dimBusinessPartner where businessPartnerNumber = '-1' \
                            union select dimBusinessPartnerGroupSK as dummyDimSK, sourceSystemCode, 'dimBusinessPartnerGroup' as dimension from {ADS_DATABASE_CURATED}.dimBusinessPartnerGroup \
                                                                                                                                            where businessPartnerGroupNumber = '-1'")
    
    
    #4.Joins to derive SKs
    isu0bpRelationsAttrDf = isu0bpRelationsAttrDf.join(dim0bpartnerDf, (isu0bpRelationsAttrDf.businessPartnerNumber == dim0bpartnerDf.businessPartnerNumber) \
                               & (isu0bpRelationsAttrDf.sourceSystemCode == dim0bpartnerDf.sourceSystemCode), how="left") \
                    .select(isu0bpRelationsAttrDf['*'], dim0bpartnerDf['dimBusinessPartnerSK'])    
    
    isu0bpRelationsAttrDf = isu0bpRelationsAttrDf.join(dim0bpGroupDf, (isu0bpRelationsAttrDf.businessPartnerGroupNumber == dim0bpGroupDf.businessPartnerGroupNumber) \
                               & (isu0bpRelationsAttrDf.sourceSystemCode == dim0bpGroupDf.sourceSystemCode), how="left") \
                    .select(isu0bpRelationsAttrDf['*'], dim0bpGroupDf['dimBusinessPartnerGroupSK'])
    
    #6.Joins to derive SKs of dummy dimension(-1) records, to be used when the lookup fails for dimensionSk
  
    isu0bpRelationsAttrDf = isu0bpRelationsAttrDf.join(dummyDimRecDf, (dummyDimRecDf.dimension == 'dimBusinessPartner') \
                               & (isu0bpRelationsAttrDf.sourceSystemCode == dummyDimRecDf.sourceSystemCode), how="left") \
                    .select(isu0bpRelationsAttrDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyBusinessPartnerSK'))
    
    isu0bpRelationsAttrDf = isu0bpRelationsAttrDf.join(dummyDimRecDf, (dummyDimRecDf.dimension == 'dimBusinessPartnerGroup') \
                               & (isu0bpRelationsAttrDf.sourceSystemCode == dummyDimRecDf.sourceSystemCode), how="left") \
                    .select(isu0bpRelationsAttrDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyBusinessPartnerGroupSK'))
    
    
    #7.SELECT / TRANSFORM
    #aggregating to address any duplicates due to failed SK lookups and dummy SKs being assigned in those cases
    isu0bpRelationsAttrDf = isu0bpRelationsAttrDf.selectExpr ( \
                                           "sourceSystemCode" \
                                          ,"coalesce(dimBusinessPartnerGroupSK, dummyBusinessPartnerGroupSK) as businessPartnerGroupSK" \
                                          ,"coalesce(dimBusinessPartnerSK, dummyBusinessPartnerSK) as businessPartnerSK" \
                                          ,"validFromDate" \
                                          ,"validToDate" \
                                          ,"relationshipNumber" \
                                          ,"relationshipTypeCode" \
                                          ,"relationshipType" \
                                         ) 
                            
    
    #6.Apply schema definition
    newSchema = StructType([
                            StructField('sourceSystemCode', StringType(), True),
                            StructField('businessPartnerGroupSK', StringType(), False),
                            StructField('businessPartnerSK', StringType(), False),
                            StructField('validFromDate', DateType(), False),
                            StructField('validToDate', DateType(), True),
                            StructField('relationshipNumber', StringType(), True),
                            StructField('relationshipTypeCode', StringType(), True),
                            StructField('relationshipType', StringType(), True)
                      ]) 

    df = spark.createDataFrame(isu0bpRelationsAttrDf.rdd, schema=newSchema)
   
    return df  
