# Databricks notebook source
###########################################################################################################################
# Loads BUSINESSPARTNERGROUP relationship table
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

def getBusinessPartnerGroupRelationship():

    #1.Load Cleansed layer table data into dataframe
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
                                      where relationshipDirection = '1'\
                                      and _RecordCurrent = 1 \
                                      and _RecordDeleted = 0")
    
     
    #1.Load dimension tables into dataframe

    dim0bpartnerDf = spark.sql(f"select sourceSystemCode, businessPartnerSK, businessPartnerNumber, validFromDate,validToDate \
                                      from {ADS_DATABASE_CURATED}.dimBusinessPartner \
                                      where _RecordCurrent = 1 and _RecordDeleted = 0")    
       
    dim0bpGroupDf = spark.sql(f"select sourceSystemCode, businessPartnerGroupSK, businessPartnerGroupNumber,validFromDate,validToDate \
                                      from {ADS_DATABASE_CURATED}.dimBusinessPartnerGroup \
                                      where _RecordCurrent = 1 and _RecordDeleted = 0") 
    
    dummyDimRecDf = spark.sql(f"select businessPartnerSK as dummyDimSK, sourceSystemCode, 'dimBusinessPartner' as dimension from {ADS_DATABASE_CURATED}.dimBusinessPartner where businessPartnerNumber = '-1' \
                            union select businessPartnerGroupSK as dummyDimSK, sourceSystemCode, 'dimBusinessPartnerGroup' as dimension from {ADS_DATABASE_CURATED}.dimBusinessPartnerGroup \
                                                                                                                                            where businessPartnerGroupNumber = '-1'")
    
    
    #2.Joins to derive SKs
    isu0bpRelationsAttrDf = isu0bpRelationsAttrDf.join(dim0bpartnerDf, (isu0bpRelationsAttrDf.businessPartnerNumber == dim0bpartnerDf.businessPartnerNumber) \
                               & (isu0bpRelationsAttrDf.sourceSystemCode == dim0bpartnerDf.sourceSystemCode), how="left") \
                    .select(isu0bpRelationsAttrDf['*'], dim0bpartnerDf['businessPartnerSK'])    
    
    isu0bpRelationsAttrDf = isu0bpRelationsAttrDf.join(dim0bpGroupDf, (isu0bpRelationsAttrDf.businessPartnerGroupNumber == dim0bpGroupDf.businessPartnerGroupNumber) \
                               & (isu0bpRelationsAttrDf.sourceSystemCode == dim0bpGroupDf.sourceSystemCode), how="left") \
                    .select(isu0bpRelationsAttrDf['*'], dim0bpGroupDf['businessPartnerGroupSK'])
    
    #2.Joins to derive SKs of dummy dimension(-1) records, to be used when the lookup fails for dimensionSk
  
    isu0bpRelationsAttrDf = isu0bpRelationsAttrDf.join(dummyDimRecDf, (dummyDimRecDf.dimension == 'dimBusinessPartner') \
                               & (isu0bpRelationsAttrDf.sourceSystemCode == dummyDimRecDf.sourceSystemCode), how="left") \
                    .select(isu0bpRelationsAttrDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyBusinessPartnerSK'))
    
    isu0bpRelationsAttrDf = isu0bpRelationsAttrDf.join(dummyDimRecDf, (dummyDimRecDf.dimension == 'dimBusinessPartnerGroup') \
                               & (isu0bpRelationsAttrDf.sourceSystemCode == dummyDimRecDf.sourceSystemCode), how="left") \
                    .select(isu0bpRelationsAttrDf['*'], dummyDimRecDf['dummyDimSK'].alias('dummyBusinessPartnerGroupSK'))
    
    #3.UNION TABLES    
    #4.SELECT / TRANSFORM
    isu0bpRelationsAttrDf = isu0bpRelationsAttrDf.selectExpr ( \
                                           "sourceSystemCode" \
                                          ,"coalesce(businessPartnerGroupSK, dummyBusinessPartnerGroupSK) as businessPartnerGroupSK" \
                                          ,"coalesce(businessPartnerSK, dummyBusinessPartnerSK) as businessPartnerSK" \
                                          ,"validFromDate" \
                                          ,"validToDate" \
                                          ,"relationshipNumber" \
                                          ,"relationshipTypeCode" \
                                          ,"relationshipType" \
                                         ) 
                            
    
    #5.Apply schema definition
    schema = StructType([
                            StructField('sourceSystemCode', StringType(), True),
                            StructField('businessPartnerGroupSK', StringType(), False),
                            StructField('businessPartnerSK', StringType(), False),
                            StructField('validFromDate', DateType(), False),
                            StructField('validToDate', DateType(), True),
                            StructField('relationshipNumber', StringType(), True),
                            StructField('relationshipTypeCode', StringType(), True),
                            StructField('relationshipType', StringType(), True)
                      ]) 

    return isu0bpRelationsAttrDf, schema  

# COMMAND ----------

df, schema = getBusinessPartnerGroupRelationship()
TemplateEtl(df, entity="brgBusinessPartnerGroupRelationship", businessKey="businessPartnerGroupSK,businessPartnerSK,validFromDate", schema=schema, AddSK=False)

# COMMAND ----------

dbutils.notebook.exit("1")
