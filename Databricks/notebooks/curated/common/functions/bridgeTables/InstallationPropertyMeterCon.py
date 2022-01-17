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
    df  = spark.sql(f"select distinct \
                           coalesce(inst.installationID,'-1') as installationID, \
                           coalesce(cont.contractID,'-1') as contractID, \
                           coalesce(prop.propertyNumber,'-1') as propertyNumber, \
                           coalesce(reg.equipmentNumber,'-1') as equipmentNumber \
                           from cleansed.ISU_0UCINSTALLA_ATTR_2 inst \
                           left join {ADS_DATABASE_CLEANSED}.ISU_0UCINSTALLAH_ATTR_2 insth on inst.installationId = insth.installationId and insth._RecordCurrent = 1 and insth._RecordDeleted = 0 \
                           left join {ADS_DATABASE_CLEANSED}.ISU_0UCCONTRACT_ATTR_2 cont on inst.installationId = cont.installationId and cont._RecordCurrent = 1 and cont._RecordDeleted = 0 \
                           left join {ADS_DATABASE_CLEANSED}.ISU_0UC_CONNOBJ_ATTR_2 prop on inst.propertyNumber = prop.propertyNumber and prop._RecordCurrent = 1 and prop._RecordDeleted = 0 \
                           left join {ADS_DATABASE_CLEANSED}.ISU_0UC_REGIST_ATTR reg on inst.installationId = reg.installationId and reg._RecordCurrent = 1 and reg._RecordDeleted = 0 \
                           where inst._RecordCurrent = 1 and inst._RecordDeleted = 0")
    
    print(f'{df.count():,} rows in df -1')
    display(df)    
     
    #3.Load dimension tables into dataframe

    dimInstallationDf = spark.sql(f"select dimInstallationSK, from {ADS_DATABASE_CURATED}.dimInstallation where _RecordCurrent = 1 and _RecordDeleted = 0") 
    dimContractDf = spark.sql(f"select dimContractSK from {ADS_DATABASE_CURATED}.dimContract where _RecordCurrent = 1 and _RecordDeleted = 0")
    dimPropertyDf = spark.sql(f"select dimPropertySK from {ADS_DATABASE_CURATED}.dimProperty where _RecordCurrent = 1 and _RecordDeleted = 0")
    dimMeterDf = spark.sql(f"select dimMeterSK,meterNumber from {ADS_DATABASE_CURATED}.dimMeter where sourceSystemCode = 'ISU' and _RecordCurrent = 1 and _RecordDeleted = 0")

    
    
    #4.Joins to derive SKs
    df = df.join(dimInstallationDf, (df.installationID == dimInstallationDf.installationID), how="left") \
            .select(df['*'], dimInstallationDf['dimInstallationSK'])    
    print(f'{df.count():,} rows in df -2')
    display(df)    
    df = df.join(dimContractDf, (df.contractID == dimContractDf.contractID), how="left") \
            .select(df['*'], dimContractDf['dimContractSK']) 
    print(f'{df.count():,} rows in df -3')
    display(df)  
    df = df.join(dimPropertyDf, (df.propertyNumber == dimPropertyDf.propertyNumber), how="left") \
            .select(df['*'], dimPropertyDf['dimPropertySK']) 
    print(f'{df.count():,} rows in df -4')
    display(df)    
    df = df.join(dimMeterDf, (df.equipmentNumber == dimMeterDf.equipmentNumber), how="left") \
            .select(df['*'], dimMeterDf['dimMeterSK'])     
    
    
    #5.SELECT / TRANSFORM
    #aggregating to address any duplicates due to failed SK lookups and dummy SKs being assigned in those cases
    df = df.selectExpr ( \
                       "dimInstallationSK" \
                      ,"dimContractSK" \
                      ,"dimPropertySK" \
                      ,"dimMeterSK" \
                      ) 
                            
    print(f'{df.count():,} rows in df -5')
    display(df)    
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
    print(f'{df.count():,} rows in df -6')
    display(df)   
    return df  
