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
         
    #2.Load dimension/relationship tables into dataframe
    dimInstallationDf = spark.sql(f"select \
                                      dimInstallationSK, \
                                      installationId, \
                                      propertyNumber \
                                      from {ADS_DATABASE_CURATED}.dimInstallation \
                                      where sourceSystemCode = 'ISU' and _RecordCurrent = 1 and _RecordDeleted = 0")     
    print(f"Number of rows in dimInstallationDf: ", dimInstallationDf.count())
    display(dimInstallationDf)
    
    dimContractDf = spark.sql(f"select \
                                    dimContractSK, \
                                    contractId, \
                                    installationId \
                                    from {ADS_DATABASE_CURATED}.dimContract \
                                    where sourceSystemCode = 'ISU' and _RecordCurrent = 1 and _RecordDeleted = 0")
    print(f"Number of rows in dimContractDf: ", dimContractDf.count())
    display(dimContractDf)
    
    dimPropertyDf = spark.sql(f"select \
                                    propertyNumber, \
                                    dimPropertySK \
                                    from {ADS_DATABASE_CURATED}.dimProperty \
                                    where sourceSystemCode = 'ISU' and _RecordCurrent = 1 and _RecordDeleted = 0")
    print(f"Number of rows in dimInstallationDf: ", dimPropertyDf.count())
    display(dimPropertyDf)
    
    dimMeterDf = spark.sql(f"select \
                                dimMeterSK, \
                                meterNumber, \
                                logicalDeviceNumber \
                                from {ADS_DATABASE_CURATED}.dimMeter \
                                where sourceSystemCode = 'ISU' and _RecordCurrent = 1 and _RecordDeleted = 0")
    print(f"Number of rows in dimMeterDf: ", dimMeterDf.count())
    display(dimMeterDf)
    
    meterInstallationDf = spark.sql(f"select \
                                meterInstallationSK, \
                                installationSK, \
                                installationId, \
                                logicalDeviceNumber \
                                from {ADS_DATABASE_CURATED}.meterInstallation \
                                where _RecordCurrent = 1 and _RecordDeleted = 0")    
    print(f"Number of rows in meterInstallationDf: ", meterInstallationDfmeterInstallationDf.count())
    display(meterInstallationDf)
    
    
    #3.Joins Tables
    df = dimInstallationDf.join(dimContractDf, (dimInstallationDf.installationID == dimContractDf.installationID), how="left") \
            .select(dimInstallationDf['*'], dimContractDf['*'])    
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
