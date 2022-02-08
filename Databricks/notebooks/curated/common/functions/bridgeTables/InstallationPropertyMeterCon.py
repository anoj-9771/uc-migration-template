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
                                      dimInstallationSK as installationSK, \
                                      installationId, \
                                      propertyNumber \
                                      from {ADS_DATABASE_CURATED}.dimInstallation \
                                      where sourceSystemCode = 'ISU' and _RecordCurrent = 1 and _RecordDeleted = 0")     
    print(f"Number of rows in dimInstallationDf: ", dimInstallationDf.count())
    #display(dimInstallationDf)
    
    dimContractDf = spark.sql(f"select \
                                    dimContractSK as contractSK, \
                                    contractId, \
                                    installationId, \
                                    validFromDate, \
                                    validToDate \
                                    from {ADS_DATABASE_CURATED}.dimContract \
                                    where sourceSystemCode = 'ISU' and _RecordCurrent = 1 and _RecordDeleted = 0")
    print(f"Number of rows in dimContractDf: ", dimContractDf.count())
    #display(dimContractDf)
    
    dimPropertyDf = spark.sql(f"select \
                                    propertyNumber, \
                                    dimPropertySK as propertySK, \
                                    propertyStartDate, \
                                    propertyEndDate \
                                    from {ADS_DATABASE_CURATED}.dimProperty \
                                    where sourceSystemCode = 'ISU' and _RecordCurrent = 1 and _RecordDeleted = 0")
    print(f"Number of rows in dimInstallationDf: ", dimPropertyDf.count())
    #display(dimPropertyDf)
    
#     dimMeterDf = spark.sql(f"select \
#                                 dimMeterSK, \
#                                 meterNumber \
#                                 from {ADS_DATABASE_CURATED}.dimMeter \
#                                 where sourceSystemCode = 'ISU' and _RecordCurrent = 1 and _RecordDeleted = 0")
#     #print(f"Number of rows in dimMeterDf: ", dimMeterDf.count())
#     #display(dimMeterDf)
    
    meterInstallationDf = spark.sql(f"select \
                                meterInstallationSK, \
                                installationSK, \
                                installationId, \
                                logicalDeviceNumber, \
                                validFromDate, \
                                validToDate \
                                from {ADS_DATABASE_CURATED}.meterInstallation \
                                where _RecordCurrent = 1 and _RecordDeleted = 0")    
    print(f"Number of rows in meterInstallationDf: ", meterInstallationDf.count())
    #display(meterInstallationDf)
    
    meterTimesliceDf = spark.sql(f"select \
                                meterSK, \
                                equipmentNumber as meterNumber, \
                                logicalDeviceNumber, \
                                validFromDate, \
                                validToDate \
                                from {ADS_DATABASE_CURATED}.meterTimeslice \
                                where _RecordCurrent = 1 and _RecordDeleted = 0")    
    print(f"Number of rows in meterTimesliceDf: ", meterTimesliceDf.count())
    #display(meterTimesliceDf)    
    
    
    #3.Joins Tables
    df = dimInstallationDf.join(dimContractDf, (dimInstallationDf.installationId == dimContractDf.installationId) \
                                             & (current_date() >= dimContractDf.validFromDate) \
                                             & (current_date() <= dimContractDf.validToDate) \
                                             , how="left") \
                           .select(dimInstallationDf['*'], dimContractDf['contractSK'], dimContractDf['contractId'])    
    print(f'{df.count():,} rows in df -1')
    #display(df)    
    
    df = df.join(meterInstallationDf, (df.installationId == meterInstallationDf.installationId) \
                                    & (current_date() >= meterInstallationDf.validFromDate) \
                                    & (current_date() <= meterInstallationDf.validToDate) \
                                    , how="left") \
            .select(df['*'], meterInstallationDf['logicalDeviceNumber']) 
    print(f'{df.count():,} rows in df -2')
    #display(df)    
            
#     df = df.join(dimMeterDf, (df.logicalDeviceNumber == dimMeterDf.logicalDeviceNumber), how="left") \
#             .select(df['*'], dimMeterDf['dimMeterSK'], dimMeterDf['meterNumber'])     
#     #print(f'{df.count():,} rows in df -3')
#     #display(df)   
    
    df = df.join(meterTimesliceDf, (df.logicalDeviceNumber == meterTimesliceDf.logicalDeviceNumber) \
                                 & (current_date() >= meterTimesliceDf.validFromDate) \
                                 & (current_date() <= meterTimesliceDf.validToDate) \
                                 , how="left") \
            .select(df['*'], meterTimesliceDf['meterSK'], meterTimesliceDf['meterNumber'])     
    print(f'{df.count():,} rows in df -3')
    #display(df)  

    df = df.join(dimPropertyDf, (df.propertyNumber == dimPropertyDf.propertyNumber) \
                 & (current_date() >= dimPropertyDf.propertyStartDate) \
                 & (current_date() <= dimPropertyDf.propertyEndDate) \
                 , how="left") \
            .select(df['*'], dimPropertyDf['propertySK']) 
    print(f'{df.count():,} rows in df -4')
    #display(df) 
    
    #5.SELECT / TRANSFORM
    df = df.selectExpr ( \
                       "installationSK" \
                      ,"installationId" \
                      ,"contractSK" \
                      ,"contractId" \
                      ,"meterSK" \
                      ,"meterNumber" \
                      ,"propertySK" \
                      ,"propertyNumber" \
                      ) 
                            
    #6.Apply schema definition
    newSchema = StructType([
                            StructField('dimInstallationSK', LongType(), False),
                            StructField('installationId', StringType(), True),
                            StructField('dimContractSK', LongType(), True),
                            StructField('contractId', StringType(), True),
                            StructField('dimMeterSK', LongType(), True),
                            StructField('meterNumber', StringType(), True),
                            StructField('dimPropertySK', LongType(), True),
                            StructField('propertyNumber', StringType(), True)
                      ]) 

    df = spark.createDataFrame(df.rdd, schema=newSchema)  
    #print(f'{df.count():,} rows in df -5')
    #display(df)
    
    return df  
