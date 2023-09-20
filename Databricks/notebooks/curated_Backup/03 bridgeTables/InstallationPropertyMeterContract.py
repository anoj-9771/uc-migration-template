# Databricks notebook source
###########################################################################################################################
# Loads INSTALLATIONPROPERTYMETERCONTRACT bridge table
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

def getInstallationPropertyMeterContract():
         
    #1.Load dimension/relationship tables into dataframe
    dimInstallationDf = spark.sql(f"select \
                                      installationSK, \
                                      installationNumber, \
                                      propertyNumber \
                                      from {ADS_DATABASE_CURATED}.dimInstallation \
                                      where sourceSystemCode = 'ISU' and divisionCode = '10' \
                                      and current_date() >= coalesce(changedDate, createdDate) \
                                      and _RecordCurrent = 1 and _RecordDeleted = 0")     
    #print(f"Number of rows in dimInstallationDf: ", dimInstallationDf.count())
    #display(dimInstallationDf)
    
    dimContractDf = spark.sql(f"select \
                                    contractSK, \
                                    contractId, \
                                    installationNumber, \
                                    validFromDate, \
                                    validToDate \
                                    from {ADS_DATABASE_CURATED}.dimContract \
                                    where sourceSystemCode = 'ISU' and _RecordCurrent = 1 and _RecordDeleted = 0")
    #print(f"Number of rows in dimContractDf: ", dimContractDf.count())
    #display(dimContractDf)
    
    dimPropertyDf = spark.sql(f"select \
                                    propertyNumber, \
                                    propertySK \
                                    from {ADS_DATABASE_CURATED}.dimProperty \
                                    where sourceSystemCode = 'ISU' and _RecordCurrent = 1 and _RecordDeleted = 0")
#    print(f"Number of rows in dimInstallationDf: ", dimPropertyDf.count())
#    display(dimPropertyDf)

#     dimMeterDf = spark.sql(f"select \
#                                 meterSK, \
#                                 meterNumber \
#                                 from {ADS_DATABASE_CURATED}.dimMeter \
#                                 where sourceSystemCode = 'ISU' and _RecordCurrent = 1 and _RecordDeleted = 0")
#     print(f"Number of rows in dimMeterDf: ", dimMeterDf.count())
#     display(dimMeterDf)
    
    meterInstallationDf = spark.sql(f"select \
                                meterInstallationSK, \
                                installationSK, \
                                installationNumber, \
                                logicalDeviceNumber, \
                                validFromDate, \
                                validToDate \
                                from {ADS_DATABASE_CURATED}.dimMeterInstallation \
                                where _RecordCurrent = 1 and _RecordDeleted = 0")    
#    print(f"Number of rows in meterInstallationDf: ", meterInstallationDf.count())
#    display(meterInstallationDf)
    
    meterTimesliceDf = spark.sql(f"select \
                                meterSK, \
                                equipmentNumber as meterNumber, \
                                logicalDeviceNumber, \
                                validFromDate, \
                                validToDate \
                                from {ADS_DATABASE_CURATED}.dimMeterTimeslice \
                                where _RecordCurrent = 1 and _RecordDeleted = 0")    
#    print(f"Number of rows in meterTimesliceDf: ", meterTimesliceDf.count())
#    display(meterTimesliceDf)    
    
    
    #2.Joins Tables
    df = dimInstallationDf.join(dimContractDf, (dimInstallationDf.installationNumber == dimContractDf.installationNumber) \
                                             & (current_date() >= dimContractDf.validFromDate) \
                                             & (current_date() <= dimContractDf.validToDate) \
                                             , how="left") \
                           .select(dimInstallationDf['*'], dimContractDf['contractSK'], dimContractDf['contractId'])    
#    print(f'{df.count():,} rows in df -1')
#    display(df)    
    
    df = df.join(meterInstallationDf, (df.installationNumber == meterInstallationDf.installationNumber) \
                                    & (current_date() >= meterInstallationDf.validFromDate) \
                                    & (current_date() <= meterInstallationDf.validToDate) \
                                    , how="left") \
            .select(df['*'], meterInstallationDf['logicalDeviceNumber']) 
#    print(f'{df.count():,} rows in df -2')
#    display(df)    
            
#    df = df.join(dimMeterDf, (df.logicalDeviceNumber == dimMeterDf.logicalDeviceNumber), how="left") \
#             .select(df['*'], dimMeterDf['meterSK'], dimMeterDf['meterNumber'])     
#    print(f'{df.count():,} rows in df -3')
#    display(df)   
    
    df = df.join(meterTimesliceDf, (df.logicalDeviceNumber == meterTimesliceDf.logicalDeviceNumber) \
                                 & (current_date() >= meterTimesliceDf.validFromDate) \
                                 & (current_date() <= meterTimesliceDf.validToDate) \
                                 , how="left") \
            .select(df['*'], meterTimesliceDf['meterSK'], meterTimesliceDf['meterNumber'])     
#    print(f'{df.count():,} rows in df -3')
#    display(df)  

    df = df.join(dimPropertyDf, (df.propertyNumber == dimPropertyDf.propertyNumber) \
                 , how="left") \
            .select(df['*'], dimPropertyDf['propertySK']) 
#    print(f'{df.count():,} rows in df -4')
#    display(df)

    #3.UNION TABLES
    #4.SELECT / TRANSFORM
    df = df.selectExpr ( \
                       "installationSK" \
                      ,"installationNumber" \
                      ,"contractSK" \
                      ,"contractId" \
                      ,"meterSK" \
                      ,"meterNumber" \
                      ,"propertySK" \
                      ,"propertyNumber" \
                      ).dropDuplicates()
                            
    #5.Apply schema definition
    schema = StructType([
                            StructField('installationSK', LongType(), True),
                            StructField('installationNumber', StringType(), True),
                            StructField('contractSK', LongType(), True),
                            StructField('contractId', StringType(), True),
                            StructField('meterSK', LongType(), True),
                            StructField('meterNumber', StringType(), True),
                            StructField('propertySK', LongType(), True),
                            StructField('propertyNumber', StringType(), True)
                      ]) 

#    print(f'{df.count():,} rows in df')
#    display(df)
    
    return df, schema  

# COMMAND ----------

df, schema = getInstallationPropertyMeterContract()
TemplateEtl(df, entity="brgInstallationPropertyMeterContract", businessKey="installationSK", schema=schema, writeMode=ADS_WRITE_MODE_OVERWRITE, AddSK=False)

# COMMAND ----------

dbutils.notebook.exit("1")
