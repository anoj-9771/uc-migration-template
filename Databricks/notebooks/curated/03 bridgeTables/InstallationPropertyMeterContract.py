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
                                      installationId, \
                                      propertyNumber \
                                      from {ADS_DATABASE_CURATED}.dimInstallation \
                                      where sourceSystemCode = 'ISU' and _RecordCurrent = 1 and _RecordDeleted = 0")     
    #print(f"Number of rows in dimInstallationDf: ", dimInstallationDf.count())
    #display(dimInstallationDf)
    
    dimContractDf = spark.sql(f"select \
                                    contractSK, \
                                    contractId, \
                                    installationId, \
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
                                installationId, \
                                logicalDeviceNumber, \
                                validFromDate, \
                                validToDate \
                                from {ADS_DATABASE_CURATED}.meterInstallation \
                                where _RecordCurrent = 1 and _RecordDeleted = 0")    
#    print(f"Number of rows in meterInstallationDf: ", meterInstallationDf.count())
#    display(meterInstallationDf)
    
    meterTimesliceDf = spark.sql(f"select \
                                meterSK, \
                                equipmentNumber as meterNumber, \
                                logicalDeviceNumber, \
                                validFromDate, \
                                validToDate \
                                from {ADS_DATABASE_CURATED}.meterTimeslice \
                                where _RecordCurrent = 1 and _RecordDeleted = 0")    
#    print(f"Number of rows in meterTimesliceDf: ", meterTimesliceDf.count())
#    display(meterTimesliceDf)    
    
    
    #2.Joins Tables
    df = dimInstallationDf.join(dimContractDf, (dimInstallationDf.installationId == dimContractDf.installationId) \
                                             & (current_date() >= dimContractDf.validFromDate) \
                                             & (current_date() <= dimContractDf.validToDate) \
                                             , how="left") \
                           .select(dimInstallationDf['*'], dimContractDf['contractSK'], dimContractDf['contractId'])    
#    print(f'{df.count():,} rows in df -1')
#    display(df)    
    
    df = df.join(meterInstallationDf, (df.installationId == meterInstallationDf.installationId) \
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
                      ,"installationId" \
                      ,"contractSK" \
                      ,"contractId" \
                      ,"meterSK" \
                      ,"meterNumber" \
                      ,"propertySK" \
                      ,"propertyNumber" \
                      ) 
                            
    #5.Apply schema definition
    schema = StructType([
                            StructField('installationSK', LongType(), True),
                            StructField('installationId', StringType(), True),
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
#TemplateEtl(df, entity="brgInstallationPropertyMeterContract", businessKey="installationSK", schema=schema, AddSK=False)
#Uncomment the above function call (TemplateElt) and delete the below lines once "DeltaSaveDataFrameToDeltaTable" is enhanced for 'Overwrite' mode
spark.sql("VACUUM curated.brgInstallationPropertyMeterContract ")

df.write \
  .format('delta') \
  .option("mergeSchema", "true") \
  .option("overwriteSchema", "true") \
  .mode("overwrite") \
  .save("dbfs:/mnt/datalake-curated/brginstallationpropertymetercontract/delta")

spark.sql("CREATE TABLE IF NOT EXISTS curated.brgInstallationPropertyMeterContract  USING DELTA LOCATION \'dbfs:/mnt/datalake-curated/brginstallationpropertymetercontract/delta\'")

verifyTableSchema(f"curated.brginstallationpropertymetercontract", schema)


# COMMAND ----------

dbutils.notebook.exit("1")
