# Databricks notebook source
###########################################################################################################################
# Loads CONTRACT dimension
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

def getContract():

    #1.Load current Cleansed layer table data into dataframe
    df = spark.sql(f"select  co.contractId, \
                             coalesce(coh.validFromDate,to_date('1900-01-01','yyyy-MM-dd')) as validFromDate, \
                             coalesce(coh.validToDate,to_date('9999-12-31','yyyy-MM-dd')) as validToDate, \
                             'ISU' as sourceSystemCode, \
                             least(coh.validFromDate, co.createdDate) as contractStartDate, \
                             coh.validToDate as contractEndDate, \
                             co.invoiceContractsJointlyCode, \
                             co.moveInDate, \
                             co.moveOutDate, \
                             ca.contractAccountNumber, \
                             ca.contractAccountCategory, \
                             ca.applicationAreaCode, \
                             co.installationNumber \
                             from {ADS_DATABASE_CLEANSED}.isu_0UCCONTRACT_ATTR_2 co left outer join \
                                  {ADS_DATABASE_CLEANSED}.isu_0UCCONTRACTH_ATTR_2 coh on co.contractId = coh.contractId \
                                                           and coh.deletedFlag is null and coh._RecordDeleted = 0 and coh._RecordCurrent = 1 left outer join \
                                  {ADS_DATABASE_CLEANSED}.isu_0CACONT_ACC_ATTR_2 ca on co.contractAccountNumber = ca.contractAccountNumber \
                                                           and ca._RecordDeleted = 0 and ca._RecordCurrent = 1 \
                             where co._RecordDeleted = 0 \
                             and   co._RecordCurrent = 1 \
                     ")

    df.createOrReplaceTempView('allcontracts')
    #2.JOIN TABLES  

    #3.UNION TABLES
    #Create dummy record
    
    dummyDimRecDf = spark.createDataFrame([("-1","1900-01-01", "9999-12-31")], ["contractId", "validFromDate", "validToDate"])
                                     
    df = df.unionByName(dummyDimRecDf,allowMissingColumns = True)
    df = df.withColumn("validFromDate",col("validFromDate").cast("date")).withColumn("validToDate",col("validToDate").cast("date"))
    
    #4.SELECT / TRANSFORM
    df = df.selectExpr( \
                  'contractId' \
                , 'validFromDate' \
                , 'validToDate' \
                , 'sourceSystemCode' \
                , 'contractStartDate' \
                , 'contractEndDate' \
                , 'invoiceContractsJointlyCode' \
                , 'moveInDate' \
                , 'moveOutDate' \
                , 'contractAccountNumber' \
                , 'contractAccountCategory' \
                , 'applicationAreaCode' \
                , 'installationNumber')

    #5.Apply schema definition
    schema = StructType([
                            StructField('contractSK', StringType(), False),
                            StructField('contractId', StringType(), False),
                            StructField('validFromDate', DateType(), True),
                            StructField('validToDate', DateType(), False),
                            StructField('sourceSystemCode', StringType(), True),
                            StructField('contractStartDate', DateType(), True),
                            StructField('contractEndDate', DateType(), True),
                            StructField('invoiceContractsJointlyCode', StringType(), True),
                            StructField('moveInDate', DateType(), True),
                            StructField('moveOutDate', DateType(), True),
                            StructField('contractAccountNumber', StringType(), True),
                            StructField('contractAccountCategory', StringType(), True),
                            StructField('applicationAreaCode', StringType(), True),
                            StructField('installationNumber', StringType(), True)
                      ])

#    display(df)
    return df, schema

# COMMAND ----------

df, schema = getContract()
TemplateEtl(df,  entity="dimContract", businessKey="contractId,validToDate", schema=schema, writeMode=ADS_WRITE_MODE_OVERWRITE, AddSK=True)

# COMMAND ----------

dbutils.notebook.exit("1")
