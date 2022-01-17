# Databricks notebook source
###########################################################################################################################
# Function: getContract
#  GETS Contract DIMENSION 
# Returns:
#  Dataframe of transformed Location
#############################################################################################################################
# Method
# 1.Create Function
# 2.Load Cleansed layer table data into dataframe and transform
# 3.JOIN TABLES
# 4.UNION TABLES
# 5.SELECT / TRANSFORM
#############################################################################################################################
#1.Create Function
def getContract():
    #DimContract
    #2.Load current Cleansed layer table data into dataframe

    df = spark.sql(f"select  co.contractId, \
                             coalesce(coh.validFromDate,to_date('1900-01-01','yyyy-MM-dd')) as validFromDate, \
                             coh.validToDate, \
                             'ISU' as sourceSystemCode, \
                             least(coh.validFromDate, co.createdDate) as contractStartDate, \
                             coh.validToDate as contractEndDate, \
                             case when co.invoiceContractsJointly = 'X' then 'Y' else 'N' end as invoiceJointlyFlag, \
                             co.moveInDate, \
                             co.moveOutDate, \
                             ca.contractAccountNumber, \
                             ca.contractAccountCategory, \
                             ca.applicationArea \
                             from {ADS_DATABASE_CLEANSED}.isu_0UCCONTRACT_ATTR_2 co left outer join \
                                  {ADS_DATABASE_CLEANSED}.isu_0UCCONTRACTH_ATTR_2 coh on co.contractId = coh.contractId left outer join \
                                  {ADS_DATABASE_CLEANSED}.isu_0CACONT_ACC_ATTR_2 ca on co.contractAccountNumber = ca.contractAccountNumber \
                             where co._RecordDeleted = 0 \
                             and   co._RecordCurrent = 1 \
                             and   coh._RecordDeleted = 0 \
                             and   coh._RecordCurrent = 1 \
                             and   ca._RecordDeleted = 0 \
                             and   ca._RecordCurrent = 1 \
                     ")

    df.createOrReplaceTempView('allcontracts')
    #3.JOIN TABLES  

    #4.UNION TABLES
    #Create dummy record
    dummyDimRecDf = spark.sql("select '-1' as contractId, \
                                     to_date('1900-01-01','yyyy-MM-dd') as validFromDate, \
                                     to_date('2099-12-31','yyyy-MM-dd') as validToDate, \
                                     'ISU' as sourceSystemCode, \
                                     to_date('1900-01-01','yyyy-MM-dd') as contractStartDate, \
                                     to_date('2099-12-31','yyyy-MM-dd') as contractEndDate, \
                                     'N' as invoiceJointlyFlag, \
                                     to_date('1900-01-01','yyyy-MM-dd') as moveInDate, \
                                     to_date('2099-12-31','yyyy-MM-dd') as moveOutDate, \
                                     'Unknown' as contractAccountNumber, \
                                     'Unknown' as contractAccountCategory, \
                                     'Unknown' as applicationArea")
                                     
    df = df.unionByName(dummyDimRecDf)

    #5.SELECT / TRANSFORM
    df = df.selectExpr( \
                  'contractId' \
                , 'validFromDate' \
                , 'validToDate' \
                , 'sourceSystemCode' \
                , 'contractStartDate' \
                , 'contractEndDate' \
                , 'invoiceJointlyFlag' \
                , 'moveInDate' \
                , 'moveOutDate' \
                , 'contractAccountNumber' \
                , 'contractAccountCategory' \
                , 'applicationArea' 
            )

    #6.Apply schema definition
    newSchema = StructType([
                            StructField('contractId', StringType(), False),
                            StructField('validFromDate', DateType(), False),
                            StructField('validToDate', DateType(), True),
                            StructField('sourceSystemCode', StringType(), True),
                            StructField('contractStartDate', DateType(), True),
                            StructField('contractEndDate', DateType(), True),
                            StructField('invoiceJointlyFlag', StringType(), True),
                            StructField('moveInDate', DateType(), True),
                            StructField('moveOutDate', DateType(), True),
                            StructField('contractAccountNumber', StringType(), True),
                            StructField('contractAccountCategory', StringType(), True),
                            StructField('applicationArea', StringType(), True)
                      ])

    df = spark.createDataFrame(df.rdd, schema=newSchema)
    return df

# COMMAND ----------


