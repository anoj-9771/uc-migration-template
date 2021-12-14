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
    #2.Load Cleansed layer table data into dataframe

    df = spark.sql(f"select  co.contractId, \
                             coalesce(coh.validFromDate,to_date('1900-01-01','yyyy-MM-dd')), \
                             coh.validToDate, \
                             'SAPISU' as sourceSystemCode, \
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
                     ")

    
    df.createOrReplaceTempView('allcontracts')
    #3.JOIN TABLES  

    #4.UNION TABLES
    #Create dummy record
#     dummyRec = tuple([-1] + ['Unknown'] * (len(HydraLocationDf.columns) - 3) + [0,0]) 
#     dummyDimRecDf = spark.createDataFrame([dummyRec],HydraLocationDf.columns)
#     HydraLocationDf = HydraLocationDf.unionByName(dummyDimRecDf, allowMissingColumns = True)

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


