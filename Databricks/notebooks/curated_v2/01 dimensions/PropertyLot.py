# Databricks notebook source
# MAGIC %run ../common/common-curated-includeMain

# COMMAND ----------

def getPropertyLot():
    
    df_isu = spark.sql(f"""select distinct 'ISU' as sourceSystemCode, 
                                        planTypeCode, 
                                        planType, 
                                        planNumber, 
                                        lotTypeCode,
                                        lotType, 
                                        lotNumber,
                                        sectionNumber,
                                        isu_0uc_connobj_attr_2.propertyNumber,
                                        CAST(a.latitude AS DECIMAL(9,6)) as latitude,
                                        CAST(a.longitude AS DECIMAL(9,6)) as longitude,
                                        _RecordDeleted from {ADS_DATABASE_CLEANSED}.isu_0uc_connobj_attr_2 
                                        left outer join (select propertyNumber, lga, latitude, longitude from 
                                              (select propertyNumber, lga, latitude, longitude, 
                                                row_number() over (partition by propertyNumber order by areaSize desc,latitude,longitude) recNum 
                                                from cleansed.hydra_tlotparcel where  _RecordCurrent = 1 ) 
                                                where recNum = 1) a on a.propertyNumber = isu_0uc_connobj_attr_2.propertyNumber
                                        where isu_0uc_connobj_attr_2.propertyNumber <> '' 
                                        and  _RecordCurrent = 1 """)
    
    #dummyDimRecDf = spark.createDataFrame([("Unknown","Unknown","Unknown","Unknown","-1")], ["planTypeCode","planNumber","lotTypeCode","lotNumber","propertyNumber"])
    dummyDimRecDf = spark.createDataFrame(["-1"], "string").toDF("propertyNumber")
    
    df = df_isu.unionByName(dummyDimRecDf, allowMissingColumns = True)
    
    schema = StructType([StructField('propertyLotSK', StringType(), False),
                         StructField('sourceSystemCode', StringType(), True),
                         StructField('planTypeCode', StringType(), True),
                         StructField("planType", StringType(), True),
                         StructField("planNumber", StringType(), True),
                         StructField("lotTypeCode", StringType(), True),
                         StructField("lotType", StringType(), True),
                         StructField("lotNumber", StringType(), True),
                         StructField("sectionNumber", StringType(), True),
                         StructField("propertyNumber", StringType(), False),
                         StructField("latitude", DecimalType(9,6), True),
                         StructField("longitude", DecimalType(9,6), True)
                        ])
    
    return df, schema

# COMMAND ----------

df, schema = getPropertyLot()
#TemplateEtl(df, entity="dimPropertyLot", businessKey="planTypeCode,planNumber,lotTypeCode,lotNumber,sectionNumber,propertyNumber", schema=schema, writeMode=ADS_WRITE_MODE_OVERWRITE, AddSK=True)
TemplateEtlSCD(df, entity="dimPropertyLot", businessKey="propertyNumber", schema=schema)

# COMMAND ----------

dbutils.notebook.exit("1")
