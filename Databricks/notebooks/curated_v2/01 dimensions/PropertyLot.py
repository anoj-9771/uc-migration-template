# Databricks notebook source
# MAGIC %run ../common/common-curated-includeMain

# COMMAND ----------

def getPropertyLot():
    
    df_isu = spark.sql(f"""select distinct planTypeCode, 
                                        planType, 
                                        planNumber, 
                                        lotTypeCode,
                                        lotType, 
                                        lotNumber,
                                        sectionNumber,
                                        propertyNumber from {ADS_DATABASE_CLEANSED}.isu_0uc_connobj_attr_2 where propertyNumber <> ''""")
    
    schema = StructType([StructField('propertyLotSK', StringType(), False),
                         StructField('planTypeCode', StringType(), True),
                         StructField("planType", StringType(), True),
                         StructField("planNumber", StringType(), True),
                         StructField("lotTypeCode", StringType(), True),
                         StructField("lotType", StringType(), True),
                         StructField("lotNumber", StringType(), True),
                         StructField("sectionNumber", StringType(), True),
                         StructField("propertyNumber", StringType(), True)])
    
    return df_isu, schema

# COMMAND ----------

df, schema = getPropertyLot()
#TemplateEtl(df, entity="dimPropertyLot", businessKey="planTypeCode,planNumber,lotTypeCode,lotNumber,sectionNumber,propertyNumber", schema=schema, writeMode=ADS_WRITE_MODE_OVERWRITE, AddSK=True)
TemplateEtlSCD(df, entity="dimPropertyLot", businessKey="planTypeCode,planNumber,lotTypeCode,lotNumber,sectionNumber,propertyNumber", schema=schema)

# COMMAND ----------

dbutils.notebook.exit("1")
