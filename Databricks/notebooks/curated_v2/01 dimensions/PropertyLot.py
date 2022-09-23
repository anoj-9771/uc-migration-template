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
                         StructField('planTypeCode', StringType(), False),
                         StructField("planType", StringType(), True),
                         StructField("planNumber", StringType(), False),
                         StructField("lotTypeCode", StringType(), False),
                         StructField("lotType", StringType(), True),
                         StructField("lotNumber", StringType(), False),
                         StructField("sectionNumber", StringType(), False),
                         StructField("propertyNumber", StringType(), False)])
    
    return df_isu, schema

# COMMAND ----------

df, schema = getPropertyLot()
#TemplateEtl(df, entity="dimPropertyLot", businessKey="planTypeCode,planNumber,lotTypeCode,lotNumber,sectionNumber,propertyNumber", schema=schema, writeMode=ADS_WRITE_MODE_OVERWRITE, AddSK=True)
TemplateEtlSCD(df, entity="dimPropertyLot", businessKey="planTypeCode,planNumber,lotTypeCode,lotNumber,sectionNumber,propertyNumber", schema=schema)

# COMMAND ----------

dbutils.notebook.exit("1")
