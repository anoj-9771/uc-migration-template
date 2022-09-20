# Databricks notebook source
# MAGIC %run ../common/common-curated-includeMain

# COMMAND ----------

def getPropertyRelation():
    
    df_isu = spark.sql(f"""select distinct property1Number,
                                        property2Number,
                                        validFromDate,
                                        validToDate,
                                        relationshipTypeCode1,
                                        relationshipType1,
                                        relationshipTypeCode2,
                                        relationshipType2 
                                        from {ADS_DATABASE_CLEANSED}.isu_zcd_tprop_rel""")
    
    schema = StructType([StructField('propertyRelationSK', StringType(), False),
                         StructField('property1Number', StringType(), True),
                         StructField("property2Number", StringType(), True),
                         StructField("validFromDate", DateType(), True),
                         StructField("validToDate", DateType(), True),
                         StructField("relationshipTypeCode1", StringType(), True),
                         StructField("relationshipType1", StringType(), True),
                         StructField("relationshipTypeCode2", StringType(), True),
                         StructField("relationshipType2", StringType(), True)])
    
    return df_isu, schema

# COMMAND ----------

df, schema = getPropertyRelation()
#TemplateEtl(df, entity="dimPropertyRelation", businessKey="property1Number,property2Number,relationshipTypeCode1,relationshipTypeCode2,validFromDate", schema=schema, writeMode=ADS_WRITE_MODE_OVERWRITE, AddSK=True)
TemplateTimeSliceEtlSCD(df, entity="dimPropertyRelation", businessKey="property1Number,property2Number,relationshipTypeCode1,relationshipTypeCode2,validFromDate", schema=schema)

# COMMAND ----------

dbutils.notebook.exit("1")
