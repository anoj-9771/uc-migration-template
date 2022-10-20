# Databricks notebook source
# MAGIC %run ../common/common-curated-includeMain

# COMMAND ----------

def getPropertyRelation():
    
    df_isu = spark.sql(f"""select distinct 'ISU' as sourceSystemCode, 
                                        property1Number,
                                        property2Number,
                                        validFromDate,
                                        validToDate,
                                        relationshipTypeCode1,
                                        relationshipType1,
                                        relationshipTypeCode2,
                                        relationshipType2 
                                        from {ADS_DATABASE_CLEANSED}.isu_zcd_tprop_rel""")
    
    dummyDimRecDf = spark.createDataFrame([("-1","-1","1900-01-01","9999-12-31","","")], ["property1Number","property2Number","validFromDate","validToDate","relationshipTypeCode1","relationshipType1"])
    
    df = df_isu.unionByName(dummyDimRecDf, allowMissingColumns = True)
    
    schema = StructType([StructField('propertyRelationSK', StringType(), False),
                         StructField('sourceSystemCode', StringType(), True),
                         StructField('property1Number', StringType(), False),
                         StructField("property2Number", StringType(), False),
                         StructField("validFromDate", DateType(), False),
                         StructField("validToDate", DateType(), False),
                         StructField("relationshipTypeCode1", StringType(), False),
                         StructField("relationshipType1", StringType(), False),
                         StructField("relationshipTypeCode2", StringType(), True),
                         StructField("relationshipType2", StringType(), True)])
    
    return df, schema

# COMMAND ----------

df, schema = getPropertyRelation()
#TemplateEtl(df, entity="dimPropertyRelation", businessKey="property1Number,property2Number,relationshipTypeCode1,relationshipTypeCode2,validFromDate", schema=schema, writeMode=ADS_WRITE_MODE_OVERWRITE, AddSK=True)
TemplateTimeSliceEtlSCD(df, entity="dimPropertyRelation", businessKey="property1Number,property2Number,relationshipTypeCode1,relationshipTypeCode2,validFromDate", schema=schema)

# COMMAND ----------

dbutils.notebook.exit("1")
