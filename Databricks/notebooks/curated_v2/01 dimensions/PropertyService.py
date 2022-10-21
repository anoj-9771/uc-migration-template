# Databricks notebook source
# MAGIC %run ../common/common-curated-includeMain

# COMMAND ----------

def getPropertyService():
    
    df_isu = spark.sql(f"""select 'ISU' as sourceSystemCode,
                        isu_vibdnode.architecturalObjectNumber as propertyNumber, 
                        isu_vibdcharact.architecturalObjectInternalId as architecturalObjectInternalId, 
                        isu_vibdcharact.validToDate, 
                        isu_vibdcharact.validFromDate,
                        isu_vibdcharact.fixtureAndFittingCharacteristicCode, 
                        isu_vibdcharact.fixtureAndFittingCharacteristic, 
                        isu_vibdcharact.supplementInfo 
                        from {ADS_DATABASE_CLEANSED}.isu_vibdcharact isu_vibdcharact inner join {ADS_DATABASE_CLEANSED}.isu_vibdnode isu_vibdnode 
                        on isu_vibdcharact.architecturalObjectInternalId = isu_vibdnode.architecturalObjectInternalId 
                        where isu_vibdcharact.fixtureAndFittingCharacteristic NOT in ('WATER DELIVERY SYSTEM', 'WATER DISTRIBUTION SYSTEM', 'WATER PRESSURE ZONE', 'WATER SUPPLY ZONE', 'RECYCLED WATER DELIVERY SYSTEM', 'RECYCLED WATER DISTRIBUTION SYSTEM', 'RECYCLED WATER SUPPLY ZONE','SEWERAGE NETWORK', 'SEWERAGE CATCHMENT', 'SCAMP','STORMWATER RECEIVING WATERS', 'STORMWATER CATCHMENT')""")
    
    dummyDimRecDf = spark.createDataFrame([("-1","9999-12-31","1900-01-01","","")], ["propertyNumber","validToDate","validFromDate","fixtureAndFittingCharacteristicCode","fixtureAndFittingCharacteristic"])
    
    df = df_isu.unionByName(dummyDimRecDf, allowMissingColumns = True)
    
    schema = StructType([StructField('propertyServiceSK', StringType(), False),
                         StructField('sourceSystemCode', StringType(), True),
                         StructField('propertyNumber', StringType(), False),
                         StructField("architecturalObjectInternalId", StringType(), True),
                         StructField("validToDate", DateType(), False),
                         StructField("validFromDate", DateType(), False),
                         StructField("fixtureAndFittingCharacteristicCode", StringType(), False),
                         StructField("fixtureAndFittingCharacteristic", StringType(), False),
                         StructField("supplementInfo", StringType(), True)])
    
    return df, schema

# COMMAND ----------

df, schema = getPropertyService()
#TemplateEtl(df, entity="dimPropertyService", businessKey="propertyNumber,fixtureAndFittingCharacteristicCode,validFromDate", schema=schema, writeMode=ADS_WRITE_MODE_OVERWRITE, AddSK=True)
TemplateTimeSliceEtlSCD(df, entity="dimPropertyService", businessKey="propertyNumber,fixtureAndFittingCharacteristicCode,validFromDate,validToDate", schema=schema)

# COMMAND ----------

dbutils.notebook.exit("1")
