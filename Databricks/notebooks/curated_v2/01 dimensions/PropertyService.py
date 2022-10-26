# Databricks notebook source
# MAGIC %run ../common/common-curated-includeMain

# COMMAND ----------

from datetime import datetime

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
                        on isu_vibdcharact.architecturalObjectInternalId = isu_vibdnode.architecturalObjectInternalId """)
    
    dummyDimRecDf = spark.createDataFrame([("-1","Unknown",
                                            datetime.strptime("9999-12-31","%Y-%m-%d").date(),
                                            datetime.strptime("1900-01-01","%Y-%m-%d").date(),
                                            "Unknown")], 
                                          ["propertyNumber","architecturalObjectInternalId","validToDate","validFromDate","fixtureAndFittingCharacteristicCode"])
    
    df = df_isu.unionByName(dummyDimRecDf, allowMissingColumns = True)
    
    schema = StructType([StructField('propertyServiceSK', StringType(), False),
                         StructField('sourceSystemCode', StringType(), True),
                         StructField('propertyNumber', StringType(), False),
                         StructField("architecturalObjectInternalId", StringType(), False),
                         StructField("validToDate", DateType(), False),
                         StructField("validFromDate", DateType(), False),
                         StructField("fixtureAndFittingCharacteristicCode", StringType(), False),
                         StructField("fixtureAndFittingCharacteristic", StringType(), True),
                         StructField("supplementInfo", StringType(), True)])
    
    return df, schema

# COMMAND ----------

df, schema = getPropertyService()
#TemplateEtl(df, entity="dimPropertyService", businessKey="propertyNumber,fixtureAndFittingCharacteristicCode,validFromDate", schema=schema, writeMode=ADS_WRITE_MODE_OVERWRITE, AddSK=True)
TemplateTimeSliceEtlSCD(df, entity="dimPropertyService", businessKey="propertyNumber,fixtureAndFittingCharacteristicCode,validFromDate,validToDate", schema=schema)

# COMMAND ----------

dbutils.notebook.exit("1")
