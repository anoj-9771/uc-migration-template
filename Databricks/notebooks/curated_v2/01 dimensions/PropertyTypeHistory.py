# Databricks notebook source
# MAGIC %run ../common/common-curated-includeMain

# COMMAND ----------

def getPropertyTypeHistory():
    
    df_isu = spark.sql(f"""select 'ISU' as sourceSystemCode,
                    propertyNumber, 
                    superiorPropertyTypeCode, 
                    superiorPropertyType, 
                    inferiorPropertyTypeCode, 
                    inferiorPropertyType, 
                    ValidFromDate, 
                    ValidToDate,
                    _RecordDeleted 
                    from {ADS_DATABASE_CLEANSED}.isu_zcd_tpropty_hist where _RecordCurrent = 1 and _RecordDeleted <> -1 """)
    '''
    spark.sql(f"""CREATE OR REPLACE TEMP VIEW view_access_property_hist 
            as with histRaw as( 
                            select a.propertyNumber, propertyTypeCode, propertyType, superiorPropertyTypeCode, superiorPropertyType, propertyTypeEffectiveFrom as validFrom, rowSupersededTimestamp as updateTS 
                            from {ADS_DATABASE_CLEANSED}.access_z309_thproperty a), 
                  histOrdered as( 
                            select propertyNumber, propertyTypeCode, propertyType, superiorPropertyTypeCode, superiorPropertyType, validFrom, updateTS, 
                            row_number() over (partition by propertyNumber, validFrom order by updateTS) as rnk 
                            from histRaw), 
                  allRows as( 
                            select a.propertyNumber, propertyTypeCode, propertyType, superiorPropertyTypeCode, superiorPropertyType, propertyTypeEffectiveFrom as validFrom, 
                            to_timestamp('99991231235959','yyyyMMddHHmmss') as updateTS 
                            from {ADS_DATABASE_CLEANSED}.access_z309_tproperty a 
                            union all 
                            select propertyNumber, propertyTypeCode, propertyType, superiorPropertyTypeCode, superiorPropertyType, validFrom, updateTS 
                            from histOrdered 
                            where rnk = 1), 
                  clean1 as( 
                            select propertyNumber, propertyTypeCode, propertyType, superiorPropertyTypeCode, superiorPropertyType, validFrom, 
                                    coalesce(lead(validFrom,1) over (partition by propertyNumber order by validFrom),to_date('99991231','yyyyMMdd')) as validTo 
                            from allRows), 
                  clean2 as( 
                            select cast(propertyNumber as string), propertyTypeCode as inferiorPropertyTypeCode, propertyType as inferiorPropertyType,superiorPropertyTypeCode, 
                            superiorPropertyType, validFrom as validFromDate, case when validTo = to_date('99991231','yyyyMMdd') then validTo else validto - interval '1' day end as validToDate 
                            from clean1 
                            where validFrom <> validTo) 
            select * from clean2;""")

    spark.sql(f"""CREATE OR REPLACE TABLE stage.access_property_hist 
    LOCATION 'dbfs:/mnt/datalake-curated/stage/access_property_hist' 
    as with accessProperties as (select propertyNumber from view_access_property_hist 
                              minus 
                              select propertyNumber from {ADS_DATABASE_CLEANSED}.isu_zcd_tpropty_hist), 
         prophist as ( 
            select propertyNumber,inferiorPropertyTypeCode,inferiorPropertyType,superiorPropertyTypeCode,superiorPropertyType,validFromDate,validToDate from {ADS_DATABASE_CLEANSED}.isu_zcd_tpropty_hist 
            union   
            select a.propertyNumber,inferiorPropertyTypeCode,inferiorPropertyType,superiorPropertyTypeCode,superiorPropertyType,validFromDate,validToDate from view_access_property_hist a  
                  join accessProperties b on a.propertyNumber = b.propertyNumber 
          ) 
    select * from prophist;""")
    
    df_access = spark.sql(f"""select 'ACCESS' as sourceSystemCode,
                    propertyNumber, 
                    superiorPropertyTypeCode, 
                    superiorPropertyType, 
                    inferiorPropertyTypeCode, 
                    inferiorPropertyType, 
                    ValidFromDate, 
                    ValidToDate 
                    from stage.access_property_hist""")
    
    df = df_isu.unionByName(df_access, allowMissingColumns = True).dropDuplicates()
    
    '''
    
    dummyDimRecDf = spark.createDataFrame([("-1","Unknown","Unknown","Unknown","Unknown","9999-12-31","1900-01-01")], 
                                          ["propertyNumber","superiorPropertyTypeCode","superiorPropertyType","inferiorPropertyTypeCode","inferiorPropertyType","ValidToDate","ValidFromDate"])
    
    dummyDimRecDf = dummyDimRecDf.withColumn("ValidFromDate", (col("ValidFromDate").cast("date")))
    dummyDimRecDf = dummyDimRecDf.withColumn("ValidToDate", (col("ValidToDate").cast("date")))
    
    df = df_isu.unionByName(dummyDimRecDf, allowMissingColumns = True)
    
    schema = StructType([StructField('propertyTypeHistorySK', StringType(), False),
                         StructField('sourceSystemCode', StringType(), True),
                         StructField('propertyNumber', StringType(), False),
                         StructField("superiorPropertyTypeCode", StringType(), False),
                         StructField("superiorPropertyType", StringType(), False),
                         StructField("inferiorPropertyTypeCode", StringType(), False),
                         StructField("inferiorPropertyType", StringType(), False),
                         StructField("ValidFromDate", DateType(), False),
                         StructField("ValidToDate", DateType(), False)])
    
    return df, schema

# COMMAND ----------

df, schema = getPropertyTypeHistory()
#TemplateEtl(df, entity="dimPropertyTypeHistory", businessKey="propertyNumber,ValidFromDate", schema=schema, writeMode=ADS_WRITE_MODE_OVERWRITE, AddSK=True)
#TemplateEtlSCD(df, entity="dimPropertyTypeHistory", businessKey="propertyNumber,ValidFromDate", schema=schema)
TemplateTimeSliceEtlSCD(df, entity="dimPropertyTypeHistory", businessKey="propertyNumber,ValidFromDate", schema=schema)

# COMMAND ----------

#dbutils.notebook.exit("1")
