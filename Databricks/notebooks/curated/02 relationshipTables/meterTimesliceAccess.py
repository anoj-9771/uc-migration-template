# Databricks notebook source
###########################################################################################################################
# Loads DATE dimension 
#############################################################################################################################
# Method
# 1.Load Cleansed layer table data into dataframe and transform
# 2.JOIN TABLES
# 3.UNION TABLES
# 4.SELECT / TRANSFORM
# 5.SCHEMA DEFINITION
#############################################################################################################################

# COMMAND ----------

# MAGIC %run ../common/common-curated-includeMain

# COMMAND ----------

# %sql
# create or replace view vw_ACCESS_MeterTimeSlice as 
# with t1 as(
#         --grab just the last row from history for a particular date
#         SELECT propertyNumber, propertyMeterNumber, rowSupersededDate, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory, 
#                 coalesce(meterGroup,  'Normal Reading') as meterGroup, isCheckMeter,
#                 row_number() over (partition by propertyNumber, propertyMeterNumber, metermakernumber, meterfitteddate, rowSupersededDate order by rowSupersededTime desc) as rn 
#         FROM cleansed.access_Z309_THPROPMETER),
#         --merge history with current, group by identical attributes
#      t2 as(
#         SELECT propertyNumber, propertyMeterNumber, meterMakerNumber, propertyMeterUpdatedDate as validFrom, to_date('99991231','yyyyMMdd') as validTo, meterSize, 
#                 meterFittedDate, meterRemovedDate,  meterClass, meterCategory, meterGroup, isCheckMeter
#         FROM cleansed.access_Z309_TPROPMETER
#         union all
#         SELECT propertyNumber, propertyMeterNumber, meterMakerNumber, null as validFrom, min(rowSupersededDate) as validto, meterSize,
#                 meterFittedDate, meterRemovedDate, meterClass, meterCategory, meterGroup, isCheckMeter 
#         FROM t1
#         where rn = 1
#         group by propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory, meterGroup, isCheckMeter),
#         --group by identical attributes, consider meterFittedDate as validFrom (will correct after) 
#      t3 as(
#         select propertyNumber, propertyMeterNumber, meterMakerNumber, min(validFrom) as validFrom, max(validto) as validto, meterSize, min(meterFittedDate) as meterFittedDate, 
#                 max(meterRemovedDate) as meterRemovedDate, meterClass, meterCategory, meterGroup, isCheckMeter from t2
#         group by propertyNumber,propertyMeterNumber, meterMakerNumber, meterFittedDate, meterSize, meterClass, meterCategory, meterGroup, isCheckMeter),
#         --change validTo the earliest of validTo and meterRemovedDate
#      t4 as(
#         select propertyNumber, propertyMeterNumber, meterMakerNumber, 
#                 coalesce(validFrom, lag(validTo,1) over (partition by propertyNumber, propertyMeterNumber, meterMakerNumber order by validTo)) as validFrom, 
#                 coalesce(meterRemovedDate,validto) as validto, meterSize, meterFittedDate, meterRemovedDate, 
#                 meterClass, meterCategory, meterGroup, isCheckMeter, row_number() over (partition by propertyNumber, propertyMeterNumber, meterMakerNumber order by validTo) as rn
#         from t3),
#         -- from date for the first row in a set is the minimum of meter fit and validFrom date
#      t5 as(
#         select propertyNumber, propertyMeterNumber, meterMakerNumber, least(validFrom, meterFittedDate) as validFrom, validTo, meterSize, meterFittedDate, meterRemovedDate, 
#                 meterClass, meterCategory, meterGroup, isCheckMeter
#         from t4
#         where rn = 1
#         union all
#         select propertyNumber, propertyMeterNumber, meterMakerNumber, validFrom, validTo, meterSize, meterFittedDate, meterRemovedDate, 
#                 meterClass, meterCategory, meterGroup, isCheckMeter
#         from t4
#         where rn > 1)
# select * 
# from t5
# order by propertymeterNumber, validTo, meterMakernumber

# COMMAND ----------

def getmeterTimesliceAccess():
    
    #1.Load current Cleansed layer table data into dataframe
    df = spark.sql(" \
     with t1 as( \
           SELECT propertyNumber, propertyMeterNumber, rowSupersededDate, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory,  \
                  coalesce(meterGroup,  'Normal Reading') as meterGroup, isCheckMeter, \
                  row_number() over (partition by propertyNumber, propertyMeterNumber, metermakernumber, meterfitteddate, rowSupersededDate order by rowSupersededTime desc) as rn \
           FROM cleansed.access_Z309_THPROPMETER), \
     t2 as( \
        SELECT propertyNumber, propertyMeterNumber, meterMakerNumber, propertyMeterUpdatedDate as validFrom, to_date('99991231','yyyyMMdd') as validTo, meterSize, \
               meterFittedDate, meterRemovedDate,  meterClass, meterCategory, meterGroup, isCheckMeter \
        FROM cleansed.access_Z309_TPROPMETER \
        union all \
        SELECT propertyNumber, propertyMeterNumber, meterMakerNumber, null as validFrom, min(rowSupersededDate) as validto, meterSize, \
               meterFittedDate, meterRemovedDate, meterClass, meterCategory, meterGroup, isCheckMeter \
        FROM t1 \
        where rn = 1 \
        group by propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory, meterGroup, isCheckMeter), \
     t3 as( \
        select propertyNumber, propertyMeterNumber, meterMakerNumber, min(validFrom) as validFrom, max(validto) as validto, meterSize, min(meterFittedDate) as meterFittedDate, \
               max(meterRemovedDate) as meterRemovedDate, meterClass, meterCategory, meterGroup, isCheckMeter \
        from t2 \
        group by propertyNumber,propertyMeterNumber, meterMakerNumber, meterFittedDate, meterSize, meterClass, meterCategory, meterGroup, isCheckMeter), \
     t4 as( \
        select propertyNumber, propertyMeterNumber, meterMakerNumber, \
               coalesce(validFrom, lag(validTo,1) over (partition by propertyNumber, propertyMeterNumber, meterMakerNumber order by validTo)) as validFrom, \
               coalesce(meterRemovedDate,validto) as validto, meterSize, meterFittedDate, meterRemovedDate, \
               meterClass, meterCategory, meterGroup, isCheckMeter, row_number() over (partition by propertyNumber, propertyMeterNumber, meterMakerNumber order by validTo) as rn \
        from t3), \
     t5 as( \
        select propertyNumber, propertyMeterNumber, meterMakerNumber, least(validFrom, meterFittedDate) as validFrom, validTo, meterSize, meterFittedDate, meterRemovedDate, \
               meterClass, meterCategory, meterGroup, isCheckMeter \
        from t4 \
        where rn = 1 \
        union all \
        select propertyNumber, propertyMeterNumber, meterMakerNumber, validFrom, validTo, meterSize, meterFittedDate, meterRemovedDate, \
                meterClass, meterCategory, meterGroup, isCheckMeter \
        from t4 \
        where rn > 1) \
        select * \
        from t5 \
        order by propertymeterNumber, validTo, meterMakernumber")
    
    #2.SELECT / TRANSFORM
    df = df.selectExpr( \
                        'propertyNumber' \
                        ,'propertyMeterNumber' \
                        ,'meterMakerNumber' \
                        ,'validFrom' \
                        ,'validTo' \
                        ,'meterSize' \
                        ,'meterFittedDate' \
                        ,'meterRemovedDate' \
                        ,'meterClass' \
                        ,'meterCategory' \
                        ,'meterGroup' \
                        ,'isCheckMeter'
                )

    #5.Apply schema definition
    schema = StructType([
                            StructField('propertyNumber', StringType(), False),
                            StructField("propertyMeterNumber", StringType(), False),
                            StructField("meterMakerNumber", StringType(), True),
                            StructField("validFrom", DateType(), False),
                            StructField("validTo", DateType(), True),
                            StructField("meterSize", StringType(), True),
                            StructField("meterFittedDate", DateType(), True),
                            StructField("meterRemovedDate", DateType(), True),
                            StructField("meterClass", StringType(), True),
                            StructField("meterCategory", StringType(), True),
                            StructField("meterGroup", StringType(), True),
                            StructField("isCheckMeter", StringType(), True)
                      ])

    return df, schema

# COMMAND ----------

df, schema = getmeterTimesliceAccess()
TemplateEtl(df, entity="meterTimesliceAccess", businessKey="propertyNumber,propertymeterNumber,validFrom", schema=schema, writeMode=ADS_WRITE_MODE_OVERWRITE, AddSK=False)

# COMMAND ----------

dbutils.notebook.exit("1")
