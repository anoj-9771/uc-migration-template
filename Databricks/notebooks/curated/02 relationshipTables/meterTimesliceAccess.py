# Databricks notebook source
# MAGIC %md
# MAGIC <b>Note that this notebook should be run via the /cleansed/ACCESS Utils/Create Missing Meters notebook</b>

# COMMAND ----------

# MAGIC %run ../common/common-curated-includeMain

# COMMAND ----------

def getmeterTimesliceAccess():
    
    #1.Load current Cleansed layer table data into dataframe
        #t1: only grab the most recent update for a given day from history
        #t2: join history and current together
        #t3: calculate the validTo date based on date updated of the next record. Set end date to meter removed date or infinite
        #t4: remove the rows where the validFrom date is greater than the validTo date (happens when row updated more than once on the same day)
        #t5: collapse adjoining date ranges
        #t6: order the rows by meter and valid from date so we can make sure we set the earliest validFrom date to the meter fit date as required
    df = spark.sql(" \
            with t1 as( \
                      SELECT 'H' as src, propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, \
                              hpm.meterClass, meterCategory, coalesce(meterGroup,  'Normal Reading') as meterGroup, isCheckMeter, mc.waterMeterType, propertyMeterUpdatedDate, \
                              row_number() over (partition by propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, metermakernumber, \
                              hpm.meterClass, meterCategory, meterGroup, isCheckMeter, mc.waterMeterType, rowSupersededDate order by rowSupersededTime desc) as rn \
                      FROM cleansed.access_Z309_THPROPMETER hpm left outer join CLEANSED.access_Z309_TMeterClass mc on mc.meterClassCode = hpm.meterClassCode \
                      ), \
                 t2 as( \
                      select 'C' as src, propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, \
                              meterClass, meterCategory, coalesce(meterGroup,  'Normal Reading') as meterGroup, isCheckMeter, waterMeterType, propertyMeterUpdatedDate \
                      from cleansed.access_z309_tpropmeter \
                      union all \
                      select src, t1.propertyNumber, t1.propertyMeterNumber, t1.meterSize, t1.meterFittedDate, greatest(t1.meterRemovedDate, pm.meterRemovedDate) as meterRemovedDate, t1.meterMakerNumber, \
                             t1.meterClass, t1.meterCategory, t1.meterGroup, t1.isCheckMeter, t1.waterMeterType, t1.propertyMeterUpdatedDate \
                      from t1 left outer join cleansed.access_z309_tpropmeter pm on pm.propertyNumber = t1.propertyNumber \
                                                                                and pm.propertyMeterNumber = t1.propertyMeterNumber \
                                                                                and pm.meterMakerNumber = t1.meterMakerNumber \
                      where rn = 1 \
                      ), \
                 t3 as( \
                      select src, propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, \
                             meterClass, meterCategory, meterGroup, isCheckMeter, waterMeterType, case when propertyMeterUpdatedDate > meterFittedDate then propertyMeterUpdatedDate else meterFittedDate end as validFrom, \
                             row_number() over (partition by propertyNumber, propertyMeterNumber, meterMakerNumber order by meterFittedDate, propertyMeterUpdatedDate) as rn \
                      from t2 \
                      ), \
                 t4 as( \
                      select propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory, \
                             meterGroup, isCheckMeter, waterMeterType, least(validFrom,meterFittedDate) as validFrom \
                      from   t3 \
                      where  rn = 1 \
                      union all \
                      select propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory, \
                             meterGroup, isCheckMeter, waterMeterType, validFrom \
                      from   t3 \
                      where  rn > 1 \
                      ), \
                 t5 as( \
                      select propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory, \
                             meterGroup, isCheckMeter, waterMeterType, validFrom, \
                             coalesce( \
                                      date_add( \
                                         lag(validFrom,1) over (partition by propertyNumber, propertyMeterNumber, meterMakerNumber order by validFrom desc),-1), \
                                      least(meterRemovedDate,to_date('99991231','yyyyMMdd'))) as validTo \
                      from t4 \
                      ), \
                 t6 as( \
                      select propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory, \
                                        meterGroup, isCheckMeter, waterMeterType, validFrom, validTo \
                      from   t5 \
                      where  not validFrom > validTo \
                      ), \
                 t7 as( \
                      SELECT \
                          propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory, \
                                        meterGroup, isCheckMeter, waterMeterType, \
                          MIN(validFrom) as validFrom, \
                          max(validTo) as validTo \
                      FROM ( \
                          SELECT \
                              propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory, \
                                        meterGroup, isCheckMeter, waterMeterType, validFrom, validTo, \
                              DATEADD( \
                                  DAY, \
                                  -COALESCE( \
                                      SUM(DATEDIFF(DAY, validFrom, validTo) +1) OVER (PARTITION BY propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, \
                                      meterRemovedDate, meterMakerNumber, meterClass, meterCategory, \
                                        meterGroup, isCheckMeter, waterMeterType ORDER BY validTo ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), \
                                      0 \
                                  ), \
                                  validFrom \
                          ) as grp \
                          FROM t6 \
                      ) withGroup \
                      GROUP BY propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory, \
                               meterGroup, isCheckMeter, waterMeterType, grp \
                      ORDER BY propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory, \
                               meterGroup, isCheckMeter, waterMeterType, validFrom \
                      ) \
            select * \
            from   t7 \
            order  by propertyNumber, propertyMeterNumber, validFrom \
     ")
    #2.SELECT / TRANSFORM
    df = df.selectExpr( \
                        'propertyNumber' \
                        ,'propertyMeterNumber' \
                        ,'meterMakerNumber' \
                        ,'meterSize' \
                        ,'meterFittedDate' \
                        ,'meterRemovedDate' \
                        ,'meterClass' \
                        ,'meterCategory' \
                        ,'meterGroup' \
                        ,'isCheckMeter' \
                        ,'waterMeterType' \
                        ,'validFrom' \
                        ,'validTo' \
                )

    #5.Apply schema definition
    schema = StructType([
                            StructField('propertyNumber', StringType(), False),
                            StructField("propertyMeterNumber", StringType(), False),
                            StructField("meterMakerNumber", StringType(), True),
                            StructField("meterSize", StringType(), True),
                            StructField("meterFittedDate", DateType(), True),
                            StructField("meterRemovedDate", DateType(), True),
                            StructField("meterClass", StringType(), True),
                            StructField("meterCategory", StringType(), True),
                            StructField("meterGroup", StringType(), True),
                            StructField("isCheckMeter", StringType(), True),
                            StructField("waterMeterType", StringType(), True),
                            StructField("validFrom", DateType(), False),
                            StructField("validTo", DateType(), True)
    ])

    return df, schema

# COMMAND ----------

df, schema = getmeterTimesliceAccess()
df.createOrReplaceTempView('ts')
TemplateEtl(df, entity="meterTimesliceAccess", businessKey="propertyNumber,propertymeterNumber,validFrom", schema=schema, writeMode=ADS_WRITE_MODE_OVERWRITE, AddSK=False)

# COMMAND ----------

dbutils.notebook.exit('0')

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from ts
# MAGIC where propertyNumber = 3100078
# MAGIC order by propertyNumber, meterMakerNumber, validFrom

# COMMAND ----------

# MAGIC %sql
# MAGIC show create table datalab.view_devicecharacteristic

# COMMAND ----------

# MAGIC %sql
# MAGIC with t1 as( 
# MAGIC                       SELECT 'H' as src, propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, 
# MAGIC                               meterClass, meterCategory, coalesce(meterGroup,  'Normal Reading') as meterGroup, isCheckMeter, propertyMeterUpdatedDate, 
# MAGIC                               row_number() over (partition by propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, metermakernumber, 
# MAGIC                               meterClass, meterCategory, meterGroup, isCheckMeter, rowSupersededDate order by rowSupersededTime desc) as rn 
# MAGIC                       FROM cleansed.access_Z309_THPROPMETER 
# MAGIC                       where propertyNumber = 5703660
# MAGIC                       ), 
# MAGIC                  t2 as( 
# MAGIC                       select 'C' as src, propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, 
# MAGIC                               meterClass, meterCategory, coalesce(meterGroup,  'Normal Reading') as meterGroup, isCheckMeter, propertyMeterUpdatedDate 
# MAGIC                       from cleansed.access_z309_tpropmeter 
# MAGIC                       where propertyNumber = 5703660
# MAGIC                       union all 
# MAGIC                       select src, propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, 
# MAGIC                              meterClass, meterCategory, meterGroup, isCheckMeter, propertyMeterUpdatedDate 
# MAGIC                       from t1 
# MAGIC                       where rn = 1 
# MAGIC                       ), 
# MAGIC                  t3 as( 
# MAGIC                       select src, propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, 
# MAGIC                              meterClass, meterCategory, meterGroup, isCheckMeter, case when propertyMeterUpdatedDate > meterFittedDate then propertyMeterUpdatedDate else meterFittedDate end as validFrom, 
# MAGIC                              row_number() over (partition by propertyNumber, propertyMeterNumber, meterMakerNumber order by meterFittedDate, propertyMeterUpdatedDate) as rn 
# MAGIC                       from t2 
# MAGIC                       ), 
# MAGIC                  t4 as( 
# MAGIC                       select propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory, 
# MAGIC                              meterGroup, isCheckMeter, least(validFrom,meterFittedDate) as validFrom 
# MAGIC                       from   t3 
# MAGIC                       where  rn = 1 
# MAGIC                       union all 
# MAGIC                       select propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory, 
# MAGIC                              meterGroup, isCheckMeter, validFrom 
# MAGIC                       from   t3 
# MAGIC                       where  rn > 1 
# MAGIC                       ), 
# MAGIC                  t5 as( 
# MAGIC                       select propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory, 
# MAGIC                              meterGroup, isCheckMeter, validFrom, 
# MAGIC                              coalesce( 
# MAGIC                                       date_add( 
# MAGIC                                          lag(validFrom,1) over (partition by propertyNumber, propertyMeterNumber, meterMakerNumber order by validFrom desc),-1), 
# MAGIC                                       least(meterRemovedDate,to_date('99991231','yyyyMMdd'))) as validTo 
# MAGIC                       from t4 
# MAGIC                       ), 
# MAGIC                  t6 as( 
# MAGIC                       select propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory, 
# MAGIC                                         meterGroup, isCheckMeter, validFrom, validTo 
# MAGIC                       from   t5 
# MAGIC                       where  not validFrom > validTo 
# MAGIC                       ), 
# MAGIC                  t7 as( 
# MAGIC                       SELECT 
# MAGIC                           propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory, 
# MAGIC                                         meterGroup, isCheckMeter, 
# MAGIC                           MIN(validFrom) as validFrom, 
# MAGIC                           max(validTo) as validTo 
# MAGIC                       FROM ( 
# MAGIC                           SELECT 
# MAGIC                               propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory, 
# MAGIC                                         meterGroup, isCheckMeter, validFrom, validTo, 
# MAGIC                               DATEADD( 
# MAGIC                                   DAY, 
# MAGIC                                   -COALESCE( 
# MAGIC                                       SUM(DATEDIFF(DAY, validFrom, validTo) +1) OVER (PARTITION BY propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, 
# MAGIC                                       meterRemovedDate, meterMakerNumber, meterClass, meterCategory, 
# MAGIC                                         meterGroup, isCheckMeter ORDER BY validTo ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 
# MAGIC                                       0 
# MAGIC                                   ), 
# MAGIC                                   validFrom 
# MAGIC                           ) as grp 
# MAGIC                           FROM t6 
# MAGIC                       ) withGroup 
# MAGIC                       GROUP BY propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory, 
# MAGIC                                meterGroup, isCheckMeter, grp 
# MAGIC                       ORDER BY propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory, 
# MAGIC                                meterGroup, isCheckMeter, validFrom 
# MAGIC                       ) 
# MAGIC             select * 
# MAGIC             from   t7
# MAGIC             order  by propertyNumber, propertyMeterNumber, validFrom 
# MAGIC -- select src, propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory,  
# MAGIC --                   meterGroup, isCheckMeter, validFrom, validTo
# MAGIC -- from t3
# MAGIC -- order by validFrom
# MAGIC        
# MAGIC --, meterMakerNumber, meterClass, meterCategory, meterGroup, isCheckMeter

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from cleansed.access_z309_tpropmeter
# MAGIC where propertyNumber = 3100078

# COMMAND ----------

# MAGIC %sql
# MAGIC with t1 as(--latest update for a day
# MAGIC           SELECT 'H' as src, propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, 
# MAGIC                   meterClass, meterCategory, coalesce(meterGroup,  'Normal Reading') as meterGroup, isCheckMeter, propertyMeterUpdatedDate, 
# MAGIC                   rank() over (partition by propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, metermakernumber, 
# MAGIC                   meterClass, meterCategory, meterGroup, isCheckMeter, rowSupersededDate order by rowSupersededTime desc) as rnk 
# MAGIC           FROM cleansed.access_Z309_THPROPMETER
# MAGIC           where meterMakerNumber = 'DTED0028'
# MAGIC           ),
# MAGIC      t2 as(
# MAGIC           select 'C' as src, propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, 
# MAGIC                   meterClass, meterCategory, coalesce(meterGroup,  'Normal Reading') as meterGroup, isCheckMeter, propertyMeterUpdatedDate
# MAGIC           from cleansed.access_z309_tpropmeter 
# MAGIC           where meterMakerNumber = 'DTED0028'
# MAGIC           union all
# MAGIC           select src, propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, 
# MAGIC                  meterClass, meterCategory, meterGroup, isCheckMeter, propertyMeterUpdatedDate 
# MAGIC           from t1
# MAGIC           where rnk = 1),
# MAGIC      t3 as(
# MAGIC           select src, propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, 
# MAGIC                  meterClass, meterCategory, meterGroup, isCheckMeter, propertyMeterUpdatedDate as validFrom, 
# MAGIC                  coalesce(date_add(
# MAGIC                      lag(propertyMeterUpdatedDate,1) over (partition by propertyNumber, propertyMeterNumber order by propertyMeterUpdatedDate desc),-1),
# MAGIC                    least(meterRemovedDate,to_date('99991231','yyyyMMdd'))) as validTo
# MAGIC           from t2),
# MAGIC      t3a as(
# MAGIC           select propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory,  
# MAGIC                             meterGroup, isCheckMeter, validFrom, validTo
# MAGIC           from   t3
# MAGIC           where  not validFrom > validTo --anomaliy if row updated more than once
# MAGIC           ),
# MAGIC      t4 as(
# MAGIC           SELECT 
# MAGIC               propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory,  
# MAGIC                             meterGroup, isCheckMeter,
# MAGIC               MIN(validFrom) as validFrom, 
# MAGIC               max(validTo) as validTo
# MAGIC           FROM (
# MAGIC               SELECT 
# MAGIC                   propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory,  
# MAGIC                             meterGroup, isCheckMeter, validFrom, validTo,
# MAGIC                   DATEADD(
# MAGIC                       DAY, 
# MAGIC                       -COALESCE(
# MAGIC                           SUM(DATEDIFF(DAY, validFrom, validTo) +1) OVER (PARTITION BY propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, 
# MAGIC                           meterRemovedDate, meterMakerNumber, meterClass, meterCategory,  
# MAGIC                             meterGroup, isCheckMeter ORDER BY validTo ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 
# MAGIC                           0
# MAGIC                       ),
# MAGIC                       validFrom
# MAGIC                   ) as grp
# MAGIC               FROM t3a
# MAGIC           ) withGroup
# MAGIC           GROUP BY propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory,  
# MAGIC                    meterGroup, isCheckMeter, grp
# MAGIC           ORDER BY propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory,  
# MAGIC                    meterGroup, isCheckMeter, validFrom),
# MAGIC      t5 as(--now we may need to set the first validFrom date to the meter fit date     
# MAGIC           select propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory,  
# MAGIC                             meterGroup, isCheckMeter, validFrom, validTo, 
# MAGIC                  row_number() over (partition by propertyNumber, propertyMeterNumber, meterMakerNumber order by validFrom) as rn
# MAGIC           from t4)
# MAGIC select propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory,  
# MAGIC        meterGroup, isCheckMeter, least(validFrom,meterFittedDate) as validFrom, validTo 
# MAGIC from   t5
# MAGIC where  rn = 1
# MAGIC union all
# MAGIC select propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory,  
# MAGIC        meterGroup, isCheckMeter, validFrom, validTo 
# MAGIC from   t5
# MAGIC where  rn > 1
# MAGIC order  by 1,2, validFrom

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from raw.access_z309_tpropmeter_bi
# MAGIC where n_mete_make = 'DTED0028'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT propertyNumber, propertyMeterNumber, rowSupersededDate, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory,  
# MAGIC                   coalesce(meterGroup,  'Normal Reading') as meterGroup, isCheckMeter, propertyMeterUpdatedDate 
# MAGIC            FROM cleansed.access_Z309_THPROPMETER
# MAGIC            where meterMakerNumber = 'DTED0028'
# MAGIC            order by propertyMeterUpdatedDate

# COMMAND ----------

# MAGIC %sql
# MAGIC with t1 as(--latest update for a day. this seems wrong
# MAGIC SELECT propertyNumber, propertyMeterNumber, rowSupersededDate, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory,  
# MAGIC                   coalesce(meterGroup,  'Normal Reading') as meterGroup, isCheckMeter, propertyMeterUpdatedDate, 
# MAGIC                   --row_number() over (partition by propertyNumber, propertyMeterNumber, metermakernumber, meterfitteddate, meterClass, meterGroup, isCheckMeter, rowsupersededDate order by rowSupersededTime desc) as rn 
# MAGIC                   rank() over (partition by propertyNumber, propertyMeterNumber, metermakernumber, meterfitteddate, meterClass, meterGroup, isCheckMeter order by rowsupersededDate desc) as rnk 
# MAGIC            FROM cleansed.access_Z309_THPROPMETER
# MAGIC            where meterMakerNumber = 'DTED0028'
# MAGIC        )
# MAGIC        select * from t1
# MAGIC        where rnk = 1

# COMMAND ----------

dbutils.notebook.exit("1")

# COMMAND ----------

df = spark.sql(" \
            with t1 as( \
                      SELECT 'H' as src, propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, \
                              meterClass, meterCategory, coalesce(meterGroup,  'Normal Reading') as meterGroup, isCheckMeter, propertyMeterUpdatedDate, \
                              row_number() over (partition by propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, metermakernumber, \
                              meterClass, meterCategory, meterGroup, isCheckMeter, rowSupersededDate order by rowSupersededTime desc) as rn \
                      FROM cleansed.access_Z309_THPROPMETER \
                       where propertyNumber = 5000109 \
                      ), \
                 t2 as( \
                      select 'C' as src, propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, \
                              meterClass, meterCategory, coalesce(meterGroup,  'Normal Reading') as meterGroup, isCheckMeter, propertyMeterUpdatedDate \
                      from cleansed.access_z309_tpropmeter \
                       where propertyNumber = 5000109 \
                      union all \
                      select src, propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, \
                             meterClass, meterCategory, meterGroup, isCheckMeter, propertyMeterUpdatedDate \
                      from t1 \
                      where rn = 1 \
                      ), \
                 t3 as( \
                      select src, propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, \
                             meterClass, meterCategory, meterGroup, isCheckMeter, propertyMeterUpdatedDate as validFrom, \
                             row_number() over (partition by propertyNumber, propertyMeterNumber, meterMakerNumber order by propertyMeterUpdatedDate) as rn \
                      from t2 \
                      ), \
                 t4 as( \
                      select propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory, \
                             meterGroup, isCheckMeter, least(validFrom,meterFittedDate) as validFrom \
                      from   t3 \
                      where  rn = 1 \
                      union all \
                      select propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory, \
                             meterGroup, isCheckMeter, validFrom \
                      from   t3 \
                      where  rn > 1 \
                      ), \
                 t5 as( \
                      select propertyNumber, propertyMeterNumber, meterSize, meterFittedDate, meterRemovedDate, meterMakerNumber, meterClass, meterCategory, \
                             meterGroup, isCheckMeter, validFrom, \
                             coalesce( \
                                      date_add( \
                                         lag(validFrom,1) over (partition by propertyNumber, propertyMeterNumber, meterMakerNumber order by validFrom desc),-1), \
                                      least(meterRemovedDate,to_date('99991231','yyyyMMdd'))) as validTo \
                      from t4 \
                      ) \
  select * from t5")
display(df)       

# COMMAND ----------


