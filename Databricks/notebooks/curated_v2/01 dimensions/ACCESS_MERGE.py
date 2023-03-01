# Databricks notebook source
# MAGIC %run ../common/common-curated-includeMain

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import lead, when, date_add, md5, concat_ws,col,lit

DB = "curated_v2"

# COMMAND ----------

def merge_access(table, bk, sk, df_access):

    spark.sql(f"""
            delete from {table} where sourceSystemCode='ACCESS'
        """)

    df_isu = spark.sql(f"""
                        select *,  
                        row_number() over(partition by {bk} order by _RecordStart asc) as row_sequence 
                        from {table} where sourceSystemCode='ISU' 
                    """)

    df_access_only = df_access.alias('access').join(df_isu.where("row_sequence=1"), bk.split(","),how="leftanti").select(['access.'+ c for c in df_access.columns]) #To Insert into ISU

    df_access_common = df_access.alias('access').join(df_isu.where("row_sequence=1").alias('isu'), bk.split(","), how="inner").select(['access.'+ c for c in df_access.columns])

    df_access_common_latest = df_access_common.where("row_sequence=1")

    df_access_common_notlatest = df_access_common.where("row_sequence>1") #To Insert into ISU

    df_isu_common_oldest = df_isu.where("row_sequence=1").alias('isu').join(df_access.where("row_sequence=1").alias('access'), bk.split(","), how="inner").select(['isu.'+ c for c in df_isu.columns])

    match_columns = [c for c in df_access.columns if c.lower() not in bk.split(',') + [sk.lower(), 'sourcesystemcode','_businesskey',
                                                                                           '_dlcuratedzonetimestamp','_recordstart',
                                                                                           '_recordend','_recorddeleted','_recordcurrent','row_sequence']]

    df_access_common_latest = df_access_common_latest.withColumn("md5", md5(concat_ws("|", *match_columns)))

    df_isu_common_oldest = df_isu_common_oldest.withColumn("md5", md5(concat_ws("|", *match_columns)))

    # this df will use to update ISU: Match by ISU PK + _RecordStart and update isu._RecordStart with access._RecordStart

    df_isu_update_1 = df_access_common_latest.alias('access').join(df_isu_common_oldest.alias('isu'),'md5', how="inner").selectExpr(
    *(['isu.' + c for c in bk.split(",")] + ['isu._RecordStart','access._RecordStart as updateRecordStart','access.sourceSystemCode'])
    )

    # To Insert into ISU

    df_access_insert = df_access_common_latest.alias('access').join(df_isu_common_oldest.alias('isu'),'md5', how="leftanti")
    df_access_insert = df_access_insert.withColumn('_RecordEnd', lit('2019-06-15 23:59:59').astype('Timestamp')) 

    # this df will use to update ISU: Match by ISU PK + _RecordStart and update isu._RecordStart with '2019-06-16'

    df_isu_update_2 = df_isu_common_oldest.alias('isu').join(df_access_common_latest.alias('access'),'md5', how="leftanti").selectExpr(
    *(['isu.' + c for c in bk.split(",")] + ['isu._RecordStart','timestamp(''2019-06-16'') as updateRecordStart','isu.sourceSystemCode']))

    #Summarize DF

    # Insert df_access_only, df_access_common_notlatest, df_access_insert
    
    df_access_only = df_access_only.withColumn('_RecordCurrent', when(col('row_sequence')==1, 1).otherwise(0))
    
    insert_df = df_access_only.unionAll(df_access_common_notlatest).unionAll(df_access_insert.drop('md5'))
    insert_df = insert_df.select(df_isu.columns).drop('row_sequence')

    print(f'Total {insert_df.count()} ACCESS Records Need Inert Into {table}')

    insert_df.write.mode("append").format("delta").saveAsTable(table)

    # Update ISU df_isu_update_1, df_isu_update_2

    update_df = df_isu_update_1.unionAll(df_isu_update_2)

    print(f'Total {update_df.count()} ISU Records Need Update In {table}')
    
    key_conditions = " AND ".join(['s.' + c + '=' + 't.' + c for c in bk.split(",")])

    DeltaTable.forName(spark, table).alias("t").merge(update_df.alias("s"), '(' + key_conditions + ') AND (t._RecordStart = s._RecordStart) AND (t.sourceSystemCode="ISU")') \
                .whenMatchedUpdate(set={"_RecordStart":"s.updateRecordStart","sourceSystemCode":"s.sourceSystemCode"}).execute()

# COMMAND ----------

def merge_access_timeslice(table, bk, sk, df_access):
    
    spark.sql(f"""
            delete from {table} where sourceSystemCode='ACCESS'
        """)

    df_isu = spark.sql(f"""
                        select *,  
                        row_number() over(partition by {bk} order by _RecordStart asc) as row_sequence 
                        from {table} where sourceSystemCode='ISU' 
                    """)

    df_access_only = df_access.alias('access').join(df_isu.where("row_sequence=1"), bk.split(","),how="leftanti").select(['access.'+ c for c in df_isu.columns]) #To Insert into ISU

    df_access_common = df_access.alias('access').join(df_isu.where("row_sequence=1").alias('isu'), bk.split(","), how="inner").select(['access.'+ c for c in df_access.columns])
    df_isu_common = df_isu.alias('isu').join(df_access.where("row_sequence=1").alias('access'), bk.split(","), how="inner").select(['isu.'+ c for c in df_isu.columns])

    df_common_start = df_isu_common.alias('isu').join(df_access_common.alias('access'),bk.split(",") + ['_RecordStart'], how="inner").select(['isu.'+ c for c in df_isu.columns])
    df_common_access_start_only = df_access_common.alias('access').join(df_isu_common.alias('isu'),bk.split(",") + ['_RecordStart'], how="leftanti").select(['access.'+ c for c in df_isu.columns])
    df_common_isu_start_only = df_isu_common.alias('isu').join(df_access_common.alias('access'),bk.split(",") + ['_RecordStart'], how="leftanti").select(['isu.'+ c for c in df_isu.columns])


    df_common_all = df_common_start.unionAll(df_common_access_start_only)\
                        .unionAll(df_common_isu_start_only)

    windowSpec  = Window.partitionBy(*bk.split(",")).orderBy("_RecordStart")

    df_common_all = df_common_all.withColumn("Next_RecordStart", lead('_RecordStart',1).over(windowSpec))

    df_common_all = df_common_all.withColumn("_RecordEnd", when(col('_RecordEnd')>col('Next_RecordStart'), date_add(col('Next_RecordStart'),-1)).otherwise(col('_RecordEnd')))

    df_common_all = df_common_all.drop("Next_RecordStart")

    #Summarize DF

    df_access_only = df_access_only.withColumn('_RecordCurrent', when(col('row_sequence')==1, 1).otherwise(0))
    
    insert_df = df_access_only.unionAll(df_common_all.where("sourceSystemCode='ACCESS'"))

    insert_df = insert_df.drop('row_sequence')

    print(f'Total {insert_df.count()} ACCESS Records Need Inert Into {table}')

    insert_df.write.mode("append").format("delta").saveAsTable(table)

# COMMAND ----------

'''
def merge_access_timeslice_2(table, bk, sk, df_access):
    
    spark.sql(f"""
            delete from {table} where sourceSystemCode='ACCESS'
        """)

    df_isu = spark.sql(f"""
                        select *,  
                        row_number() over(partition by {bk} order by _RecordStart asc) as row_sequence 
                        from {table} where sourceSystemCode='ISU' 
                    """)

    df_access_only = df_access.alias('access').join(df_isu.where("row_sequence=1"), bk.split(","),how="leftanti").select(['access.'+ c for c in df_isu.columns]) #To Insert into ISU

    df_access_common = df_access.alias('access').join(df_isu.where("row_sequence=1").alias('isu'), bk.split(","), how="inner").select(['access.'+ c for c in df_access.columns])
    df_isu_common = df_isu.alias('isu').join(df_access.where("row_sequence=1").alias('access'), bk.split(","), how="inner").select(['isu.'+ c for c in df_isu.columns])

    df_common_start = df_isu_common.alias('isu').join(df_access_common.alias('access'),bk.split(",") + ['_RecordStart'], how="inner").select(['isu.'+ c for c in df_isu.columns])
    df_common_access_start_only = df_access_common.alias('access').join(df_isu_common.alias('isu'),bk.split(",") + ['_RecordStart'], how="leftanti").select(['access.'+ c for c in df_isu.columns])
    
    df_access_only = df_access_only.withColumn('_RecordCurrent', when(col('row_sequence')==1, 1).otherwise(0))
    
    insert_df = df_access_only.unionAll(df_common_access_start_only)
    
    insert_df = insert_df.drop('row_sequence')
    
    print(f'Total {insert_df.count()} ACCESS Records Need Inert Into {table}')

    insert_df.write.mode("append").format("delta").saveAsTable(table)
    
''' 

# COMMAND ----------

'''
def merge_access_timeslice_old(table, bk, sk, df_access):

    spark.sql(f"""
                delete from {table} where sourceSystemCode='ACCESS'
            """)

    df_isu = spark.sql(f"""
                            select * from {table} where sourceSystemCode='ISU' 
                        """)

    df_access_only = df_access.alias('access').join(df_isu, bk.split(",") + ['_RecordStart'],how="leftanti").select(['access.'+ c for c in df_isu.columns]) #To Insert into ISU
    
    print(f'Total {df_access_only.count()} ACCESS Records Need Inert Into {table}')

    df_access_only.write.mode("append").format("delta").saveAsTable(table)
'''

# COMMAND ----------

# curated_v2.dimDevice

bk = 'deviceNumber'
sk = 'deviceSK'
table = f'{DB}.dimDevice'

df_access = spark.sql(f"""
            select 
                md5(concat(d.propertyNumber,'-',d.propertyMeterNumber,'|',d.validFrom,'|',d.validTo)) as deviceSK,
                'ACCESS' as sourceSystemCode,
                concat(d.propertyNumber,'-',d.propertyMeterNumber) as deviceNumber,
                cast(null as string) as materialNumber,
                d.meterMakerNumber as deviceId,
                cast(null as string) as inspectionRelevanceIndicator,
                concat(if(length(split(d.meterSize,' ')[0])=2,'0',''), split(d.meterSize,' ')[0]) as deviceSize,
                split(d.meterSize,' ')[1] as deviceSizeUnit,
                cast(null as string) as assetManufacturerName,
                cast(null as string) as manufacturerSerialNumber,
                cast(null as string) as manufacturerModelNumber,
                concat(d.propertyNumber,'-',d.propertyMeterNumber) as objectNumber,
                cast(case lower(waterMeterType)  
                    when 'recycled' then 2000
                    when 'potable' then 1000
                    when 'standpipe' then 9000 
                    else null end as string) as functionClassCode,
                case lower(waterMeterType)  
                    when 'recycled' then 'Recycled Water'
                    when 'potable' then 'Drinking Water' 
                    when 'standpipe' then 'Standpipe' 
                    else null end  as functionClass,
                case lower(waterMeterType)  
                    when 'standpipe' then 'L_ST'
                    when 'recycled' then if(meterCategoryCode = 'H', 'H_RE', 'L_RE')
                    when 'potable' then if(meterCategoryCode = 'H', 'H_DR', 'L_DR') 
                    else null end  as constructionClassCode,
                if(d.meterCategoryCode='H','Heavy Meter','Light Meter') as constructionClass,
                case meterGroupCode  
                    when 'N' then 'NORMAL'
                    when 'A' then 'AMI' 
                    when 'R' then 'AMR'  
                    else if(lower(waterMeterType) = 'standpipe','STANDPIPE',null) end as deviceCategoryName,
                cast(null as string) as deviceCategoryDescription,
                cast(null as string) as ptiNumber,
                cast(null as string) as ggwaNumber,
                cast(null as string) as certificationRequirementType,
                concat(d.propertyNumber,'-',d.propertyMeterNumber) as _BusinessKey,
                now() as _DLCuratedZoneTimeStamp,
                to_timestamp(d.validFrom) as _RecordStart,
                to_timestamp(d.validTo) as _RecordEnd,
                0 as _RecordDeleted,
                0 as _RecordCurrent,
                row_number() over(partition by concat(d.propertyNumber,'-',d.propertyMeterNumber) order by d.validFrom desc, d.validTo desc) as row_sequence 
              from cleansed.access_meterTimeslice d 
                    where d.meterMakerNumber is not null 
                    and d.meterMakerNumber <> ''
                    and d.meterMakerNumber <> '0' and to_timestamp(d.validTo) >= to_timestamp(d.validFrom)
                    """)

merge_access(table, bk, sk, df_access)

# COMMAND ----------

# curated_v2.dimDeviceHistory

bk = 'deviceNumber'
sk = 'deviceHistorySK'
table = f'{DB}.dimDeviceHistory'

df_access = spark.sql(f"""
            select 
                md5(concat(propertyNumber,'-',propertyMeterNumber,'|',validTo,'|',validFrom)) as deviceHistorySK,
                'ACCESS' as sourceSystemCode,
                concat(propertyNumber,'-',propertyMeterNumber) as deviceNumber,
                validTo as validToDate,
                validFrom as validFromDate,
                cast(propertyMeterNumber as long) as logicalDeviceNumber,
                cast(null as string) as deviceLocation,
                cast(null as string) as deviceCategoryCombination,
                cast(null as string) as registerGroupCode,
                cast(null as string) as registerGroup,
                meterFittedDate as installationDate,
                meterRemovedDate as deviceRemovalDate,
                meterChangeReasonCode as activityReasonCode,
                meterExchangeReason as activityReason,
                cast(null as string) as windingGroup,
                0 as advancedMeterCapabilityGroup,
                0 as messageAttributeId,
                meterFittedDate as firstInstallationDate,
                meterRemovedDate as lastDeviceRemovalDate,
                concat(propertyNumber,'-',propertyMeterNumber,'|',validTo) as _BusinessKey,
                now() as _DLCuratedZoneTimeStamp,
                to_timestamp(validFrom) as _RecordStart,
                to_timestamp(validTo) as _RecordEnd,
                0 as _RecordDeleted,
                0 as _RecordCurrent,
                row_number() over(partition by concat(propertyNumber,'-',propertyMeterNumber) order by validFrom desc, validTo desc) as row_sequence
              from cleansed.access_meterTimeslice 
              where meterMakerNumber is not null 
                    and meterMakerNumber <> ''
                    and meterMakerNumber <> '0' and to_timestamp(validTo) >= to_timestamp(validFrom)
            """)

merge_access_timeslice(table, bk, sk, df_access)

# COMMAND ----------

bk = 'registerNumber,deviceNumber' #don't need validFrom or validTo BK
sk = 'registerHistorySK'
table = f'{DB}.dimRegisterHistory'

df_access = spark.sql(f"""
                select 
                md5(concat('1','|',propertyNumber,'-',propertyMeterNumber,'|',validTo,'|',validFrom)) as registerHistorySK,
                'ACCESS' as sourceSystemCode,
                '1' as registerNumber,
                concat(propertyNumber,'-',propertyMeterNumber) as deviceNumber,
                validTo as validToDate,
                validFrom as validFromDate,
                concat(propertyNumber,'-',propertyMeterNumber) as logicalRegisterNumber,
                '3' as divisionCategoryCode,
                'Water' as divisionCategory,
                '01' as registerIdCode,
                'Water Meter' as registerId,
                '07' as registerTypeCode,
                'Water Usage' as registerType,
                '5' as registerCategoryCode,
                'Cumulating consumption register' as registerCategory,
                '2' as reactiveApparentOrActiveRegisterCode,
                'Active register' as reactiveApparentOrActiveRegister,
                if(split(meterSize,' ')[1] = 'mm','KL','GAL') unitOfMeasurementMeterReading,
                cast(null as string) as doNotReadFlag,
                concat('1','|',propertyNumber,'-',propertyMeterNumber,'|',validTo) as _BusinessKey,
                now() as _DLCuratedZoneTimeStamp,
                to_timestamp(validFrom) as _RecordStart,
                to_timestamp(validTo) as _RecordEnd,
                0 as _RecordDeleted,
                0 as _RecordCurrent,
                row_number() over(partition by concat(propertyNumber,'-',propertyMeterNumber) order by validFrom desc, validTo desc) as row_sequence
              from cleansed.access_meterTimeslice 
              where meterMakerNumber is not null 
                    and meterMakerNumber <> ''
                    and meterMakerNumber <> '0' and to_timestamp(validTo) >= to_timestamp(validFrom)
    """)

merge_access_timeslice(table, bk, sk, df_access)

# COMMAND ----------

bk = 'logicalRegisterNumber,installationNumber'
sk = 'registerInstallationHistorySK'
table = f'{DB}.dimRegisterInstallationHistory'


df_access = spark.sql(f"""
        select 
            md5(concat(propertyNumber,'-',propertyMeterNumber,'|',propertyNumber,'-',propertyMeterNumber,'|',validTo,'|',validFrom)) as registerInstallationHistorySK,
            'ACCESS' as sourceSystemCode,
            concat(propertyNumber,'-',propertyMeterNumber) as logicalRegisterNumber,
            concat(propertyNumber,'-',propertyMeterNumber) as installationNumber,
            validTo as validToDate,
            validFrom as validFromDate,
            cast('0' as long) as operationCode,
            cast(null as string) as operationDescription,
            case lower(waterMeterType)  
                when 'recycled' then concat('RW-',concat(if(length(split(meterSize,' ')[0])=2,'0',''), split(meterSize,' ')[0])) 
                when 'potable' then concat('DW-',concat(if(length(split(meterSize,' ')[0])=2,'0',''), split(meterSize,' ')[0])) 
                else cast(null as string) end as rateTypeCode,
            case lower(waterMeterType)  
                when 'recycled' then concat('Recycled Water - ',concat(if(length(split(meterSize,' ')[0])=2,'0',''), split(meterSize,' ')[0])) 
                when 'potable' then concat('Drinking Water - ',concat(if(length(split(meterSize,' ')[0])=2,'0',''), split(meterSize,' ')[0])) 
                else cast(null as string) end as rateType,
            'N' as registerNotRelevantToBilling,
            cast('0001' as string) as rateFactGroupCode,
            'Metered' as rateFactGroup,
            concat(propertyNumber,'-',propertyMeterNumber,'|',propertyNumber,'-',propertyMeterNumber,'|',validTo) as _BusinessKey,
            now() as _DLCuratedZoneTimeStamp,
            to_timestamp(validFrom) as _RecordStart,
            to_timestamp(validTo) as _RecordEnd,
            0 as _RecordDeleted,
            0 as _RecordCurrent,
            row_number() over(partition by concat(propertyNumber,'-',propertyMeterNumber) order by validFrom desc, validTo desc) as row_sequence
            from cleansed.access_meterTimeslice 
            where meterMakerNumber is not null 
                and meterMakerNumber <> ''
                and meterMakerNumber <> '0' and to_timestamp(validTo) >= to_timestamp(validFrom)
        """)

merge_access_timeslice(table, bk, sk, df_access)

# COMMAND ----------

bk = 'propertyNumber'
sk = 'propertySK'
table = f'{DB}.dimProperty'

def getAccessProperty():

    #1.Load current Cleansed layer table data into dataframe
    #build a dataframe with unique properties and lot details, 4174119 is incorrectly present on Tlot
    lotDf = spark.sql(f"""select propertyNumber,planTypeCode,planType,planNumber,lotNumber,lotTypeCode, lotType,sectionNumber from 
                          ( 
                            select propertyNumber, 
                                    coalesce(pts.PLAN_TYPE,planTypeCode) as planTypeCode, 
                                    coalesce(pts.description,planType) as planType, 
                                    planNumber as planNumber, 
                                    lotNumber  as lotNumber, 
                                    lotTypeCode as lotTypeCode, 
                                    coalesce(plts.domainValueText,lotType) as lotType, 
                                    sectionNumber as sectionNumber, 
                                    row_number() over (partition by propertyNumber order by planNumber,lotNumber,sectionNumber) recNum 
                            from {ADS_DATABASE_CLEANSED}.access_z309_tlot tlot 
                                 left outer join {ADS_DATABASE_CLEANSED}.isu_zcd_tplantype_tx pts on pts.plan_type = case when planTypeCode = 'DP' then '01' 
                                                                                              when planTypeCode = 'PSP' then '03' 
                                                                                              when planTypeCode = 'PDP' then '04' 
                                                                                              when planTypeCode = 'CN' then '05' 
                                                                                              when planTypeCode = 'SP' then '02' 
                                                                                              else null end 
                                 left outer join {ADS_DATABASE_CLEANSED}.isu_dd07t plts on lotTypeCode = plts.domainValueSingleUpperLimit and domainName = 'ZCD_DO_ADDR_LOT_TYPE' 
                            where tlot._RecordCurrent = 1 
                         ) numRec where recNum = 1
                        union all 
                        select propertyNumber, 
                                '02' as planTypeCode, 
                                'Strata Plan' as planType, 
                                strataPlanNumber as planNumber, 
                                strataPlanLot as lotNumber, 
                                '01' as lotTypeCode, 
                                'Full' as lotType, 
                                null as sectionNumber 
                        from {ADS_DATABASE_CLEANSED}.access_z309_tstrataunits 
                        where _RecordCurrent = 1 
                        """)
    lotDf.createOrReplaceTempView('lots')
    #build a dataframe with parent properties (strata units with children, joint services (M), super lots (P) and link lots(U)), 'normal' properties with themselves)
    parentDf = spark.sql(f"""with t1 as( 
                                select su.propertyNumber, 
                                        ms.masterPropertyNumber as parentPropertyNumber, 
                                        pr.propertyTypeCode as parentPropertyTypeCode, 
                                        coalesce(infsap.inferiorPropertyType,pr.propertyType) as parentPropertyType, 
                                        pr.superiorPropertyTypeCode as parentSuperiorPropertyTypeCode, 
                                        coalesce(supsap.superiorPropertyType,pr.superiorPropertyType) as parentSuperiorPropertyType, 
                                       'Child of Master Strata' as relationshipType 
                                from {ADS_DATABASE_CLEANSED}.access_z309_tstrataunits su 
                                      inner join {ADS_DATABASE_CLEANSED}.access_z309_tmastrataplan ms on su.strataPlanNumber = ms.strataPlanNumber and ms._RecordCurrent = 1 
                                      left outer join {ADS_DATABASE_CLEANSED}.access_z309_tproperty pr on pr.propertynumber = ms.masterPropertynumber and pr._RecordCurrent = 1 
                                      left outer join {ADS_DATABASE_CLEANSED}.isu_zcd_tinfprty_tx infsap on infsap.inferiorPropertyTypeCode = pr.propertyTypeCode and infsap._RecordCurrent = 1 
                                      left outer join {ADS_DATABASE_CLEANSED}.isu_zcd_tsupprtyp_tx supsap on supsap.superiorPropertyTypeCode = pr.superiorPropertyTypeCode and supsap._RecordCurrent = 1 
                                where su._RecordCurrent = 1), 
                             remainingProps as(select propertyNumber 
                                               from   {ADS_DATABASE_CLEANSED}.access_z309_tproperty 
                                               where  _RecordCurrent = 1 
                                               minus 
                                               select propertyNumber 
                                               from   t1), 
                             relatedprops as( 
                                select rp.propertyNumber as propertyNumber, 
                                        rp.relatedPropertyNumber as parentPropertyNumber, 
                                        pr.propertyTypeCode as parentPropertyTypeCode, 
                                        coalesce(infsap.inferiorPropertyType,pr.propertyType) as parentPropertyType, 
                                        pr.superiorPropertyTypeCode as parentSuperiorPropertyTypeCode, 
                                        coalesce(supsap.superiorPropertyType,pr.superiorPropertyType) as parentSuperiorPropertyType, 
                                        rp.relationshipType as relationshipType, 
                                        row_number() over (partition by rp.propertyNumber order by relationshipTypecode desc) as rn 
                                from {ADS_DATABASE_CLEANSED}.access_z309_trelatedProps rp inner join remainingProps rem on rp.propertyNumber = rem.propertyNumber 
                                       left outer join {ADS_DATABASE_CLEANSED}.access_z309_tproperty pr on pr.propertynumber = rp.relatedPropertynumber 
                                       left outer join {ADS_DATABASE_CLEANSED}.isu_zcd_tinfprty_tx infsap on infsap.inferiorPropertyTypeCode = pr.propertyTypeCode and infsap._RecordCurrent = 1 
                                       left outer join {ADS_DATABASE_CLEANSED}.isu_zcd_tsupprtyp_tx supsap on supsap.superiorPropertyTypeCode = pr.superiorPropertyTypeCode and supsap._RecordCurrent = 1 
                                where rp.relationshipTypeCode in ('M','P','U') 
                                and   rp._RecordCurrent = 1), 
                              t3 as(select * from t1 
                                    union 
                                    select propertyNumber, 
                                           parentPropertyNumber, 
                                           parentPropertyTypeCode, 
                                           parentPropertyType, 
                                           parentSuperiorPropertyTypeCode, 
                                           parentSuperiorPropertyType, 
                                           relationshipType 
                                    from relatedprops 
                                    where rn = 1), 
                              t4 as(select propertyNumber 
                                    from {ADS_DATABASE_CLEANSED}.access_z309_tproperty 
                                    where _RecordCurrent = 1 
                                    minus 
                                    select propertyNumber from t3) 
                            select * from t3 
                            union all 
                            select pr.propertyNumber, 
                                    pr.propertyNumber as parentPropertyNumber, 
                                    propertyTypeCode as parentPropertyTypeCode, 
                                    propertyType as parentPropertyType, 
                                    superiorPropertyTypeCode as parentSuperiorPropertyTypeCode, 
                                    superiorPropertyType as parentSuperiorPropertyType, 
                                    'Self as Parent' as relationshipType 
                            from {ADS_DATABASE_CLEANSED}.access_z309_tproperty pr, t4 
                            where pr.propertyNumber = t4.propertyNumber 
                            and pr._RecordCurrent = 1 
                            """)
    parentDf.createOrReplaceTempView('parents')
    
    systemAreaDf = spark.sql(f"""with t1 as ( 
                            select propertyNumber, wn.waterNetworkSK as potableSK, wnr.waterNetworkSK as recycledSK, 
                                    sewerNetworkSK as sewerNetworkSK, stormWaterNetworkSK as stormWaterNetworkSK, 
                                    row_number() over (partition by propertyNumber order by lp.waterPressureZone desc, lp.recycledSupplyZone desc, 
                                    lp.sewerScamp desc, lp.stormWaterCatchment desc) as rn 
                            from {ADS_DATABASE_CLEANSED}.hydra_TLotParcel lp 
                                  left outer join {ADS_DATABASE_CURATED_V2}.dimWaterNetwork wn on lp.waterPressureZone = wn.pressureArea and wn._RecordCurrent = 1 
                                  left outer join {ADS_DATABASE_CURATED_V2}.dimWaterNetwork wnr on lp.recycledSupplyZone = wnr.supplyZone and wnr._RecordCurrent = 1 
                                  left outer join {ADS_DATABASE_CURATED_V2}.dimSewerNetwork snw on lp.sewerScamp = snw.SCAMP and snw._RecordCurrent = 1 
                                  left outer join {ADS_DATABASE_CURATED_V2}.dimStormWaterNetwork sw on lp.stormWaterCatchment = sw.stormWaterCatchment and sw._RecordCurrent = 1 
                            where propertyNumber is not null 
                            and lp._RecordCurrent = 1) 
                            select propertyNumber, potableSK, recycledSK, sewerNetworkSK, stormWaterNetworkSK 
                            from t1 
                            where rn = 1 
                            """)
    systemAreaDf.createOrReplaceTempView('systemareas')
    
    df_access = spark.sql(f"""
                select 
                distinct md5(concat(pt.propertyNumber,'|',pt.validFrom)) as propertySK,
                'ACCESS' as sourceSystemCode, 
                cast(pt.propertyNumber as string) as propertyNumber, 
                cast(pt.propertyNumber as string) as premise, 
                if(isnull(sa.potableSK),'Unknown',sa.potableSK) as waterNetworkSK_drinkingWater, 
                if(isnull(sa.recycledSK),'Unknown',sa.recycledSK) as waterNetworkSK_recycledWater, 
                if(isnull(sa.sewerNetworkSK),'Unknown',sa.sewerNetworkSK) as sewerNetworkSK,
                if(isnull(sa.stormWaterNetworkSK),'Unknown',sa.stormWaterNetworkSK) as stormWaterNetworkSK,
                cast(null as string) as objectNumber, 
                pt.lotDescription as propertyInfo, 
                cast(null as Date) as buildingFeeDate , 
                cast(null as Date) as connectionValidFromDate , 
                cast(null as string) as CRMConnectionObjectGUID, 
                cast(pt.propertyNumber as string) as architecturalObjectNumber, 
                cast(null as string) as architecturalObjectTypeCode, 
                cast(null as string) as architecturalObjectType,
                cast(null as string) as parentArchitecturalObjectTypeCode,
                cast(null as string) as parentArchitecturalObjectType,
                cast(null as string) as parentArchitecturalObjectNumber,
                'BLANK' as hydraBand,
                --cast(coalesce(ha.propertyArea, -1) as decimal(18,6)) as hydraCalculatedArea, 
                cast(coalesce(ha.propertyArea, -1) as double) as hydraCalculatedArea, 
                case ha.propertyAreaTypeCode  
                  when 'H' then 'HAM' 
                  when 'M' then 'M2' 
                  else if(ha.propertyAreaTypeCode is null or trim(ha.propertyAreaTypeCode) = '', 'Unknown', ha.propertyAreaTypeCode)  
                  end hydraAreaUnit, 
                --cast(pt.propertyArea as decimal(18,6)) as overrideArea,
                cast(pt.propertyArea as double) as overrideArea,
                case pt.propertyAreaTypeCode 
                  when 'H' then 'HAM' 
                  when 'M' then 'M2' 
                  else null end as overrideAreaUnit,
                if(isnull(sa.stormWaterNetworkSK), 'N','Y' ) as stormWaterAssessmentFlag,
                if(ha.propertyArea > 0, 'Y','N') as hydraAreaFlag,
                cast(null as string) as comments, 
                cast(pt.propertyNumber as string) as _BusinessKey,
                now() as _DLCuratedZoneTimeStamp,
                to_timestamp(pt.validFrom) as _RecordStart,
                to_timestamp(pt.validTo) as _RecordEnd,
                0 as _RecordDeleted,
                0 as _RecordCurrent,
                row_number() over(partition by pt.propertyNumber order by pt.validFrom desc, pt.validTo desc) as row_sequence 
         from cleansed.access_propertyTimeslice pt  
         left outer join parents pp on pp.propertyNumber = pt.propertyNumber 
         left outer join systemAreas sa on sa.propertyNumber = pp.parentPropertyNumber 
         left outer join cleansed.access_z309_thydraareas ha on pt.propertyNumber = ha.propertyNumber where to_timestamp(pt.validTo) >= to_timestamp(pt.validFrom) 
    """)

    return df_access


df_access = getAccessProperty()

merge_access(table, bk, sk, df_access)

# COMMAND ----------

bk = 'propertyNumber'
sk = 'propertyTypeHistorySK'
table = f'{DB}.dimPropertyTypeHistory'

df_access = spark.sql(f"""
    
         select 
          md5(concat(propertyNumber,'|',validFrom,'|',validFrom)) as propertyTypeHistorySK,
          'ACCESS' as sourceSystemCode,
          cast(propertyNumber as string) propertyNumber,
          coalesce(superiorPropertyTypeCode,'UnKnown') as superiorPropertyTypeCode,
          coalesce(superiorPropertyType,'UnKnown') as superiorPropertyType,
          'Unknown' as inferiorPropertyTypeCode,
          'Unknown' as inferiorPropertyType,
          validFrom as ValidFromDate,
          validTo as ValidToDate,
          concat(propertyNumber,'|',validFrom) as _BusinessKey,
          now() as _DLCuratedZoneTimeStamp,
          to_timestamp(validFrom) as _RecordStart,
          to_timestamp(validTo) as _RecordEnd,
          0 as _RecordDeleted,
          0 as _RecordCurrent,
          row_number() over(partition by propertyNumber order by validFrom desc, validTo desc) as row_sequence  
          from cleansed.access_propertyTypeTimeslice 
          where to_timestamp(validTo) >= to_timestamp(validFrom)
    
    """)

merge_access_timeslice(table, bk, sk, df_access)

# COMMAND ----------

bk = 'propertyNumber'
sk = 'propertyLotSK'
table = f'{DB}.dimPropertyLot'

df_access = spark.sql(f"""
        select 
            md5(concat(propertyNumber,'|',validFrom)) as propertyLotSK,
            'ACCESS' as sourceSystemCode,
            cast(null as string) as planTypeCode,
            cast(planType as string) as planType,
            cast(planNumber as string) as planNumber,
            cast(lotTypeCode as string) as lotTypeCode,
            cast(lotType as string) as lotType,
            cast(lotNumber as string) as lotNumber,
            cast(sectionNumber as string) as sectionNumber,
            cast(propertyNumber as string) as propertyNumber,
            cast(0 as Decimal(9,6)) as latitude,
            cast(0 as Decimal(9,6)) as longitude,
            cast(propertyNumber as string) as _BusinessKey,
            now() as _DLCuratedZoneTimeStamp,
            to_timestamp(validFrom) as _RecordStart,
            to_timestamp(validTo) as _RecordEnd,
            0 as _RecordDeleted,
            0 as _RecordCurrent,
            row_number() over(partition by propertyNumber order by validFrom desc, validTo desc) as row_sequence  
            from cleansed.access_propertyLotTimeslice where to_timestamp(validTo) >= to_timestamp(validFrom) 
        """)

merge_access(table, bk, sk, df_access)

# COMMAND ----------

bk = 'propertyNumber,fixtureAndFittingCharacteristicCode'
sk = 'propertyServiceSK'
table = f'{DB}.dimPropertyService'

df_access = spark.sql(f"""
        select 
          md5(concat(propertyNumber,'|','Unknown','|',validTo,'|',validFrom)) as propertyServiceSK,
          'ACCESS' as sourceSystemCode,
          cast(propertyNumber as string) as propertyNumber,
          '-1' as architecturalObjectInternalId,
          validTo as validToDate,
          validFrom as validFromDate,
          'Unknown' as fixtureAndFittingCharacteristicCode,
          'Unknown' as fixtureAndFittingCharacteristic,
          'Unknown' as supplementInfo,
          concat(propertyNumber,'|','Unknown','|',validTo) as _BusinessKey,
          now() as _DLCuratedZoneTimeStamp,
          to_timestamp(validFrom) as _RecordStart,
          to_timestamp(validTo) as _RecordEnd,
          0 as _RecordDeleted,
          0 as _RecordCurrent,
          row_number() over(partition by propertyNumber order by validFrom desc, validTo desc) as row_sequence 
          from cleansed.access_facilityTimeslice where to_timestamp(validTo) >= to_timestamp(validFrom)
        """)

merge_access_timeslice(table, bk, sk, df_access)

# COMMAND ----------

bk = 'locationId'
sk = 'locationSK'
table = f'{DB}.dimLocation'

df_access = spark.sql(f"""
    select 
    md5(concat(locationId,'|',validFrom))  as locationSK,
    cast(locationId as string) as locationId,
    sourceSystemCode,
    formattedAddress,
    addressNumber,
    buildingName1,
    buildingName2,
    unitDetails,
    floorNumber,
    houseNumber,
    lotDetails,
    streetName,
    streetLine1,
    streetLine2,
    suburb,
    streetCode,
    cityCode,
    postCode,
    stateCode,
    --LGACode,
    LGA,
    --latitude,
    --longitude,
    concat(locationId,'|',validFrom) as _BusinessKey,
    now() as _DLCuratedZoneTimeStamp,
    to_timestamp(validFrom) as _RecordStart,
    to_timestamp(validTo) as _RecordEnd,
    0 as _RecordDeleted,
    0 as _RecordCurrent,
    row_number() over(partition by locationId order by validFrom desc, validTo desc) as row_sequence 
    from cleansed.access_propertyAddressTimeslice where to_timestamp(validTo) >= to_timestamp(validFrom)
""")

merge_access(table, bk, sk, df_access)
