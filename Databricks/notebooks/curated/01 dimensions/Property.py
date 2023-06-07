# Databricks notebook source
###########################################################################################################################
# Loads PROPERTY dimension 
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

# MAGIC %run ./SewerNetwork

# COMMAND ----------

# MAGIC %run ./StormWaterNetwork

# COMMAND ----------

# MAGIC %run ./WaterNetwork

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Need to Run Property Type History Before Property

# COMMAND ----------

# MAGIC %run ./PropertyTypeHistory

# COMMAND ----------

# MAGIC %run ./PropertyLot

# COMMAND ----------

# MAGIC %run ./Location

# COMMAND ----------

#-----------------------------------------------------------------------------------------------
# Note: Due to the fact that dimProperty relies on the system area tables having been populated,
# SewerNetwork, StormWaterNetwork and WaterNetwork notebooks are included in this notebook. 
# This takes care of the load sequence.
#-----------------------------------------------------------------------------------------------

# COMMAND ----------

def getProperty():

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
                            from {ADS_DATABASE_CLEANSED}.access.z309_tlot tlot 
                                 left outer join {ADS_DATABASE_CLEANSED}.isu.zcd_tplantype_tx pts on pts.plan_type = case when planTypeCode = 'DP' then '01' 
                                                                                              when planTypeCode = 'PSP' then '03' 
                                                                                              when planTypeCode = 'PDP' then '04' 
                                                                                              when planTypeCode = 'CN' then '05' 
                                                                                              when planTypeCode = 'SP' then '02' 
                                                                                              else null end 
                                 left outer join {ADS_DATABASE_CLEANSED}.isu.dd07t plts on lotTypeCode = plts.domainValueSingleUpperLimit and domainName = 'ZCD_DO_ADDR_LOT_TYPE' 
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
                        from {ADS_DATABASE_CLEANSED}.access.z309_tstrataunits 
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
                                from {ADS_DATABASE_CLEANSED}.access.z309_tstrataunits su 
                                      inner join {ADS_DATABASE_CLEANSED}.access.z309_tmastrataplan ms on su.strataPlanNumber = ms.strataPlanNumber and ms._RecordCurrent = 1 
                                      left outer join {ADS_DATABASE_CLEANSED}.access.z309_tproperty pr on pr.propertynumber = ms.masterPropertynumber and pr._RecordCurrent = 1 
                                      left outer join {ADS_DATABASE_CLEANSED}.isu.zcd_tinfprty_tx infsap on infsap.inferiorPropertyTypeCode = pr.propertyTypeCode and infsap._RecordCurrent = 1 
                                      left outer join {ADS_DATABASE_CLEANSED}.isu.zcd_tsupprtyp_tx supsap on supsap.superiorPropertyTypeCode = pr.superiorPropertyTypeCode and supsap._RecordCurrent = 1 
                                where su._RecordCurrent = 1), 
                             remainingProps as(select propertyNumber 
                                               from   {ADS_DATABASE_CLEANSED}.access.z309_tproperty 
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
                                from {ADS_DATABASE_CLEANSED}.access.z309_trelatedProps rp inner join remainingProps rem on rp.propertyNumber = rem.propertyNumber 
                                       left outer join {ADS_DATABASE_CLEANSED}.access.z309_tproperty pr on pr.propertynumber = rp.relatedPropertynumber 
                                       left outer join {ADS_DATABASE_CLEANSED}.isu.zcd_tinfprty_tx infsap on infsap.inferiorPropertyTypeCode = pr.propertyTypeCode and infsap._RecordCurrent = 1 
                                       left outer join {ADS_DATABASE_CLEANSED}.isu.zcd_tsupprtyp_tx supsap on supsap.superiorPropertyTypeCode = pr.superiorPropertyTypeCode and supsap._RecordCurrent = 1 
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
                                    from {ADS_DATABASE_CLEANSED}.access.z309_tproperty 
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
                            from {ADS_DATABASE_CLEANSED}.access.z309_tproperty pr, t4 
                            where pr.propertyNumber = t4.propertyNumber 
                            and pr._RecordCurrent = 1 
                            """)
    parentDf.createOrReplaceTempView('parents')
    
    systemAreaDf = spark.sql(f"""with t1 as ( 
                            select propertyNumber, wn.waterNetworkSK as potableSK, wnr.waterNetworkSK as recycledSK, 
                                    sewerNetworkSK as sewerNetworkSK, stormWaterNetworkSK as stormWaterNetworkSK, 
                                    row_number() over (partition by propertyNumber order by lp.waterPressureZone desc, lp.recycledSupplyZone desc, 
                                    lp.sewerScamp desc, lp.stormWaterCatchment desc) as rn 
                            from {ADS_DATABASE_CLEANSED}.hydra.TLotParcel lp 
                                  left outer join {ADS_DATABASE_CURATED}.dim.WaterNetwork wn on lp.waterPressureZone = wn.pressureArea and wn._RecordCurrent = 1 
                                  left outer join {ADS_DATABASE_CURATED}.dim.WaterNetwork wnr on lp.recycledSupplyZone = wnr.supplyZone and wnr._RecordCurrent = 1 
                                  left outer join {ADS_DATABASE_CURATED}.dim.SewerNetwork snw on lp.sewerScamp = snw.SCAMP and snw._RecordCurrent = 1 
                                  left outer join {ADS_DATABASE_CURATED}.dim.StormWaterNetwork sw on lp.stormWaterCatchment = sw.stormWaterCatchment and sw._RecordCurrent = 1 
                            where propertyNumber is not null 
                            and lp._RecordCurrent = 1) 
                            select propertyNumber, potableSK, recycledSK, sewerNetworkSK, stormWaterNetworkSK 
                            from t1 
                            where rn = 1 
                            """)
    systemAreaDf.createOrReplaceTempView('systemareas')
    
    #TODO: ACCESS Field Mapping
    #1.Load Cleansed layer table data into dataframe 
    accessZ309TpropertyDf = spark.sql(f"""select distinct 'ACCESS' as sourceSystemCode, 
                                            cast(pr.propertyNumber as string) as propertyNumber, 
                                            cast(pr.propertyNumber as string) as premise, 
                                            potableSK as waterNetworkSK_drinkingWater, 
                                            recycledSK as WaterNetworkSK_recycledWater, 
                                            sewerNetworkSK, 
                                            stormWaterNetworkSK, 
                                            null as objectNumber, 
                                            null as propertyInfo, 
                                            null as buildingFeeDate , 
                                            null as connectionValidFromDate , 
                                            null as CRMConnectionObjectGUID, 
                                            null as architecturalObjectNumber, 
                                            null as architecturalObjectTypeCode, 
                                            null as architecturalObjectType,
                                            null as parentArchitecturalObjectTypeCode,
                                            null as parentArchitecturalObjectType,
                                            null as parentArchitecturalObjectNumber,
                                            null as hydraBand,
                                            -1 as hydraCalculatedArea,
                                            'Unknown' as hydraAreaUnit,
                                            null as overrideArea,
                                            null as overrideAreaUnit,
                                            null as stormWaterAssessmentFlag,
                                            null as hydraAreaFlag,
                                            null as comments,
                                            pr._RecordDeleted 
                                     from {ADS_DATABASE_CLEANSED}.access.z309_tproperty pr left outer join 
                                          lots lo on lo.propertyNumber = pr.propertyNumber left outer join 
                                          parents pp on pp.propertyNumber = pr.propertyNumber 
                                          left outer join {ADS_DATABASE_CLEANSED}.isu.zcd_tinfprty_tx infsap on infsap.inferiorPropertyTypeCode = pr.propertyTypeCode and infsap._RecordCurrent = 1 
                                          left outer join {ADS_DATABASE_CLEANSED}.isu.zcd_tsupprtyp_tx supsap on supsap.superiorPropertyTypeCode = pr.superiorPropertyTypeCode and supsap._RecordCurrent = 1 
                                          left outer join systemAreas sa on sa.propertyNumber = pp.parentPropertyNumber 
                                     where pr._RecordCurrent = 1 
                                     """)
    accessZ309TpropertyDf.createOrReplaceTempView('ACCESS')
    #print(f'{accessZ309TpropertyDf.count():,} rows from ACCESS')
    
    sapisuDf = spark.sql(f"""select 
                                'ISU' as sourceSystemCode, 
                                co.propertyNumber, 
                                coalesce(regexp_replace(0ucp.premise, r'^[0]*', ''),'-1') as premise, 
                                potableSK as waterNetworkSK_drinkingWater, 
                                recycledSK as waterNetworkSK_recycledWater, 
                                sewerNetworkSK, 
                                stormWaterNetworkSK, 
                                co.objectNumber, 
                                co.propertyInfo, 
                                co.buildingFeeDate as buildingFeeDate, 
                                co.validFromDate as connectionValidFromDate , 
                                co.CRMConnectionObjectGUID, 
                                vn.architecturalObjectNumber, 
                                vn.architecturalObjectTypeCode, 
                                vn.architecturalObjectType,
                                vn.parentArchitecturalObjectTypeCode,
                                vn.parentArchitecturalObjectType,
                                vn.parentArchitecturalObjectNumber,
                                vd.hydraBand,
                                coalesce(vd.hydraCalculatedArea, -1) as hydraCalculatedArea,
                                if(vd.hydraAreaUnit is null or trim(vd.hydraAreaUnit) = '', 'Unknown', vd.hydraAreaUnit) as hydraAreaUnit,
                                vd.overrideArea,
                                vd.overrideAreaUnit,
                                coalesce(vd.stormWaterAssessmentFlag, 'N') as stormWaterAssessmentFlag,
                                coalesce(vd.hydraAreaFlag, 'N') as hydraAreaFlag,
                                vd.comments,
                                co._RecordDeleted 
                        from 
                              {ADS_DATABASE_CLEANSED}.isu.0uc_connobj_attr_2 co 
                              left outer join {ADS_DATABASE_CLEANSED}.isu.0ucpremise_attr_2 0ucp 
                              on co.propertyNumber = 0ucp.propertyNumber 
                              left outer join {ADS_DATABASE_CLEANSED}.isu.vibdao vd 
                              on co.architecturalObjectInternalId = vd.architecturalObjectInternalId and vd._RecordCurrent = 1 
                              left outer join {ADS_DATABASE_CLEANSED}.isu.vibdnode vn 
                              on co.architecturalObjectInternalId = vn.architecturalObjectInternalId and vn._RecordCurrent = 1 
                              left outer join systemAreas sa 
                              on sa.propertyNumber = coalesce(int(vn.parentArchitecturalObjectNumber),int(co.propertyNumber)) 
                         where co.propertyNumber <> '' 
                         and   co._RecordCurrent = 1 
                        """)
    sapisuDf.createOrReplaceTempView('ISU')
    #print(f'{sapisuDf.count():,} rows from SAP')
    #print('Creating 4 dummy rows...')
   
    #2.JOIN TABLES
    
    #3.UNION TABLES
#     df = spark.sql(f"""with propsFromACCESS as ( 
#                          select propertyNumber 
#                          from   ACCESS 
#                          minus 
#                          select propertyNumber 
#                          from   ISU) 
#                     select a.* 
#                     from   ACCESS a, 
#                            propsFromACCESS b 
#                     where  a.propertyNumber = b.propertyNumber 
#                     union all 
#                     select * 
#                     from   ISU""")
    df = sapisuDf
    dummyDimRecDf = spark.sql(f"""select waterNetworkSK as dummyDimSK, 'dimWaterNetwork_drinkingWater' as dimension from {ADS_DATABASE_CURATED}.dim.WaterNetwork 
                          where supplyZone='Unknown' and  pressureArea='Unknown' 
                          union select waterNetworkSK as dummyDimSK, 'dimWaterNetwork_recycledWater' as dimension from {ADS_DATABASE_CURATED}.dim.WaterNetwork 
                          where pressureArea='Unknown' and supplyZone='Unknown'  
                          union select sewerNetworkSK as dummyDimSK, 'dimSewerNetwork' as dimension from {ADS_DATABASE_CURATED}.dim.SewerNetwork where SCAMP='Unknown' 
                          union select stormWaterNetworkSK as dummyDimSK, 'dimStormWaterNetwork' as dimension from {ADS_DATABASE_CURATED}.dim.StormWaterNetwork where stormWaterCatchment='Unknown' 
                          """)
    df = df.join(dummyDimRecDf, (dummyDimRecDf.dimension == 'dimWaterNetwork_drinkingWater'), how="left") \
                  .select(df['*'], dummyDimRecDf['dummyDimSK'].alias('dummyWaterNetworkSK_drinkingWater'))
    df = df.join(dummyDimRecDf, (dummyDimRecDf.dimension == 'dimWaterNetwork_recycledWater'), how="left") \
                  .select(df['*'], dummyDimRecDf['dummyDimSK'].alias('dummyWaterNetworkSK_recycledWater'))
    df = df.join(dummyDimRecDf, (dummyDimRecDf.dimension == 'dimSewerNetwork'), how="left") \
                  .select(df['*'], dummyDimRecDf['dummyDimSK'].alias('dummySewerNetworkSK'))
    df = df.join(dummyDimRecDf, (dummyDimRecDf.dimension == 'dimStormWaterNetwork'), how="left") \
                  .select(df['*'], dummyDimRecDf['dummyDimSK'].alias('dummyStormWaterNetworkSK'))
    
    dummyDimDf = spark.sql(f"""select '-1' as propertyNumber, wn.waterNetworkSK as waterNetworkSK_drinkingWater, wn.waterNetworkSK as waterNetworkSK_recycledWater, sewerNetworkSK, stormWaterNetworkSK , 
                                  -1 as hydraCalculatedArea, 'Unknown' as hydraAreaUnit, '-1' as premise  
                            from {ADS_DATABASE_CURATED}.dim.WaterNetwork wn, 
                                 {ADS_DATABASE_CURATED}.dim.SewerNetwork snw, 
                                 {ADS_DATABASE_CURATED}.dim.StormWaterNetwork sw 
                            where wn.pressureArea = 'Unknown' 
                            and   wn.supplyZone = 'Unknown' 
                            and   wn._RecordCurrent = 1 
                            and   snw.SCAMP = 'Unknown' 
                            and   snw._RecordCurrent = 1 
                            and   sw.stormWaterCatchment = 'Unknown' 
                            and   sw._RecordCurrent = 1 
                            """)
    df = df.unionByName(dummyDimDf, allowMissingColumns = True)
    
    #4.SELECT / TRANSFORM
    df = df.selectExpr( 
                       "sourceSystemCode" \
                       ,"propertyNumber" \
                       ,"premise" \
                       ,"coalesce(waterNetworkSK_drinkingWater, dummyWaterNetworkSK_drinkingWater) as waterNetworkSK_drinkingWater" \
                       ,"coalesce(waterNetworkSK_recycledWater, dummyWaterNetworkSK_recycledWater) as waterNetworkSK_recycledWater" \
                       ,"coalesce(sewerNetworkSK, dummySewerNetworkSK) as sewerNetworkSK" \
                       ,"coalesce(stormWaterNetworkSK, dummyStormWaterNetworkSK) as stormWaterNetworkSK" \
                       ,"objectNumber" \
                       ,"propertyInfo" \
                       ,"buildingFeeDate" \
                       ,"connectionValidFromDate" \
                       ,"CRMConnectionObjectGUID" \
                       ,"architecturalObjectNumber" \
                       ,"architecturalObjectTypeCode" \
                       ,"architecturalObjectType" \
                       ,"parentArchitecturalObjectTypeCode" \
                       ,"parentArchitecturalObjectType" \
                       ,"parentArchitecturalObjectNumber" \
                       ,"hydraBand" \
                       ,"hydraCalculatedArea" \
                       ,"hydraAreaUnit" \
                       ,"overrideArea" \
                       ,"overrideAreaUnit" \
                       ,"stormWaterAssessmentFlag" \
                       ,"hydraAreaFlag" \
                       ,"comments" \
                        ,"_RecordDeleted" 
                        )
    
    #set system area defaults
    df.createOrReplaceTempView('allProps')
#     df = spark.sql(f"select a.propertyNumber, sourceSystemCode, coalesce(waterNetworkSK_drinkingWater,potableSK) as waterNetworkSK_drinkingWater, coalesce(waterNetworkSK_recycledWater,recycledSK) as waterNetworkSK_recycledWater,  \
#                             coalesce(a.sewerNetworkSK,b.sewerNetworkSK) as sewerNetworkSK, coalesce(a.stormWaterNetworkSK, b.stormWaterNetworkSK) as stormWaterNetworkSK, propertyTypeCode \
#                             propertyType, superiorPropertyTypeCode, superiorPropertyType, areaSize, parentPropertyNumber, parentPropertyTypeCode, parentPropertyType, parentSuperiorPropertyTypeCode, parentSuperiorPropertyType, \
#                             planTypeCode, planType, lotTypeCode, lotType, planNumber, lotNumber, sectionNumber, architecturalTypeCode, architecturalType \
#                       from allprops a, \
#                            systemareas b \
#                       where b.propertyNumber = '-1' \
#                       ")
    
    #5.Apply schema definition
    schema = StructType([
                            StructField('propertySK', StringType(), False),
                            StructField('sourceSystemCode', StringType(), True),
                            StructField("propertyNumber", StringType(), False),
                            StructField("premise", StringType(), False),
                            StructField("waterNetworkSK_drinkingWater", StringType(), False),
                            StructField("waterNetworkSK_recycledWater", StringType(), False),
                            StructField("sewerNetworkSK", StringType(), False),
                            StructField("stormWaterNetworkSK", StringType(), False),
                            StructField("objectNumber", StringType(), True),
                            StructField("propertyInfo", StringType(), True),
                            StructField("buildingFeeDate", DateType(), True),
                            StructField("connectionValidFromDate", DateType(), True),
                            StructField("CRMConnectionObjectGUID", StringType(), True),
                            StructField('architecturalObjectNumber', StringType(), True),
                            StructField('architecturalObjectTypeCode', StringType(), True),
                            StructField('architecturalObjectType', StringType(), True),
                            StructField('parentArchitecturalObjectTypeCode', StringType(), True),
                            StructField('parentArchitecturalObjectType', StringType(), True),
                            StructField('parentArchitecturalObjectNumber', StringType(), True),
                            StructField('hydraBand', StringType(), True),
                            StructField('hydraCalculatedArea', DecimalType(18,6), False),
                            StructField('hydraAreaUnit', StringType(), False),
                            StructField('overrideArea', DecimalType(18,6), True),
                            StructField('overrideAreaUnit', StringType(), True),
                            StructField('stormWaterAssessmentFlag', StringType(), True),
                            StructField('hydraAreaFlag', StringType(), True),
                            StructField('comments', StringType(), True),
                      ])

    return df, schema

# COMMAND ----------

df, schema = getProperty()
#TemplateEtl(df, entity="dim.Property", businessKey="propertyNumber", schema=schema, writeMode=ADS_WRITE_MODE_OVERWRITE, AddSK=True)

curnt_table = f'{ADS_DATABASE_CURATED}.dim.property'
curnt_pk = 'propertyNumber' 
curnt_recordStart_pk = 'propertyNumber'
history_table = f'{ADS_DATABASE_CURATED}.dim.propertyTypeHistory'
history_table_pk = 'propertyNumber'
history_table_pk_convert = 'propertyNumber'

df_ = appendRecordStartFromHistoryTable(df,history_table,history_table_pk,curnt_pk,history_table_pk_convert,curnt_recordStart_pk)
updateDBTableWithLatestRecordStart(df_, curnt_table, curnt_pk)

TemplateEtlSCD(df_, entity="dim.property", businessKey="propertyNumber", schema=schema)

# COMMAND ----------

# MAGIC %md
# MAGIC Set the network details for cancelled properties to those of their now active equivalent (to be expanded for SAP cancelled props)

# COMMAND ----------

spark.sql(f"""with t1 as (select cp.propertyNumber, waterNetworkSK_drinkingWater, waterNetworkSK_recycledWater, sewerNetworkSK, stormWaterNetworkSK 
             from   {ADS_DATABASE_CURATED}.dim.property p,
                    curated.ACCESSCancelledActiveProps cp
             where  p.propertyNumber = cp.activeProperty and p._RecordCurrent = 1)
         merge into {ADS_DATABASE_CURATED}.dim.property p
         using      t1
         on         p.propertyNumber = t1.propertyNumber and p._RecordCurrent = 1
         when matched then update 
              set p.waterNetworkSK_drinkingWater = t1.waterNetworkSK_drinkingWater,
                  p.waterNetworkSK_recycledWater = t1.waterNetworkSK_recycledWater,
                  p.sewerNetworkSK = t1.sewerNetworkSK,
                  p.stormWaterNetworkSK = t1.stormWaterNetworkSK""")

# COMMAND ----------

dbutils.notebook.exit("1")
