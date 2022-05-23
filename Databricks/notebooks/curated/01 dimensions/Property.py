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

#-----------------------------------------------------------------------------------------------
# Note: Due to the fact that dimProperty relies on the system area tables having been populated,
# SewerNetwork, StormWaterNetwork and WaterNetwork notebooks are included in this notebook. 
# This takes care of the load sequence.
#-----------------------------------------------------------------------------------------------

# COMMAND ----------

def getProperty():

    #1.Load current Cleansed layer table data into dataframe
    #build a dataframe with unique properties and lot details, 4174119 is incorrectly present on Tlot
    lotDf = spark.sql(f"select propertyNumber, \
                                first(coalesce(planTypeCodeSAP,planTypeCode)) as planTypeCode, \
                                first(coalesce(planTypeSAP,planType)) as planType, \
                                first(planNumber) as planNumber, \
                                first(lotNumber)  as lotNumber, \
                                first(lotTypeCode) as lotTypeCode, \
                                first(coalesce(lotTypeSAP,lotType)) as lotType, \
                                first(sectionNumber) as sectionNumber \
                        from {ADS_DATABASE_CLEANSED}.access_z309_tlot \
                        where _RecordCurrent = 1 \
                        group by propertyNumber \
                        union all \
                        select propertyNumber, \
                                '02' as planTypeCode, \
                                'Strata Plan' as planType, \
                                strataPlanNumber as planNumber, \
                                strataPlanLot as lotNumber, \
                                '01' as lotTypeCode, \
                                'Full' as lotType, \
                                null as sectionNumber \
                        from {ADS_DATABASE_CLEANSED}.access_z309_tstrataunits \
                        where _RecordCurrent = 1 \
                        ")
    lotDf.createOrReplaceTempView('lots')
    #build a dataframe with parent properties (strata units with children, joint services (M), super lots (P) and link lots(U)), 'normal' properties with themselves)
    parentDf = spark.sql(f"with t1 as( \
                                select su.propertyNumber, \
                                        ms.masterPropertyNumber as parentPropertyNumber, \
                                        pr.propertyTypeCode as parentPropertyTypeCode, \
                                        coalesce(pr.propertyTypeSAP,pr.propertyType) as parentPropertyType, \
                                        pr.superiorPropertyTypeCode as parentSuperiorPropertyTypeCode, \
                                        coalesce(pr.superiorPropertyTypeSAP,pr.superiorPropertyType) as parentSuperiorPropertyType, \
                                       'Child of Master Strata' as relationshipType \
                                from {ADS_DATABASE_CLEANSED}.access_z309_tstrataunits su \
                                      inner join {ADS_DATABASE_CLEANSED}.access_z309_tmastrataplan ms on su.strataPlanNumber = ms.strataPlanNumber and ms._RecordCurrent = 1 \
                                      left outer join {ADS_DATABASE_CLEANSED}.access_z309_tproperty pr on pr.propertynumber = ms.masterPropertynumber and pr._RecordCurrent = 1 \
                                where su._RecordCurrent = 1), \
                             remainingProps as(select propertyNumber \
                                               from   {ADS_DATABASE_CLEANSED}.access_z309_tproperty \
                                               where  _RecordCurrent = 1 \
                                               minus \
                                               select propertyNumber \
                                               from   t1), \
                             relatedprops as( \
                                select rp.propertyNumber as propertyNumber, \
                                        rp.relatedPropertyNumber as parentPropertyNumber, \
                                        pr.propertyTypeCode as parentPropertyTypeCode, \
                                        coalesce(pr.propertyTypeSAP,pr.propertyType) as parentPropertyType, \
                                        pr.superiorPropertyTypeCode as parentSuperiorPropertyTypeCode, \
                                        coalesce(pr.superiorPropertyTypeSAP,pr.superiorPropertyType) as parentSuperiorPropertyType, \
                                        rp.relationshipType as relationshipType, \
                                        row_number() over (partition by rp.propertyNumber order by relationshipTypecode desc) as rn \
                                from {ADS_DATABASE_CLEANSED}.access_z309_trelatedProps rp inner join remainingProps rem on rp.propertyNumber = rem.propertyNumber \
                                       left outer join {ADS_DATABASE_CLEANSED}.access_z309_tproperty pr on pr.propertynumber = rp.relatedPropertynumber \
                                where rp.relationshipTypeCode in ('M','P','U') \
                                and   rp._RecordCurrent = 1), \
                              t3 as(select * from t1 \
                                    union \
                                    select propertyNumber, \
                                           parentPropertyNumber, \
                                           parentPropertyTypeCode, \
                                           parentPropertyType, \
                                           parentSuperiorPropertyTypeCode, \
                                           parentSuperiorPropertyType, \
                                           relationshipType \
                                    from relatedprops \
                                    where rn = 1), \
                              t4 as(select propertyNumber \
                                    from {ADS_DATABASE_CLEANSED}.access_z309_tproperty \
                                    where _RecordCurrent = 1 \
                                    minus \
                                    select propertyNumber from t3) \
                            select * from t3 \
                            union all \
                            select pr.propertyNumber, \
                                    pr.propertyNumber as parentPropertyNumber, \
                                    propertyTypeCode as parentPropertyTypeCode, \
                                    propertyType as parentPropertyType, \
                                    superiorPropertyTypeCode as parentSuperiorPropertyTypeCode, \
                                    superiorPropertyType as parentSuperiorPropertyType, \
                                    'Self as Parent' as relationshipType \
                            from {ADS_DATABASE_CLEANSED}.access_z309_tproperty pr, t4 \
                            where pr.propertyNumber = t4.propertyNumber \
                            and pr._RecordCurrent = 1 \
                            ")
    parentDf.createOrReplaceTempView('parents')
    
    systemAreaDf = spark.sql(f"with t1 as ( \
                            select propertyNumber, wn.waterNetworkSK as potableSK, wnr.waterNetworkSK as recycledSK, \
                                    sewerNetworkSK as sewerNetworkSK, stormWaterNetworkSK as stormWaterNetworkSK, \
                                    row_number() over (partition by propertyNumber order by lp.waterPressureZone desc, lp.recycledSupplyZone desc, \
                                    lp.sewerScamp desc, lp.stormWaterCatchment desc) as rn \
                            from {ADS_DATABASE_CLEANSED}.hydra_TLotParcel lp \
                                  left outer join {ADS_DATABASE_CURATED}.dimWaterNetwork wn on lp.waterPressureZone = wn.pressureArea and wn._RecordCurrent = 1 \
                                  left outer join {ADS_DATABASE_CURATED}.dimWaterNetwork wnr on lp.recycledSupplyZone = wnr.supplyZone and wnr._RecordCurrent = 1 \
                                  left outer join {ADS_DATABASE_CURATED}.dimSewerNetwork snw on lp.sewerScamp = snw.SCAMP and snw._RecordCurrent = 1 \
                                  left outer join {ADS_DATABASE_CURATED}.dimStormWaterNetwork sw on lp.stormWaterCatchment = sw.stormWaterCatchment and sw._RecordCurrent = 1 \
                            where propertyNumber is not null \
                            and lp._RecordCurrent = 1) \
                            select propertyNumber, potableSK, recycledSK, sewerNetworkSK, stormWaterNetworkSK \
                            from t1 \
                            where rn = 1 \
                            union all \
                            select '-1' as propertyNumber, wn.waterNetworkSK as potableSK, wnr.waterNetworkSK as recycledSK, sewerNetworkSK, stormWaterNetworkSK \
                            from {ADS_DATABASE_CURATED}.dimWaterNetwork wn, \
                                 {ADS_DATABASE_CURATED}.dimWaterNetwork wnr, \
                                 {ADS_DATABASE_CURATED}.dimSewerNetwork snw, \
                                 {ADS_DATABASE_CURATED}.dimStormWaterNetwork sw \
                            where wn.pressureArea = '-1' \
                            and   wn._RecordCurrent = 1 \
                            and   wnr.supplyZone = '-1' \
                            and   wnr._RecordCurrent = 1 \
                            and   snw.SCAMP = '-1' \
                            and   snw._RecordCurrent = 1 \
                            and   sw.stormWaterCatchment = '-1' \
                            and   sw._RecordCurrent = 1 \
                            ")
    systemAreaDf.createOrReplaceTempView('systemareas')
    
    #1.Load Cleansed layer table data into dataframe
    accessZ309TpropertyDf = spark.sql(f"select distinct cast(pr.propertyNumber as string), \
                                            'ACCESS' as sourceSystemCode, \
                                            potableSK as waterNetworkSK_drinkingWater, \
                                            recycledSK as WaterNetworkSK_recycledWater, \
                                            sewerNetworkSK, \
                                            stormWaterNetworkSK, \
                                            propertyTypeCode, \
                                            initcap(coalesce(propertyTypeSAP,propertyType)) as propertyType, \
                                            superiorPropertyTypeCode, \
                                            initcap(coalesce(superiorPropertyTypeSAP,superiorPropertyType)) as superiorPropertyType, \
                                            CASE WHEN propertyAreaTypeCode == 'H' THEN  propertyArea * 10000 \
                                                                                  ELSE propertyArea END AS areaSize, \
                                            pp.parentPropertyNumber as parentPropertyNumber, \
                                            pp.parentPropertyTypeCode as parentPropertyTypeCode, \
                                            pp.parentPropertyType as parentPropertyType, \
                                            pp.parentSuperiorPropertyTypeCode as parentSuperiorPropertyTypeCode, \
                                            pp.parentSuperiorPropertyType as parentSuperiorPropertyType, \
                                            lo.planTypeCode, \
                                            lo.planType, \
                                            lo.lotTypeCode as lotTypeCode, \
                                            lo.lotType as lotType, \
                                            lo.planNumber as planNumber, \
                                            lo.lotNumber as lotNumber, \
                                            lo.sectionNumber as sectionNumber, \
                                            null as architecturalTypeCode, \
                                            null as architecturalType \
                                     from {ADS_DATABASE_CLEANSED}.access_z309_tproperty pr left outer join \
                                          lots lo on lo.propertyNumber = pr.propertyNumber left outer join \
                                          parents pp on pp.propertyNumber = pr.propertyNumber inner join \
                                          systemAreas sa on sa.propertyNumber = coalesce(pp.parentPropertyNumber,'-1') \
                                     where pr._RecordCurrent = 1 \
                                     ")
    accessZ309TpropertyDf.createOrReplaceTempView('ACCESS')
    #print(f'{accessZ309TpropertyDf.count():,} rows from ACCESS')
    
    sapisuDf = spark.sql(f"select co.propertyNumber, \
                                'ISU' as sourceSystemCode, \
                                potableSK as waterNetworkSK_drinkingWater, \
                                recycledSK as waterNetworkSK_recycledWater, \
                                sewerNetworkSK, \
                                stormWaterNetworkSK, \
                                co.inferiorPropertyTypecode as propertyTypeCode, \
                                initcap(co.inferiorPropertyType) as propertyType, \
                                co.superiorPropertyTypecode as superiorPropertyTypeCode, \
                                initcap(co.superiorPropertyType) as superiorPropertyType, \
                                CASE WHEN vd.hydraAreaUnit == 'HAR' THEN cast(vd.hydraCalculatedArea * 10000 as dec(18,6)) \
                                     WHEN vd.hydraAreaUnit == 'M2'  THEN cast(vd.hydraCalculatedArea as dec(18,6)) \
                                                                    ELSE null END AS areaSize, \
                                coalesce(vn.parentArchitecturalObjectNumber,co.propertyNumber) as parentPropertyNumber, \
                                pa.inferiorPropertyTypeCode as parentPropertyTypeCode, \
                                initcap(pa.inferiorPropertyType) as parentPropertyType, \
                                pa.superiorPropertyTypeCode as parentSuperiorPropertyTypeCode, \
                                initcap(pa.superiorPropertyType) as parentSuperiorPropertyType, \
                                co.planTypeCode, \
                                co.planType, \
                                co.lotTypeCode, \
                                dt.domainValueText as lotType, \
                                co.planNumber, \
                                co.lotNumber, \
                                co.sectionNumber, \
                                co.architecturalObjectTypeCode as architecturalTypeCode, \
                                co.architecturalObjectType as architecturalType \
                        from {ADS_DATABASE_CLEANSED}.isu_0uc_connobj_attr_2 co left outer join \
                             {ADS_DATABASE_CLEANSED}.isu_vibdao vd on co.architecturalObjectInternalId = vd.architecturalObjectInternalId and vd._RecordDeleted = 0 \
                              and vd._RecordCurrent = 1 left outer join \
                             {ADS_DATABASE_CLEANSED}.isu_vibdnode vn on co.architecturalObjectInternalId = vn.architecturalObjectInternalId and vn._RecordDeleted = 0 \
                              and   vn._RecordCurrent = 1 left outer join \
                             {ADS_DATABASE_CLEANSED}.isu_0uc_connobj_attr_2 pa on pa.architecturalObjectId = coalesce(vn.parentArchitecturalObjectNumber,co.propertyNumber) \
                              and   pa._RecordCurrent = 1 and pa._RecordDeleted = 0 left outer join \
                             {ADS_DATABASE_CLEANSED}.isu_dd07t dt on co.lotTypeCode = dt.domainValueSingleUpperLimit and domainName = 'ZCD_DO_ADDR_LOT_TYPE' \
                              and dt._RecordDeleted = 0 and dt._RecordCurrent = 1 inner join \
                              systemAreas sa on sa.propertyNumber = coalesce(int(vn.parentArchitecturalObjectNumber),int(co.propertyNumber)) \
                         where co.propertyNumber <> '' \
                         and   co._RecordDeleted = 0 \
                         and   co._RecordCurrent = 1 \
                        ")
    sapisuDf.createOrReplaceTempView('ISU')
    #print(f'{sapisuDf.count():,} rows from SAP')
    #print('Creating 4 dummy rows...')
   
    #2.JOIN TABLES
    
    #3.UNION TABLES
    df = spark.sql("with propsFromACCESS as ( \
                         select propertyNumber \
                         from   ACCESS \
                         minus \
                         select propertyNumber \
                         from   ISU) \
                    select a.* \
                    from   ACCESS a, \
                           propsFromACCESS b \
                    where  a.propertyNumber = b.propertyNumber \
                    union all \
                    select * \
                    from   ISU")
    
    dummyDimRecDf = spark.sql(f"select waterNetworkSK as dummyDimSK, 'dimWaterNetwork' as dimension from {ADS_DATABASE_CURATED}.dimWaterNetwork where pressureArea in ('-1') and supplyZone in ('-1') \
                          union select sewerNetworkSK as dummyDimSK, 'dimSewerNetwork' as dimension from {ADS_DATABASE_CURATED}.dimSewerNetwork where SCAMP in ('-1') \
                          union select stormWaterNetworkSK as dummyDimSK, 'dimStormWaterNetwork' as dimension from {ADS_DATABASE_CURATED}.dimStormWaterNetwork where stormWaterCatchment in ('-1') \
                          ")
    df = df.join(dummyDimRecDf, (dummyDimRecDf.dimension == 'dimWaterNetwork'), how="left") \
                  .select(df['*'], dummyDimRecDf['dummyDimSK'].alias('dummyWaterNetworkSK'))
    df = df.join(dummyDimRecDf, (dummyDimRecDf.dimension == 'dimSewerNetwork'), how="left") \
                  .select(df['*'], dummyDimRecDf['dummyDimSK'].alias('dummySewerNetworkSK'))
    df = df.join(dummyDimRecDf, (dummyDimRecDf.dimension == 'dimStormWaterNetwork'), how="left") \
                  .select(df['*'], dummyDimRecDf['dummyDimSK'].alias('dummyStormWaterNetworkSK'))
    
    #4.SELECT / TRANSFORM
    df = df.selectExpr( \
                         "propertyNumber" \
                        ,"sourceSystemCode" \
                        ,"coalesce(waterNetworkSK_drinkingWater, dummyWaterNetworkSK) as waterNetworkSK_drinkingWater" \
                        ,"coalesce(waterNetworkSK_recycledWater, dummyWaterNetworkSK) as waterNetworkSK_recycledWater" \
                        ,"coalesce(sewerNetworkSK, dummySewerNetworkSK) as sewerNetworkSK" \
                        ,"coalesce(stormWaterNetworkSK, dummyStormWaterNetworkSK) as stormWaterNetworkSK" \
                        ,"propertyTypeCode" \
                        ,"propertyType" \
                        ,"superiorPropertyTypeCode" \
                        ,"superiorPropertyType" \
                        ,"areaSize" \
                        ,'parentPropertyNumber' \
                        ,'parentPropertyTypeCode' \
                        ,'parentPropertyType' \
                        ,'parentSuperiorPropertyTypeCode' \
                        ,'parentSuperiorPropertyType' \
                        ,'planTypeCode' \
                        ,'planType' \
                        ,'lotTypeCode' \
                        ,'lotType' \
                        ,'planNumber' \
                        ,'lotNumber' \
                        ,'sectionNumber' \
                        ,'architecturalTypeCode' \
                        ,'architecturalType' \
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
                            StructField('propertySK', LongType(), False),
                            StructField("propertyNumber", StringType(), False),
                            StructField("sourceSystemCode", StringType(), False),
                            StructField("waterNetworkSK_drinkingWater", StringType(), False),
                            StructField("waterNetworkSK_recycledWater", StringType(), False),
                            StructField("sewerNetworkSK", StringType(), False),
                            StructField("stormWaterNetworkSK", StringType(), False),
                            StructField("propertyTypeCode", StringType(), True),
                            StructField("propertyType", StringType(), True),
                            StructField("superiorPropertyTypeCode", StringType(), True),
                            StructField("superiorPropertyType", StringType(), True),
                            StructField("areaSize", DecimalType(18,6), True),
                            StructField('parentPropertyNumber', StringType(), True),
                            StructField('parentPropertyTypeCode', StringType(), True),
                            StructField('parentPropertyType', StringType(), True),
                            StructField('parentSuperiorPropertyTypeCode', StringType(), True),
                            StructField('parentSuperiorPropertyType', StringType(), True),
                            StructField('planTypeCode', StringType(), True),
                            StructField('planType', StringType(), True),
                            StructField('lotTypeCode', StringType(), True),
                            StructField('lotType', StringType(), True),
                            StructField('planNumber', StringType(), True),
                            StructField('lotNumber', StringType(), True),
                            StructField('sectionNumber', StringType(), True),
                            StructField('architecturalTypeCode', StringType(), True),
                            StructField('architecturalType', StringType(), True),
                      ])

    return df, schema


# COMMAND ----------

df, schema = getProperty()
TemplateEtl(df, entity="dimProperty", businessKey="propertyNumber", schema=schema, writeMode=ADS_WRITE_MODE_MERGE, AddSK=True)

# COMMAND ----------

dbutils.notebook.exit("1")

# COMMAND ----------


