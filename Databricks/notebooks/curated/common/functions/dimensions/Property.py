# Databricks notebook source
###########################################################################################################################
# Function: getProperty
#  GETS Property DIMENSION 
# Returns:
#  Dataframe of transformed Property
#############################################################################################################################
# Method
# 1.Create Function
# 2.Load Cleansed layer table data into dataframe and transform
# 3.JOIN TABLES
# 4.UNION TABLES
# 5.SELECT / TRANSFORM
#############################################################################################################################
#1.Create Function
def getProperty():

#     spark.udf.register("TidyCase", GeneralToTidyCase)  
    #build a dataframe with unique properties and lot details, 4174119 is incorrectly present on Tlot
    lotDf = spark.sql(f"select propertyNumber, \
                                first(planTypeCode) as planTypeCode, \
                                first(planType) as planType, \
                                first(planNumber) as planNumber, \
                                first(lotNumber)  as lotNumber, \
                                first(lotTypeCode) as lotTypeCode, \
                                first(lotType) as lotType, \
                                first(sectionNumber) as sectionNumber \
                        from {ADS_DATABASE_CLEANSED}.access_z309_tlot \
                        where _RecordCurrent = 1 \
                        group by propertyNumber \
                        union all \
                        select propertyNumber, \
                                'SP' as planTypeCode, \
                                'Strata Plan' as planType, \
                                strataPlanNumber as planNumber, \
                                strataPlanLot as lotNumber, \
                                '01' as lotTypeCode, \
                                'Lot' as lotType, \
                                null as sectionNumber \
                        from {ADS_DATABASE_CLEANSED}.access_z309_tstrataunits \
                        where _RecordCurrent = 1 \
                        ")
    lotDf.createOrReplaceTempView('lots')
    #build a dataframe with parent properties (strata units with children, joint services, super lots and link lots), 'normal' properties with themselves)
    parentDf = spark.sql(f"with t1 as( \
                                select su.propertyNumber, \
                                        ms.masterPropertyNumber as parentPropertyNumber, \
                                        pr.propertyTypeCode as parentPropertyTypeCode, \
                                        pr.propertyType as parentPropertyType, \
                                        pr.superiorPropertyTypeCode as parentSuperiorPropertyTypeCode, \
                                        pr.superiorPropertyType as parentSuperiorPropertyType, \
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
                             t2 as( \
                                select rp.propertyNumber as propertyNumber, \
                                        first(rp.relatedPropertyNumber) as parentPropertyNumber, \
                                        first(pr.propertyTypeCode) as parentPropertyTypeCode, \
                                        first(pr.propertyType) as parentPropertyType, \
                                        first(pr.superiorPropertyTypeCode) as parentSuperiorPropertyTypeCode, \
                                        first(pr.superiorPropertyType) as parentSuperiorPropertyType, \
                                        first(rp.relationshipType) as relationshipType \
                                from {ADS_DATABASE_CLEANSED}.access_z309_trelatedProps rp inner join remainingProps rem on rp.propertyNumber = rem.propertyNumber \
                                       left outer join {ADS_DATABASE_CLEANSED}.access_z309_tproperty pr on pr.propertynumber = rp.relatedPropertynumber \
                                where rp.relationshipTypeCode in ('M','P','U') \
                                and   rp._RecordCurrent = 1 \
                                group by rp.propertyNumber \
                                order by parentPropertyTypeCode desc), \
                              t3 as(select * from t1 \
                                    union \
                                    select * from t2), \
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
    
    systemAreaDf = spark.sql(f" \
                            select propertyNumber, first(wn.dimWaterNetworkSK) as potableSK, first(wnr.dimWaterNetworkSK) as recycledSK, first(dimSewerNetworkSK) as sewerNetworkSK, first(dimStormWaterNetworkSK) as stormWaterNetworkSK \
                            from {ADS_DATABASE_CLEANSED}.hydra_TLotParcel lp left outer join curated.dimWaterNetwork wn on lp.waterPressureZone = wn.pressureArea and wn._RecordCurrent = 1 \
                                  left outer join {ADS_DATABASE_CURATED}.dimWaterNetwork wnr on lp.recycledSupplyZone = wnr.supplyZone and wnr._RecordCurrent = 1 \
                                  left outer join {ADS_DATABASE_CURATED}.dimSewerNetwork snw on lp.sewerScamp = snw.SCAMP and snw._RecordCurrent = 1 \
                                  left outer join {ADS_DATABASE_CURATED}.dimStormWaterNetwork sw on lp.stormWaterCatchment = sw.stormWaterCatchment and sw._RecordCurrent = 1\
                            where propertyNumber is not null \
                            and lp._RecordCurrent = 1 \
                            group by propertyNumber \
                            ")
    systemAreaDf.createOrReplaceTempView('systemareas')
    #dimProperty
    #2.Load Cleansed layer table data into dataframe
    accessZ309TpropertyDf = spark.sql(f"select distinct cast(pr.propertyNumber as string), \
                                            'ACCESS' as sourceSystemCode, \
                                            potableSK as waterNetworkSK_drinkingWater, \
                                            recycledSK as WaterNetworkSK_recycledWater, \
                                            sewerNetworkSK, \
                                            stormWaterNetworkSK, \
                                            propertyTypeCode, \
                                            propertyType, \
                                            superiorPropertyTypeCode, \
                                            superiorPropertyType, \
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
                                          parents pp on pp.propertyNumber = pr.propertyNumber left outer join \
                                          systemAreas sa on sa.propertyNumber = pp.parentPropertyNumber \
                                     where pr._RecordCurrent = 1 \
                                     ")
    
    print(f'{accessZ309TpropertyDf.count():,} rows from ACCESS')
    
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
                              and dt._RecordDeleted = 0 and dt._RecordCurrent = 1 left outer join \
                              systemAreas sa on sa.propertyNumber = coalesce(vn.parentArchitecturalObjectNumber,co.propertyNumber) \
                         where co.propertyNumber <> '' \
                         and   co._RecordDeleted = 0 \
                         and   co._RecordCurrent = 1 \
                        ")
    
    print(f'{sapisuDf.count():,} rows from SAP')
    print('Creating 4 dummy rows...')
    #Dummy Record to be added to Property Dimension
    dummyDimRecDf = spark.createDataFrame([("ISU","-1"),("ACCESS","-2"),("ISU","-3"),("ACCESS","-4")], ["sourceSystemCode", "propertyNumber"])

    #3.JOIN TABLES  
    #4.UNION TABLES
    df = accessZ309TpropertyDf.union(sapisuDf)
    df = df.unionByName(dummyDimRecDf, allowMissingColumns = True)
    print(f'{df.count():,} rows after Union')

    #5.SELECT / TRANSFORM
    df = df.selectExpr( \
                         "propertyNumber" \
                        ,"sourceSystemCode" \
                        ,"waterNetworkSK_drinkingWater" \
                        ,"waterNetworkSK_recycledWater" \
                        ,"sewerNetworkSK" \
                        ,"stormWaterNetworkSK" \
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
                                            
    #6.Apply schema definition
    newSchema = StructType([
                            StructField("propertyNumber", StringType(), False),
                            StructField("sourceSystemCode", StringType(), False),
                            StructField("waterNetworkSK_drinkingWater", StringType(), True),
                            StructField("waterNetworkSK_recycledWater", StringType(), True),
                            StructField("sewerNetworkSK", StringType(), True),
                            StructField("stormWaterNetworkSK", StringType(), True),
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

    df = spark.createDataFrame(df.rdd, schema=newSchema)
    return df


# COMMAND ----------

# ISUDummy = tuple(['-1','ISU','1900-01-01','9999-12-31'] + ['Unknown'] * 4 + [0] + ['Unknown'] * (len(sapisuDf.columns) - 9)) #this only works as long as all output columns are string
#     ACCESSDummy = tuple(['-2','ACCESS','1900-01-01','9999-12-31'] + ['Unknown'] * 4 + [0] + ['Unknown'] * (len(sapisuDf.columns) - 9)) #this only works as long as all output columns are string
#     dummyDimRecDf = spark.createDataFrame([ISUDummy, ACCESSDummy], sapisuDf.columns)
#     dummyDimRecDf = dummyDimRecDf.withColumn("propertyStartDate",dummyDimRecDf['propertyStartDate'].cast(DateType())).withColumn("propertyEndDate",dummyDimRecDf['propertyEndDate'].cast(DateType()))
