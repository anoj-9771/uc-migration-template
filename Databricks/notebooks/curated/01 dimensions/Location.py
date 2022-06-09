# Databricks notebook source
###########################################################################################################################
# Loads LOCATION dimension 
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

def getLocation():

    #1.Load Cleansed layer table data into dataframe
    #collect parent properties and then parents of child properties so you get the parent address against the child property
    ISULocationDf = spark.sql(f"select distinct d.propertyNumber as locationID, \
                                     'ISU' as sourceSystemCode, \
                                     upper(trim(trim(coalesce(c.houseNumber2,'')||' '||coalesce(c.houseNumber1,''))||' '||trim(c.streetName||' '||coalesce(c.streetLine1,''))||' '||coalesce(c.streetLine2,''))|| \
                                     ', '||c.cityName||' NSW '||c.postCode)  as formattedAddress, \
                                     upper(c.houseNumber2) as houseNumber2, \
                                     upper(c.houseNumber1) as houseNumber1, \
                                     upper(c.streetName) as streetName, \
                                     upper(trim(coalesce(streetLine1,'')||' '||coalesce(streetLine2,''))) as streetType, \
                                     coalesce(a.LGA,d.LGA) as LGA, \
                                     upper(c.cityName) as suburb, \
                                     c.stateCode as state, \
                                     c.postCode, \
                                     a.latitude, \
                                     a.longitude \
                                     from {ADS_DATABASE_CLEANSED}.isu_0uc_connobj_attr_2 d left outer join {ADS_DATABASE_CLEANSED}.isu_vibdnode b on d.propertyNumber = b.architecturalObjectNumber \
                                         left outer join (select propertyNumber, lga, latitude, longitude from \
                                              (select propertyNumber, lga, latitude, longitude, \
                                                row_number() over (partition by propertyNumber order by areaSize desc) recNum \
                                                from cleansed.hydra_tlotparcel where _RecordDeleted = 0 and _RecordCurrent = 1 ) \
                                                where recNum = 1) a on a.propertyNumber = b.architecturalObjectNumber, \
                                          {ADS_DATABASE_CLEANSED}.isu_0funct_loc_attr c \
                                     where b.architecturalObjectNumber = c.functionalLocationNumber \
                                     and b.parentArchitecturalObjectNumber is null \
                                     and b._RecordDeleted = 0 \
                                     and b._RecordCurrent = 1 \
                                     and c._RecordDeleted = 0 \
                                     and c._RecordCurrent = 1 \
                                     and d._RecordDeleted = 0 \
                                     and d._RecordCurrent = 1 \
                                     union all \
                                     select distinct d.propertyNumber as locationID, \
                                     'ISU' as sourceSystemCode, \
                                     upper(trim(trim(coalesce(c1.houseNumber2,'')||' '||coalesce(c1.houseNumber1,''))||' '||trim(c1.streetName||' '||coalesce(c1.streetLine1,''))||' '||coalesce(c1.streetLine2,''))||', '||c1.cityName||' NSW '||c1.postCode)  as formattedAddress, \
                                     upper(c.houseNumber2) as houseNumber2, \
                                     upper(c.houseNumber1) as houseNumber1, \
                                     upper(c.streetName) as streetName, \
                                     upper(trim(coalesce(c.streetLine1,'')||' '||coalesce(c.streetLine2,''))) as streetType, \
                                     coalesce(a.LGA,d.LGA) as LGA, \
                                     upper(c.cityName) as suburb, \
                                     c.stateCode as state, \
                                     c.postCode, \
                                     a.latitude, \
                                     a.longitude \
                                     from {ADS_DATABASE_CLEANSED}.isu_0uc_connobj_attr_2 d left outer join {ADS_DATABASE_CLEANSED}.isu_vibdnode b on d.propertyNumber = b.architecturalObjectNumber \
                                         left outer join (select propertyNumber, lga, latitude, longitude from \
                                              (select propertyNumber, lga, latitude, longitude, \
                                                row_number() over (partition by propertyNumber order by areaSize desc) recNum \
                                                from cleansed.hydra_tlotparcel where _RecordDeleted = 0 and _RecordCurrent = 1 ) \
                                                where recNum = 1) a on a.propertyNumber = b.parentArchitecturalObjectNumber, \
                                          {ADS_DATABASE_CLEANSED}.isu_0funct_loc_attr c, \
                                          {ADS_DATABASE_CLEANSED}.isu_0funct_loc_attr c1 \
                                     where b.architecturalObjectNumber = c.functionalLocationNumber \
                                     and b.parentArchitecturalObjectNumber = c1.functionalLocationNumber \
                                     and b._RecordDeleted = 0 \
                                     and b._RecordCurrent = 1 \
                                     and c._RecordDeleted = 0 \
                                     and c._RecordCurrent = 1 \
                                     and d._RecordDeleted = 0 \
                                     and d._RecordCurrent = 1 \
                               ")

    
    ISULocationDf.createOrReplaceTempView('allLocations')
    
    missingProps = spark.sql(f"select propertyNumber \
                               from   {ADS_DATABASE_CLEANSED}.access_Z309_TPropertyAddress \
                               where  _RecordCurrent = 1 \
                               minus \
                               select locationID \
                               from   allLocations")
    
    missingProps.createOrReplaceTempView('missingProps')
    
    #For the purposes of linking to Hydra only master strata, super lots and link lots need be considered
    parentDf = spark.sql(f"with t1 as( \
                                select su.propertyNumber, \
                                       ms.masterPropertyNumber as parentPropertyNumber, \
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
                                        rp.relationshipType as relationshipType, \
                                        rank() over (partition by rp.propertyNumber order by relationshipTypecode desc) as rnk \
                                from {ADS_DATABASE_CLEANSED}.access_z309_trelatedProps rp inner join remainingProps rem on rp.propertyNumber = rem.propertyNumber \
                                       left outer join {ADS_DATABASE_CLEANSED}.access_z309_tproperty pr on pr.propertynumber = rp.relatedPropertynumber \
                                where rp.relationshipTypeCode in ('P','U') \
                                and   rp._RecordCurrent = 1), \
                              t3 as(select * from t1 \
                                    union \
                                    select propertyNumber, \
                                           parentPropertyNumber, \
                                           relationshipType \
                                    from relatedprops \
                                    where rnk = 1), \
                              t4 as(select propertyNumber \
                                    from {ADS_DATABASE_CLEANSED}.access_z309_tproperty \
                                    where _RecordCurrent = 1 \
                                    minus \
                                    select propertyNumber from t3) \
                            select * from t3 \
                            union all \
                            select pr.propertyNumber, \
                                    pr.propertyNumber as parentPropertyNumber, \
                                    'Self as Parent' as relationshipType \
                            from {ADS_DATABASE_CLEANSED}.access_z309_tproperty pr, t4 \
                            where pr.propertyNumber = t4.propertyNumber \
                            and pr._RecordCurrent = 1 \
                            ")
    parentDf.createOrReplaceTempView('parents')
    
    ACCESSDf = spark.sql(f"select pa.propertyNumber as locationID, \
                               'ACCESS' as sourceSystemCode, \
                               trim(trim(case when pa.lotNumber is not null then 'LOT '||trim(pa.lotNumber) \
                                              when pa.roadsideMailBox is not null then 'RMB '||trim(pa.roadsideMailBox) \
                                              else trim(trim(trim(coalesce(pa.floorLevelType,'')||' '||coalesce(pa.floorLevelNumber,''))||' '||coalesce(pa.flatUnitType,'')||' '||coalesce(pa.flatUnitNumber,''))||' '|| \
                                                   case when pa.houseNumber2 > 0 then trim(cast(pa.houseNumber1 as string)||coalesce(pa.houseNumber1Suffix,''))||'-'||cast(pa.houseNumber2 as string)||coalesce(pa.houseNumber2Suffix,'') \
                                                        else ltrim('0',cast(pa.houseNumber1 as string))||coalesce(pa.houseNumber1Suffix,'') end) end)|| \
                                    ' '||trim(trim(sg.streetName||' '||coalesce(sg.streetType,''))||' '||coalesce(sg.streetSuffix,''))||', '||sg.suburb||' NSW '||sg.postcode) as formattedAddress, \
                               case when pa.lotNumber is not null then 'LOT '||trim(pa.lotNumber) \
                                    when pa.roadsideMailBox is not null then 'RMB '||trim(pa.roadsideMailBox) \
                                    else trim(trim(coalesce(pa.floorLevelType,'')||' '||coalesce(pa.floorLevelNumber,''))||' '||coalesce(pa.flatUnitType,'')||' '||coalesce(pa.flatUnitNumber,'')) end as houseNumber2, \
                               case when pa.houseNumber2 > 0 then trim(cast(pa.houseNumber1 as string)||coalesce(pa.houseNumber1Suffix,''))||'-'||cast(pa.houseNumber2 as string)||coalesce(pa.houseNumber2Suffix,'') \
                                    else ltrim('0',cast(pa.houseNumber1 as string))||coalesce(pa.houseNumber1Suffix,'') end as housenumber1, \
                               sg.streetName as streetName, \
                           trim(coalesce(sg.streetType,'')||' '||coalesce(sg.streetSuffix,'')) as streetType, \
                           coalesce(hy.LGA,pr.LGA) as LGA, \
                           sg.suburb as suburb, \
                           'NSW' as state, \
                           sg.postcode as postcode, \
                           latitude, \
                           longitude \
                           from {ADS_DATABASE_CLEANSED}.access_z309_tpropertyaddress pa left outer join \
                                 {ADS_DATABASE_CLEANSED}.access_z309_tstreetguide sg on pa.streetGuideCode = sg.streetGuideCode, \
                                 parents pp, \
                                 {ADS_DATABASE_CLEANSED}.access_z309_tproperty pr, \
                                 missingProps mp left outer join \
                                 (select propertyNumber, lga, latitude, longitude from \
                                         (select propertyNumber, lga, latitude, longitude, \
                                                 row_number() over (partition by propertyNumber order by areaSize desc) recNum \
                                          from cleansed.hydra_tlotparcel where _RecordDeleted = 0 and _RecordCurrent = 1 ) \
                                          where recNum = 1) hy on hy.propertyNumber = pp.parentPropertyNumber\
                            where pa.propertyNumber = pp.propertyNumber \
                            and   pa.propertyNumber = pr.propertyNumber \
                            and   pa.propertyNumber = mp.propertyNumber \
                            and   pa._RecordCurrent = 1 \
                            and   sg._RecordCurrent = 1 \
                        ")
    ACCESSDf.createOrReplaceTempView('ACCESS')

    #2.JOIN TABLES  

    #3.UNION TABLES
    #Create dummy record
#     dummyRec = tuple([-1] + ['Unknown'] * (len(ISULocationDf.columns) - 3) + [0,0]) 
    #dummyDimRecDf = spark.createDataFrame([dummyRec],ISULocationDf.columns)
    dummyDimRecDf = spark.createDataFrame([("-1","Unknown","Unknown")], [ "locationID","formattedAddress","LGA"])
    ISULocationDf = ISULocationDf.unionByName(dummyDimRecDf, allowMissingColumns = True)
    locationDf = ISULocationDf.unionByName(ACCESSDf, allowMissingColumns = True)

    #4.SELECT / TRANSFORM
    df = locationDf.selectExpr( \
                             "locationID" \
                            ,"sourceSystemCode" \
                            ,"formattedAddress" \
                            ,"houseNumber2" \
                            ,"houseNumber1" \
                            ,"streetName" \
                            ,"streetType" \
                            ,"LGA" \
                            ,"suburb" \
                            ,"state" \
                            ,"postCode"
                            ,"CAST(latitude AS DECIMAL(9,6)) as latitude" \
                            ,"CAST(longitude AS DECIMAL(9,6)) as longitude"                   
                            )
    #5.Apply schema definition
    schema = StructType([
                            StructField('locationSK', StringType(), False),
                            StructField("locationID", StringType(), False),
                            StructField("sourceSystemCode", StringType(), True),
                            StructField("formattedAddress", StringType(), True),
                            StructField("houseNumber2", StringType(), True),
                            StructField("houseNumber1", StringType(), True),
                            StructField("streetName", StringType(), True),
                            StructField("streetType", StringType(), True),
                            StructField("LGA", StringType(), True),
                            StructField("suburb", StringType(), True),
                            StructField("state", StringType(), True),
                            StructField("postCode", StringType(), True),
                            StructField("latitude", DecimalType(9,6), True),
                            StructField("longitude", DecimalType(9,6), True)
                      ])

    return df, schema

# COMMAND ----------

df, schema = getLocation()
TemplateEtl(df, entity="dimLocation", businessKey="locationId", schema=schema, writeMode=ADS_WRITE_MODE_MERGE, AddSK=True)

# COMMAND ----------

dbutils.notebook.exit("1")

# COMMAND ----------


