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

# MAGIC %md
# MAGIC
# MAGIC Need to Run Property Type History Before Location

# COMMAND ----------

def getLocation():

    #1.Load Cleansed layer table data into dataframe
    #collect parent properties and then parents of child properties so you get the parent address against the child property
    ISULocationDf = spark.sql(f"""select distinct d.propertyNumber as locationId, 
                                     'ISU' as sourceSystemCode, 
                                     upper(trim(trim(trim(trim(trim(coalesce(c.locationDescription,''))||' '||coalesce(c.buildingNumber,''))||' '||(coalesce(c.houseNumber2,'')||' '||(coalesce(c.floorNumber,'')||' '||coalesce(c.houseNumber1,''))||' '||coalesce(c.houseNumber3,''))||' '||trim(coalesce(c.streetName,'')||' '||coalesce(c.streetLine1,''))||' '||coalesce(c.streetLine2,''))||'  '||coalesce(c.cityName,'')||' NSW '||coalesce(c.postCode,''))))  as formattedAddress, 
                                     c.addressNumber as addressNumber, 
                                     upper(c.locationDescription) as buildingName1, 
                                     upper(c.buildingNumber) as buildingName2, 
                                     upper(c.houseNumber2) as unitDetails,
                                     upper(c.floorNumber) as floorNumber, 
                                     upper(c.houseNumber1) as houseNumber, 
                                     upper(c.houseNumber3) as lotDetails,
                                     upper(c.streetName) as streetName, 
                                     upper(trim(coalesce(c.streetLine1,''))) as streetLine1, 
                                     upper(trim(coalesce(c.streetLine2,''))) as streetLine2, 
                                     upper(c.cityName) as suburb, 
                                     d.streetCode as streetCode, 
                                     d.cityCode as cityCode, 
                                     c.postcode as postcode, 
                                     c.stateCode as stateCode, 
                                     --coalesce(a.LGA,d.LGA) as LGA, 
                                     --d.politicalRegionCode as LGACode, This info must come from Hydra
                                     a.LGA as LGA,
                                     --a.latitude, 
                                     --a.longitude,
                                     d._RecordDeleted 
                                     from {ADS_DATABASE_CLEANSED}.isu_0uc_connobj_attr_2 d left outer join {ADS_DATABASE_CLEANSED}.isu_vibdnode b on d.propertyNumber = b.architecturalObjectNumber 
                                         left outer join (select propertyNumber, lga, latitude, longitude from 
                                              (select propertyNumber, lga, latitude, longitude, 
                                                row_number() over (partition by propertyNumber order by areaSize desc,latitude,longitude,lga) recNum 
                                                from cleansed.hydra_tlotparcel where  _RecordCurrent = 1 ) 
                                                where recNum = 1) a on a.propertyNumber = b.architecturalObjectNumber, 
                                          {ADS_DATABASE_CLEANSED}.isu_0funct_loc_attr c 
                                     where b.architecturalObjectNumber = c.functionalLocationNumber 
                                     and b.parentArchitecturalObjectNumber is null 
                                     and b._RecordCurrent = 1 
                                     and c._RecordCurrent = 1 
                                     and d._RecordCurrent = 1 
                                     union all 
                                     select distinct d.propertyNumber as locationId, 
                                     'ISU' as sourceSystemCode, 
                                     upper(trim(trim(trim(trim(trim(coalesce(c.locationDescription,''))||' '||coalesce(c.buildingNumber,''))||' '||(coalesce(c.houseNumber2,'')||' '||(coalesce(c.floorNumber,'')||' '||coalesce(c.houseNumber1,''))||' '||coalesce(c.houseNumber3,''))||' '||trim(coalesce(c.streetName,'')||' '||coalesce(c.streetLine1,''))||' '||coalesce(c.streetLine2,''))||'  '||coalesce(c.cityName,'')||' NSW '||coalesce(c.postCode,''))))  as formattedAddress, 
                                     c.addressNumber as addressNumber, 
                                     upper(c.locationDescription) as buildingName1, 
                                     upper(c.buildingNumber) as buildingName2, 
                                     upper(c.houseNumber2) as unitDetails,
                                     upper(c.floorNumber) as floorNumber, 
                                     upper(c.houseNumber1) as houseNumber, 
                                     upper(c.houseNumber3) as lotDetails,
                                     upper(c.streetName) as streetName, 
                                     upper(trim(coalesce(c.streetLine1,''))) as streetLine1, 
                                     upper(trim(coalesce(c.streetLine2,''))) as streetLine2, 
                                     upper(c.cityName) as suburb, 
                                     d.streetCode as streetCode, 
                                     d.cityCode as cityCode, 
                                     c.postcode as postcode, 
                                     c.stateCode as stateCode, 
                                     --coalesce(a.LGA,d.LGA) as LGA, 
                                     --d.politicalRegionCode as LGACode, 
                                     a.LGA AS LGA,
                                     --a.latitude, 
                                     --a.longitude,
                                     d._RecordDeleted 
                                     from {ADS_DATABASE_CLEANSED}.isu_0uc_connobj_attr_2 d left outer join {ADS_DATABASE_CLEANSED}.isu_vibdnode b on d.propertyNumber = b.architecturalObjectNumber 
                                         left outer join (select propertyNumber, lga, latitude, longitude from 
                                              (select propertyNumber, lga, latitude, longitude, 
                                                row_number() over (partition by propertyNumber order by areaSize desc,latitude,longitude,lga) recNum 
                                                from {ADS_DATABASE_CLEANSED}.hydra_tlotparcel where _RecordCurrent = 1 ) 
                                                where recNum = 1) a on a.propertyNumber = b.parentArchitecturalObjectNumber, 
                                          {ADS_DATABASE_CLEANSED}.isu_0funct_loc_attr c, 
                                          {ADS_DATABASE_CLEANSED}.isu_0funct_loc_attr c1 
                                     where b.architecturalObjectNumber = c.functionalLocationNumber 
                                     and b.parentArchitecturalObjectNumber = c1.functionalLocationNumber 
                                     and b._RecordCurrent = 1 
                                     and c._RecordCurrent = 1 
                                     and d._RecordCurrent = 1 
                               """)

    
    ISULocationDf.createOrReplaceTempView('allLocations')
    
    missingProps = spark.sql(f"""select propertyNumber 
                               from   {ADS_DATABASE_CLEANSED}.access_Z309_TPropertyAddress 
                               where  _RecordCurrent = 1 
                               minus 
                               select locationId 
                               from   allLocations""")
    
    missingProps.createOrReplaceTempView('missingProps')
    
    #For the purposes of linking to Hydra only master strata, super lots and link lots need be considered
    parentDf = spark.sql(f"""with t1 as( 
                                select su.propertyNumber, 
                                       ms.masterPropertyNumber as parentPropertyNumber, 
                                       'Child of Master Strata' as relationshipType 
                                from {ADS_DATABASE_CLEANSED}.access_z309_tstrataunits su 
                                      inner join {ADS_DATABASE_CLEANSED}.access_z309_tmastrataplan ms on su.strataPlanNumber = ms.strataPlanNumber and ms._RecordCurrent = 1 
                                      left outer join {ADS_DATABASE_CLEANSED}.access_z309_tproperty pr on pr.propertynumber = ms.masterPropertynumber and pr._RecordCurrent = 1 
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
                                        rp.relationshipType as relationshipType, 
                                        rank() over (partition by rp.propertyNumber order by relationshipTypecode desc) as rnk 
                                from {ADS_DATABASE_CLEANSED}.access_z309_trelatedProps rp inner join remainingProps rem on rp.propertyNumber = rem.propertyNumber 
                                       left outer join {ADS_DATABASE_CLEANSED}.access_z309_tproperty pr on pr.propertynumber = rp.relatedPropertynumber 
                                where rp.relationshipTypeCode in ('P','U') 
                                and   rp._RecordCurrent = 1), 
                              t3 as(select * from t1 
                                    union 
                                    select propertyNumber, 
                                           parentPropertyNumber, 
                                           relationshipType 
                                    from relatedprops 
                                    where rnk = 1), 
                              t4 as(select propertyNumber 
                                    from {ADS_DATABASE_CLEANSED}.access_z309_tproperty 
                                    where _RecordCurrent = 1 
                                    minus 
                                    select propertyNumber from t3) 
                            select * from t3 
                            union all 
                            select pr.propertyNumber, 
                                    pr.propertyNumber as parentPropertyNumber, 
                                    'Self as Parent' as relationshipType 
                            from {ADS_DATABASE_CLEANSED}.access_z309_tproperty pr, t4 
                            where pr.propertyNumber = t4.propertyNumber 
                            and pr._RecordCurrent = 1 
                            """)
    parentDf.createOrReplaceTempView('parents')
    
    ACCESSDf = spark.sql(f"""select pa.propertyNumber as locationId, 
                               'ACCESS' as sourceSystemCode, 
                               trim(trim(coalesce(pa.buildingName1,'')||' '||coalesce(pa.buildingName2,''))||
                                (case when pa.lotNumber is not null then ' LOT '||trim(pa.lotNumber) 
                                    when pa.roadsideMailBox is not null then ' RMB '||trim(pa.roadsideMailBox)
                                    else '' end)||
                                (case when pa.floorLevelType is not null then ' ' || trim(coalesce(pa.floorLevelType,'')||' '||coalesce(pa.floorLevelNumber,''))
                                      else '' end)||
                                (case when pa.flatUnitType is not null then ' ' || trim(coalesce(pa.flatUnitType,'')||' '||coalesce(pa.flatUnitNumber,''))
                                      else '' end)||
                                (case when pa.houseNumber1 > 0 and pa.houseNumber2 > 0  then ' ' || trim(cast(pa.houseNumber1 as string)||coalesce(pa.houseNumber1Suffix,''))||'-'||cast(pa.houseNumber2 as string)||coalesce(pa.houseNumber2Suffix,'') 
                                      when pa.houseNumber1 > 0 and pa.houseNumber2 <= 0 then ' ' || trim(cast(pa.houseNumber1 as string)||coalesce(pa.houseNumber1Suffix,'')) 
                                      when pa.houseNumber1 <= 0 and pa.houseNumber2 > 0 then ' ' || trim(cast(pa.houseNumber2 as string)||coalesce(pa.houseNumber2Suffix,'')) 
                                      else '' end)||
                                (' '|| trim(trim(sg.streetName||' '||coalesce(sg.streetType,''))||' '||coalesce(sg.streetSuffix,''))||', '||sg.suburb||' NSW '||sg.postcode)) 
                             as formattedAddress, 
                             null as addressNumber, 
                             pa.buildingName1 as buildingName1,
                             pa.buildingName2 as buildingName2,
                             (case when pa.flatUnitType is not null then ' ' || trim(coalesce(pa.flatUnitType,'')||' '||coalesce(pa.flatUnitNumber,''))
                                      else '' end) as unitDetails,
                             (case when pa.floorLevelType is not null then ' ' || trim(coalesce(pa.floorLevelType,'')||' '||coalesce(pa.floorLevelNumber,''))
                                      else '' end) as floorNumber,
                             (case when pa.houseNumber1 > 0 and pa.houseNumber2 > 0  then ' ' || trim(cast(pa.houseNumber1 as string)||coalesce(pa.houseNumber1Suffix,''))||'-'||cast(pa.houseNumber2 as string)||coalesce(pa.houseNumber2Suffix,'') 
                                      when pa.houseNumber1 > 0 and pa.houseNumber2 <= 0 then ' ' || trim(cast(pa.houseNumber1 as string)||coalesce(pa.houseNumber1Suffix,'')) 
                                      when pa.houseNumber1 <= 0 and pa.houseNumber2 > 0 then ' ' || trim(cast(pa.houseNumber2 as string)||coalesce(pa.houseNumber2Suffix,'')) 
                                      else '' end) as houseNumber,
                             (case when pa.lotNumber is not null then ' LOT '||trim(pa.lotNumber) 
                                    when pa.roadsideMailBox is not null then ' RMB '||trim(pa.roadsideMailBox)
                                    else '' end) as lotDetails,
                               sg.streetName as streetName, 
                           coalesce(sg.streetType,'') as streetLine1, 
                           coalesce(sg.streetSuffix,'') as streetLine2, 
                           sg.suburb as suburb, 
                           sg.streetGuideCode as streetCode, 
                           sg.suburb as cityCode, 
                           sg.postcode as postCode, 
                           'NSW' as stateCode, 
                           --sg.LGACode as LGACode, 
                           --coalesce(hy.LGA,pr.LGA) as LGA, 
                           hy.LGA AS LGA,
                           --latitude, 
                           --longitude,
                           pa._RecordDeleted 
                           from {ADS_DATABASE_CLEANSED}.access_z309_tpropertyaddress pa left outer join 
                                 {ADS_DATABASE_CLEANSED}.access_z309_tstreetguide sg on pa.streetGuideCode = sg.streetGuideCode, 
                                 parents pp, 
                                 {ADS_DATABASE_CLEANSED}.access_z309_tproperty pr, 
                                 missingProps mp left outer join 
                                 (select propertyNumber, lga, latitude, longitude from 
                                         (select propertyNumber, lga, latitude, longitude, 
                                                 row_number() over (partition by propertyNumber order by areaSize desc,latitude,longitude) recNum 
                                          from {ADS_DATABASE_CLEANSED}.hydra_tlotparcel where  _RecordCurrent = 1 ) 
                                          where recNum = 1) hy on hy.propertyNumber = pp.parentPropertyNumber
                            where pa.propertyNumber = pp.propertyNumber 
                            and   pa.propertyNumber = pr.propertyNumber 
                            and   pa.propertyNumber = mp.propertyNumber 
                            and   pa._RecordCurrent = 1 
                            and   sg._RecordCurrent = 1 
                        """)
    ACCESSDf.createOrReplaceTempView('ACCESS')

    #2.JOIN TABLES  

    #3.UNION TABLES
    #Create dummy record
    #dummyRec = tuple([-1] + ['Unknown'] * (len(ISULocationDf.columns) - 3) + [0,0]) 
    #dummyDimRecDf = spark.createDataFrame([dummyRec],ISULocationDf.columns)
    dummyDimRecDf = spark.createDataFrame(["-1"], "string").toDF("locationId")
    
    ISULocationDf = ISULocationDf.unionByName(dummyDimRecDf, allowMissingColumns = True)
#     locationDf = ISULocationDf.unionByName(ACCESSDf, allowMissingColumns = True)
    locationDf = ISULocationDf

    #4.SELECT / TRANSFORM
    df = locationDf.selectExpr( \
                             "locationId" \
                            ,"sourceSystemCode" \
                            ,"formattedAddress" \
                            ,"addressNumber" \
                            ,"buildingName1" \
                            ,"buildingName2" \
                            ,"unitDetails" \
                            ,"floorNumber" \
                            ,"houseNumber" \
                            ,"lotDetails" \
                            ,"streetName" \
                            ,"streetLine1" \
                            ,"streetLine2" \
                            ,"suburb" \
                            ,"streetCode" \
                            ,"cityCode" \
                            ,"postCode" \
                            ,"stateCode" \
                            #,"LGACode"\
                            ,"LGA" \
#                             ,"CAST(latitude AS DECIMAL(9,6)) as latitude" \
#                             ,"CAST(longitude AS DECIMAL(9,6)) as longitude" \
                            ,"_RecordDeleted" 
                            )
    #5.Apply schema definition
    schema = StructType([
                            StructField('locationSK', StringType(), False),
                            StructField("locationId", StringType(), False),
                            StructField("sourceSystemCode", StringType(), True),
                            StructField("formattedAddress", StringType(), True),
                            StructField("addressNumber", StringType(), True),
                            StructField("buildingName1", StringType(), True),
                            StructField("buildingName2", StringType(), True),
                            StructField("unitDetails", StringType(), True),
                            StructField("floorNumber", StringType(), True),
                            StructField("houseNumber", StringType(), True),
                            StructField("lotDetails", StringType(), True),
                            StructField("streetName", StringType(), True),
                            StructField("streetLine1", StringType(), True),
                            StructField("streetLine2", StringType(), True),
                            StructField("suburb", StringType(), True),
                            StructField("streetCode", StringType(), True),
                            StructField("cityCode", StringType(), True),
                            StructField("postCode", StringType(), True),
                            StructField("stateCode", StringType(), True),
                            #StructField("LGACode", StringType(), True),
                            StructField("LGA", StringType(), True)
#                             StructField("latitude", DecimalType(9,6), True),
#                             StructField("longitude", DecimalType(9,6), True)
                      ])

    return df, schema

# COMMAND ----------

df, schema = getLocation()
#TemplateEtl(df, entity="dimLocation", businessKey="locationId", schema=schema, writeMode=ADS_WRITE_MODE_OVERWRITE, AddSK=True)

curnt_table = f'{ADS_DATABASE_CURATED}.dimLocation'
curnt_pk = 'locationId' 
curnt_recordStart_pk = 'locationId'
history_table = f'{ADS_DATABASE_CURATED}.dimPropertyTypeHistory'
history_table_pk = 'propertyNumber'
history_table_pk_convert = 'propertyNumber as locationId'

df_ = appendRecordStartFromHistoryTable(df,history_table,history_table_pk,curnt_pk,history_table_pk_convert,curnt_recordStart_pk)
updateDBTableWithLatestRecordStart(df_, curnt_table, curnt_pk)

TemplateEtlSCD(df_, entity="dimLocation", businessKey="locationId", schema=schema)

# COMMAND ----------

# MAGIC %md
# MAGIC Set the LGA and lat/lon details for cancelled properties to those of their now active equivalent (to be expanded for SAP cancelled props)

# COMMAND ----------

# MAGIC %sql
# MAGIC with t1 as (select cp.propertyNumber, LGA
# MAGIC             from   curated.dimLocation l,
# MAGIC                    curated.ACCESSCancelledActiveProps cp
# MAGIC             where  l.locationId = cp.activeProperty and l._RecordCurrent = 1)
# MAGIC
# MAGIC merge into curated.dimLocation l
# MAGIC using      t1
# MAGIC on         l.locationId = t1.propertyNumber and l._RecordCurrent = 1
# MAGIC when matched then update 
# MAGIC              set l.LGA = t1.LGA

# COMMAND ----------

#dbutils.notebook.exit("1")
