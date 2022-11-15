# Databricks notebook source
# MAGIC %run ../../atf-common

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
target_df = spark.sql("select * from curated_v2.dimLocation")
target_df.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated_v2.dimLocation

# COMMAND ----------

# DBTITLE 1,Source with mapping for active and deleted records
source_isu = spark.sql("""
select distinct d.propertyNumber as locationID, 
                                     --'ISU' as sourceSystemCode, 
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
                                     coalesce(a.LGA,d.LGA) as LGA, 
                                     d.politicalRegionCode as LGACode, 
                                     a.latitude, 
                                     a.longitude,
                                     c._recordCurrent,
                                     c._recordDeleted
                                     from cleansed.isu_0uc_connobj_attr_2 d left outer join cleansed.isu_vibdnode b on d.propertyNumber = b.architecturalObjectNumber 
                                         left outer join (select propertyNumber, lga, latitude, longitude from 
                                              (select propertyNumber, lga, latitude, longitude, 
                                                row_number() over (partition by propertyNumber order by areaSize desc,latitude,longitude) recNum 
                                                from cleansed.hydra_tlotparcel where _RecordDeleted = 0 and _RecordCurrent = 1 ) 
                                                where recNum = 1) a on a.propertyNumber = b.architecturalObjectNumber, 
                                          cleansed.isu_0funct_loc_attr c 
                                     where b.architecturalObjectNumber = c.functionalLocationNumber 
                                     and b.parentArchitecturalObjectNumber is null 
                                     
                                     union all 
                                     select distinct d.propertyNumber as locationID, 
                                    -- 'ISU' as sourceSystemCode, 
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
                                     coalesce(a.LGA,d.LGA) as LGA, 
                                     d.politicalRegionCode as LGACode, 
                                     a.latitude, 
                                     a.longitude,
                                     c._recordCurrent,
                                     c._recordDeleted
                                     from cleansed.isu_0uc_connobj_attr_2 d left outer join cleansed.isu_vibdnode b on d.propertyNumber = b.architecturalObjectNumber 
                                         left outer join (select propertyNumber, lga, latitude, longitude from 
                                              (select propertyNumber, lga, latitude, longitude, 
                                                row_number() over (partition by propertyNumber order by areaSize desc,latitude,longitude) recNum 
                                                from cleansed.hydra_tlotparcel where _RecordDeleted = 0 and _RecordCurrent = 1 ) 
                                                where recNum = 1) a on a.propertyNumber = b.parentArchitecturalObjectNumber, 
                                          cleansed.isu_0funct_loc_attr c, 
                                          cleansed.isu_0funct_loc_attr c1 
                                     where b.architecturalObjectNumber = c.functionalLocationNumber 
                                     and b.parentArchitecturalObjectNumber = c1.functionalLocationNumber 
                                     


""")
source_isu.createOrReplaceTempView("source_view")
#display(source_isu)
#source_isu.count()

src_a = spark.sql("Select * except(_RecordCurrent,_recordDeleted) from source_view where _RecordCurrent=1 and _recordDeleted=0 ")
src_d = spark.sql("Select * except(_RecordCurrent,_recordDeleted) from source_view where _RecordCurrent=0 and _recordDeleted=1 ")
src_a.createOrReplaceTempView("src_a")
src_d.createOrReplaceTempView("src_d")


# COMMAND ----------

# DBTITLE 1,Define Variables for ATF
keyColumns = 'locationId'
mandatoryColumns = 'locationId'

columns = ("""
locationId
,formattedAddress
,addressNumber
,buildingName1 
,buildingName2
,unitDetails
,floorNumber
,houseNumber
,lotDetails
,streetName
,streetLine1
,streetLine2
,suburb
,streetCode
,cityCode
,postCode
,stateCode
,LGACode
,LGA
,latitude
,longitude 

""")

source_a = spark.sql(f"""
Select {columns}
From src_a
""")

source_d = spark.sql(f"""
Select {columns}
From src_d
""")

# COMMAND ----------

#ALWAYS RUN THIS AT THE END
RunTests()

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) as recCount from curated_v2.dimLocation
# MAGIC                 WHERE _RecordCurrent=0 and _RecordDeleted=1

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) as recCount from curated_v2.dimLocation
# MAGIC                 WHERE _RecordCurrent=0 and _RecordDeleted=0

# COMMAND ----------

target_isu= spark.sql("""
select
locationId
,formattedAddress
,addressNumber
,buildingName1 
,buildingName2
,unitDetails
,floorNumber
,houseNumber
,lotDetails
,streetName
,streetLine1
,streetLine2
,suburb
,streetCode
,cityCode
,postCode
,stateCode

,LGA
,LGACode
,latitude
,longitude
,_RecordCurrent
,_RecordDeleted
from
curated_v2.dimLocation
                WHERE sourceSystemCode='ISU'""")

# COMMAND ----------

target_isu.createOrReplaceTempView("target_view")
target_a = spark.sql("Select * except(_RecordCurrent,_recordDeleted) from target_view where _RecordCurrent=1 and _recordDeleted=0 ")
target_d = spark.sql("Select * except(_RecordCurrent,_recordDeleted) from target_view where _RecordCurrent=0 and _recordDeleted=1 ")
target_a.createOrReplaceTempView("target_a")
target_d.createOrReplaceTempView("target_d")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from target_a

# COMMAND ----------

src_act=spark.sql("select * from src_a")
tgt_act=spark.sql("select * from target_a ") 
print("Source Count:",src_act.count())
print("Target Count:",tgt_act.count())
diff1=src_act.subtract(tgt_act)
diff2=tgt_act.subtract(src_act)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated_v2.dimLocation where locationId=-1

# COMMAND ----------

# DBTITLE 1,Source
# MAGIC %sql
# MAGIC select
# MAGIC locationId
# MAGIC ,formattedAddress
# MAGIC ,addressNumber
# MAGIC ,buildingName1 
# MAGIC ,buildingName2
# MAGIC ,unitDetails
# MAGIC ,floorNumber
# MAGIC ,houseNumber
# MAGIC ,lotDetails
# MAGIC ,streetName
# MAGIC ,streetLine1
# MAGIC ,streetLine2
# MAGIC ,suburb
# MAGIC ,streetCode
# MAGIC ,cityCode
# MAGIC ,postCode
# MAGIC ,stateCode
# MAGIC ,LGACode
# MAGIC ,LGA
# MAGIC ,latitude
# MAGIC ,longitude
# MAGIC 
# MAGIC from
# MAGIC src_a
# MAGIC                 WHERE  locationId='5442605'

# COMMAND ----------

# DBTITLE 1,Target
# MAGIC %sql
# MAGIC select
# MAGIC locationId
# MAGIC ,formattedAddress
# MAGIC ,addressNumber
# MAGIC ,buildingName1 
# MAGIC ,buildingName2
# MAGIC ,unitDetails
# MAGIC ,floorNumber
# MAGIC ,houseNumber
# MAGIC ,lotDetails
# MAGIC ,streetName
# MAGIC ,streetLine1
# MAGIC ,streetLine2
# MAGIC ,suburb
# MAGIC ,streetCode
# MAGIC ,cityCode
# MAGIC ,postCode
# MAGIC ,stateCode
# MAGIC ,LGACode
# MAGIC ,LGA
# MAGIC ,latitude
# MAGIC ,longitude
# MAGIC from
# MAGIC curated_v2.dimLocation
# MAGIC                 WHERE sourceSystemCode='ISU' and locationId='54426053380675'

# COMMAND ----------

# DBTITLE 1,S vs T
# MAGIC %sql
# MAGIC select
# MAGIC locationId
# MAGIC ,formattedAddress
# MAGIC ,addressNumber
# MAGIC ,buildingName1 
# MAGIC ,buildingName2
# MAGIC ,unitDetails
# MAGIC ,floorNumber
# MAGIC ,houseNumber
# MAGIC ,lotDetails
# MAGIC ,streetName
# MAGIC ,streetLine1
# MAGIC ,streetLine2
# MAGIC ,suburb
# MAGIC ,streetCode
# MAGIC ,cityCode
# MAGIC ,postCode
# MAGIC ,stateCode
# MAGIC ,LGACode
# MAGIC ,LGA
# MAGIC ,latitude
# MAGIC ,longitude
# MAGIC 
# MAGIC from
# MAGIC src_a
# MAGIC except
# MAGIC select
# MAGIC locationId
# MAGIC ,formattedAddress
# MAGIC ,addressNumber
# MAGIC ,buildingName1 
# MAGIC ,buildingName2
# MAGIC ,unitDetails
# MAGIC ,floorNumber
# MAGIC ,houseNumber
# MAGIC ,lotDetails
# MAGIC ,streetName
# MAGIC ,streetLine1
# MAGIC ,streetLine2
# MAGIC ,suburb
# MAGIC ,streetCode
# MAGIC ,cityCode
# MAGIC ,postCode
# MAGIC ,stateCode
# MAGIC ,LGACode
# MAGIC ,LGA
# MAGIC ,latitude
# MAGIC ,longitude
# MAGIC 
# MAGIC from
# MAGIC curated_v2.dimLocation
# MAGIC                 WHERE sourceSystemCode='ISU'and _RecordCurrent=1 and _recordDeleted=0

# COMMAND ----------

# DBTITLE 1,T vs S
# MAGIC %sql
# MAGIC select
# MAGIC locationId
# MAGIC ,formattedAddress
# MAGIC ,addressNumber
# MAGIC ,buildingName1 
# MAGIC ,buildingName2
# MAGIC ,unitDetails
# MAGIC ,floorNumber
# MAGIC ,houseNumber
# MAGIC ,lotDetails
# MAGIC ,streetName
# MAGIC ,streetLine1
# MAGIC ,streetLine2
# MAGIC ,suburb
# MAGIC ,streetCode
# MAGIC ,cityCode
# MAGIC ,postCode
# MAGIC ,stateCode
# MAGIC ,LGACode
# MAGIC ,LGA
# MAGIC ,latitude
# MAGIC ,longitude
# MAGIC 
# MAGIC from
# MAGIC curated_v2.dimLocation
# MAGIC                 WHERE sourceSystemCode='ISU' and _RecordCurrent=1 and _recordDeleted=0
# MAGIC                 except
# MAGIC                 select
# MAGIC locationId
# MAGIC ,formattedAddress
# MAGIC ,addressNumber
# MAGIC ,buildingName1 
# MAGIC ,buildingName2
# MAGIC ,unitDetails
# MAGIC ,floorNumber
# MAGIC ,houseNumber
# MAGIC ,lotDetails
# MAGIC ,streetName
# MAGIC ,streetLine1
# MAGIC ,streetLine2
# MAGIC ,suburb
# MAGIC ,streetCode
# MAGIC ,cityCode
# MAGIC ,postCode
# MAGIC ,stateCode
# MAGIC ,LGACode
# MAGIC ,LGA
# MAGIC ,latitude
# MAGIC ,longitude
# MAGIC 
# MAGIC from
# MAGIC src_a

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated_v2.dimLocation where locationId=-1

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) as recCount from curated_v2.dimPropertyLot
# MAGIC                 WHERE _RecordCurrent=0 and _RecordDeleted=0

# COMMAND ----------

# MAGIC %sql
# MAGIC select  distinct sourceSystemCode from curated_v2.dimPropertyLot

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated_v2.dimPropertyService where sourceSystemCode='null'

# COMMAND ----------

df = spark.sql(""" select property1Number, property2Number, validFromDate, relationshipTypeCode1 from curated_v2.dimPropertyRelation
                   where sourceSystemCode is null
               """)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Investigation for failed tests

# COMMAND ----------

# DBTITLE 1,Business Key Null Check
df = spark.sql(""" select property1Number, property2Number, validFromDate, relationshipTypeCode1 from curated_v2.dimPropertyRelation
                   where (property1Number is NULL or property1Number in ('',' ') or UPPER(property1Number)='NULL') 
                   or (property2Number is NULL or property2Number in ('',' ') or UPPER(property2Number)='NULL') 
                   or (validFromDate is NULL or validFromDate in ('',' ') or UPPER(validFromDate)='NULL') 
                   or (relationshipTypeCode1 is NULL or relationshipTypeCode1 in ('',' ') or UPPER(relationshipTypeCode1)='NULL') 
                   or (relationshipTypeCode2 is NULL or relationshipTypeCode2 in ('',' ') or UPPER(relationshipTypeCode2)='NULL') 
               """)

print("Count of records where business columns are NULL/BLANK :", df.count())
display(df)

# COMMAND ----------

activeTgtdf = spark.sql("""select property1Number,property2Number from curated_v2.dimPropertyRelation where 
_RecordCurrent=1 and _RecordDeleted=0 """)
activeSourcedf = spark.sql("""select property1Number,property2Number from cleansed.isu_zcd_tprop_rel where 
_RecordCurrent=1 and _RecordDeleted=0 """)     
diff1 = activeSourcedf.subtract(activeTgtdf) 
diff2 = activeTgtdf.subtract(activeSourcedf)                           
display(diff1) 
display(diff2)


# COMMAND ----------

print("Source-Target count is : ",diff1.count())
display(diff1)
print("Target-Source count is : ",diff2.count())
display(diff2)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from curated_v2.dimPropertyRelation where 
# MAGIC _RecordCurrent=1 and _RecordDeleted=0

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from cleansed.isu_zcd_tprop_rel where _RecordCurrent=1 and _RecordDeleted=0

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from curated_v2.dimPropertyRelation where 
# MAGIC _RecordCurrent=0 and _RecordDeleted=1

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from cleansed.isu_zcd_tprop_rel where _RecordCurrent=0 and _RecordDeleted=1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed.isu_zcd_tprop_rel where property1Number='3104749' and property2Number='5778798' 

# COMMAND ----------

# DBTITLE 1,3723868
# MAGIC %sql
# MAGIC select * from curated_v2.dimPropertyRelation where property1Number='3104749' and property2Number='5778798' 

# COMMAND ----------

# DBTITLE 1,Target
target_df = spark.sql("select * from curated_v2.dimPropertyRelation")
display(target_df)

target = spark.sql("""
select
property1Number 
, property2Number 
, validFromDate 
, validToDate 
, relationshipTypeCode1 
, relationshipType1 
, relationshipTypeCode2 
, relationshipType2 
from curated_v2.dimPropertyRelation
""")
target.createOrReplaceTempView("target_view")

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
target_df.printSchema()

# COMMAND ----------

# DBTITLE 1,[Source] Applying Transformation
source = spark.sql("""
select
property1Number 
, property2Number 
, validFromDate 
, validToDate 
, relationshipTypeCode1 
, relationshipType1 
, relationshipTypeCode2 
, relationshipType2 
,_RecordDeleted
from cleansed.isu_zcd_tprop_rel
""")
source.createOrReplaceTempView("source_view")
display(source)
source.count()

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Check of all records
# MAGIC %sql
# MAGIC select property1Number, property2Number, validFromDate, relationshipTypeCode1, relationshipTypeCode2, COUNT(*) as counts
# MAGIC from curated_v2.dimPropertyRelation
# MAGIC group by property1Number, property2Number, validFromDate, relationshipTypeCode1, relationshipTypeCode2
# MAGIC having COUNT(*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Duplicate Check of active records
# MAGIC %sql
# MAGIC select property1Number, property2Number, validFromDate, relationshipTypeCode1, relationshipTypeCode2, COUNT(*) as counts
# MAGIC from curated_v2.dimPropertyRelation where _RecordCurrent=1 and _recordDeleted=0
# MAGIC group by property1Number, property2Number, validFromDate, relationshipTypeCode1, relationshipTypeCode2
# MAGIC having COUNT(*) > 1

# COMMAND ----------

# DBTITLE 1,[Verification] Count Check
# MAGIC %sql
# MAGIC select 'Target' as TableName, COUNT(*) as RecordCount from  curated_v2.dimPropertyRelation where _RecordCurrent=1 and _recordDeleted=0
# MAGIC union all 
# MAGIC select 'Source' as TableName, COUNT(*) AS RecordCount from source_view where _recordDeleted<>1

# COMMAND ----------

# DBTITLE 1,SK columns validation
sk_chk1=spark.sql(""" Select * from curated_v2.dimPropertyRelation where (propertyRelationSK is NULL or propertyRelationSK in ('',' ') or UPPER(propertyRelationSK)='NULL')""")
sk_chk2=spark.sql("Select propertyRelationSK,count(*) from curated_v2.dimPropertyRelation where _RecordCurrent=1 and _recordDeleted=0 group by propertyRelationSK having count(*) > 1")
sk_chk3=spark.sql("Select propertyRelationSK,count(*) from curated_v2.dimPropertyRelation group by propertyRelationSK having count(*) > 1")

print("Count of records where SK columns are NULL/BLANK :",sk_chk1.count())
print("Duplicate count of SK for active records :",sk_chk2.count())
print("Duplicate count of SK for all records :",sk_chk3.count())

# COMMAND ----------

# DBTITLE 1,Date Validation
d1=spark.sql("""Select * from curated_v2.dimPropertyRelation where date(_recordStart) > date(_recordEnd)""")
print("Count of records of where _recordStart is greater than _recordEnd:", d1.count())

# COMMAND ----------

# DBTITLE 1,S vs T and T vs S
src_act=spark.sql("""select property1Number 
, property2Number 
, validFromDate 
, validToDate 
, relationshipTypeCode1 
, relationshipType1 
, relationshipTypeCode2 
, relationshipType2  from source_view""")
tgt_act=spark.sql("""select property1Number 
, property2Number 
, validFromDate 
, validToDate 
, relationshipTypeCode1 
, relationshipType1 
, relationshipTypeCode2 
, relationshipType2  from curated_v2.dimPropertyRelation where _RecordCurrent=1 and _recordDeleted=0""")
print("Source Count:",src_act.count())
print("Target Count:",tgt_act.count())

diff1=src_act.subtract(tgt_act)
diff2=tgt_act.subtract(src_act)
