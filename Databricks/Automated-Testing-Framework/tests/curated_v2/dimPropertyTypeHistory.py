# Databricks notebook source
# MAGIC %run ../../atf-common

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
target_df = spark.sql("select * from curated_v2.dimPropertyTypeHistory")
target_df.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated_v2.dimPropertyTypeHistory

# COMMAND ----------

source_isu = spark.sql("""
select
'ISU' as sourceSystemCode
,planTypeCode 
, planType
, planNumber 
, lotTypeCode
, lotType  
, lotNumber  
, sectionNumber  
, propertyNumber 
,_RecordCurrent
,_recordDeleted

from cleansed.isu_0uc_connobj_attr_2
where propertyNumber <> ''

""")
source_isu.createOrReplaceTempView("source_view")
#display(source_isu)
#source_isu.count()

src_a = spark.sql("Select * except(_RecordCurrent,_recordDeleted) from source_view where _RecordCurrent=1 and _recordDeleted=0 ")
src_d = spark.sql("Select * except(_RecordCurrent,_recordDeleted) from source_view where _RecordCurrent=0 and _recordDeleted=1 ")
src_a.createOrReplaceTempView("src_a")
src_d.createOrReplaceTempView("src_d")


# COMMAND ----------

# DBTITLE 1,Source with mapping for active and deleted records
source_isu = spark.sql("""select
'ISU' as sourceSystemCode
,propertyNumber
,superiorPropertyTypeCode
,superiorPropertyType
,inferiorPropertyTypeCode
,inferiorPropertyType
,ValidFromDate
,ValidToDate
,ValidFromDate as _RecordStart
,ValidToDate as _RecordEnd
,_RecordCurrent
,_recordDeleted
from
cleansed.isu_zcd_tpropty_hist
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
keyColumns = 'propertyNumber,validFromDate'
mandatoryColumns = 'propertyNumber,superiorPropertyTypeCode,superiorPropertyType,inferiorPropertyTypeCode,inferiorPropertyType,ValidFromDate,ValidToDate'

columns = ("""
propertyNumber
,superiorPropertyTypeCode
,superiorPropertyType
,inferiorPropertyTypeCode
,inferiorPropertyType
,ValidFromDate
,ValidToDate

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

# DBTITLE 1,Date validation for valid(To/From)Date
# MAGIC %sql
# MAGIC SELECT * FROM curated_v2.dimPropertyTypeHistory
# MAGIC WHERE validFromDate > validToDate

# COMMAND ----------

# DBTITLE 1,Date validation for valid(To/From)Date = _record(Start/End)
# MAGIC %sql
# MAGIC SELECT * FROM curated_v2.dimPropertyTypeHistory
# MAGIC WHERE (date(_recordStart) <> ValidFromDate)
# MAGIC OR (date(_recordEnd)<>ValidToDate)

# COMMAND ----------

# DBTITLE 1,Check 'Code' columns have matching 'Description' Columns - superiorPropertyType
# MAGIC %sql
# MAGIC select distinct superiorPropertyTypeCode, superiorPropertyType 
# MAGIC from curated_v2.dimPropertyTypeHistory

# COMMAND ----------

# DBTITLE 1,Check 'Code' columns have matching 'Description' Columns - inferiorPropertyType
# MAGIC %sql
# MAGIC select distinct inferiorPropertyTypeCode, inferiorPropertyType 
# MAGIC from curated_v2.dimPropertyTypeHistory

# COMMAND ----------

# DBTITLE 1,Check dummy record is populated
# MAGIC %sql
# MAGIC select * from curated_v2.dimPropertyTypeHistory where sourceSystemCode is null

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM curated_v2.dimPropertyTypeHistory where propertyNumber=-1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM curated_v2.dimPropertyTypeHistory
# MAGIC                 WHERE date(_recordStart) > date(_recordEnd)

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from curated_v2.dimPropertyTypeHistory WHERE (propertyTypeHistorySK is NULL or propertyTypeHistorySK in ('',' ') or UPPER(propertyTypeHistorySK)='NULL')
# MAGIC     
# MAGIC     
# MAGIC             --sqlQuery = sqlQuery + f"WHERE ({column} is NULL or {column} in ('',' ') or UPPER({column})='NULL') "
# MAGIC       
# MAGIC            -- sqlQuery = sqlQuery + f"OR ({column} is NULL or {column} in ('',' ') or UPPER({column})='NULL') "

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT propertyTypeHistorySK,COUNT (*) as recCount FROM curated_v2.dimPropertyTypeHistory GROUP BY propertyTypeHistorySK HAVING COUNT (*) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated_v2.dimPropertyTypeHistory where sourceSystemCode='ACCESS'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated_v2.dimPropertyTypeHistory where propertyTypeHistorySK='a794140b755a5de787aee3ad5201cf8c'

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) as recCount from curated_v2.dimPropertyTypeHistory
# MAGIC                 WHERE _RecordCurrent=0 and _RecordDeleted=0

# COMMAND ----------

# MAGIC %sql
# MAGIC select  distinct sourceSystemCode from curated_v2.dimPropertyTypeHistory

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated_v2.dimPropertyTypeHistory where sourceSystemCode is null

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
