# Databricks notebook source
# MAGIC %run ../../atf-common

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
lakedftarget = spark.sql("select * from curated.dimStormWaterNetwork")
lakedftarget.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated.dimStormWaterNetwork

# COMMAND ----------

# DBTITLE 1,Source with mapping for active and deleted records
source = spark.sql("""
select
level30 as stormWaterNetwork
, level40 as stormwaterCatchment
,_RecordCurrent
,_RecordDeleted
from
cleansed.hydra_tsystemarea where product = 'StormWater'
""")
source.createOrReplaceTempView("source_view")
#display(source_isu)
#source_isu.count()

src_a = spark.sql("Select * except(_RecordCurrent,_recordDeleted) from source_view where _RecordCurrent=1 and _recordDeleted=0 ")
src_d = spark.sql("Select * except(_RecordCurrent,_recordDeleted) from source_view where _RecordCurrent=0 and _recordDeleted=1 ")
src_a.createOrReplaceTempView("src_a")
src_d.createOrReplaceTempView("src_d")


# COMMAND ----------

keyColumns = 'stormWaterNetworkSK'
mandatoryColumns = 'stormWaterNetwork, stormWaterCatchment'

columns = ("""
stormWaterNetwork, 
stormWaterCatchment
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

src=spark.sql("""
select
level30 as stormWaterNetwork
,level40 as stormwaterCatchment
from
cleansed.hydra_tsystemarea where product = 'StormWater'
and _RecordCurrent not in ('0') and _RecordDeleted not in ('1')
""")
src.createOrReplaceTempView("src")
display(src)
src.count()

# COMMAND ----------

tgt=spark.sql("""
select
stormWaterNetwork
,stormwaterCatchment
from
curated_v2.dimStormWaterNetwork where _recordCurrent = 1 and _recordDeleted = 0
""")
tgt.createOrReplaceTempView("tgt")
display(tgt)
tgt.count()

# COMMAND ----------

# DBTITLE 1,Duplicate Check- Active & All Records
dup_act=spark.sql("Select stormWaterNetworkSK,count(*) from curated_v2.dimStormWaterNetwork where _RecordCurrent=1 and _recordDeleted=0 group by stormWaterNetworkSK having count(*) > 1")
dup_all=spark.sql("Select stormWaterNetworkSK, count(*) from  curated_v2.dimStormWaterNetwork group by stormWaterNetworkSK having count(*) > 1")
dup_act1=spark.sql("Select stormWaterNetworkSK,date(_recordStart) as start_date,count(*) from  curated_v2.dimStormWaterNetwork where _RecordCurrent=1 and _recordDeleted=0 group by 1,2 having count(*) > 1")
dup_all1=spark.sql("Select stormWaterNetworkSK,date(_recordStart) as start_date,count(*) from  curated_v2.dimStormWaterNetwork group by 1,2 having count(*) > 1")
print("Duplicate count of active records :",dup_act.count())
print("Duplicate count of all records :",dup_all.count())
print("Duplicate count of active records with start date :",dup_act1.count())
print("Duplicate count of all records with start date :",dup_all1.count())

# COMMAND ----------

# DBTITLE 1,business columns Validation
BusCol_chk=spark.sql(""" Select * from curated_v2.dimStormWaterNetwork
                         where (stormWaterNetworkSK is NULL or stormWaterNetworkSK in ('',' ') or UPPER(stormWaterNetworkSK)='NULL') 
                         """)
print("Count of records where business columns are NULL/BLANK :",BusCol_chk.count())



BusCol_chk1=spark.sql(""" select distinct length(stormWaterNetworkSK) from curated_v2.dimStormWaterNetwork
                         where length(stormWaterNetworkSK)<>2
                         """)
print("Expected count of distinct length of Business Column is 1, Actual count is :",BusCol_chk1.count())

# COMMAND ----------

# DBTITLE 1,SK columns validation
sk_chk1=spark.sql(""" Select * from curated_v2.dimStormWaterNetwork where (stormWaterNetworkSK is NULL or stormWaterNetworkSK in ('',' ') or UPPER(stormWaterNetworkSK)='NULL')""")
sk_chk2=spark.sql("Select stormWaterNetworkSK,count(*) from curated_v2.dimStormWaterNetwork where _RecordCurrent=1 and _recordDeleted=0 group by stormWaterNetworkSK having count(*) > 1")
sk_chk3=spark.sql("Select stormWaterNetworkSK,count(*) from curated_v2.dimStormWaterNetwork group by stormWaterNetworkSK having count(*) > 1")

print("Count of records where SK columns are NULL/BLANK :",sk_chk1.count())
print("Duplicate count of SK for active records :",sk_chk2.count())
print("Duplicate count of SK for all records :",sk_chk3.count())

# COMMAND ----------

# DBTITLE 1,Date Validation
d1=spark.sql("""Select * from curated_v2.dimStormWaterNetwork where date(_recordStart) > date(_recordEnd)""")
print("Count of records of where _recordStart is greater than _recordEnd:", d1.count())


# COMMAND ----------

# DBTITLE 1,Overlap and Gap Validation
d2=spark.sql("""
Select stormWaterNetworkSK from
(Select stormWaterNetworkSK, date(_recordStart) as start_date,date(_recordEnd) as end_date,
max(date(_recordStart)) over (partition by stormWaterNetworkSK order by _recordStart rows between 1 following and 1 following) as nxt_date 
from curated_v2.dimStormWaterNetwork )
where  DATEDIFF(day,nxt_date,end_date) <> 1
""")

print("Count of records of where overlap and gap is observed:", d2.count())			

# COMMAND ----------

# DBTITLE 1,Exact duplicates in target
# need to check how this work with other duplicate query
tgtColumns = 'stormwaterNetworkSK,stormwaterNetwork,stormwaterCatchment,_BusinessKey,_DLCuratedZoneTimeStamp,_RecordStart,_RecordEnd,_RecordDeleted,_RecordCurrent'

df = spark.sql(f"select {tgtColumns} ,count(*) as recCount from curated_v2.dimStormWaterNetwork  \
                GROUP BY {tgtColumns} HAVING COUNT (*) > 1")
print("Any exact duplicates in the target table")
display(df)

# COMMAND ----------

src_act=spark.sql("select * from src")
tgt_act=spark.sql("select * from tgt ") 
print("Source Count:",src_act.count())
print("Target Count:",tgt_act.count())

diff1=src_act.subtract(tgt_act)
diff2=tgt_act.subtract(src_act)

# COMMAND ----------

print("Source-Target count is : ", diff1.count())
display(diff1)
print("Target-Source count is : ", diff2.count())
display(diff2)

# COMMAND ----------

sk_chk1=spark.sql(""" Select * from curated_v2.dimstormWaterNetwork where (stormwaterNetworkSK is NULL or stormwaterNetworkSK in ('',' ') or UPPER(stormwaterNetworkSK)='NULL')""")
sk_chk2=spark.sql("Select stormwaterNetworkSK,count(*) from curated_v2.dimstormWaterNetwork where _RecordCurrent=1 and _recordDeleted=0 group by stormwaterNetworkSK having count(*) > 1")
sk_chk3=spark.sql("Select stormwaterNetworkSK,count(*) from curated_v2.dimstormWaterNetwork group by stormwaterNetworkSK having count(*) > 1")

print("Count of records where SK columns are NULL/BLANK :",sk_chk1.count())
print("Duplicate count of SK for active records :",sk_chk2.count())
print("Duplicate count of SK for all records :",sk_chk3.count())

# COMMAND ----------

d1=spark.sql("""Select * from curated_v2.dimstormWaterNetwork where date(_recordStart) > date(_recordEnd)""")
print("Count of records of where _recordStart is greater than _recordEnd:", d1.count())

# COMMAND ----------

tgtColumns='stormwaterNetwork,stormwaterCatchment'

df = spark.sql(f"select {tgtColumns} ,count(*) as recCount from curated_v2.dimStormWaterNetwork  \
                GROUP BY {tgtColumns} HAVING COUNT (*) > 1")
print("Any exact duplicates in the target table")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history curated_v2.dimWaterNetwork

# COMMAND ----------

# ensuring all historical and future records be falling under _RecordCurrent=0 and _recordDeleted=0 .
adt_chk1=spark.sql("""
Select * from (
select * from (
Select stormWaterNetworkSK, date(_RecordStart) as start_dt ,date(_RecordEnd) as end_dt ,_RecordCurrent,_RecordDeleted,max_date
from curated_v2.dimStormWaterNetwork as a,
(Select date(max(_DLCuratedZoneTimeStamp)) as max_date from curated_v2.dimStormWaterNetwork) as b
)
where (max_date > end_dt) or (max_date < start_dt)
)
where _RecordCurrent <> 0 and _RecordDeleted <> 0 
""")
# ensuring all active records should be having high end date=9999-12-31 or future date greater than latest load date.
adt_chk2=spark.sql("""
select * from (
select * from (
Select stormWaterNetworkSK, date(_RecordStart) as start_dt ,date(_RecordEnd) as end_dt ,_RecordCurrent,_RecordDeleted,max_date
from curated_v2.dimStormWaterNetwork as a,
(Select date(max(_DLCuratedZoneTimeStamp)) as max_date from curated_v2.dimStormWaterNetwork) as b
)
where ((max_date < end_dt) and (max_date > start_dt)) or (end_dt='9999-12-31')
)
where _RecordCurrent <> 1 and _RecordDeleted <> 0 """)

print("Count of records where _RecordCurrent=0 and _recordDeleted=0 is not as expected:",adt_chk1.count())
print("Count of records where _RecordCurrent=1 and _recordDeleted=0 is not as expected:",adt_chk2.count())

# COMMAND ----------

# DBTITLE 1,Null validation of specific columns
from pyspark.sql.functions import col

for name in tgt.columns: 

    rec=tgt.filter(col(name).isNull())
    print("column "+name+" count is: ", rec.count())
