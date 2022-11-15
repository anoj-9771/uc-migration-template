# Databricks notebook source
# MAGIC %run ../../atf-common

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
targetdf=spark.sql("select * from curated_v2.dimWaterNetwork")
targetdf.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated_v2.dimWaterNetwork

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed.hydra_tsystemarea

# COMMAND ----------

# DBTITLE 1,Source with mapping for active and deleted records
source = spark.sql("""
select
level30 as deliverySystem
,level40 as distributionSystem
,level50 as supplyZone
,coalesce(level60,'Unknown') as pressureArea
,case 
when product = 'Water' then 'Y'
else 'N' end as isPotableWaterNetwork
,case
when product = 'RecycledWater' then 'Y'
else 'N' end as isRecycledWaterNetwork
,_RecordCurrent
,_RecordDeleted
from
cleansed.hydra_tsystemarea where product in ('Water','RecycledWater') 
""")
source.createOrReplaceTempView("source_view")
#display(source_isu)
#source_isu.count()

src_a = spark.sql("Select * except(_RecordCurrent,_recordDeleted) from source_view where _RecordCurrent=1 and _recordDeleted=0 ")
src_d = spark.sql("Select * except(_RecordCurrent,_recordDeleted) from source_view where _RecordCurrent=0 and _recordDeleted=1 ")
src_a.createOrReplaceTempView("src_a")
src_d.createOrReplaceTempView("src_d")

# COMMAND ----------

keyColumns = 'waterNetworkSK'
mandatoryColumns = 'deliverySystem, distributionSystem, supplyZone, isPotableWaterNetwork, isRecycledWaterNetwork'

columns = ("""
deliverySystem
,distributionSystem
,supplyZone
,pressureArea
,isPotableWaterNetwork
,isRecycledWaterNetwork
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
# MAGIC select
# MAGIC level30 as deliverySystem
# MAGIC ,level40 as distributionSystem
# MAGIC ,level50 as supplyZone
# MAGIC ,coalesce(level60,'Unknown') as pressureArea
# MAGIC ,case 
# MAGIC when product = 'Water' then 'Y'
# MAGIC else 'N' end as isPotableWaterNetwork
# MAGIC ,case
# MAGIC when product = 'RecycledWater' then 'Y'
# MAGIC else 'N' end as isRecycledWaterNetwork
# MAGIC , _RecordStart
# MAGIC , _RecordEnd
# MAGIC , _RecordDeleted
# MAGIC , _RecordCurrent
# MAGIC from
# MAGIC cleansed.hydra_tsystemarea where product in ('Water','RecycledWater') 
# MAGIC and _RecordCurrent not in ('0') and _RecordDeleted not in ('1')

# COMMAND ----------

src=spark.sql("""
select
level30 as deliverySystem
,level40 as distributionSystem
,level50 as supplyZone
,coalesce(level60,'Unknown') as pressureArea
,case 
when product = 'Water' then 'Y'
else 'N' end as isPotableWaterNetwork
,case
when product = 'RecycledWater' then 'Y'
else 'N' end as isRecycledWaterNetwork
from
cleansed.hydra_tsystemarea where product in ('Water','RecycledWater') 
and _RecordCurrent not in ('0') and _RecordDeleted not in ('1')
""")
src.createOrReplaceTempView("src")
display(src)
src.count()

# COMMAND ----------

tgt=spark.sql("""
select
deliverySystem
,distributionSystem
,supplyZone
,pressureArea
,isPotableWaterNetwork
,isRecycledWaterNetwork
from
curated_v2.dimWaterNetwork where _recordCurrent = 1 and _recordDeleted = 0
""")
tgt.createOrReplaceTempView("tgt")
display(tgt)
tgt.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated_v2.dimWaterNetwork where deliverySystem='Unknown'

# COMMAND ----------

# DBTITLE 1,Duplicate Check- Active & All Records
dup_act=spark.sql("Select waterNetworkSK,count(*) from curated_v2.dimWaterNetwork where _RecordCurrent=1 and _recordDeleted=0 group by waterNetworkSK having count(*) > 1")
dup_all=spark.sql("Select waterNetworkSK, count(*) from  curated_v2.dimWaterNetwork group by waterNetworkSK having count(*) > 1")
dup_act1=spark.sql("Select waterNetworkSK,date(_recordStart) as start_date,count(*) from  curated_v2.dimWaterNetwork where _RecordCurrent=1 and _recordDeleted=0 group by 1,2 having count(*) > 1")
dup_all1=spark.sql("Select waterNetworkSK,date(_recordStart) as start_date,count(*) from  curated_v2.dimWaterNetwork group by 1,2 having count(*) > 1")
print("Duplicate count of active records :",dup_act.count())
print("Duplicate count of all records :",dup_all.count())
print("Duplicate count of active records with start date :",dup_act1.count())
print("Duplicate count of all records with start date :",dup_all1.count())

# COMMAND ----------

# DBTITLE 1,business columns Validation
BusCol_chk=spark.sql(""" Select * from curated_v2.dimWaterNetwork
                         where (waterNetworkSK is NULL or waterNetworkSK in ('',' ') or UPPER(waterNetworkSK)='NULL') 
                         """)
print("Count of records where business columns are NULL/BLANK :",BusCol_chk.count())



BusCol_chk1=spark.sql(""" select distinct length(waterNetworkSK) from curated_v2.dimWaterNetwork
                         where length(waterNetworkSK)<>2
                         """)
print("Expected count of distinct length of Business Column is 1, Actual count is :",BusCol_chk2.count())



# COMMAND ----------

# DBTITLE 1,SK columns validation
sk_chk1=spark.sql(""" Select * from curated_v2.dimWaterNetwork where (waterNetworkSK is NULL or waterNetworkSK in ('',' ') or UPPER(waterNetworkSK)='NULL')""")
sk_chk2=spark.sql("Select waterNetworkSK,count(*) from curated_v2.dimWaterNetwork where _RecordCurrent=1 and _recordDeleted=0 group by waterNetworkSK having count(*) > 1")
sk_chk3=spark.sql("Select waterNetworkSK,count(*) from curated_v2.dimWaterNetwork group by waterNetworkSK having count(*) > 1")

print("Count of records where SK columns are NULL/BLANK :",sk_chk1.count())
print("Duplicate count of SK for active records :",sk_chk2.count())
print("Duplicate count of SK for all records :",sk_chk3.count())

# COMMAND ----------

# DBTITLE 1,Date Validation
d1=spark.sql("""Select * from curated_v2.dimWaterNetwork where date(_recordStart) > date(_recordEnd)""")
print("Count of records of where _recordStart is greater than _recordEnd:", d1.count())


# COMMAND ----------

# DBTITLE 1,Overlap and Gap Validation
d2=spark.sql("""
Select waterNetworkSK from
(Select waterNetworkSK, date(_recordStart) as start_date,date(_recordEnd) as end_date,
max(date(_recordStart)) over (partition by waterNetworkSK order by _recordStart rows between 1 following and 1 following) as nxt_date 
from curated_v2.dimWaterNetwork )
where  DATEDIFF(day,nxt_date,end_date) <> 1
""")

print("Count of records of where overlap and gap is observed:", d2.count())			

# COMMAND ----------

# DBTITLE 1,Exact duplicates in target
# need to check how this work with other duplicate query
tgtColumns = 'waterNetworkSK,deliverySystem,distributionSystem,supplyZone,pressureArea,isPotableWaterNetwork,isRecycledWaterNetwork,_BusinessKey,_DLCuratedZoneTimeStamp,_RecordStart,_RecordEnd,_RecordDeleted,_RecordCurrent'

df = spark.sql(f"select {tgtColumns} ,count(*) as recCount from curated_v2.dimWaterNetwork  \
                GROUP BY {tgtColumns} HAVING COUNT (*) > 1")
print("Any exact duplicates in the target table")
display(df)

# COMMAND ----------

src_act=spark.sql("select * from src")
tgt_act=spark.sql("select * from tgt ") # _RecordCurrent=1
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

sk_chk1=spark.sql(""" Select * from curated_v2.dimWaterNetwork where (waterNetworkSK is NULL or waterNetworkSK in ('',' ') or UPPER(waterNetworkSK)='NULL')""")
sk_chk2=spark.sql("Select waterNetworkSK,count(*) from curated_v2.dimWaterNetwork where _RecordCurrent=1 and _recordDeleted=0 group by waterNetworkSK having count(*) > 1")
sk_chk3=spark.sql("Select waterNetworkSK,count(*) from curated_v2.dimWaterNetwork group by waterNetworkSK having count(*) > 1")

print("Count of records where SK columns are NULL/BLANK :",sk_chk1.count())
print("Duplicate count of SK for active records :",sk_chk2.count())
print("Duplicate count of SK for all records :",sk_chk3.count())

# COMMAND ----------

d1=spark.sql("""Select * from curated_v2.dimWaterNetwork where date(_recordStart) > date(_recordEnd)""")
print("Count of records of where _recordStart is greater than _recordEnd:", d1.count())

# COMMAND ----------

# need to check how this work with other duplicate query

tgtColumns='deliverySystem,distributionSystem,supplyZone,pressureArea,isPotableWaterNetwork,isRecycledWaterNetwork'

df = spark.sql(f"select {tgtColumns} ,count(*) as recCount from curated_v2.dimWaterNetwork  \
                GROUP BY {tgtColumns} HAVING COUNT (*) > 1")
print("Any exact duplicates in the target table")
display(df)

# COMMAND ----------

print("Source Count:",src.count())
print("Target Count:",tgt.count())

diff1=src.subtract(tgt)
diff2=tgt.subtract(src)
display(diff1)
display(diff2)

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history curated_v2.dimWaterNetwork

# COMMAND ----------

# ensuring all historical and future records be falling under _RecordCurrent=0 and _recordDeleted=0 .
adt_chk1=spark.sql("""
Select * from (
select * from (
Select waterNetworkSK, date(_RecordStart) as start_dt ,date(_RecordEnd) as end_dt ,_RecordCurrent,_RecordDeleted,max_date
from curated_v2.dimWaterNetwork as a,
(Select date(max(_DLCuratedZoneTimeStamp)) as max_date from curated_v2.dimWaterNetwork) as b
)
where (max_date > end_dt) or (max_date < start_dt)
)
where _RecordCurrent <> 0 and _RecordDeleted <> 0 
""")
# ensuring all active records should be having high end date=9999-12-31 or future date greater than latest load date.
adt_chk2=spark.sql("""
select * from (
select * from (
Select waterNetworkSK, date(_RecordStart) as start_dt ,date(_RecordEnd) as end_dt ,_RecordCurrent,_RecordDeleted,max_date
from curated_v2.dimWaterNetwork as a,
(Select date(max(_DLCuratedZoneTimeStamp)) as max_date from curated_v2.dimWaterNetwork) as b
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

# COMMAND ----------

a1=spark.sql("Select deliverySystem,distributionSystem,count(*) from curated_v2.dimWaterNetwork group by 1,2")

display(a1)

