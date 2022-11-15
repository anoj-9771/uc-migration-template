# Databricks notebook source
# MAGIC %run ../../atf-common

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
target_df = spark.sql("select * from curated_v2.dimPropertyRelation")
target_df.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated_v2.dimPropertyRelation

# COMMAND ----------

# DBTITLE 1,Source with mapping for active and deleted records
source_isu = spark.sql("""
select
'ISU' as sourceSystemCode
, property1Number 
, property2Number 
, validFromDate 
, validToDate 
, relationshipTypeCode1 
, relationshipType1 
, relationshipTypeCode2 
, relationshipType2 
,_RecordCurrent
,_RecordDeleted
from cleansed.isu_zcd_tprop_rel
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
keyColumns = 'property1Number, property2Number, validFromDate, relationshipTypeCode1'
mandatoryColumns = 'property1Number, property2Number, validFromDate, validToDate,  relationshipTypeCode1, relationshipType1'

columns = ("""
sourceSystemCode
, property1Number 
, property2Number 
, validFromDate 
, validToDate 
, relationshipTypeCode1 
, relationshipType1 
, relationshipTypeCode2 
, relationshipType2 
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

# DBTITLE 1,Check 'Code' columns have matching 'Description' Columns - relationshipType1
# MAGIC %sql
# MAGIC select distinct relationshipTypeCode1, relationshipType1 
# MAGIC from curated_v2.dimPropertyRelation

# COMMAND ----------

# DBTITLE 1,Check 'Code' columns have matching 'Description' Columns - relationshipType2
# MAGIC %sql
# MAGIC select distinct relationshipTypeCode2, relationshipType2 
# MAGIC from curated_v2.dimPropertyRelation

# COMMAND ----------

# DBTITLE 1,Check dummy record is populated
df = spark.sql("select * from curated_v2.dimPropertyService where sourceSystemCode is null")
display(df)
