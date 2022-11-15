# Databricks notebook source
# MAGIC %run ../../atf-common

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from curated_v2.dimContractAccount

# COMMAND ----------

# DBTITLE 1,[Target] Schema Check
target_df = spark.sql("select * from curated_v2.dimContractAccount")
target_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Source with mapping for active and deleted records
source_isu=spark.sql("""
select
'ISU' as sourceSystemCode,
contractAccountNumber,
legacyContractAccountNumber,
applicationAreaCode,
applicationArea,
contractAccountCategoryCode,
contractAccountCategory,
createdBy,
createdDate,
lastChangedBy,
lastChangedDate,
_RecordStart,
_RecordEnd,
_RecordCurrent,
_RecordDeleted
from
cleansed.isu_0cacont_acc_attr_2 """)
source_isu.createOrReplaceTempView("source_view")
src_a = spark.sql("Select * except(_RecordCurrent,_recordDeleted) from source_view where _RecordCurrent=1 and _recordDeleted=0 ")
src_d = spark.sql("Select * except(_RecordCurrent,_recordDeleted) from source_view where _RecordCurrent=0 and _recordDeleted=1 ")
src_a.createOrReplaceTempView("src_a")
src_d.createOrReplaceTempView("src_d")


# COMMAND ----------

# DBTITLE 1,Define Variables for ATF
keyColumns = 'contractAccountNumber'
mandatoryColumns = 'contractAccountNumber'

columns = ("""
sourceSystemCode,
contractAccountNumber,
legacyContractAccountNumber,
applicationAreaCode,
applicationArea,
contractAccountCategoryCode,
contractAccountCategory,
createdBy,
createdDate,
lastChangedBy,
lastChangedDate


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

# MAGIC %md
# MAGIC Investigation for failed tests

# COMMAND ----------

# MAGIC %sql
# MAGIC Select sourceSystemCode,
# MAGIC contractAccountNumber,
# MAGIC legacyContractAccountNumber,
# MAGIC applicationAreaCode,
# MAGIC applicationArea,
# MAGIC contractAccountCategoryCode,
# MAGIC contractAccountCategory,
# MAGIC createdBy,
# MAGIC createdDate,
# MAGIC lastChangedBy,
# MAGIC lastChangedDate from source_view where _RecordCurrent=1 and _recordDeleted=0
# MAGIC minus
# MAGIC Select 
# MAGIC sourceSystemCode,
# MAGIC contractAccountNumber,
# MAGIC legacyContractAccountNumber,
# MAGIC applicationAreaCode,
# MAGIC applicationArea,
# MAGIC contractAccountCategoryCode,
# MAGIC contractAccountCategory,
# MAGIC createdBy,
# MAGIC createdDate,
# MAGIC lastChangedBy,
# MAGIC lastChangedDate
# MAGIC from curated_v2.dimContractAccount where _RecordCurrent=1 and _recordDeleted=0

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(*)  from curated_v2.dimContractAccount where _RecordCurrent=1 and _recordDeleted=0
